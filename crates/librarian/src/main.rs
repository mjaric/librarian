//! librarian — the bookshelf backend.
//!
//! Data subcommands (`sync`, `repair`) are pure clients: they enqueue a
//! Postgres job and NOTIFY; only `daemon` executes. `migrate`, `status` and
//! `retry-failed` are direct, fast DB operations.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand};

use bookshelf_core::{EventLog, InterruptFlag, StorePostgres};

use librarian::config::Config;
use librarian::gutenberg_org::GutenbergOrg;
use librarian::provider::{ensure_known_key, resolve, CycleOpts, Provider};
use librarian::queue::JobQueue;
use librarian::trigger::Trigger;

#[derive(Parser)]
#[command(
    name = "librarian",
    version,
    about = "bookshelf backend — provider-based archive synchronizer"
)]
struct Cli {
    /// Config path (default: $BOOKSHELF_CONFIG, then ./librarian.toml)
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Apply sqlx migrations and exit (other subcommands auto-migrate too)
    Migrate,
    /// Enqueue a sync job (weekly full rsync delta; targeted with --only/--limit)
    Sync {
        /// Enqueue the daily today.rss feed cycle instead of a full cycle
        #[arg(long)]
        feed: bool,
        /// Pull at most N book dirs (module listing → targeted pull)
        #[arg(long)]
        limit: Option<usize>,
        /// Pull exactly these book ids (repeatable)
        #[arg(long = "only")]
        only: Vec<i64>,
        /// Pull files but skip RDF ingest
        #[arg(long)]
        no_ingest: bool,
        /// Block until the job reaches a terminal state
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value = "project-gutenberg")]
        provider: String,
    },
    /// Enqueue an HTTP repair job for pending/size-mismatch files
    Repair {
        #[arg(long = "only")]
        only: Vec<i64>,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value = "project-gutenberg")]
        provider: String,
    },
    /// Run the backend: embedded scheduler + the sole job executor
    ///
    /// The daemon embeds its own schedule (feed every feed_check_days, full
    /// rsync every full_sync_interval_days) — no external timers needed.
    ///
    /// Example systemd unit:
    ///
    ///   [Unit]
    ///   Description=bookshelf librarian daemon
    ///   After=network-online.target postgresql.service
    ///
    ///   [Service]
    ///   ExecStart=/usr/local/bin/librarian daemon --config /etc/librarian.toml
    ///   Environment=BOOKSHELF_DATABASE_URL=postgres://...
    ///   Restart=on-failure
    ///   KillSignal=SIGTERM
    ///   TimeoutStopSec=30
    ///
    ///   [Install]
    ///   WantedBy=multi-user.target
    Daemon {
        /// Override feed_check_days (0 disables feed checks)
        #[arg(long = "feed-days")]
        feed_days: Option<u64>,
        /// Override full_sync_interval_days
        #[arg(long = "full-days")]
        full_days: Option<u64>,
        #[arg(long, default_value = "project-gutenberg")]
        provider: String,
    },
    /// Per-status book counts, mirror stats, repair queue state
    Status {
        #[arg(long, default_value = "project-gutenberg")]
        provider: String,
    },
    /// Downloaded vs total per source (live remote listing + local DB)
    Progress {
        /// Restrict to one source (default: all registered providers)
        #[arg(long)]
        provider: Option<String>,
    },
    /// failed_permanent books → discovered, attempts reset
    RetryFailed {
        #[arg(long, default_value = "project-gutenberg")]
        provider: String,
    },
}

/// Mask the credentials of a database URL for logging.
fn redact_url(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('@') {
            Some((_, hostpart)) => format!("{scheme}://***@{hostpart}"),
            None => url.to_string(),
        },
        None => url.to_string(),
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn open_store(cfg: &Config) -> anyhow::Result<Arc<StorePostgres>> {
    let store = StorePostgres::connect(&cfg.database_url).await?;
    store.migrate().await?;
    Ok(Arc::new(store))
}

fn build_providers(
    cfg: Arc<Config>,
    store: Arc<StorePostgres>,
    events: Arc<EventLog>,
    interrupt: InterruptFlag,
) -> anyhow::Result<Vec<Arc<dyn Provider>>> {
    Ok(vec![Arc::new(GutenbergOrg::new(
        cfg, store, events, interrupt,
    )?) as Arc<dyn Provider>])
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let cfg = Arc::new(Config::load(cli.config.as_deref())?);

    match cli.command {
        Cmd::Migrate => {
            let store = open_store(&cfg).await?;
            println!("migrations applied ({})", redact_url(&cfg.database_url));
            drop(store);
            Ok(())
        }
        Cmd::Sync {
            feed,
            limit,
            only,
            no_ingest,
            wait,
            provider,
        } => {
            let store = open_store(&cfg).await?;
            ensure_known_key(&provider)?;
            let queue = JobQueue::new(store.pool().clone());
            let opts = CycleOpts { feed, limit, only: only.clone(), no_ingest };
            let kind = if feed { "feed_cycle" } else { "full_cycle" };
            let id = queue
                .enqueue(&provider, kind, &serde_json::to_value(&opts)?, Trigger::Cli)
                .await?;
            println!("enqueued job {id} ({kind})");
            if wait {
                wait_for_job(&queue, &store, &provider, id).await?;
            }
            Ok(())
        }
        Cmd::Repair { only, wait, provider } => {
            let store = open_store(&cfg).await?;
            ensure_known_key(&provider)?;
            let queue = JobQueue::new(store.pool().clone());
            let payload = serde_json::json!({ "only": only });
            let id = queue
                .enqueue(&provider, "repair", &payload, Trigger::Cli)
                .await?;
            println!("enqueued job {id} (repair)");
            if wait {
                wait_for_job(&queue, &store, &provider, id).await?;
            }
            Ok(())
        }
        Cmd::Daemon {
            feed_days,
            full_days,
            provider,
        } => {
            let mut cfg = Arc::try_unwrap(cfg)
                .map_err(|_| anyhow::anyhow!("config arc"))?; // sole owner here
            if let Some(fd) = feed_days {
                cfg.feed_check_days = fd;
            }
            if let Some(fu) = full_days {
                cfg.full_sync_interval_days = fu;
            }
            let cfg = Arc::new(cfg);
            run_daemon(cfg, &provider).await
        }
        Cmd::Status { provider } => {
            let store = open_store(&cfg).await?;
            let events = Arc::new(EventLog::open(&cfg.events_path())?);
            let providers = build_providers(cfg, store, events, InterruptFlag::new())?;
            let p = resolve(&providers, &provider)?;
            let s = p.status().await?;
            println!("provider: {}", p.key());
            for (status, count) in s.books_by_status {
                println!("  books {status}: {count}");
            }
            println!("  categories: {}", s.categories);
            println!("  mirror files: {} ({:.2} GiB)", s.mirror_files, s.mirror_bytes as f64 / (1024.0 * 1024.0 * 1024.0));
            println!("  repair pending: {}, failed: {}", s.repair_pending, s.repair_failed);
            if let Some(t) = s.min_retry_at {
                println!("  min retry_at: {t}");
            }
            if let Some(run) = s.last_run {
                println!("  last run: {run}");
            }
            if let Some(t) = s.next_full_sync {
                println!("  next full sync: {t}");
            }
            if let Some(t) = s.next_feed_check {
                println!("  next feed check: {t}");
            }
            Ok(())
        }
        Cmd::Progress { provider } => {
            let store = open_store(&cfg).await?;
            let events = Arc::new(EventLog::open(&cfg.events_path())?);
            let providers = build_providers(cfg, store, events, InterruptFlag::new())?;
            let selected: Vec<_> = match &provider {
                Some(key) => vec![resolve(&providers, key)?.clone()],
                None => providers.clone(),
            };
            let mut total_fraction = 0.0f64;
            for p in &selected {
                let s = p.progress().await?;
                let pct = s.fraction() * 100.0;
                println!(
                    "{}: {}/{} books downloaded ({pct:.2}%) — synced {} ({:.2}%), enriched {}, failed {} | mirror {} files, {:.2} GiB",
                    s.source,
                    s.ingested,
                    s.total_remote,
                    s.synced,
                    if s.total_remote > 0 { s.synced as f64 / s.total_remote as f64 * 100.0 } else { 0.0 },
                    s.enriched,
                    s.failed_permanent,
                    s.mirror_files,
                    s.mirror_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                );
                total_fraction += s.fraction();
            }
            if selected.len() > 1 {
                println!(
                    "all sources: {:.2}% of combined catalogs present",
                    100.0 * total_fraction / selected.len() as f64
                );
            }
            Ok(())
        }
        Cmd::RetryFailed { provider } => {
            let store = open_store(&cfg).await?;
            let events = Arc::new(EventLog::open(&cfg.events_path())?);
            let providers = build_providers(cfg, store, events, InterruptFlag::new())?;
            let p = resolve(&providers, &provider)?;
            let n = p.retry_failed().await?;
            println!("reset {n} failed_permanent books to discovered");
            Ok(())
        }
    }
}

/// `--wait` client: heartbeat warning + poll the job row to terminal state.
async fn wait_for_job(
    queue: &JobQueue,
    store: &Arc<StorePostgres>,
    provider: &str,
    id: i64,
) -> anyhow::Result<()> {
    // daemon heartbeat staleness check (warn once)
    if let Some(hb) = store.get_meta(provider, "daemon_heartbeat").await? {
        if let Ok(t) = time::OffsetDateTime::parse(
            &hb,
            &time::format_description::well_known::Rfc3339,
        ) {
            let age = time::OffsetDateTime::now_utc() - t;
            if age.whole_seconds() > 15 {
                eprintln!("warning: daemon not running — job stays queued (heartbeat {age:?} old)");
            }
        }
    }
    loop {
        let job = queue
            .get(id)
            .await?
            .with_context(|| format!("job {id} vanished"))?;
        match job.status.as_str() {
            "queued" | "running" => {
                let pos = queue.position(id).await.unwrap_or(0);
                println!("\rjob {id}: {} ({} queued ahead)", job.status, pos);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            "done" => {
                println!("job {id}: done (run_id {:?})", job.run_id);
                return Ok(());
            }
            "failed" => {
                anyhow::bail!("job {id} failed: {}", job.error.unwrap_or_default());
            }
            other => anyhow::bail!("job {id} in unknown state {other}"),
        }
    }
}

async fn run_daemon(cfg: Arc<Config>, provider_key: &str) -> anyhow::Result<()> {
    let store = open_store(&cfg).await?;
    let events = Arc::new(EventLog::open(&cfg.events_path())?);
    let interrupt = InterruptFlag::new();
    let providers = build_providers(cfg.clone(), store.clone(), events, interrupt.clone())?;
    let provider = resolve(&providers, provider_key)?;
    let source = provider.key();
    let queue = JobQueue::new(store.pool().clone());

    // -- startup reclaim + meta anchors
    let reclaimed = store.reclaim_running_jobs().await?;
    if reclaimed > 0 {
        tracing::warn!(reclaimed, "requeued jobs stuck in running");
    }
    let now = time::OffsetDateTime::now_utc();
    let rfc3339 = |t: time::OffsetDateTime| {
        t.format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default()
    };
    store.set_meta(source, "daemon_anchor", &rfc3339(now)).await?;
    if store.get_meta(source, "next_full_sync").await?.is_none() {
        let first = if cfg.backfill_on_start {
            now
        } else {
            now + time::Duration::days(cfg.full_sync_interval_days as i64)
        };
        store.set_meta(source, "next_full_sync", &rfc3339(first)).await?;
    }

    // -- signals
    let sig_flag = interrupt.clone();
    tokio::spawn(async move {
        let term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        let mut term = match term {
            Ok(s) => s,
            Err(_) => return,
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => sig_flag.set(),
            _ = term.recv() => sig_flag.set(),
        }
    });

    // -- scheduler half
    {
        let store = store.clone();
        let queue = queue.clone();
        let cfg = cfg.clone();
        let source = source.to_string();
        let flag = interrupt.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if flag.is_set() {
                    break;
                }
                if let Err(e) = scheduler_tick(&store, &queue, &cfg, &source).await {
                    tracing::warn!(error = %e, "scheduler tick failed");
                }
            }
        });
    }

    // -- worker half
    let mut listener = librarian::queue::listen(queue.pool()).await?;
    tracing::info!(source, "daemon ready");
    println!("daemon ready (source {source})");

    let heartbeat_every = std::time::Duration::from_secs(5);
    let mut last_heartbeat = time::OffsetDateTime::UNIX_EPOCH;
    loop {
        if interrupt.is_set() {
            break;
        }
        if time::OffsetDateTime::now_utc() - last_heartbeat >= heartbeat_every {
            store.set_meta(source, "daemon_heartbeat", &rfc3339(time::OffsetDateTime::now_utc())).await?;
            last_heartbeat = time::OffsetDateTime::now_utc();
        }
        match queue.pick_next().await? {
            None => {
                // wake on NOTIFY with a 5 s poll floor
                let _ = tokio::time::timeout(heartbeat_every, listener.recv()).await;
            }
            Some(job) => {
                tracing::info!(job = job.id, kind = %job.kind, "executing job");
                let outcome = execute_job(provider.as_ref(), &job).await;
                match outcome {
                    Ok(report) => {
                        if interrupt.is_set()
                            || report
                                .aborted_reason
                                .as_deref()
                                .map(|r| r == "interrupted")
                                .unwrap_or(false)
                        {
                            let requeued = queue.requeue_interrupted(job.id).await?;
                            tracing::warn!(job = job.id, requeued, "interrupted — job requeued");
                            break;
                        }
                        queue.complete(job.id, Some(report.run_id), None).await?;
                        tracing::info!(job = job.id, run_id = report.run_id, "job done");
                    }
                    Err(e) => {
                        if interrupt.is_set() {
                            let requeued = queue.requeue_interrupted(job.id).await?;
                            tracing::warn!(job = job.id, requeued, "interrupted — job requeued");
                            break;
                        }
                        tracing::error!(job = job.id, error = %e, "job failed");
                        queue.complete(job.id, None, Some(&e.to_string())).await?;
                    }
                }
            }
        }
    }

    // graceful exit: sync_runs rows for this daemon's jobs were already
    // finalized by the providers themselves ('interrupted')
    println!("daemon stopped cleanly");
    Ok(())
}

#[derive(Debug)]
struct JobSummary {
    run_id: i64,
    aborted_reason: Option<String>,
}

async fn execute_job(provider: &dyn Provider, job: &librarian::queue::JobRow) -> anyhow::Result<JobSummary> {
    match job.kind.as_str() {
        "full_cycle" => {
            let opts: CycleOpts =
                serde_json::from_value(job.payload.clone()).context("full_cycle payload")?;
            let r = provider.full_cycle(opts).await?;
            Ok(JobSummary { run_id: r.run_id, aborted_reason: r.aborted_reason })
        }
        "feed_cycle" => {
            let r = provider.feed_cycle().await?;
            Ok(JobSummary { run_id: r.run_id, aborted_reason: r.aborted_reason })
        }
        "repair" => {
            let only: Vec<i64> = job
                .payload
                .get("only")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let only_ref = if only.is_empty() { None } else { Some(&only[..]) };
            let r = provider.repair(only_ref).await?;
            Ok(JobSummary { run_id: r.run_id, aborted_reason: r.aborted_reason })
        }
        other => anyhow::bail!("unknown job kind {other:?}"),
    }
}

async fn scheduler_tick(
    store: &Arc<StorePostgres>,
    queue: &JobQueue,
    cfg: &Config,
    source: &str,
) -> anyhow::Result<()> {
    let now = time::OffsetDateTime::now_utc();
    let rfc3339 = |t: time::OffsetDateTime| {
        t.format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default()
    };

    // feed due?
    if cfg.feed_check_days > 0 {
        let due = match store.get_meta(source, "last_feed_check").await? {
            None => true,
            Some(last) => {
                time::OffsetDateTime::parse(
                    &last,
                    &time::format_description::well_known::Rfc3339,
                )
                .map(|t| now - t >= time::Duration::days(cfg.feed_check_days as i64))
                .unwrap_or(true)
            }
        };
        if due {
            if let Some(id) = queue
                .enqueue_coalesced(source, "feed_cycle", &serde_json::json!({}))
                .await?
            {
                tracing::info!(job = id, "scheduler: feed cycle due");
            }
        }
    }

    // full due?
    if let Some(next) = store.get_meta(source, "next_full_sync").await? {
        if let Ok(next) =
            time::OffsetDateTime::parse(&next, &time::format_description::well_known::Rfc3339)
        {
            if next <= now {
                if let Some(id) = queue
                    .enqueue_coalesced(source, "full_cycle", &serde_json::json!({}))
                    .await?
                {
                    tracing::info!(job = id, "scheduler: full cycle due");
                }
                // advance anchor regardless — a cycle already queued/running
                // absorbs this due date (coalescing)
                let advanced = now + time::Duration::days(cfg.full_sync_interval_days as i64);
                store.set_meta(source, "next_full_sync", &rfc3339(advanced)).await?;
            }
        }
    }
    Ok(())
}
