//! librarian — the bookshelf backend.
//!
//! Data subcommands (`sync`, `repair`) are pure clients: they enqueue a
//! Postgres job and NOTIFY; only `daemon` executes. `migrate`, `status`,
//! `runs` and `jobs` are direct, fast DB reads (plus `watch`, which loops
//! those reads); `retry-failed` is a direct DB repair.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand};

use bookshelf_core::{EventLog, InterruptFlag, StorePostgres};

use librarian::config::Config;
use librarian::gutenberg_org::{GutenbergOrg, SOURCE_KEY};
use librarian::monitor;
use librarian::provider::{AdoptAction, CycleOpts, Provider, ensure_known_key, resolve};
use librarian::queue::JobQueue;
use librarian::supervisor::{LauncherKind, ProcessLauncher, SyncLauncher};
use librarian::supervisor_docker::DockerLauncher;
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
    /// Daemon liveness + active cycle + queue, then the provider report
    Status {
        #[arg(long, default_value = "project-gutenberg")]
        provider: String,
    },
    /// Live-updating dashboard of the cheap DB views (Ctrl-C exits)
    Watch {
        /// Seconds between refreshes (≥ 1)
        #[arg(long, default_value_t = 2)]
        interval: u64,
        #[arg(long, default_value = "project-gutenberg")]
        provider: String,
    },
    /// Recent sync_runs rows, newest first (in-flight ones marked running)
    Runs {
        /// How many runs to show
        #[arg(long, default_value_t = 20)]
        limit: i64,
        #[arg(long, default_value = "project-gutenberg")]
        provider: String,
    },
    /// Recent queue jobs, newest first
    Jobs {
        /// How many jobs to show
        #[arg(long, default_value_t = 20)]
        limit: i64,
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
    let launcher: Arc<dyn SyncLauncher> = match cfg.supervisor_launcher {
        LauncherKind::Process => Arc::new(ProcessLauncher),
        LauncherKind::Docker => {
            let Some(image) = cfg.docker_image.as_deref() else {
                anyhow::bail!("launcher = \"docker\" requires [supervisor] docker_image");
            };
            DockerLauncher::probe(Path::new("docker"))?;
            let docker = DockerLauncher::new(image);
            let swept = docker.reap_orphans(SOURCE_KEY, &cfg.run_root())?;
            if swept > 0 {
                tracing::info!(swept, "boot sweep: removed orphaned docker transfers");
            }
            Arc::new(docker)
        }
    };
    Ok(vec![
        Arc::new(GutenbergOrg::new(cfg, store, events, interrupt, launcher)?) as Arc<dyn Provider>,
    ])
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
            let opts = CycleOpts {
                feed,
                limit,
                only: only.clone(),
                no_ingest,
            };
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
        Cmd::Repair {
            only,
            wait,
            provider,
        } => {
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
            let mut cfg = Arc::try_unwrap(cfg).map_err(|_| anyhow::anyhow!("config arc"))?; // sole owner here
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
            // cheap DB views first — the provider walk below stays one-off
            println!("{}", monitor::daemon_section(&store, &provider).await?);
            println!();
            println!(
                "{}",
                monitor::active_cycle_section(&store, &provider).await?
            );
            println!();
            println!("{}", monitor::queue_section(&store, &provider).await?);
            println!();
            let events = Arc::new(EventLog::open(&cfg.events_path())?);
            let providers = build_providers(cfg, store, events, InterruptFlag::new())?;
            let p = resolve(&providers, &provider)?;
            let s = p.status().await?;
            println!("provider: {}", p.key());
            for (status, count) in s.books_by_status {
                println!("  books {status}: {count}");
            }
            println!("  categories: {}", s.categories);
            println!(
                "  mirror files: {} ({:.2} GiB)",
                s.mirror_files,
                s.mirror_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
            );
            println!(
                "  repair pending: {}, failed: {}",
                s.repair_pending, s.repair_failed
            );
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
        Cmd::Watch { interval, provider } => {
            if interval == 0 {
                anyhow::bail!("--interval must be at least 1 second, got {interval}");
            }
            ensure_known_key(&provider)?;
            let store = open_store(&cfg).await?;
            loop {
                print!("\x1b[2J\x1b[H");
                print!("{}", monitor::watch_frame(&store, &provider, 3).await);
                let _ = std::io::stdout().flush();
                tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
            }
        }
        Cmd::Runs { limit, provider } => {
            if limit < 1 {
                anyhow::bail!("--limit must be at least 1, got {limit}");
            }
            ensure_known_key(&provider)?;
            let store = open_store(&cfg).await?;
            let runs = store.recent_runs(&provider, limit).await?;
            println!("RUNS — {provider}, last {limit}");
            println!(
                "{}",
                monitor::runs_table(&runs, time::OffsetDateTime::now_utc())
            );
            Ok(())
        }
        Cmd::Jobs { limit, provider } => {
            if limit < 1 {
                anyhow::bail!("--limit must be at least 1, got {limit}");
            }
            ensure_known_key(&provider)?;
            let store = open_store(&cfg).await?;
            let queue = JobQueue::new(store.pool().clone());
            let jobs = queue.recent(&provider, limit).await?;
            println!("JOBS — {provider}, last {limit}");
            println!("{}", monitor::jobs_table(&jobs));
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
                    if s.total_remote > 0 {
                        s.synced as f64 / s.total_remote as f64 * 100.0
                    } else {
                        0.0
                    },
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
        if let Ok(t) =
            time::OffsetDateTime::parse(&hb, &time::format_description::well_known::Rfc3339)
        {
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

    // -- observability: in-memory snapshot always on, OTLP export opt-in
    let obs = librarian::observability::Observability::new(source, cfg.otlp_endpoint.as_deref());
    provider.set_observability(obs.clone());

    // -- signals (before adoption: SIGTERM mid-adopt must detach cleanly)
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

    // -- executor fence
    // The session-scoped advisory lock fail-fasts a second daemon for this
    // source. The ping task below exists because the lock lives on ONE
    // pooled session: if that session dies (PG restart, network drop),
    // Postgres RELEASES the lock and a successor could start while this
    // daemon keeps running. On ping failure the flag stops this daemon
    // rather than run unsupervised beside another executor.
    let executor_guard = store
        .executor_lock(&format!("librarian-executor:{source}"))
        .await?;
    {
        let flag = interrupt.clone();
        tokio::spawn(async move {
            let guard = executor_guard;
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if let Err(e) = guard.ping().await {
                    tracing::error!(error = %e, "executor fence ping failed — stopping daemon");
                    flag.set();
                    break;
                }
            }
        });
    }

    // -- meta anchors (before adoption: it can run for the length of a
    // resumed transfer, and the anchor should reflect daemon start)
    let now = time::OffsetDateTime::now_utc();
    let rfc3339 = |t: time::OffsetDateTime| {
        t.format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default()
    };
    store
        .set_meta(source, "daemon_anchor", &rfc3339(now))
        .await?;
    if store.get_meta(source, "next_full_sync").await?.is_none() {
        let first = if cfg.backfill_on_start {
            now
        } else {
            now + time::Duration::days(cfg.full_sync_interval_days as i64)
        };
        store
            .set_meta(source, "next_full_sync", &rfc3339(first))
            .await?;
    }

    // -- boot-time adoption of detached runs (replaces the boot reclaim)
    let report = provider.adopt().await?;
    // Captured before the match: the Failed arm moves the error string out
    // of report.action, so the boot sweep below cannot re-inspect it.
    let left_detached = matches!(report.action, AdoptAction::LeftDetached);
    match report.action {
        AdoptAction::AdoptedAndCompleted { run_id } => {
            let job_id = report.job_id.context("adopted run without a bound job")?;
            queue.complete(job_id, Some(run_id), None).await?;
            tracing::info!(job = job_id, run_id, "adopted detached run — completed");
        }
        AdoptAction::Requeued => {
            if let Some(job_id) = report.job_id {
                let requeued = queue.requeue_interrupted(job_id).await?;
                tracing::warn!(
                    job = job_id,
                    requeued,
                    "adopted run not resumable — job requeued"
                );
            }
        }
        AdoptAction::Failed(error) => {
            if let Some(job_id) = report.job_id {
                tracing::error!(job = job_id, error = %error, "adopted run failed");
                queue.complete(job_id, None, Some(&error)).await?;
            }
        }
        AdoptAction::LeftDetached => {
            // Boot adoption was cut short by shutdown: the surviving
            // transfer keeps running and the job deliberately stays
            // 'running' (enqueue_coalesced fences new cycles; the next
            // daemon binds job + artifacts and resumes). Touch NOTHING.
            tracing::info!("adopt interrupted by shutdown — run left alive, next daemon re-adopts");
        }
        AdoptAction::Nothing => {
            // Reclaim replacement for the artifact-less window: a job can
            // be stuck 'running' when the previous daemon died outside any
            // adoptable transfer (listing/ingest/repair phases, or a stop
            // before the spawn). The old boot reclaim covered exactly this
            // — keep it, scoped to the running job via the frozen queue API.
            if let Some(job) = queue
                .recent(source, 50)
                .await?
                .into_iter()
                .find(|job| job.status == "running")
            {
                let requeued = queue.requeue_interrupted(job.id).await?;
                tracing::warn!(
                    job = job.id,
                    requeued,
                    "running job without run artifacts — requeued"
                );
            }
        }
    }
    // Boot sweep: close sync_runs rows still open from daemons that died
    // without artifacts (adoption closes the artifact-bearing ones). Skipped
    // entirely on LeftDetached — that run is alive and its row must stay open.
    // Safe without further guards: the executor fence was taken BEFORE adopt,
    // so no other daemon for this source exists at this point.
    if !left_detached {
        let closed = store.abort_stale_runs(source).await?;
        if closed > 0 {
            tracing::warn!(
                closed,
                "closed abandoned sync_runs left open by dead daemons"
            );
        }
    }
    // A hard kill (SIGKILL/crash) bypasses the cycle guard's Drop — clear
    // any stale active_run so the CLI never sees a ghost run. (Any adopt
    // guard has dropped by the time we get here.)
    store.clear_meta(source, "active_run").await?;

    // -- scheduler half
    {
        let store = store.clone();
        let queue = queue.clone();
        let cfg = cfg.clone();
        let source = source.to_string();
        let flag = interrupt.clone();
        let obs = obs.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut obs_ticks: u64 = 0;
            let mut no_mirror_logged = false;
            loop {
                tick.tick().await;
                if flag.is_set() {
                    break;
                }
                // Heartbeat from the scheduler half: the worker loop blocks
                // inside execute_job during long cycles, so this 30 s write
                // keeps the heartbeat a "process alive" signal. Written
                // before the snapshot refresh so heartbeat_age reflects it.
                let now = time::OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_default();
                if let Err(e) = store.set_meta(&source, "daemon_heartbeat", &now).await {
                    tracing::warn!(error = %e, "heartbeat write failed");
                }
                // Snapshot refresh is observability, not scheduling: never
                // fail the tick because of it. The mirror book count is a
                // blocking filesystem walk (thousands of stat calls on a
                // full mirror): refresh it every 10th tick (30 s cadence →
                // every 5 min), including the immediate first tick so a
                // fresh daemon publishes a real number; ingest_gap follows
                // every tick from the fresh DB counts. No mirror dir
                // (non-mirror host) → zero, logged once at debug.
                obs_ticks += 1;
                let mirror_books = if obs_ticks % 10 == 1 {
                    let mirror_dir = cfg.mirror_dir();
                    match tokio::task::spawn_blocking(move || {
                        librarian::observability::count_mirror_rdfs(&mirror_dir)
                    })
                    .await
                    .unwrap_or_else(|e| {
                        tracing::warn!(error = %e, "mirror walk join failed");
                        Some(0) // count as zero, never "no mirror dir"
                    }) {
                        Some(count) => Some(count),
                        None => {
                            if !no_mirror_logged {
                                no_mirror_logged = true;
                                tracing::debug!(
                                    dir = %cfg.mirror_dir().display(),
                                    "no mirror dir on this host; mirror books/ingest gap stay zero"
                                );
                            }
                            Some(0)
                        }
                    }
                } else {
                    None // off-walk tick: keep the last count
                };
                if let Err(e) = refresh_observability(&store, &source, &obs, mirror_books).await {
                    tracing::warn!(error = %e, "observability snapshot refresh failed");
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
            store
                .set_meta(
                    source,
                    "daemon_heartbeat",
                    &rfc3339(time::OffsetDateTime::now_utc()),
                )
                .await?;
            last_heartbeat = time::OffsetDateTime::now_utc();
        }
        match queue.pick_next().await? {
            None => {
                // wake on NOTIFY with a 5 s poll floor
                let _ = tokio::time::timeout(heartbeat_every, listener.recv()).await;
            }
            Some(job) => {
                tracing::info!(job = job.id, kind = %job.kind, "executing job");
                let started = std::time::Instant::now();
                // Trace root for this job: fresh trace id, detached from
                // any ambient context; phase spans in the provider parent
                // onto it. Ended below on every path.
                let root = obs.start_job_root(source, &job.kind, job.id);
                let outcome = execute_job(provider.as_ref(), &job).await;
                // Cycle accounting: ok | failed | interrupted | detached,
                // feeding the in-memory totals and the otel counter.
                let cycle_outcome = match &outcome {
                    Ok(r) if r.aborted_reason.as_deref() == Some("detached") => "detached",
                    Ok(r) => {
                        if interrupt.is_set() || r.aborted_reason.as_deref() == Some("interrupted")
                        {
                            "interrupted"
                        } else {
                            "ok"
                        }
                    }
                    Err(_) if interrupt.is_set() => "interrupted",
                    Err(_) => "failed",
                };
                obs.record_cycle(&job.kind, cycle_outcome, started.elapsed().as_secs_f64());
                // Close the job's trace root on every path: ok, failed,
                // interrupted (the run_id is only known on success).
                root.finish(
                    outcome.as_ref().ok().map(|r| r.run_id),
                    cycle_outcome,
                    started.elapsed().as_secs_f64(),
                );
                match outcome {
                    Ok(report) => {
                        // BEFORE the interrupt check: a detached transfer is
                        // the whole point — the job stays 'running' so the
                        // next daemon adopts it (enqueue_coalesced already
                        // fences new cycles meanwhile), and we break WITHOUT
                        // requeueing.
                        if report.aborted_reason.as_deref() == Some("detached") {
                            tracing::info!(
                                job = job.id,
                                run_id = report.run_id,
                                "job left running (detached); next daemon adopts"
                            );
                            break;
                        }
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
    obs.shutdown().await;
    println!("daemon stopped cleanly");
    Ok(())
}

#[derive(Debug)]
struct JobSummary {
    run_id: i64,
    aborted_reason: Option<String>,
}

async fn execute_job(
    provider: &dyn Provider,
    job: &librarian::queue::JobRow,
) -> anyhow::Result<JobSummary> {
    match job.kind.as_str() {
        "full_cycle" => {
            let opts: CycleOpts =
                serde_json::from_value(job.payload.clone()).context("full_cycle payload")?;
            let r = provider.full_cycle(opts).await?;
            Ok(JobSummary {
                run_id: r.run_id,
                aborted_reason: r.aborted_reason,
            })
        }
        "feed_cycle" => {
            let r = provider.feed_cycle().await?;
            Ok(JobSummary {
                run_id: r.run_id,
                aborted_reason: r.aborted_reason,
            })
        }
        "repair" => {
            let only: Vec<i64> = job
                .payload
                .get("only")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();
            let only_ref = if only.is_empty() {
                None
            } else {
                Some(&only[..])
            };
            let r = provider.repair(only_ref).await?;
            Ok(JobSummary {
                run_id: r.run_id,
                aborted_reason: r.aborted_reason,
            })
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
                time::OffsetDateTime::parse(&last, &time::format_description::well_known::Rfc3339)
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
                store
                    .set_meta(source, "next_full_sync", &rfc3339(advanced))
                    .await?;
            }
        }
    }
    Ok(())
}

async fn refresh_observability(
    store: &Arc<StorePostgres>,
    source: &str,
    obs: &librarian::observability::Observability,
    mirror_books: Option<u64>,
) -> anyhow::Result<()> {
    let books = store.book_status_counts(source).await?;
    let files = store.file_status_counts(source).await?;
    let depths: Vec<(String, i64)> = bookshelf_core::sqlx::query_as(
        "SELECT status, count(*) FROM jobs \
         WHERE status IN ('queued', 'running') AND source = $1 GROUP BY status",
    )
    .bind(source)
    .fetch_all(store.pool())
    .await?;
    let heartbeat_age_s = store
        .get_meta(source, "daemon_heartbeat")
        .await?
        .and_then(|t| {
            time::OffsetDateTime::parse(&t, &time::format_description::well_known::Rfc3339)
                .ok()
                .map(|t| (time::OffsetDateTime::now_utc() - t).whole_seconds())
        });
    // One get_meta: the live cycle's phase string → gauge code. No row
    // (between cycles) or unparsable row = idle.
    let phase = store.get_meta(source, "active_run").await?.and_then(|raw| {
        serde_json::from_str::<bookshelf_core::observability::ActiveRun>(&raw)
            .ok()
            .map(|run| run.phase)
    });
    let snap = obs.snapshot();
    let mut s = snap.lock();
    if let Some(mirror) = mirror_books {
        s.mirror_books = mirror; // off-walk ticks keep the last count
    }
    let db_books: i64 = books.iter().map(|(_, c)| *c).sum();
    s.ingest_gap = librarian::observability::ingest_gap(s.mirror_books, db_books);
    s.active_phase = librarian::observability::phase_code(phase.as_deref());
    s.book_status_counts = books;
    s.file_status_counts = files;
    s.queue_queued = depths
        .iter()
        .find(|(status, _)| status == "queued")
        .map(|(_, c)| *c)
        .unwrap_or(0);
    s.queue_running = depths
        .iter()
        .find(|(status, _)| status == "running")
        .map(|(_, c)| *c)
        .unwrap_or(0);
    s.heartbeat_age_s = heartbeat_age_s;
    Ok(())
}
