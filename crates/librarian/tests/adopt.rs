//! Boot-time adoption against a live Postgres (skipped when
//! BOOKSHELF_DATABASE_URL is unset): a finished detached run adopts to
//! completion, a dead-unreaped one requeues its job. Probe rows and run
//! dirs are cleaned before/after. Like every DB-gated test here, this
//! MUTATES the DB it points at — point BOOKSHELF_DATABASE_URL at a scratch
//! DB (the repair tail of the adopt path reads pending files of the whole
//! source).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bookshelf_core::{EventLog, InterruptFlag, RunIntent, StorePostgres, write_intent};
use librarian::config::{Config, TriageMode};
use librarian::gutenberg_org::GutenbergOrg;
use librarian::gutenberg_org::rdf::Format;
use librarian::provider::{AdoptAction, Provider};
use librarian::supervisor::{
    LauncherKind, Observation, ProcessLauncher, RsyncSpec, SupervisorCfg, SyncLauncher,
};

const SOURCE: &str = "project-gutenberg";
/// Distinctive sync_runs cycle marker — the cleanup deletes exactly this.
const PROBE_CYCLE: &str = "adopt-probe";

/// The tests share one scratch DB and `adopt` looks at the whole source's
/// run root + running job, so they must not interleave.
static SERIAL: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

async fn cleanup(store: &StorePostgres) {
    let _ = bookshelf_core::sqlx::query("DELETE FROM jobs WHERE payload->>'probe' = 'adopt-probe'")
        .execute(store.pool())
        .await;
    let _ = bookshelf_core::sqlx::query("DELETE FROM sync_runs WHERE cycle = $1")
        .bind(PROBE_CYCLE)
        .execute(store.pool())
        .await;
}

async fn setup(tag: &str) -> Option<(Arc<StorePostgres>, Config)> {
    let url = std::env::var("BOOKSHELF_DATABASE_URL").ok()?;
    // The provider constructs an RsyncRunner, which probes rsync on PATH.
    bookshelf_core::RsyncRunner::new(InterruptFlag::new()).ok()?;
    let store = Arc::new(StorePostgres::connect(&url).await.ok()?);
    store.migrate().await.ok()?;
    cleanup(&store).await;
    Some((store, test_config(&url, tag)))
}

fn test_config(url: &str, tag: &str) -> Config {
    let tmp =
        std::env::temp_dir().join(format!("bookshelf-adopt-test-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    Config {
        database_url: url.to_string(),
        library_dir: tmp,
        rsync_host: "gutenberg.pglaf.org".into(),
        rsync_module: "gutenberg-epub".into(),
        download_host: "https://www.gutenberg.org".into(),
        formats: vec![Format::Txt],
        max_parallel_downloads: 1,
        request_interval_ms: 1000,
        timeout_secs: 10,
        max_total_attempts: 1,
        circuit_breaker: 15,
        full_sync_interval_days: 7,
        feed_check_days: 1,
        backfill_on_start: false,
        contact_email: String::new(),
        triage: TriageMode::Rules,
        agent_provider: "zai".into(),
        agent_model: "glm-5.3".into(),
        otlp_endpoint: None,
        supervisor: SupervisorCfg::default(),
        supervisor_launcher: LauncherKind::Process,
        docker_image: None,
    }
}

/// A provider over the temp library dir (run root, events, mirror).
fn provider(store: Arc<StorePostgres>, cfg: Config) -> GutenbergOrg {
    let events = Arc::new(EventLog::open(&cfg.events_path()).unwrap());
    let interrupt = InterruptFlag::new();
    let launcher: Arc<dyn SyncLauncher> = Arc::new(ProcessLauncher);
    GutenbergOrg::new(Arc::new(cfg), store, events, interrupt, launcher).unwrap()
}

/// Scripted launcher for mapping tests: the adopted run always looks Live
/// to `supervise`, regardless of what the run dir holds. `spawn` is never
/// reached on the adoption path (the intent file already exists).
struct AlwaysLive {
    terminates: std::sync::atomic::AtomicUsize,
}

impl AlwaysLive {
    fn new() -> Self {
        Self {
            terminates: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl SyncLauncher for AlwaysLive {
    fn spawn(&self, _spec: &RsyncSpec, _intent: &RunIntent) -> anyhow::Result<()> {
        anyhow::bail!("adoption must not spawn")
    }

    fn observe(&self, _spec: &RsyncSpec) -> anyhow::Result<Observation> {
        Ok(Observation::Live)
    }

    fn terminate(&self, _spec: &RsyncSpec) -> anyhow::Result<()> {
        self.terminates
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn reap(&self, _spec: &RsyncSpec) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Provider with an injected launcher + interrupt (mapping tests).
fn provider_with(
    store: Arc<StorePostgres>,
    cfg: Config,
    interrupt: InterruptFlag,
    launcher: Arc<dyn SyncLauncher>,
) -> GutenbergOrg {
    let events = Arc::new(EventLog::open(&cfg.events_path()).unwrap());
    GutenbergOrg::new(Arc::new(cfg), store, events, interrupt, launcher).unwrap()
}

/// Insert a job row directly as 'running' — no enqueue/pick race with a
/// live daemon, and the probe payload marker makes cleanup surgical.
async fn insert_running_job(store: &StorePostgres) -> i64 {
    bookshelf_core::sqlx::query_scalar(
        "INSERT INTO jobs (source, kind, payload, origin, priority, status, started_at) \
         VALUES ($1, 'full_cycle', '{\"probe\":\"adopt-probe\"}', 'schedule', 0, 'running', now()) \
         RETURNING id",
    )
    .bind(SOURCE)
    .fetch_one(store.pool())
    .await
    .unwrap()
}

/// Craft a run dir with intent + exit + itemize.log.
fn craft_run_dir(cfg: &Config, run_id: i64, with_exit: bool) -> PathBuf {
    let run_dir = cfg.run_root().join(format!("{SOURCE}-r{run_id}"));
    std::fs::create_dir_all(&run_dir).unwrap();
    write_intent(
        &run_dir,
        &RunIntent {
            attempt: 1,
            host: "gutenberg.pglaf.org".into(),
            started_at: "2026-08-29T00:00:00Z".into(),
        },
    )
    .unwrap();
    if with_exit {
        std::fs::write(run_dir.join("exit"), "0\n").unwrap();
    }
    std::fs::write(
        run_dir.join("itemize.log"),
        ">f+++++++++|51564/pg51564-images.epub|24846294\n\
         *deleting|1342/pg1342-h.zip|0\n",
    )
    .unwrap();
    run_dir
}

#[tokio::test]
async fn finished_detached_run_adopts_to_completion() {
    let _guard = SERIAL.lock();
    let Some((store, cfg)) = setup("done").await else {
        eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
        return;
    };
    let p = provider(store.clone(), cfg.clone());

    // A running job + an open run + a run dir whose rsync exited 0.
    let job_id = insert_running_job(&store).await;
    let run_id = store.start_run(SOURCE, PROBE_CYCLE).await.unwrap();
    let run_dir = craft_run_dir(&cfg, run_id, true);

    let report = p.adopt().await.unwrap();
    assert_eq!(report.job_id, Some(job_id));
    match report.action {
        AdoptAction::AdoptedAndCompleted { run_id: adopted } => {
            assert_eq!(adopted, run_id);
        }
        other => panic!("expected AdoptedAndCompleted, got {other:?}"),
    }

    // Job done, bound to the run (the daemon's adopt mapping does the
    // queue.complete; here we replicate its post-condition).
    queue_complete(&store, job_id, Some(run_id)).await;
    let (status, job_run): (String, Option<i64>) =
        bookshelf_core::sqlx::query_as("SELECT status, run_id FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(status, "done");
    assert_eq!(job_run, Some(run_id));

    // Run finalized with the itemize tally and no abort.
    let (finished, files, bytes, aborted): (bool, i32, i64, Option<String>) =
        bookshelf_core::sqlx::query_as(
            "SELECT finished_at IS NOT NULL, transferred_files, transferred_bytes, aborted_reason \
             FROM sync_runs WHERE id = $1",
        )
        .bind(run_id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert!(finished);
    assert_eq!((files, bytes), (1, 24846294));
    assert_eq!(aborted, None);

    // Artifacts reaped, adoption event logged.
    assert!(!run_dir.exists(), "adopted run dir must be cleared");
    let events = std::fs::read_to_string(cfg.events_path()).unwrap();
    assert!(events.contains("\"kind\":\"cycle.adopted\""));

    cleanup(&store).await;
    let _ = std::fs::remove_dir_all(cfg.library_dir);
}

#[tokio::test]
async fn dead_unreaped_run_requeues_job() {
    let _guard = SERIAL.lock();
    let Some((store, cfg)) = setup("requeue").await else {
        eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
        return;
    };
    let p = provider(store.clone(), cfg.clone());

    let job_id = insert_running_job(&store).await;
    let run_id = store.start_run(SOURCE, PROBE_CYCLE).await.unwrap();
    // Intent only: no pid, no exit — the OOM/SIGKILL signature.
    craft_run_dir(&cfg, run_id, false);

    let report = p.adopt().await.unwrap();
    assert_eq!(report.job_id, Some(job_id));
    assert!(matches!(report.action, AdoptAction::Requeued));

    // Job back to queued (attempts+1), abandoned run closed as interrupted.
    // (The daemon's Requeued mapping does the requeue; replicate it here.)
    queue_requeue(&store, job_id).await;
    let (status, attempts): (String, i32) =
        bookshelf_core::sqlx::query_as("SELECT status, attempts FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(status, "queued");
    assert_eq!(attempts, 1);
    let (finished, aborted): (bool, Option<String>) = bookshelf_core::sqlx::query_as(
        "SELECT finished_at IS NOT NULL, aborted_reason FROM sync_runs WHERE id = $1",
    )
    .bind(run_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert!(finished);
    assert_eq!(aborted.as_deref(), Some("interrupted"));

    cleanup(&store).await;
    let _ = std::fs::remove_dir_all(cfg.library_dir);
}

#[tokio::test]
async fn stop_during_adopt_leaves_run_alive() {
    let _guard = SERIAL.lock();
    let Some((store, mut cfg)) = setup("left").await else {
        eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
        return;
    };
    // Fast supervise polling so the shutdown race resolves quickly.
    cfg.supervisor.poll = Duration::from_millis(100);
    let interrupt = InterruptFlag::new();
    let live = Arc::new(AlwaysLive::new());
    let launcher: Arc<dyn SyncLauncher> = live.clone();
    let p = provider_with(store.clone(), cfg.clone(), interrupt.clone(), launcher);

    let job_id = insert_running_job(&store).await;
    let run_id = store.start_run(SOURCE, PROBE_CYCLE).await.unwrap();
    // The exit record passes adopt's pre-check (a finalized artifact); the
    // AlwaysLive stub then keeps `supervise` watching a "live" run.
    let run_dir = craft_run_dir(&cfg, run_id, true);

    // SIGTERM arrives mid-watch, on_daemon_stop defaults to "detach".
    let stop = interrupt.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        stop.set();
    });

    let report = p.adopt().await.unwrap();
    assert_eq!(report.job_id, Some(job_id));
    assert!(
        matches!(report.action, AdoptAction::LeftDetached),
        "expected LeftDetached, got {:?}",
        report.action
    );
    assert_eq!(
        live.terminates.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "detach must never terminate the surviving transfer"
    );

    // Job untouched: still 'running', no attempts burned, no run_id set.
    let (status, attempts, job_run): (String, i32, Option<i64>) =
        bookshelf_core::sqlx::query_as("SELECT status, attempts, run_id FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(status, "running");
    assert_eq!(attempts, 0);
    assert_eq!(job_run, None);

    // Run row still open, artifacts intact — the next daemon's adopt
    // binds job + artifacts and resumes.
    let (finished, aborted): (bool, Option<String>) = bookshelf_core::sqlx::query_as(
        "SELECT finished_at IS NULL, aborted_reason FROM sync_runs WHERE id = $1",
    )
    .bind(run_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert!(finished, "run row must stay open for re-adoption");
    assert_eq!(aborted, None);
    assert!(run_dir.exists(), "run artifacts must survive");

    cleanup(&store).await;
    let _ = std::fs::remove_dir_all(cfg.library_dir);
}

#[tokio::test]
async fn empty_run_root_is_nothing() {
    let _guard = SERIAL.lock();
    let Some((store, cfg)) = setup("nothing").await else {
        eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
        return;
    };
    let p = provider(store.clone(), cfg.clone());
    let report = p.adopt().await.unwrap();
    assert_eq!(report.job_id, None);
    assert!(matches!(report.action, AdoptAction::Nothing));
    let _ = std::fs::remove_dir_all(cfg.library_dir);
}

/// The daemon's AdoptedAndCompleted mapping, replicated for the assertion.
async fn queue_complete(store: &StorePostgres, job_id: i64, run_id: Option<i64>) {
    bookshelf_core::sqlx::query(
        "UPDATE jobs SET status = 'done', run_id = $2, finished_at = now() WHERE id = $1",
    )
    .bind(job_id)
    .bind(run_id)
    .execute(store.pool())
    .await
    .unwrap();
}
/// The daemon's Requeued mapping, replicated for the assertion.
async fn queue_requeue(store: &StorePostgres, job_id: i64) {
    bookshelf_core::sqlx::query(
        "UPDATE jobs SET attempts = attempts + 1, status = 'queued' WHERE id = $1",
    )
    .bind(job_id)
    .execute(store.pool())
    .await
    .unwrap();
}
