//! Detached-run supervision: the durable replacement for the in-memory
//! rsync retry ladder. A transfer is spawned through a [`SyncLauncher`]
//! (today [`ProcessLauncher`] over bookshelf-core's detached-run
//! primitives) and watched by one state machine ([`supervise`]) used BOTH
//! for fresh runs and for boot-time adoption: exit file first, liveness
//! second, progress stall last.
//!
//! Durability: all attempt state lives in the run dir — `intent.json` /
//! `pid` / `exit` / `stderr.log` / `itemize.log` (written by rsync via the
//! spec's `--log-file`) plus this module's `args.json` sidecar (the exact
//! transfer args, which adoption cannot reconstruct faithfully). Nothing
//! lives in daemon memory, so a restarted daemon re-enters the same ladder
//! at the recorded attempt via `Provider::adopt`.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;

use bookshelf_core::{
    ExitClass, InterruptFlag, LiveState, RunIntent, StorePostgres, classify_exit, clear_run,
    itemize_delta, read_exit, read_intent, read_pgid, run_is_live, spawn_detached, terminate_group,
};

use crate::gutenberg_org::mirror::host_pair;

/// Graceful-stop policy for a running detached transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnStop {
    /// Leave the rsync running; the job stays `running` and the next
    /// daemon's `adopt` resumes it.
    Detach,
    /// Terminate the transfer's process group before stopping.
    Kill,
}

/// Which launcher spawns transfers. `Docker` is a later workstream; it
/// fails fast at provider build time for now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LauncherKind {
    Process,
    Docker,
}

/// Supervision knobs (from the `[supervisor]` config table).
#[derive(Debug, Clone)]
pub struct SupervisorCfg {
    pub on_daemon_stop: OnStop,
    pub poll: Duration,
    /// No itemize growth for this long ⇒ the attempt is terminated and the
    /// retry ladder consumes it. Default 30 min — deliberately far above
    /// rsync's own `--timeout=600`, so a wedged CONNECTION dies by rsync's
    /// hand first and only a connected-but-silent transfer reaches this.
    pub progress_stall: Duration,
    /// Transfer attempts per run (today's `vec![P,F,P,F]` shape).
    pub max_attempts: u32,
}

impl Default for SupervisorCfg {
    fn default() -> Self {
        Self {
            on_daemon_stop: OnStop::Detach,
            poll: Duration::from_secs(10),
            progress_stall: Duration::from_secs(30 * 60),
            max_attempts: 4,
        }
    }
}

/// One transfer to supervise, fully described by durable artifacts.
#[derive(Debug, Clone)]
pub struct RsyncSpec {
    pub source: String,
    pub run_id: i64,
    pub args: Vec<String>,
    pub run_dir: PathBuf,
}

/// Launcher-polled state of the run dir. The exit file beats liveness
/// (the wrapper writes `exit` right before dying); `Absent` = neither pid
/// nor exit record at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observation {
    Live,
    Exited(i32),
    DeadUnreaped,
    Absent,
}

/// Spawns and observes transfers. One impl today ([`ProcessLauncher`]);
/// the docker launcher lands in a later workstream behind the same trait.
pub trait SyncLauncher: Send + Sync {
    fn spawn(&self, spec: &RsyncSpec, intent: &RunIntent) -> anyhow::Result<()>;
    fn observe(&self, spec: &RsyncSpec) -> anyhow::Result<Observation>;
    fn terminate(&self, spec: &RsyncSpec) -> anyhow::Result<()>;
    fn reap(&self, spec: &RsyncSpec) -> anyhow::Result<()>;
}

/// The real launcher over core's detached-run primitives. `spawn_detached`
/// persists the intent itself; liveness is a `/proc` starttime
/// fingerprint, so runs stay observable across daemon restarts (and PID
/// reuse).
pub struct ProcessLauncher;

impl SyncLauncher for ProcessLauncher {
    fn spawn(&self, spec: &RsyncSpec, intent: &RunIntent) -> anyhow::Result<()> {
        spawn_detached(&spec.args, &spec.run_dir, intent)
    }

    fn observe(&self, spec: &RsyncSpec) -> anyhow::Result<Observation> {
        if let Some(code) = read_exit(&spec.run_dir)? {
            return Ok(Observation::Exited(code));
        }
        Ok(match run_is_live(&spec.run_dir) {
            LiveState::Live => Observation::Live,
            LiveState::Dead => Observation::DeadUnreaped,
            LiveState::Gone => Observation::Absent,
        })
    }

    fn terminate(&self, spec: &RsyncSpec) -> anyhow::Result<()> {
        if let Some(pgid) = read_pgid(&spec.run_dir)? {
            terminate_group(pgid); // TERM → 10 s → KILL, group-wide
        }
        Ok(())
    }

    fn reap(&self, spec: &RsyncSpec) -> anyhow::Result<()> {
        clear_run(&spec.run_dir)
    }
}

/// How a supervised transfer ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SuperviseOutcome {
    Completed {
        code: i32,
    },
    Failed {
        reason: String,
        code: Option<i32>,
    },
    /// Graceful stop with Kill (child terminated) — or the interrupt hit
    /// between attempts, where nothing was running to kill.
    Interrupted,
    /// Graceful stop with Detach — child untouched, job stays `running`.
    LeftRunning,
}

/// Transport ladder delay AFTER attempt N failed retriably: 1 → 300 s,
/// 2 → 600 s, 3 → 3600 s, 4 → None (exhausted). Pure.
pub fn ladder_delay(attempt: u32) -> Option<Duration> {
    match attempt {
        1 => Some(Duration::from_secs(300)),
        2 => Some(Duration::from_secs(600)),
        3 => Some(Duration::from_secs(3600)),
        _ => None,
    }
}

/// How often `supervise` mirrors itemize deltas into `sync_runs`.
const PROGRESS_FLUSH_EVERY: Duration = Duration::from_secs(30);

/// Resolves once the flag is set. `InterruptFlag` has no async wait, so
/// poll coarsely — it is a single `AtomicBool`.
async fn wait_flag(flag: &InterruptFlag) {
    loop {
        if flag.is_set() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Ladder delay as `supervise` actually sleeps it. Unit tests must not
/// wait real minutes; production takes [`ladder_delay`] verbatim.
fn sleep_delay(delay: Option<Duration>) -> Option<Duration> {
    if cfg!(test) {
        delay.map(|_| Duration::from_millis(2))
    } else {
        delay
    }
}

/// How long a continuous `Absent` from our own spawn is tolerated before
/// the attempt counts as a retryable failure. 60 s in production (a fresh
/// `sh` writes its pid witness within milliseconds; anything longer means
/// the wrapper is never coming up); shrunk under test like [`sleep_delay`].
fn absent_deadline() -> Duration {
    if cfg!(test) {
        Duration::from_millis(20)
    } else {
        Duration::from_secs(60)
    }
}

/// The rsync URL host baked into transfer args (`host::module/`, or the
/// per-id `host::module/./id/` variants). Flags never carry `::`.
fn host_from_args(args: &[String]) -> anyhow::Result<String> {
    args.iter()
        .find(|a| !a.starts_with("--") && a.contains("::"))
        .and_then(|a| a.split_once("::"))
        .map(|(host, _)| host.to_string())
        .ok_or_else(|| anyhow::anyhow!("rsync args carry no host URL"))
}

/// Re-host transfer args for the next ladder attempt: every non-flag arg
/// with a `host::` URL is rewritten (a full spec has one, a targeted spec
/// one per id).
fn args_with_host(args: &[String], host: &str) -> Vec<String> {
    args.iter()
        .map(|a| match a.split_once("::") {
            Some((_, rest)) if !a.starts_with("--") => format!("{host}::{rest}"),
            _ => a.clone(),
        })
        .collect()
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// Durable copy of the transfer args (`<run_dir>/args.json`, tmp+rename).
/// `spawn_detached` records intent/host/attempt, but the ARGS themselves
/// are the one thing adoption cannot reconstruct faithfully (full vs
/// targeted vs per-id escalation) — so the supervisor rewrites this on
/// every spawn and `adopt` rebuilds the exact spec from it.
fn write_args_json(run_dir: &Path, args: &[String]) -> anyhow::Result<()> {
    std::fs::create_dir_all(run_dir)
        .with_context(|| format!("creating run dir {}", run_dir.display()))?;
    let json = serde_json::to_vec(args).context("serializing rsync args")?;
    let tmp = run_dir.join("args.json.tmp");
    std::fs::write(&tmp, json).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, run_dir.join("args.json"))
        .with_context(|| format!("renaming args into {}", run_dir.display()))?;
    Ok(())
}

/// The recorded transfer args; `Ok(None)` when absent (dir never spawned
/// by this supervisor).
pub(crate) fn read_args_json(run_dir: &Path) -> anyhow::Result<Option<Vec<String>>> {
    let raw = match std::fs::read_to_string(run_dir.join("args.json")) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).context("reading run args"),
    };
    serde_json::from_str(&raw)
        .map(Some)
        .with_context(|| format!("malformed args.json in {}", run_dir.display()))
}

/// Mutable ladder state for one `supervise` call.
struct Ladder {
    attempt: u32,
    host: String,
    /// The primary host once seen — odd attempts run it by parity.
    primary: Option<String>,
    /// True while the current attempt was spawned by THIS supervise call:
    /// an `Absent` observation right after our own spawn is the pid-write
    /// race, not a death.
    spawned_here: bool,
    /// Stall clock: restarted at every spawn and on every itemize growth.
    stall_since: Instant,
}

/// What one retryable failure consumed.
enum Step {
    /// Attempt+1 spawned — keep watching.
    Respawned,
    /// Ladder budget spent.
    Exhausted,
    /// The daemon is stopping; carry the outcome out.
    Stop(SuperviseOutcome),
}

impl Ladder {
    /// `host` is the current attempt's host; attempt numbering and host
    /// parity come from the recorded intent (adoption) or the fresh spawn.
    fn new(attempt: u32, host: String) -> Self {
        Self {
            primary: (attempt % 2 == 1).then(|| host.clone()),
            attempt,
            host,
            spawned_here: false,
            stall_since: Instant::now(),
        }
    }

    /// Consume one retryable failure: sleep the ladder delay
    /// (interruptible), then respawn attempt+1 on the parity host. All
    /// state is durable afterwards — the spawn rewrites `intent.json` and
    /// `args.json`, so a daemon death mid-ladder resumes at this same
    /// attempt via adoption.
    async fn step(
        &mut self,
        launcher: &dyn SyncLauncher,
        spec: &RsyncSpec,
        cfg: &SupervisorCfg,
        interrupt: &InterruptFlag,
    ) -> anyhow::Result<Step> {
        if self.attempt >= cfg.max_attempts {
            return Ok(Step::Exhausted);
        }
        match sleep_delay(ladder_delay(self.attempt)) {
            None => return Ok(Step::Exhausted),
            Some(delay) => {
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = wait_flag(interrupt) => {
                        // Nothing is running between attempts — Detach and
                        // Kill converge on "stop without touching a child".
                        return Ok(Step::Stop(match cfg.on_daemon_stop {
                            OnStop::Detach => SuperviseOutcome::LeftRunning,
                            OnStop::Kill => SuperviseOutcome::Interrupted,
                        }));
                    }
                }
            }
        }
        self.attempt += 1;
        // Host parity: odd attempt = primary, even = alternate. Exact for
        // the shipped pglaf ⇄ ibiblio pair (host_pair is an involution
        // there); a custom primary keeps host rotation but may drift from
        // strict parity after an adoption that started on an even attempt.
        self.host = match (&self.primary, self.attempt % 2) {
            (Some(p), 1) => p.clone(),
            (Some(p), _) => host_pair(p).1.to_string(),
            (None, _) => host_pair(&self.host).1.to_string(),
        };
        let next = RsyncSpec {
            source: spec.source.clone(),
            run_id: spec.run_id,
            args: args_with_host(&spec.args, &self.host),
            run_dir: spec.run_dir.clone(),
        };
        // The dead/terminated attempt may have left a stale `exit` record —
        // the next attempt must be judged on its own merits. (Safe: any
        // terminate happened before this point, and the wrapper writes its
        // exit file exactly once per attempt.)
        let _ = std::fs::remove_file(spec.run_dir.join("exit"));
        write_args_json(&spec.run_dir, &next.args)?;
        launcher.spawn(
            &next,
            &RunIntent {
                attempt: self.attempt,
                host: self.host.clone(),
                started_at: now_rfc3339(),
            },
        )?;
        self.spawned_here = true;
        self.stall_since = Instant::now();
        Ok(Step::Respawned)
    }
}

/// Run one transfer to an end — or to a detach. Single state machine for
/// fresh runs and adoption: poll order is exit file FIRST (the wrapper's
/// last word beats a `/proc` race), liveness second, progress stall last.
///
/// The store interaction is progress mirroring only (observability, never
/// control): `update_run_progress` failures are logged and swallowed.
pub async fn supervise(
    launcher: &dyn SyncLauncher,
    spec: &RsyncSpec,
    cfg: &SupervisorCfg,
    interrupt: &InterruptFlag,
    store: &std::sync::Arc<StorePostgres>,
) -> anyhow::Result<SuperviseOutcome> {
    // Adoption re-enters at the recorded attempt/host; a fresh run starts
    // the ladder at attempt 1 on the spec's own host.
    let existing = read_intent(&spec.run_dir)?;
    let (attempt, host) = match &existing {
        Some(intent) => (intent.attempt, intent.host.clone()),
        None => {
            let host = host_from_args(&spec.args)?;
            write_args_json(&spec.run_dir, &spec.args)?;
            launcher.spawn(
                spec,
                &RunIntent {
                    attempt: 1,
                    host: host.clone(),
                    started_at: now_rfc3339(),
                },
            )?;
            (1, host)
        }
    };
    let mut ladder = Ladder::new(attempt, host);
    ladder.spawned_here = existing.is_none();

    // Cumulative counters over ALL attempts: rsync appends to the log, so
    // one pass over the whole log at start (then incremental reads from
    // its end) IS the run's true tally — across respawns AND daemon
    // restarts alike. Seeding from the sync_runs row instead would
    // double-count on adoption: the row already holds the previous
    // daemon's flush, and the log is re-read from zero.
    let mut itemize_offset: u64 = 0;
    let (seed_files, seed_bytes) = itemize_delta(&spec.run_dir, &mut itemize_offset)?;
    let (mut cum_files, mut cum_bytes) = (seed_files as i64, seed_bytes as i64);
    let mut last_flush: Option<Instant> = None;
    // Continuous-Absent clock: how long our own spawn has gone without a
    // pid witness. Any non-Absent observation resets it.
    let mut absent_since: Option<Instant> = None;

    loop {
        if interrupt.is_set() {
            return Ok(match cfg.on_daemon_stop {
                OnStop::Detach => SuperviseOutcome::LeftRunning, // never touch the child
                OnStop::Kill => {
                    launcher.terminate(spec)?;
                    SuperviseOutcome::Interrupted
                }
            });
        }

        let observation = launcher.observe(spec)?;
        if observation != Observation::Absent {
            absent_since = None;
        }
        match observation {
            Observation::Exited(code) => match classify_exit(code) {
                ExitClass::Ok => return Ok(SuperviseOutcome::Completed { code }),
                ExitClass::Partial => {
                    // 23/24: files vanished upstream — same as today, keep
                    // what arrived.
                    tracing::warn!(code, "rsync partial transfer; keeping what arrived");
                    return Ok(SuperviseOutcome::Completed { code });
                }
                ExitClass::Fatal => {
                    tracing::error!(code, "rsync fatal error — aborting run");
                    return Ok(SuperviseOutcome::Failed {
                        reason: format!("rsync fatal exit {code}"),
                        code: Some(code),
                    });
                }
                ExitClass::Retryable => {
                    tracing::warn!(code, attempt = ladder.attempt, "rsync retryable failure");
                    match ladder.step(launcher, spec, cfg, interrupt).await? {
                        Step::Respawned => {}
                        Step::Exhausted => {
                            return Ok(SuperviseOutcome::Failed {
                                reason: "rsync ladder exhausted".into(),
                                code: Some(code),
                            });
                        }
                        Step::Stop(outcome) => return Ok(outcome),
                    }
                }
            },
            Observation::Live => {
                let (files, bytes) = itemize_delta(&spec.run_dir, &mut itemize_offset)?;
                if files > 0 || bytes > 0 {
                    cum_files += files as i64;
                    cum_bytes += bytes as i64;
                    ladder.stall_since = Instant::now();
                    if last_flush.map_or(true, |t| t.elapsed() >= PROGRESS_FLUSH_EVERY) {
                        if let Err(e) = store
                            .update_run_progress(spec.run_id, cum_files, cum_bytes)
                            .await
                        {
                            tracing::debug!(error = %e, "run progress write failed");
                        }
                        last_flush = Some(Instant::now());
                    }
                }
                if ladder.stall_since.elapsed() >= cfg.progress_stall {
                    tracing::warn!(
                        attempt = ladder.attempt,
                        stalled_for = ?cfg.progress_stall,
                        "no itemize progress — terminating attempt for the ladder"
                    );
                    launcher.terminate(spec)?;
                    // A stalled attempt is one retryable ladder step.
                    match ladder.step(launcher, spec, cfg, interrupt).await? {
                        Step::Respawned => {}
                        Step::Exhausted => {
                            return Ok(SuperviseOutcome::Failed {
                                reason: "rsync ladder exhausted".into(),
                                code: None,
                            });
                        }
                        Step::Stop(outcome) => return Ok(outcome),
                    }
                }
            }
            Observation::DeadUnreaped => {
                // Wrapper died without writing `exit` — OOM/SIGKILL class,
                // a retryable ladder step like any other transport error.
                tracing::warn!(
                    attempt = ladder.attempt,
                    "detached run died unreaped (OOM/SIGKILL?)"
                );
                match ladder.step(launcher, spec, cfg, interrupt).await? {
                    Step::Respawned => {}
                    Step::Exhausted => {
                        return Ok(SuperviseOutcome::Failed {
                            reason: "rsync ladder exhausted".into(),
                            code: None,
                        });
                    }
                    Step::Stop(outcome) => return Ok(outcome),
                }
            }
            Observation::Absent => {
                if ladder.spawned_here {
                    // Usually the spawn race: the wrapper has not written
                    // its pid yet. But if Absent persists past the deadline
                    // the wrapper is never coming up (exec failure, missing
                    // sh) — recover exactly like DeadUnreaped instead of
                    // polling forever.
                    let since = absent_since.get_or_insert_with(Instant::now);
                    if since.elapsed() < absent_deadline() {
                        // Give it another poll.
                    } else {
                        tracing::warn!(
                            attempt = ladder.attempt,
                            absent_for = ?since.elapsed(),
                            "detached wrapper never wrote its pid — retryable failure"
                        );
                        absent_since = None;
                        match ladder.step(launcher, spec, cfg, interrupt).await? {
                            Step::Respawned => {}
                            Step::Exhausted => {
                                return Ok(SuperviseOutcome::Failed {
                                    reason: "rsync ladder exhausted".into(),
                                    code: None,
                                });
                            }
                            Step::Stop(outcome) => return Ok(outcome),
                        }
                    }
                } else {
                    // Adopted an intent-only dir: no process ever
                    // materialized — same recovery as DeadUnreaped.
                    tracing::warn!(
                        attempt = ladder.attempt,
                        "adopted run has intent but no process record"
                    );
                    match ladder.step(launcher, spec, cfg, interrupt).await? {
                        Step::Respawned => {}
                        Step::Exhausted => {
                            return Ok(SuperviseOutcome::Failed {
                                reason: "rsync ladder exhausted".into(),
                                code: None,
                            });
                        }
                        Step::Stop(outcome) => return Ok(outcome),
                    }
                }
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(cfg.poll) => {}
            _ = wait_flag(interrupt) => {} // the loop head reacts
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    /// `supervise` takes the store by its frozen signature, and
    /// `StorePostgres` has exactly one constructor — an eager connect.
    /// The scripted-ladder tests therefore self-skip without
    /// BOOKSHELF_DATABASE_URL like the DB-gated suite; supervise treats
    /// every store interaction as best-effort progress mirroring, so the
    /// ladder logic itself never touches the DB. Everything pure is
    /// tested unconditionally further down.
    async fn test_store() -> Option<Arc<StorePostgres>> {
        let url = std::env::var("BOOKSHELF_DATABASE_URL").ok()?;
        StorePostgres::connect(&url).await.ok().map(Arc::new)
    }

    /// Scripted launcher: `observe` pops the script, returning
    /// `default_observation` once it is exhausted; every spawn and
    /// terminate is recorded for assertions.
    struct FakeLauncher {
        script: parking_lot::Mutex<Vec<Observation>>,
        default_observation: Observation,
        spawns: parking_lot::Mutex<Vec<(u32, String)>>,
        terminates: AtomicUsize,
    }

    impl FakeLauncher {
        fn scripted(observations: Vec<Observation>) -> Self {
            Self {
                script: parking_lot::Mutex::new(observations),
                default_observation: Observation::Live,
                spawns: parking_lot::Mutex::new(Vec::new()),
                terminates: AtomicUsize::new(0),
            }
        }

        fn spawn_list(&self) -> Vec<(u32, String)> {
            self.spawns.lock().clone()
        }

        fn terminate_count(&self) -> usize {
            self.terminates.load(AtomicOrdering::SeqCst)
        }

        /// Append an observation from another task (scripted timing).
        fn push(&self, observation: Observation) {
            self.script.lock().push(observation);
        }
    }

    impl SyncLauncher for FakeLauncher {
        fn spawn(&self, _spec: &RsyncSpec, intent: &RunIntent) -> anyhow::Result<()> {
            self.spawns
                .lock()
                .push((intent.attempt, intent.host.clone()));
            Ok(())
        }

        fn observe(&self, _spec: &RsyncSpec) -> anyhow::Result<Observation> {
            let mut script = self.script.lock();
            if script.is_empty() {
                return Ok(self.default_observation);
            }
            Ok(script.remove(0))
        }

        fn terminate(&self, _spec: &RsyncSpec) -> anyhow::Result<()> {
            self.terminates.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }

        fn reap(&self, _spec: &RsyncSpec) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn test_cfg(stall_ms: u64, max_attempts: u32) -> SupervisorCfg {
        SupervisorCfg {
            poll: Duration::from_millis(1),
            progress_stall: Duration::from_millis(stall_ms),
            max_attempts,
            ..Default::default()
        }
    }

    /// A spec whose args carry the pglaf primary, in a fresh temp run dir.
    fn spec(tag: &str) -> RsyncSpec {
        let run_dir = std::env::temp_dir().join(format!(
            "bookshelf-supervisor-test-{}-{tag}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&run_dir);
        std::fs::create_dir_all(&run_dir).unwrap();
        RsyncSpec {
            source: "project-gutenberg".into(),
            run_id: 0,
            args: vec![
                "-a".into(),
                "--timeout=600".into(),
                "gutenberg.pglaf.org::gutenberg-epub/".into(),
                "/tmp/bookshelf-supervisor-dest/".into(),
            ],
            run_dir,
        }
    }

    fn rm_dir(spec: &RsyncSpec) {
        let _ = std::fs::remove_dir_all(&spec.run_dir);
    }

    #[tokio::test]
    async fn instant_success() {
        let Some(store) = test_store().await else {
            eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
            return;
        };
        let sp = spec("instant");
        let fake = FakeLauncher::scripted(vec![Observation::Exited(0)]);
        let outcome = supervise(
            &fake,
            &sp,
            &test_cfg(1_000, 4),
            &InterruptFlag::new(),
            &store,
        )
        .await
        .unwrap();
        assert_eq!(outcome, SuperviseOutcome::Completed { code: 0 });
        assert_eq!(fake.spawn_list(), vec![(1, "gutenberg.pglaf.org".into())]);
        assert_eq!(fake.terminate_count(), 0);
        rm_dir(&sp);
    }

    #[tokio::test]
    async fn retryable_then_success_after_respawn() {
        let Some(store) = test_store().await else {
            eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
            return;
        };
        let sp = spec("retry");
        let fake = FakeLauncher::scripted(vec![
            Observation::Exited(5),
            Observation::Live,
            Observation::Exited(0),
        ]);
        let outcome = supervise(
            &fake,
            &sp,
            &test_cfg(1_000, 4),
            &InterruptFlag::new(),
            &store,
        )
        .await
        .unwrap();
        assert_eq!(outcome, SuperviseOutcome::Completed { code: 0 });
        // Attempt parity: odd = primary (pglaf), even = alternate (ibiblio).
        assert_eq!(
            fake.spawn_list(),
            vec![
                (1, "gutenberg.pglaf.org".into()),
                (2, "rsync.ibiblio.org".into()),
            ]
        );
        assert_eq!(fake.terminate_count(), 0);
        rm_dir(&sp);
    }

    #[tokio::test]
    async fn ladder_exhaustion_fails() {
        let Some(store) = test_store().await else {
            eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
            return;
        };
        // Budget exhausted by max_attempts...
        let sp = spec("exhausted-max");
        let fake = FakeLauncher::scripted(vec![Observation::Exited(5), Observation::Exited(5)]);
        let outcome = supervise(
            &fake,
            &sp,
            &test_cfg(1_000, 2),
            &InterruptFlag::new(),
            &store,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            SuperviseOutcome::Failed {
                reason: "rsync ladder exhausted".into(),
                code: Some(5),
            }
        );
        assert_eq!(fake.spawn_list().len(), 2);
        rm_dir(&sp);

        // ...and by the pure ladder itself (attempt 4 has no delay).
        let sp = spec("exhausted-ladder");
        let fake = FakeLauncher::scripted(vec![
            Observation::Exited(5),
            Observation::Exited(5),
            Observation::Exited(5),
            Observation::Exited(5),
        ]);
        let outcome = supervise(
            &fake,
            &sp,
            &test_cfg(1_000, 5),
            &InterruptFlag::new(),
            &store,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            SuperviseOutcome::Failed {
                reason: "rsync ladder exhausted".into(),
                code: Some(5),
            }
        );
        assert_eq!(fake.spawn_list().len(), 4);
        rm_dir(&sp);
    }

    #[tokio::test]
    async fn interrupt_with_detach_leaves_child_running() {
        let Some(store) = test_store().await else {
            eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
            return;
        };
        let sp = spec("detach");
        let fake = FakeLauncher::scripted(vec![]);
        let flag = InterruptFlag::new();
        let stop = flag.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            stop.set();
        });
        let outcome = supervise(&fake, &sp, &test_cfg(1_000, 4), &flag, &store)
            .await
            .unwrap();
        assert_eq!(outcome, SuperviseOutcome::LeftRunning);
        // Detach never touches the child.
        assert_eq!(fake.terminate_count(), 0);
        rm_dir(&sp);
    }

    #[tokio::test]
    async fn interrupt_with_kill_terminates() {
        let Some(store) = test_store().await else {
            eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
            return;
        };
        let sp = spec("kill");
        let fake = FakeLauncher::scripted(vec![]);
        let mut cfg = test_cfg(1_000, 4);
        cfg.on_daemon_stop = OnStop::Kill;
        let flag = InterruptFlag::new();
        let stop = flag.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            stop.set();
        });
        let outcome = supervise(&fake, &sp, &cfg, &flag, &store).await.unwrap();
        assert_eq!(outcome, SuperviseOutcome::Interrupted);
        assert_eq!(fake.terminate_count(), 1);
        rm_dir(&sp);
    }

    #[tokio::test]
    async fn stall_terminates_and_consumes_a_ladder_step() {
        let Some(store) = test_store().await else {
            eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
            return;
        };
        let sp = spec("stall");
        // Attempt 1 stays Live past the stall window (no itemize.log at
        // all — the chosen stall clock runs from attempt start). A timer
        // task appends the clean exit at 150 ms, well inside attempt 2's
        // fresh window (≈50 ms of Live after the respawn << 100 ms), so
        // exactly ONE stall step is consumed regardless of poll speed.
        let fake = Arc::new(FakeLauncher::scripted(vec![]));
        let pusher = fake.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            pusher.push(Observation::Exited(0));
        });
        let outcome = supervise(
            fake.as_ref(),
            &sp,
            &test_cfg(100, 4),
            &InterruptFlag::new(),
            &store,
        )
        .await
        .unwrap();
        assert_eq!(outcome, SuperviseOutcome::Completed { code: 0 });
        assert_eq!(
            fake.terminate_count(),
            1,
            "stall must terminate the attempt"
        );
        assert_eq!(
            fake.spawn_list(),
            vec![
                (1, "gutenberg.pglaf.org".into()),
                (2, "rsync.ibiblio.org".into()),
            ]
        );
        rm_dir(&sp);
    }

    #[tokio::test]
    async fn persistent_absent_consumes_ladder_then_fails() {
        let Some(store) = test_store().await else {
            eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
            return;
        };
        let sp = spec("absent");
        // The wrapper never writes its pid: Absent from the first poll
        // onward. Each attempt burns the (test-shrunk) absent deadline,
        // consumes a ladder step, and respawns — until the budget is spent.
        let mut fake = FakeLauncher::scripted(vec![]);
        fake.default_observation = Observation::Absent;
        let outcome = supervise(
            &fake,
            &sp,
            &test_cfg(1_000, 3),
            &InterruptFlag::new(),
            &store,
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            SuperviseOutcome::Failed {
                reason: "rsync ladder exhausted".into(),
                code: None,
            }
        );
        // Attempts 1..3 spawned, each Absent past the deadline; nothing to
        // terminate — there was never a process.
        assert_eq!(fake.spawn_list().len(), 3);
        assert_eq!(fake.terminate_count(), 0);
        rm_dir(&sp);
    }

    // -- pure helpers (always run, no DB) --------------------------------

    #[test]
    fn ladder_delay_mapping() {
        assert_eq!(ladder_delay(1), Some(Duration::from_secs(300)));
        assert_eq!(ladder_delay(2), Some(Duration::from_secs(600)));
        assert_eq!(ladder_delay(3), Some(Duration::from_secs(3600)));
        assert_eq!(ladder_delay(4), None);
        assert_eq!(ladder_delay(0), None);
    }

    #[test]
    fn args_host_rewrite_hits_every_url_and_keeps_flags() {
        let args = vec![
            "-a".to_string(),
            "--timeout=600".to_string(),
            "--log-file=/tmp/r/itemize.log".to_string(),
            "gutenberg.pglaf.org::gutenberg-epub/".to_string(),
            "gutenberg.pglaf.org::gutenberg-epub/./1342/".to_string(),
            "/tmp/dest/".to_string(),
        ];
        let swapped = args_with_host(&args, "rsync.ibiblio.org");
        assert_eq!(swapped[0], "-a");
        assert_eq!(swapped[2], "--log-file=/tmp/r/itemize.log");
        assert_eq!(swapped[3], "rsync.ibiblio.org::gutenberg-epub/");
        assert_eq!(swapped[4], "rsync.ibiblio.org::gutenberg-epub/./1342/");
        assert_eq!(swapped[5], "/tmp/dest/");
    }

    #[test]
    fn host_extraction_from_args() {
        let args = vec![
            "-a".to_string(),
            "gutenberg.pglaf.org::gutenberg-epub/".to_string(),
        ];
        assert_eq!(host_from_args(&args).unwrap(), "gutenberg.pglaf.org");
        let hostless = vec!["-a".to_string(), "/tmp/dest/".to_string()];
        assert!(host_from_args(&hostless).is_err());
    }

    #[test]
    fn args_json_roundtrip() {
        let sp = spec("args-json");
        let args = vec![
            "-a".to_string(),
            "gutenberg.pglaf.org::gutenberg-epub/./1342/".to_string(),
            "/tmp/dest/".to_string(),
        ];
        write_args_json(&sp.run_dir, &args).unwrap();
        assert_eq!(read_args_json(&sp.run_dir).unwrap(), Some(args));
        // A dir without the sidecar reads as None, not an error.
        assert_eq!(read_args_json(&sp.run_dir.parent().unwrap()).unwrap(), None);
        rm_dir(&sp);
    }
}
