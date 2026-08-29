//! Generic rsync runner: shells out to the system `rsync` binary, streams
//! `--itemize-changes --out-format='%i|%n|%b'` lines, classifies exit codes
//! and guarantees the child can never outlive this process.
//!
//! Child-process hygiene (replaces any supervision tree): every rsync runs in
//! its own process group, and `prctl(PR_SET_PDEATHSIG, SIGKILL)` (with the
//! standard fork-race check) makes the kernel kill it the instant this
//! process dies — even on SIGKILL. Wedged transfers are independently
//! bounded by rsync's own `--timeout`. Graceful shutdown is cooperative:
//! the caller sets the [`InterruptFlag`]; the runner group-TERMs the active
//! child, waits up to 10 s, then group-KILLs.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;

/// Cooperative shutdown signal shared between the daemon's signal handler and
/// the runner. Set on SIGTERM/SIGINT; cycles check it between phases.
#[derive(Clone, Default)]
pub struct InterruptFlag(Arc<AtomicBool>);

impl InterruptFlag {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub fn is_set(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// One parsed `%i|%n|%b` line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ItemizeLine {
    /// e.g. `>f+++++++++`, `<f.st......`, `*deleting`
    pub itemize: String,
    /// Path relative to the transfer root, e.g. `1342/pg1342-images.epub`.
    pub name: String,
    /// `%b` — bytes actually transferred (0 for deletions).
    pub bytes: u64,
}

impl ItemizeLine {
    pub fn is_deletion(&self) -> bool {
        self.itemize.starts_with("*deleting")
    }

    /// A real file transfer (not a deletion, not a directory/no-change line).
    pub fn is_file_transfer(&self) -> bool {
        if self.is_deletion() || self.name.ends_with('/') {
            return false;
        }
        // Itemize codes: `>f...`, `<f...`, `cf...`, `hf...` are files;
        // `.d..` is a directory line. Symlink/special lines land in the
        // caller's non-matching path.
        matches!(self.itemize.as_bytes().get(1), Some(b'f'))
    }
}

/// Parse `itemize|name|bytes`. Returns None for lines not in out-format shape.
pub fn parse_itemize(line: &str) -> Option<ItemizeLine> {
    let line = line.trim_end_matches(['\r', '\n']);
    let mut parts = line.splitn(3, '|');
    let itemize = parts.next()?.to_string();
    if itemize.is_empty() {
        return None;
    }
    let name = parts.next()?.to_string();
    let bytes: u64 = parts.next().unwrap_or("0").trim().parse().unwrap_or(0);
    if name.is_empty() {
        return None;
    }
    Some(ItemizeLine {
        itemize,
        name,
        bytes,
    })
}

/// Deterministic exit-code table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitClass {
    /// 0 — clean.
    Ok,
    /// 23/24 — partial transfer (files vanished upstream); keep what arrived.
    Partial,
    /// 5/6/10/11 — socket/timeout/io; retry ladder applies.
    Retryable,
    /// anything else — abort the run.
    Fatal,
}

pub fn classify_exit(code: i32) -> ExitClass {
    match code {
        0 => ExitClass::Ok,
        23 | 24 => ExitClass::Partial,
        5 | 6 | 10 | 11 => ExitClass::Retryable,
        _ => ExitClass::Fatal,
    }
}

#[derive(Debug, Default)]
pub struct RsyncOutcome {
    /// Raw exit code (None when the binary could not spawn or was killed).
    pub code: Option<i32>,
    pub stdout: Vec<String>,
    pub stderr: String,
    pub interrupted: bool,
    pub spawn_error: Option<String>,
}

impl RsyncOutcome {
    pub fn class(&self) -> Option<ExitClass> {
        self.code.map(classify_exit)
    }
}

/// Cheap-clone live progress handle for one rsync invocation. The stdout
/// reader thread feeds raw itemize lines to [`RsyncProgress::note`]; the
/// daemon publishes snapshots every few seconds. All fields are atomics —
/// reading never blocks the transfer.
#[derive(Debug, Default, Clone)]
pub struct RsyncProgress(Arc<ProgressInner>);

#[derive(Debug, Default)]
struct ProgressInner {
    files: AtomicU64,
    bytes: AtomicU64,
    /// Unix epoch milliseconds of the last itemize line, 0 = none yet.
    last_item_unix_ms: AtomicU64,
    /// Monotonic totals since process start; [`RsyncProgress::reset`]
    /// never touches these.
    cum_files: AtomicU64,
    cum_bytes: AtomicU64,
}

impl RsyncProgress {
    pub fn new() -> Self {
        Self::default()
    }
    /// Zero the per-attempt counters. `run_blocking` calls this at the
    /// start of every attempt so retry ladders never double-count. The
    /// cumulative totals ([`RsyncProgress::cumulative_snapshot`]) are
    /// never touched — they are monotonic for the process lifetime.
    pub fn reset(&self) {
        self.0.files.store(0, Ordering::Relaxed);
        self.0.bytes.store(0, Ordering::Relaxed);
        self.0.last_item_unix_ms.store(0, Ordering::Relaxed);
    }

    /// Account one raw `%i|%n|%b` stdout line. Unparseable lines are
    /// ignored. File transfers bump `files`/`bytes`; any parsed line
    /// (including deletions and directory entries) refreshes the
    /// last-item timestamp.
    pub fn note(&self, raw_line: &str) {
        let Some(line) = parse_itemize(raw_line) else {
            return;
        };
        if line.is_file_transfer() {
            self.0.files.fetch_add(1, Ordering::Relaxed);
            self.0.bytes.fetch_add(line.bytes, Ordering::Relaxed);
            self.0.cum_files.fetch_add(1, Ordering::Relaxed);
            self.0.cum_bytes.fetch_add(line.bytes, Ordering::Relaxed);
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.0.last_item_unix_ms.store(now_ms, Ordering::Relaxed);
    }

    /// `(files, bytes, last_item_unix_ms)` — last is 0 when nothing arrived.
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.0.files.load(Ordering::Relaxed),
            self.0.bytes.load(Ordering::Relaxed),
            self.0.last_item_unix_ms.load(Ordering::Relaxed),
        )
    }

    /// Cumulative `(files, bytes)` since process start. Never reset —
    /// the source of truth for monotonic "downloaded total" metrics.
    pub fn cumulative_snapshot(&self) -> (u64, u64) {
        (
            self.0.cum_files.load(Ordering::Relaxed),
            self.0.cum_bytes.load(Ordering::Relaxed),
        )
    }
}

pub struct RsyncRunner {
    flag: InterruptFlag,
}

impl RsyncRunner {
    /// Fails fast with a clear message when `rsync` is not on PATH.
    pub fn new(flag: InterruptFlag) -> anyhow::Result<Self> {
        let probe = Command::new("rsync")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .context("rsync binary not found on PATH — install rsync (>= 3.x)")?;
        anyhow::ensure!(
            probe.success(),
            "rsync --version failed with status {probe}"
        );
        Ok(Self { flag })
    }

    /// Run `rsync <args>` to completion, streaming stdout into the returned
    /// outcome. Blocking — call from `spawn_blocking`. Every attempt resets
    /// `progress` first (retry ladders re-run `run_blocking` per attempt,
    /// so counters reflect the current attempt, never an accumulation).
    /// When the interrupt flag is set mid-run the child's process group is
    /// TERMed, then KILLed after 10 s, and `interrupted: true` is returned.
    pub fn run_blocking(&self, args: &[String], progress: &RsyncProgress) -> RsyncOutcome {
        progress.reset();
        let mut outcome = RsyncOutcome::default();
        let mut child = match spawn_hygienic(args) {
            Ok(c) => c,
            Err(e) => {
                outcome.spawn_error = Some(e.to_string());
                return outcome;
            }
        };
        let pgid = child.id() as i32;

        let stdout = child.stdout.take().expect("rsync stdout piped");
        let stderr = child.stderr.take().expect("rsync stderr piped");
        // Owned clone for the reader thread (std::thread needs 'static).
        let progress = progress.clone();
        let out_handle = std::thread::spawn(move || {
            let mut lines = Vec::new();
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                tracing::debug!(target: "librarian::rsync", line = %line, "rsync");
                progress.note(&line);
                lines.push(line);
            }
            lines
        });
        let err_handle = std::thread::spawn(move || {
            let mut buf = String::new();
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                buf.push_str(&line);
                buf.push('\n');
            }
            buf
        });

        // Poll-wait so the interrupt flag is honored within ~50 ms.
        let status = loop {
            if let Ok(Some(st)) = child.try_wait() {
                break Some(st);
            }
            if self.flag.is_set() {
                kill_group(&mut child, pgid);
                outcome.interrupted = true;
                break None;
            }
            std::thread::sleep(Duration::from_millis(50));
        };

        outcome.stdout = out_handle.join().unwrap_or_default();
        outcome.stderr = err_handle.join().unwrap_or_default();
        outcome.code = status.map(|s| s.code().unwrap_or(-1));
        outcome
    }
}

/// Spawn `rsync` in its own process group with PDEATHSIG set. See module docs.
fn spawn_hygienic(args: &[String]) -> std::io::Result<Child> {
    use std::os::unix::process::CommandExt;
    let my_pid = std::process::id() as libc::pid_t;
    let mut cmd = Command::new("rsync");
    cmd.args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    unsafe {
        cmd.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            // Fork race: if the original parent died between fork and prctl,
            // we are already orphaned and the death signal fired for the
            // wrong parent — exit now rather than run unattended.
            if libc::getppid() != my_pid {
                libc::_exit(127);
            }
            Ok(())
        });
    }
    cmd.spawn()
}

/// SIGTERM the group, wait up to 10 s, then SIGKILL, then reap.
fn kill_group(child: &mut Child, pgid: i32) {
    unsafe {
        libc::kill(-pgid, libc::SIGTERM);
    }
    for _ in 0..200 {
        if let Ok(Some(_)) = child.try_wait() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    unsafe {
        libc::kill(-pgid, libc::SIGKILL);
    }
    let _ = child.wait();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn note_counts_transfers_and_ignores_noise() {
        let p = RsyncProgress::new();
        p.note("<f.st......|1342/pg1342-images.epub|24846294");
        p.note(">f+++++++++|51564/pg51564.rdf|18220");
        // Unparseable lines are ignored (no separator, empty itemize).
        p.note("sent 1,234 bytes  received 99 bytes");
        p.note("");
        let (files, bytes, last_ms) = p.snapshot();
        assert_eq!(files, 2);
        assert_eq!(bytes, 24_846_294 + 18_220);
        assert!(last_ms > 0, "parsed lines must refresh the item timestamp");
    }

    #[test]
    fn deletions_and_dirs_are_items_but_not_files() {
        let p = RsyncProgress::new();
        p.note("*deleting|1342/pg1342-h.zip|0");
        p.note(".d..t......|1342/|0");
        assert_eq!(p.snapshot().0, 0);
        assert_eq!(p.snapshot().1, 0);
        assert!(p.snapshot().2 > 0);
    }

    #[test]
    fn reset_zeroes_counters() {
        let p = RsyncProgress::new();
        p.note(">f+++++++++|51564/pg51564.rdf|18220");
        p.reset();
        assert_eq!(p.snapshot(), (0, 0, 0));
    }

    #[test]
    fn reset_keeps_cumulative_totals() {
        let p = RsyncProgress::new();
        p.note(">f+++++++++|51564/pg51564.rdf|18220");
        p.reset();
        // Per-attempt counters zero; cumulative pair is monotonic.
        assert_eq!(p.snapshot(), (0, 0, 0));
        assert_eq!(p.cumulative_snapshot(), (1, 18_220));
        // The next attempt keeps growing the cumulative pair.
        p.note(">f+++++++++|1342/pg1342.rdf|1000");
        assert_eq!(p.cumulative_snapshot(), (2, 19_220));
    }
}
