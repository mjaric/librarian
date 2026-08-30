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
//!
//! Detached runs (the supervisor-facing counterpart) deliberately invert
//! that invariant: [`spawn_detached`] starts a wrapper in its own session
//! with no PDEATHSIG so the rsync child outlives this process, leaving
//! durable witnesses in a run dir (intent/pid/exit/itemize) that a future
//! supervisor can read, poll and adopt.

use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
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

/// Parse `itemize|name|bytes`. Tolerates both shapes the run protocol
/// produces: bare `--out-format` lines (stdout) and `--log-file` lines,
/// which rsync prefixes with `YYYY/MM/DD HH:MM:SS [pid] ` before the
/// configured `--log-file-format` output. Returns None for lines not in
/// (possibly prefixed) out-format shape.
pub fn parse_itemize(line: &str) -> Option<ItemizeLine> {
    let line = line.trim_end_matches(['\r', '\n']);
    let line = strip_rsync_log_prefix(line).unwrap_or(line);
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

/// rsync's `--log-file` writer prepends `%Y/%m/%d %H:%M:%S [pid] ` to
/// every line, ahead of the format's own output — a bare `%i` would
/// otherwise never sit at column 0. Returns the line without that
/// prefix, or None when the line does not carry it (a stdout
/// `--out-format` line must pass through untouched).
fn strip_rsync_log_prefix(line: &str) -> Option<&str> {
    let b = line.as_bytes();
    if b.len() < 23
        || b[4] != b'/'
        || b[7] != b'/'
        || b[10] != b' '
        || b[13] != b':'
        || b[16] != b':'
        || b[19] != b' '
        || b[20] != b'['
    {
        return None;
    }
    let digits = |s: &[u8]| s.iter().all(u8::is_ascii_digit);
    let date_time = [
        &b[0..4],
        &b[5..7],
        &b[8..10],
        &b[11..13],
        &b[14..16],
        &b[17..19],
    ];
    if !date_time.iter().all(|f| digits(f)) {
        return None;
    }
    let close = b.iter().position(|&c| c == b']')?;
    let pid = b.get(21..close)?;
    if pid.is_empty() || !digits(pid) || b.get(close + 1) != Some(&b' ') {
        return None;
    }
    line.get(close + 2..)
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

// -- detached runs ---------------------------------------------------------
//
// Durable state lives in the run dir; the wrapper (see [`spawn_detached`])
// maintains the pid witness and the exit record:
//
//   intent.json — per-attempt intent, rewritten before every (re)spawn
//   pid         — "<pgid> <starttime>", written by the wrapper before rsync
//   stderr.log  — rsync's merged stdout+stderr, appended across attempts
//   itemize.log — NOT written by the wrapper; the caller passes rsync's
//                 `--log-file`/`--log-file-format` args to point it here
//   exit        — wrapper-written exit code, rename-atomic
//
// Supervisor contract: poll [`run_is_live`] + [`itemize_delta`]; an `exit`
// file (or a [`LiveState::Dead`]) ends the run. The recorded starttime
// defeats PID reuse. The spawner does not keep a Child handle, so an exited
// wrapper it spawned lingers as a zombie until the spawner itself exits —
// harmless: [`run_is_live`] reads zombie leaders as not live, and a
// supervisor adopting a run after the daemon died sees a properly reaped
// process tree.

/// Detached-run wrapper protocol spoken by [`spawn_detached`]: records the
/// pid+starttime witness, runs `rsync` with output merged into `stderr.log`,
/// then records the exit code. `$$` is the wrapper's pid AND its process
/// group id — setsid makes the wrapper a session (and group) leader, so the
/// whole run is one killable group. The starttime arithmetic (strip through
/// the last `)`, then field 20) mirrors [`proc_stat`] in Rust.
///
/// Public because it is shared by EVERY launcher — this process's
/// [`spawn_detached`] and the docker launcher alike — so the run-dir
/// artifact protocol (pid/stderr.log/exit) stays single-sourced. Changes
/// here are protocol changes; every launcher runs the byte-identical script.
pub const DETACHED_WRAPPER: &str = r#"#!/bin/sh
# Detached rsync wrapper: durable pid+starttime witness and exit recorder.
RUN_DIR="$1"; shift
printf '%s %s\n' "$$" "$(sed 's/.*) //' /proc/self/stat | cut -d' ' -f20)" \
  > "$RUN_DIR/pid.tmp" && mv "$RUN_DIR/pid.tmp" "$RUN_DIR/pid"
rsync "$@" >> "$RUN_DIR/stderr.log" 2>&1
status=$?
printf '%s\n' "$status" > "$RUN_DIR/exit.tmp" && mv "$RUN_DIR/exit.tmp" "$RUN_DIR/exit"
exit "$status"
"#;

/// Durable per-attempt intent for one detached rsync run (written by the
/// caller before spawn, rewritten on every respawn attempt).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RunIntent {
    pub attempt: u32,
    pub host: String,
    /// RFC 3339 timestamp of the spawn decision (caller-supplied).
    pub started_at: String,
}

/// Serialize the intent to `<run_dir>/intent.json` (tmp + rename, so a
/// concurrent reader never sees a torn file). Creates `run_dir` when missing.
pub fn write_intent(run_dir: &Path, intent: &RunIntent) -> anyhow::Result<()> {
    std::fs::create_dir_all(run_dir)
        .with_context(|| format!("creating run dir {}", run_dir.display()))?;
    let json = serde_json::to_string_pretty(intent).context("serializing run intent")?;
    let tmp = run_dir.join("intent.json.tmp");
    std::fs::write(&tmp, json).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, run_dir.join("intent.json"))
        .with_context(|| format!("renaming intent into {}", run_dir.display()))?;
    Ok(())
}

/// Last written intent; `Ok(None)` when no run was ever spawned here.
pub fn read_intent(run_dir: &Path) -> anyhow::Result<Option<RunIntent>> {
    let raw = match std::fs::read_to_string(run_dir.join("intent.json")) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).context("reading run intent"),
    };
    serde_json::from_str(&raw)
        .map(Some)
        .with_context(|| format!("malformed intent.json in {}", run_dir.display()))
}

/// Spawn `sh -c WRAPPER <run_dir> <args...>` detached: the wrapper gets its
/// own session via `setsid` and deliberately NO PDEATHSIG — outliving this
/// process is the entire point (a supervisor adopts the run later). stdout
/// is discarded; the wrapper appends rsync's output to `<run_dir>/stderr.log`.
/// Writes `intent.json` before spawning. Does not wait — the returned
/// `Child` handle is dropped immediately (dropping never kills), and the
/// exited wrapper may linger as a zombie until this process exits.
pub fn spawn_detached(args: &[String], run_dir: &Path, intent: &RunIntent) -> anyhow::Result<()> {
    use std::os::unix::process::CommandExt;
    write_intent(run_dir, intent)
        .with_context(|| format!("recording intent for detached run in {}", run_dir.display()))?;
    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg(DETACHED_WRAPPER)
        // POSIX `sh -c` makes the first operand $0, so the run_dir needs a
        // conventional argv[0] placeholder before it to land in $1.
        .arg("rsync-wrapper")
        .arg(run_dir)
        .args(args)
        .stdout(Stdio::null());
    unsafe {
        // Own session and process group: nothing outside the run signals it,
        // and the run survives our death by construction.
        cmd.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let _child = cmd
        .spawn()
        .with_context(|| format!("spawning detached rsync wrapper in {}", run_dir.display()))?;
    Ok(())
}

/// Liveness of the recorded run. `Gone` = no pid file; `Dead` = pid file
/// exists but `/proc/<pid>` is missing (or a zombie) or the starttime
/// differs (PID reuse guard); `Live` otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveState {
    Live,
    Dead,
    Gone,
}

pub fn run_is_live(run_dir: &Path) -> LiveState {
    let Some((pid, recorded_start)) = read_pid_file(run_dir) else {
        return LiveState::Gone;
    };
    match proc_stat(pid) {
        Some((state, start)) if state != 'Z' && start == recorded_start => LiveState::Live,
        _ => LiveState::Dead,
    }
}

pub fn read_exit(run_dir: &Path) -> anyhow::Result<Option<i32>> {
    let raw = match std::fs::read_to_string(run_dir.join("exit")) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).context("reading detached-run exit file"),
    };
    raw.trim()
        .parse()
        .map(Some)
        .with_context(|| format!("malformed exit file in {}", run_dir.display()))
}

/// The recorded process group id — the pid file's first integer (the wrapper
/// is a session leader, so its pid is the group id).
pub fn read_pgid(run_dir: &Path) -> anyhow::Result<Option<i32>> {
    match read_pid_file(run_dir) {
        Some((pgid, _)) => Ok(Some(pgid)),
        None if run_dir.join("pid").exists() => Err(anyhow::anyhow!(
            "malformed pid file in {}",
            run_dir.display()
        )),
        None => Ok(None),
    }
}

/// TERM the run's process group, poll up to 10 s, then KILL. The detached
/// analogue of [`kill_group`]: there is no `Child` handle, so liveness is
/// read from `/proc` (the wrapper's sh is the group leader and lives exactly
/// as long as its rsync — it waits on it), and our own zombie leader is
/// reaped via WNOHANG; waitpid fails harmlessly for adopted runs.
pub fn terminate_group(pgid: i32) {
    unsafe {
        libc::kill(-pgid, libc::SIGTERM);
    }
    for _ in 0..200 {
        unsafe {
            libc::waitpid(pgid, std::ptr::null_mut(), libc::WNOHANG);
        }
        if matches!(proc_stat(pgid), None | Some(('Z', _))) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    unsafe {
        libc::kill(-pgid, libc::SIGKILL);
    }
}

/// Fold newly appended `%i|%n|%b` lines from `<run_dir>/itemize.log` into a
/// `(files, bytes)` delta. `offset` is the caller's durable read position in
/// bytes; it advances only over complete lines, so a torn tail written by a
/// live rsync stays for the next poll. Counting mirrors
/// [`RsyncProgress::note`]: file transfers only, deletions and dir lines
/// parse but never count.
pub fn itemize_delta(run_dir: &Path, offset: &mut u64) -> anyhow::Result<(u64, u64)> {
    let mut file = match std::fs::File::open(run_dir.join("itemize.log")) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(e) => return Err(e).context("opening detached-run itemize log"),
    };
    if file.metadata()?.len() <= *offset {
        return Ok((0, 0));
    }
    file.seek(SeekFrom::Start(*offset))?;
    let mut fresh = Vec::new();
    file.read_to_end(&mut fresh)?;
    let Some(end) = fresh.iter().rposition(|&b| b == b'\n') else {
        return Ok((0, 0));
    };
    let consumed = end as u64 + 1;
    let (mut files, mut bytes) = (0u64, 0u64);
    for line in String::from_utf8_lossy(&fresh[..consumed as usize]).lines() {
        if let Some(item) = parse_itemize(line) {
            if item.is_file_transfer() {
                files += 1;
                bytes += item.bytes;
            }
        }
    }
    *offset += consumed;
    Ok((files, bytes))
}

/// Remove the whole run dir — the reap step after a successful finalize.
/// Idempotent: an already-gone dir is a successful reap.
pub fn clear_run(run_dir: &Path) -> anyhow::Result<()> {
    match std::fs::remove_dir_all(run_dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("removing run dir {}", run_dir.display())),
    }
}

/// `<pgid> <starttime>` from the wrapper's pid witness. `None` when absent
/// or malformed (a torn read is impossible — the wrapper renames atomically).
fn read_pid_file(run_dir: &Path) -> Option<(i32, u64)> {
    let raw = std::fs::read_to_string(run_dir.join("pid")).ok()?;
    let mut fields = raw.split_whitespace();
    let pgid = fields.next()?.parse().ok()?;
    let starttime = fields.next()?.parse().ok()?;
    Some((pgid, starttime))
}

/// `(state, starttime)` from `/proc/<pid>/stat`. comm may contain spaces and
/// parens, so everything through the LAST `)` is stripped; the remainder
/// starts at stat field 3 (state), making starttime (field 22) the 20th
/// field of the remainder — the same arithmetic as the wrapper's
/// `sed 's/.*) //' | cut -d' ' -f20`.
fn proc_stat(pid: i32) -> Option<(char, u64)> {
    let raw = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rest = raw.rsplit_once(')')?.1;
    let mut fields = rest.split_whitespace();
    let state = fields.next()?.chars().next()?;
    let starttime = fields.nth(18)?.parse().ok()?;
    Some((state, starttime))
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

    #[test]
    fn parse_accepts_rsync_log_file_prefixed_lines() {
        // The exact shape rsync writes for --log-file: a timestamp+pid
        // prefix ahead of the --log-file-format output (seen in a real
        // run — without the strip, every transfer line parses as noise).
        let line = "2026/08/30 00:48:37 [2506646] >f+++++++++|42/pg42.bin|12584487";
        let item = parse_itemize(line).expect("prefixed transfer must parse");
        assert!(item.is_file_transfer());
        assert_eq!(item.name, "42/pg42.bin");
        assert_eq!(item.bytes, 12_584_487);

        let del = parse_itemize("2026/08/30 00:48:37 [2506646] *deleting|1342/pg1342-h.zip|0")
            .expect("prefixed deletion must parse");
        assert!(del.is_deletion());
        assert!(!del.is_file_transfer());

        // Prefixed bookkeeping lines are not itemize lines.
        assert_eq!(
            parse_itemize("2026/08/30 00:48:29 [2506646] building file list"),
            None
        );
        assert_eq!(
            parse_itemize("2026/08/30 00:48:37 [2506646] sent 1 bytes"),
            None
        );

        // A malformed "prefix" (bad digits) is not stripped; the line has
        // no out-format fields and stays unparsed.
        assert_eq!(
            parse_itemize("2026/13/45 99:99:99 [abc] not an itemize"),
            None
        );

        // Bare stdout shapes still parse untouched.
        let bare = parse_itemize(">f+++++++++|51564/pg51564.rdf|18220").unwrap();
        assert_eq!(bare.name, "51564/pg51564.rdf");
    }

    // -- detached runs -----------------------------------------------------

    /// Collision-free scratch dir under temp_dir — hand-rolled, no deps.
    fn unique_run_dir(tag: &str) -> std::path::PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "bookshelf-core-{tag}-{}-{}-{}",
            std::process::id(),
            nanos,
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn gone_or_zombie(pgid: i32) -> bool {
        matches!(proc_stat(pgid), None | Some(('Z', _)))
    }

    #[test]
    fn intent_roundtrips_and_missing_is_none() {
        let dir = unique_run_dir("intent");
        assert_eq!(read_intent(&dir).unwrap(), None, "no intent yet");
        let intent = RunIntent {
            attempt: 3,
            host: "rsync://example.org::gutenberg".into(),
            started_at: "2026-08-29T12:00:00Z".into(),
        };
        write_intent(&dir, &intent).unwrap();
        assert_eq!(read_intent(&dir).unwrap(), Some(intent));
        clear_run(&dir).unwrap();
        assert!(!dir.exists(), "clear_run must remove the run dir");
        assert_eq!(
            read_intent(&dir).unwrap(),
            None,
            "cleared run has no intent"
        );
    }

    #[test]
    fn run_is_live_distinguishes_live_dead_gone() {
        use std::os::unix::process::CommandExt;
        let dir = unique_run_dir("liveness");
        assert_eq!(run_is_live(&dir), LiveState::Gone, "no pid file");
        let mut child = Command::new("sleep")
            .arg("30")
            .process_group(0)
            .spawn()
            .expect("spawn sleep probe");
        let pid = child.id() as i32;
        let (_, starttime) = proc_stat(pid).expect("fresh child has /proc stat");
        let witness = dir.join("pid");
        std::fs::write(&witness, format!("{pid} {starttime}\n")).unwrap();
        assert_eq!(run_is_live(&dir), LiveState::Live);
        // Same pid, different starttime — a reused PID must read as dead.
        std::fs::write(&witness, format!("{pid} {}\n", starttime + 1_000_000)).unwrap();
        assert_eq!(run_is_live(&dir), LiveState::Dead);
        std::fs::remove_file(&witness).unwrap();
        assert_eq!(run_is_live(&dir), LiveState::Gone);
        child.kill().expect("kill sleep probe");
        child.wait().expect("reap sleep probe");
        clear_run(&dir).unwrap();
    }

    #[test]
    fn read_exit_roundtrip() {
        let dir = unique_run_dir("exit");
        assert_eq!(read_exit(&dir).unwrap(), None, "no exit file yet");
        std::fs::write(dir.join("exit"), "23\n").unwrap();
        assert_eq!(read_exit(&dir).unwrap(), Some(23));
        clear_run(&dir).unwrap();
    }

    #[test]
    fn itemize_delta_offsets_over_chunks() {
        let dir = unique_run_dir("itemize");
        let log = dir.join("itemize.log");
        let mut offset = 0u64;
        assert_eq!(
            itemize_delta(&dir, &mut offset).unwrap(),
            (0, 0),
            "no log yet"
        );
        let chunk1 = ">f+++++++++|1342/1342.txt|500\n<f.st......|1342/pic.jpg|300\n";
        let torn = ">f+++++++++|1342/new|12";
        let chunk2 = format!("*deleting|1342/old.txt|0\n.d..t......|1342/|0\n{torn}");
        std::fs::write(&log, chunk1).unwrap();
        assert_eq!(itemize_delta(&dir, &mut offset).unwrap(), (2, 800));
        assert_eq!(offset, chunk1.len() as u64);
        // A torn tail line is held back until its newline arrives; deletions
        // and dir lines parse but never count as file transfers.
        std::fs::write(&log, format!("{chunk1}{chunk2}")).unwrap();
        assert_eq!(
            itemize_delta(&dir, &mut offset).unwrap(),
            (0, 0),
            "only the deletion/dir lines completed; the torn file line waits"
        );
        assert_eq!(offset, (chunk1.len() + chunk2.len() - torn.len()) as u64);
        std::fs::write(&log, format!("{chunk1}{chunk2}3\n")).unwrap();
        assert_eq!(itemize_delta(&dir, &mut offset).unwrap(), (1, 123));
        assert_eq!(offset, (chunk1.len() + chunk2.len() + 2) as u64);
        // Idempotent: a poll with no new bytes is a zero delta.
        assert_eq!(itemize_delta(&dir, &mut offset).unwrap(), (0, 0));
        assert_eq!(itemize_delta(&dir, &mut offset).unwrap(), (0, 0));
        clear_run(&dir).unwrap();
    }

    #[test]
    fn terminate_group_kills_a_sleep_group() {
        use std::os::unix::process::CommandExt;
        let child = Command::new("sh")
            .arg("-c")
            .arg("sleep 30 & wait")
            .process_group(0)
            .spawn()
            .expect("spawn sleep group");
        let pgid = child.id() as i32;
        // No Child handle is kept across the call — the detached path.
        terminate_group(pgid);
        let mut gone = gone_or_zombie(pgid);
        for _ in 0..100 {
            if gone {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
            gone = gone_or_zombie(pgid);
        }
        assert!(
            gone,
            "sleep group must be gone (or a reaped zombie) after terminate_group"
        );
    }

    #[test]
    fn spawn_detached_writes_witness_intent_and_exit() {
        // Protocol smoke against the real rsync binary; vacuous without it.
        let probe = Command::new("rsync").arg("--version").status();
        if probe.map(|s| !s.success()).unwrap_or(true) {
            eprintln!("SKIP: rsync not on PATH");
            return;
        }
        let dir = unique_run_dir("detached");
        let intent = RunIntent {
            attempt: 1,
            host: "local-test".into(),
            started_at: "2026-08-29T12:00:00Z".into(),
        };
        spawn_detached(&["--version".to_string()], &dir, &intent).unwrap();
        assert_eq!(read_intent(&dir).unwrap().unwrap(), intent);
        let mut pgid = None;
        for _ in 0..250 {
            if let Some(p) = read_pgid(&dir).unwrap() {
                pgid = Some(p);
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let pgid = pgid.expect("wrapper must publish the pid witness");
        let raw = std::fs::read_to_string(dir.join("pid")).unwrap();
        let mut fields = raw.split_whitespace();
        let recorded: i32 = fields.next().unwrap().parse().unwrap();
        let starttime: u64 = fields.next().unwrap().parse().unwrap();
        assert_eq!(recorded, pgid, "pid file format is \"<pgid> <starttime>\"");
        assert!(starttime > 0, "starttime must be a real /proc clock count");
        // `rsync --version` exits 0; the wrapper records it via tmp+rename.
        let mut exit = None;
        for _ in 0..250 {
            if let Some(code) = read_exit(&dir).unwrap() {
                exit = Some(code);
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(exit, Some(0), "wrapper must record the exit code");
        let mut dead = run_is_live(&dir) == LiveState::Dead;
        for _ in 0..100 {
            if dead {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
            dead = run_is_live(&dir) == LiveState::Dead;
        }
        assert!(dead, "exited run must no longer read as live");
        // A late TERM of the finished group is a harmless no-op that also
        // reaps our zombie wrapper.
        terminate_group(pgid);
        clear_run(&dir).unwrap();
        assert!(!dir.exists());
    }
}
