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
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// outcome. Blocking — call from `spawn_blocking`. When the interrupt
    /// flag is set mid-run the child's process group is TERMed, then KILLed
    /// after 10 s, and `interrupted: true` is returned.
    pub fn run_blocking(&self, args: &[String]) -> RsyncOutcome {
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
        let out_handle = std::thread::spawn(move || {
            let mut lines = Vec::new();
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                tracing::debug!(target: "librarian::rsync", line = %line, "rsync");
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
