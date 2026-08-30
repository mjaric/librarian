//! Docker launcher for detached rsync transfers: the second
//! [`SyncLauncher`] implementation, uniform with [`ProcessLauncher`] so
//! boot adoption behaves identically for both.
//!
//! Protocol: one container per transfer, named
//! `librarian-rsync-<source>-r<run_id>`, running the shared
//! [`bookshelf_core::DETACHED_WRAPPER`] (`--entrypoint sh <image> -c
//! WRAPPER rsync-wrapper <run_dir> <args...>`) — byte-identical with
//! `spawn_detached`, so the run-dir artifact protocol
//! (intent/pid/stderr.log/exit/itemize.log) is single-sourced. The library
//! root (the run dir's grandparent, per the frozen
//! `<library_dir>/run/<source>-r<run_id>` layout) is bind-mounted rw AT
//! ITS OWN absolute path — exactly one `-v` — so the spec's
//! `--log-file=<run_dir>/itemize.log` and the mirror dest resolve
//! identically inside and outside the container.
//!
//! Politeness invariant: containers are created with `--restart=no`. The
//! supervisor owns retries; a docker restart policy must never race it.
//!
//! Authority: the wrapper's `exit` file beats `docker inspect` (same
//! precedence as the process launcher). The run dir's `pid` file holds a
//! container-internal pid — NOT a host pid — so `/proc`-based liveness
//! (`run_is_live`) must never be used here; liveness is
//! `docker inspect -f '{{.State.Status}}'`.

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use anyhow::Context;

use bookshelf_core::{DETACHED_WRAPPER, RunIntent, clear_run, read_exit, write_intent};

use crate::supervisor::{Observation, RsyncSpec, SyncLauncher};

/// Spawns and observes detached rsync transfers as one docker container
/// each, over the same run-dir artifacts as [`ProcessLauncher`].
pub struct DockerLauncher {
    image: String,
    /// Injectable for tests (defaults to "docker").
    docker_bin: PathBuf,
}

impl DockerLauncher {
    pub fn new(image: &str) -> Self {
        Self::with_bin(image, PathBuf::from("docker"))
    }

    /// Test seam: run `bin` instead of the PATH `docker`. Public only
    /// because the integration tests live in `tests/` and cannot see
    /// `pub(crate)`.
    pub fn with_bin(image: &str, bin: PathBuf) -> Self {
        Self {
            image: image.to_string(),
            docker_bin: bin,
        }
    }

    /// Fails fast with a clear message when the docker CLI is unusable
    /// (the `RsyncRunner::new` probe pattern).
    pub fn probe(bin: &Path) -> anyhow::Result<()> {
        let probe = Command::new(bin)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .context(
                "docker binary not found on PATH — install Docker or use launcher = \"process\"",
            )?;
        anyhow::ensure!(
            probe.success(),
            "docker --version failed with status {probe}"
        );
        Ok(())
    }

    /// Boot sweep: stop+rm containers labeled for this source that have no
    /// matching run dir on disk (insurance for wiped libraries). Errors are
    /// ignored per container — the sweep is insurance, never a reason to
    /// fail the boot. Returns the number of containers removed.
    pub fn reap_orphans(&self, source: &str, run_root: &Path) -> anyhow::Result<usize> {
        let ps = ps_args(source);
        let listing = checked(&ps, self.exec(&ps)?)?;
        let mut swept = 0;
        for name in listing.lines().map(str::trim).filter(|n| !n.is_empty()) {
            let has_run_dir = match run_id_from_name(name) {
                Some(run_id) => run_root.join(run_dir_name(source, run_id)).is_dir(),
                None => false,
            };
            if has_run_dir {
                continue;
            }
            let stop = stop_args(name);
            let _ = self.exec(&stop).and_then(|out| checked(&stop, out));
            let rm = rm_args(name);
            if self.exec(&rm).and_then(|out| checked(&rm, out)).is_ok() {
                swept += 1;
            }
        }
        Ok(swept)
    }

    fn exec(&self, args: &[String]) -> anyhow::Result<Output> {
        Command::new(&self.docker_bin)
            .args(args)
            .output()
            .with_context(|| format!("running docker {} failed", args[0]))
    }
}

impl SyncLauncher for DockerLauncher {
    fn spawn(&self, spec: &RsyncSpec, intent: &RunIntent) -> anyhow::Result<()> {
        // Host side first: intent.json exists before anything inside the
        // container can race it (same order as `spawn_detached`).
        write_intent(&spec.run_dir, intent)?;
        let create = create_args(
            &self.image,
            &spec.source,
            spec.run_id,
            &spec.run_dir,
            &spec.args,
        )?;
        checked(&create, self.exec(&create)?)?;
        let start = vec![
            "start".to_string(),
            container_name(&spec.source, spec.run_id),
        ];
        checked(&start, self.exec(&start)?)?;
        Ok(())
    }

    fn observe(&self, spec: &RsyncSpec) -> anyhow::Result<Observation> {
        // The wrapper's exit file is the last word — read before any
        // inspect, exactly like the process launcher. The pid file's pid is
        // container-internal, so `run_is_live` is off-limits here.
        if let Some(code) = read_exit(&spec.run_dir)? {
            return Ok(Observation::Exited(code));
        }
        let name = container_name(&spec.source, spec.run_id);
        let args = status_args(&name);
        let out = self.exec(&args)?;
        if !out.status.success() {
            if is_missing(&out) {
                return Ok(Observation::Absent);
            }
            anyhow::bail!("docker inspect {name} failed: {}", stderr_head(&out));
        }
        match String::from_utf8_lossy(&out.stdout).trim() {
            "running" => Ok(Observation::Live),
            // Died without the wrapper writing `exit` — the OOM-killed
            // process-launcher class.
            "exited" | "dead" | "created" => Ok(Observation::DeadUnreaped),
            other => {
                anyhow::bail!("docker inspect {name} reported unexpected status {other:?}")
            }
        }
    }

    fn terminate(&self, spec: &RsyncSpec) -> anyhow::Result<()> {
        // Blocking up to ~10 s is accepted — same as the process launcher's
        // terminate_group (TERM → 10 s → KILL).
        let name = container_name(&spec.source, spec.run_id);
        let args = stop_args(&name);
        let out = self.exec(&args)?;
        if !out.status.success() && !is_missing(&out) {
            anyhow::bail!("docker stop {name} failed: {}", stderr_head(&out));
        }
        Ok(())
    }

    fn reap(&self, spec: &RsyncSpec) -> anyhow::Result<()> {
        let name = container_name(&spec.source, spec.run_id);
        let args = rm_args(&name);
        let out = self.exec(&args)?;
        if !out.status.success() && !is_missing(&out) {
            anyhow::bail!("docker rm {name} failed: {}", stderr_head(&out));
        }
        clear_run(&spec.run_dir)
    }
}

/// Keep `[A-Za-z0-9._-]`, map everything else to `-`. Provider keys are
/// clean already; this is defensive against future sources.
fn sanitize_source(source: &str) -> String {
    source
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Container name for one transfer. Public so the `tests/` integration
/// suite can assert it without docker.
pub fn container_name(source: &str, run_id: i64) -> String {
    format!("librarian-rsync-{}-r{run_id}", sanitize_source(source))
}

/// Run-dir name for one transfer (raw source — the run-dir protocol keeps
/// the provider key verbatim).
fn run_dir_name(source: &str, run_id: i64) -> String {
    format!("{source}-r{run_id}")
}

/// Parse the run id back out of a container name (the `-r` separator
/// written by [`container_name`], split from the right — the sanitized
/// source itself may contain `-r`; same shape as mirror's `run_id_of`).
fn run_id_from_name(name: &str) -> Option<i64> {
    name.rsplit_once("-r")?.1.parse().ok()
}

/// The bind-mount root: the canonicalized run dir's grandparent, per the
/// frozen layout `<library_dir>/run/<source>-r<run_id>`.
fn library_root(run_dir: &Path) -> anyhow::Result<&Path> {
    run_dir.parent().and_then(|p| p.parent()).context(format!(
        "deriving library root from run dir {}",
        run_dir.display()
    ))
}

/// The single `-v` argument value: the library root bound rw at its own
/// absolute path, so spec paths resolve identically on both sides.
fn mount_arg(root: &Path) -> String {
    format!("{}:{}:rw", root.display(), root.display())
}

/// The container argv after the image: the shared wrapper, byte-identical
/// with `spawn_detached` (`-c WRAPPER rsync-wrapper <run_dir> <args...>`;
/// `rsync-wrapper` is the conventional $0 placeholder).
fn entrypoint_tail(run_dir: &Path, args: &[String]) -> Vec<String> {
    let mut v = vec![
        "-c".to_string(),
        DETACHED_WRAPPER.to_string(),
        "rsync-wrapper".to_string(),
        run_dir.display().to_string(),
    ];
    v.extend(args.iter().cloned());
    v
}

/// `docker create` argv: name, labels, politeness restart policy, the one
/// mount, then the image and the wrapper tail. `run_dir` is canonicalized
/// here (it exists by then — `write_intent` ran first).
fn create_args(
    image: &str,
    source: &str,
    run_id: i64,
    run_dir: &Path,
    args: &[String],
) -> anyhow::Result<Vec<String>> {
    let run_dir = std::fs::canonicalize(run_dir)
        .with_context(|| format!("canonicalizing run dir {}", run_dir.display()))?;
    let root = library_root(&run_dir)?;
    let mut v = vec![
        "create".to_string(),
        "--name".to_string(),
        container_name(source, run_id),
        "--label".to_string(),
        format!("librarian.source={source}"),
        "--label".to_string(),
        format!("librarian.run_id={run_id}"),
        "--restart=no".to_string(),
        "-v".to_string(),
        mount_arg(root),
        "--entrypoint".to_string(),
        "sh".to_string(),
        image.to_string(),
    ];
    v.extend(entrypoint_tail(&run_dir, args));
    Ok(v)
}

fn status_args(name: &str) -> Vec<String> {
    vec![
        "inspect".to_string(),
        "-f".to_string(),
        "{{.State.Status}}".to_string(),
        name.to_string(),
    ]
}

fn stop_args(name: &str) -> Vec<String> {
    vec![
        "stop".to_string(),
        "-t".to_string(),
        "10".to_string(),
        name.to_string(),
    ]
}

fn rm_args(name: &str) -> Vec<String> {
    vec!["rm".to_string(), "-f".to_string(), name.to_string()]
}

fn ps_args(source: &str) -> Vec<String> {
    vec![
        "ps".to_string(),
        "-a".to_string(),
        "--filter".to_string(),
        format!("label=librarian.source={source}"),
        "--format".to_string(),
        "{{.Names}}".to_string(),
    ]
}

/// docker's missing-object marker (`inspect` vs `stop`/`rm` phrasing).
fn is_missing(out: &Output) -> bool {
    let stderr = String::from_utf8_lossy(&out.stderr);
    stderr.contains("No such object") || stderr.contains("No such container")
}

/// First ~300 chars of stderr for error messages (the `FetchError`-style
/// body head, so triage output stays bounded).
fn stderr_head(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr)
        .trim()
        .chars()
        .take(300)
        .collect()
}

/// Run a docker subcommand to completion: non-zero exit → anyhow error
/// with the stderr head; success → trimmed stdout.
fn checked(args: &[String], out: Output) -> anyhow::Result<String> {
    if !out.status.success() {
        anyhow::bail!("docker {} failed: {}", args[0], stderr_head(&out));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}
