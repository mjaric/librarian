//! Integration tests for the docker supervisor launcher — no real docker.
//! A fake `docker` shell script (written under a temp dir, chmod +x)
//! records its argv (NUL-separated) to a log and emits canned responses:
//! `inspect` says "running" or "exited" based on a sentinel file, and
//! not-found/missing-container errors are emulated on demand. Exit files
//! are crafted host-side, as the wrapper would.

use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use bookshelf_core::{DETACHED_WRAPPER, RunIntent};
use librarian::supervisor::{Observation, RsyncSpec, SyncLauncher};
use librarian::supervisor_docker::{DockerLauncher, container_name};

/// One fake docker binary + its state dir (sentinels) + argv log.
struct Fake {
    bin: PathBuf,
    state: PathBuf,
    log: PathBuf,
    root: PathBuf,
}

/// Build a fake `docker` script with this test's paths baked in. Sentinels
/// (created by tests on demand): `exited`, `notfound`, `ps`.
fn fake(tag: &str) -> Fake {
    let base = std::env::temp_dir().join(format!("bookshelf-supervisor-docker-{tag}"));
    let _ = std::fs::remove_dir_all(&base);
    let state = base.join("state");
    std::fs::create_dir_all(&state).unwrap();
    let log = state.join("argv.log");
    let bin = base.join("docker");
    let script = format!(
        r#"#!/bin/sh
printf '%s\0' "$@" >> "{log}"
printf '\0' >> "{log}"
case "$1" in
  --version) echo "Docker version 99.0.0-fake" ;;
  create) echo "fake-container-id" ;;
  start) : ;;
  inspect)
    if [ -f "{state}/notfound" ]; then
      echo "Error: No such object: $4" >&2
      exit 1
    fi
    if [ -f "{state}/exited" ]; then
      echo "exited"
    else
      echo "running"
    fi
    ;;
  stop|rm)
    if [ -f "{state}/notfound" ]; then
      echo "Error response from daemon: No such container: $2" >&2
      exit 1
    fi
    ;;
  ps) cat "{state}/ps" 2>/dev/null ;;
  *) echo "unexpected docker argv: $*" >&2; exit 64 ;;
esac
exit 0
"#,
        log = log.display().to_string(),
        state = state.display().to_string()
    );
    let mut f = std::fs::File::create(&bin).unwrap();
    f.write_all(script.as_bytes()).unwrap();
    drop(f);
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    // This kernel intermittently fails exec with ETXTBSY when a file is
    // exec'd microseconds after its write fd closed (reproduced standalone
    // at ~3% per exec, same thread). Production execs an installed docker
    // and never sees this; only this harness writes what it runs. Settle
    // the fresh inode with a probe — after one clean exec the inode stays
    // execable (verified 2000/2000).
    for _ in 0..500 {
        match Command::new(&bin).output() {
            Ok(_) => break,
            Err(e) if e.raw_os_error() == Some(26) => {
                std::thread::sleep(std::time::Duration::from_millis(1))
            }
            Err(e) => panic!("fake docker unusable: {e}"),
        }
    }

    Fake {
        bin,
        state,
        log,
        root: base.join("library"),
    }
}

/// A realistic spec (mirror.rs shapes) with its run dir pre-created.
fn spec(root: &Path, run_id: i64) -> RsyncSpec {
    let run_dir = root
        .join("run")
        .join(format!("project-gutenberg-r{run_id}"));
    std::fs::create_dir_all(&run_dir).unwrap();
    RsyncSpec {
        source: "project-gutenberg".into(),
        run_id,
        args: vec![
            "--delete".into(),
            "-a".into(),
            "--timeout=600".into(),
            "--partial-dir=.rsync-partial".into(),
            format!("--log-file={}/itemize.log", run_dir.display()),
            "--log-file-format=%i|%n|%b".into(),
            "gutenberg.pglaf.org::gutenberg-epub/".into(),
            format!("{}/mirror/", root.display()),
        ],
        run_dir: run_dir.clone(),
    }
}

/// Split the argv log into invocations: args are NUL-separated and each
/// invocation ends with an extra NUL (an empty token). A missing log means
/// docker was never invoked.
fn invocations(log: &Path) -> Vec<Vec<String>> {
    let Ok(raw) = std::fs::read(log) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut cur = Vec::new();
    for tok in raw.split(|b| *b == 0) {
        if tok.is_empty() {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
        } else {
            cur.push(String::from_utf8(tok.to_vec()).unwrap());
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn intent() -> RunIntent {
    RunIntent {
        attempt: 1,
        host: "gutenberg.pglaf.org".into(),
        started_at: "2026-08-30T00:00:00Z".into(),
    }
}

#[test]
fn probe_runs_a_version_check() {
    let f = fake("probe");
    DockerLauncher::probe(&f.bin).unwrap();
    assert_eq!(invocations(&f.log), vec![vec!["--version".to_string()]]);
}

#[test]
fn create_argv_matches_protocol() {
    let f = fake("create-argv");
    let sp = spec(&f.root, 5);
    DockerLauncher::with_bin("fake-image:latest", f.bin.clone())
        .spawn(&sp, &intent())
        .unwrap();

    // Host-side intent exists before the container ever starts.
    assert!(sp.run_dir.join("intent.json").is_file());

    let inv = invocations(&f.log);
    assert_eq!(inv.len(), 2, "create then start");

    let run_dir = std::fs::canonicalize(&sp.run_dir).unwrap();
    let root = std::fs::canonicalize(&f.root).unwrap();
    let mut expected = vec![
        "create".to_string(),
        "--name".to_string(),
        "librarian-rsync-project-gutenberg-r5".to_string(),
        "--label".to_string(),
        "librarian.source=project-gutenberg".to_string(),
        "--label".to_string(),
        "librarian.run_id=5".to_string(),
        "--restart=no".to_string(),
        "-v".to_string(),
        format!("{}:{}:rw", root.display(), root.display()),
        "--entrypoint".to_string(),
        "sh".to_string(),
        "fake-image:latest".to_string(),
        "-c".to_string(),
        DETACHED_WRAPPER.to_string(),
        "rsync-wrapper".to_string(),
        run_dir.display().to_string(),
    ];
    expected.extend(sp.args.iter().cloned());
    assert_eq!(
        inv[0], expected,
        "create argv must match the frozen protocol"
    );
    assert_eq!(
        inv[0].iter().filter(|a| *a == "-v").count(),
        1,
        "exactly one mount"
    );
    assert_eq!(
        inv[1],
        vec![
            "start".to_string(),
            "librarian-rsync-project-gutenberg-r5".to_string()
        ]
    );
}

#[test]
fn observe_exit_file_beats_inspect() {
    let f = fake("exit-beats-inspect");
    let sp = spec(&f.root, 6);
    std::fs::write(sp.run_dir.join("exit"), "3\n").unwrap();
    // The fake would answer "running" — the exit file must win, without
    // docker even being asked.
    let l = DockerLauncher::with_bin("img", f.bin.clone());
    assert_eq!(l.observe(&sp).unwrap(), Observation::Exited(3));
    assert!(
        invocations(&f.log).is_empty(),
        "inspect must not run when exit exists"
    );
}

#[test]
fn observe_maps_inspect_statuses() {
    // Exited container without an exit file → died unreaped.
    let f = fake("dead-unreaped");
    let sp = spec(&f.root, 7);
    std::fs::write(f.state.join("exited"), "").unwrap();
    let l = DockerLauncher::with_bin("img", f.bin.clone());
    assert_eq!(l.observe(&sp).unwrap(), Observation::DeadUnreaped);

    // Missing container → Absent.
    let f = fake("absent");
    let sp = spec(&f.root, 8);
    std::fs::write(f.state.join("notfound"), "").unwrap();
    let l = DockerLauncher::with_bin("img", f.bin.clone());
    assert_eq!(l.observe(&sp).unwrap(), Observation::Absent);

    // Running container without an exit file → Live.
    let f = fake("live");
    let sp = spec(&f.root, 9);
    let l = DockerLauncher::with_bin("img", f.bin.clone());
    assert_eq!(l.observe(&sp).unwrap(), Observation::Live);
}

#[test]
fn terminate_and_reap_tolerate_missing_container() {
    let f = fake("idempotent");
    let sp = spec(&f.root, 10);
    std::fs::write(f.state.join("notfound"), "").unwrap();
    let l = DockerLauncher::with_bin("img", f.bin.clone());
    l.terminate(&sp).unwrap();
    l.reap(&sp).unwrap();
    // reap also clears the run dir.
    assert!(!sp.run_dir.exists());
}

#[test]
fn reap_orphans_sweeps_only_missing_run_dirs() {
    let f = fake("orphans");
    let kept = spec(&f.root, 1); // run dir exists → not an orphan
    std::fs::write(
        f.state.join("ps"),
        "librarian-rsync-project-gutenberg-r1\n\
         librarian-rsync-project-gutenberg-r2\n",
    )
    .unwrap();
    let swept = DockerLauncher::with_bin("img", f.bin.clone())
        .reap_orphans("project-gutenberg", &f.root.join("run"))
        .unwrap();
    assert_eq!(swept, 1);
    let inv = invocations(&f.log);
    let verbs: Vec<&str> = inv.iter().map(|i| i[0].as_str()).collect();
    assert_eq!(verbs, vec!["ps", "stop", "rm"]);
    assert_eq!(
        inv[2].last().unwrap(),
        "librarian-rsync-project-gutenberg-r2"
    );
    assert!(
        !inv.iter()
            .any(|i| i.contains(&"librarian-rsync-project-gutenberg-r1".to_string())),
        "the container with a matching run dir must be untouched"
    );
    assert!(kept.run_dir.is_dir());
}

#[test]
fn container_name_sanitizes_source() {
    assert_eq!(
        container_name("project-gutenberg", 7),
        "librarian-rsync-project-gutenberg-r7"
    );
    let name = container_name("we ird/so:urce", 1);
    assert_eq!(name, "librarian-rsync-we-ird-so-urce-r1");
    assert!(
        name.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    );
}
