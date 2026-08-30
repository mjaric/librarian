//! The real `ProcessLauncher` over a REAL detached rsync (local paths, no
//! network): spawn → durable exit record → itemize.log written via the
//! spec's `--log-file` → reap. Skips when rsync is absent (the daemon tier
//! requires it anyway).

use std::time::Duration;

use bookshelf_core::{InterruptFlag, RunIntent, read_exit};
use librarian::supervisor::{ProcessLauncher, RsyncSpec, SyncLauncher};

fn rsync_on_path() -> bool {
    std::process::Command::new("rsync")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .status()
        .is_ok()
}

#[test]
fn detached_rsync_runs_observes_and_reaps() {
    if !rsync_on_path() {
        eprintln!("SKIP: rsync not on PATH");
        return;
    }
    let base = std::env::temp_dir().join(format!("bookshelf-detached-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let src = base.join("src");
    let dest = base.join("dest");
    let run_dir = base.join("run/project-gutenberg-r1");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("pg1.txt"), "hello, detached world\n").unwrap();

    let spec = RsyncSpec {
        source: "project-gutenberg".into(),
        run_id: 1,
        args: vec![
            "-a".into(),
            "--timeout=30".into(),
            format!("--log-file={}/itemize.log", run_dir.display()),
            "--log-file-format=%i|%n|%b".into(),
            format!("{}/", src.display()),
            format!("{}/", dest.display()),
        ],
        run_dir: run_dir.clone(),
    };

    let launcher = ProcessLauncher;
    launcher
        .spawn(
            &spec,
            &RunIntent {
                attempt: 1,
                host: "local".into(),
                started_at: "2026-08-29T00:00:00Z".into(),
            },
        )
        .unwrap();

    // The detached wrapper records its pid witness and runs rsync. Observe
    // exactly what `supervise` observes: poll the exit file. (A local copy
    // this small can finish between two polls, so `Live` itself is not
    // asserted here — run_is_live is core's own tested surface.)
    let mut code = None;
    for _ in 0..100 {
        if let Some(c) = read_exit(&run_dir).unwrap() {
            code = Some(c);
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(code, Some(0), "detached run must record a clean exit");

    // The caller-passed --log-file produced parseable itemize output.
    // Parse the REAL log (rsync prefixes log-file lines with a timestamp
    // + pid) the same way `Mirror::finalize` and `itemize_delta` will.
    let itemize = std::fs::read_to_string(run_dir.join("itemize.log")).unwrap();
    let parsed: Vec<_> = itemize
        .lines()
        .filter_map(bookshelf_core::parse_itemize)
        .collect();
    assert!(
        parsed
            .iter()
            .any(|l| l.is_file_transfer() && l.name.ends_with("pg1.txt") && l.bytes > 0),
        "itemize.log must yield the transferred file through parse_itemize, got: {parsed:?}"
    );

    // And the reap is final.
    launcher.reap(&spec).unwrap();
    assert!(!run_dir.exists());
    let _ = std::fs::remove_dir_all(&base);

    // InterruptFlag import anchor: the supervisor contract takes the flag
    // even though this wiring test never sets it.
    let _ = InterruptFlag::new();
}
