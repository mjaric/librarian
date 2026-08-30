//! The gutenberg-epub mirror transport: builds durable rsync specs on the
//! supervisor ([`RsyncSpec`]) and folds finished detached runs back into
//! the cycle's [`PullResult`] ([`Mirror::finalize`]). Host rotation is the
//! connection-level pair (primary + alternate, see [`host_pair`]); the
//! retry ladder itself lives in `crate::supervisor`. Listings
//! ([`Mirror::total_books`]/[`Mirror::list_ids`]) keep the blocking
//! runner with their own host fallback.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::SOURCE_KEY;
use super::rdf::{Format, MirrorEntry, parse_mirror_name};
use crate::supervisor::RsyncSpec;
use anyhow::Context;
use bookshelf_core::{
    ExitClass, InterruptFlag, RsyncProgress, RsyncRunner, classify_exit, parse_itemize, read_intent,
};

/// Classic fallback rsync host.
pub const FALLBACK_HOST: &str = "rsync.ibiblio.org";
/// Alternate host used when the primary already is [`FALLBACK_HOST`], so
/// the retry ladder always has two distinct hosts to rotate through.
pub const ALT_HOST: &str = "gutenberg.pglaf.org";

/// `(primary, alternate)` hosts for the retry ladder and listing loops.
/// Pure and total: the two returned hosts are never equal. When the
/// configured primary already is the classic fallback
/// (`rsync.ibiblio.org`, the A/B winner), the alternate is [`ALT_HOST`];
/// any other primary alternates with [`FALLBACK_HOST`].
pub(crate) fn host_pair(primary_host: &str) -> (&str, &'static str) {
    let alternate = if primary_host == FALLBACK_HOST {
        ALT_HOST
    } else {
        FALLBACK_HOST
    };
    (primary_host, alternate)
}

/// `<source>-r<run_id>` dir name → run id (the frozen naming convention is
/// what makes specs self-describing). 0 when unparsable — progress writes
/// against run 0 affect no row, and nothing else consumes it.
fn run_id_of(run_dir: &Path) -> i64 {
    run_dir
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.rsplit_once("-r"))
        .and_then(|(_, id)| id.parse().ok())
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
pub struct MirrorTransfer {
    pub book_id: i64,
    pub entry: MirrorEntry,
    pub path: String,
    pub bytes: u64,
}

#[derive(Debug, Default)]
pub struct PullResult {
    pub transfers: Vec<MirrorTransfer>,
    pub removals: Vec<MirrorTransfer>,
    /// Ids whose `pg{id}.rdf` arrived (the ingest set).
    pub rdf_ids: Vec<i64>,
    pub rsync_exit: Option<i32>,
    pub transferred_files: i64,
    pub transferred_bytes: i64,
    pub interrupted: bool,
    /// True when the ladder was exhausted (`aborted_reason = 'rsync_failed'`).
    pub failed: bool,
    pub host_used: String,
}

pub struct Mirror {
    runner: Arc<RsyncRunner>,
    primary_host: String,
    module: String,
    dest: PathBuf,
    formats: Vec<Format>,
    interrupt: InterruptFlag,
    /// Live counters for the current pull (observability view; the durable
    /// progress lives in the run dir + sync_runs row under supervision).
    progress: RsyncProgress,
    /// Host of the most recent spec built (primary or fallback).
    host: Arc<parking_lot::Mutex<Option<String>>>,
}

/// Clonable live view of the mirror's current pull, for observability
/// consumers (the 5 s `active_run` publisher, metrics).
#[derive(Clone)]
pub struct MirrorLive {
    progress: RsyncProgress,
    host: Arc<parking_lot::Mutex<Option<String>>>,
}

impl MirrorLive {
    /// `(files, bytes, last_item_unix_ms)` of the current rsync attempt.
    pub fn snapshot(&self) -> (u64, u64, u64) {
        self.progress.snapshot()
    }

    /// Cumulative `(files, bytes)` since process start — never reset by
    /// the transfer paths; the source for the monotonic rsync counters.
    pub fn cumulative_snapshot(&self) -> (u64, u64) {
        self.progress.cumulative_snapshot()
    }

    pub fn host(&self) -> Option<String> {
        self.host.lock().clone()
    }
}

impl Mirror {
    pub fn new(
        runner: Arc<RsyncRunner>,
        primary_host: &str,
        module: &str,
        dest: PathBuf,
        formats: Vec<Format>,
        interrupt: InterruptFlag,
    ) -> Self {
        Self {
            runner,
            primary_host: primary_host.to_string(),
            module: module.to_string(),
            dest,
            formats,
            interrupt,
            progress: RsyncProgress::new(),
            host: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Clonable handle for a background publisher task.
    pub fn live(&self) -> MirrorLive {
        MirrorLive {
            progress: self.progress.clone(),
            host: self.host.clone(),
        }
    }

    /// Filter list: book dirs, rdf, and one pattern per selected format.
    fn include_args(&self) -> Vec<String> {
        let mut args = vec![
            "--include=/*/".to_string(),
            "--include=/*/pg[0-9]*.rdf".to_string(),
        ];
        for f in &self.formats {
            let pattern = match f {
                Format::Txt => "/*/pg[0-9]*.txt",
                Format::EpubImages => "/*/pg[0-9]*-images.epub",
                Format::HtmlZip => "/*/pg[0-9]*-h.zip",
                Format::Cover => "/*/pg[0-9]*.cover.medium.jpg",
            };
            args.push(format!("--include={pattern}"));
        }
        args.push("--exclude=*".to_string());
        args
    }

    /// Host for transfer attempt N: odd = primary, even = alternate.
    fn host_for_attempt(&self, attempt: u32) -> &str {
        let (primary, alternate) = host_pair(&self.primary_host);
        if attempt % 2 == 1 { primary } else { alternate }
    }

    /// Transfer flags shared by every supervised spec. Itemize goes to the
    /// run dir's log file (stdout is discarded by the detached wrapper),
    /// and `--partial-dir` keeps interrupted attempts resumable by rsync's
    /// own delta logic.
    fn transfer_args(&self, run_dir: &Path) -> Vec<String> {
        vec![
            "-a".to_string(),
            "--timeout=600".to_string(),
            "--partial-dir=.rsync-partial".to_string(),
            format!("--log-file={}/itemize.log", run_dir.display()),
            "--log-file-format=%i|%n|%b".to_string(),
        ]
    }

    /// Durable args for a FULL pull over the whole module with `--delete`.
    pub fn full_spec(&self, run_dir: &Path, attempt: u32) -> RsyncSpec {
        let host = self.host_for_attempt(attempt);
        let mut args = vec!["--delete".to_string()];
        args.extend(self.transfer_args(run_dir));
        args.extend(self.include_args());
        args.push(format!("{host}::{}/", self.module));
        args.push(format!("{}/", self.dest.display()));
        *self.host.lock() = Some(host.to_string());
        RsyncSpec {
            source: SOURCE_KEY.to_string(),
            run_id: run_id_of(run_dir),
            args,
            run_dir: run_dir.to_path_buf(),
        }
    }

    /// Durable args for a TARGETED pull of specific book dirs over one
    /// connection (`--relative` with `/./` marking the destination cut).
    pub fn targeted_spec(&self, run_dir: &Path, ids: &[i64], attempt: u32) -> RsyncSpec {
        let host = self.host_for_attempt(attempt);
        let mut args = self.transfer_args(run_dir);
        args.push("--relative".to_string());
        args.extend(self.include_args());
        for id in ids {
            args.push(format!("{host}::{}/./{id}/", self.module));
        }
        args.push(format!("{}/", self.dest.display()));
        *self.host.lock() = Some(host.to_string());
        RsyncSpec {
            source: SOURCE_KEY.to_string(),
            run_id: run_id_of(run_dir),
            args,
            run_dir: run_dir.to_path_buf(),
        }
    }

    /// Number of book dirs in the remote module (one listing connection).
    /// Falls back to the second host; 0 when unreachable.
    pub async fn total_books(&self) -> i64 {
        let (primary, alternate) = host_pair(&self.primary_host);
        for host in [primary, alternate] {
            let args = vec![
                "--list-only".to_string(),
                format!("{host}::{}/", self.module),
            ];
            let runner = self.runner.clone();
            // Listing connections produce no itemize lines; keep them off
            // the live transfer counters with a throwaway progress.
            let outcome = tokio::task::spawn_blocking(move || {
                runner.run_blocking(&args, &RsyncProgress::default())
            })
            .await
            .unwrap_or_default();
            if self.interrupt.is_set() {
                return 0;
            }
            if outcome.class() == Some(ExitClass::Ok) {
                let count = outcome
                    .stdout
                    .iter()
                    .filter(|line| {
                        let mut it = line.split_whitespace();
                        let perms = it.next().unwrap_or_default();
                        let name = it.next_back().unwrap_or_default();
                        perms.starts_with('d')
                            && name.bytes().all(|b| b.is_ascii_digit())
                            && !name.is_empty()
                    })
                    .count() as i64;
                return count;
            }
            tracing::warn!(host, code = ?outcome.code, "module listing failed");
        }
        0
    }

    /// First `n` book ids from a module listing (one connection, no file
    /// transfer). Empty on connection failure after host fallback.
    pub async fn list_ids(&self, n: usize) -> Vec<i64> {
        let (primary, alternate) = host_pair(&self.primary_host);
        for host in [primary, alternate] {
            let args = vec![
                "--list-only".to_string(),
                format!("{host}::{}/", self.module),
            ];
            let runner = self.runner.clone();
            let outcome = tokio::task::spawn_blocking(move || {
                runner.run_blocking(&args, &RsyncProgress::default())
            })
            .await
            .unwrap_or_default();
            if self.interrupt.is_set() {
                return Vec::new();
            }
            if outcome.class() == Some(ExitClass::Ok) {
                let mut ids = BTreeSet::new();
                for line in &outcome.stdout {
                    // drwxr-xr-x   4096 2026/08/27 14:22 1342
                    let mut it = line.split_whitespace();
                    let perms = it.next().unwrap_or_default();
                    let name = it.next_back().unwrap_or_default();
                    if perms.starts_with('d')
                        && name.bytes().all(|b| b.is_ascii_digit())
                        && !name.is_empty()
                    {
                        if let Ok(id) = name.parse::<i64>() {
                            ids.insert(id);
                        }
                    }
                    if ids.len() >= n {
                        break;
                    }
                }
                return ids.into_iter().take(n).collect();
            }
            tracing::warn!(host, code = ?outcome.code, stderr = %outcome.stderr, "module listing failed");
        }
        Vec::new()
    }

    /// Fold a finished detached run into the cycle's PullResult: the run
    /// dir's whole `itemize.log` maps to transfers/removals/rdf ids and
    /// counters (rsync appends across ladder attempts, so one pass over
    /// the file is the run's true tally); `host_used` comes from the
    /// recorded intent; `interrupted` is rsync's aborted-by-signal code.
    ///
    /// `failed` reflects only the passed `code` (Fatal). Ladder
    /// exhaustion is recorded by the CALLER — `supervise` reports it as
    /// `SuperviseOutcome::Failed`, and the cycle arms the flag there (the
    /// last retryable exit code alone would read as recoverable).
    pub fn finalize(&self, run_dir: &Path, code: i32) -> anyhow::Result<PullResult> {
        let mut result = PullResult {
            rsync_exit: Some(code),
            host_used: read_intent(run_dir)?
                .map(|intent| intent.host)
                .unwrap_or_default(),
            interrupted: code == 20,
            failed: classify_exit(code) == ExitClass::Fatal,
            ..Default::default()
        };
        let raw = match std::fs::read_to_string(run_dir.join("itemize.log")) {
            Ok(raw) => raw,
            // Nothing itemized (e.g. nothing to transfer): a valid outcome.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(result),
            Err(e) => {
                return Err(e).context(format!("reading itemize log in {}", run_dir.display()));
            }
        };
        for line in raw.lines() {
            let Some(item) = parse_itemize(line) else {
                // `--log-file` also records bookkeeping (`building file
                // list`, the `sent N bytes` summary) without out-format
                // shape — expected, not a parse failure.
                if line.contains('|') {
                    tracing::warn!(line = %line, "unparseable rsync line");
                }
                continue;
            };
            let Some((book_id, entry)) = parse_mirror_name(&item.name) else {
                // filter leftovers (LICENSE.txt, README.md, …) — harmless
                continue;
            };
            if item.is_deletion() {
                result.removals.push(MirrorTransfer {
                    book_id,
                    entry,
                    path: item.name.clone(),
                    bytes: 0,
                });
            } else if item.is_file_transfer() {
                result.transferred_files += 1;
                result.transferred_bytes += item.bytes as i64;
                match entry {
                    MirrorEntry::Rdf => {
                        result.rdf_ids.push(book_id);
                    }
                    MirrorEntry::Format(_) => {
                        result.transfers.push(MirrorTransfer {
                            book_id,
                            entry,
                            path: item.name.clone(),
                            bytes: item.bytes,
                        });
                    }
                }
            }
        }
        Ok(result)
    }
}

impl PullResult {
    /// Fold another pull's tally into this one (per-id escalation).
    pub(crate) fn merge(&mut self, other: PullResult) {
        self.transfers.extend(other.transfers);
        self.removals.extend(other.removals);
        self.rdf_ids.extend(other.rdf_ids);
        self.transferred_files += other.transferred_files;
        self.transferred_bytes += other.transferred_bytes;
        self.rsync_exit = other.rsync_exit.or(self.rsync_exit);
        self.interrupted |= other.interrupted;
        self.failed |= other.failed;
    }

    /// Deduped, sorted ingest set.
    pub fn ingest_ids(&self) -> Vec<i64> {
        let set: BTreeSet<i64> = self.rdf_ids.iter().copied().collect();
        set.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bookshelf_core::{InterruptFlag, RunIntent, write_intent};

    #[test]
    fn primary_fallback_gets_pglaf_alternate() {
        // The A/B winner as primary must NOT pair with itself four times.
        assert_eq!(host_pair(FALLBACK_HOST), (FALLBACK_HOST, ALT_HOST));
    }

    #[test]
    fn primary_pglaf_gets_fallback_alternate() {
        assert_eq!(host_pair(ALT_HOST), (ALT_HOST, FALLBACK_HOST));
    }

    #[test]
    fn any_other_primary_keeps_fallback_alternate() {
        assert_eq!(
            host_pair("mirror.example.org"),
            ("mirror.example.org", FALLBACK_HOST)
        );
    }

    #[test]
    fn run_id_parsed_from_frozen_dir_name() {
        assert_eq!(
            run_id_of(Path::new("/tmp/run/project-gutenberg-r1234")),
            1234
        );
        assert_eq!(run_id_of(Path::new("/tmp/run/garbage")), 0);
    }

    /// rsync must be on PATH for the runner probe — skip vacuously where
    /// it is absent (the daemon tier requires it anyway).
    fn test_mirror(dest: &Path) -> Option<Mirror> {
        let Ok(runner) = RsyncRunner::new(InterruptFlag::new()) else {
            eprintln!("SKIP: rsync not on PATH");
            return None;
        };
        Some(Mirror::new(
            Arc::new(runner),
            ALT_HOST,
            "gutenberg-epub",
            dest.to_path_buf(),
            vec![Format::Txt, Format::EpubImages],
            InterruptFlag::new(),
        ))
    }

    #[test]
    fn full_spec_carries_delete_log_and_parity_hosts() {
        let Some(mirror) = test_mirror(&std::env::temp_dir().join("bookshelf-mirror-spec")) else {
            return;
        };
        let run_dir = std::env::temp_dir().join("bookshelf-mirror-run-r7");
        let spec = mirror.full_spec(&run_dir, 1);
        assert_eq!(spec.run_id, 7);
        assert_eq!(spec.source, SOURCE_KEY);
        assert_eq!(spec.run_dir, run_dir);
        assert!(spec.args.iter().any(|a| a == "--delete"));
        assert!(
            spec.args
                .iter()
                .any(|a| a == &format!("--log-file={}/itemize.log", run_dir.display()))
        );
        assert!(spec.args.iter().any(|a| a == "--log-file-format=%i|%n|%b"));
        assert!(
            spec.args
                .iter()
                .any(|a| a == "--partial-dir=.rsync-partial")
        );
        assert!(!spec.args.iter().any(|a| a == "--itemize-changes"));
        assert!(
            spec.args
                .iter()
                .any(|a| a.contains("gutenberg-epub/") && a.ends_with("::gutenberg-epub/"))
        );
        assert!(spec.args.contains(&format!("{ALT_HOST}::gutenberg-epub/")));
        // Even attempt → the alternate host.
        let spec = mirror.full_spec(&run_dir, 2);
        assert!(
            spec.args
                .contains(&format!("{FALLBACK_HOST}::gutenberg-epub/"))
        );
    }

    #[test]
    fn targeted_spec_is_relative_with_one_source_per_id() {
        let Some(mirror) = test_mirror(&std::env::temp_dir().join("bookshelf-mirror-spec")) else {
            return;
        };
        let run_dir = std::env::temp_dir().join("bookshelf-mirror-run-r9");
        let spec = mirror.targeted_spec(&run_dir, &[1342, 51564], 1);
        assert_eq!(spec.run_id, 9);
        assert!(spec.args.iter().any(|a| a == "--relative"));
        assert!(
            spec.args
                .contains(&format!("{ALT_HOST}::gutenberg-epub/./1342/"))
        );
        assert!(
            spec.args
                .contains(&format!("{ALT_HOST}::gutenberg-epub/./51564/"))
        );
    }

    #[test]
    fn finalize_maps_itemize_log_to_pull_result() {
        let Some(mirror) = test_mirror(&std::env::temp_dir().join("bookshelf-mirror-finalize"))
        else {
            return;
        };
        let run_dir = std::env::temp_dir().join("bookshelf-mirror-finalize-r42");
        let _ = std::fs::remove_dir_all(&run_dir);
        std::fs::create_dir_all(&run_dir).unwrap();
        // Same shapes as tests/rsync_parse.rs.
        std::fs::write(
            run_dir.join("itemize.log"),
            "<f.st......|1342/pg1342-images.epub|24846294\n\
             >f+++++++++|51564/pg51564.rdf|18220\n\
             *deleting|1342/pg1342-h.zip|0\n",
        )
        .unwrap();
        write_intent(
            &run_dir,
            &RunIntent {
                attempt: 1,
                host: ALT_HOST.to_string(),
                started_at: "2026-08-29T00:00:00Z".into(),
            },
        )
        .unwrap();

        let pull = mirror.finalize(&run_dir, 0).unwrap();
        assert_eq!(pull.rsync_exit, Some(0));
        assert_eq!(pull.host_used, ALT_HOST);
        assert!(!pull.interrupted);
        assert!(!pull.failed);
        assert_eq!(pull.transferred_files, 2);
        assert_eq!(pull.transferred_bytes, 24846294 + 18220);
        assert_eq!(pull.ingest_ids(), vec![51564]);
        assert_eq!(pull.transfers.len(), 1);
        assert_eq!(pull.transfers[0].book_id, 1342);
        assert_eq!(pull.removals.len(), 1);

        // Exit 20 marks the interrupted run AND arms `failed` (20 is in
        // the Fatal class — the Interrupted mapping clears it at the
        // caller); other fatal codes arm `failed` alone.
        let pull = mirror.finalize(&run_dir, 20).unwrap();
        assert!(pull.interrupted);
        assert!(pull.failed);
        let pull = mirror.finalize(&run_dir, 255).unwrap();
        assert!(pull.failed);
        let _ = std::fs::remove_dir_all(&run_dir);
    }
}
