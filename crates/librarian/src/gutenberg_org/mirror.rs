//! The gutenberg-epub mirror transport: builds rsync invocations on the
//! shared [`RsyncRunner`], maps `%i|%n|%b` lines to `(book_id, format)`,
//! applies the rsync retry ladder (+5 min / +10 min / +1 h) and the
//! connection-level fallback host (`rsync.ibiblio.org`).

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use super::rdf::{Format, MirrorEntry, parse_mirror_name};
use bookshelf_core::{ExitClass, InterruptFlag, RsyncRunner, parse_itemize};

pub const FALLBACK_HOST: &str = "rsync.ibiblio.org";

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

    fn base_args(&self, relative: bool) -> Vec<String> {
        let mut args = vec![
            "-a".to_string(),
            "--timeout=600".to_string(),
            "--itemize-changes".to_string(),
            "--out-format=%i|%n|%b".to_string(),
        ];
        if relative {
            args.push("--relative".to_string());
        }
        args.extend(self.include_args());
        args
    }

    fn full_args(&self, host: &str) -> Vec<String> {
        let mut args = vec![
            "-a".to_string(),
            "--delete".to_string(),
            "--timeout=600".to_string(),
            "--itemize-changes".to_string(),
            "--out-format=%i|%n|%b".to_string(),
        ];
        args.extend(self.include_args());
        args.push(format!("{host}::{}/", self.module));
        args.push(format!("{}/", self.dest.display()));
        args
    }

    fn targeted_args(&self, host: &str, ids: &[i64]) -> Vec<String> {
        let mut args = self.base_args(true);
        for id in ids {
            args.push(format!("{host}::{}/./{id}/", self.module));
        }
        args.push(format!("{}/", self.dest.display()));
        args
    }

    /// Weekly/first full pull over the whole module with `--delete`.
    pub async fn full_pull(&self) -> PullResult {
        let primary = self.full_args(&self.primary_host);
        let fallback = self.full_args(FALLBACK_HOST);
        self.run_ladder(vec![primary.clone(), fallback.clone(), primary, fallback])
            .await
    }

    /// Targeted pull of specific book dirs over one connection
    /// (`--relative` with `/./` marking the destination cut). Falls back to
    /// one invocation per id when the daemon module mishandles multi-source.
    pub async fn targeted_pull(&self, ids: &[i64]) -> PullResult {
        let mut result = self.targeted_pull_batch(ids).await;
        if result.failed && ids.len() > 1 {
            tracing::warn!("multi-source --relative pull failed; retrying per id");
            let mut combined = PullResult {
                host_used: result.host_used.clone(),
                ..Default::default()
            };
            let mut any_ok = false;
            for id in ids {
                if self.interrupt.is_set() {
                    combined.interrupted = true;
                    break;
                }
                let one = self.targeted_pull_batch(&[*id]).await;
                if one.failed {
                    continue;
                }
                any_ok = true;
                combined.merge(one);
            }
            if any_ok || !combined.transfers.is_empty() {
                combined.failed = false;
                result = combined;
            }
        }
        result
    }

    /// Number of book dirs in the remote module (one listing connection).
    /// Falls back to the second host; 0 when unreachable.
    pub async fn total_books(&self) -> i64 {
        for host in [self.primary_host.as_str(), FALLBACK_HOST] {
            let args = vec![
                "--list-only".to_string(),
                format!("{host}::{}/", self.module),
            ];
            let runner = self.runner.clone();
            let outcome = tokio::task::spawn_blocking(move || runner.run_blocking(&args))
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

    /// One batched `--relative` invocation ladder (no per-id escalation).
    async fn targeted_pull_batch(&self, ids: &[i64]) -> PullResult {
        let primary = self.targeted_args(&self.primary_host, ids);
        let fallback = self.targeted_args(FALLBACK_HOST, ids);
        self.run_ladder(vec![primary.clone(), fallback.clone(), primary, fallback])
            .await
    }

    /// First `n` book ids from a module listing (one connection, no file
    /// transfer). Empty on connection failure after host fallback.
    pub async fn list_ids(&self, n: usize) -> Vec<i64> {
        for host in [self.primary_host.as_str(), FALLBACK_HOST] {
            let args = vec![
                "--list-only".to_string(),
                format!("{host}::{}/", self.module),
            ];
            let runner = self.runner.clone();
            let outcome = tokio::task::spawn_blocking(move || runner.run_blocking(&args))
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

    /// Run the attempt sequence with the ladder delays (+5 min, +10 min,
    /// +1 h) between retries; Ok/Partial stops the ladder, Fatal aborts
    /// immediately (the caller marks `rsync_failed` only for retryable
    /// exhaustion; Fatal outcomes still report `failed`).
    async fn run_ladder(&self, attempts: Vec<Vec<String>>) -> PullResult {
        let delays = [
            Duration::ZERO,
            Duration::from_secs(5 * 60),
            Duration::from_secs(10 * 60),
            Duration::from_secs(60 * 60),
        ];
        let mut result = PullResult::default();
        let hosts = [self.primary_host.as_str(), FALLBACK_HOST];

        for (i, args) in attempts.into_iter().enumerate() {
            if i > 0 {
                tracing::warn!(retry_in = ?delays[i], "rsync retry ladder");
                tokio::time::sleep(delays[i]).await;
            }
            if self.interrupt.is_set() {
                result.interrupted = true;
                return result;
            }
            result.host_used = hosts[i % 2].to_string();
            let runner = self.runner.clone();
            let outcome = tokio::task::spawn_blocking(move || runner.run_blocking(&args))
                .await
                .unwrap_or_default();
            result.rsync_exit = outcome.code.or(result.rsync_exit);
            if outcome.interrupted {
                result.interrupted = true;
                return result;
            }
            match outcome.class() {
                Some(ExitClass::Ok) | Some(ExitClass::Partial) => {
                    self.absorb(&mut result, &outcome);
                    if outcome.class() == Some(ExitClass::Partial) {
                        tracing::warn!(
                            "rsync partial transfer (files vanished upstream); keeping what arrived"
                        );
                    }
                    return result;
                }
                Some(ExitClass::Fatal) => {
                    tracing::error!(code = ?outcome.code, stderr = %outcome.stderr, "rsync fatal error — aborting run");
                    self.absorb(&mut result, &outcome);
                    result.failed = true;
                    return result;
                }
                _ => {
                    // Retryable (5/6/10/11) or spawn failure: next attempt.
                    tracing::warn!(code = ?outcome.code, spawn_error = ?outcome.spawn_error, stderr = %outcome.stderr, "rsync retryable failure");
                    self.absorb(&mut result, &outcome);
                }
            }
        }
        result.failed = true;
        result
    }

    /// Fold an outcome's itemize lines into the result.
    fn absorb(&self, result: &mut PullResult, outcome: &bookshelf_core::RsyncOutcome) {
        for raw in &outcome.stdout {
            let Some(line) = parse_itemize(raw) else {
                tracing::warn!(line = %raw, "unparseable rsync line");
                continue;
            };
            let Some((book_id, entry)) = parse_mirror_name(&line.name) else {
                // filter leftovers (LICENSE.txt, README.md, …) — harmless
                continue;
            };
            if line.is_deletion() {
                result.removals.push(MirrorTransfer {
                    book_id,
                    entry,
                    path: line.name.clone(),
                    bytes: 0,
                });
            } else if line.is_file_transfer() {
                result.transferred_files += 1;
                result.transferred_bytes += line.bytes as i64;
                match entry {
                    MirrorEntry::Rdf => {
                        result.rdf_ids.push(book_id);
                    }
                    MirrorEntry::Format(_) => {
                        result.transfers.push(MirrorTransfer {
                            book_id,
                            entry,
                            path: line.name.clone(),
                            bytes: line.bytes,
                        });
                    }
                }
            }
        }
    }
}

impl PullResult {
    fn merge(&mut self, other: PullResult) {
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
