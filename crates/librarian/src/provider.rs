//! The provider abstraction: one trait, one registry. A second source later
//! is a new module + one registry entry (graduate to its own crate only when
//! it outgrows the binary).

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Per-cycle knobs (CLI flags → job payload → here).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CycleOpts {
    #[serde(default)]
    pub feed: bool,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub only: Vec<i64>,
    #[serde(default)]
    pub no_ingest: bool,
}

#[derive(Debug, Clone, Default)]
pub struct CycleReport {
    pub run_id: i64,
    pub new_books: i64,
    pub enriched: i64,
    pub transferred_files: i64,
    pub transferred_bytes: i64,
    pub aborted_reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RepairReport {
    pub run_id: i64,
    pub repaired: i64,
    pub skipped: i64,
    pub failed: i64,
    pub deferred: i64,
    pub aborted_reason: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct StatusReport {
    pub books_by_status: Vec<(String, i64)>,
    pub categories: i64,
    pub mirror_files: u64,
    pub mirror_bytes: u64,
    pub repair_pending: i64,
    pub repair_failed: i64,
    pub min_retry_at: Option<String>,
    pub last_run: Option<String>,
    pub next_full_sync: Option<String>,
    pub next_feed_check: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ProgressReport {
    pub source: String,
    /// Books available on the remote mirror (live catalog listing).
    pub total_remote: i64,
    /// Books ingested into the local DB (any status).
    pub ingested: i64,
    pub synced: i64,
    pub enriched: i64,
    pub discovered: i64,
    pub failed_permanent: i64,
    pub mirror_files: u64,
    pub mirror_bytes: u64,
}

impl ProgressReport {
    /// Share of the remote catalog present locally, 0.0..1.0.
    pub fn fraction(&self) -> f64 {
        if self.total_remote <= 0 {
            0.0
        } else {
            self.ingested as f64 / self.total_remote as f64
        }
    }
}

#[async_trait]
pub trait Provider: Send + Sync {
    /// Stable source key, e.g. "project-gutenberg".
    fn key(&self) -> &'static str;

    /// Weekly full rsync delta pull + ingest (targeted when `only`/`limit`).
    async fn full_cycle(&self, opts: CycleOpts) -> anyhow::Result<CycleReport>;

    /// Daily light check of the official today.rss feed.
    async fn feed_cycle(&self) -> anyhow::Result<CycleReport>;

    /// HTTP gap-fill for pending/size-mismatch files.
    async fn repair(&self, only: Option<&[i64]>) -> anyhow::Result<RepairReport>;

    async fn status(&self) -> anyhow::Result<StatusReport>;

    /// failed_permanent books → discovered. Returns the reset count.
    async fn retry_failed(&self) -> anyhow::Result<u64>;

    /// Downloaded vs total per source (live remote catalog + local DB).
    async fn progress(&self) -> anyhow::Result<ProgressReport>;
}

/// Provider keys compiled into this binary — client-side validation so
/// enqueue-only subcommands need neither rsync nor an event log.
pub fn known_keys() -> &'static [&'static str] {
    &[crate::gutenberg_org::SOURCE_KEY]
}

pub fn ensure_known_key(key: &str) -> anyhow::Result<()> {
    if known_keys().contains(&key) {
        Ok(())
    } else {
        let available = known_keys();
        anyhow::bail!("unknown provider {key:?} (available: {available:?})")
    }
}

/// Resolve a provider key against the registry; unknown keys list the options.
pub fn resolve<'a>(
    providers: &'a [Arc<dyn Provider>],
    key: &str,
) -> anyhow::Result<&'a Arc<dyn Provider>> {
    providers
        .iter()
        .find(|p| p.key() == key)
        .ok_or_else(|| {
            let keys: Vec<&str> = providers.iter().map(|p| p.key()).collect();
            anyhow::anyhow!("unknown provider {key:?} (available: {keys:?})")
        })
}
