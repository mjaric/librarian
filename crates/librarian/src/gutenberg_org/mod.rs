//! The `gutenberg_org` provider: weekly full rsync delta + daily today.rss
//! feed + RDF ingest from the local mirror + polite HTTP repair. Source key
//! is the constant `project-gutenberg`, written into every DB row and event
//! line (single identity per provider — not a config key).

pub mod feed;
pub mod mirror;
pub mod rdf;
pub mod taxonomy;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use bookshelf_core::triage_rules;
use bookshelf_core::{FetchError, InterruptFlag, PoliteClient, RsyncRunner, StorePostgres};
use async_trait::async_trait;
use bookshelf_core::domain::{EventKind, EventSink, Json, Triage};
use time::OffsetDateTime;

use crate::config::{Config, TriageMode};
use crate::provider::{CycleOpts, CycleReport, ProgressReport, Provider, RepairReport, StatusReport};
use mirror::{Mirror, PullResult};
use rdf::MirrorEntry;

pub const SOURCE_KEY: &str = "project-gutenberg";

pub struct GutenbergOrg {
    cfg: Arc<Config>,
    store: Arc<StorePostgres>,
    events: Arc<dyn EventSink>,
    http: Arc<PoliteClient>,
    mirror: Mirror,
    interrupt: InterruptFlag,
    triage: Option<Arc<dyn Triage>>,
}
impl GutenbergOrg {
    pub fn new(
        cfg: Arc<Config>,
        store: Arc<StorePostgres>,
        events: Arc<dyn EventSink>,
        interrupt: InterruptFlag,
    ) -> anyhow::Result<Self> {
        let http = PoliteClient::new(
            &cfg.user_agent(),
            Duration::from_secs(cfg.timeout_secs),
            Duration::from_millis(cfg.request_interval_ms),
            cfg.max_parallel_downloads,
        )?;
        let runner = Arc::new(RsyncRunner::new(interrupt.clone())?);
        let mirror = Mirror::new(
            runner,
            &cfg.rsync_host,
            &cfg.rsync_module,
            cfg.mirror_dir(),
            cfg.formats.clone(),
            interrupt.clone(),
        );
        let (triage, agent_desc) = match cfg.triage {
            TriageMode::Rules => (None, None),
            TriageMode::Agent => {
                #[cfg(feature = "agent")]
                {
                    match bookshelf_core::adapters::triage_agent::by_provider(
                        &cfg.agent_provider,
                        &cfg.agent_model,
                    ) {
                        Ok(a) => (
                            Some(Arc::from(a)),
                            Some(format!("{}/{}", cfg.agent_provider, cfg.agent_model)),
                        ),
                        Err(e) => {
                            tracing::warn!(error = %e, "agent triage unavailable — using deterministic rules");
                            (None, None)
                        }
                    }
                }
                #[cfg(not(feature = "agent"))]
                {
                    tracing::warn!("triage=\"agent\" configured but the agent feature is off — using rules");
                    (None, None)
                }
            }
        };
        if let Some(desc) = &agent_desc {
            tracing::info!(agent = %desc, "LLM triage agent enabled");
        }
        Ok(Self {
            cfg,
            store,
            events,
            http: Arc::new(http),
            mirror,
            interrupt,
            triage,
        })
    }

    fn ev(&self, kind: EventKind, book_id: Option<i64>, detail: Json) {
        let sink = self.events.clone();
        tokio::spawn(async move {
            sink.emit(SOURCE_KEY, kind, book_id, detail).await;
        });
    }

    /// await-able event emit for paths where ordering matters little but
    /// flush-before-exit does (feed.checked, taxonomy.updated).
    async fn ev_now(&self, kind: EventKind, book_id: Option<i64>, detail: Json) {
        self.events.emit(SOURCE_KEY, kind, book_id, detail).await;
    }

    /// Local scan of `mirror/*/pg*.rdf` (first run ingest set).
    async fn scan_mirror_rdfs(&self) -> Vec<i64> {
        let dir = self.cfg.mirror_dir();
        let mut ids = Vec::new();
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(_) => return ids,
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !name.bytes().all(|b| b.is_ascii_digit()) || name.is_empty() {
                continue;
            }
            if entry.path().join(format!("pg{name}.rdf")).is_file() {
                ids.push(name.parse().unwrap_or(0));
            }
        }
        ids.sort_unstable();
        ids
    }

    /// Events + book_files updates for a completed pull.
    async fn record_pull(&self, pull: &PullResult) {
        for t in &pull.transfers {
            if let MirrorEntry::Format(f) = t.entry {
                self.ev(
                    EventKind::FileTransferred,
                    Some(t.book_id),
                    serde_json::json!({
                        "format": f.key(),
                        "path": format!("mirror/{}", t.path),
                        "bytes": t.bytes,
                    }),
                );
                // No-op when the book is not ingested yet (ingest will stat
                // the mirror and mark the file done itself).
                let rel = format!("mirror/{}", t.path);
                let _ = self
                    .store
                    .set_file_done(SOURCE_KEY, t.book_id, f.key(), &rel, None)
                    .await;
            }
        }
        for r in &pull.removals {
            if let MirrorEntry::Format(f) = r.entry {
                self.ev(
                    EventKind::FileRemoved,
                    Some(r.book_id),
                    serde_json::json!({
                        "format": f.key(),
                        "path": format!("mirror/{}", r.path),
                    }),
                );
            }
        }
        if !pull.host_used.is_empty() {
            let host = pull.host_used.clone();
            let _ = self.store.set_meta(SOURCE_KEY, "last_rsync_host", &host).await;
        }
    }

    /// Ingest RDFs for the given ids. Returns (new_books, enriched).
    /// Deviation from plan: progress is logged per 500 books instead of
    /// wrapping each 500 in one transaction — the per-statement pool already
    /// keeps throughput comparable and the store API stays boring.
    async fn ingest(&self, ids: Vec<i64>, report: &mut CycleReport) -> anyhow::Result<(i64, i64)> {
        let mut new_leaves: Vec<String> = Vec::new();
        let mut new_books = 0i64;
        let mut enriched = 0i64;
        let total = ids.len();
        for (n, id) in ids.into_iter().enumerate() {
            if self.interrupt.is_set() {
                break;
            }
            if n % 500 == 0 {
                tracing::info!(ingested = n, total, "ingest progress");
            }
            match self.ingest_one(id, &mut new_leaves).await {
                Ok(Some(true)) => {
                    new_books += 1;
                    enriched += 1;
                }
                Ok(Some(false)) => enriched += 1,
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(id, error = %e, "ingest failed for book");
                }
            }
        }
        report.new_books = new_books;
        report.enriched = enriched;
        if !new_leaves.is_empty() {
            self.ev_now(
                EventKind::TaxonomyUpdated,
                None,
                serde_json::json!({ "new_leaves": new_leaves }),
            )
            .await;
        }
        Ok((new_books, enriched))
    }

    /// Returns Some(is_new) when the book was ingested, None when skipped.
    async fn ingest_one(&self, id: i64, new_leaves: &mut Vec<String>) -> anyhow::Result<Option<bool>> {
        let rdf_path = self
            .cfg
            .mirror_dir()
            .join(id.to_string())
            .join(format!("pg{id}.rdf"));
        let xml = match tokio::fs::read_to_string(&rdf_path).await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(id, error = %e, "no rdf in mirror for listed id");
                return Ok(None);
            }
        };
        let parsed = rdf::parse_rdf(&xml)?;

        let authors = serde_json::to_value(&parsed.authors)?;
        let subjects = serde_json::to_value(&parsed.subjects)?;
        let bookshelves = serde_json::to_value(&parsed.bookshelves)?;
        let new_book = bookshelf_core::adapters::store_postgres::NewBook {
            source: SOURCE_KEY,
            id,
            r#type: &parsed.r#type,
            title: &parsed.title,
            language: &parsed.language,
            issued: parsed.issued_date(),
            publisher: parsed.publisher.as_deref(),
            rights: parsed.rights.as_deref(),
            description: parsed.description.as_deref(),
            reading_ease: parsed.reading_ease.as_deref(),
            downloads: parsed.downloads.map(|d| d as i32),
            authors: &authors,
            subjects: &subjects,
            bookshelves: &bookshelves,
            status: "discovered",
        };

        let existing = self.store.get_book(SOURCE_KEY, id).await?;
        match &existing {
            None => {
                self.store.insert_book(&new_book).await?;
                self.ev(
                    EventKind::BookDiscovered,
                    Some(id),
                    serde_json::json!({ "title": parsed.title }),
                );
            }
            Some(prev) => {
                let changed = self.changed_fields(prev, &parsed);
                if !changed.is_empty() {
                    self.store.update_book_fields(&new_book).await?;
                    self.ev(
                        EventKind::BookMetadataUpdated,
                        Some(id),
                        serde_json::json!({ "fields": changed }),
                    );
                }
            }
        }

        // -- files per configured format
        let mut files_detail: Vec<&'static str> = Vec::new();
        for f in self.cfg.formats.iter().copied() {
            let entry = parsed.format_entry(f);
            let rel = format!("mirror/{}/{}", id, f.mirror_name(id));
            let abs = self.cfg.library_dir.join(&rel);
            let local = tokio::fs::metadata(&abs).await.ok();
            match (entry, local) {
                (Some(e), Some(_meta)) => {
                    self.store
                        .upsert_file_knowledge(
                            SOURCE_KEY,
                            id,
                            f.key(),
                            Some(&e.url),
                            e.extent,
                            e.modified.as_deref(),
                        )
                        .await?;
                    // The mirror is the source of truth for rsync-delivered
                    // files: RDF `extent` describes the HTTP pretty-URL body
                    // and differs by a few bytes from mirror copies (verified:
                    // pg1342 epub.images mirror 24846294 vs extent 24846290).
                    // Extent is enforced only on the HTTP repair path.
                    self.store
                        .set_file_done(SOURCE_KEY, id, f.key(), &rel, None)
                        .await?;
                    files_detail.push(f.key());
                }
                (None, None) => {
                    self.store
                        .upsert_file_knowledge(SOURCE_KEY, id, f.key(), None, None, None)
                        .await?;
                    // emit file.skipped only on the first observation
                    let existing_row = self
                        .store
                        .get_files(SOURCE_KEY, id)
                        .await?
                        .into_iter()
                        .find(|r| r.format == f.key());
                    let first = existing_row.map(|r| r.status == "pending").unwrap_or(true);
                    self.store
                        .set_file_state(
                            SOURCE_KEY,
                            id,
                            f.key(),
                            "skipped",
                            None,
                            None,
                            None,
                            Some("absent-in-rdf"),
                        )
                        .await?;
                    if first {
                        self.ev(
                            EventKind::FileSkipped,
                            Some(id),
                            serde_json::json!({ "format": f.key(), "reason": "absent-in-rdf" }),
                        );
                    }
                }
                (Some(e), None) => {
                    // RDF lists it, mirror lacks it → repair queue.
                    self.store
                        .upsert_file_knowledge(
                            SOURCE_KEY,
                            id,
                            f.key(),
                            Some(&e.url),
                            e.extent,
                            e.modified.as_deref(),
                        )
                        .await?;
                    self.store
                        .set_file_state(
                            SOURCE_KEY, id, f.key(), "pending", None, None, None, None,
                        )
                        .await?;
                }
                (None, Some(_)) => {
                    // Mirrored but RDF has no entry (no url, no extent):
                    // keep what we have; nothing to verify against.
                    self.store
                        .set_file_done(SOURCE_KEY, id, f.key(), &rel, None)
                        .await?;
                    files_detail.push(f.key());
                }
            }
        }

        // -- category reconcile
        let leaves = taxonomy::category_leaves(&parsed.bookshelves);
        for leaf in &leaves {
            let existing_cat = self.store.get_category(SOURCE_KEY, leaf).await?;
            let parent = match existing_cat {
                Some(c) => c.parent,
                None => {
                    let created = self.store.upsert_category_leaf(SOURCE_KEY, leaf).await?;
                    if created {
                        new_leaves.push((*leaf).to_string());
                    }
                    self.store.get_category(SOURCE_KEY, leaf).await?.and_then(|c| c.parent)
                }
            };
            if self.store.link_category(SOURCE_KEY, id, leaf).await? {
                self.ev(
                    EventKind::BookCategoryAdded,
                    Some(id),
                    serde_json::json!({ "category": leaf, "parent": parent }),
                );
            }
        }
        let current: HashSet<&str> = leaves.iter().copied().collect();
        for gone in self.store.book_categories(SOURCE_KEY, id).await? {
            if !current.contains(gone.as_str()) {
                if self.store.unlink_category(SOURCE_KEY, id, &gone).await? {
                    self.ev(
                        EventKind::BookCategoryRemoved,
                        Some(id),
                        serde_json::json!({ "category": gone }),
                    );
                }
            }
        }

        // -- status recompute + sidecar
        let is_new = existing.is_none();
        if is_new {
            self.ev(
                EventKind::BookEnriched,
                Some(id),
                serde_json::json!({ "files": files_detail }),
            );
        }
        self.recompute_book_status(id, is_new).await?;
        Ok(Some(is_new))
    }

    fn changed_fields(
        &self,
        prev: &bookshelf_core::domain::Book,
        parsed: &rdf::RdfBook,
    ) -> Vec<&'static str> {
        let mut fields = Vec::new();
        if prev.title != parsed.title {
            fields.push("title");
        }
        if prev.language != parsed.language {
            fields.push("language");
        }
        if prev.issued != parsed.issued_date() {
            fields.push("issued");
        }
        if prev.publisher.as_deref() != parsed.publisher.as_deref() {
            fields.push("publisher");
        }
        if prev.rights.as_deref() != parsed.rights.as_deref() {
            fields.push("rights");
        }
        if prev.description.as_deref() != parsed.description.as_deref() {
            fields.push("description");
        }
        if prev.reading_ease.as_deref() != parsed.reading_ease.as_deref() {
            fields.push("reading_ease");
        }
        if prev.downloads != parsed.downloads.map(|d| d as i32) {
            fields.push("downloads");
        }
        if prev.authors != serde_json::to_value(&parsed.authors).unwrap_or_default() {
            fields.push("authors");
        }
        if prev.subjects != serde_json::to_value(&parsed.subjects).unwrap_or_default() {
            fields.push("subjects");
        }
        if prev.bookshelves != serde_json::to_value(&parsed.bookshelves).unwrap_or_default() {
            fields.push("bookshelves");
        }
        fields
    }

    /// All configured formats terminal (done/skipped with no outstanding
    /// error) → synced (+ sidecar); else enriched.
    async fn recompute_book_status(&self, id: i64, is_new: bool) -> anyhow::Result<()> {
        recompute_book_status(&self.store, &self.events, &self.cfg, id, is_new).await
    }


    /// Shared tail of full/feed cycles: record pull, ingest, repair.
    /// `requested` = the ids the caller asked to pull (targeted/feed runs):
    /// they are ALWAYS (re-)ingested — ingest is idempotent, and a targeted
    /// pull with zero new transfers must still refresh state for those ids.
    async fn cycle_tail(
        &self,
        pull: &PullResult,
        report: &mut CycleReport,
        ingest: bool,
        requested: &[i64],
    ) -> anyhow::Result<()> {
        self.record_pull(pull).await;
        if self.interrupt.is_set() {
            return Ok(());
        }
        if ingest {
            let mut ids: Vec<i64> = requested.to_vec();
            ids.extend(pull.ingest_ids());
            ids.sort_unstable();
            ids.dedup();
            let ids = if ids.is_empty()
                && self.store.book_count(SOURCE_KEY).await? == 0
            {
                // first run: the pull was a full one — scan the mirror
                self.scan_mirror_rdfs().await
            } else {
                ids
            };
            if !ids.is_empty() {
                self.ingest(ids, report).await?;
            }
        }
        if self.interrupt.is_set() {
            return Ok(());
        }
        let only_slice = if requested.is_empty() { None } else { Some(requested) };
        let repair = self.repair_pass(only_slice).await?;
        if let Some(reason) = repair.aborted_reason.clone() {
            report.aborted_reason = Some(reason);
        }
        Ok(())
    }

    async fn fetch_feed(&self) -> anyhow::Result<feed::FeedHead> {
        let url = format!("{}/cache/epub/feeds/today.rss", self.cfg.download_host);
        let mut attempt = 0;
        loop {
            match self.http.fetch(&url).await {
                Ok(resp) => {
                    let xml = resp.text().await.map_err(|e| anyhow::anyhow!("feed body: {e}"))?;
                    return Ok(feed::parse_feed(&xml)?);
                }
                Err(e)
                    if attempt == 0
                        && e.status == Some(429)
                        && e.retry_after.map(|d| !d.is_zero()).unwrap_or(false) =>
                {
                    attempt += 1;
                    tracing::warn!(retry_in = ?e.retry_after, "feed 429 — one retry after Retry-After");
                    tokio::time::sleep(e.retry_after.unwrap_or(Duration::from_secs(60))).await;
                }
                Err(e) => return Err(anyhow::anyhow!("feed fetch: {e}")),
            }
        }
    }
}

/// tokio-friendly JSON sidecar write.
async fn async_move_write(path: &std::path::Path, value: &Json) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let body = serde_json::to_string_pretty(value)?;
    let tmp = path.with_extension("json.tmp");
    tokio::fs::write(&tmp, body.as_bytes()).await?;
    tokio::fs::rename(&tmp, path).await?;
    Ok(())
}

#[async_trait]
impl Provider for GutenbergOrg {
    fn key(&self) -> &'static str {
        SOURCE_KEY
    }

    async fn full_cycle(&self, opts: CycleOpts) -> anyhow::Result<CycleReport> {
        let run_id = self.store.start_run(SOURCE_KEY, "full").await?;
        let mut report = CycleReport { run_id, ..Default::default() };
        self.store.apply_category_seed(SOURCE_KEY, taxonomy::SEED).await?;

        if self.interrupt.is_set() {
            self.store.finish_run(run_id, None, 0, 0, 0, 0, 0, 0, Some("interrupted")).await?;
            report.aborted_reason = Some("interrupted".into());
            return Ok(report);
        }

        let pull = if !opts.only.is_empty() {
            self.mirror.targeted_pull(&opts.only).await
        } else if let Some(n) = opts.limit {
            let ids = self.mirror.list_ids(n).await;
            if ids.is_empty() {
                self.store
                    .finish_run(run_id, None, 0, 0, 0, 0, 0, 0, Some("rsync_failed"))
                    .await?;
                report.aborted_reason = Some("rsync_failed".into());
                anyhow::bail!("module listing returned no ids (host unreachable?)");
            }
            self.mirror.targeted_pull(&ids).await
        } else {
            self.mirror.full_pull().await
        };

        report.transferred_files = pull.transferred_files;
        report.transferred_bytes = pull.transferred_bytes;

        if pull.interrupted || self.interrupt.is_set() {
            self.store
                .finish_run(run_id, pull.rsync_exit, pull.transferred_files, pull.transferred_bytes, 0, 0, 0, 0, Some("interrupted"))
                .await?;
            report.aborted_reason = Some("interrupted".into());
            return Ok(report);
        }
        if pull.failed {
            self.store
                .finish_run(run_id, pull.rsync_exit, pull.transferred_files, pull.transferred_bytes, 0, 0, 0, 0, Some("rsync_failed"))
                .await?;
            report.aborted_reason = Some("rsync_failed".into());
            return Ok(report);
        }

        self.cycle_tail(&pull, &mut report, !opts.no_ingest, &opts.only)
            .await?;

        let files_failed = 0;
        let aborted = report.aborted_reason.clone();
        self.store
            .finish_run(
                run_id,
                pull.rsync_exit,
                pull.transferred_files,
                pull.transferred_bytes,
                report.new_books,
                report.enriched,
                files_failed,
                0,
                aborted.as_deref(),
            )
            .await?;
        Ok(report)
    }

    async fn feed_cycle(&self) -> anyhow::Result<CycleReport> {
        let run_id = self.store.start_run(SOURCE_KEY, "feed").await?;
        let mut report = CycleReport { run_id, ..Default::default() };
        self.store.apply_category_seed(SOURCE_KEY, taxonomy::SEED).await?;

        let head = match self.fetch_feed().await {
            Ok(h) => h,
            Err(e) => {
                // timeout/5xx/etc: warn and skip — the weekly full pull covers the gap
                tracing::warn!(error = %e, "feed fetch failed — skipping feed cycle");
                self.store
                    .finish_run(run_id, None, 0, 0, 0, 0, 0, 0, Some("feed_fetch_failed"))
                    .await?;
                report.aborted_reason = Some("feed_fetch_failed".into());
                return Ok(report);
            }
        };

        let pulled = head.ids.clone();
        let pull = if pulled.is_empty() {
            mirror::PullResult::default()
        } else {
            self.mirror.targeted_pull(&pulled).await
        };
        report.transferred_files = pull.transferred_files;
        report.transferred_bytes = pull.transferred_bytes;

        if pull.interrupted || self.interrupt.is_set() || pull.failed {
            let reason = if pull.failed { "rsync_failed" } else { "interrupted" };
            self.store
                .finish_run(run_id, pull.rsync_exit, pull.transferred_files, pull.transferred_bytes, 0, 0, 0, 0, Some(reason))
                .await?;
            report.aborted_reason = Some(reason.into());
            return Ok(report);
        }

        self.cycle_tail(&pull, &mut report, true, &[]).await?;

        self.ev_now(
            EventKind::FeedChecked,
            None,
            serde_json::json!({
                "pub_date": head.pub_date,
                "items": head.ids.len(),
                "pulled_ids": pulled,
            }),
        )
        .await;
        self.store.set_meta(SOURCE_KEY, "last_feed_pub_date", &head.pub_date).await?;
        self.store
            .set_meta(
                SOURCE_KEY,
                "last_feed_check",
                &OffsetDateTime::now_utc().format(&time::format_description::well_known::Rfc3339).unwrap_or_default(),
            )
            .await?;

        let aborted = report.aborted_reason.clone();
        self.store
            .finish_run(
                run_id,
                pull.rsync_exit,
                pull.transferred_files,
                pull.transferred_bytes,
                report.new_books,
                report.enriched,
                0,
                0,
                aborted.as_deref(),
            )
            .await?;
        Ok(report)
    }

    async fn repair(&self, only: Option<&[i64]>) -> anyhow::Result<RepairReport> {
        let run_id = self.store.start_run(SOURCE_KEY, "repair").await?;
        let mut report = self.repair_pass(only).await?;
        report.run_id = run_id;
        self.store
            .finish_run(
                run_id,
                None,
                0,
                0,
                0,
                0,
                report.failed,
                report.skipped,
                report.aborted_reason.as_deref(),
            )
            .await?;
        Ok(report)
    }

    async fn status(&self) -> anyhow::Result<StatusReport> {
        let mut s = StatusReport {
            books_by_status: self.store.book_status_counts(SOURCE_KEY).await?,
            categories: self.store.category_count(SOURCE_KEY).await?,
            ..Default::default()
        };
        let (pending, failed) = self.store.repair_pending_count(SOURCE_KEY).await?;
        s.repair_pending = pending;
        s.repair_failed = failed;
        if let Some(t) = self.store.min_retry_at(SOURCE_KEY).await? {
            s.min_retry_at = Some(t.to_string());
        }
        if let Some(run) = self.store.last_run(SOURCE_KEY).await? {
            s.last_run = Some(format!(
                "{} run #{}: {} files / {} bytes, +{} new, {} enriched, aborted={:?}",
                run.cycle,
                run.id,
                run.transferred_files.unwrap_or(0),
                run.transferred_bytes.unwrap_or(0),
                run.new_books.unwrap_or(0),
                run.enriched.unwrap_or(0),
                run.aborted_reason,
            ));
        }
        if let Some(next) = self.store.get_meta(SOURCE_KEY, "next_full_sync").await? {
            s.next_full_sync = Some(next);
        }
        if let Some(last_check) = self.store.get_meta(SOURCE_KEY, "last_feed_check").await? {
            if self.cfg.feed_check_days > 0 {
                if let Ok(t) = OffsetDateTime::parse(
                    &last_check,
                    &time::format_description::well_known::Rfc3339,
                ) {
                    let next = t + time::Duration::days(self.cfg.feed_check_days as i64);
                    s.next_feed_check = Some(next.to_string());
                }
            }
        }
        // mirror walk (blocking; one-off command)
        let mirror = self.cfg.mirror_dir();
        let (files, bytes) = tokio::task::spawn_blocking(move || walk_dir(&mirror))
            .await
            .unwrap_or((0, 0));
        s.mirror_files = files;
        s.mirror_bytes = bytes;
        Ok(s)
    }

    async fn retry_failed(&self) -> anyhow::Result<u64> {
        self.store.retry_failed(SOURCE_KEY).await
    }

    async fn progress(&self) -> anyhow::Result<ProgressReport> {
        let total_remote = self.mirror.total_books().await;
        let by_status: std::collections::HashMap<String, i64> =
            self.store.book_status_counts(SOURCE_KEY).await?.into_iter().collect();
        let ingested: i64 = by_status.values().sum();
        let mirror = self.cfg.mirror_dir();
        let (files, bytes) = tokio::task::spawn_blocking(move || walk_dir(&mirror))
            .await
            .unwrap_or((0, 0));
        Ok(ProgressReport {
            source: SOURCE_KEY.to_string(),
            total_remote,
            ingested,
            synced: *by_status.get("synced").unwrap_or(&0),
            enriched: *by_status.get("enriched").unwrap_or(&0),
            discovered: *by_status.get("discovered").unwrap_or(&0),
            failed_permanent: *by_status.get("failed_permanent").unwrap_or(&0),
            mirror_files: files,
            mirror_bytes: bytes,
        })
    }
}

fn walk_dir(root: &std::path::Path) -> (u64, u64) {
    let mut files = 0u64;
    let mut bytes = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(ft) = entry.file_type() else { continue };
            if ft.is_dir() {
                stack.push(entry.path());
            } else if ft.is_file() {
                files += 1;
                bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    (files, bytes)
}

// ---------------------------------------------------------------------------
// HTTP repair pass (Step 7)
// ---------------------------------------------------------------------------

#[derive(Default)]
struct RepairCounters {
    repaired: i64,
    skipped: i64,
    failed: i64,
    deferred: i64,
    consecutive_retriable: i64,
    abort_reason: Option<String>,
}

impl GutenbergOrg {
    /// Done files whose local copy is gone re-enter the repair queue.
    async fn requeue_missing_local(&self, only: Option<&[i64]>) {
        let Ok(done) = self.store.done_files(SOURCE_KEY, only).await else {
            return;
        };
        for f in done {
            let Some(rel) = &f.path else { continue };
            if tokio::fs::try_exists(self.cfg.library_dir.join(rel)).await.unwrap_or(false) {
                continue;
            }
            tracing::warn!(book = f.book_id, format = %f.format, "done file missing on disk — requeueing for repair");
            let _ = self
                .store
                .set_file_state(
                    SOURCE_KEY,
                    f.book_id,
                    &f.format,
                    "pending",
                    None,
                    None,
                    None,
                    Some("missing_local"),
                )
                .await;
        }
    }

    /// Process the repair queue: `book_files` rows that are pending, or done
    /// with `size_mismatch`, whose retry_at has matured. `max_parallel`
    /// workers share the global rate limiter (1 request / interval) and the
    /// circuit breaker (N consecutive ladder-retriable → abort pass).
    async fn repair_pass(&self, only: Option<&[i64]>) -> anyhow::Result<RepairReport> {
        // Pre-pass: done files whose local copy vanished (disk loss/manual
        // deletion) re-enter the queue as pending — repair is the gap-filler.
        self.requeue_missing_local(only).await;
        let queue = self.store.repair_queue(SOURCE_KEY, only).await?;
        let mut report = RepairReport::default();
        if queue.is_empty() {
            return Ok(report);
        }
        tracing::info!(items = queue.len(), "repair pass");

        let counters = Arc::new(parking_lot::Mutex::new(RepairCounters::default()));
        let workers = self.cfg.max_parallel_downloads.max(1);
        let mut handles = Vec::new();
        for w in 0..workers {
            // stride-partition keeps every worker busy without a shared
            // consumer; the polite limiter still serializes actual requests
            let items: Vec<_> = queue.iter().skip(w).step_by(workers).cloned().collect();
            if items.is_empty() {
                continue;
            }
            let store = self.store.clone();
            let events = self.events.clone();
            let http = self.http.clone();
            let cfg = self.cfg.clone();
            let triage = self.triage.clone();
            let interrupt = self.interrupt.clone();
            let counters = counters.clone();
            handles.push(tokio::spawn(async move {
                for file in items {
                    if interrupt.is_set() || counters.lock().abort_reason.is_some() {
                        break;
                    }
                    repair_one(&file, &store, &events, &http, &cfg, triage.as_deref(), &counters).await;
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }

        let c = counters.lock();
        report.repaired = c.repaired;
        report.skipped = c.skipped;
        report.failed = c.failed;
        report.deferred = c.deferred;
        report.aborted_reason = c.abort_reason.clone();
        if let Some(reason) = &report.aborted_reason {
            tracing::warn!(reason, "repair pass aborted");
        }
        Ok(report)
    }
}

/// Handle one repair item end-to-end.
#[allow(clippy::too_many_arguments)]
async fn repair_one(
    file: &bookshelf_core::domain::BookFile,
    store: &Arc<StorePostgres>,
    events: &Arc<dyn EventSink>,
    http: &Arc<PoliteClient>,
    cfg: &Arc<Config>,
    triage: Option<&dyn Triage>,
    counters: &Arc<parking_lot::Mutex<RepairCounters>>,
) {
    let Some(url) = file.url.clone() else {
        let _ = store
            .set_file_state(
                SOURCE_KEY,
                file.book_id,
                &file.format,
                "skipped",
                None,
                None,
                None,
                Some("absent-in-rdf"),
            )
            .await;
        emit(
            events,
            EventKind::FileSkipped,
            Some(file.book_id),
            serde_json::json!({ "format": file.format, "reason": "absent-in-rdf" }),
        );
        counters.lock().skipped += 1;
        return;
    };

    let _permit = http.semaphore().acquire().await;
    match http.fetch(&url).await {
        Ok(resp) => {
            let content_type = PoliteClient::content_type_of(&resp);
            let expected = file.bytes_expected;
            let declared = resp.content_length();
            if declared.is_some() && expected.is_some() && declared != expected.map(|e| e as u64) {
                fail_ladder(
                    file,
                    store,
                    events,
                    cfg,
                    triage,
                    counters,
                    &FetchError {
                        url: url.clone(),
                        kind: bookshelf_core::FetchErrorKind::Body,
                        status: None,
                        retry_after: None,
                        body_head: format!("Content-Length {declared:?} != extent {expected:?}"),
                        headers: Json::Null,
                    },
                    "truncated",
                )
                .await;
                return;
            }
            match resp.bytes().await {
                Ok(body) => {
                    if content_type.starts_with("text/html") {
                        // 200 carrying HTML on a file download: ambiguous —
                        // agent (when configured) or body-error ladder.
                        let ctx = bookshelf_core::domain::TriageContext {
                            tool: "http",
                            url_or_dest: url.clone(),
                            status: Some(200),
                            headers: serde_json::json!({ "content-type": content_type }),
                            body_head: body.iter().copied().take(500).map(|b| b as char).collect(),
                            attempts: file.attempts as u32,
                            error: "200 text/html on file download".into(),
                        };
                        let decision = match triage {
                            Some(t) => t.decide(&ctx).await,
                            None => bookshelf_core::domain::TriageDecision::Defer,
                        };
                        apply_decision(file, store, events, cfg, counters, decision, "html_body").await;
                        return;
                    }
                    if let Some(exp) = expected {
                        if body.len() as i64 != exp {
                            fail_ladder(
                                file,
                                store,
                                events,
                                cfg,
                                triage,
                                counters,
                                &FetchError {
                                    url: url.clone(),
                                    kind: bookshelf_core::FetchErrorKind::Body,
                                    status: None,
                                    retry_after: None,
                                    body_head: format!("body {} != extent {exp}", body.len()),
                                    headers: Json::Null,
                                },
                                "truncated",
                            )
                            .await;
                            return;
                        }
                    }
                    // atomic .part + rename into the mirror
                    let rel = file.path.clone().unwrap_or_else(|| {
                        format!("mirror/{}/{}", file.book_id, mirror_name_for(&file.format, file.book_id))
                    });
                    let abs = cfg.library_dir.join(&rel);
                    if let Some(parent) = abs.parent() {
                        let _ = tokio::fs::create_dir_all(parent).await;
                    }
                    let tmp = abs.with_extension("part");
                    if let Err(e) = tokio::fs::write(&tmp, &body).await {
                        tracing::warn!(id = file.book_id, error = %e, "repair write failed");
                        counters.lock().deferred += 1;
                        return;
                    }
                    if let Err(e) = tokio::fs::rename(&tmp, &abs).await {
                        tracing::warn!(id = file.book_id, error = %e, "repair rename failed");
                        counters.lock().deferred += 1;
                        return;
                    }
                    let _ = store
                        .set_file_done(SOURCE_KEY, file.book_id, &file.format, &rel, None)
                        .await;
                    emit(
                        events,
                        EventKind::FileRepaired,
                        Some(file.book_id),
                        serde_json::json!({
                            "format": file.format,
                            "path": rel,
                            "bytes": body.len(),
                        }),
                    );
                    let _ = recompute_book_status(store, events, cfg, file.book_id, false).await;
                    let mut c = counters.lock();
                    c.repaired += 1;
                    c.consecutive_retriable = 0;
                }
                Err(e) => {
                    fail_ladder(
                        file,
                        store,
                        events,
                        cfg,
                        triage,
                        counters,
                        &FetchError {
                            url: url.clone(),
                            kind: bookshelf_core::FetchErrorKind::Body,
                            status: None,
                            retry_after: None,
                            body_head: e.to_string().chars().take(500).collect(),
                            headers: Json::Null,
                        },
                        "body_read",
                    )
                    .await;
                }
            }
        }
        Err(e) => {
            fail_ladder(file, store, events, cfg, triage, counters, &e, &e.to_string()).await;
        }
    }
}

/// One failed attempt: triage (agent only when ambiguous, else rules) →
/// persist attempts/retry_at/last_error + `file.failed` event.
#[allow(clippy::too_many_arguments)]
async fn fail_ladder(
    file: &bookshelf_core::domain::BookFile,
    store: &Arc<StorePostgres>,
    events: &Arc<dyn EventSink>,
    cfg: &Arc<Config>,
    triage: Option<&dyn Triage>,
    counters: &Arc<parking_lot::Mutex<RepairCounters>>,
    err: &FetchError,
    label: &str,
) {
    let attempts = file.attempts as u32 + 1;
    let decision = triage_rules::decide(err, attempts);
    let decision = if attempts >= 3 {
        // 3+ consecutive same-URL failures: ambiguous → consult the agent
        match triage {
            Some(t) => {
                let ctx = bookshelf_core::domain::TriageContext {
                    tool: "http",
                    url_or_dest: err.url.clone(),
                    status: err.status,
                    headers: serde_json::json!({ "retry-after": err.retry_after.map(|d| d.as_secs()) }),
                    body_head: err.body_head.chars().take(500).collect(),
                    attempts,
                    error: label.to_string(),
                };
                t.decide(&ctx).await
            }
            None => decision,
        }
    } else {
        decision
    };
    apply_decision(file, store, events, cfg, counters, decision, label).await;
}

#[allow(clippy::too_many_arguments)]
async fn apply_decision(
    file: &bookshelf_core::domain::BookFile,
    store: &Arc<StorePostgres>,
    events: &Arc<dyn EventSink>,
    cfg: &Arc<Config>,
    counters: &Arc<parking_lot::Mutex<RepairCounters>>,
    decision: bookshelf_core::domain::TriageDecision,
    label: &str,
) {
    use bookshelf_core::domain::TriageDecision;
    let attempts = file.attempts + 1;
    match decision {
        TriageDecision::Skip => {
            let (status, reason) = if matches!(file_status_of(file), 404 | 410) {
                ("skipped", "404".to_string())
            } else {
                ("failed", label.to_string())
            };
            let _ = store
                .set_file_state(
                    SOURCE_KEY,
                    file.book_id,
                    &file.format,
                    status,
                    None,
                    Some(attempts),
                    None,
                    Some(&reason),
                )
                .await;
            if status == "skipped" {
                emit(
                    events,
                    EventKind::FileSkipped,
                    Some(file.book_id),
                    serde_json::json!({ "format": file.format, "reason": reason }),
                );
                let _ = recompute_book_status(store, events, cfg, file.book_id, false).await;
                let mut c = counters.lock();
                c.skipped += 1;
                c.consecutive_retriable = 0;
            } else {
                emit(
                    events,
                    EventKind::FileFailed,
                    Some(file.book_id),
                    serde_json::json!({
                        "format": file.format,
                        "error": reason,
                        "retry_at": null,
                    }),
                );
                let _ = store
                    .set_book_status(
                        SOURCE_KEY,
                        file.book_id,
                        "failed_permanent",
                        Some(&label),
                    )
                    .await;
                emit(
                    events,
                    EventKind::BookFailedPermanent,
                    Some(file.book_id),
                    serde_json::json!({ "error": label }),
                );
                let mut c = counters.lock();
                c.failed += 1;
                c.consecutive_retriable = 0;
            }
        }
        TriageDecision::RetryAfter(d) => {
            if attempts as u32 >= cfg.max_total_attempts {
                {
                    let mut c = counters.lock();
                    c.failed += 1;
                }
                let _ = store
                    .set_file_state(
                        SOURCE_KEY,
                        file.book_id,
                        &file.format,
                        "failed",
                        None,
                        Some(attempts),
                        None,
                        Some(&format!("{label}: attempts exhausted")),
                    )
                    .await;
                let _ = store
                    .set_book_status(SOURCE_KEY, file.book_id, "failed_permanent", Some(label))
                    .await;
                emit(
                    events,
                    EventKind::FileFailed,
                    Some(file.book_id),
                    serde_json::json!({
                        "format": file.format,
                        "error": format!("{label}: attempts exhausted"),
                        "retry_at": null,
                    }),
                );
                emit(
                    events,
                    EventKind::BookFailedPermanent,
                    Some(file.book_id),
                    serde_json::json!({ "error": label }),
                );
                return;
            }
            let trip = {
                let mut c = counters.lock();
                c.consecutive_retriable += 1;
                let trip = c.consecutive_retriable >= cfg.circuit_breaker as i64;
                if trip {
                    c.abort_reason = Some("site_overloaded".into());
                }
                trip
            };
            let _ = trip;
            let retry_at = OffsetDateTime::now_utc()
                + time::Duration::try_from(d).unwrap_or(time::Duration::seconds(3600));
            let _ = store
                .set_file_state(
                    SOURCE_KEY,
                    file.book_id,
                    &file.format,
                    "pending",
                    None,
                    Some(attempts),
                    Some(retry_at),
                    Some(label),
                )
                .await;
            emit(
                events,
                EventKind::FileFailed,
                Some(file.book_id),
                serde_json::json!({
                    "format": file.format,
                    "error": label,
                    "retry_at": retry_to_string(retry_at),
                }),
            );
            {
                let mut c = counters.lock();
                c.deferred += 1;
            }
        }
        TriageDecision::Defer => {
            let _ = store
                .set_file_state(
                    SOURCE_KEY,
                    file.book_id,
                    &file.format,
                    "pending",
                    None,
                    Some(attempts),
                    None,
                    Some(label),
                )
                .await;
            emit(
                events,
                EventKind::FileFailed,
                Some(file.book_id),
                serde_json::json!({
                    "format": file.format,
                    "error": label,
                    "retry_at": null,
                }),
            );
            counters.lock().deferred += 1;
        }
    }
}

fn file_status_of(file: &bookshelf_core::domain::BookFile) -> u16 {
    // last_error for skip-eligible statuses carries the HTTP code
    file.last_error
        .as_deref()
        .and_then(|s| s.trim_start_matches("HTTP ").parse().ok())
        .unwrap_or(0)
}

fn retry_to_string(t: OffsetDateTime) -> String {
    t.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

fn mirror_name_for(format: &str, id: i64) -> String {
    rdf::Format::parse_key(format)
        .map(|f| f.mirror_name(id))
        .unwrap_or_else(|| format!("pg{id}.bin"))
}

/// Fire-and-forget event helper shared by worker code.
fn emit(events: &Arc<dyn EventSink>, kind: EventKind, book_id: Option<i64>, detail: Json) {
    let sink = events.clone();
    tokio::spawn(async move {
        sink.emit(SOURCE_KEY, kind, book_id, detail).await;
    });
}

/// Free-standing status recompute shared by ingest and repair workers.
async fn recompute_book_status(
    store: &Arc<StorePostgres>,
    events: &Arc<dyn EventSink>,
    cfg: &Arc<Config>,
    id: i64,
    is_new: bool,
) -> anyhow::Result<()> {
    let rows = store.get_files(SOURCE_KEY, id).await?;
    let all_terminal = cfg.formats.iter().all(|f| {
        rows.iter().any(|r| {
            r.format == f.key()
                && matches!(r.status.as_str(), "done" | "skipped")
                && r.last_error.is_none()
        })
    });
    let book = store.get_book(SOURCE_KEY, id).await?;
    let status = match book
        .as_ref()
        .and_then(|b| bookshelf_core::domain::BookStatus::parse(&b.status))
    {
        Some(s) => s,
        None => return Ok(()),
    };
    if all_terminal {
        if status != bookshelf_core::domain::BookStatus::Synced {
            store.set_book_status(SOURCE_KEY, id, "synced", None).await?;
            emit(events, EventKind::BookSynced, Some(id), serde_json::json!({}));
            write_sidecar(store, cfg, id).await;
        }
    } else if is_new {
        store.set_book_status(SOURCE_KEY, id, "enriched", None).await?;
    } else if status == bookshelf_core::domain::BookStatus::Synced {
        // a format went non-terminal again (e.g. updated upstream)
        store.set_book_status(SOURCE_KEY, id, "enriched", None).await?;
    }
    Ok(())
}

/// Free-standing sidecar writer: {library_dir}/meta/{id}.json.
async fn write_sidecar(store: &Arc<StorePostgres>, cfg: &Arc<Config>, id: i64) {
    let Some(book) = store.get_book(SOURCE_KEY, id).await.ok().flatten() else {
        return;
    };
    let cats = store.book_categories(SOURCE_KEY, id).await.unwrap_or_default();
    let files = store.get_files(SOURCE_KEY, id).await.unwrap_or_default();
    let file_map: serde_json::Map<String, Json> = files
        .into_iter()
        .map(|f| {
            (
                f.format,
                serde_json::json!({
                    "path": f.path,
                    "url": f.url,
                    "bytes_expected": f.bytes_expected,
                    "status": f.status,
                }),
            )
        })
        .collect();
    let sidecar = serde_json::json!({
        "source": SOURCE_KEY,
        "id": id,
        "type": book.r#type,
        "title": book.title,
        "language": book.language,
        "issued": book.issued.map(|d| d.to_string()),
        "publisher": book.publisher,
        "rights": book.rights,
        "description": book.description,
        "reading_ease": book.reading_ease,
        "downloads": book.downloads,
        "authors": book.authors,
        "subjects": book.subjects,
        "bookshelves": book.bookshelves,
        "categories": cats,
        "files": file_map,
        "generated_at": OffsetDateTime::now_utc().unix_timestamp(),
    });
    let path = cfg.meta_dir().join(format!("{id}.json"));
    if let Err(e) = async_move_write(&path, &sidecar).await {
        tracing::warn!(id, error = %e, "sidecar write failed");
    }
}
