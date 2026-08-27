//! Domain model shared by every provider: books, files, categories, sync runs,
//! the event taxonomy and the ports that vary between deployments
//! (EventSink: JSONL file today, table tomorrow; Triage: deterministic rules
//! or LLM agent).
//!
//! The Store port is deliberately the concrete [`crate::StorePostgres`]
//! adapter: a trait duplicating its ~25 methods exists for exactly one impl
//! and no second backend is planned (AGENTS.md — hexagonal, but not at the
//! cost of maintainability). Extract the trait from real usage when a second
//! backend actually appears.

use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use time::OffsetDateTime;

pub type Json = serde_json::Value;

// ---------------------------------------------------------------------------
// Status enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookStatus {
    Discovered,
    Enriched,
    Synced,
    FailedPermanent,
}

impl BookStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            BookStatus::Discovered => "discovered",
            BookStatus::Enriched => "enriched",
            BookStatus::Synced => "synced",
            BookStatus::FailedPermanent => "failed_permanent",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "discovered" => Some(BookStatus::Discovered),
            "enriched" => Some(BookStatus::Enriched),
            "synced" => Some(BookStatus::Synced),
            "failed_permanent" => Some(BookStatus::FailedPermanent),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileStatus {
    Pending,
    Done,
    Skipped,
    Failed,
}

impl FileStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            FileStatus::Pending => "pending",
            FileStatus::Done => "done",
            FileStatus::Skipped => "skipped",
            FileStatus::Failed => "failed",
        }
    }
}

// ---------------------------------------------------------------------------
// Entities (state lives in Postgres; every row carries its source)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Book {
    pub source: String,
    pub id: i64,
    #[sqlx(rename = "type")]
    pub r#type: String,
    pub title: String,
    pub language: String,
    pub issued: Option<time::Date>,
    pub publisher: Option<String>,
    pub rights: Option<String>,
    pub description: Option<String>,
    pub reading_ease: Option<String>,
    pub downloads: Option<i32>,
    pub authors: Json,
    pub subjects: Json,
    pub bookshelves: Json,
    pub status: String,
    pub attempts: i32,
    pub retry_at: Option<OffsetDateTime>,
    pub last_error: Option<String>,
    pub first_seen: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct BookFile {
    pub source: String,
    pub book_id: i64,
    pub format: String,
    pub url: Option<String>,
    pub bytes_expected: Option<i64>,
    pub remote_modified: Option<String>,
    pub path: Option<String>,
    pub status: String,
    pub attempts: i32,
    pub retry_at: Option<OffsetDateTime>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Category {
    pub source: String,
    pub name: String,
    pub parent: Option<String>,
    pub bookshelf_id: Option<i32>,
    pub updated_at: OffsetDateTime,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SyncRun {
    pub id: i64,
    pub source: String,
    pub cycle: String,
    pub started_at: OffsetDateTime,
    pub finished_at: Option<OffsetDateTime>,
    pub rsync_exit: Option<i32>,
    pub transferred_files: Option<i32>,
    pub transferred_bytes: Option<i64>,
    pub new_books: Option<i32>,
    pub enriched: Option<i32>,
    pub files_failed: Option<i32>,
    pub files_skipped: Option<i32>,
    pub aborted_reason: Option<String>,
}

// ---------------------------------------------------------------------------
// Events (append-only audit trail; JSONL lines)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum EventKind {
    #[serde(rename = "book.discovered")]
    BookDiscovered,
    #[serde(rename = "book.metadata_updated")]
    BookMetadataUpdated,
    #[serde(rename = "book.enriched")]
    BookEnriched,
    #[serde(rename = "book.category_added")]
    BookCategoryAdded,
    #[serde(rename = "book.category_removed")]
    BookCategoryRemoved,
    #[serde(rename = "file.transferred")]
    FileTransferred,
    #[serde(rename = "file.removed")]
    FileRemoved,
    #[serde(rename = "file.skipped")]
    FileSkipped,
    #[serde(rename = "file.repaired")]
    FileRepaired,
    #[serde(rename = "file.failed")]
    FileFailed,
    #[serde(rename = "book.synced")]
    BookSynced,
    #[serde(rename = "book.failed_permanent")]
    BookFailedPermanent,
    #[serde(rename = "taxonomy.updated")]
    TaxonomyUpdated,
    #[serde(rename = "feed.checked")]
    FeedChecked,
}

impl EventKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventKind::BookDiscovered => "book.discovered",
            EventKind::BookMetadataUpdated => "book.metadata_updated",
            EventKind::BookEnriched => "book.enriched",
            EventKind::BookCategoryAdded => "book.category_added",
            EventKind::BookCategoryRemoved => "book.category_removed",
            EventKind::FileTransferred => "file.transferred",
            EventKind::FileRemoved => "file.removed",
            EventKind::FileSkipped => "file.skipped",
            EventKind::FileRepaired => "file.repaired",
            EventKind::FileFailed => "file.failed",
            EventKind::BookSynced => "book.synced",
            EventKind::BookFailedPermanent => "book.failed_permanent",
            EventKind::TaxonomyUpdated => "taxonomy.updated",
            EventKind::FeedChecked => "feed.checked",
        }
    }
}

/// Port: where events go. Today: JSONL file. Tomorrow maybe a table.
#[async_trait]
pub trait EventSink: Send + Sync {
    async fn emit(&self, source: &str, kind: EventKind, book_id: Option<i64>, detail: Json);
}

// ---------------------------------------------------------------------------
// Triage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriageDecision {
    /// Item is permanently unavailable/blocked — do not retry.
    Skip,
    /// Transient — retry after the given delay.
    RetryAfter(Duration),
    /// Wait for the next sync run (retry_at cleared).
    Defer,
}

/// Everything the triage layer may look at for one failed interaction.
#[derive(Debug, Clone)]
pub struct TriageContext {
    /// "http" | "rsync"
    pub tool: &'static str,
    pub url_or_dest: String,
    pub status: Option<u16>,
    pub headers: Json,
    pub body_head: String,
    pub attempts: u32,
    pub error: String,
}

/// Port: classify a failed fetch/rsync into a recovery action.
#[async_trait]
pub trait Triage: Send + Sync {
    async fn decide(&self, ctx: &TriageContext) -> TriageDecision;
}
