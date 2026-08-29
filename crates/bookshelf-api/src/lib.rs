//! Wire DTOs shared by every bookshelf frontend shell (the web crate today,
//! a desktop shell later) and the servers that feed them.
//!
//! Serde only — this crate must compile to wasm32 for the browser bundle,
//! so no sqlx/tokio may leak in here. Rows come out of `StorePostgres` as
//! domain types; the server maps them onto these.

use serde::{Deserialize, Serialize};

/// One catalog hit: enough to render a card, a spine or a search result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookHit {
    pub id: i64,
    pub title: String,
    /// Display names, e.g. `"Austen, Jane"`.
    pub authors: Vec<String>,
    pub year: Option<i32>,
    pub language: String,
    pub downloads: Option<i64>,
    pub has_cover: bool,
    /// Leaf names this book is shelved under.
    pub categories: Vec<String>,
    /// Size of the mirrored plain-text copy — the spine-thickness proxy
    /// (≈ page count) for shelf rendering.
    pub txt_bytes: Option<i64>,
}

/// Catalog totals for the home footer / status chips.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stats {
    pub books: i64,
    pub categories: i64,
    pub synced: i64,
    /// Display-formatted `finished_at` of the last completed sync run.
    pub last_sync: Option<String>,
}

/// A top group with its leaves and per-leaf book counts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryGroup {
    /// Top group name; leaves without one are filed under `"Unassigned"`.
    pub group: String,
    pub leaves: Vec<CategoryLeaf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryLeaf {
    pub name: String,
    pub books: i64,
}

/// One page of a shelf listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryBooksPage {
    pub category: String,
    pub total: i64,
    pub offset: i64,
    pub items: Vec<BookHit>,
}

/// Full author entry for the book page.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorBio {
    pub name: String,
    pub birth: Option<i16>,
    pub death: Option<i16>,
    pub wikipedia: Option<String>,
}

/// A locally mirrored format, ready to take out.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOffer {
    /// Mirror format key: `txt` | `epub.images` | `html.zip`.
    pub format: String,
    /// Human label: "Plain text", "EPUB (images)", "HTML (zipped)".
    pub label: String,
    /// Filename extension for the download.
    pub extension: String,
    pub bytes: Option<i64>,
}

/// Everything the book page shows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookDetail {
    pub id: i64,
    pub title: String,
    pub authors: Vec<AuthorBio>,
    pub year: Option<i32>,
    pub language: String,
    pub publisher: Option<String>,
    pub rights: Option<String>,
    pub description: Option<String>,
    pub downloads: Option<i64>,
    /// Flesch score parsed out of the raw `reading_ease` sentence.
    pub reading_ease: Option<f32>,
    pub subjects: Vec<String>,
    pub categories: Vec<String>,
    pub files: Vec<FileOffer>,
    pub has_cover: bool,
}

/// Where a search query should look.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SearchScope {
    All,
    Title,
    Author,
}

impl SearchScope {
    /// Query-param spelling (`scope=all`).
    pub fn from_param(s: &str) -> Option<Self> {
        match s {
            "all" => Some(Self::All),
            "title" => Some(Self::Title),
            "author" => Some(Self::Author),
            _ => None,
        }
    }
}
