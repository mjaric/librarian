//! JSON API: catalog reads over `StorePostgres`, mapped onto `bookshelf-api`
//! DTOs, plus cover/file streaming out of the local mirror.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path as AxPath, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use bookshelf_api::{
    AuthorBio, BookDetail, BookHit, CategoryBooksPage, CategoryGroup, CategoryLeaf, FileOffer,
    Stats,
};
use bookshelf_core::StorePostgres;
use bookshelf_core::domain::{BookHitRow, CatalogScope};
use serde::Deserialize;
use time::OffsetDateTime;

/// Mirror source this catalog serves. The single registered provider today
/// (see `librarian::gutenberg_org::SOURCE_KEY`); a second provider turns this
/// into a config-selected key.
const SOURCE: &str = "project-gutenberg";

/// The one-page budget for card grids — enough to fill a screen, never a wall.
pub const PAGE: i64 = 24;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<StorePostgres>,
    /// Canonicalized `library_dir`; every served file must live under it.
    pub library_root: PathBuf,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/stats", get(stats))
        .route("/search", get(search))
        .route("/recent", get(recent))
        .route("/categories", get(categories))
        .route("/categories/{leaf}/books", get(category_books))
        .route("/books/random", get(random_book))
        .route("/books/{id}", get(book))
        .route("/books/{id}/files/{format}", get(book_file))
        .route("/covers/{id}", get(cover))
}

// -- helpers -------------------------------------------------------------

struct ApiError(anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        tracing::error!("api error: {:#}", self.0);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": self.0.to_string() })),
        )
            .into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        Self(e)
    }
}
fn not_found(msg: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": msg })),
    )
        .into_response()
}

fn hit(row: BookHitRow) -> BookHit {
    BookHit {
        id: row.id,
        title: row.title,
        authors: author_names(&row.authors),
        year: row.issued.map(|d| d.year()),
        language: row.language,
        downloads: row.downloads.map(i64::from),
        has_cover: row.has_cover,
        categories: row.categories,
        txt_bytes: row.txt_bytes,
    }
}

/// `[{name, birth, death, wikipedia}]` → display names. Rows come from the
/// RDF parser; tolerate anything unexpected by skipping it.
fn author_names(json: &serde_json::Value) -> Vec<String> {
    json.as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(|e| e.get("name").and_then(|n| n.as_str()))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn author_bios(json: &serde_json::Value) -> Vec<AuthorBio> {
    json.as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(|e| {
                    let name = e.get("name")?.as_str()?.to_string();
                    Some(AuthorBio {
                        name,
                        birth: e
                            .get("birth")
                            .and_then(|v| v.as_i64())
                            .and_then(|v| v.try_into().ok()),
                        death: e
                            .get("death")
                            .and_then(|v| v.as_i64())
                            .and_then(|v| v.try_into().ok()),
                        wikipedia: e
                            .get("wikipedia")
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Pull the Flesch number out of the raw sentence
/// (`"Reading ease score: 69.2 (8th9th grade). …"`).
fn flesch_score(raw: &str) -> Option<f32> {
    let rest = raw.split("score:").nth(1)?;
    let num: String = rest
        .trim_start()
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    num.parse().ok()
}

fn format_ts(t: OffsetDateTime) -> String {
    use time::format_description;
    let fmt = format_description::parse_borrowed::<2>("[year]-[month]-[day] [hour]:[minute]")
        .expect("static format");
    t.format(&fmt).unwrap_or_else(|_| t.to_string())
}

/// (format key, label, filename extension) for every mirrored book format.
fn format_offer(format: &str, bytes: Option<i64>) -> Option<FileOffer> {
    let (label, extension) = match format {
        "txt" => ("Plain text", "txt"),
        "epub.images" => ("EPUB (with images)", "epub"),
        "html.zip" => ("HTML (zipped)", "zip"),
        _ => return None, // 'cover' and anything non-book
    };
    Some(FileOffer {
        format: format.to_string(),
        label: label.to_string(),
        extension: extension.to_string(),
        bytes,
    })
}

/// Stream a file from the mirror. Guards traversal by canonicalizing under
/// the configured library root.
async fn stream_from_mirror(
    state: &AppState,
    rel: &str,
    content_type: &'static str,
    filename: &str,
    attachment: bool,
) -> Response {
    let full = state.library_root.join(rel);
    let canonical = match tokio::fs::canonicalize(&full).await {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                format!("file missing from mirror: {e}"),
            )
                .into_response();
        }
    };
    if !canonical.starts_with(&state.library_root) {
        return (StatusCode::FORBIDDEN, "path escapes the library").into_response();
    }
    let file = match tokio::fs::File::open(&canonical).await {
        Ok(f) => f,
        Err(e) => return (StatusCode::NOT_FOUND, format!("cannot open file: {e}")).into_response(),
    };
    let len = file.metadata().await.map(|m| m.len()).unwrap_or(0);
    let stream = tokio_util::io::ReaderStream::new(file);

    let mut resp = Response::new(Body::from_stream(stream));
    let headers = resp.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    // HeaderValue::from_str never fails for ascii filenames; mirror names are.
    if let Ok(len) = HeaderValue::from_str(&len.to_string()) {
        headers.insert(header::CONTENT_LENGTH, len);
    }
    let disposition = if attachment { "attachment" } else { "inline" };
    if let Ok(cd) = HeaderValue::from_str(&format!("{disposition}; filename=\"{filename}\"")) {
        headers.insert(header::CONTENT_DISPOSITION, cd);
    }
    resp
}

async fn stats(State(state): State<AppState>) -> Result<Response, ApiError> {
    let (books, categories, synced) = state.store.catalog_stats(SOURCE).await?;
    let last_sync = state
        .store
        .last_run(SOURCE)
        .await?
        .and_then(|r| r.finished_at)
        .map(format_ts);
    Ok(Json(Stats {
        books,
        categories,
        synced,
        last_sync,
    })
    .into_response())
}

#[derive(Deserialize)]
struct SearchParams {
    q: String,
    scope: Option<String>,
    limit: Option<i64>,
}

async fn search(
    Query(p): Query<SearchParams>,
    State(state): State<AppState>,
) -> Result<Response, ApiError> {
    let scope = match p.scope.as_deref() {
        Some("title") => CatalogScope::Title,
        Some("author") => CatalogScope::Author,
        _ => CatalogScope::All,
    };
    let limit = p.limit.unwrap_or(40).clamp(1, 100);
    let hits = state.store.search_books(SOURCE, &p.q, scope, limit).await?;
    let hits: Vec<BookHit> = hits.into_iter().map(hit).collect();
    Ok(Json(hits).into_response())
}

async fn recent(
    Query(p): Query<RecentParams>,
    State(state): State<AppState>,
) -> Result<Response, ApiError> {
    let limit = p.limit.unwrap_or(10).clamp(1, 24);
    let rows = state.store.recent_books(SOURCE, limit).await?;
    let hits: Vec<BookHit> = rows.into_iter().map(hit).collect();
    Ok(Json(hits).into_response())
}

#[derive(Deserialize)]
struct RecentParams {
    limit: Option<i64>,
}

async fn categories(State(state): State<AppState>) -> Result<Response, ApiError> {
    let rows = state.store.category_counts(SOURCE).await?;
    let mut groups: HashMap<String, Vec<CategoryLeaf>> = HashMap::new();
    for row in rows {
        let group = row.parent.unwrap_or_else(|| "Unassigned".into());
        groups.entry(group).or_default().push(CategoryLeaf {
            name: row.leaf,
            books: row.books,
        });
    }
    let mut ranked: Vec<(String, i64, Vec<CategoryLeaf>)> = groups
        .into_iter()
        .map(|(group, mut leaves)| {
            leaves.sort_by(|a, b| b.books.cmp(&a.books).then_with(|| a.name.cmp(&b.name)));
            let total: i64 = leaves.iter().map(|l| l.books).sum();
            (group, total, leaves)
        })
        .collect();
    ranked.sort_by(|a, b| b.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    let groups: Vec<CategoryGroup> = ranked
        .into_iter()
        .map(|(group, _, leaves)| CategoryGroup { group, leaves })
        .collect();
    Ok(Json(groups).into_response())
}

#[derive(Deserialize)]
struct CategoryBooksParams {
    q: Option<String>,
    offset: Option<i64>,
    limit: Option<i64>,
}

async fn category_books(
    AxPath(leaf): AxPath<String>,
    Query(p): Query<CategoryBooksParams>,
    State(state): State<AppState>,
) -> Result<Response, ApiError> {
    let q = p.q.unwrap_or_default();
    let offset = p.offset.unwrap_or(0).max(0);
    let limit = p.limit.unwrap_or(PAGE).clamp(1, 100);
    let (rows, total) = state
        .store
        .books_in_category(SOURCE, &leaf, &q, limit, offset)
        .await?;
    let items = rows.into_iter().map(hit).collect();
    Ok(Json(CategoryBooksPage {
        category: leaf,
        total,
        offset,
        items,
    })
    .into_response())
}

async fn random_book(State(state): State<AppState>) -> Result<Response, ApiError> {
    let row = state.store.random_book(SOURCE).await?;
    match row {
        Some(row) => Ok(Json(hit(row)).into_response()),
        None => Ok(not_found("catalog is empty")),
    }
}

async fn book(
    AxPath(id): AxPath<i64>,
    State(state): State<AppState>,
) -> Result<Response, ApiError> {
    let Some(b) = state.store.get_book(SOURCE, id).await? else {
        return Ok(not_found("no such book in this catalog"));
    };
    let files = state.store.get_files(SOURCE, id).await?;
    let categories = state.store.book_categories(SOURCE, id).await?;

    let offers: Vec<FileOffer> = files
        .iter()
        .filter(|f| f.status == "done")
        .filter_map(|f| format_offer(&f.format, f.bytes_expected))
        .collect();
    let has_cover = files
        .iter()
        .any(|f| f.format == "cover" && f.status == "done");

    // Subjects: [{scheme, value}] → plain values, deduped, capped for chips.
    let mut subjects: Vec<String> = Vec::new();
    if let Some(entries) = b.subjects.as_array() {
        for e in entries {
            if let Some(v) = e.get("value").and_then(|v| v.as_str()) {
                if !subjects.iter().any(|s| s == v) {
                    subjects.push(v.to_string());
                }
            }
        }
    }
    subjects.truncate(16);

    let detail = BookDetail {
        id: b.id,
        title: b.title,
        authors: author_bios(&b.authors),
        year: b.issued.map(|d| d.year()),
        language: b.language,
        publisher: b.publisher,
        rights: b.rights,
        description: b.description,
        downloads: b.downloads.map(i64::from),
        reading_ease: b.reading_ease.as_deref().and_then(flesch_score),
        subjects,
        categories,
        files: offers,
        has_cover,
    };
    Ok(Json(detail).into_response())
}

async fn cover(
    AxPath(id): AxPath<i64>,
    State(state): State<AppState>,
) -> Result<Response, ApiError> {
    let files = state.store.get_files(SOURCE, id).await?;
    let Some(cover) = files
        .iter()
        .find(|f| f.format == "cover" && f.status == "done" && f.path.is_some())
    else {
        return Ok(not_found("no mirrored cover"));
    };
    let rel = cover.path.as_deref().expect("checked above");
    Ok(stream_from_mirror(
        &state,
        rel,
        "image/jpeg",
        &format!("pg{id}.cover.medium.jpg"),
        false,
    )
    .await)
}

async fn book_file(
    AxPath((id, format)): AxPath<(i64, String)>,
    Query(p): Query<FileParams>,
    State(state): State<AppState>,
) -> Result<Response, ApiError> {
    let files = state.store.get_files(SOURCE, id).await?;
    let Some(f) = files
        .iter()
        .find(|f| f.format == format && f.status == "done" && f.path.is_some())
    else {
        return Ok(not_found("no such file in this catalog"));
    };
    let rel = f.path.clone().unwrap_or_default();
    let (content_type, ext) = match format.as_str() {
        "txt" => ("text/plain; charset=utf-8", "txt"),
        "epub.images" => ("application/epub+zip", "epub"),
        "html.zip" => ("application/zip", "zip"),
        _ => ("application/octet-stream", "dat"),
    };
    let filename = Path::new(&rel)
        .file_name()
        .and_then(|n| n.to_str())
        .map(str::to_string)
        .unwrap_or_else(|| format!("pg{id}.{ext}"));
    // `?disposition=inline` lets the browser render plain text in a tab.
    let inline = p.disposition.as_deref() == Some("inline") && content_type.starts_with("text/");
    Ok(stream_from_mirror(&state, &rel, content_type, &filename, !inline).await)
}

#[derive(Deserialize)]
struct FileParams {
    disposition: Option<String>,
}
