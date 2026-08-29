//! Typed client for the librarian-web JSON API. Failures come back as
//! display-ready strings — the UI has nowhere better to send them.

use serde::de::DeserializeOwned;

use bookshelf_api::{BookDetail, BookHit, CategoryBooksPage, CategoryGroup, Stats};

async fn get<T: DeserializeOwned>(path: &str) -> Result<T, String> {
    let resp = gloo_net::http::Request::get(path)
        .send()
        .await
        .map_err(|e| format!("cannot reach the server: {e}"))?;
    if !resp.ok() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        let head: String = body.chars().take(160).collect();
        return Err(if head.is_empty() {
            format!("HTTP {status}")
        } else {
            format!("HTTP {status}: {head}")
        });
    }
    resp.json::<T>()
        .await
        .map_err(|e| format!("bad response from the server: {e}"))
}

pub async fn stats() -> Result<Stats, String> {
    get("/api/stats").await
}

/// `scope` is the wire spelling: `all` | `title` | `author`.
pub async fn search(q: &str, scope: &str) -> Result<Vec<BookHit>, String> {
    get(&format!(
        "/api/search?q={}&scope={}&limit=60",
        urlencode(q),
        urlencode(scope)
    ))
    .await
}

pub async fn categories() -> Result<Vec<CategoryGroup>, String> {
    get("/api/categories").await
}

pub async fn category_books(leaf: &str, q: &str, limit: i64) -> Result<CategoryBooksPage, String> {
    get(&format!(
        "/api/categories/{}/books?q={}&limit={}",
        urlencode(leaf),
        urlencode(q),
        limit
    ))
    .await
}

/// Fetch one book; `Ok(None)` when the catalogue has no such id (HTTP 404).
pub async fn book(id: i64) -> Result<Option<BookDetail>, String> {
    match get::<BookDetail>(&format!("/api/books/{id}")).await {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.starts_with("HTTP 404") => Ok(None),
        Err(e) => Err(e),
    }
}

pub async fn recent(limit: i64) -> Result<Vec<BookHit>, String> {
    get(&format!("/api/recent?limit={limit}")).await
}

pub async fn random() -> Result<BookHit, String> {
    get("/api/books/random").await
}

/// Minimal percent-encoding, safe for both query components and path
/// segments (space → `%20`; `+` is literal in paths, so never emitted).
pub fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Back-decode a `+`-encoded query value.
pub fn urldecode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = &s[i + 1..i + 3];
                if let Ok(v) = u8::from_str_radix(hex, 16) {
                    out.push(v);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
