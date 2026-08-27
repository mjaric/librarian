//! Polite HTTP client: one shared `reqwest::Client`, a global rate limiter
//! (every request — feed and repair — waits for its slot), a concurrency
//! semaphore for parallel downloads, and rich `FetchError`s that carry the
//! status, `Retry-After`, response headers and the first 500 bytes of the
//! body so the triage layer can decide without re-fetching.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use tokio::sync::{Mutex, Semaphore};

use crate::domain::Json;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchErrorKind {
    Timeout,
    Connect,
    Status,
    Body,
}

#[derive(Debug, Clone)]
pub struct FetchError {
    pub url: String,
    pub kind: FetchErrorKind,
    pub status: Option<u16>,
    pub retry_after: Option<Duration>,
    pub body_head: String,
    pub headers: Json,
}

impl FetchError {
    async fn from_response(url: &str, resp: reqwest::Response) -> Self {
        let status = resp.status().as_u16();
        let headers: Json = resp
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_string(),
                    Json::String(v.to_str().unwrap_or("<binary>").to_string()),
                )
            })
            .collect::<serde_json::Map<String, Json>>()
            .into();
        let retry_after = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        // Error bodies are small; read fully and keep the head.
        let body_head = resp
            .text()
            .await
            .unwrap_or_default()
            .chars()
            .take(500)
            .collect();
        Self {
            url: url.to_string(),
            kind: FetchErrorKind::Status,
            status: Some(status),
            retry_after,
            body_head,
            headers,
        }
    }

    fn from_err(url: &str, e: &reqwest::Error) -> Self {
        let kind = if e.is_timeout() {
            FetchErrorKind::Timeout
        } else if e.is_connect() {
            FetchErrorKind::Connect
        } else {
            FetchErrorKind::Body
        };
        Self {
            url: url.to_string(),
            kind,
            status: e.status().map(|s| s.as_u16()),
            retry_after: None,
            body_head: String::new(),
            headers: Json::Null,
        }
    }
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.kind, self.status) {
            (FetchErrorKind::Status, Some(s)) => write!(f, "HTTP {s}"),
            (k, _) => write!(f, "{k:?} error"),
        }?;
        if !self.body_head.is_empty() {
            write!(
                f,
                ": {}",
                self.body_head.chars().take(120).collect::<String>()
            )?;
        }
        Ok(())
    }
}

pub struct PoliteClient {
    client: reqwest::Client,
    next_slot: Mutex<Instant>,
    interval: Duration,
    semaphore: Arc<Semaphore>,
}

impl PoliteClient {
    /// Content-Type of a response as a lossy string (empty when absent).
    pub fn content_type_of(resp: &reqwest::Response) -> String {
        resp.headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string()
    }

    pub fn new(
        user_agent: &str,
        timeout: Duration,
        request_interval: Duration,
        max_parallel: usize,
    ) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(user_agent)
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()
            .context("building reqwest client")?;
        Ok(Self {
            client,
            next_slot: Mutex::new(Instant::now()),
            interval: request_interval,
            semaphore: Arc::new(Semaphore::new(max_parallel.max(1))),
        })
    }

    /// Global rate limiter: every request sleeps to its slot, then the slot
    /// advances by one interval. Queued requests therefore space themselves.
    async fn throttle(&self) {
        let mut slot = self.next_slot.lock().await;
        let now = Instant::now();
        if *slot > now {
            tokio::time::sleep(*slot - now).await;
        }
        *slot = Instant::now() + self.interval;
    }

    pub fn semaphore(&self) -> &Arc<Semaphore> {
        &self.semaphore
    }

    /// GET a URL politely. Returns the response only for 2xx statuses; every
    /// other outcome is a `FetchError` carrying everything triage needs.
    pub async fn fetch(&self, url: &str) -> Result<reqwest::Response, FetchError> {
        self.throttle().await;
        match self.client.get(url).send().await {
            Ok(resp) if resp.status().is_success() => Ok(resp),
            Ok(resp) => Err(FetchError::from_response(url, resp).await),
            Err(e) => Err(FetchError::from_err(url, &e)),
        }
    }
}
