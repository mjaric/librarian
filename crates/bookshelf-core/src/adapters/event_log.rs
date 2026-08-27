//! Append-only JSONL event log: `{library_dir}/events.jsonl`.
//!
//! One JSON object per line, never read, rewritten or truncated by this
//! program. A single OS file handle opened `create+append` behind a mutex —
//! appends of lines smaller than PIPE_BUF are atomic on Linux, and the mutex
//! serializes our own writers.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use parking_lot::Mutex;

use anyhow::Context;
use async_trait::async_trait;
use time::format_description::well_known::Rfc3339;

use crate::domain::{EventKind, EventSink, Json};

pub struct EventLog {
    file: Mutex<std::fs::File>,
}

impl EventLog {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating event log dir {}", parent.display()))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("opening event log {}", path.display()))?;
        Ok(Self { file: Mutex::new(file) })
    }

    /// Write one event line + flush. Blocking; cheap enough for 76k-book runs.
    pub fn emit_sync(
        &self,
        source: &str,
        kind: EventKind,
        book_id: Option<i64>,
        detail: Json,
    ) -> anyhow::Result<()> {
        let ts = time::OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into());
        let line = serde_json::json!({
            "ts": ts,
            "source": source,
            "kind": kind.as_str(),
            "book_id": book_id,
            "detail": detail,
        });
        let mut f = self.file.lock();
        writeln!(f, "{line}")?;
        f.flush()?;
        Ok(())
    }
}

#[async_trait]
impl EventSink for EventLog {
    async fn emit(&self, source: &str, kind: EventKind, book_id: Option<i64>, detail: Json) {
        if let Err(e) = self.emit_sync(source, kind, book_id, detail) {
            tracing::error!(kind = kind.as_str(), error = %e, "event log write failed");
        }
    }
}
