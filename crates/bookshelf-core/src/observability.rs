//! Wire types for live observability. Plain serde structs only — the
//! OpenTelemetry sink lives in the `librarian` crate, never here. These are
//! the shapes the daemon publishes (today: the `active_run` row in `meta`,
//! read back by the CLI/web surfaces).

use serde::{Deserialize, Serialize};

/// One live cycle, published to `meta` under the `active_run` key (JSON)
/// every few seconds and cleared on every cycle exit path.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ActiveRun {
    /// Job row driving this cycle (0 when unresolvable, e.g. manual runs).
    pub job_id: i64,
    pub run_id: Option<i64>,
    /// Job kind, e.g. "full_cycle" | "feed_cycle".
    pub kind: String,
    /// "listing" | "transferring" | "ingesting" | "repairing".
    pub phase: String,
    /// RFC3339 cycle start.
    pub started_at: String,
    /// Files transferred by the current rsync attempt.
    pub files: u64,
    /// Bytes transferred by the current rsync attempt.
    pub bytes: u64,
    /// RFC3339 of the last itemize line, None before the first item.
    pub last_item_at: Option<String>,
    /// rsync host currently in use (primary or fallback).
    pub host: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_run_serde_roundtrip() {
        let run = ActiveRun {
            job_id: 42,
            run_id: Some(7),
            kind: "full_cycle".into(),
            phase: "transferring".into(),
            started_at: "2026-08-29T10:00:00Z".into(),
            files: 1234,
            bytes: 9_876_543,
            last_item_at: Some("2026-08-29T10:05:12Z".into()),
            host: Some("gutenberg.pglaf.org".into()),
        };
        let json = serde_json::to_string(&run).unwrap();
        assert_eq!(serde_json::from_str::<ActiveRun>(&json).unwrap(), run);
    }

    #[test]
    fn active_run_optional_fields_survive_null() {
        let run = ActiveRun {
            job_id: 0,
            run_id: None,
            kind: "feed_cycle".into(),
            phase: "listing".into(),
            started_at: "2026-08-29T10:00:00Z".into(),
            files: 0,
            bytes: 0,
            last_item_at: None,
            host: None,
        };
        let json = serde_json::to_string(&run).unwrap();
        assert!(json.contains(r#""run_id":null"#));
        assert_eq!(serde_json::from_str::<ActiveRun>(&json).unwrap(), run);
    }
}
