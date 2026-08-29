//! CLI monitoring surface: read-only views over the store backing `status`,
//! `watch`, `runs` and `jobs`. Every collector here is a cheap DB read — no
//! provider instantiation, no mirror walks (those stay one-off inside
//! `status`) — so the `watch` loop can refresh every couple of seconds
//! without touching the library tree.
//!
//! Layering: the pure helpers (human bytes/duration, truncation, stall
//! hints, table padding) are unit-tested below; the async collectors return
//! ready-to-print blocks and surface read failures to the caller (the
//! `watch` loop degrades per section, one-shot commands propagate).

use std::fmt::Write as _;

use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

use bookshelf_core::StorePostgres;
use bookshelf_core::domain::SyncRun;
use bookshelf_core::observability::ActiveRun;

use crate::queue::JobRow;

/// The daemon rewrites `daemon_heartbeat` every 5 s (worker) / 30 s
/// (scheduler tick during long cycles); past this the process is gone.
const HEARTBEAT_FRESH_SECS: i64 = 90;
/// `transferring` with no new itemize line for this long → trickle/stall hint.
const ITEM_SILENCE_HINT_SECS: i64 = 120;
/// `listing` running longer than this → server-side walk hint.
const LISTING_HINT_SECS: i64 = 300;

// ---------------------------------------------------------------------------
// Section collectors (async, DB-backed)
// ---------------------------------------------------------------------------

/// DAEMON: process liveness + scheduler anchors + last rsync host.
pub async fn daemon_section(store: &StorePostgres, source: &str) -> anyhow::Result<String> {
    let heartbeat = store.get_meta(source, "daemon_heartbeat").await?;
    let next_full_sync = store.get_meta(source, "next_full_sync").await?;
    let last_feed_check = store.get_meta(source, "last_feed_check").await?;
    let rsync_host = store.get_meta(source, "last_rsync_host").await?;

    let heartbeat = match heartbeat
        .as_deref()
        .and_then(|v| OffsetDateTime::parse(v, &Rfc3339).ok())
    {
        None => "no heartbeat".to_string(),
        Some(t) => {
            let age = (OffsetDateTime::now_utc() - t).whole_seconds();
            if age < HEARTBEAT_FRESH_SECS {
                format!("alive ({age}s ago)")
            } else {
                format!("STALE ({})", human_duration(age))
            }
        }
    };
    Ok(format!(
        "DAEMON\n  \
         heartbeat: {heartbeat}\n  \
         next full sync: {}\n  \
         last feed check: {}\n  \
         rsync host: {}",
        fmt_meta_dt(next_full_sync),
        fmt_meta_dt(last_feed_check),
        rsync_host.as_deref().unwrap_or("·"),
    ))
}

/// ACTIVE CYCLE: the live `active_run` snapshot, or the idle line.
pub async fn active_cycle_section(store: &StorePostgres, source: &str) -> anyhow::Result<String> {
    let raw = store.get_meta(source, "active_run").await?;
    let Some(raw) = raw.filter(|v| !v.trim().is_empty()) else {
        return Ok("ACTIVE CYCLE\n  idle (no active cycle)".to_string());
    };
    let Some(run) = parse_active_run(&raw) else {
        // A ghost snapshot matters — say so instead of pretending idle.
        return Ok(format!(
            "ACTIVE CYCLE\n  unparsable active_run meta: {}",
            truncate_line(&raw, 80)
        ));
    };

    let now = OffsetDateTime::now_utc();
    let started = OffsetDateTime::parse(&run.started_at, &Rfc3339).ok();
    let elapsed = started.map(|t| (now - t).whole_seconds());
    let item_age = run
        .last_item_at
        .as_deref()
        .and_then(|v| OffsetDateTime::parse(v, &Rfc3339).ok())
        .map(|t| (now - t).whole_seconds());

    let run_id = run
        .run_id
        .map(|r| r.to_string())
        .unwrap_or_else(|| "·".to_string());
    let mut out = format!(
        "ACTIVE CYCLE\n  {} — job {} run {run_id} — phase {}",
        run.kind, run.job_id, run.phase,
    );
    if let Some(el) = elapsed {
        out.push_str(&format!(", elapsed {}", human_duration(el)));
    }
    out.push_str(&format!(
        "\n  files: {}, bytes: {}",
        run.files,
        human_bytes(run.bytes)
    ));
    if run.phase == "transferring" && elapsed.is_some_and(|e| e > 0) {
        let rate = run.bytes as f64 / elapsed.unwrap_or(1) as f64;
        out.push_str(&format!(" ({}/s)", human_bytes(rate as u64)));
    }
    if let Some(host) = &run.host {
        out.push_str(&format!("\n  host: {host}"));
    }
    match item_age {
        Some(age) => out.push_str(&format!("\n  last item: {} ago", human_duration(age))),
        None => out.push_str("\n  last item: none yet"),
    }
    if let Some(hint) = stall_hint(&run.phase, elapsed.unwrap_or(0), item_age) {
        out.push_str(&format!("\n  {hint}"));
    }
    Ok(out)
}

/// QUEUE: queued/running depth, source-scoped (same query the daemon's
/// snapshot refresh uses).
pub async fn queue_section(store: &StorePostgres, source: &str) -> anyhow::Result<String> {
    let depths: Vec<(String, i64)> = bookshelf_core::sqlx::query_as(
        "SELECT status, count(*) FROM jobs \
         WHERE status IN ('queued', 'running') AND source = $1 GROUP BY status",
    )
    .bind(source)
    .fetch_all(store.pool())
    .await?;
    let count = |status: &str| {
        depths
            .iter()
            .find(|(s, _)| s == status)
            .map(|(_, c)| *c)
            .unwrap_or(0)
    };
    Ok(format!(
        "QUEUE\n  queued: {}, running: {}",
        count("queued"),
        count("running")
    ))
}

/// BOOKS/FILES: per-status counts — the source data behind the
/// `librarian.books` / `librarian.files` gauges.
pub async fn counts_section(store: &StorePostgres, source: &str) -> anyhow::Result<String> {
    let books = store.book_status_counts(source).await?;
    let files = store.file_status_counts(source).await?;
    Ok(format!(
        "BOOKS\n{}\nFILES\n{}",
        pairs(&books),
        pairs(&files)
    ))
}

/// The `watch` frame: everything cheap, one screenful. Section read failures
/// degrade to an inline note — a transient hiccup must not kill the loop.
pub async fn watch_frame(store: &StorePostgres, source: &str, runs_limit: i64) -> String {
    let now = OffsetDateTime::now_utc();
    let mut frame = format!(
        "librarian watch — {source} — {} (Ctrl-C to exit)",
        fmt_dt(now)
    );
    for section in [
        daemon_section(store, source).await,
        active_cycle_section(store, source).await,
        queue_section(store, source).await,
        counts_section(store, source).await,
        runs_section(store, source, runs_limit).await,
    ] {
        match section {
            Ok(text) => frame.push_str(&format!("\n\n{text}")),
            Err(e) => frame.push_str(&format!("\n\n(error: {e})")),
        }
    }
    frame.push('\n');
    frame
}

/// LAST RUNS block for the `watch` frame.
async fn runs_section(store: &StorePostgres, source: &str, limit: i64) -> anyhow::Result<String> {
    let runs = store.recent_runs(source, limit).await?;
    let now = OffsetDateTime::now_utc();
    Ok(format!("LAST {limit} RUNS\n{}", runs_table(&runs, now)))
}

// ---------------------------------------------------------------------------
// Table renderers (pure)
// ---------------------------------------------------------------------------

/// The `runs` view: id, cycle, started→finished, duration, transfers, abort.
/// In-flight runs render '· running' with a live duration.
pub fn runs_table(runs: &[SyncRun], now: OffsetDateTime) -> String {
    let rows: Vec<Vec<String>> = runs
        .iter()
        .map(|r| {
            let finished = match r.finished_at {
                Some(f) => fmt_dt(f),
                None => "· running".to_string(),
            };
            let duration = match r.finished_at {
                Some(f) => human_duration((f - r.started_at).whole_seconds()),
                None => human_duration((now - r.started_at).whole_seconds()),
            };
            vec![
                r.id.to_string(),
                r.cycle.clone(),
                format!("{}→{}", fmt_dt(r.started_at), finished),
                duration,
                opt_str(r.transferred_files),
                r.transferred_bytes
                    .map(|b| human_bytes(b.max(0) as u64))
                    .unwrap_or_else(|| "·".into()),
                opt_str(r.new_books),
                opt_str(r.enriched),
                r.aborted_reason.clone().unwrap_or_else(|| "·".into()),
            ]
        })
        .collect();
    table(
        &[
            "id",
            "cycle",
            "started→finished",
            "duration",
            "files",
            "bytes",
            "new",
            "enriched",
            "aborted",
        ],
        &rows,
        &[true, false, false, false, true, true, true, true, false],
    )
}

/// The `jobs` view: queue identity, scheduling facts and the error column
/// (flattened, truncated — errors can be multi-line stack-ish strings).
pub fn jobs_table(jobs: &[JobRow]) -> String {
    let rows: Vec<Vec<String>> = jobs
        .iter()
        .map(|j| {
            vec![
                j.id.to_string(),
                j.kind.clone(),
                j.status.clone(),
                j.priority.to_string(),
                j.attempts.to_string(),
                j.origin.clone(),
                j.run_id
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "·".into()),
                fmt_dt(j.enqueued_at),
                j.started_at.map(fmt_dt).unwrap_or_else(|| "·".into()),
                j.finished_at.map(fmt_dt).unwrap_or_else(|| "·".into()),
                j.error
                    .as_deref()
                    .map(|e| truncate_line(e, 60))
                    .unwrap_or_default(),
            ]
        })
        .collect();
    table(
        &[
            "id",
            "kind",
            "status",
            "prio",
            "att",
            "origin",
            "run",
            "enqueued_at",
            "started_at",
            "finished_at",
            "error",
        ],
        &rows,
        &[
            true, false, false, true, true, false, true, false, false, false, false,
        ],
    )
}

/// Left/right-aligned monospace table with two-space gutters; column widths
/// follow the widest cell (char count — cells may carry ·, → or ⚠).
pub fn table(headers: &[&str], rows: &[Vec<String>], right: &[bool]) -> String {
    let ncols = headers.len();
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate().take(ncols) {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    let render = |cells: &[String]| {
        let mut line = String::new();
        for (i, cell) in cells.iter().enumerate().take(ncols) {
            if i > 0 {
                line.push_str("  ");
            }
            if right.get(i).copied().unwrap_or(false) {
                let _ = write!(line, "{cell:>0$}", widths[i]);
            } else {
                let _ = write!(line, "{cell:<0$}", widths[i]);
            }
        }
        line.trim_end().to_string()
    };
    let header_cells: Vec<String> = headers.iter().map(|h| h.to_string()).collect();
    let mut out = render(&header_cells);
    for row in rows {
        out.push('\n');
        out.push_str(&render(row));
    }
    out
}

// ---------------------------------------------------------------------------
// Pure formatting helpers
// ---------------------------------------------------------------------------

/// Parse the `active_run` meta value; None on any deviation (the watch loop
/// renders the idle/unparsable lines instead of failing).
pub fn parse_active_run(json: &str) -> Option<ActiveRun> {
    serde_json::from_str(json).ok()
}

/// Stall hints for a live cycle — the trickle case that motivated them:
/// rsync can sit minutes without a new itemize line while trickling one huge
/// file, and the first listing is the *remote's* walk, not ours.
pub fn stall_hint(phase: &str, elapsed_s: i64, last_item_age_s: Option<i64>) -> Option<String> {
    match phase {
        "transferring" => {
            let age = last_item_age_s?;
            (age > ITEM_SILENCE_HINT_SECS)
                .then(|| format!("⚠ no items for {}m (trickle/stall?)", age / 60))
        }
        "listing" => (elapsed_s > LISTING_HINT_SECS)
            .then(|| "⚠ listing still running (server-side walk)".to_string()),
        _ => None,
    }
}

/// 1024-based human bytes: `0 B`, `1023 B`, `1.50 KiB`, `9.42 MiB`, …
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 7] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

/// Coarse wall-clock: `59s`, `2m 5s`, `2h 12m`, `1d 1h` — good enough to
/// eyeball cycles and silences; never sub-second noise.
pub fn human_duration(secs: i64) -> String {
    let s = secs.max(0);
    if s < 60 {
        format!("{s}s")
    } else if s < 3600 {
        format!("{}m {}s", s / 60, s % 60)
    } else if s < 86_400 {
        format!("{}h {}m", s / 3600, (s % 3600) / 60)
    } else {
        format!("{}d {}h", s / 86_400, (s % 86_400) / 3600)
    }
}

/// One-line bounded string for table cells: flatten whitespace, cap at
/// `max_chars` with an ellipsis.
pub fn truncate_line(s: &str, max_chars: usize) -> String {
    let flat = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max_chars {
        flat
    } else {
        let keep: String = flat.chars().take(max_chars.saturating_sub(1)).collect();
        format!("{keep}…")
    }
}

fn opt_str<T: std::fmt::Display>(v: Option<T>) -> String {
    v.map(|v| v.to_string()).unwrap_or_else(|| "·".to_string())
}

fn fmt_dt(t: OffsetDateTime) -> String {
    t.to_offset(time::UtcOffset::UTC)
        .format(&format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second]"
        ))
        .unwrap_or_else(|_| t.to_string())
}

/// Meta dates arrive as RFC3339 text; render compact UTC, fall back to the
/// raw string when a key holds something unexpected.
fn fmt_meta_dt(value: Option<String>) -> String {
    match value {
        None => "·".to_string(),
        Some(v) => match OffsetDateTime::parse(&v, &Rfc3339) {
            Ok(t) => fmt_dt(t),
            Err(_) => v,
        },
    }
}

fn pairs(counts: &[(String, i64)]) -> String {
    if counts.is_empty() {
        return "  (none)".to_string();
    }
    let joined: Vec<String> = counts.iter().map(|(s, c)| format!("{s}: {c}")).collect();
    format!("  {}", joined.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_bytes_scales() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1023), "1023 B");
        assert_eq!(human_bytes(1024), "1.00 KiB");
        assert_eq!(human_bytes(1536), "1.50 KiB");
        assert_eq!(human_bytes(9_876_543), "9.42 MiB");
        assert_eq!(human_bytes(10_590_684_694), "9.86 GiB");
    }

    #[test]
    fn human_duration_buckets() {
        assert_eq!(human_duration(0), "0s");
        assert_eq!(human_duration(59), "59s");
        assert_eq!(human_duration(60), "1m 0s");
        assert_eq!(human_duration(125), "2m 5s");
        assert_eq!(human_duration(3600), "1h 0m");
        assert_eq!(human_duration(7920), "2h 12m");
        assert_eq!(human_duration(90_000), "1d 1h");
        assert_eq!(human_duration(-5), "0s"); // clock skew never goes negative
    }

    #[test]
    fn truncate_line_flattens_and_caps() {
        assert_eq!(truncate_line("boom: line1\nline2", 60), "boom: line1 line2");
        let long = truncate_line(&"x".repeat(100), 60);
        assert_eq!(long.chars().count(), 60);
        assert!(long.ends_with('…'));
        assert_eq!(truncate_line("short", 60), "short");
    }

    #[test]
    fn stall_hints_fire_per_phase() {
        // transferring, silent for 3 min → trickle/stall hint
        assert_eq!(
            stall_hint("transferring", 600, Some(180)).as_deref(),
            Some("⚠ no items for 3m (trickle/stall?)")
        );
        // fresh items → no hint
        assert_eq!(stall_hint("transferring", 600, Some(30)), None);
        // no item yet at all → unknown, no hint (rsync may just be starting)
        assert_eq!(stall_hint("transferring", 600, None), None);
        // exactly at the threshold stays quiet, past it hints
        assert_eq!(stall_hint("transferring", 600, Some(120)), None);
        assert!(stall_hint("transferring", 600, Some(121)).is_some());
        // listing past 5 min → server-side walk hint
        assert_eq!(
            stall_hint("listing", 301, None).as_deref(),
            Some("⚠ listing still running (server-side walk)")
        );
        assert_eq!(stall_hint("listing", 300, None), None);
        // other phases never hint
        assert_eq!(stall_hint("ingesting", 9_000, Some(9_000)), None);
        assert_eq!(stall_hint("repairing", 9_000, Some(9_000)), None);
    }

    #[test]
    fn active_run_parses_from_meta_json() {
        let json = r#"{"job_id":42,"run_id":7,"kind":"full_cycle","phase":"transferring",
            "started_at":"2026-08-29T10:00:00Z","files":1234,"bytes":9876543,
            "last_item_at":"2026-08-29T10:05:12Z","host":"gutenberg.pglaf.org"}"#;
        let run = parse_active_run(json).expect("valid snapshot parses");
        assert_eq!(run.kind, "full_cycle");
        assert_eq!(run.phase, "transferring");
        assert_eq!(run.job_id, 42);
        assert_eq!(run.run_id, Some(7));
        assert_eq!(run.bytes, 9_876_543);
        assert_eq!(run.host.as_deref(), Some("gutenberg.pglaf.org"));
        // garbage / missing fields → None, never a panic (watch must survive)
        assert!(parse_active_run("not json").is_none());
        assert!(parse_active_run("{}").is_none());
    }

    #[test]
    fn table_aligns_columns() {
        let rows = vec![
            vec!["1".to_string(), "full".to_string(), "9.40 MiB".to_string()],
            vec!["1024".to_string(), "feed".to_string(), "·".to_string()],
        ];
        let out = table(&["id", "cycle", "bytes"], &rows, &[true, false, true]);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "  id  cycle     bytes");
        assert_eq!(lines[1], "   1  full   9.40 MiB");
        assert_eq!(lines[2], "1024  feed          ·");
    }

    #[test]
    fn runs_and_jobs_tables_render_placeholders() {
        let run = SyncRun {
            id: 7,
            source: "cli-test".into(),
            cycle: "full".into(),
            started_at: OffsetDateTime::UNIX_EPOCH,
            finished_at: None,
            rsync_exit: None,
            transferred_files: None,
            transferred_bytes: None,
            new_books: None,
            enriched: None,
            files_failed: None,
            files_skipped: None,
            aborted_reason: None,
        };
        let out = runs_table(&[run], OffsetDateTime::UNIX_EPOCH);
        assert!(out.contains("· running"), "in-flight run marked:\n{out}");

        let job = JobRow {
            id: 9,
            source: "cli-test".into(),
            kind: "full_cycle".into(),
            payload: serde_json::json!({}),
            origin: "cli".into(),
            priority: 10,
            status: "queued".into(),
            attempts: 0,
            run_id: None,
            error: None,
            enqueued_at: OffsetDateTime::UNIX_EPOCH,
            started_at: None,
            finished_at: None,
        };
        let out = jobs_table(&[job]);
        assert!(out.contains("full_cycle"));
        assert!(out.contains("queued"));
    }
}
