//! Postgres-backed job queue. The daemon is the sole executor; CLI
//! subcommands and the scheduler are clients that INSERT + NOTIFY.
//!
//! Pickup is a single atomic statement using `FOR UPDATE SKIP LOCKED`;
//! wake-ups arrive via `LISTEN librarian_jobs` (5 s poll floor elsewhere).
//! Scheduled jobs (priority 0) always outrank cli jobs (priority 10).

use bookshelf_core::sqlx::postgres::{PgListener, PgPool};
use serde::Serialize;
use time::OffsetDateTime;

use crate::trigger::Trigger;

pub const NOTIFY_CHANNEL: &str = "librarian_jobs";

#[derive(Debug, Clone, bookshelf_core::sqlx::FromRow)]
pub struct JobRow {
    pub id: i64,
    pub source: String,
    pub kind: String,
    pub payload: serde_json::Value,
    pub origin: String,
    pub priority: i32,
    pub status: String,
    pub attempts: i32,
    pub run_id: Option<i64>,
    pub error: Option<String>,
    pub enqueued_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
}

#[derive(Clone)]
pub struct JobQueue {
    pool: PgPool,
}

impl JobQueue {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// INSERT a job and NOTIFY the daemon in one transaction.
    pub async fn enqueue(
        &self,
        source: &str,
        kind: &str,
        payload: &serde_json::Value,
        origin: Trigger,
    ) -> anyhow::Result<i64> {
        let mut tx = self.pool.begin().await?;
        let id: i64 = bookshelf_core::sqlx::query_scalar(
            "INSERT INTO jobs (source, kind, payload, origin, priority) \
             VALUES ($1, $2, $3, $4, $5) RETURNING id",
        )
        .bind(source)
        .bind(kind)
        .bind(payload)
        .bind(origin.as_str())
        .bind(origin.priority())
        .fetch_one(&mut *tx)
        .await?;
        let _ = bookshelf_core::sqlx::query("SELECT pg_notify($1, $2)")
            .bind(NOTIFY_CHANNEL)
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(id)
    }

    /// Scheduler enqueue: skip when the same kind is already queued/running
    /// (prevents pileup when a cycle outlasts its interval). Returns the new
    /// job id, or None when coalesced.
    pub async fn enqueue_coalesced(
        &self,
        source: &str,
        kind: &str,
        payload: &serde_json::Value,
    ) -> anyhow::Result<Option<i64>> {
        let mut tx = self.pool.begin().await?;
        let id: Option<i64> = bookshelf_core::sqlx::query_scalar(
            "INSERT INTO jobs (source, kind, payload, origin, priority) \
             SELECT $1, $2, $3, 'schedule', 0 \
             WHERE NOT EXISTS ( \
               SELECT 1 FROM jobs WHERE source = $1 AND kind = $2 \
               AND status IN ('queued', 'running')) \
             RETURNING id",
        )
        .bind(source)
        .bind(kind)
        .bind(payload)
        .fetch_optional(&mut *tx)
        .await?;
        if id.is_some() {
            let _ = bookshelf_core::sqlx::query("SELECT pg_notify($1, $2)")
                .bind(NOTIFY_CHANNEL)
                .bind(id.map(|i| i.to_string()).unwrap_or_default())
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(id)
    }

    /// Atomically claim the next queued job: priority ASC, enqueued_at ASC,
    /// FOR UPDATE SKIP LOCKED.
    pub async fn pick_next(&self) -> anyhow::Result<Option<JobRow>> {
        let job = bookshelf_core::sqlx::query_as::<_, JobRow>(
            "UPDATE jobs SET status = 'running', started_at = now() \
             WHERE id = ( \
               SELECT id FROM jobs WHERE status = 'queued' \
               ORDER BY priority, enqueued_at LIMIT 1 FOR UPDATE SKIP LOCKED) \
             RETURNING *",
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(job)
    }

    /// Terminal completion: done (with run_id) or failed (with error).
    pub async fn complete(
        &self,
        id: i64,
        run_id: Option<i64>,
        error: Option<&str>,
    ) -> anyhow::Result<()> {
        let status = if error.is_some() { "failed" } else { "done" };
        bookshelf_core::sqlx::query(
            "UPDATE jobs SET status = $2, run_id = $3, error = $4, finished_at = now() \
             WHERE id = $1",
        )
        .bind(id)
        .bind(status)
        .bind(run_id)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Interrupted mid-run: back to queued with attempts+1 (≥3 → failed).
    /// Returns true when the job was requeued (false = permanently failed).
    pub async fn requeue_interrupted(&self, id: i64) -> anyhow::Result<bool> {
        let status: Option<String> = bookshelf_core::sqlx::query_scalar(
            "UPDATE jobs SET attempts = attempts + 1, \
             status = CASE WHEN attempts + 1 >= 3 THEN 'failed' ELSE 'queued' END, \
             error = CASE WHEN attempts + 1 >= 3 THEN 'interrupted too many times' ELSE error END, \
             finished_at = CASE WHEN attempts + 1 >= 3 THEN now() ELSE finished_at END \
             WHERE id = $1 RETURNING status",
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await?;
        Ok(status.as_deref() == Some("queued"))
    }

    pub async fn get(&self, id: i64) -> anyhow::Result<Option<JobRow>> {
        let job = bookshelf_core::sqlx::query_as::<_, JobRow>("SELECT * FROM jobs WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(job)
    }

    /// Queue position of a job (how many queued jobs are ahead of it).
    pub async fn position(&self, id: i64) -> anyhow::Result<i64> {
        Ok(bookshelf_core::sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM jobs WHERE status = 'queued' AND id != $1 \
             AND (priority, enqueued_at) <= \
               (SELECT (priority, enqueued_at) FROM jobs WHERE id = $1)",
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await?)
    }
}

/// Listener convenience for the daemon worker loop.
pub async fn listen(pool: &PgPool) -> anyhow::Result<PgListener> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(NOTIFY_CHANNEL).await?;
    Ok(listener)
}

/// Payload helper: serialize CycleOpts-shaped payloads uniformly.
pub fn payload<T: Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
}
