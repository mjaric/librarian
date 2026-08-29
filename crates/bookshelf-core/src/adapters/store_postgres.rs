//! PostgreSQL store: sqlx pool + embedded migrations + every domain query.
//! Runtime `sqlx::query`/`query_as` only — no `query!` macros, so no
//! DATABASE_URL at compile time and no offline `.sqlx` cache.

use anyhow::Context;
use sqlx::postgres::{PgPool, PgPoolOptions};
use time::OffsetDateTime;

use crate::domain::{
    Book, BookFile, BookHitRow, CatalogScope, Category, CategoryCountRow, SyncRun,
};

pub struct StorePostgres {
    pool: PgPool,
}

impl StorePostgres {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        // Never log credentials: mask the userinfo part of the URL.
        let shown = match database_url.split_once("://") {
            Some((scheme, rest)) => match rest.split_once('@') {
                Some((_, hostpart)) => format!("{scheme}://***@{hostpart}"),
                None => database_url.to_string(),
            },
            None => database_url.to_string(),
        };
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(std::time::Duration::from_secs(10))
            .connect(database_url)
            .await
            .with_context(|| format!("connecting to postgres ({shown})"))?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Apply embedded migrations (idempotent; tracked in `_sqlx_migrations`).
    pub async fn migrate(&self) -> anyhow::Result<()> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .context("applying sqlx migrations")?;
        Ok(())
    }

    // -- meta ------------------------------------------------------------

    pub async fn get_meta(&self, source: &str, key: &str) -> anyhow::Result<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM meta WHERE source = $1 AND key = $2")
                .bind(source)
                .bind(key)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|r| r.0))
    }

    pub async fn set_meta(&self, source: &str, key: &str, value: &str) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO meta (source, key, value) VALUES ($1, $2, $3) \
             ON CONFLICT (source, key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(source)
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Idempotent delete of one meta key (e.g. `active_run` on cycle exit).
    pub async fn clear_meta(&self, source: &str, key: &str) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM meta WHERE source = $1 AND key = $2")
            .bind(source)
            .bind(key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // -- sync runs ---------------------------------------------------------

    pub async fn start_run(&self, source: &str, cycle: &str) -> anyhow::Result<i64> {
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO sync_runs (source, cycle, started_at) VALUES ($1, $2, now()) RETURNING id",
        )
        .bind(source)
        .bind(cycle)
        .fetch_one(&self.pool)
        .await?;
        Ok(id)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn finish_run(
        &self,
        id: i64,
        rsync_exit: Option<i32>,
        transferred_files: i64,
        transferred_bytes: i64,
        new_books: i64,
        enriched: i64,
        files_failed: i64,
        files_skipped: i64,
        aborted_reason: Option<&str>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE sync_runs SET finished_at = now(), rsync_exit = $2, \
             transferred_files = $3, transferred_bytes = $4, new_books = $5, \
             enriched = $6, files_failed = $7, files_skipped = $8, aborted_reason = $9 \
             WHERE id = $1",
        )
        .bind(id)
        .bind(rsync_exit)
        .bind(transferred_files as i32)
        .bind(transferred_bytes)
        .bind(new_books as i32)
        .bind(enriched as i32)
        .bind(files_failed as i32)
        .bind(files_skipped as i32)
        .bind(aborted_reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn last_run(&self, source: &str) -> anyhow::Result<Option<SyncRun>> {
        let run = sqlx::query_as::<_, SyncRun>(
            "SELECT * FROM sync_runs WHERE source = $1 AND finished_at IS NOT NULL \
             ORDER BY id DESC LIMIT 1",
        )
        .bind(source)
        .fetch_optional(&self.pool)
        .await?;
        Ok(run)
    }

    /// Recent runs for one source, newest first — including in-flight ones
    /// (finished_at IS NULL; the CLI `runs` view renders those '· running').
    pub async fn recent_runs(&self, source: &str, limit: i64) -> anyhow::Result<Vec<SyncRun>> {
        let runs = sqlx::query_as::<_, SyncRun>(
            "SELECT * FROM sync_runs WHERE source = $1 ORDER BY id DESC LIMIT $2",
        )
        .bind(source)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(runs)
    }

    // -- books -----------------------------------------------------------

    pub async fn get_book(&self, source: &str, id: i64) -> anyhow::Result<Option<Book>> {
        let book = sqlx::query_as::<_, Book>("SELECT * FROM books WHERE source = $1 AND id = $2")
            .bind(source)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(book)
    }

    /// All book ids for one source, ascending — the reconcile step diffs
    /// this against the local mirror every cycle (~76k ids is fine).
    pub async fn book_ids(&self, source: &str) -> anyhow::Result<Vec<i64>> {
        let rows: Vec<(i64,)> =
            sqlx::query_as("SELECT id FROM books WHERE source = $1 ORDER BY id")
                .bind(source)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    /// Insert a new book. Returns false when the row already existed.
    pub async fn insert_book(&self, book: &NewBook<'_>) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "INSERT INTO books (source, id, type, title, language, issued, publisher, \
             rights, description, reading_ease, downloads, authors, subjects, bookshelves, \
             status, first_seen, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, now(), now()) \
             ON CONFLICT (source, id) DO NOTHING",
        )
        .bind(book.source)
        .bind(book.id)
        .bind(&book.r#type)
        .bind(&book.title)
        .bind(&book.language)
        .bind(book.issued)
        .bind(&book.publisher)
        .bind(&book.rights)
        .bind(&book.description)
        .bind(&book.reading_ease)
        .bind(book.downloads)
        .bind(&book.authors)
        .bind(&book.subjects)
        .bind(&book.bookshelves)
        .bind(book.status)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() == 1)
    }

    pub async fn update_book_fields(&self, book: &NewBook<'_>) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE books SET type = $3, title = $4, language = $5, issued = $6, \
             publisher = $7, rights = $8, description = $9, reading_ease = $10, \
             downloads = $11, authors = $12, subjects = $13, bookshelves = $14, \
             updated_at = now() WHERE source = $1 AND id = $2",
        )
        .bind(book.source)
        .bind(book.id)
        .bind(&book.r#type)
        .bind(&book.title)
        .bind(&book.language)
        .bind(book.issued)
        .bind(&book.publisher)
        .bind(&book.rights)
        .bind(&book.description)
        .bind(&book.reading_ease)
        .bind(book.downloads)
        .bind(&book.authors)
        .bind(&book.subjects)
        .bind(&book.bookshelves)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_book_status(
        &self,
        source: &str,
        id: i64,
        status: &str,
        last_error: Option<&str>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE books SET status = $3, last_error = $4, updated_at = now() \
             WHERE source = $1 AND id = $2",
        )
        .bind(source)
        .bind(id)
        .bind(status)
        .bind(last_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn book_status_counts(&self, source: &str) -> anyhow::Result<Vec<(String, i64)>> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT status, count(*) FROM books WHERE source = $1 GROUP BY status ORDER BY status",
        )
        .bind(source)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn file_status_counts(&self, source: &str) -> anyhow::Result<Vec<(String, i64)>> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT status, count(*) FROM book_files WHERE source = $1 GROUP BY status ORDER BY status",
        )
        .bind(source)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// failed_permanent → discovered, attempts = 0. Returns reset count.
    pub async fn retry_failed(&self, source: &str) -> anyhow::Result<u64> {
        let res = sqlx::query(
            "UPDATE books SET status = 'discovered', attempts = 0, last_error = NULL, \
             retry_at = NULL, updated_at = now() \
             WHERE source = $1 AND status = 'failed_permanent'",
        )
        .bind(source)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    // -- book files --------------------------------------------------------

    pub async fn get_files(&self, source: &str, book_id: i64) -> anyhow::Result<Vec<BookFile>> {
        let files = sqlx::query_as::<_, BookFile>(
            "SELECT * FROM book_files WHERE source = $1 AND book_id = $2 ORDER BY format",
        )
        .bind(source)
        .bind(book_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(files)
    }

    /// Insert or refresh a file row from RDF knowledge (url/extent/modified).
    /// Keeps terminal statuses of past runs; a fresh row starts pending.
    pub async fn upsert_file_knowledge(
        &self,
        source: &str,
        book_id: i64,
        format: &str,
        url: Option<&str>,
        bytes_expected: Option<i64>,
        remote_modified: Option<&str>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO book_files (source, book_id, format, url, bytes_expected, remote_modified, status) \
             VALUES ($1, $2, $3, $4, $5, $6, 'pending') \
             ON CONFLICT (source, book_id, format) DO UPDATE SET \
             url = EXCLUDED.url, bytes_expected = EXCLUDED.bytes_expected, \
             remote_modified = EXCLUDED.remote_modified",
        )
        .bind(source)
        .bind(book_id)
        .bind(format)
        .bind(url)
        .bind(bytes_expected)
        .bind(remote_modified)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_file_state(
        &self,
        source: &str,
        book_id: i64,
        format: &str,
        status: &str,
        path: Option<&str>,
        attempts: Option<i32>,
        retry_at: Option<OffsetDateTime>,
        last_error: Option<&str>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE book_files SET status = $4, path = COALESCE($5, path), \
             attempts = COALESCE($6, attempts), retry_at = $7, \
             last_error = $8 WHERE source = $1 AND book_id = $2 AND format = $3",
        )
        .bind(source)
        .bind(book_id)
        .bind(format)
        .bind(status)
        .bind(path)
        .bind(attempts)
        .bind(retry_at)
        .bind(last_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_file_done(
        &self,
        source: &str,
        book_id: i64,
        format: &str,
        path: &str,
        size_error: Option<&str>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE book_files SET status = 'done', path = $4, last_error = $5, \
             retry_at = NULL WHERE source = $1 AND book_id = $2 AND format = $3",
        )
        .bind(source)
        .bind(book_id)
        .bind(format)
        .bind(path)
        .bind(size_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Repair queue: pending files, or done files flagged size_mismatch,
    /// whose retry_at has matured (or was cleared for the next cycle).
    pub async fn repair_queue(
        &self,
        source: &str,
        only: Option<&[i64]>,
    ) -> anyhow::Result<Vec<BookFile>> {
        let sql = "SELECT * FROM book_files WHERE source = $1 \
             AND (status = 'pending' OR (status = 'done' AND last_error = 'size_mismatch')) \
             AND (retry_at IS NULL OR retry_at <= now()) \
             AND ($2::bigint[] IS NULL OR book_id = ANY($2)) \
             ORDER BY book_id, format";
        let files = sqlx::query_as::<_, BookFile>(sql)
            .bind(source)
            .bind(only)
            .fetch_all(&self.pool)
            .await?;
        Ok(files)
    }

    pub async fn repair_pending_count(&self, source: &str) -> anyhow::Result<(i64, i64)> {
        let (pending, failed): (i64, i64) = sqlx::query_as(
            "SELECT \
               count(*) FILTER (WHERE status = 'pending'), \
               count(*) FILTER (WHERE status = 'failed') \
             FROM book_files WHERE source = $1",
        )
        .bind(source)
        .fetch_one(&self.pool)
        .await?;
        Ok((pending, failed))
    }

    pub async fn min_retry_at(&self, source: &str) -> anyhow::Result<Option<OffsetDateTime>> {
        let v = sqlx::query_scalar::<_, Option<OffsetDateTime>>(
            "SELECT min(retry_at) FROM book_files WHERE source = $1 AND status = 'pending'",
        )
        .bind(source)
        .fetch_one(&self.pool)
        .await?;
        Ok(v)
    }

    // -- categories ----------------------------------------------------------

    /// Seed rows: (parent, leaf, bookshelf_id). ON CONFLICT DO NOTHING — RDF
    /// remains the leaf-set source of truth; the seed only supplies parents.
    pub async fn apply_category_seed(
        &self,
        source: &str,
        seed: &[(&str, &str, i32)],
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;
        for (parent, leaf, bookshelf_id) in seed {
            sqlx::query(
                "INSERT INTO categories (source, name, parent, bookshelf_id, updated_at) \
                 VALUES ($1, $2, $3, $4, now()) \
                 ON CONFLICT (source, name) DO NOTHING",
            )
            .bind(source)
            .bind(leaf)
            .bind(parent)
            .bind(bookshelf_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Create a leaf on first sight (parent NULL — resolved later by seed).
    /// Returns true when the row was newly inserted.
    pub async fn upsert_category_leaf(&self, source: &str, name: &str) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "INSERT INTO categories (source, name, parent, updated_at) \
             VALUES ($1, $2, NULL, now()) ON CONFLICT (source, name) DO NOTHING",
        )
        .bind(source)
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() == 1)
    }

    pub async fn get_category(&self, source: &str, name: &str) -> anyhow::Result<Option<Category>> {
        let c = sqlx::query_as::<_, Category>(
            "SELECT * FROM categories WHERE source = $1 AND name = $2",
        )
        .bind(source)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(c)
    }

    /// Link a book to a category. Returns true when the link was new.
    pub async fn link_category(
        &self,
        source: &str,
        book_id: i64,
        category: &str,
    ) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "INSERT INTO book_categories (source, book_id, category) VALUES ($1, $2, $3) \
             ON CONFLICT DO NOTHING",
        )
        .bind(source)
        .bind(book_id)
        .bind(category)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() == 1)
    }

    /// Unlink. Returns true when a link was actually removed.
    pub async fn unlink_category(
        &self,
        source: &str,
        book_id: i64,
        category: &str,
    ) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "DELETE FROM book_categories WHERE source = $1 AND book_id = $2 AND category = $3",
        )
        .bind(source)
        .bind(book_id)
        .bind(category)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() == 1)
    }

    pub async fn book_categories(&self, source: &str, book_id: i64) -> anyhow::Result<Vec<String>> {
        let names: Vec<(String,)> = sqlx::query_as(
            "SELECT category FROM book_categories WHERE source = $1 AND book_id = $2",
        )
        .bind(source)
        .bind(book_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(names.into_iter().map(|n| n.0).collect())
    }

    pub async fn category_count(&self, source: &str) -> anyhow::Result<i64> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM categories WHERE source = $1")
                .bind(source)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    // -- catalog reads (web UI) ---------------------------------------------
    //
    // Read-only projections for the web front end. `strpos(lower(..))` beats
    // ILIKE here: no wildcard escaping, and these tables are seq-scanned
    // either way at mirror scale (single-user local catalog).

    /// (books, categories, synced) totals.
    pub async fn catalog_stats(&self, source: &str) -> anyhow::Result<(i64, i64, i64)> {
        let stats = sqlx::query_as(
            "SELECT (SELECT count(*) FROM books WHERE source = $1), \
                    (SELECT count(*) FROM categories WHERE source = $1), \
                    (SELECT count(*) FROM books WHERE source = $1 AND status = 'synced')",
        )
        .bind(source)
        .fetch_one(&self.pool)
        .await?;
        Ok(stats)
    }

    /// Every leaf with its parent group and book count.
    pub async fn category_counts(&self, source: &str) -> anyhow::Result<Vec<CategoryCountRow>> {
        let rows = sqlx::query_as::<_, CategoryCountRow>(
            "SELECT c.parent, c.name AS leaf, \
                    (SELECT count(*) FROM book_categories bc \
                      WHERE bc.source = c.source AND bc.category = c.name) AS books \
             FROM categories c WHERE c.source = $1 \
             ORDER BY c.parent NULLS LAST, books DESC, c.name",
        )
        .bind(source)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// The card/spine projection shared by every catalog list query.
    const HIT_COLUMNS: &'static str = "b.id, b.title, b.authors, b.issued, b.language, b.downloads, \
        EXISTS(SELECT 1 FROM book_files f WHERE f.source = b.source AND f.book_id = b.id \
               AND f.format = 'cover' AND f.status = 'done') AS has_cover, \
        COALESCE((SELECT array_agg(bc.category ORDER BY bc.category) FROM book_categories bc \
                  WHERE bc.source = b.source AND bc.book_id = b.id), '{}') AS categories, \
        (SELECT f.bytes_expected FROM book_files f WHERE f.source = b.source AND f.book_id = b.id \
               AND f.format = 'txt' AND f.status = 'done' LIMIT 1) AS txt_bytes";

    /// Books shelved under one leaf, optionally text-filtered, paged by
    /// downloads. Returns (hits, total matching).
    pub async fn books_in_category(
        &self,
        source: &str,
        category: &str,
        q: &str,
        limit: i64,
        offset: i64,
    ) -> anyhow::Result<(Vec<BookHitRow>, i64)> {
        let hits = sqlx::query_as::<_, BookHitRow>(sqlx::AssertSqlSafe(format!(
            "SELECT {} FROM books b \
             JOIN book_categories bc ON bc.source = b.source AND bc.book_id = b.id \
             WHERE b.source = $1 AND bc.category = $2 \
             AND ($3::text = '' OR strpos(lower(b.title), lower($3)) > 0 \
                  OR strpos(lower(b.authors::text), lower($3)) > 0) \
             ORDER BY b.downloads DESC NULLS LAST, b.id \
             LIMIT $4 OFFSET $5",
            Self::HIT_COLUMNS
        )))
        .bind(source)
        .bind(category)
        .bind(q)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM books b \
             JOIN book_categories bc ON bc.source = b.source AND bc.book_id = b.id \
             WHERE b.source = $1 AND bc.category = $2 \
             AND ($3::text = '' OR strpos(lower(b.title), lower($3)) > 0 \
                  OR strpos(lower(b.authors::text), lower($3)) > 0)",
        )
        .bind(source)
        .bind(category)
        .bind(q)
        .fetch_one(&self.pool)
        .await?;
        Ok((hits, total))
    }

    /// Catalog search over title and/or author names, best-downloaded first.
    pub async fn search_books(
        &self,
        source: &str,
        q: &str,
        scope: CatalogScope,
        limit: i64,
    ) -> anyhow::Result<Vec<BookHitRow>> {
        let cond = match scope {
            CatalogScope::All => {
                "strpos(lower(b.title), lower($2)) > 0 OR strpos(lower(b.authors::text), lower($2)) > 0"
            }
            CatalogScope::Title => "strpos(lower(b.title), lower($2)) > 0",
            CatalogScope::Author => "strpos(lower(b.authors::text), lower($2)) > 0",
        };
        let hits = sqlx::query_as::<_, BookHitRow>(sqlx::AssertSqlSafe(format!(
            "SELECT {} FROM books b WHERE b.source = $1 AND ({cond}) \
             ORDER BY b.downloads DESC NULLS LAST, b.id LIMIT $3",
            Self::HIT_COLUMNS
        )))
        .bind(source)
        .bind(q)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(hits)
    }

    /// Most recently updated books (fresh on the shelf).
    pub async fn recent_books(&self, source: &str, limit: i64) -> anyhow::Result<Vec<BookHitRow>> {
        let hits = sqlx::query_as::<_, BookHitRow>(sqlx::AssertSqlSafe(format!(
            "SELECT {} FROM books b WHERE b.source = $1 \
             ORDER BY b.updated_at DESC, b.id DESC LIMIT $2",
            Self::HIT_COLUMNS
        )))
        .bind(source)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(hits)
    }

    /// One random hit ("surprise me").
    pub async fn random_book(&self, source: &str) -> anyhow::Result<Option<BookHitRow>> {
        let hit = sqlx::query_as::<_, BookHitRow>(sqlx::AssertSqlSafe(format!(
            "SELECT {} FROM books b WHERE b.source = $1 ORDER BY random() LIMIT 1",
            Self::HIT_COLUMNS
        )))
        .bind(source)
        .fetch_optional(&self.pool)
        .await?;
        Ok(hit)
    }

    // -- jobs (queue helpers used by tests and ops) ---------------------------

    pub async fn jobs_by_status(&self, source: &str) -> anyhow::Result<Vec<(String, i64)>> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT status, count(*) FROM jobs WHERE source = $1 GROUP BY status")
                .bind(source)
                .fetch_all(&self.pool)
                .await?;
        Ok(rows)
    }

    /// Requeue jobs stuck in `running` (daemon died mid-job). Returns count.
    pub async fn reclaim_running_jobs(&self) -> anyhow::Result<u64> {
        let res = sqlx::query("UPDATE jobs SET status = 'queued' WHERE status = 'running'")
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
    }
}

/// A book write payload shared by insert/update.
pub struct NewBook<'a> {
    pub source: &'a str,
    pub id: i64,
    pub r#type: &'a str,
    pub title: &'a str,
    pub language: &'a str,
    pub issued: Option<time::Date>,
    pub publisher: Option<&'a str>,
    pub rights: Option<&'a str>,
    pub description: Option<&'a str>,
    pub reading_ease: Option<&'a str>,
    pub downloads: Option<i32>,
    pub authors: &'a crate::domain::Json,
    pub subjects: &'a crate::domain::Json,
    pub bookshelves: &'a crate::domain::Json,
    pub status: &'a str,
}

impl StorePostgres {
    /// All `done` file rows (source-scoped, optionally book-filtered) — the
    /// repair pre-pass stats their local paths to find vanished copies.
    pub async fn done_files(
        &self,
        source: &str,
        only: Option<&[i64]>,
    ) -> anyhow::Result<Vec<BookFile>> {
        let files = sqlx::query_as::<_, BookFile>(
            "SELECT * FROM book_files WHERE source = $1 AND status = 'done' \
             AND ($2::bigint[] IS NULL OR book_id = ANY($2)) ORDER BY book_id, format",
        )
        .bind(source)
        .bind(only)
        .fetch_all(&self.pool)
        .await?;
        Ok(files)
    }
}
