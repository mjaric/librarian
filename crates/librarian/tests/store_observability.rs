//! Store observability helpers against a live Postgres (skipped when
//! BOOKSHELF_DATABASE_URL is unset): `clear_meta` idempotency and the
//! status-count shapes. Uses the scratch source key 'store-test' and
//! cleans up its probe rows.

use bookshelf_core::StorePostgres;

const PROBE: &str = "store-test";

/// The two tests share the 'store-test' probe rows — keep them serial.
static SERIES_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn setup() -> Option<std::sync::Arc<StorePostgres>> {
    let url = std::env::var("BOOKSHELF_DATABASE_URL").ok()?;
    let store = StorePostgres::connect(&url).await.ok()?;
    store.migrate().await.ok()?;
    Some(std::sync::Arc::new(store))
}

async fn cleanup(store: &StorePostgres) {
    let _ = bookshelf_core::sqlx::query("DELETE FROM book_files WHERE source = $1")
        .bind(PROBE)
        .execute(store.pool())
        .await;
    let _ = bookshelf_core::sqlx::query("DELETE FROM books WHERE source = $1")
        .bind(PROBE)
        .execute(store.pool())
        .await;
    let _ = bookshelf_core::sqlx::query("DELETE FROM meta WHERE source = $1")
        .bind(PROBE)
        .execute(store.pool())
        .await;
}

#[tokio::test]
async fn clear_meta_is_idempotent_delete() {
    let _serial = SERIES_LOCK.lock().await;
    let Some(store) = setup().await else {
        eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
        return;
    };
    cleanup(&store).await;

    store
        .set_meta(PROBE, "active_run", r#"{"kind":"full_cycle"}"#)
        .await
        .unwrap();
    let value = store.get_meta(PROBE, "active_run").await.unwrap();
    assert_eq!(value.as_deref(), Some(r#"{"kind":"full_cycle"}"#));

    store.clear_meta(PROBE, "active_run").await.unwrap();
    let value = store.get_meta(PROBE, "active_run").await.unwrap();
    assert_eq!(value, None, "clear_meta must delete the row");

    // Second clear on a missing row: still fine.
    store.clear_meta(PROBE, "active_run").await.unwrap();

    cleanup(&store).await;
}

#[tokio::test]
async fn status_counts_group_by_status() {
    let _serial = SERIES_LOCK.lock().await;
    let Some(store) = setup().await else {
        eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
        return;
    };
    cleanup(&store).await;

    for (id, status) in [(1, "synced"), (2, "synced"), (3, "discovered")] {
        bookshelf_core::sqlx::query(
            "INSERT INTO books (source, id, title, status, first_seen, updated_at) \
             VALUES ($1, $2, 'probe', $3, now(), now())",
        )
        .bind(PROBE)
        .bind(id)
        .bind(status)
        .execute(store.pool())
        .await
        .unwrap();
    }
    for (id, format, status) in [
        (1, "txt", "done"),
        (1, "epub.images", "done"),
        (2, "txt", "pending"),
    ] {
        bookshelf_core::sqlx::query(
            "INSERT INTO book_files (source, book_id, format, status) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(PROBE)
        .bind(id)
        .bind(format)
        .bind(status)
        .execute(store.pool())
        .await
        .unwrap();
    }

    let books = store.book_status_counts(PROBE).await.unwrap();
    assert_eq!(
        books,
        vec![("discovered".to_string(), 1), ("synced".to_string(), 2)],
        "book counts grouped by status, ordered by status"
    );

    let files = store.file_status_counts(PROBE).await.unwrap();
    assert_eq!(
        files,
        vec![("done".to_string(), 2), ("pending".to_string(), 1)],
        "file counts grouped by status, ordered by status"
    );

    cleanup(&store).await;
}
