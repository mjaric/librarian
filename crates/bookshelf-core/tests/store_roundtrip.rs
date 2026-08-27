//! Store roundtrip against live Postgres (skipped without
//! BOOKSHELF_DATABASE_URL): insert a book, set status, read back.

use bookshelf_core::adapters::store_postgres::NewBook;
use bookshelf_core::StorePostgres;
use time::OffsetDateTime;

#[tokio::test]
async fn set_book_status_updates_row() {
    let Ok(url) = std::env::var("BOOKSHELF_DATABASE_URL") else {
        eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
        return;
    };
    let store = StorePostgres::connect(&url).await.unwrap();
    store.migrate().await.unwrap();
    let (source, id) = ("store-test", 999999);
    let _ = bookshelf_core::sqlx::query("DELETE FROM book_categories WHERE source=$1 AND book_id=$2")
        .bind(source).bind(id).execute(store.pool()).await;
    let _ = bookshelf_core::sqlx::query("DELETE FROM book_files WHERE source=$1 AND book_id=$2")
        .bind(source).bind(id).execute(store.pool()).await;
    let _ = bookshelf_core::sqlx::query("DELETE FROM books WHERE source=$1 AND id=$2")
        .bind(source).bind(id).execute(store.pool()).await;

    let book = NewBook {
        source,
        id,
        r#type: "Text",
        title: "Probe",
        language: "en",
        issued: None,
        publisher: None,
        rights: None,
        description: None,
        reading_ease: None,
        downloads: None,
        authors: &serde_json::json!([]),
        subjects: &serde_json::json!([]),
        bookshelves: &serde_json::json!([]),
        status: "discovered",
    };
    assert!(store.insert_book(&book).await.unwrap(), "fresh insert");
    let before = OffsetDateTime::now_utc();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    store.set_book_status(source, id, "synced", None).await.unwrap();
    let row = store.get_book(source, id).await.unwrap().unwrap();
    assert_eq!(row.status, "synced", "status must be updated");
    assert!(row.updated_at > before, "updated_at must move: {}", row.updated_at);

    let _ = bookshelf_core::sqlx::query("DELETE FROM book_files WHERE source=$1 AND book_id=$2")
        .bind(source).bind(id).execute(store.pool()).await;
    let _ = bookshelf_core::sqlx::query("DELETE FROM books WHERE source=$1 AND id=$2")
        .bind(source).bind(id).execute(store.pool()).await;
}
