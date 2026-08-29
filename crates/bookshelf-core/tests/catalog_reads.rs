//! Catalog reads against live Postgres (skipped without
//! BOOKSHELF_DATABASE_URL): seed two probe books + one leaf, then exercise
//! search / category tree / shelf paging the web UI relies on.

use bookshelf_core::StorePostgres;
use bookshelf_core::adapters::store_postgres::NewBook;
use bookshelf_core::domain::CatalogScope;

const SOURCE: &str = "store-test";
const ALPHA: i64 = 999998;
const BETA: i64 = 999999;

async fn cleanup(store: &StorePostgres) {
    for id in [ALPHA, BETA] {
        let _ = bookshelf_core::sqlx::query(
            "DELETE FROM book_categories WHERE source=$1 AND book_id=$2",
        )
        .bind(SOURCE)
        .bind(id)
        .execute(store.pool())
        .await;
        let _ =
            bookshelf_core::sqlx::query("DELETE FROM book_files WHERE source=$1 AND book_id=$2")
                .bind(SOURCE)
                .bind(id)
                .execute(store.pool())
                .await;
        let _ = bookshelf_core::sqlx::query("DELETE FROM books WHERE source=$1 AND id=$2")
            .bind(SOURCE)
            .bind(id)
            .execute(store.pool())
            .await;
    }
    let _ = bookshelf_core::sqlx::query("DELETE FROM categories WHERE source=$1")
        .bind(SOURCE)
        .execute(store.pool())
        .await;
}

async fn seed(store: &StorePostgres) {
    let alpha = NewBook {
        source: SOURCE,
        id: ALPHA,
        r#type: "Text",
        title: "Alpha Probe on the Shelf",
        language: "en",
        issued: None,
        publisher: None,
        rights: None,
        description: None,
        reading_ease: None,
        downloads: Some(5),
        authors: &serde_json::json!([{"name": "Doe, Jane", "birth": 1900, "death": 1990}]),
        subjects: &serde_json::json!([]),
        bookshelves: &serde_json::json!([]),
        status: "synced",
    };
    let beta = NewBook {
        id: BETA,
        title: "Beta Probe",
        downloads: Some(9),
        authors: &serde_json::json!([{"name": "Roe, John", "birth": null, "death": null}]),
        ..alpha
    };
    store.insert_book(&alpha).await.unwrap();
    store.insert_book(&beta).await.unwrap();
    store
        .apply_category_seed(SOURCE, &[("Fiction", "Probe Shelf", 999)])
        .await
        .unwrap();
    store
        .link_category(SOURCE, BETA, "Probe Shelf")
        .await
        .unwrap();
}

#[tokio::test]
async fn catalog_reads_search_tree_and_shelf() {
    let Ok(url) = std::env::var("BOOKSHELF_DATABASE_URL") else {
        eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
        return;
    };
    let store = StorePostgres::connect(&url).await.unwrap();
    store.migrate().await.unwrap();
    cleanup(&store).await;
    seed(&store).await;

    // Search: scope + download ordering.
    let all = store
        .search_books(SOURCE, "probe", CatalogScope::All, 10)
        .await
        .unwrap();
    assert_eq!(all.len(), 2, "both probes match 'probe'");
    assert_eq!(all[0].id, BETA, "higher downloads first");
    let title = store
        .search_books(SOURCE, "alpha", CatalogScope::Title, 10)
        .await
        .unwrap();
    assert_eq!(title.len(), 1);
    let author = store
        .search_books(SOURCE, "roe, john", CatalogScope::Author, 10)
        .await
        .unwrap();
    assert_eq!(author.len(), 1, "author scope matches the Beta author");
    let none = store
        .search_books(SOURCE, "absent", CatalogScope::All, 10)
        .await
        .unwrap();
    assert!(none.is_empty());

    // Category tree: leaf carries parent and count.
    let tree = store.category_counts(SOURCE).await.unwrap();
    let row = tree
        .iter()
        .find(|r| r.leaf == "Probe Shelf")
        .expect("leaf present");
    assert_eq!(row.parent.as_deref(), Some("Fiction"));
    assert_eq!(row.books, 1);

    // Shelf listing: total, hit projection, q filter.
    let (hits, total) = store
        .books_in_category(SOURCE, "Probe Shelf", "", 10, 0)
        .await
        .unwrap();
    assert_eq!(total, 1);
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, BETA);
    assert_eq!(hits[0].categories, vec!["Probe Shelf".to_string()]);
    assert!(!hits[0].has_cover);
    let (_, filtered) = store
        .books_in_category(SOURCE, "Probe Shelf", "nope", 10, 0)
        .await
        .unwrap();
    assert_eq!(filtered, 0, "q excludes the shelf's books");

    // Stats are source-scoped.
    let (books, cats, synced) = store.catalog_stats(SOURCE).await.unwrap();
    assert_eq!((books, cats), (2, 1));
    assert_eq!(synced, 2);

    // Recent + random.
    assert_eq!(
        store.recent_books(SOURCE, 1).await.unwrap().len(),
        1,
        "recent limited"
    );
    assert!(store.random_book(SOURCE).await.unwrap().is_some());

    cleanup(&store).await;
}
