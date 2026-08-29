//! Monitoring reads against live Postgres (skipped without
//! BOOKSHELF_DATABASE_URL): probe rows under source 'cli-test' with a job
//! kind no other test touches (tests/queue.rs wipes by kind) — each test
//! deletes only its own table's rows, so the two never interfere.

use std::sync::Arc;

use bookshelf_core::StorePostgres;
use librarian::monitor;
use librarian::queue::JobQueue;
use librarian::trigger::Trigger;

const SOURCE: &str = "cli-test";
const KIND: &str = "cli-test-probe";

async fn setup() -> Option<Arc<StorePostgres>> {
    let url = std::env::var("BOOKSHELF_DATABASE_URL").ok()?;
    let store = Arc::new(StorePostgres::connect(&url).await.ok()?);
    store.migrate().await.ok()?;
    Some(store)
}

async fn wipe_jobs(store: &StorePostgres) {
    let _ = bookshelf_core::sqlx::query("DELETE FROM jobs WHERE source = $1")
        .bind(SOURCE)
        .execute(store.pool())
        .await;
}

async fn wipe_runs(store: &StorePostgres) {
    let _ = bookshelf_core::sqlx::query("DELETE FROM sync_runs WHERE source = $1")
        .bind(SOURCE)
        .execute(store.pool())
        .await;
}

#[tokio::test]
async fn recent_runs_newest_first_with_shape() {
    let Some(store) = setup().await else {
        eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
        return;
    };
    wipe_runs(&store).await;

    let first = store.start_run(SOURCE, "full").await.unwrap();
    let second = store.start_run(SOURCE, "feed").await.unwrap();
    let third = store.start_run(SOURCE, "full").await.unwrap(); // stays running
    store
        .finish_run(first, Some(0), 10, 1_500_000, 2, 3, 0, 1, None)
        .await
        .unwrap();
    store
        .finish_run(second, Some(0), 1, 2_048, 0, 1, 0, 0, Some("interrupted"))
        .await
        .unwrap();

    let runs = store.recent_runs(SOURCE, 10).await.unwrap();
    assert!(runs.len() >= 3, "all three probes visible");
    // newest first: the still-running probe leads, ids strictly descending
    assert_eq!(runs[0].id, third);
    assert_eq!(runs[1].id, second);
    assert_eq!(runs[2].id, first);
    assert!(runs[0].finished_at.is_none(), "third probe never finished");
    assert_eq!(runs[1].transferred_files, Some(1));
    assert_eq!(runs[1].transferred_bytes, Some(2_048));
    assert_eq!(runs[1].aborted_reason.as_deref(), Some("interrupted"));
    assert_eq!(runs[2].transferred_files, Some(10));
    assert_eq!(runs[2].transferred_bytes, Some(1_500_000));
    assert_eq!(runs[2].aborted_reason, None);

    // the table renders all three, running row included
    let rendered = monitor::runs_table(&runs, time::OffsetDateTime::now_utc());
    assert!(
        rendered.contains("· running"),
        "running marker:\n{rendered}"
    );
    assert!(rendered.contains("interrupted"));

    wipe_runs(&store).await;
}

#[tokio::test]
async fn queue_recent_lists_probe_jobs() {
    let Some(store) = setup().await else {
        eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
        return;
    };
    wipe_jobs(&store).await;
    let queue = JobQueue::new(store.pool().clone());

    let a = queue
        .enqueue(SOURCE, KIND, &serde_json::json!({}), Trigger::Cli)
        .await
        .unwrap();
    let b = queue
        .enqueue(
            SOURCE,
            KIND,
            &serde_json::json!({"only": [1342]}),
            Trigger::Cli,
        )
        .await
        .unwrap();

    let jobs = queue.recent(SOURCE, 1).await.unwrap();
    assert_eq!(jobs.len(), 1, "limit must cap the listing");
    assert_eq!(jobs[0].id, b, "newest first");

    let jobs = queue.recent(SOURCE, 10).await.unwrap();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].id, b);
    assert_eq!(jobs[1].id, a);
    assert_eq!(jobs[0].kind, KIND);
    assert_eq!(jobs[0].status, "queued");
    assert_eq!(jobs[0].origin, "cli");
    assert_eq!(jobs[0].priority, 10);
    assert_eq!(jobs[0].attempts, 0);
    assert_eq!(jobs[0].payload, serde_json::json!({"only": [1342]}));
    assert!(jobs[0].error.is_none());
    assert!(jobs[0].started_at.is_none());

    // the table renders the probe jobs without panicking
    let rendered = monitor::jobs_table(&jobs);
    assert!(rendered.contains(KIND), "kind column:\n{rendered}");
    assert!(rendered.contains("queued"));

    wipe_jobs(&store).await;
}
