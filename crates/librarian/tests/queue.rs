//! Queue semantics against a live Postgres (skipped when
//! BOOKSHELF_DATABASE_URL is unset): scheduled jobs outrank cli jobs in
//! pickup; duplicate schedule enqueues of the same kind coalesce.

use bookshelf_core::StorePostgres;
use librarian::queue::JobQueue;
use librarian::trigger::Trigger;

async fn setup() -> Option<(JobQueue, std::sync::Arc<StorePostgres>)> {
    let url = std::env::var("BOOKSHELF_DATABASE_URL").ok()?;
    let store = StorePostgres::connect(&url).await.ok()?;
    store.migrate().await.ok()?;
    // clean slate for the kinds this test uses
    let _ = bookshelf_core::sqlx::query(
        "DELETE FROM jobs WHERE kind IN ('full_cycle','feed_cycle','repair')",
    )
    .execute(store.pool())
    .await;
    Some((
        JobQueue::new(store.pool().clone()),
        std::sync::Arc::new(store),
    ))
}

#[tokio::test]
async fn schedule_outranks_cli_and_coalesces() {
    let Some((queue, _store)) = setup().await else {
        eprintln!("SKIP: BOOKSHELF_DATABASE_URL not set");
        return;
    };

    // cli job first, schedule job second — pickup must return the schedule job
    let cli_id = queue
        .enqueue(
            "project-gutenberg",
            "full_cycle",
            &serde_json::json!({}),
            Trigger::Cli,
        )
        .await
        .unwrap();
    let sched_id = queue
        .enqueue(
            "project-gutenberg",
            "full_cycle",
            &serde_json::json!({}),
            Trigger::Schedule,
        )
        .await
        .unwrap();
    let picked = queue.pick_next().await.unwrap().expect("job picked");
    assert_eq!(
        picked.id, sched_id,
        "schedule job must outrank the older cli job"
    );
    assert_eq!(picked.origin, "schedule");
    assert_eq!(picked.priority, 0);

    // coalescing: first schedule enqueue lands, second is skipped
    let first = queue
        .enqueue_coalesced("project-gutenberg", "feed_cycle", &serde_json::json!({}))
        .await
        .unwrap();
    assert!(first.is_some());
    let second = queue
        .enqueue_coalesced("project-gutenberg", "feed_cycle", &serde_json::json!({}))
        .await
        .unwrap();
    assert!(second.is_none(), "duplicate schedule enqueue must coalesce");

    let queued: i64 = bookshelf_core::sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind = 'feed_cycle' AND status = 'queued'",
    )
    .fetch_one(queue.pool())
    .await
    .unwrap();
    assert_eq!(queued, 1);

    // cleanup: drop the rows this test created
    let _ = bookshelf_core::sqlx::query("DELETE FROM jobs WHERE id IN ($1, $2, $3)")
        .bind(cli_id)
        .bind(sched_id)
        .bind(first.unwrap())
        .execute(queue.pool())
        .await;
}
