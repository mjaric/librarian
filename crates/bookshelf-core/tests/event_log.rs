//! Append-only JSONL event log: 3 emits → 3 valid lines each tagged with the
//! source; reopen-and-emit appends without truncation.

use bookshelf_core::EventLog;
use bookshelf_core::domain::{EventKind, EventSink};

#[tokio::test]
async fn emits_valid_lines_and_appends() {
    let dir = std::env::temp_dir().join(format!("librarian-evlog-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("events.jsonl");
    let _ = std::fs::remove_file(&path);

    {
        let log = EventLog::open(&path).unwrap();
        log.emit(
            "project-gutenberg",
            EventKind::BookDiscovered,
            Some(1342),
            serde_json::json!({"title": "Pride and Prejudice"}),
        )
        .await;
        log.emit(
            "project-gutenberg",
            EventKind::FileTransferred,
            Some(1342),
            serde_json::json!({"format": "txt", "bytes": 772386}),
        )
        .await;
        log.emit(
            "project-gutenberg",
            EventKind::BookSynced,
            Some(1342),
            serde_json::json!({}),
        )
        .await;
    }

    let lines: Vec<String> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(lines.len(), 3);
    for (i, line) in lines.iter().enumerate() {
        let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON line");
        assert_eq!(v["source"], "project-gutenberg");
        assert_eq!(v["book_id"], 1342);
        assert!(v["ts"].as_str().is_some());
        let kinds = ["book.discovered", "file.transferred", "book.synced"];
        assert_eq!(v["kind"], kinds[i], "line {i}: {line}");
    }

    // reopen and append — no truncation
    {
        let log = EventLog::open(&path).unwrap();
        log.emit(
            "project-gutenberg",
            EventKind::FeedChecked,
            None,
            serde_json::json!({"items": 0}),
        )
        .await;
    }
    let lines: Vec<String> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(lines.len(), 4);
    let last: serde_json::Value = serde_json::from_str(&lines[3]).unwrap();
    assert_eq!(last["kind"], "feed.checked");

    std::fs::remove_file(&path).ok();
    std::fs::remove_dir(&dir).ok();
}
