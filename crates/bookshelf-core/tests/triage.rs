//! Deterministic triage table: 429 + Retry-After, 503 ladder ordering,
//! 404 skip, 12-attempt exhaustion.

use std::time::Duration;

use bookshelf_core::adapters::triage_rules::{LadderStep, decide, ladder_step};
use bookshelf_core::domain::TriageDecision;
use bookshelf_core::{FetchError, FetchErrorKind};

fn status_err(code: u16, retry_after: Option<Duration>) -> FetchError {
    FetchError {
        url: "https://www.gutenberg.org/cache/epub/1342/pg1342.txt".into(),
        kind: FetchErrorKind::Status,
        status: Some(code),
        retry_after,
        body_head: String::new(),
        headers: serde_json::Value::Null,
    }
}

#[test]
fn rate_limit_honors_retry_after_over_ladder() {
    let e = status_err(429, Some(Duration::from_secs(120)));
    assert_eq!(
        decide(&e, 1),
        TriageDecision::RetryAfter(Duration::from_secs(120))
    );
    // the server's explicit Retry-After is honored at every attempt
    assert_eq!(
        decide(&e, 2),
        TriageDecision::RetryAfter(Duration::from_secs(120))
    );
    // without Retry-After, the ladder applies
    let e = status_err(429, None);
    assert_eq!(
        decide(&e, 1),
        TriageDecision::RetryAfter(Duration::from_secs(300))
    );
}

#[test]
fn server_errors_follow_ladder_ordering() {
    let e = status_err(503, None);
    assert_eq!(
        decide(&e, 1),
        TriageDecision::RetryAfter(Duration::from_secs(5 * 60))
    );
    assert_eq!(
        decide(&e, 2),
        TriageDecision::RetryAfter(Duration::from_secs(10 * 60))
    );
    assert_eq!(
        decide(&e, 3),
        TriageDecision::RetryAfter(Duration::from_secs(60 * 60))
    );
    assert_eq!(decide(&e, 4), TriageDecision::Defer);
    assert_eq!(decide(&e, 11), TriageDecision::Defer);
}

#[test]
fn gone_is_skip() {
    assert_eq!(decide(&status_err(404, None), 1), TriageDecision::Skip);
    assert_eq!(decide(&status_err(410, None), 7), TriageDecision::Skip);
}

#[test]
fn twelve_attempts_fail() {
    assert_eq!(ladder_step(11), LadderStep::Defer);
    assert_eq!(ladder_step(12), LadderStep::Fail);
    assert_eq!(ladder_step(13), LadderStep::Fail);
}

#[test]
fn connect_and_timeout_are_laddered() {
    let mut e = status_err(0, None);
    e.kind = FetchErrorKind::Connect;
    e.status = None;
    assert_eq!(
        decide(&e, 1),
        TriageDecision::RetryAfter(Duration::from_secs(5 * 60))
    );
    e.kind = FetchErrorKind::Timeout;
    assert_eq!(decide(&e, 4), TriageDecision::Defer);
}
