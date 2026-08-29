//! Deterministic triage rules — the keyless default. Every non-happy-path
//! HTTP outcome maps to a recovery action without any model call:
//!
//! | situation                        | decision                    |
//! |----------------------------------|-----------------------------|
//! | connect error / timeout          | `RetryAfter(ladder)`        |
//! | HTTP 429                         | `RetryAfter(Retry-After)` — honor it exactly; ladder without it |
//! | HTTP 500/502/503/504             | `RetryAfter(ladder)`        |
//! | HTTP 404/410                     | `Skip` (file.skipped `404`) |
//! | other 4xx                        | `Skip` (permanent)          |
//! | 200 + Content-Length ≠ extent    | `RetryAfter(ladder)` (`truncated`) |
//!
//! Ladder (attempts = failures so far): 1→+5 min, 2→+10 min, 3→+1 h,
//! 4..=11→`Defer` (retry_at cleared; next cycle retries), ≥12→`Fail`
//! (the file exhausts `max_total_attempts` and goes terminal-failed).

use std::time::Duration;

use crate::domain::{Triage, TriageContext, TriageDecision};
use async_trait::async_trait;

use super::http::{FetchError, FetchErrorKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LadderStep {
    RetryAfter(Duration),
    Defer,
    Fail,
}

pub fn ladder_step(attempts: u32) -> LadderStep {
    match attempts {
        1 => LadderStep::RetryAfter(Duration::from_secs(5 * 60)),
        2 => LadderStep::RetryAfter(Duration::from_secs(10 * 60)),
        3 => LadderStep::RetryAfter(Duration::from_secs(60 * 60)),
        4..=11 => LadderStep::Defer,
        _ => LadderStep::Fail,
    }
}

fn from_ladder(attempts: u32) -> TriageDecision {
    match ladder_step(attempts) {
        LadderStep::RetryAfter(d) => TriageDecision::RetryAfter(d),
        LadderStep::Defer => TriageDecision::Defer,
        LadderStep::Fail => TriageDecision::Defer, // terminal handling is the caller's
    }
}

/// The deterministic decision for one failed HTTP fetch.
pub fn decide(err: &FetchError, attempts: u32) -> TriageDecision {
    match err.kind {
        FetchErrorKind::Timeout | FetchErrorKind::Connect | FetchErrorKind::Body => {
            from_ladder(attempts)
        }
        FetchErrorKind::Status => match err.status.unwrap_or(0) {
            429 => {
                // Honor the server's explicit Retry-After exactly (RFC 9110
                // politeness); with none, fall to the ladder.
                match err.retry_after {
                    Some(ra) => TriageDecision::RetryAfter(ra),
                    None => from_ladder(attempts),
                }
            }
            500 | 502 | 503 | 504 => from_ladder(attempts),
            404 | 410 => TriageDecision::Skip,
            _ => TriageDecision::Skip, // other 4xx: skip permanent
        },
    }
}

/// Rule-based `Triage` port implementation (keyless, default).
pub struct RulesTriage;

#[async_trait]
impl Triage for RulesTriage {
    async fn decide(&self, ctx: &TriageContext) -> TriageDecision {
        let err = FetchError {
            url: ctx.url_or_dest.clone(),
            kind: match ctx.tool {
                "http" => FetchErrorKind::Status,
                _ => FetchErrorKind::Connect,
            },
            status: ctx.status,
            retry_after: ctx
                .headers
                .get("retry-after")
                .and_then(|v| v.as_str())
                .and_then(|s| s.trim().parse::<u64>().ok())
                .map(Duration::from_secs),
            body_head: ctx.body_head.clone(),
            headers: serde_json::Value::Null,
        };
        decide(&err, ctx.attempts)
    }
}
