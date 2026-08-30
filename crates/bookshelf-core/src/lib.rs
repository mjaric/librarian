//! bookshelf-core — shared infrastructure library for all bookshelf providers.
//!
//! Owns the source-agnostic domain model, ports (EventSink, Triage) and the
//! concrete adapters: Postgres store, JSONL event log, polite HTTP client,
//! rsync runner, deterministic triage rules and (feature `agent`) the LLM
//! triage agent. Everything Gutenberg-specific lives in the `librarian`
//! binary's `gutenberg_org` module.

pub mod adapters;
pub mod domain;
pub mod observability;

/// Re-export so downstream crates share this crate's exact sqlx version
/// (queue code in `librarian` runs raw queries against the same pool type).
pub use sqlx;

pub use adapters::event_log::EventLog;
pub use adapters::http::{FetchError, FetchErrorKind, PoliteClient};
pub use adapters::rsync::{
    DETACHED_WRAPPER, ExitClass, InterruptFlag, ItemizeLine, LiveState, RsyncOutcome,
    RsyncProgress, RsyncRunner, RunIntent, classify_exit, clear_run, itemize_delta, parse_itemize,
    read_exit, read_intent, read_pgid, run_is_live, spawn_detached, terminate_group, write_intent,
};
pub use adapters::store_postgres::{ExecutorGuard, StorePostgres};
pub use adapters::triage_rules;
pub use observability::ActiveRun;
