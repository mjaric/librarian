//! librarian — the bookshelf backend binary's library surface.
//!
//! Provider abstraction + registry, Postgres job queue, trigger tagging,
//! configuration, and the first provider module `gutenberg_org`.

pub mod config;
pub mod gutenberg_org;
pub mod provider;
pub mod queue;
pub mod trigger;
