//! Job origin tagging: how the application was started determines who
//! enqueues — CLI subcommands (one-shot clients) or the daemon's scheduler.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    Cli,
    Schedule,
}

impl Trigger {
    pub fn as_str(&self) -> &'static str {
        match self {
            Trigger::Cli => "cli",
            Trigger::Schedule => "schedule",
        }
    }

    /// Scheduled jobs always outrank cli jobs.
    pub fn priority(&self) -> i32 {
        match self {
            Trigger::Schedule => 0,
            Trigger::Cli => 10,
        }
    }
}
