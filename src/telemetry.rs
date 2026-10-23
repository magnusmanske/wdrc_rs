//! Health and metrics for the Buggregator service dashboard (see `wikimisc::telemetry`).
//! Metric names live here so the set stays small and every label bounded.

use std::time::Duration;
use wikimisc::metrics::{counter, gauge};
use wikimisc::telemetry::{Telemetry, TelemetryBuilder};
use wikimisc::timestamp::TimeStamp;

/// Starts pushing to the dashboard (token from `$DASHBOARD_TOKEN`). Optional: without a token,
/// or on any error, the bot just runs without it; the outcome is logged. On SIGTERM it sends a
/// final push and exits.
pub fn start_bot() {
    builder("bot").install_or_log();
}

/// Like `start_bot`, for the web service. Signals are left to its graceful shutdown
/// (`wikimisc::server::serve`), which sends the final push once requests have drained.
pub fn start_web() {
    builder("web").handle_signals(false).install_or_log();
}

fn builder(component: &str) -> TelemetryBuilder {
    Telemetry::builder(component).version(env!("CARGO_PKG_VERSION"))
}

/// An API request for `action` (one of a fixed set, see `api::Action::name`).
pub fn record_api_request(action: &'static str) {
    counter!("wdrc_api_requests_total", "action" => action).increment(1);
}

/// How one `run_once` ended: `more_work`, `caught_up` or `error`.
pub fn record_run(result: &'static str) {
    counter!("wdrc_runs_total", "result" => result).increment(1);
}

/// A batch of recent changes was stored, up to `cursor_timestamp`.
pub fn record_batch(recent_changes: usize, statements: usize, cursor_timestamp: &str) {
    counter!("wdrc_recent_changes_total").increment(recent_changes as u64);
    counter!("wdrc_statements_total").increment(statements as u64);
    if let Some(lag) = lag(cursor_timestamp) {
        gauge!("wdrc_lag_seconds").set(lag.as_secs_f64());
    }
}

/// Updating the `deletions` or `redirects` table failed.
pub fn record_aux_failure(table: &'static str) {
    counter!("wdrc_aux_update_failures_total", "table" => table).increment(1);
}

/// The number of texts held in memory.
pub fn set_text_cache_entries(entries: usize) {
    gauge!("wdrc_text_cache_entries").set(entries as f64);
}

/// How far a MediaWiki timestamp (`YYYYMMDDHHMMSS`) lies behind now; `None` if unparseable.
pub fn lag(timestamp: &str) -> Option<Duration> {
    let then = TimeStamp::str2utc(timestamp)?;
    let now = TimeStamp::str2utc(&TimeStamp::now())?;
    (now - then).to_std().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lag() {
        assert!(lag("20200101000000").unwrap() > Duration::from_secs(86400));
        assert!(lag(&TimeStamp::now()).unwrap() < Duration::from_secs(5));
        assert_eq!(lag("garbage"), None);
        // A cursor ahead of the local clock is no lag, not an error.
        assert_eq!(lag("29990101000000"), None);
    }

    #[test]
    fn test_recording_without_telemetry_is_a_noop() {
        record_run("caught_up");
        record_batch(10, 20, "20200101000000");
        record_aux_failure("deletions");
        set_text_cache_entries(5);
        record_api_request("lag");
    }
}
