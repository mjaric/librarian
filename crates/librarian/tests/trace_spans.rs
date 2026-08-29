//! Trace plumbing that is pure: phase→span naming and the disabled /
//! absent-endpoint construction paths. No collector exists in the test
//! env, so no exporter, provider or network is ever initialized here —
//! `Observability::new(.., None)` and `disabled()` never touch the global
//! SDK providers.

use librarian::observability::{Observability, phase_span_name};

#[test]
fn cycle_phases_map_to_a_handful_of_spans() {
    assert_eq!(phase_span_name("transferring"), Some("rsync.pull"));
    assert_eq!(phase_span_name("ingesting"), Some("ingest"));
    assert_eq!(phase_span_name("repairing"), Some("repair.pass"));
    // Pre-transfer phases have no span of their own: the job root covers
    // them, keeping the volume discipline (≤ ~6 spans per job).
    assert_eq!(phase_span_name("listing"), None);
    assert_eq!(phase_span_name("nonsense"), None);
}

#[test]
fn disabled_handle_constructs_and_stays_snapshot_only() {
    let obs = Observability::disabled("trace-test");
    let snap = obs.snapshot();
    snap.lock().rsync_files = 3;
    assert_eq!(snap.lock().rsync_files, 3);
    // Cycle accounting works without otel.
    obs.record_cycle("full_cycle", "ok", 1.5);
    assert_eq!(
        obs.cycle_totals()
            .get(&("full_cycle".to_string(), "ok".to_string())),
        Some(&1)
    );
    // No parked span context without a trace pipeline.
    assert!(obs.take_job_span_context().is_none());
}

#[test]
fn absent_endpoint_constructs_without_trace_pipeline() {
    let obs = Observability::new("trace-test", None);
    // Root-span start + finish must be safe no-ops when the endpoint is
    // absent: no provider, no exporter, no parked context.
    let root = obs.start_job_root("trace-test", "full_cycle", 42);
    root.finish(Some(7), "ok", 0.25);
    assert!(obs.take_job_span_context().is_none());
    obs.record_cycle("full_cycle", "ok", 0.25);
    assert_eq!(
        obs.cycle_totals()
            .get(&("full_cycle".to_string(), "ok".to_string())),
        Some(&1)
    );
}
