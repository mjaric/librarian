//! Daemon observability: one in-memory snapshot of DB/queue/live state,
//! refreshed by background tasks, plus the opt-in OpenTelemetry sink
//! (OTLP/HTTP metrics + per-job traces, protobuf) for the user's collector.
//!
//! Layering: `bookshelf-core` exposes only plain counters/structs; every
//! otel type lives here. Observable-gauge callbacks run synchronously on
//! the SDK's collection thread — they must only read this in-memory state,
//! never touch the DB.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::Context;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, ObservableGauge};
use opentelemetry::trace::{Span as _, SpanContext, Tracer as _};
use opentelemetry_otlp::MetricExporter;
use opentelemetry_otlp::Protocol;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::{BatchSpanProcessor, SdkTracerProvider};
use parking_lot::Mutex;

/// Shared, cheaply-clonable snapshot. Writers: scheduler tick (DB/queue
/// fields, 30 s) and the per-cycle active-run task (rsync fields, 5 s).
pub type SharedSnapshot = Arc<Mutex<ObservabilitySnapshot>>;

#[derive(Debug, Default, Clone)]
pub struct ObservabilitySnapshot {
    // -- refreshed by the scheduler tick from the store -------------------
    pub book_status_counts: Vec<(String, i64)>,
    pub file_status_counts: Vec<(String, i64)>,
    pub queue_queued: i64,
    pub queue_running: i64,
    /// Seconds since the daemon's `daemon_heartbeat` meta write; None when
    /// no heartbeat was ever written.
    pub heartbeat_age_s: Option<i64>,
    /// Books with an RDF on the local mirror (0 when this host has no
    /// mirror). Refreshed every 10th scheduler tick — a filesystem walk.
    pub mirror_books: u64,
    /// Mirror books not yet in the DB ([`ingest_gap`]); recomputed every
    /// tick from the fresh DB counts and heals to ~0 at cycle end via the
    /// mirror reconcile. Negative when the DB is ahead of the mirror.
    pub ingest_gap: i64,
    /// Live cycle phase as a gauge code ([`phase_code`]); 0 = idle.
    pub active_phase: u8,
    // -- refreshed by the active-run task from the mirror -----------------
    pub rsync_files: u64,
    pub rsync_bytes: u64,
    /// Unix ms of the last itemize line (0 = no run / nothing yet).
    pub rsync_last_item_unix_ms: u64,
    /// Cumulative rsync totals since process start (never reset
    /// in-process). These double as the publisher's last-seen values:
    /// [`Observability::observe_rsync_totals`] computes each tick's
    /// counter delta against them before overwriting.
    pub rsync_files_total: u64,
    pub rsync_bytes_total: u64,
}

#[derive(Clone)]
pub struct Observability {
    source: String,
    snapshot: SharedSnapshot,
    /// In-memory cycle accounting: (kind, outcome) → count.
    cycles: Arc<Mutex<HashMap<(String, String), u64>>>,
    otel: Option<Arc<Otel>>,
}

struct Otel {
    instruments: Instruments,
    provider: SdkMeterProvider,
    /// Trace pipeline; None when the trace exporter failed to initialize —
    /// degraded to metrics-only, never fatal.
    tracer: Option<TracePipeline>,
}

/// The OTLP/HTTP trace pipeline plus the parked root `SpanContext` of the
/// job currently dispatched by the worker loop.
struct TracePipeline {
    provider: SdkTracerProvider,
    /// Parked by [`Observability::start_job_root`] before dispatch, taken
    /// by the provider's `ActiveRunGuard` to parent its phase spans.
    job_sc: Mutex<Option<SpanContext>>,
}

/// Instruments with observable callbacks must stay alive to stay registered.
struct Instruments {
    _books: ObservableGauge<u64>,
    _files: ObservableGauge<u64>,
    _queue: ObservableGauge<u64>,
    _rsync_files: ObservableGauge<u64>,
    _rsync_bytes: ObservableGauge<u64>,
    _rsync_last_item_age: ObservableGauge<f64>,
    _heartbeat_age: ObservableGauge<f64>,
    _mirror_books: ObservableGauge<u64>,
    _ingest_gap: ObservableGauge<i64>,
    _active_phase: ObservableGauge<u64>,
    cycles: Counter<u64>,
    /// Monotonic rsync counters — only ever add()-ed by
    /// [`Observability::observe_rsync_totals`].
    rsync_files_total: Counter<u64>,
    rsync_bytes_total: Counter<u64>,
    cycle_duration: Histogram<f64>,
}

impl Observability {
    /// Snapshot-only handle (no otel). Used by tests and as the fallback
    /// when exporter initialization fails.
    pub fn disabled(source: &str) -> Self {
        Self {
            source: source.to_string(),
            snapshot: Arc::new(Mutex::new(ObservabilitySnapshot::default())),
            cycles: Arc::new(Mutex::new(HashMap::new())),
            otel: None,
        }
    }

    /// Build the handle; when `otlp_endpoint` is Some, also initialize the
    /// OTLP/HTTP metrics pipeline. Exporter failures are logged and degrade
    /// to the disabled variant — metrics must never take the daemon down.
    pub fn new(source: &str, otlp_endpoint: Option<&str>) -> Self {
        let mut obs = Self::disabled(source);
        if let Some(endpoint) = otlp_endpoint {
            match Self::init_otel(source, endpoint, &obs.snapshot) {
                Ok(otel) => obs.otel = Some(Arc::new(otel)),
                Err(e) => tracing::warn!(
                    error = %e,
                    endpoint,
                    "OTLP metrics disabled (exporter init failed)"
                ),
            }
        }
        obs
    }

    fn init_otel(source: &str, endpoint: &str, snapshot: &SharedSnapshot) -> anyhow::Result<Otel> {
        // OTLP/HTTP binary protobuf on :4318. otlp 0.32 uses a programmatic
        // `with_endpoint` VERBATIM (only the env-var paths auto-append the
        // signal path), so join /v1/metrics here; trailing slash trimmed.
        let metrics_url = format!("{}/v1/metrics", endpoint.trim_end_matches('/'));
        let exporter = MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(metrics_url)
            .with_timeout(Duration::from_secs(10))
            .build()?;
        let reader = PeriodicReader::builder(exporter)
            .with_interval(Duration::from_secs(30))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        global::set_meter_provider(provider.clone());

        let meter = global::meter("librarian");
        let instruments = register_instruments(&meter, source, snapshot);
        // Traces share the endpoint and the same client flavor; exporter
        // failures degrade to metrics-only — never take the daemon down.
        let tracer = match Self::init_tracer(endpoint) {
            Ok(pipeline) => {
                tracing::info!(endpoint, "OTLP traces enabled");
                Some(pipeline)
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    endpoint,
                    "OTLP traces disabled (exporter init failed)"
                );
                None
            }
        };
        tracing::info!(endpoint, "OTLP metrics enabled");
        Ok(Otel {
            instruments,
            provider,
            tracer,
        })
    }

    fn init_tracer(endpoint: &str) -> anyhow::Result<TracePipeline> {
        // Same join as metrics — a programmatic endpoint is used verbatim,
        // so /v1/traces is appended here; trailing slash trimmed.
        let traces_url = format!("{}/v1/traces", endpoint.trim_end_matches('/'));
        let exporter = SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(traces_url)
            .with_timeout(Duration::from_secs(10))
            .build()?;
        let processor = BatchSpanProcessor::builder(exporter).build();
        let provider = SdkTracerProvider::builder()
            .with_resource(Resource::builder().with_service_name("librarian").build())
            .with_span_processor(processor)
            .build();
        global::set_tracer_provider(provider.clone());
        Ok(TracePipeline {
            provider,
            job_sc: Mutex::new(None),
        })
    }

    /// The daemon tracer ("librarian"). A no-op tracer when tracing is off.
    pub fn tracer(&self) -> global::BoxedTracer {
        global::tracer("librarian")
    }

    /// Root span of one executed job: fresh trace id, NO parent — jobs are
    /// trace roots, never nested under an ambient context (`Context::new()`
    /// carries no span, so the SDK generates a new trace id). Attributes
    /// `source`/`kind`/`job_id` at creation; `run_id`/`outcome`/
    /// `duration_secs` land on it via [`JobRoot::finish`]. The root's
    /// `SpanContext` is parked for the provider's phase spans. A no-op span
    /// when tracing is off.
    pub fn start_job_root(&self, source: &str, kind: &str, job_id: i64) -> JobRoot {
        let mut root = JobRoot {
            span: None,
            otel: self.otel.clone(),
        };
        let Some(otel) = &self.otel else {
            return root;
        };
        let Some(trace) = &otel.tracer else {
            return root;
        };
        let tracer = self.tracer();
        let mut builder = tracer.span_builder(kind.to_string());
        builder.attributes = Some(vec![
            KeyValue::new("source", source.to_string()),
            KeyValue::new("kind", kind.to_string()),
            KeyValue::new("job_id", job_id),
        ]);
        let span = tracer.build_with_context(builder, &Context::new());
        if span.is_recording() {
            *trace.job_sc.lock() = Some(span.span_context().clone());
            root.span = Some(span);
        }
        root
    }

    /// Take the parked root `SpanContext` of the running job. None when
    /// tracing is off or no job root is parked.
    pub fn take_job_span_context(&self) -> Option<SpanContext> {
        let trace = self.otel.as_ref()?.tracer.as_ref()?;
        trace.job_sc.lock().take()
    }

    /// Handle for background tasks to publish into.
    pub fn snapshot(&self) -> SharedSnapshot {
        self.snapshot.clone()
    }

    /// Fold the mirror's cumulative rsync totals into the snapshot and
    /// feed the monotonic OTel counters. The 5 s active-run publisher
    /// calls this every tick: the delta since the previous tick (the
    /// snapshot's cumulative fields double as last-seen values) is
    /// add()-ed to `librarian.rsync_files_total` /
    /// `librarian.rsync_bytes_total`. Counters only ever add(); the
    /// first tick after process start adds from zero — by definition
    /// correct for "since process start".
    pub fn observe_rsync_totals(&self, cum_files: u64, cum_bytes: u64) {
        let (d_files, d_bytes) = {
            let mut s = self.snapshot.lock();
            let d = rsync_counter_deltas(
                (s.rsync_files_total, s.rsync_bytes_total),
                (cum_files, cum_bytes),
            );
            s.rsync_files_total = cum_files;
            s.rsync_bytes_total = cum_bytes;
            d
        };
        if let Some(otel) = &self.otel {
            let attrs = [KeyValue::new("source", self.source.clone())];
            otel.instruments.rsync_files_total.add(d_files, &attrs);
            otel.instruments.rsync_bytes_total.add(d_bytes, &attrs);
        }
    }

    /// In-memory cycle totals — the `librarian.cycles` accounting.
    pub fn cycle_totals(&self) -> HashMap<(String, String), u64> {
        self.cycles.lock().clone()
    }

    /// One finished job: bump the in-memory total and, when otel is on,
    /// the counter + duration histogram.
    pub fn record_cycle(&self, kind: &str, outcome: &str, duration_secs: f64) {
        *self
            .cycles
            .lock()
            .entry((kind.to_string(), outcome.to_string()))
            .or_insert(0) += 1;
        if let Some(otel) = &self.otel {
            let attrs = [
                KeyValue::new("source", self.source.clone()),
                KeyValue::new("kind", kind.to_string()),
                KeyValue::new("outcome", outcome.to_string()),
            ];
            otel.instruments.cycles.add(1, &attrs);
            otel.instruments.cycle_duration.record(
                duration_secs,
                &[
                    KeyValue::new("source", self.source.clone()),
                    KeyValue::new("kind", kind.to_string()),
                ],
            );
        }
    }

    /// Flush + shut down both providers (meter + tracer; cooperative
    /// shutdown path). Errors are logged, never propagated.
    pub async fn shutdown(&self) {
        let Some(otel) = &self.otel else {
            return;
        };
        let meter_provider = otel.provider.clone();
        let tracer_provider = otel.tracer.as_ref().map(|t| t.provider.clone());
        match tokio::task::spawn_blocking(move || {
            if let Some(tracer_provider) = tracer_provider {
                match tracer_provider.shutdown() {
                    Ok(()) => tracing::debug!("otel traces flushed on shutdown"),
                    Err(e) => tracing::warn!(error = %e, "otel tracer shutdown failed"),
                }
            }
            meter_provider.shutdown()
        })
        .await
        {
            Ok(Ok(())) => tracing::debug!("otel metrics flushed on shutdown"),
            Ok(Err(e)) => tracing::warn!(error = %e, "otel shutdown failed"),
            Err(e) => tracing::warn!(error = %e, "otel shutdown task failed"),
        }
    }
}

/// Pure delta math for the monotonic rsync counters: how much to add()
/// this tick, given the previously published totals and the mirror's
/// cumulative pair. Saturating — a backwards read can only add zero,
/// never corrupt the monotonic series.
fn rsync_counter_deltas(prev: (u64, u64), cum: (u64, u64)) -> (u64, u64) {
    (cum.0.saturating_sub(prev.0), cum.1.saturating_sub(prev.1))
}

/// Pure phase → gauge-code mapping for `librarian.active_phase`: the
/// `active_run` meta's phase string collapsed for dashboard value
/// mappings. Absent (`None`, idle) and unknown phases are 0.
pub fn phase_code(phase: Option<&str>) -> u8 {
    match phase {
        Some("listing") => 1,
        Some("transferring") => 2,
        Some("ingesting") => 3,
        Some("repairing") => 4,
        _ => 0,
    }
}

/// Pure ingest-gap math: mirror books not yet in the DB. Negative when
/// the DB is ahead of the local mirror (e.g. non-mirror hosts).
pub fn ingest_gap(mirror: u64, db: i64) -> i64 {
    mirror as i64 - db
}

/// Blocking mirror walk (`dir` is the mirror root: one numeric subdir per
/// book): count the subdirs holding their `pg{id}.rdf`. `None` when the
/// root itself is absent — this host has no local mirror and the
/// mirror-derived gauges stay zero. Runs on a blocking thread via the
/// scheduler tick; never from a gauge callback.
pub fn count_mirror_rdfs(dir: &std::path::Path) -> Option<u64> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            tracing::warn!(error = %e, dir = %dir.display(), "mirror walk failed");
            return Some(0);
        }
    };
    let mut count = 0u64;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.bytes().all(|b| b.is_ascii_digit()) || name.is_empty() {
            continue;
        }
        if entry.path().join(format!("pg{name}.rdf")).is_file() {
            count += 1;
        }
    }
    Some(count)
}

/// Handle to the root span of one executed job, owned by the worker loop.
/// [`JobRoot::finish`] records the outcome and ends the span on every exit
/// path; a no-op when tracing is off.
pub struct JobRoot {
    span: Option<global::BoxedSpan>,
    otel: Option<Arc<Otel>>,
}

impl JobRoot {
    /// Record `run_id` (when the summary is known), `outcome` and
    /// `duration_secs` on the root span and end it. Also clears the parked
    /// span context (covers jobs without an `ActiveRunGuard`, e.g. repair).
    pub fn finish(self, run_id: Option<i64>, outcome: &str, duration_secs: f64) {
        if let Some(trace) = self.otel.as_ref().and_then(|otel| otel.tracer.as_ref()) {
            *trace.job_sc.lock() = None;
        }
        let Some(mut span) = self.span else {
            return;
        };
        let mut attrs = vec![
            KeyValue::new("outcome", outcome.to_string()),
            KeyValue::new("duration_secs", duration_secs),
        ];
        if let Some(run_id) = run_id {
            attrs.push(KeyValue::new("run_id", run_id));
        }
        span.set_attributes(attrs);
        span.end();
    }
}

fn register_instruments(
    meter: &opentelemetry::metrics::Meter,
    source: &str,
    snapshot: &SharedSnapshot,
) -> Instruments {
    let source = source.to_string();
    let src = |v: &str| [KeyValue::new("source", v.to_string())];

    // DB-backed gauges (sync-only callback: snapshot read, no IO).
    let books = {
        let snapshot = snapshot.clone();
        let source = source.clone();
        meter
            .u64_observable_gauge("librarian.books")
            .with_description("Books by status")
            .with_callback(move |observer| {
                let counts = snapshot.lock().book_status_counts.clone();
                for (status, count) in counts {
                    observer.observe(
                        count.max(0) as u64,
                        &[
                            KeyValue::new("source", source.clone()),
                            KeyValue::new("status", status),
                        ],
                    );
                }
            })
            .build()
    };
    let files = {
        let snapshot = snapshot.clone();
        let source = source.clone();
        meter
            .u64_observable_gauge("librarian.files")
            .with_description("Book files by status")
            .with_callback(move |observer| {
                let counts = snapshot.lock().file_status_counts.clone();
                for (status, count) in counts {
                    observer.observe(
                        count.max(0) as u64,
                        &[
                            KeyValue::new("source", source.clone()),
                            KeyValue::new("status", status),
                        ],
                    );
                }
            })
            .build()
    };
    let queue = {
        let snapshot = snapshot.clone();
        let source = source.clone();
        meter
            .u64_observable_gauge("librarian.queue")
            .with_description("Job queue depth by status")
            .with_callback(move |observer| {
                let s = snapshot.lock();
                for (status, depth) in [("queued", s.queue_queued), ("running", s.queue_running)] {
                    observer.observe(
                        depth.max(0) as u64,
                        &[
                            KeyValue::new("source", source.clone()),
                            KeyValue::new("status", status),
                        ],
                    );
                }
            })
            .build()
    };
    let rsync_files = {
        let snapshot = snapshot.clone();
        let source = source.clone();
        meter
            .u64_observable_gauge("librarian.rsync_files")
            .with_description("Files moved by the current rsync attempt")
            .with_callback(move |observer| {
                observer.observe(snapshot.lock().rsync_files, &src(&source));
            })
            .build()
    };
    let rsync_bytes = {
        let snapshot = snapshot.clone();
        let source = source.clone();
        meter
            .u64_observable_gauge("librarian.rsync_bytes")
            .with_description("Bytes moved by the current rsync attempt")
            .with_callback(move |observer| {
                observer.observe(snapshot.lock().rsync_bytes, &src(&source));
            })
            .build()
    };
    let rsync_last_item_age = {
        let snapshot = snapshot.clone();
        let source = source.clone();
        meter
            .f64_observable_gauge("librarian.rsync_last_item_age_seconds")
            .with_description("Age of the last rsync itemize line")
            .with_callback(move |observer| {
                let s = snapshot.lock();
                if s.rsync_last_item_unix_ms == 0 {
                    return; // no active run — skip the observation
                }
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let age = now_ms.saturating_sub(s.rsync_last_item_unix_ms) as f64 / 1000.0;
                observer.observe(age, &src(&source));
            })
            .build()
    };
    let heartbeat_age = {
        let snapshot = snapshot.clone();
        let source = source.clone();
        meter
            .f64_observable_gauge("librarian.heartbeat_age_seconds")
            .with_description("Age of the daemon heartbeat")
            .with_callback(move |observer| {
                if let Some(age) = snapshot.lock().heartbeat_age_s {
                    observer.observe(age as f64, &src(&source));
                }
            })
            .build()
    };
    let mirror_books = {
        let snapshot = snapshot.clone();
        let source = source.clone();
        meter
            .u64_observable_gauge("librarian.mirror_books")
            .with_description("Books with an RDF on the local mirror")
            .with_callback(move |observer| {
                observer.observe(snapshot.lock().mirror_books, &src(&source));
            })
            .build()
    };
    let ingest_gap = {
        let snapshot = snapshot.clone();
        let source = source.clone();
        meter
            .i64_observable_gauge("librarian.ingest_gap")
            .with_description(
                "Mirror books not yet in the DB; heals to ~0 at cycle end via reconcile",
            )
            .with_callback(move |observer| {
                observer.observe(snapshot.lock().ingest_gap, &src(&source));
            })
            .build()
    };
    let active_phase = {
        let snapshot = snapshot.clone();
        let source = source.clone();
        meter
            .u64_observable_gauge("librarian.active_phase")
            .with_description("0=idle,1=listing,2=transferring,3=ingesting,4=repairing")
            .with_callback(move |observer| {
                observer.observe(u64::from(snapshot.lock().active_phase), &src(&source));
            })
            .build()
    };
    let cycles_counter = meter
        .u64_counter("librarian.cycles")
        .with_description("Completed cycles by kind and outcome")
        .build();
    let rsync_files_total = meter
        .u64_counter("librarian.rsync_files_total")
        .with_description("Files moved by rsync, monotonic since process start")
        .build();
    let rsync_bytes_total = meter
        .u64_counter("librarian.rsync_bytes_total")
        .with_description("Bytes moved by rsync, monotonic since process start")
        .build();
    let cycle_duration = meter
        .f64_histogram("librarian.cycle_duration_seconds")
        .with_description("Cycle wall-clock duration")
        .build();

    Instruments {
        _books: books,
        _files: files,
        _queue: queue,
        _rsync_files: rsync_files,
        _rsync_bytes: rsync_bytes,
        _rsync_last_item_age: rsync_last_item_age,
        _heartbeat_age: heartbeat_age,
        _mirror_books: mirror_books,
        _ingest_gap: ingest_gap,
        _active_phase: active_phase,
        cycles: cycles_counter,
        rsync_files_total,
        rsync_bytes_total,
        cycle_duration,
    }
}

/// Trace span name for an ActiveRun phase — a handful of spans per job,
/// nothing per file/book. Phases without a span (e.g. `listing`) are
/// covered by the job root span itself.
pub fn phase_span_name(phase: &str) -> Option<&'static str> {
    match phase {
        "transferring" => Some("rsync.pull"),
        "ingesting" => Some("ingest"),
        "repairing" => Some("repair.pass"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rsync_counter_deltas_add_from_zero_then_increment() {
        // First publish: counters start at 0, so the whole cumulative
        // pair is added — correct for "since process start".
        assert_eq!(rsync_counter_deltas((0, 0), (10, 500)), (10, 500));
        // Later ticks add only the delta.
        assert_eq!(rsync_counter_deltas((10, 500), (12, 900)), (2, 400));
        assert_eq!(rsync_counter_deltas((12, 900), (12, 900)), (0, 0));
    }

    #[test]
    fn rsync_counter_deltas_never_go_backwards() {
        // A backwards cumulative read saturates at zero — the monotonic
        // series must not learn about it.
        assert_eq!(rsync_counter_deltas((10, 500), (5, 100)), (0, 0));
    }

    #[test]
    fn phase_code_maps_every_phase() {
        assert_eq!(phase_code(None), 0);
        assert_eq!(phase_code(Some("listing")), 1);
        assert_eq!(phase_code(Some("transferring")), 2);
        assert_eq!(phase_code(Some("ingesting")), 3);
        assert_eq!(phase_code(Some("repairing")), 4);
    }

    #[test]
    fn phase_code_unknown_is_idle() {
        assert_eq!(phase_code(Some("")), 0);
        assert_eq!(phase_code(Some("warping")), 0);
    }

    #[test]
    fn ingest_gap_is_mirror_minus_db() {
        assert_eq!(ingest_gap(0, 0), 0);
        assert_eq!(ingest_gap(100, 40), 60);
        // DB ahead of the mirror (transfers not started / non-mirror host).
        assert_eq!(ingest_gap(40, 100), -60);
    }
}
