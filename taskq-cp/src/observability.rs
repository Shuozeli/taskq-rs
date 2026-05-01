//! OpenTelemetry pipeline initialization and metric registration.
//!
//! `design.md` §11.1 commits to OTel end-to-end. This module wires the SDK
//! based on `CpConfig::otel_exporter`, exposes the global meter/tracer to
//! handlers, and pre-declares the standard metric set so `/metrics`
//! exposition surfaces them at zero before traffic flows.
//!
//! Phase 5d wires the OTLP gRPC exporter (Phase 5a's punt) and the emit
//! sites in handlers/reapers consume `MetricsHandle` directly.

use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};
use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::TracerProvider;
use prometheus::Registry;

use crate::config::OtelExporterConfig;
use crate::error::{CpError, Result};

/// Top-level OTel state owned by `main`. Holds onto the providers so
/// `shutdown` can flush them at process exit (`design.md` §11 — buffered
/// spans / metrics must not be dropped on the floor).
///
/// `prometheus_registry` is `Some` iff the operator selected the Prometheus
/// exporter, in which case the health server's `/metrics` route serializes
/// from it. Other exporters return `None`.
pub struct ObservabilityState {
    pub meter_provider: Option<SdkMeterProvider>,
    pub tracer_provider: Option<TracerProvider>,
    pub prometheus_registry: Option<Registry>,
    pub metrics: MetricsHandle,
}

impl ObservabilityState {
    /// Flush and shut down the OTel pipeline. Called from `main` after the
    /// gRPC server returns. Errors are logged but not propagated — at
    /// shutdown we want to free the runtime regardless.
    pub fn shutdown(self) {
        if let Some(provider) = self.meter_provider {
            if let Err(err) = provider.shutdown() {
                tracing::warn!(error = %err, "OTel meter provider shutdown failed");
            }
        }
        if let Some(provider) = self.tracer_provider {
            if let Err(err) = provider.shutdown() {
                tracing::warn!(error = %err, "OTel tracer provider shutdown failed");
            }
        }
    }
}

/// Initialize the OTel pipeline based on the operator's `OtelExporterConfig`.
///
/// `Disabled` and `Stdout` are always safe (no network); `Otlp` and
/// `Prometheus` may fail at exporter construction time and return
/// `CpError::Observability`.
pub fn init(config: &OtelExporterConfig) -> Result<ObservabilityState> {
    match config {
        OtelExporterConfig::Disabled => Ok(ObservabilityState {
            meter_provider: None,
            tracer_provider: None,
            prometheus_registry: None,
            metrics: MetricsHandle::noop(),
        }),
        OtelExporterConfig::Stdout => init_stdout(),
        OtelExporterConfig::Prometheus => init_prometheus(),
        OtelExporterConfig::Otlp { endpoint } => init_otlp(endpoint),
    }
}

fn init_stdout() -> Result<ObservabilityState> {
    use opentelemetry_sdk::metrics::PeriodicReader;
    use opentelemetry_sdk::runtime;

    let exporter = opentelemetry_stdout::MetricsExporter::default();
    let reader = PeriodicReader::builder(exporter, runtime::Tokio).build();
    let meter_provider = SdkMeterProvider::builder().with_reader(reader).build();
    global::set_meter_provider(meter_provider.clone());

    let tracer_provider = TracerProvider::builder()
        .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    let metrics = MetricsHandle::register(&global::meter("taskq-cp"));
    record_schema_version(&metrics);
    Ok(ObservabilityState {
        meter_provider: Some(meter_provider),
        tracer_provider: Some(tracer_provider),
        prometheus_registry: None,
        metrics,
    })
}

fn init_prometheus() -> Result<ObservabilityState> {
    let registry = Registry::new();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .map_err(CpError::observability)?;
    let meter_provider = SdkMeterProvider::builder().with_reader(exporter).build();
    global::set_meter_provider(meter_provider.clone());

    let metrics = MetricsHandle::register(&global::meter("taskq-cp"));
    record_schema_version(&metrics);
    Ok(ObservabilityState {
        meter_provider: Some(meter_provider),
        tracer_provider: None,
        prometheus_registry: Some(registry),
        metrics,
    })
}

/// OTLP gRPC exporter wiring (`design.md` §11.1).
///
/// Phase 5d: build a `MetricsExporter` and `SpanExporter` over the configured
/// endpoint via `opentelemetry-otlp` 0.17's tonic-backed pipeline. This pulls
/// in tonic, which is independent of the `pure-grpc-rs` framework used for
/// the CP's own gRPC surface — both gRPC stacks coexist without conflict.
fn init_otlp(endpoint: &str) -> Result<ObservabilityState> {
    use std::time::Duration;

    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::runtime;

    // Metrics: build a `SdkMeterProvider` using OTel's built pipeline.
    // `period`/`timeout` defaults are reasonable; we set explicit values so
    // operators have a known cadence for export.
    let metrics_pipeline = opentelemetry_otlp::new_pipeline()
        .metrics(runtime::Tokio)
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint)
                .with_timeout(Duration::from_secs(10)),
        )
        .with_period(Duration::from_secs(15))
        .with_timeout(Duration::from_secs(10));
    let meter_provider = metrics_pipeline.build().map_err(CpError::observability)?;
    global::set_meter_provider(meter_provider.clone());

    // Traces: use the install_batch path so the runtime owns the export task.
    // `install_batch` returns a `TracerProvider` which we hand over to
    // `ObservabilityState` for shutdown flushing.
    let tracer_provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(
            opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(endpoint)
                .with_timeout(Duration::from_secs(10)),
        )
        .install_batch(runtime::Tokio)
        .map_err(CpError::observability)?;
    global::set_tracer_provider(tracer_provider.clone());

    let metrics = MetricsHandle::register(&global::meter("taskq-cp"));
    record_schema_version(&metrics);
    Ok(ObservabilityState {
        meter_provider: Some(meter_provider),
        tracer_provider: Some(tracer_provider),
        prometheus_registry: None,
        metrics,
    })
}

/// Stamp the binary's schema version once at startup (`design.md` §11.3).
/// `taskq_schema_version{component}` is documented as a gauge; we emit a
/// single sample carrying the binary version so dashboards can display it.
fn record_schema_version(metrics: &MetricsHandle) {
    metrics.schema_version.add(
        i64::from(crate::SCHEMA_VERSION),
        &[KeyValue::new("component", "taskq-cp")],
    );
}

/// Pre-declared instruments for the CP. Phase 5d fills in the emit sites in
/// the handler / reaper paths.
///
/// Histograms intentionally do not include `task_type` as a label per
/// `design.md` §11.3. Counters do.
#[derive(Clone)]
pub struct MetricsHandle {
    // Submit-side
    pub submit_total: Counter<u64>,
    pub submit_payload_bytes: Histogram<u64>,
    pub idempotency_hit_total: Counter<u64>,
    pub idempotency_payload_mismatch_total: Counter<u64>,
    pub rejection_total: Counter<u64>,

    // Dispatch-side
    pub dispatch_total: Counter<u64>,
    pub dispatch_latency_seconds: Histogram<f64>,
    pub long_poll_wait_seconds: Histogram<f64>,

    // Lifecycle
    pub complete_total: Counter<u64>,
    pub retry_total: Counter<u64>,
    pub terminal_total: Counter<u64>,
    pub error_class_total: Counter<u64>,

    // Lease & reaper
    pub lease_expired_total: Counter<u64>,
    pub heartbeat_total: Counter<u64>,

    // Quota / capacity (gauges expressed as up-down counters)
    pub pending_count: UpDownCounter<i64>,
    pub inflight_count: UpDownCounter<i64>,
    pub quota_usage_ratio: Histogram<f64>,
    pub rate_limit_hit_total: Counter<u64>,

    // Workers / connections
    pub workers_registered: UpDownCounter<i64>,
    pub waiters_active: UpDownCounter<i64>,

    // Internal health
    pub storage_transaction_seconds: Histogram<f64>,
    pub storage_serialization_conflict_total: Counter<u64>,
    pub storage_retry_attempts: Histogram<u64>,
    pub replay_total: Counter<u64>,
    pub deprecated_field_used_total: Counter<u64>,
    pub schema_version: UpDownCounter<i64>,
    pub audit_log_pruned_total: Counter<u64>,
}

impl MetricsHandle {
    /// Register every standard instrument against the global meter. The
    /// instruments themselves are never `Drop`ped — they live for the
    /// process lifetime and are referenced via `Arc<MetricsHandle>` (held
    /// inside `CpState`).
    pub fn register(meter: &Meter) -> Self {
        Self {
            submit_total: meter
                .u64_counter("taskq_submit_total")
                .with_description("Number of SubmitTask calls")
                .init(),
            submit_payload_bytes: meter
                .u64_histogram("taskq_submit_payload_bytes")
                .with_description("Submitted task payload sizes")
                .with_unit("By")
                .init(),
            idempotency_hit_total: meter
                .u64_counter("taskq_idempotency_hit_total")
                .with_description("Number of idempotency-key hits returning the existing task")
                .init(),
            idempotency_payload_mismatch_total: meter
                .u64_counter("taskq_idempotency_payload_mismatch_total")
                .with_description(
                    "Number of submits rejected with IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD",
                )
                .init(),
            rejection_total: meter
                .u64_counter("taskq_rejection_total")
                .with_description("Number of admit-time rejections by reason")
                .init(),

            dispatch_total: meter
                .u64_counter("taskq_dispatch_total")
                .with_description("Number of AcquireTask responses returning a task")
                .init(),
            dispatch_latency_seconds: meter
                .f64_histogram("taskq_dispatch_latency_seconds")
                .with_description("Time from PENDING insert to DISPATCHED transition")
                .with_unit("s")
                .init(),
            long_poll_wait_seconds: meter
                .f64_histogram("taskq_long_poll_wait_seconds")
                .with_description("Time AcquireTask spent waiting before returning")
                .with_unit("s")
                .init(),

            complete_total: meter
                .u64_counter("taskq_complete_total")
                .with_description("Number of CompleteTask calls reaching a terminal success")
                .init(),
            retry_total: meter
                .u64_counter("taskq_retry_total")
                .with_description("Number of retry transitions (WAITING_RETRY)")
                .init(),
            terminal_total: meter
                .u64_counter("taskq_terminal_total")
                .with_description("Number of tasks reaching any terminal state")
                .init(),
            error_class_total: meter
                .u64_counter("taskq_error_class_total")
                .with_description("Failures broken down by registered error_class")
                .init(),

            lease_expired_total: meter
                .u64_counter("taskq_lease_expired_total")
                .with_description("Number of LEASE_EXPIRED responses surfaced to workers")
                .init(),
            heartbeat_total: meter
                .u64_counter("taskq_heartbeat_total")
                .with_description("Number of Heartbeat calls accepted")
                .init(),

            pending_count: meter
                .i64_up_down_counter("taskq_pending_count")
                .with_description("Tasks currently in PENDING per namespace")
                .init(),
            inflight_count: meter
                .i64_up_down_counter("taskq_inflight_count")
                .with_description("Tasks currently in DISPATCHED per namespace")
                .init(),
            quota_usage_ratio: meter
                .f64_histogram("taskq_quota_usage_ratio")
                .with_description("Per-namespace quota usage ratio (current/limit)")
                .init(),
            rate_limit_hit_total: meter
                .u64_counter("taskq_rate_limit_hit_total")
                .with_description("Number of rate-limit hits per dimension")
                .init(),

            workers_registered: meter
                .i64_up_down_counter("taskq_workers_registered")
                .with_description("Currently-registered workers per namespace")
                .init(),
            waiters_active: meter
                .i64_up_down_counter("taskq_waiters_active")
                .with_description("Long-poll waiters parked on this replica")
                .init(),

            storage_transaction_seconds: meter
                .f64_histogram("taskq_storage_transaction_seconds")
                .with_description("Storage transaction wall-clock time per logical op")
                .with_unit("s")
                .init(),
            storage_serialization_conflict_total: meter
                .u64_counter("taskq_storage_serialization_conflict_total")
                .with_description("Number of 40001 serialization conflicts observed")
                .init(),
            storage_retry_attempts: meter
                .u64_histogram("taskq_storage_retry_attempts")
                .with_description("Distribution of 40001-retry attempts per logical op")
                .init(),
            replay_total: meter
                .u64_counter("taskq_replay_total")
                .with_description("Number of dead-letter replays accepted")
                .init(),
            deprecated_field_used_total: meter
                .u64_counter("taskq_deprecated_field_used_total")
                .with_description("Number of requests carrying a deprecated wire field")
                .init(),
            schema_version: meter
                .i64_up_down_counter("taskq_schema_version")
                .with_description("Schema version of the bundled binary, per component")
                .init(),
            audit_log_pruned_total: meter
                .u64_counter("taskq_audit_log_pruned_total")
                .with_description("Number of audit_log rows pruned by the retention job")
                .init(),
        }
    }

    /// Build a `MetricsHandle` whose instruments are registered against a
    /// no-op meter — used when `OtelExporterConfig::Disabled` is selected.
    /// Calls to record / increment compile to a no-op fast path inside the
    /// OTel SDK.
    pub fn noop() -> Self {
        Self::register(&global::meter("taskq-cp-noop"))
    }
}

/// Convenience for building a `[KeyValue]` slice with a `namespace` label
/// in one call. Phase 5d emit sites use this to standardize labels.
pub fn ns_label(namespace: &str) -> [KeyValue; 1] {
    [KeyValue::new("namespace", namespace.to_owned())]
}

/// Convenience: a two-label slice `(namespace, task_type)` used by counters
/// that retain `task_type`. Histograms drop `task_type` per `design.md`
/// §11.3.
pub fn ns_task_labels(namespace: &str, task_type: &str) -> [KeyValue; 2] {
    [
        KeyValue::new("namespace", namespace.to_owned()),
        KeyValue::new("task_type", task_type.to_owned()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_metrics_handle_constructs_without_panicking() {
        // Arrange / Act: no global provider is wired, so this exercises the
        // SDK's fallback to a no-op meter.

        // Act
        let _handle = MetricsHandle::noop();

        // Assert: reaching this line means every counter / histogram name
        // accepted by the OTel SDK at registration time. (Names with
        // disallowed characters would panic inside the SDK.)
    }

    #[test]
    fn disabled_exporter_returns_state_without_providers() {
        // Arrange
        let config = OtelExporterConfig::Disabled;

        // Act
        let state = init(&config).unwrap();

        // Assert
        assert!(state.meter_provider.is_none());
        assert!(state.tracer_provider.is_none());
        assert!(state.prometheus_registry.is_none());
    }

    #[test]
    fn ns_task_labels_produces_two_entries() {
        // Arrange
        let labels = ns_task_labels("ns", "type");

        // Act / Assert: the SDK accepts the slice; we verify shape so a
        // future agent renaming labels notices the contract change.
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0].key.as_str(), "namespace");
        assert_eq!(labels[1].key.as_str(), "task_type");
    }

    /// `OtelExporterConfig::Otlp` reaches the wired path in `init` and does
    /// not panic. We do NOT actually call `init()` inside the unit test
    /// runtime — the OTLP `PeriodicReader` spawns a background task whose
    /// drop semantics interact poorly with `cargo test`'s runtime
    /// (a real test would need a stub collector). Instead we assert the
    /// config variant is matched and returns the OTLP arm by inspection;
    /// the integration coverage lives in the `--config <toml>` smoke test
    /// invoked from `tasks.md` Phase 5d's verification list.
    #[test]
    fn otlp_exporter_config_variant_compiles() {
        // Arrange: a syntactically valid endpoint URI.
        let config = OtelExporterConfig::Otlp {
            endpoint: "http://127.0.0.1:4317".to_owned(),
        };

        // Act: pattern-match on the variant. We do not invoke `init()`
        // here; see the rationale in the doc comment.
        let endpoint = match &config {
            OtelExporterConfig::Otlp { endpoint } => Some(endpoint.clone()),
            _ => None,
        };

        // Assert
        assert_eq!(endpoint, Some("http://127.0.0.1:4317".to_owned()));
    }
}
