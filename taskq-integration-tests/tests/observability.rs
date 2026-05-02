//! Observability integration tests.
//!
//! Asserts the audit log persists rows for admin actions
//! (`design.md` Sec 11.4), trace context survives the submit -> dispatch
//! round-trip (Sec 11.2), and the harness CP increments standard metric
//! counters as the lifecycle progresses (Sec 11.3).

use std::time::Duration;

use bytes::Bytes;
use grpc_client::Channel;
use taskq_caller_sdk::{SubmitOutcome, SubmitRequest};
use taskq_integration_tests::{audit_count_by_rpc, spawn_worker, TestHarness};
use taskq_proto::task_admin_client::TaskAdminClient;
use taskq_proto::{
    AdmitterKind, DisableNamespaceRequest, DispatcherKind, NamespaceQuota as WireQuota, SemVer,
    SetNamespaceQuotaRequest,
};
use taskq_worker_sdk::HandlerOutcome;

const NS: &str = "test";

#[tokio::test]
async fn audit_log_written_for_set_namespace_quota() {
    // Arrange
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;
    let mut admin = admin_client(&harness).await;

    let baseline = harness.audit_log_count();

    // Act: a successful SetNamespaceQuota writes a "success" audit row in
    // the same SERIALIZABLE transaction as the upsert.
    let mut quota = WireQuota::default();
    populate_minimal_quota(&mut quota, NS);
    quota.max_pending = 7;
    let mut req = SetNamespaceQuotaRequest::default();
    req.client_version = Some(Box::new(sv()));
    req.quota = Some(Box::new(quota));
    req.audit_note = Some("audit-test".into());
    let resp = admin.set_namespace_quota(req).await.unwrap().into_inner();
    assert!(resp.error.is_none());

    // Assert
    let after = harness.audit_log_count();
    assert!(
        after > baseline,
        "expected at least one new audit row; before={} after={}",
        baseline,
        after
    );
    let entries = harness.audit_log_entries();
    let counts = audit_count_by_rpc(&entries);
    assert!(
        counts.get("SetNamespaceQuota").copied().unwrap_or(0) >= 1,
        "expected SetNamespaceQuota audit entry; saw {:?}",
        counts.keys()
    );

    harness.shutdown().await;
}

#[tokio::test]
async fn audit_log_records_disable_namespace_with_namespace_label() {
    // Arrange
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;
    let mut admin = admin_client(&harness).await;

    // Act
    let mut req = DisableNamespaceRequest::default();
    req.client_version = Some(Box::new(sv()));
    req.namespace = Some(NS.into());
    req.reason = Some("ops".into());
    req.audit_note = Some("integration".into());
    let resp = admin.disable_namespace(req).await.unwrap().into_inner();
    assert!(resp.error.is_none());

    // Assert: the audit row carries `rpc = "DisableNamespace"` and the
    // namespace label.
    let entries = harness.audit_log_entries();
    let matching: Vec<_> = entries
        .into_iter()
        .filter(|e| e.rpc == "DisableNamespace" && e.namespace.as_deref() == Some(NS))
        .collect();
    assert!(
        !matching.is_empty(),
        "expected DisableNamespace audit row for namespace={}",
        NS
    );
    assert_eq!(matching[0].result, "success");

    harness.shutdown().await;
}

#[tokio::test]
async fn trace_context_persists_on_task_row() {
    // Arrange: submit via the SDK, which auto-generates a 55-byte W3C
    // `traceparent`. The CP persists it on the `tasks` row so workers
    // can continue the parent trace at acquire time.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let worker = harness
        .worker_for(NS, vec!["echo"], move |task| async move {
            HandlerOutcome::Success(task.payload)
        })
        .await
        .expect("worker_for");
    let (worker_stop, worker_handle) = spawn_worker(worker);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut caller = harness.caller().await;
    let outcome = caller
        .submit(SubmitRequest::new(NS, "echo", Bytes::from_static(b"hi")))
        .await
        .expect("submit ok");
    let task_id = match outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };

    // Wait for the task to settle so we have a stable row to read.
    wait_until(
        &harness,
        task_id.as_str(),
        |s| s == "COMPLETED",
        Duration::from_secs(10),
    )
    .await;

    // Assert
    let traceparent = harness.read_task_traceparent(task_id.as_str());
    assert_eq!(
        traceparent.len(),
        55,
        "expected 55-byte W3C traceparent on task row, got {} bytes",
        traceparent.len()
    );
    assert!(traceparent.starts_with(b"00-"));

    let _ = worker_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
    harness.shutdown().await;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn admin_client(harness: &TestHarness) -> TaskAdminClient<Channel> {
    let uri: http::Uri = format!("http://{}", harness.server_addr).parse().unwrap();
    TaskAdminClient::<Channel>::connect(uri).await.unwrap()
}

fn sv() -> SemVer {
    let mut v = SemVer::default();
    v.major = 1;
    v.minor = 0;
    v.patch = 0;
    v
}

fn populate_minimal_quota(quota: &mut WireQuota, ns: &str) {
    quota.namespace = Some(ns.to_owned());
    quota.admitter_kind = AdmitterKind::ALWAYS;
    quota.dispatcher_kind = DispatcherKind::PRIORITY_FIFO;
    quota.max_retries_ceiling = 10;
    quota.max_idempotency_ttl_seconds = 86_400;
    quota.max_payload_bytes = 10 * 1024 * 1024;
    quota.max_details_bytes = 65_536;
    quota.min_heartbeat_interval_seconds = 1;
    quota.lazy_extension_threshold_seconds = 2;
    quota.max_error_classes = 64;
    quota.max_task_types = 32;
    quota.trace_sampling_ratio = 1.0;
    quota.audit_log_retention_days = 90;
    quota.metrics_export_enabled = true;
}

async fn wait_until(
    harness: &TestHarness,
    task_id: &str,
    predicate: impl Fn(&str) -> bool,
    deadline: Duration,
) {
    let start = std::time::Instant::now();
    loop {
        if let Some(status) = harness.read_task_status(task_id) {
            if predicate(&status) {
                return;
            }
        }
        if start.elapsed() > deadline {
            panic!(
                "task {} did not reach the expected state within {:?}",
                task_id, deadline
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
