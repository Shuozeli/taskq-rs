//! Admin RPC integration tests.
//!
//! Each test stands up a fresh `TestHarness`, constructs a wire-level
//! `TaskAdminClient`, calls one admin verb (`SetNamespaceQuota`,
//! `DisableNamespace`, `PurgeTasks`, `ReplayDeadLetters`, ...) and
//! asserts on the persisted side effects: namespace_quota row content,
//! `tasks` table row count, audit-log delta, etc.

use std::time::Duration;

use bytes::Bytes;
use grpc_client::Channel;
use taskq_caller_sdk::{SubmitOutcome, SubmitRequest};
use taskq_integration_tests::{spawn_worker, TestHarness};
use taskq_proto::task_admin_client::TaskAdminClient;
use taskq_proto::{
    AdmitterKind, DisableNamespaceRequest, DispatcherKind, GetNamespaceQuotaRequest,
    NamespaceQuota as WireQuota, PurgeTasksRequest, ReplayDeadLettersRequest, SemVer,
    SetNamespaceQuotaRequest,
};
use taskq_worker_sdk::{ErrorClassRegistry, HandlerOutcome};

const NS: &str = "test";

#[tokio::test]
async fn set_namespace_quota_persists_max_pending() {
    // Arrange
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;
    let mut admin = admin_client(&harness).await;

    // Act: install a new quota with max_pending = 50.
    let mut quota = WireQuota::default();
    populate_minimal_quota(&mut quota, NS);
    quota.max_pending = 50;
    let mut req = SetNamespaceQuotaRequest::default();
    req.client_version = Some(Box::new(sv()));
    req.quota = Some(Box::new(quota));
    req.audit_note = Some("test".into());
    let resp = admin.set_namespace_quota(req).await.unwrap().into_inner();
    assert!(resp.error.is_none(), "{:?}", resp.error);

    // Read back to verify persistence.
    let mut get = GetNamespaceQuotaRequest::default();
    get.client_version = Some(Box::new(sv()));
    get.namespace = Some(NS.into());
    let read_resp = admin.get_namespace_quota(get).await.unwrap().into_inner();

    // Assert
    let read_quota = read_resp.quota.expect("quota present");
    assert_eq!(read_quota.max_pending, 50);

    harness.shutdown().await;
}

#[tokio::test]
async fn disable_namespace_persists_disabled_flag() {
    // Arrange
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;
    let mut admin = admin_client(&harness).await;

    // Act
    let mut req = DisableNamespaceRequest::default();
    req.client_version = Some(Box::new(sv()));
    req.namespace = Some(NS.into());
    req.reason = Some("integration test".into());
    let resp = admin.disable_namespace(req).await.unwrap().into_inner();
    assert!(resp.error.is_none(), "{:?}", resp.error);

    // Assert: the `namespace_quota.disabled` column flipped to 1.
    let conn = harness.open_sidecar_connection();
    let disabled: i64 = conn
        .query_row(
            "SELECT disabled FROM namespace_quota WHERE namespace = ?1",
            [NS],
            |row| row.get(0),
        )
        .expect("disabled column read");
    assert_eq!(disabled, 1);

    harness.shutdown().await;
}

#[tokio::test]
async fn purge_tasks_cancels_matching_and_releases_keys() {
    // Arrange: submit a couple of tasks (no worker, so they stay PENDING),
    // then purge.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;
    let mut caller = harness.caller().await;
    let mut admin = admin_client(&harness).await;

    let mut task_ids = Vec::new();
    for i in 0..3u32 {
        let mut req = SubmitRequest::new(NS, "purge-target", Bytes::from(format!("p-{i}")));
        req.idempotency_key = Some(format!("purge-key-{i}"));
        let outcome = caller.submit(req).await.expect("submit ok");
        let id = match outcome {
            SubmitOutcome::Created { task_id, .. } => task_id,
            other => panic!("expected Created, got {other:?}"),
        };
        task_ids.push(id);
    }

    // Act
    let mut purge = PurgeTasksRequest::default();
    purge.client_version = Some(Box::new(sv()));
    purge.namespace = Some(NS.into());
    purge.confirm_namespace = Some(NS.into());
    purge.filter = Some(String::new());
    purge.max_tasks = 10;
    purge.audit_note = Some("integration test".into());
    let resp = admin.purge_tasks(purge).await.unwrap().into_inner();

    // Assert
    assert!(resp.error.is_none(), "{:?}", resp.error);
    assert_eq!(resp.purged_count, 3);
    for task_id in &task_ids {
        let status = harness
            .read_task_status(task_id.as_str())
            .expect("task row");
        assert_eq!(status, "CANCELLED");
    }

    // The dedup key should now be reusable for a fresh submission.
    let mut req = SubmitRequest::new(NS, "purge-target", Bytes::from_static(b"new"));
    req.idempotency_key = Some("purge-key-0".into());
    let after = caller.submit(req).await.expect("resubmit");
    match after {
        SubmitOutcome::Created { .. } => {}
        other => panic!("expected fresh Created after purge, got {other:?}"),
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn replay_dead_letters_resets_failed_tasks_to_pending() {
    // Arrange: produce a FAILED_NONRETRYABLE task, then replay it.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let worker = harness
        .worker_for(NS, vec!["broken"], move |_task| async move {
            let registry = ErrorClassRegistry::new(["permanent"]);
            HandlerOutcome::NonRetryableFailure {
                error_class: registry.error_class("permanent").unwrap(),
                message: "fatal".into(),
                details: None,
            }
        })
        .await
        .expect("worker_for");
    let (worker_stop, worker_handle) = spawn_worker(worker);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut caller = harness.caller().await;
    let mut req = SubmitRequest::new(NS, "broken", Bytes::from_static(b"x"));
    req.idempotency_key = Some("replay-key".into());
    let outcome = caller.submit(req).await.expect("submit ok");
    let task_id = match outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };
    // Wait until the task lands in a terminal failure state.
    wait_until(
        &harness,
        task_id.as_str(),
        |s| s == "FAILED_NONRETRYABLE",
        Duration::from_secs(10),
    )
    .await;

    // Stop the worker so the replay does not immediately re-fail.
    let _ = worker_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;

    // Act
    let mut admin = admin_client(&harness).await;
    let mut replay = ReplayDeadLettersRequest::default();
    replay.client_version = Some(Box::new(sv()));
    replay.namespace = Some(NS.into());
    replay.confirm_namespace = Some(NS.into());
    replay.filter = Some(String::new());
    replay.max_tasks = 10;
    replay.audit_note = Some("integration test".into());
    let resp = admin
        .replay_dead_letters(replay)
        .await
        .unwrap()
        .into_inner();

    // Assert
    assert!(resp.error.is_none(), "{:?}", resp.error);
    assert_eq!(resp.replayed.unwrap_or_default().len(), 1);
    let status = harness
        .read_task_status(task_id.as_str())
        .expect("task row");
    assert_eq!(status, "PENDING");
    let attempt = harness
        .read_task_attempt(task_id.as_str())
        .expect("attempt");
    assert_eq!(attempt, 0);

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

/// Fill a wire `NamespaceQuota` with values that satisfy the CP's
/// validation (`lazy_extension_threshold_seconds >= 2 *
/// min_heartbeat_interval_seconds`, `max_idempotency_ttl_seconds <= 90d`).
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
