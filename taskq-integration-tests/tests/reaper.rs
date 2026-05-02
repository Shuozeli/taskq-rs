//! Reaper integration tests.
//!
//! Reaper A reclaims expired leases (`design.md` Sec 6.6); Reaper B
//! reclaims runtime rows whose worker stopped heartbeating, stamping
//! `declared_dead_at` so subsequent heartbeats from the dead worker return
//! `WORKER_DEREGISTERED`.
//!
//! These tests bypass the worker SDK heartbeat loop and seed runtime rows
//! directly via the storage trait; the harness drives Reaper A / Reaper B
//! ticks deterministically (`harness.tick_reaper_a()` / `tick_reaper_b()`)
//! rather than waiting on the production polling cadence.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use grpc_client::Channel;
use taskq_integration_tests::TestHarness;
use taskq_proto::task_worker_client::TaskWorkerClient;
use taskq_proto::{HeartbeatRequest, RegisterWorkerRequest, SemVer};
use taskq_storage::{
    IdempotencyKey, Namespace, NewDedupRecord, NewLease, NewTask, TaskId, TaskType, Timestamp,
    WorkerId,
};

const NS: &str = "test";

#[tokio::test]
async fn reaper_a_reclaims_expired_leases() {
    // Arrange
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;
    let (task_id, _worker_id) = seed_dispatched_runtime(&harness, NS, "echo", true).await;

    // Sanity: task is DISPATCHED.
    assert_eq!(
        harness.read_task_status(&task_id.to_string()).as_deref(),
        Some("DISPATCHED")
    );

    // Act: trigger Reaper A.
    harness.tick_reaper_a().await;

    // Assert: task is back in PENDING with attempt incremented.
    let status = harness
        .read_task_status(&task_id.to_string())
        .expect("task row");
    assert_eq!(status, "PENDING");
    let attempt = harness.read_task_attempt(&task_id.to_string());
    assert_eq!(attempt, Some(1));

    harness.shutdown().await;
}

#[tokio::test]
async fn reaper_b_reclaims_after_dead_worker_heartbeat_stops() {
    // Arrange: insert a non-expired runtime row with a stale heartbeat.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;
    let (task_id, worker_id) = seed_dispatched_runtime(&harness, NS, "echo", false).await;

    // Sanity: declared_dead_at NULL pre-tick.
    assert!(harness
        .read_worker_declared_dead_at(&worker_id.to_string())
        .is_none());

    // Act
    harness.tick_reaper_b().await;

    // Assert: declared_dead_at is now stamped on the worker row, the
    // task is back to PENDING, and the runtime row is gone.
    let dead_at = harness.read_worker_declared_dead_at(&worker_id.to_string());
    assert!(dead_at.is_some(), "expected declared_dead_at to be set");
    let status = harness
        .read_task_status(&task_id.to_string())
        .expect("task row");
    assert_eq!(status, "PENDING");

    harness.shutdown().await;
}

#[tokio::test]
async fn reaper_b_subsequent_heartbeat_returns_worker_deregistered() {
    // Arrange: register a worker via the wire so we have a real worker_id
    // backed by a heartbeat row, then stamp the row stale by directly
    // updating storage.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let uri: http::Uri = format!("http://{}", harness.server_addr).parse().unwrap();
    let mut client = TaskWorkerClient::<Channel>::connect(uri).await.unwrap();

    let mut sv = SemVer::default();
    sv.major = 1;
    sv.minor = 0;
    sv.patch = 0;
    let mut reg = RegisterWorkerRequest::default();
    reg.client_version = Some(Box::new(sv.clone()));
    reg.namespace = Some(NS.to_owned());
    reg.task_types = Some(vec!["echo".into()]);
    reg.declared_concurrency = 1;
    let resp = client.register(reg).await.unwrap().into_inner();
    let worker_id_str = resp.worker_id.expect("worker_id");

    // Seed a stale runtime row so Reaper B has something to reclaim.
    seed_runtime_for_existing_worker(&harness, NS, "echo", &worker_id_str, false).await;

    // Make the heartbeat stale enough that Reaper B reclaims.
    rewind_heartbeat(&harness, &worker_id_str, 10_000);

    harness.tick_reaper_b().await;

    // Act: send a heartbeat from the dead worker.
    let mut hb = HeartbeatRequest::default();
    hb.client_version = Some(Box::new(sv.clone()));
    hb.worker_id = Some(worker_id_str.clone());
    hb.namespace = Some(NS.to_owned());
    let hb_resp = client.heartbeat(hb).await.unwrap().into_inner();

    // Assert: heartbeat was rejected with WORKER_DEREGISTERED.
    assert!(
        hb_resp.worker_deregistered,
        "expected worker_deregistered=true; got {:?}",
        hb_resp.error
    );

    harness.shutdown().await;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_ts() -> Timestamp {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    Timestamp::from_unix_millis(ms)
}

/// Insert a task in PENDING then DISPATCHED with a runtime row whose
/// `timeout_at` is either already elapsed (`expired=true`) or fresh
/// (`expired=false`). Returns the task_id and worker_id so the caller can
/// assert on them.
async fn seed_dispatched_runtime(
    harness: &TestHarness,
    ns: &str,
    task_type: &str,
    expired: bool,
) -> (TaskId, WorkerId) {
    let task_id = TaskId::generate();
    let worker_id = WorkerId::generate();
    let now = now_ts();
    let timeout_at = if expired {
        Timestamp::from_unix_millis(now.as_unix_millis() - 60_000)
    } else {
        Timestamp::from_unix_millis(now.as_unix_millis() + 60_000)
    };
    let acquired_at = Timestamp::from_unix_millis(now.as_unix_millis() - 120_000);

    let mut tx = harness
        .state
        .storage
        .begin_dyn()
        .await
        .expect("begin_dyn must succeed");

    // Insert the task in PENDING + dedup row, then ack into DISPATCHED.
    let task = NewTask {
        task_id,
        namespace: Namespace::new(ns),
        task_type: TaskType::new(task_type),
        priority: 0,
        payload: Bytes::from_static(b"{}"),
        payload_hash: [0u8; 32],
        submitted_at: acquired_at,
        expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
        max_retries: 3,
        retry_initial_ms: 1_000,
        retry_max_ms: 10_000,
        retry_coefficient: 2.0,
        traceparent: Bytes::new(),
        tracestate: Bytes::new(),
        format_version: 1,
    };
    let dedup = NewDedupRecord {
        namespace: Namespace::new(ns),
        key: IdempotencyKey::new(format!("seed-{task_id}")),
        payload_hash: [0u8; 32],
        expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
    };
    tx.insert_task(task, dedup)
        .await
        .expect("insert_task must succeed");
    tx.record_acquisition(NewLease {
        task_id,
        attempt_number: 0,
        worker_id,
        acquired_at,
        timeout_at,
    })
    .await
    .expect("record_acquisition must succeed");
    // Make sure the worker has a heartbeat row so Reaper B can find it.
    tx.record_worker_heartbeat(&worker_id, &Namespace::new(ns), acquired_at)
        .await
        .expect("record_worker_heartbeat must succeed");
    tx.commit_dyn().await.expect("commit must succeed");

    // Flip the task's status to DISPATCHED via raw SQL -- the trait does
    // not expose a status setter, but storage tests need the status to
    // match the runtime row. Without this the reaper sees a PENDING task
    // with a runtime row, which is a logically inconsistent state.
    let conn = harness.open_sidecar_connection();
    conn.execute(
        "UPDATE tasks SET status = 'DISPATCHED' WHERE task_id = ?1",
        rusqlite::params![task_id.to_string()],
    )
    .expect("UPDATE tasks");

    (task_id, worker_id)
}

/// Seed a runtime row for an existing worker (e.g. one minted via
/// `Register`). `expired=false` keeps `timeout_at` in the future so only
/// Reaper B (heartbeat-staleness) reclaims it.
async fn seed_runtime_for_existing_worker(
    harness: &TestHarness,
    ns: &str,
    task_type: &str,
    worker_id_str: &str,
    expired: bool,
) {
    use std::str::FromStr;
    let worker_id = uuid::Uuid::from_str(worker_id_str)
        .map(WorkerId::from_uuid)
        .expect("worker_id must parse");
    let task_id = TaskId::generate();
    let now = now_ts();
    let timeout_at = if expired {
        Timestamp::from_unix_millis(now.as_unix_millis() - 60_000)
    } else {
        Timestamp::from_unix_millis(now.as_unix_millis() + 60_000)
    };
    let acquired_at = now;

    let mut tx = harness.state.storage.begin_dyn().await.expect("begin_dyn");
    let task = NewTask {
        task_id,
        namespace: Namespace::new(ns),
        task_type: TaskType::new(task_type),
        priority: 0,
        payload: Bytes::from_static(b"{}"),
        payload_hash: [0u8; 32],
        submitted_at: acquired_at,
        expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
        max_retries: 3,
        retry_initial_ms: 1_000,
        retry_max_ms: 10_000,
        retry_coefficient: 2.0,
        traceparent: Bytes::new(),
        tracestate: Bytes::new(),
        format_version: 1,
    };
    let dedup = NewDedupRecord {
        namespace: Namespace::new(ns),
        key: IdempotencyKey::new(format!("seed-existing-{task_id}")),
        payload_hash: [0u8; 32],
        expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
    };
    tx.insert_task(task, dedup).await.expect("insert_task");
    tx.record_acquisition(NewLease {
        task_id,
        attempt_number: 0,
        worker_id,
        acquired_at,
        timeout_at,
    })
    .await
    .expect("record_acquisition");
    tx.commit_dyn().await.expect("commit");

    // Mirror the seed_dispatched_runtime helper: flip the task to
    // DISPATCHED via raw SQL so the row state matches the runtime row.
    let conn = harness.open_sidecar_connection();
    conn.execute(
        "UPDATE tasks SET status = 'DISPATCHED' WHERE task_id = ?1",
        rusqlite::params![task_id.to_string()],
    )
    .expect("UPDATE tasks");
}

/// Direct UPDATE on `worker_heartbeats.last_heartbeat_at` to make the
/// worker look stale. Used to drive Reaper B without waiting for natural
/// staleness.
fn rewind_heartbeat(harness: &TestHarness, worker_id_str: &str, by_ms: i64) {
    let conn = harness.open_sidecar_connection();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let stale = now - by_ms;
    conn.execute(
        "UPDATE worker_heartbeats SET last_heartbeat_at = ?1 WHERE worker_id = ?2",
        rusqlite::params![stale, worker_id_str],
    )
    .expect("UPDATE worker_heartbeats");
}
