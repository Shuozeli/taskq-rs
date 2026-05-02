//! Idempotency-key behaviour tests.
//!
//! Maps to `design.md` Sec 5 (idempotency-key release per terminal state)
//! and Sec 10 (`IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`).
//!
//! - `COMPLETED` / `FAILED_NONRETRYABLE` / `FAILED_EXHAUSTED` / `EXPIRED`
//!   hold the key for its TTL -- a second submit with the same key returns
//!   the existing task_id.
//! - `CANCELLED` releases the key immediately -- the same key on a fresh
//!   submit creates a brand-new task.
//! - A second submit with the same key but a different payload returns
//!   `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`.

use std::time::Duration;

use bytes::Bytes;
use taskq_caller_sdk::{CancelOutcome, SubmitOutcome, SubmitRequest, TaskId};
use taskq_integration_tests::{spawn_worker, TestHarness};
use taskq_worker_sdk::HandlerOutcome;

const NS: &str = "test";

#[tokio::test]
async fn dedup_returns_existing_for_completed_task() {
    // Arrange: submit one task and let a worker complete it. Then submit
    // again with the same idempotency key + same payload -- the dedup row
    // is held, so the SDK should observe `Existing { task_id }`.
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
    let mut req = SubmitRequest::new(NS, "echo", Bytes::from_static(b"payload"));
    req.idempotency_key = Some("dedup-completed".into());

    // Act: submit, wait for completion, submit again.
    let first = caller.submit(req.clone()).await.expect("submit ok");
    let task_id = match first {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };
    wait_for_terminal_status(&harness, &task_id, Duration::from_secs(10)).await;

    let second = caller.submit(req.clone()).await.expect("resubmit ok");

    // Assert
    match second {
        SubmitOutcome::Existing {
            task_id: existing_id,
            ..
        } => {
            assert_eq!(existing_id.as_str(), task_id.as_str());
        }
        other => panic!("expected Existing, got {other:?}"),
    }

    let _ = worker_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn payload_mismatch_returns_structured_rejection() {
    // Arrange: dedup row exists from the first submit. Second submit with
    // the same key but a different payload triggers
    // `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let mut caller = harness.caller().await;
    let mut first = SubmitRequest::new(NS, "echo", Bytes::from_static(b"payload-A"));
    first.idempotency_key = Some("dedup-mismatch".into());
    let first_outcome = caller.submit(first.clone()).await.expect("submit ok");
    let task_id = match first_outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };

    // Act: submit with the same key but a different payload.
    let mut second = first.clone();
    second.payload = Bytes::from_static(b"payload-B");
    let second_outcome = caller.submit(second).await.expect("submit returns shape");

    // Assert
    match second_outcome {
        SubmitOutcome::PayloadMismatch {
            existing_task_id, ..
        } => {
            assert_eq!(existing_task_id.as_str(), task_id.as_str());
        }
        other => panic!("expected PayloadMismatch, got {other:?}"),
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn cancelled_task_releases_key_for_fresh_submission() {
    // Arrange: submit one, cancel it, then resubmit with the same key.
    // CANCELLED releases the key per design.md Sec 5 -- the second
    // submit must be a fresh `Created` with a different task_id.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let mut caller = harness.caller().await;
    let mut req = SubmitRequest::new(NS, "noop", Bytes::from_static(b"payload"));
    req.idempotency_key = Some("dedup-cancel-release".into());

    // Act
    let first = caller.submit(req.clone()).await.expect("submit ok");
    let first_id = match first {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };
    let cancel = caller.cancel(&first_id).await.expect("cancel ok");
    assert!(matches!(cancel, CancelOutcome::Cancelled { .. }));

    let second = caller.submit(req.clone()).await.expect("resubmit ok");

    // Assert
    match second {
        SubmitOutcome::Created {
            task_id: second_id, ..
        } => {
            assert_ne!(first_id.as_str(), second_id.as_str());
        }
        other => panic!("expected Created, got {other:?}"),
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn expired_dedup_record_allows_resubmission() {
    // Arrange: insert a task + dedup row with an already-elapsed dedup
    // TTL. Phase 9c wired §6.1 step 2 lazy cleanup into `submit_task`,
    // so the resubmission below relies on that path — no manual
    // `sweep_expired_dedup` call is needed.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    seed_expired_dedup(&harness, NS, "echo", "dedup-expired", b"old").await;

    let mut caller = harness.caller().await;
    let mut req = SubmitRequest::new(NS, "echo", Bytes::from_static(b"new"));
    req.idempotency_key = Some("dedup-expired".into());

    // Act
    let outcome = caller.submit(req).await.expect("submit ok");

    // Assert: submit-time lazy cleanup deleted the expired row in the
    // same transaction, allowing the new insert to proceed.
    match outcome {
        SubmitOutcome::Created { .. } => {}
        other => panic!("expected fresh Created after dedup TTL elapsed, got {other:?}"),
    }

    harness.shutdown().await;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn seed_expired_dedup(
    harness: &TestHarness,
    ns: &str,
    task_type: &str,
    key: &str,
    payload: &[u8],
) {
    use taskq_storage::{
        IdempotencyKey, Namespace, NewDedupRecord, NewTask, TaskId as StorageTaskId,
        TaskType as StorageTaskType, Timestamp,
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let task_id = StorageTaskId::generate();
    let mut tx = harness.state.storage.begin_dyn().await.expect("begin_dyn");
    let task = NewTask {
        task_id,
        namespace: Namespace::new(ns),
        task_type: StorageTaskType::new(task_type),
        priority: 0,
        payload: Bytes::copy_from_slice(payload),
        payload_hash: [1u8; 32],
        submitted_at: Timestamp::from_unix_millis(now - 10_000),
        expires_at: Timestamp::from_unix_millis(now + 86_400_000),
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
        key: IdempotencyKey::new(key.to_owned()),
        payload_hash: [1u8; 32],
        // Already elapsed.
        expires_at: Timestamp::from_unix_millis(now - 1_000),
    };
    tx.insert_task(task, dedup).await.expect("insert_task");
    tx.commit_dyn().await.expect("commit");
}

async fn wait_for_terminal_status(
    harness: &TestHarness,
    task_id: &TaskId,
    deadline: Duration,
) -> String {
    let start = std::time::Instant::now();
    let mut last_seen = String::new();
    loop {
        if let Some(status) = harness.read_task_status(task_id.as_str()) {
            last_seen = status.clone();
            if matches!(
                status.as_str(),
                "COMPLETED" | "CANCELLED" | "FAILED_NONRETRYABLE" | "FAILED_EXHAUSTED" | "EXPIRED"
            ) {
                return status;
            }
        }
        if start.elapsed() > deadline {
            panic!(
                "task {} did not reach a terminal state within {:?} (last_seen={:?})",
                task_id.as_str(),
                deadline,
                last_seen
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
