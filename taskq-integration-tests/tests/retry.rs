//! Retry-path lifecycle tests.
//!
//! Maps to `design.md` Sec 6.4-6.5 -- a retryable failure transitions the
//! task to `WAITING_RETRY` with `retry_after`, after which the dispatcher
//! re-picks the row once the retry timer elapses. The non-retryable path
//! short-circuits straight to `FAILED_NONRETRYABLE` without consulting
//! retry config (`design.md` Sec 6.5).
//!
//! Phase 9c wired the full §6.5 mapping table: a retryable failure with
//! `attempt < max_retries` and `retry_after <= expires_at` lands in
//! `WAITING_RETRY` with a jittered backoff (per-task `retry_initial_ms`,
//! `retry_max_ms`, `retry_coefficient` from the task row). Once
//! `attempt >= max_retries` the task transitions to `FAILED_EXHAUSTED`;
//! a backoff that pushes past `expires_at` lands in `EXPIRED`. These
//! tests exercise the full mapping.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use taskq_caller_sdk::{CancelOutcome, RetryConfigOverride, SubmitOutcome, SubmitRequest, TaskId};
use taskq_integration_tests::{spawn_worker, TestHarness};
use taskq_worker_sdk::{ErrorClassRegistry, HandlerOutcome};

const NS: &str = "test";

/// Build a `SubmitRequest` with a retry config large enough to exercise
/// the WAITING_RETRY path. The default `RetryConfigOverride.max_retries`
/// of 0 would short-circuit to `FAILED_EXHAUSTED` on the first attempt —
/// test bodies that want to observe a real retry loop set this
/// explicitly.
fn submit_with_retries(task_type: &str, payload: &'static [u8], max_retries: u32) -> SubmitRequest {
    let mut req = SubmitRequest::new(NS, task_type, Bytes::from_static(payload));
    req.retry_config = Some(RetryConfigOverride {
        initial_ms: 50,
        max_ms: 200,
        coefficient: 2.0,
        max_retries,
    });
    req
}

#[tokio::test]
async fn retryable_failure_schedules_retry_then_succeeds() {
    // Arrange: handler fails the first attempt with retryable=true and
    // succeeds on every subsequent attempt.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_h = Arc::clone(&attempts);
    let worker = harness
        .worker_for(NS, vec!["flaky"], move |task| {
            let counter = Arc::clone(&attempts_h);
            async move {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    let registry = ErrorClassRegistry::new(["transient"]);
                    HandlerOutcome::RetryableFailure {
                        error_class: registry.error_class("transient").unwrap(),
                        message: "first attempt fails".into(),
                        details: None,
                    }
                } else {
                    HandlerOutcome::Success(task.payload)
                }
            }
        })
        .await
        .expect("worker_for must succeed");
    let (worker_stop, worker_handle) = spawn_worker(worker);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut caller = harness.caller().await;
    let req = submit_with_retries("flaky", b"flaky-payload", 3);

    // Act
    let outcome = caller.submit(req).await.expect("submit ok");
    let task_id = match outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };
    let status = wait_for_terminal_status(&harness, &task_id, Duration::from_secs(15)).await;

    // Assert: completed after at least one retry.
    assert_eq!(status, "COMPLETED");
    assert!(
        attempts.load(Ordering::SeqCst) >= 2,
        "handler should have been invoked at least twice"
    );

    let _ = worker_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn retryable_failure_eventually_succeeds_after_multiple_attempts() {
    // Arrange: handler fails for the first two attempts and succeeds on
    // the third. Verifies the WAITING_RETRY -> PENDING -> DISPATCHED loop
    // can iterate more than once.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_h = Arc::clone(&attempts);
    let worker = harness
        .worker_for(NS, vec!["doomed-twice"], move |task| {
            let counter = Arc::clone(&attempts_h);
            async move {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    let registry = ErrorClassRegistry::new(["transient"]);
                    HandlerOutcome::RetryableFailure {
                        error_class: registry.error_class("transient").unwrap(),
                        message: "still failing".into(),
                        details: None,
                    }
                } else {
                    HandlerOutcome::Success(task.payload)
                }
            }
        })
        .await
        .expect("worker_for must succeed");
    let (worker_stop, worker_handle) = spawn_worker(worker);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut caller = harness.caller().await;
    let req = submit_with_retries("doomed-twice", b"x", 5);

    // Act
    let outcome = caller.submit(req).await.expect("submit ok");
    let task_id = match outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };
    let status = wait_for_terminal_status(&harness, &task_id, Duration::from_secs(15)).await;

    // Assert
    assert_eq!(status, "COMPLETED");
    assert!(
        attempts.load(Ordering::SeqCst) >= 3,
        "handler should have been invoked at least three times"
    );

    let _ = worker_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn nonretryable_failure_terminates_immediately() {
    // Arrange: handler returns retryable=false on the first attempt.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_h = Arc::clone(&attempts);
    let worker = harness
        .worker_for(NS, vec!["broken"], move |_task| {
            let counter = Arc::clone(&attempts_h);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                let registry = ErrorClassRegistry::new(["permanent"]);
                HandlerOutcome::NonRetryableFailure {
                    error_class: registry.error_class("permanent").unwrap(),
                    message: "no point retrying".into(),
                    details: None,
                }
            }
        })
        .await
        .expect("worker_for must succeed");
    let (worker_stop, worker_handle) = spawn_worker(worker);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut caller = harness.caller().await;
    // Even with a full retry budget, retryable=false must short-circuit.
    let req = submit_with_retries("broken", b"x", 5);

    // Act
    let outcome = caller.submit(req).await.expect("submit ok");
    let task_id = match outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };
    let status = wait_for_terminal_status(&harness, &task_id, Duration::from_secs(10)).await;

    // Assert: only one attempt regardless of retry budget.
    assert_eq!(status, "FAILED_NONRETRYABLE");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    let _ = worker_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn retryable_failure_can_be_cancelled_during_waiting_retry() {
    // Arrange: handler always returns retryable -- the task lands in
    // WAITING_RETRY between attempts. We cancel during that window and
    // verify the task transitions to CANCELLED rather than retrying
    // again.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let worker = harness
        .worker_for(NS, vec!["forever"], move |_task| async move {
            let registry = ErrorClassRegistry::new(["transient"]);
            HandlerOutcome::RetryableFailure {
                error_class: registry.error_class("transient").unwrap(),
                message: "forever".into(),
                details: None,
            }
        })
        .await
        .expect("worker_for must succeed");
    let (worker_stop, worker_handle) = spawn_worker(worker);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut caller = harness.caller().await;
    // Generous retry budget so the task lingers in WAITING_RETRY.
    let req = submit_with_retries("forever", b"x", 20);

    // Act: submit, wait for status to flip to WAITING_RETRY, cancel.
    let outcome = caller.submit(req).await.expect("submit ok");
    let task_id = match outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };
    wait_for_status(&harness, &task_id, "WAITING_RETRY", Duration::from_secs(10)).await;
    let cancel = caller.cancel(&task_id).await.expect("cancel ok");

    // Assert
    assert!(matches!(cancel, CancelOutcome::Cancelled { .. }));
    let status = wait_for_terminal_status(&harness, &task_id, Duration::from_secs(5)).await;
    assert_eq!(status, "CANCELLED");

    let _ = worker_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn retries_exhausted_lands_in_failed_exhausted_terminal() {
    // Arrange: handler always returns retryable=true. With max_retries=2
    // the §6.5 mapping table says: attempts 0 and 1 land in WAITING_RETRY,
    // attempt 2 satisfies `attempt >= max_retries` and the task lands in
    // FAILED_EXHAUSTED rather than continuing the retry loop.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_h = Arc::clone(&attempts);
    let worker = harness
        .worker_for(NS, vec!["always-fails"], move |_task| {
            let counter = Arc::clone(&attempts_h);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                let registry = ErrorClassRegistry::new(["transient"]);
                HandlerOutcome::RetryableFailure {
                    error_class: registry.error_class("transient").unwrap(),
                    message: "still failing".into(),
                    details: None,
                }
            }
        })
        .await
        .expect("worker_for must succeed");
    let (worker_stop, worker_handle) = spawn_worker(worker);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut caller = harness.caller().await;
    let req = submit_with_retries("always-fails", b"x", 2);
    let mut caller_req = req;
    caller_req.idempotency_key = Some("retries-exhausted".into());

    // Act
    let outcome = caller.submit(caller_req).await.expect("submit ok");
    let task_id = match outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };
    let status = wait_for_terminal_status(&harness, &task_id, Duration::from_secs(15)).await;

    // Assert: terminal state is FAILED_EXHAUSTED and the idempotency
    // key is held (per §5 it does not release on FAILED_EXHAUSTED).
    assert_eq!(status, "FAILED_EXHAUSTED");
    assert!(
        attempts.load(Ordering::SeqCst) >= 3,
        "handler must have been invoked at least 3 times before exhaustion"
    );
    assert!(
        idempotency_key_held(&harness, NS, "retries-exhausted"),
        "idempotency key must remain held after FAILED_EXHAUSTED"
    );

    let _ = worker_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn retry_past_ttl_lands_in_expired_terminal() {
    // Arrange: handler reports retryable=true. The task's `expires_at` is
    // set very close to now so the §6.5 mapping picks `EXPIRED` once the
    // backoff math pushes `retry_after` past the TTL even with retry
    // budget remaining.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let worker = harness
        .worker_for(NS, vec!["expires-fast"], move |_task| async move {
            let registry = ErrorClassRegistry::new(["transient"]);
            HandlerOutcome::RetryableFailure {
                error_class: registry.error_class("transient").unwrap(),
                message: "transient".into(),
                details: None,
            }
        })
        .await
        .expect("worker_for must succeed");
    let (worker_stop, worker_handle) = spawn_worker(worker);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut caller = harness.caller().await;
    // expires_at well in the past relative to now + smallest possible
    // backoff. We pick a TTL of ~50ms — by the time the worker reports
    // failure, `retry_after = now + rand(initial/2, initial*1.5)` will
    // overshoot the task's `expires_at`, and the §6.5 mapping forces
    // EXPIRED rather than WAITING_RETRY. We still budget retries so the
    // mapping cannot fall through to FAILED_EXHAUSTED.
    let mut req = submit_with_retries("expires-fast", b"x", 5);
    req.expires_at = Some(SystemTime::now() + Duration::from_millis(50));
    req.idempotency_key = Some("expires-fast-key".into());
    if let Some(ref mut rc) = req.retry_config {
        rc.initial_ms = 1_000;
        rc.max_ms = 60_000;
    }

    // Act
    let outcome = caller.submit(req).await.expect("submit ok");
    let task_id = match outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };
    let status = wait_for_terminal_status(&harness, &task_id, Duration::from_secs(15)).await;

    // Assert: terminal state is EXPIRED.
    assert_eq!(status, "EXPIRED");

    let _ = worker_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
    harness.shutdown().await;
}

/// Read the `idempotency_keys` table directly to confirm the dedup row
/// for `(namespace, key)` is still present. Used by `FAILED_EXHAUSTED`
/// assertions: per `design.md` §5 the key is held for non-`CANCELLED`
/// terminal states.
fn idempotency_key_held(harness: &TestHarness, namespace: &str, key: &str) -> bool {
    let conn = harness.open_sidecar_connection();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM idempotency_keys WHERE namespace = ?1 AND key = ?2",
            rusqlite::params![namespace, key],
            |row| row.get(0),
        )
        .unwrap_or(0);
    count > 0
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
            if is_terminal_db_str(&status) {
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

async fn wait_for_status(
    harness: &TestHarness,
    task_id: &TaskId,
    expected: &str,
    deadline: Duration,
) {
    let start = std::time::Instant::now();
    loop {
        if let Some(status) = harness.read_task_status(task_id.as_str()) {
            if status == expected {
                return;
            }
        }
        if start.elapsed() > deadline {
            panic!(
                "task {} did not reach status {} within {:?}",
                task_id.as_str(),
                expected,
                deadline
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn is_terminal_db_str(s: &str) -> bool {
    matches!(
        s,
        "COMPLETED" | "CANCELLED" | "FAILED_NONRETRYABLE" | "FAILED_EXHAUSTED" | "EXPIRED"
    )
}
