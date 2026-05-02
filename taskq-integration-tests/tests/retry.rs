//! Retry-path lifecycle tests.
//!
//! Maps to `design.md` Sec 6.4-6.5 -- a retryable failure transitions the
//! task to `WAITING_RETRY` with `retry_after`, after which the dispatcher
//! re-picks the row once the retry timer elapses. The non-retryable path
//! short-circuits straight to `FAILED_NONRETRYABLE` without consulting
//! retry config (`design.md` Sec 6.5).
//!
//! Phase 5b note: the CP currently uses a fixed 1s retry seed and does not
//! evaluate `expires_at` / `max_retries` ceilings against the retry
//! computation (`taskq-cp/src/handlers/task_worker.rs` carries a
//! Phase-5b TODO marker for the full mapping). The tests below pin the
//! behaviours that ARE wired -- retry-then-succeed, retry-then-success
//! after multiple attempts, immediate non-retryable termination, and the
//! retry-then-cancel observability flow -- and steer clear of the
//! mappings that need the deeper retry math.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use taskq_caller_sdk::{CancelOutcome, SubmitOutcome, SubmitRequest, TaskId};
use taskq_integration_tests::{spawn_worker, TestHarness};
use taskq_worker_sdk::{ErrorClassRegistry, HandlerOutcome};

const NS: &str = "test";

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
    let req = SubmitRequest::new(NS, "flaky", Bytes::from_static(b"flaky-payload"));

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
    let req = SubmitRequest::new(NS, "doomed-twice", Bytes::from_static(b"x"));

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
    let req = SubmitRequest::new(NS, "broken", Bytes::from_static(b"x"));

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
    let req = SubmitRequest::new(NS, "forever", Bytes::from_static(b"x"));

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
