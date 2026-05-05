//! End-to-end submit -> dispatch -> complete lifecycle tests.
//!
//! Each test stands up a fresh `TestHarness`, attaches one or more workers
//! via the SDK, exercises one path from `design.md` Sec 6, and asserts on
//! the observable outcome (terminal state, attempt counter, audit log).
//!
//! Reads happen via either the harness's sidecar SQLite reader (status
//! polling, where wire latency would be a distraction) or via the caller
//! SDK's `get_result` (when we want to assert that the wire-level
//! `task_results` projection round-trips through `taskq-cp`'s
//! `GetTaskResult` handler).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use taskq_caller_sdk::{CancelOutcome, SubmitOutcome, SubmitRequest, TaskId, TaskOutcome};
use taskq_integration_tests::{spawn_worker, TestHarness};
use taskq_worker_sdk::{ErrorClassRegistry, HandlerOutcome};

const NS: &str = "test";

#[tokio::test]
async fn submit_dispatch_complete_round_trip() {
    // Arrange
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let task_count = Arc::new(AtomicUsize::new(0));
    let task_count_handler = Arc::clone(&task_count);
    let worker = harness
        .worker_for(NS, vec!["echo"], move |task| {
            let counter = Arc::clone(&task_count_handler);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                HandlerOutcome::Success(task.payload)
            }
        })
        .await
        .expect("worker_for must succeed");
    let (worker_stop, worker_handle) = spawn_worker(worker);
    // Allow the worker's first AcquireTask long-poll to register before we
    // submit the task -- otherwise the wake_one fires while the waiter
    // pool entry is still being created.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut caller = harness.caller().await;

    // Act
    let outcome = caller
        .submit(SubmitRequest::new(NS, "echo", Bytes::from_static(b"hello")))
        .await
        .expect("submit must succeed");
    let task_id = match outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };

    let status = wait_for_terminal_status(&harness, &task_id, Duration::from_secs(10)).await;

    // Assert
    assert_eq!(status, "COMPLETED");
    assert_eq!(task_count.load(Ordering::SeqCst), 1);

    // Cleanup
    let _ = worker_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn multiple_workers_dispatch_distinct_tasks() {
    // Arrange
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let total = Arc::new(AtomicUsize::new(0));

    let mut worker_stops = Vec::new();
    let mut worker_handles = Vec::new();
    for _ in 0..3 {
        let counter = Arc::clone(&total);
        let worker = harness
            .worker_for(NS, vec!["echo"], move |task| {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    HandlerOutcome::Success(task.payload)
                }
            })
            .await
            .expect("worker_for must succeed");
        let (stop, handle) = spawn_worker(worker);
        worker_stops.push(stop);
        worker_handles.push(handle);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut caller = harness.caller().await;

    // Act
    let mut task_ids = Vec::new();
    for i in 0..5u32 {
        let outcome = caller
            .submit(SubmitRequest::new(
                NS,
                "echo",
                Bytes::from(format!("payload-{i}").into_bytes()),
            ))
            .await
            .expect("submit must succeed");
        let task_id = match outcome {
            SubmitOutcome::Created { task_id, .. } => task_id,
            other => panic!("expected Created, got {other:?}"),
        };
        task_ids.push(task_id);
    }

    for task_id in &task_ids {
        let status = wait_for_terminal_status(&harness, task_id, Duration::from_secs(10)).await;
        assert_eq!(status, "COMPLETED");
    }

    // Assert: each task processed exactly once.
    assert_eq!(total.load(Ordering::SeqCst), task_ids.len());

    // Cleanup
    for stop in worker_stops {
        let _ = stop.send(());
    }
    for handle in worker_handles {
        let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    }
    harness.shutdown().await;
}

#[tokio::test]
async fn cancel_pending_task_releases_idempotency_key() {
    // Arrange: no worker -- the task stays in PENDING long enough for us
    // to cancel it before any dispatch.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;
    let mut caller = harness.caller().await;
    let mut req = SubmitRequest::new(NS, "noop", Bytes::from_static(b"first"));
    req.idempotency_key = Some("dedup-cancel".into());

    // Act
    let first = caller.submit(req.clone()).await.expect("submit ok");
    let task_id = match first {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };
    let cancel = caller.cancel(&task_id).await.expect("cancel ok");
    assert!(matches!(cancel, CancelOutcome::Cancelled { .. }));

    // Resubmit with the same idempotency key but a different payload --
    // CANCELLED releases the key per design.md Sec 5, so this should be a
    // fresh Created (not a PayloadMismatch and not Existing).
    let mut req_again = req.clone();
    req_again.payload = Bytes::from_static(b"second");
    let second = caller.submit(req_again).await.expect("resubmit ok");

    // Assert
    match second {
        SubmitOutcome::Created {
            task_id: new_id, ..
        } => {
            assert_ne!(new_id.as_str(), task_id.as_str(), "fresh task_id expected");
        }
        other => panic!("expected fresh Created, got {other:?}"),
    }

    harness.shutdown().await;
}

#[tokio::test]
async fn cancel_after_complete_returns_already_terminal() {
    // Arrange: a worker that completes immediately so the cancel races
    // and observes the terminal state.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let worker = harness
        .worker_for(NS, vec!["fast"], move |task| async move {
            HandlerOutcome::Success(task.payload)
        })
        .await
        .expect("worker_for must succeed");
    let (worker_stop, worker_handle) = spawn_worker(worker);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut caller = harness.caller().await;

    // Act: submit, wait for completion, then cancel.
    let submit_outcome = caller
        .submit(SubmitRequest::new(NS, "fast", Bytes::from_static(b"x")))
        .await
        .expect("submit ok");
    let task_id = match submit_outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };
    let status = wait_for_terminal_status(&harness, &task_id, Duration::from_secs(10)).await;
    assert_eq!(status, "COMPLETED");

    let cancel = caller.cancel(&task_id).await.expect("cancel ok");

    // Assert: cancel observes the existing terminal state.
    use taskq_caller_sdk::TerminalState;
    match cancel {
        CancelOutcome::AlreadyTerminal { final_status, .. } => {
            assert_eq!(final_status, TerminalState::COMPLETED);
        }
        other => panic!("expected AlreadyTerminal, got {other:?}"),
    }

    let _ = worker_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn read_side_surfaces_success_with_payload() {
    // Arrange: a worker that echoes the payload as Success.
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

    // Act: submit, wait for the task to land terminal, then read via the
    // wire path (not the sidecar reader) so we exercise the
    // `task_results` projection through `taskq-cp::handlers::get_task_result_impl`.
    let submit = caller
        .submit(SubmitRequest::new(NS, "echo", Bytes::from_static(b"hello")))
        .await
        .expect("submit");
    let task_id = match submit {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };
    let _ = wait_for_terminal_status(&harness, &task_id, Duration::from_secs(10)).await;
    let state = caller.get_result(&task_id).await.expect("get_result");

    // Assert: the read-side surfaces the worker-reported outcome and
    // round-trips the payload bytes byte-for-byte.
    use taskq_caller_sdk::TerminalState;
    assert_eq!(state.status, TerminalState::COMPLETED);
    assert_eq!(state.outcome, Some(TaskOutcome::Success));
    assert_eq!(
        state.result_payload.as_deref(),
        Some(b"hello".as_slice()),
        "result_payload must round-trip through task_results table"
    );
    assert!(
        state.failure.is_none(),
        "Success outcome must not carry a failure record"
    );

    let _ = worker_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
    harness.shutdown().await;
}

#[tokio::test]
async fn read_side_surfaces_nonretryable_failure_with_class_message_details() {
    // Arrange: a worker that returns NonRetryableFailure with the full
    // class+message+details triple so we can assert all three fields
    // round-trip through the `task_results` projection.
    let harness = TestHarness::start_in_memory().await;
    harness.seed_namespace(NS).await;

    let worker = harness
        .worker_for(NS, vec!["broken"], move |_task| async move {
            let registry = ErrorClassRegistry::new(["permanent"]);
            HandlerOutcome::NonRetryableFailure {
                error_class: registry.error_class("permanent").unwrap(),
                message: "card declined".into(),
                details: Some(Bytes::from_static(b"detail-payload")),
            }
        })
        .await
        .expect("worker_for");
    let (worker_stop, worker_handle) = spawn_worker(worker);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut caller = harness.caller().await;

    // Act
    let submit = caller
        .submit(SubmitRequest::new(NS, "broken", Bytes::from_static(b"x")))
        .await
        .expect("submit");
    let task_id = match submit {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => panic!("expected Created, got {other:?}"),
    };
    let status = wait_for_terminal_status(&harness, &task_id, Duration::from_secs(10)).await;
    assert_eq!(status, "FAILED_NONRETRYABLE");
    let state = caller.get_result(&task_id).await.expect("get_result");

    // Assert: the wire path projects the worker-reported failure record
    // with class, message, details, and `retryable=false` all intact.
    use taskq_caller_sdk::TerminalState;
    assert_eq!(state.status, TerminalState::FAILED_NONRETRYABLE);
    assert_eq!(state.outcome, Some(TaskOutcome::NonretryableFail));
    assert!(
        state.result_payload.is_none(),
        "failure outcomes must not carry a result_payload"
    );
    let failure = state.failure.expect("failure record present");
    assert_eq!(failure.error_class, "permanent");
    assert_eq!(failure.message, "card declined");
    assert_eq!(failure.details.as_ref(), b"detail-payload");
    assert!(!failure.retryable);

    let _ = worker_stop.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), worker_handle).await;
    harness.shutdown().await;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Poll the harness's SQLite directly until `task_id` reaches a terminal
/// state or `deadline` elapses. We read the row directly (rather than
/// going through the CP's `GetTaskResult` RPC) because terminal-status
/// polling doesn't need the wire round-trip; tests that *do* care about
/// the wire projection (e.g. `read_side_surfaces_*`) call
/// `caller.get_result` instead.
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

fn is_terminal_db_str(s: &str) -> bool {
    matches!(
        s,
        "COMPLETED" | "CANCELLED" | "FAILED_NONRETRYABLE" | "FAILED_EXHAUSTED" | "EXPIRED"
    )
}
