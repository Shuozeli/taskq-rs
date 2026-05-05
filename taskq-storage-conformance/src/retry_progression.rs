//! Conformance: worker-driven retry progresses `attempt_number`.
//!
//! Not one of the original §8.2 #1–#6 requirements but a cross-backend
//! correctness invariant that surfaced as a bug in both backends:
//!
//! 1. `complete_task(WaitingRetry)` MUST bump `tasks.attempt_number` so a
//!    follow-up `complete_task` for the next attempt does not collide on
//!    the `task_results` PRIMARY KEY (`task_id`, `attempt_number`).
//! 2. `pick_and_lock_pending` MUST also pick rows in `WAITING_RETRY` (with
//!    `retry_after <= now`) — there is no separate timer that promotes
//!    them back to PENDING. Backends that filter only `status = 'PENDING'`
//!    leave retryable failures stuck forever.
//!
//! This test seeds one task, drives it through two retryable failures
//! using the public `Storage` / `StorageTx` surface, and asserts that
//! `task_results` ends up with rows at attempt-numbers `[0, 1]` rather
//! than colliding on `(task_id, 0)` twice.

use bytes::Bytes;
use taskq_storage::ids::{Namespace, TaskId, TaskType, Timestamp, WorkerId};
use taskq_storage::types::{
    LeaseRef, NamespaceFilter, NewLease, PickCriteria, PickOrdering, TaskOutcome, TaskOutcomeKind,
    TaskTypeFilter,
};
use taskq_storage::{Storage, StorageTx};

use crate::helpers::{make_task, ONE_HOUR_MS, T0_MS};

/// Drive one task through a Submit -> Dispatch -> WaitingRetry ->
/// re-Dispatch -> WaitingRetry sequence and assert that:
///
/// - the dispatcher picks the task on the second iteration (so a backend
///   that filters only `status = 'PENDING'` would fail here),
/// - `complete_task(WaitingRetry)` bumps the task row's
///   `attempt_number` from 0 -> 1,
/// - the second `complete_task(WaitingRetry)` writes a new
///   `task_results` row at `attempt_number = 1` instead of colliding on
///   the first row,
/// - `get_latest_task_result` returns the attempt = 1 row.
pub async fn worker_driven_retry_bumps_attempt_and_writes_per_attempt_row<S: Storage>(storage: &S) {
    // Arrange — one task, max_retries=2 so two retryable failures both
    // classify as WAITING_RETRY (we never reach FailedExhausted in this
    // test; that path is exercised by the lifecycle integration suite).
    let namespace = Namespace::new("retry-progression-ns");
    let task_type = TaskType::new("rp.task");
    let task_id = TaskId::generate();
    let (mut task, dedup) = make_task(
        task_id,
        namespace.as_str(),
        task_type.as_str(),
        "rp-key",
        [0xAA; 32],
    );
    task.max_retries = 2;
    task.retry_initial_ms = 1; // ensure retry_after computed by callers
    task.retry_max_ms = 1;

    let mut tx = storage.begin().await.expect("seed begin");
    tx.insert_task(task, dedup).await.expect("seed insert");
    tx.commit().await.expect("seed commit");

    // First dispatch — pick PENDING, stamp a runtime row.
    let now1 = Timestamp::from_unix_millis(T0_MS + 10);
    let timeout_at1 = Timestamp::from_unix_millis(T0_MS + ONE_HOUR_MS);
    let worker_id1 = WorkerId::generate();
    let mut tx = storage.begin().await.expect("dispatch1 begin");
    let locked1 = tx
        .pick_and_lock_pending(PickCriteria {
            namespace_filter: NamespaceFilter::Single(namespace.clone()),
            task_types_filter: TaskTypeFilter::AnyOf(vec![task_type.clone()]),
            ordering: PickOrdering::PriorityFifo,
            now: now1,
        })
        .await
        .expect("pick PENDING")
        .expect("a row must be available");
    assert_eq!(
        locked1.task_id, task_id,
        "dispatcher returned the wrong task on the first pick"
    );
    assert_eq!(
        locked1.attempt_number, 0,
        "first dispatch must surface attempt_number=0"
    );
    tx.record_acquisition(NewLease {
        task_id,
        attempt_number: 0,
        worker_id: worker_id1,
        acquired_at: now1,
        timeout_at: timeout_at1,
    })
    .await
    .expect("record_acquisition #1");
    tx.commit().await.expect("dispatch1 commit");

    // Act — first WAITING_RETRY transition. attempt_number=0 row in
    // task_results, and tasks.attempt_number must bump to 1.
    let mut tx = storage.begin().await.expect("complete1 begin");
    tx.complete_task(
        &LeaseRef {
            task_id,
            attempt_number: 0,
            worker_id: worker_id1,
        },
        TaskOutcome::WaitingRetry {
            error_class: "transient".into(),
            error_message: "first failure".into(),
            error_details: Bytes::from_static(b"d0"),
            retry_after: Timestamp::from_unix_millis(T0_MS + 11),
            recorded_at: Timestamp::from_unix_millis(T0_MS + 12),
        },
    )
    .await
    .expect("complete_task #1");
    tx.commit().await.expect("complete1 commit");

    // Second dispatch — picks the WAITING_RETRY row whose retry_after is
    // in the past. Backends that filter only `status = 'PENDING'` will
    // return None here and the test fails on the unwrap below.
    let now2 = Timestamp::from_unix_millis(T0_MS + 13);
    let timeout_at2 = Timestamp::from_unix_millis(T0_MS + ONE_HOUR_MS + 1);
    let worker_id2 = WorkerId::generate();
    let mut tx = storage.begin().await.expect("dispatch2 begin");
    let locked2 = tx
        .pick_and_lock_pending(PickCriteria {
            namespace_filter: NamespaceFilter::Single(namespace.clone()),
            task_types_filter: TaskTypeFilter::AnyOf(vec![task_type.clone()]),
            ordering: PickOrdering::PriorityFifo,
            now: now2,
        })
        .await
        .expect("pick WAITING_RETRY")
        .expect(
            "WAITING_RETRY row must be re-dispatched once retry_after is in the past — \
             a backend that filters only `status = 'PENDING'` would fail here",
        );
    assert_eq!(locked2.task_id, task_id);
    assert_eq!(
        locked2.attempt_number, 1,
        "second dispatch must surface the bumped attempt_number — without the bump, \
         the follow-up complete_task collides on the task_results PK"
    );
    tx.record_acquisition(NewLease {
        task_id,
        attempt_number: 1,
        worker_id: worker_id2,
        acquired_at: now2,
        timeout_at: timeout_at2,
    })
    .await
    .expect("record_acquisition #2");
    tx.commit().await.expect("dispatch2 commit");

    // Second WAITING_RETRY transition — writes attempt_number=1 row and
    // bumps tasks.attempt_number to 2. Without the §1 fix this INSERT
    // would collide on PK(task_id, 1) ... actually no: without the fix
    // it would collide on PK(task_id, 0) because the bump never
    // happened. Either way the contract checked here is the same:
    // exactly one task_results row per distinct attempt.
    let mut tx = storage.begin().await.expect("complete2 begin");
    tx.complete_task(
        &LeaseRef {
            task_id,
            attempt_number: 1,
            worker_id: worker_id2,
        },
        TaskOutcome::WaitingRetry {
            error_class: "transient".into(),
            error_message: "second failure".into(),
            error_details: Bytes::from_static(b"d1"),
            retry_after: Timestamp::from_unix_millis(T0_MS + 14),
            recorded_at: Timestamp::from_unix_millis(T0_MS + 15),
        },
    )
    .await
    .expect(
        "second complete_task(WaitingRetry) must succeed — a constraint violation here \
         indicates the WAITING_RETRY transition did not bump attempt_number",
    );
    tx.commit().await.expect("complete2 commit");

    // Assert — the latest task_results row is at attempt_number = 1
    // (the most recent failure), and the task row's attempt_number has
    // been bumped to 2 (ready for the next dispatch).
    let mut tx = storage.begin().await.expect("verify begin");
    let latest = tx
        .get_latest_task_result(task_id)
        .await
        .expect("get_latest_task_result")
        .expect("task_results must have a row");
    assert_eq!(
        latest.attempt_number, 1,
        "latest task_results row must be the second attempt"
    );
    assert!(matches!(latest.outcome, TaskOutcomeKind::RetryableFail));

    let task_row = tx
        .get_task_by_id(task_id)
        .await
        .expect("get_task_by_id")
        .expect("task row");
    assert_eq!(
        task_row.attempt_number, 2,
        "tasks.attempt_number must be bumped past the last completed attempt — \
         was {}, expected 2",
        task_row.attempt_number
    );
    tx.commit().await.expect("verify commit");
}
