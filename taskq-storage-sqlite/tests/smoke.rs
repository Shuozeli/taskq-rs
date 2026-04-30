//! Smoke tests for the SQLite backend. In-memory databases only — no
//! external infra required.
//!
//! Following the AAA testing pattern from `.claude/rules/shared/common/
//! testing-patterns.md`: each test marks Arrange / Act / Assert phases
//! explicitly with comments, has a single Act, and a behavior-describing
//! name.

use bytes::Bytes;

use taskq_storage::{
    error::StorageError,
    ids::{IdempotencyKey, Namespace, TaskId, TaskType, Timestamp, WorkerId},
    traits::{Storage, StorageTx},
    types::{
        NewDedupRecord, NewLease, NewTask, PickCriteria, PickOrdering, TaskOutcome, TaskTypeFilter,
    },
};
use taskq_storage_sqlite::SqliteStorage;

fn new_task(task_id: TaskId, namespace: &str, key: &str) -> (NewTask, NewDedupRecord) {
    let payload = b"hello".to_vec();
    let payload_hash = [0xAA; 32];
    let task = NewTask {
        task_id,
        namespace: Namespace::new(namespace),
        task_type: TaskType::new("smoke.echo"),
        priority: 0,
        payload: Bytes::from(payload),
        payload_hash,
        submitted_at: Timestamp::from_unix_millis(1_700_000_000_000),
        expires_at: Timestamp::from_unix_millis(1_800_000_000_000),
        max_retries: 3,
        retry_initial_ms: 1_000,
        retry_max_ms: 60_000,
        retry_coefficient: 2.0,
        traceparent: Bytes::from_static(b"00-trace-span-01"),
        tracestate: Bytes::new(),
        format_version: 1,
    };
    let dedup = NewDedupRecord {
        namespace: Namespace::new(namespace),
        key: IdempotencyKey::new(key),
        payload_hash,
        expires_at: Timestamp::from_unix_millis(1_800_000_000_000),
    };
    (task, dedup)
}

#[tokio::test]
async fn submit_acquire_complete_round_trip_persists_terminal_state() {
    // Arrange: open an in-memory SQLite backend, insert one task with a
    // fresh idempotency key, and acquire it as a worker.
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("open in-memory sqlite");

    let task_id = TaskId::generate();
    let (task, dedup) = new_task(task_id, "tenant_a", "key-1");

    let mut tx = storage.begin().await.expect("begin tx");
    tx.insert_task(task.clone(), dedup.clone())
        .await
        .expect("insert task");
    tx.commit().await.expect("commit insert");

    let mut tx = storage.begin().await.expect("begin tx");
    let lookup = tx
        .lookup_idempotency(&Namespace::new("tenant_a"), &IdempotencyKey::new("key-1"))
        .await
        .expect("lookup");
    assert!(
        lookup.is_some(),
        "freshly inserted dedup row should be found"
    );

    let worker = WorkerId::generate();
    let criteria = PickCriteria {
        namespace_filter: taskq_storage::types::NamespaceFilter::Single(Namespace::new("tenant_a")),
        task_types_filter: TaskTypeFilter::AnyOf(vec![TaskType::new("smoke.echo")]),
        ordering: PickOrdering::PriorityFifo,
        now: Timestamp::from_unix_millis(1_700_000_001_000),
    };
    let locked = tx.pick_and_lock_pending(criteria).await.expect("pick");
    let locked = locked.expect("a pending task is available");
    assert_eq!(locked.task_id, task_id);
    assert_eq!(locked.attempt_number, 0);

    let lease = NewLease {
        task_id: locked.task_id,
        attempt_number: locked.attempt_number,
        worker_id: worker,
        acquired_at: Timestamp::from_unix_millis(1_700_000_001_000),
        timeout_at: Timestamp::from_unix_millis(1_700_000_061_000),
    };
    tx.record_acquisition(lease).await.expect("record acq");
    tx.commit().await.expect("commit acquire");

    // Act: complete the task as success.
    let mut tx = storage.begin().await.expect("begin tx");
    tx.complete_task(
        &taskq_storage::types::LeaseRef {
            task_id,
            attempt_number: 0,
            worker_id: worker,
        },
        TaskOutcome::Success {
            result_payload: Bytes::from_static(b"ok"),
            recorded_at: Timestamp::from_unix_millis(1_700_000_002_000),
        },
    )
    .await
    .expect("complete");
    tx.commit().await.expect("commit complete");

    // Assert: a subsequent pick finds nothing (task is COMPLETED), and the
    // idempotency-key lookup still returns the original task_id.
    let mut tx = storage.begin().await.expect("begin tx");
    let again = tx
        .pick_and_lock_pending(PickCriteria {
            namespace_filter: taskq_storage::types::NamespaceFilter::Single(Namespace::new(
                "tenant_a",
            )),
            task_types_filter: TaskTypeFilter::AnyOf(vec![TaskType::new("smoke.echo")]),
            ordering: PickOrdering::PriorityFifo,
            now: Timestamp::from_unix_millis(1_700_000_003_000),
        })
        .await
        .expect("re-pick");
    assert!(again.is_none(), "completed task must not be re-pickable");

    let lookup = tx
        .lookup_idempotency(&Namespace::new("tenant_a"), &IdempotencyKey::new("key-1"))
        .await
        .expect("lookup");
    let lookup = lookup.expect("dedup row preserved through completion");
    assert_eq!(lookup.task_id, task_id);
    tx.commit().await.expect("commit final");
}

#[tokio::test]
async fn duplicate_idempotency_key_returns_constraint_violation() {
    // Arrange: open an in-memory backend and insert one task. The second
    // insert reuses the same `(namespace, key)` pair so the
    // `idempotency_keys` PK constraint must trigger.
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("open in-memory sqlite");

    let first_id = TaskId::generate();
    let (task, dedup) = new_task(first_id, "tenant_b", "shared-key");

    let mut tx = storage.begin().await.expect("begin tx");
    tx.insert_task(task, dedup).await.expect("first insert");
    tx.commit().await.expect("commit first");

    // Act: try to insert a different task with the same key.
    let second_id = TaskId::generate();
    let (mut task2, dedup2) = new_task(second_id, "tenant_b", "shared-key");
    // Different payload hash to make the mismatch detectable above this
    // layer; the PK constraint fires regardless of payload content.
    task2.payload_hash = [0xBB; 32];

    let mut tx = storage.begin().await.expect("begin tx");
    let result = tx.insert_task(task2, dedup2).await;
    let err = result.expect_err("duplicate key must error");

    // Assert: the error is a ConstraintViolation (PK violation on
    // `idempotency_keys`).
    match err {
        StorageError::ConstraintViolation(_) => {}
        other => panic!("expected ConstraintViolation, got {other:?}"),
    }
    tx.rollback().await.expect("rollback");
}
