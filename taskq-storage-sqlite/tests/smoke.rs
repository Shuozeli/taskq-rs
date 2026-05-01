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
        NamespaceQuotaUpsert, NewDedupRecord, NewLease, NewTask, PickCriteria, PickOrdering,
        ReplayOutcome, TaskFilter, TaskOutcome, TaskStatus, TaskTypeFilter, TerminalState,
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

fn well_formed_upsert() -> NamespaceQuotaUpsert {
    NamespaceQuotaUpsert {
        max_pending: Some(100),
        max_inflight: Some(10),
        max_workers: Some(20),
        max_waiters_per_replica: Some(5000),
        max_submit_rpm: Some(600),
        max_dispatch_rpm: Some(120),
        max_replay_per_second: Some(50),
        max_retries_ceiling: 5,
        max_idempotency_ttl_seconds: 86_400,
        max_payload_bytes: 4_096,
        max_details_bytes: 1_024,
        min_heartbeat_interval_seconds: 5,
        lazy_extension_threshold_seconds: 30,
        max_error_classes: 16,
        max_task_types: 8,
        trace_sampling_ratio: 0.5,
        log_level_override: None,
        audit_log_retention_days: 14,
        metrics_export_enabled: true,
    }
}

#[tokio::test]
async fn upsert_namespace_quota_inserts_then_overwrites_writable_subset() {
    // Arrange: open an in-memory backend.
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("open in-memory sqlite");

    // Act 1: first upsert creates the row.
    let mut tx = storage.begin().await.expect("begin tx");
    tx.upsert_namespace_quota(&Namespace::new("ns-x"), well_formed_upsert())
        .await
        .expect("first upsert");
    tx.commit().await.expect("commit");

    // Act 2: second upsert with mutated value overwrites it.
    let mut bumped = well_formed_upsert();
    bumped.max_retries_ceiling = 9;
    let mut tx = storage.begin().await.expect("begin tx");
    tx.upsert_namespace_quota(&Namespace::new("ns-x"), bumped)
        .await
        .expect("second upsert");
    tx.commit().await.expect("commit");

    // Assert: the persisted quota reflects the latest write.
    let mut tx = storage.begin().await.expect("begin tx");
    let quota = tx
        .get_namespace_quota(&Namespace::new("ns-x"))
        .await
        .expect("quota present");
    tx.commit().await.expect("commit");
    assert_eq!(quota.max_retries_ceiling, 9);
    assert_eq!(quota.max_payload_bytes, 4_096);
}

#[tokio::test]
async fn list_tasks_by_filter_returns_oldest_first_for_namespace() {
    // Arrange: insert two tasks in `ns-list` with different submit times.
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("open in-memory sqlite");
    let early_id = TaskId::generate();
    let late_id = TaskId::generate();

    let mut tx = storage.begin().await.expect("begin tx");
    let (mut early_task, early_dedup) = new_task(early_id, "ns-list", "k-early");
    early_task.submitted_at = Timestamp::from_unix_millis(1_000);
    tx.insert_task(early_task, early_dedup)
        .await
        .expect("insert early");
    let (mut late_task, late_dedup) = new_task(late_id, "ns-list", "k-late");
    late_task.submitted_at = Timestamp::from_unix_millis(2_000);
    tx.insert_task(late_task, late_dedup)
        .await
        .expect("insert late");
    tx.commit().await.expect("commit");

    // Act
    let mut tx = storage.begin().await.expect("begin tx");
    let tasks = tx
        .list_tasks_by_filter(&Namespace::new("ns-list"), TaskFilter::default(), 10)
        .await
        .expect("list");
    tx.commit().await.expect("commit");

    // Assert
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].task_id, early_id);
    assert_eq!(tasks[1].task_id, late_id);
}

#[tokio::test]
async fn list_tasks_by_terminal_status_only_returns_terminal_failures() {
    // Arrange: provision one PENDING task (excluded) and one
    // FAILED_NONRETRYABLE task (included).
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("open in-memory sqlite");
    let pending_id = TaskId::generate();
    let failed_id = TaskId::generate();
    let worker_id = WorkerId::generate();
    let now = Timestamp::from_unix_millis(1_000);

    let mut tx = storage.begin().await.expect("begin tx");
    let (pending_task, pending_dedup) = new_task(pending_id, "ns-term", "k-p");
    tx.insert_task(pending_task, pending_dedup)
        .await
        .expect("insert pending");
    let (failed_task, failed_dedup) = new_task(failed_id, "ns-term", "k-f");
    tx.insert_task(failed_task, failed_dedup)
        .await
        .expect("insert failed");
    tx.record_acquisition(NewLease {
        task_id: failed_id,
        attempt_number: 0,
        worker_id,
        acquired_at: now,
        timeout_at: Timestamp::from_unix_millis(60_000),
    })
    .await
    .expect("record acquisition");
    tx.complete_task(
        &taskq_storage::types::LeaseRef {
            task_id: failed_id,
            attempt_number: 0,
            worker_id,
        },
        TaskOutcome::FailedNonretryable {
            error_class: "boom".to_owned(),
            error_message: "test".to_owned(),
            error_details: Bytes::new(),
            recorded_at: now,
        },
    )
    .await
    .expect("complete-as-failed");
    tx.commit().await.expect("commit");

    // Act
    let mut tx = storage.begin().await.expect("begin tx");
    let tasks = tx
        .list_tasks_by_terminal_status(
            &Namespace::new("ns-term"),
            vec![TerminalState::FailedNonretryable],
            10,
        )
        .await
        .expect("list");
    tx.commit().await.expect("commit");

    // Assert
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task_id, failed_id);
}

#[tokio::test]
async fn replay_task_resets_failed_task_to_pending() {
    // Arrange: create + fail a task so it lands in FAILED_NONRETRYABLE.
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("open in-memory sqlite");
    let task_id = TaskId::generate();
    let worker_id = WorkerId::generate();
    let now = Timestamp::from_unix_millis(1_000);

    let mut tx = storage.begin().await.expect("begin tx");
    let (task, dedup) = new_task(task_id, "ns-replay", "k1");
    tx.insert_task(task, dedup).await.expect("insert");
    tx.record_acquisition(NewLease {
        task_id,
        attempt_number: 0,
        worker_id,
        acquired_at: now,
        timeout_at: Timestamp::from_unix_millis(60_000),
    })
    .await
    .expect("record acquisition");
    tx.complete_task(
        &taskq_storage::types::LeaseRef {
            task_id,
            attempt_number: 0,
            worker_id,
        },
        TaskOutcome::FailedNonretryable {
            error_class: "boom".to_owned(),
            error_message: "test".to_owned(),
            error_details: Bytes::new(),
            recorded_at: now,
        },
    )
    .await
    .expect("complete-as-failed");
    tx.commit().await.expect("commit");

    // Act
    let mut tx = storage.begin().await.expect("begin tx");
    let outcome = tx.replay_task(task_id).await.expect("replay");
    tx.commit().await.expect("commit");

    // Assert
    assert_eq!(outcome, ReplayOutcome::Replayed);
    let mut tx = storage.begin().await.expect("begin tx");
    let task = tx
        .get_task_by_id(task_id)
        .await
        .expect("ok")
        .expect("present");
    tx.commit().await.expect("commit");
    assert_eq!(task.status, TaskStatus::Pending);
    assert_eq!(task.attempt_number, 0);
}

#[tokio::test]
async fn replay_task_refuses_when_status_is_not_terminal_failure() {
    // Arrange: a freshly-inserted PENDING task is not eligible for replay.
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("open in-memory sqlite");
    let task_id = TaskId::generate();
    let mut tx = storage.begin().await.expect("begin tx");
    let (task, dedup) = new_task(task_id, "ns-pending", "k1");
    tx.insert_task(task, dedup).await.expect("insert");
    tx.commit().await.expect("commit");

    // Act
    let mut tx = storage.begin().await.expect("begin tx");
    let outcome = tx.replay_task(task_id).await.expect("replay");
    tx.commit().await.expect("commit");

    // Assert
    assert_eq!(outcome, ReplayOutcome::NotInTerminalFailureState);
}

#[tokio::test]
async fn add_error_classes_is_idempotent_on_repeated_insert() {
    // Arrange
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("open in-memory sqlite");
    let ns = Namespace::new("ns-classes");
    let classes = vec!["transient".to_owned(), "permanent".to_owned()];

    // Act: insert twice.
    let mut tx = storage.begin().await.expect("begin tx");
    tx.add_error_classes(&ns, &classes).await.expect("add 1");
    tx.add_error_classes(&ns, &classes).await.expect("add 2");
    tx.commit().await.expect("commit");

    // Assert: no error, second insert was a no-op via INSERT OR IGNORE.
    // (We don't yet have a count primitive; the success of the second
    // insert is the assertion — a non-idempotent insert would have
    // surfaced a PK constraint violation.)
}

#[tokio::test]
async fn add_task_types_is_idempotent_on_repeated_insert() {
    // Arrange
    let storage = SqliteStorage::open_in_memory()
        .await
        .expect("open in-memory sqlite");
    let ns = Namespace::new("ns-types");
    let types = vec![TaskType::new("email"), TaskType::new("sms")];

    // Act: insert twice.
    let mut tx = storage.begin().await.expect("begin tx");
    tx.add_task_types(&ns, &types).await.expect("add 1");
    tx.add_task_types(&ns, &types).await.expect("add 2");
    tx.commit().await.expect("commit");

    // Assert: completed without error — INSERT OR IGNORE prevents the
    // PK violation on the second pass.
}
