//! Conformance requirement #1: external consistency.
//!
//! `design.md` §8.2 #1: "Strict serializability for state-transition
//! transactions; commit order respects real-time order."
//!
//! The carve-outs from §1.1 (heartbeats, rate quotas) are NOT subject to
//! this requirement and are tested elsewhere (or, for rate quotas, are
//! deliberately left untested for the eventually-consistent path).
//!
//! ## Test convention
//!
//! Every function below is structured Arrange / Act / Assert with explicit
//! comments. One Act per test; descriptive `<behaviour>` names. The bodies
//! exercise the public [`Storage`] / [`StorageTx`] surface only — no
//! backend-specific casts.

use taskq_storage::ids::{IdempotencyKey, Namespace, TaskId};
use taskq_storage::{Storage, StorageError, StorageTx};

use crate::helpers::make_task;

/// Two transactions racing the same `(namespace, key)` insert MUST collapse
/// to exactly one task row. The loser observes either a constraint
/// violation or a serialization conflict; either is a valid signal for the
/// CP layer to surface idempotency-mismatch / retry semantics.
pub async fn concurrent_inserts_serialize_correctly<S: Storage>(storage: &S) {
    // Arrange — two pending submits with the same (namespace, key) and
    // *different* payload hashes (so the loser cannot be a benign idempotent
    // hit; it MUST surface as a constraint or serialization error).
    let namespace = "external-consistency-ns";
    let key = "shared-key";
    let task_id_a = TaskId::generate();
    let task_id_b = TaskId::generate();
    let (task_a, dedup_a) = make_task(task_id_a, namespace, "ec.task", key, [0xAAu8; 32]);
    let (task_b, dedup_b) = make_task(task_id_b, namespace, "ec.task", key, [0xBBu8; 32]);

    // Act — race two `begin → insert_task → commit` flows. `tokio::join!`
    // polls both futures concurrently; on a true concurrent-writer backend
    // they overlap. On single-writer backends (SQLite) `begin` serializes
    // them, which is also a valid serial schedule.
    let outcome_a = async {
        let mut tx = storage.begin().await?;
        let res = tx.insert_task(task_a, dedup_a).await;
        match res {
            Ok(_) => tx.commit().await.map(|_| true),
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    };
    let outcome_b = async {
        let mut tx = storage.begin().await?;
        let res = tx.insert_task(task_b, dedup_b).await;
        match res {
            Ok(_) => tx.commit().await.map(|_| true),
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    };
    let (result_a, result_b) = tokio::join!(outcome_a, outcome_b);

    // Assert — exactly one tx commits Ok(true); the other surfaces
    // ConstraintViolation or SerializationConflict.
    let successes = [&result_a, &result_b]
        .iter()
        .filter(|r| matches!(r, Ok(true)))
        .count();
    assert_eq!(
        successes, 1,
        "exactly one of two concurrent inserts must succeed (got A = {result_a:?}, B = {result_b:?})"
    );
    for outcome in [&result_a, &result_b] {
        match outcome {
            Ok(true) => {}
            Err(StorageError::ConstraintViolation(_))
            | Err(StorageError::SerializationConflict) => {}
            other => {
                panic!("expected Ok or ConstraintViolation/SerializationConflict, got {other:?}")
            }
        }
    }

    // And exactly one task row should be visible afterwards via either id.
    let mut tx = storage.begin().await.expect("post-race begin");
    let row = tx
        .lookup_idempotency(&Namespace::new(namespace), &IdempotencyKey::new(key))
        .await
        .expect("lookup")
        .expect("dedup row present after race");
    tx.commit().await.expect("post-race commit");
    assert!(
        row.task_id == task_id_a || row.task_id == task_id_b,
        "post-race dedup row task_id must match one of the two submitters"
    );
}

/// A transaction started after another transaction's commit MUST observe
/// the committed effect — the "reads-after-writes" half of strict
/// serializability.
pub async fn read_after_write_observes_committed_state<S: Storage>(storage: &S) {
    // Arrange — a single fresh task to insert.
    let namespace = "read-after-write-ns";
    let task_id = TaskId::generate();
    let (task, dedup) = make_task(task_id, namespace, "ec.read", "k1", [0x11u8; 32]);

    // Act — Tx A inserts and commits, then Tx B starts AFTER A.commit
    // and reads the row. The read must succeed.
    let mut tx_a = storage.begin().await.expect("begin a");
    tx_a.insert_task(task, dedup).await.expect("insert a");
    tx_a.commit().await.expect("commit a");

    let mut tx_b = storage.begin().await.expect("begin b");
    let observed = tx_b
        .get_task_by_id(task_id)
        .await
        .expect("get_task_by_id b");
    tx_b.commit().await.expect("commit b");

    // Assert — Tx B saw the row Tx A committed.
    let observed = observed.expect("task A's commit must be visible to B");
    assert_eq!(observed.task_id, task_id);
    assert_eq!(observed.namespace.as_str(), namespace);
}
