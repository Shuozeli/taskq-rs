//! Conformance requirement #4: atomic conditional insert.
//!
//! `design.md` §8.2 #4: "Idempotency-key insertion is 'insert if not exists,
//! else return existing' within a transaction. Postgres `INSERT ... ON
//! CONFLICT`, FoundationDB read-then-write inside a transaction
//! (conflict-tracked), SQLite `INSERT OR IGNORE` all qualify."
//!
//! The trait method exercising this is `StorageTx::insert_task` paired with
//! `lookup_idempotency` per `design.md` §6.1.

use taskq_storage::ids::{IdempotencyKey, Namespace, TaskId};
use taskq_storage::{Storage, StorageError, StorageTx};

use crate::helpers::make_task;

/// Two concurrent submitters racing the same `(namespace, key)` MUST
/// collapse to exactly one task row. Use *identical* payload hashes so a
/// post-race lookup confirms the dedup record points at the winner.
pub async fn idempotency_key_insert_serializes_under_contention<S: Storage>(storage: &S) {
    // Arrange — two task ids, same key, same payload hash.
    let namespace = "ci-shared";
    let key = "ci-key";
    let shared_hash = [0x42u8; 32];
    let task_id_a = TaskId::generate();
    let task_id_b = TaskId::generate();
    let (task_a, dedup_a) = make_task(task_id_a, namespace, "ci.task", key, shared_hash);
    let (task_b, dedup_b) = make_task(task_id_b, namespace, "ci.task", key, shared_hash);

    // Act — race two transactions through `insert_task → commit`.
    let race_a = async {
        let mut tx = storage.begin().await?;
        match tx.insert_task(task_a, dedup_a).await {
            Ok(_) => tx.commit().await.map(|_| true),
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    };
    let race_b = async {
        let mut tx = storage.begin().await?;
        match tx.insert_task(task_b, dedup_b).await {
            Ok(_) => tx.commit().await.map(|_| true),
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    };
    let (res_a, res_b) = tokio::join!(race_a, race_b);

    // Assert — exactly one wins; the other surfaces the constraint.
    let success_count = [&res_a, &res_b]
        .iter()
        .filter(|r| matches!(r, Ok(true)))
        .count();
    assert_eq!(
        success_count, 1,
        "exactly one of two racing inserts must commit; got A = {res_a:?}, B = {res_b:?}"
    );
    for outcome in [&res_a, &res_b] {
        match outcome {
            Ok(true) => {}
            Err(StorageError::ConstraintViolation(_))
            | Err(StorageError::SerializationConflict) => {}
            other => panic!(
                "expected ConstraintViolation or SerializationConflict on the loser, got {other:?}"
            ),
        }
    }

    // The dedup row's task_id must be the winner's.
    let mut tx = storage.begin().await.expect("post-race begin");
    let row = tx
        .lookup_idempotency(&Namespace::new(namespace), &IdempotencyKey::new(key))
        .await
        .expect("lookup")
        .expect("dedup row exists after race");
    tx.commit().await.expect("post-race commit");
    assert!(
        row.task_id == task_id_a || row.task_id == task_id_b,
        "dedup row must point at one of the two racing task ids"
    );
    assert_eq!(row.payload_hash, shared_hash);
}

/// Inserting two tasks with the same `(namespace, key)` but *different*
/// payload hashes MUST surface `StorageError::ConstraintViolation` on the
/// second insert (the unique-index violation; the CP layer translates this
/// to `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`).
pub async fn idempotency_key_insert_with_different_payload_returns_constraint_violation<
    S: Storage,
>(
    storage: &S,
) {
    // Arrange — first commit a task; then attempt a second submit with
    // the same key and a different payload hash.
    let namespace = "ci-mismatch";
    let key = "ci-key";
    let task_id_a = TaskId::generate();
    let (task_a, dedup_a) = make_task(task_id_a, namespace, "ci.task", key, [0x01u8; 32]);
    let mut tx_a = storage.begin().await.expect("begin a");
    tx_a.insert_task(task_a, dedup_a)
        .await
        .expect("first insert succeeds");
    tx_a.commit().await.expect("commit a");

    let task_id_b = TaskId::generate();
    let (task_b, dedup_b) = make_task(task_id_b, namespace, "ci.task", key, [0x02u8; 32]);

    // Act — second insert reuses the key with a fresh payload hash.
    let mut tx_b = storage.begin().await.expect("begin b");
    let result = tx_b.insert_task(task_b, dedup_b).await;
    let _ = tx_b.rollback().await;

    // Assert — ConstraintViolation surfaces. (Per `design.md` §6.1 the CP
    // layer maps this to IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD.)
    match result {
        Err(StorageError::ConstraintViolation(_)) => {}
        other => panic!("expected ConstraintViolation, got {other:?}"),
    }
}

/// `lookup_idempotency` MUST return the previously-inserted dedup record
/// (matching `task_id` + `payload_hash`) when a row exists.
pub async fn idempotency_key_lookup_finds_existing_record<S: Storage>(storage: &S) {
    // Arrange — insert one task.
    let namespace = "ci-lookup";
    let key = "ci-key";
    let payload_hash = [0xCDu8; 32];
    let task_id = TaskId::generate();
    let (task, dedup) = make_task(task_id, namespace, "ci.task", key, payload_hash);

    let mut tx_seed = storage.begin().await.expect("seed begin");
    tx_seed.insert_task(task, dedup).await.expect("seed insert");
    tx_seed.commit().await.expect("seed commit");

    // Act — look up the record from a fresh transaction.
    let mut tx_lookup = storage.begin().await.expect("lookup begin");
    let record = tx_lookup
        .lookup_idempotency(&Namespace::new(namespace), &IdempotencyKey::new(key))
        .await
        .expect("lookup ok");
    tx_lookup.commit().await.expect("lookup commit");

    // Assert — the record points at our task with the same hash.
    let record = record.expect("dedup row must exist after insert");
    assert_eq!(record.task_id, task_id);
    assert_eq!(record.payload_hash, payload_hash);
}
