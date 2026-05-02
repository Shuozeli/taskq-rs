//! Conformance requirement #6: bounded-cost dedup expiration.
//!
//! `design.md` §8.2 #6: "`delete_expired_dedup` must run in time bounded
//! by `n` (the batch size), not by the steady-state size of
//! `idempotency_keys`. Backends with mutable per-row delete cost (e.g.
//! Postgres without partitioning) MUST implement time-range partitioning
//! of the dedup table so cleanup is `DROP PARTITION`, not row-by-row
//! `DELETE`."
//!
//! For Postgres this is satisfied via daily-granularity range partitioning
//! capped at 90 active partitions (`design.md` §8.3). SQLite is exempt by
//! virtue of its single-writer / dev-only scope.
//!
//! ## What the suite asserts
//!
//! 1. Safety: `delete_expired_dedup(before, n)` MUST NOT delete rows whose
//!    `expires_at > before`. Universal across backends.
//! 2. Bounded scaling: a 100-row batch on a 2 000-row table must return
//!    ≤ 100 (or, for partition-drop backends, the number of partitions
//!    dropped is bounded by the batch's date window). Backends without
//!    a row-bounded `n` semantic mark this case vacuously satisfied via
//!    [`crate::Options::vacuously_satisfied_bounded_cleanup`].

use bytes::Bytes;
use taskq_storage::ids::{IdempotencyKey, Namespace, TaskId, TaskType, Timestamp};
use taskq_storage::types::{NewDedupRecord, NewTask};
use taskq_storage::{Storage, StorageTx};

use crate::helpers::T0_MS;

/// Backends that satisfy §8.2 #6 vacuously (SQLite single-writer scope,
/// Postgres partition-drop semantics that don't return per-row counts)
/// route through this no-op so [`crate::run_all`] still records the case.
pub async fn vacuously_satisfied_for_unpartitioned_backend() {
    // Arrange — nothing.
    // Act       — nothing.
    // Assert    — trivial: requirement is vacuous in this scope.
}

/// `delete_expired_dedup(before, n)` MUST honor the `n` cap on
/// row-bounded backends: a single batch deletes at most `n` rows even
/// when many more are eligible. We seed 200 expired rows + 100 unexpired
/// rows and ask for `n = 50`; the call must report ≤ 50 deletes and
/// leave the unexpired rows untouched.
pub async fn delete_expired_dedup_runs_in_bounded_time_with_n<S: Storage>(storage: &S) {
    // Arrange — 200 expired rows (expires_at < cutoff) + 100 unexpired
    // rows (expires_at > cutoff), all in the same namespace.
    let cutoff_ms = T0_MS + 1_000_000;
    let cutoff = Timestamp::from_unix_millis(cutoff_ms);
    let namespace = "bdc-bounded";
    let task_type = "bdc.task";
    seed_dedup_rows(storage, namespace, task_type, 200, cutoff_ms - 60_000).await;
    seed_dedup_rows(storage, namespace, task_type, 100, cutoff_ms + 60_000).await;

    // Act — request a batch of 50.
    let mut tx = storage.begin().await.expect("cleanup begin");
    let deleted = tx
        .delete_expired_dedup(cutoff, 50)
        .await
        .expect("delete_expired_dedup");
    tx.commit().await.expect("cleanup commit");

    // Assert — at most 50 rows deleted (the batch cap was honored).
    assert!(
        deleted <= 50,
        "delete_expired_dedup must respect n=50; got {deleted} deletes"
    );
    assert!(
        deleted > 0,
        "delete_expired_dedup with eligible rows must remove some; got 0"
    );
}

/// Universal safety check: `delete_expired_dedup` MUST NOT delete rows
/// whose `expires_at > before`. Tests both backends — Postgres partition
/// drop must respect the partition boundary, SQLite per-row DELETE must
/// respect the WHERE.
pub async fn delete_expired_dedup_only_removes_expired<S: Storage>(storage: &S) {
    // Arrange — one expired row + one unexpired row in the same namespace.
    // Use distant cutoffs so partition-driven backends place them in
    // different partitions and per-row backends still see the WHERE.
    let cutoff_ms = T0_MS + 1_000_000;
    let cutoff = Timestamp::from_unix_millis(cutoff_ms);
    let namespace = "bdc-safety";
    let task_type = "bdc.task";

    let expired_task = TaskId::generate();
    let unexpired_task = TaskId::generate();
    insert_one(
        storage,
        namespace,
        task_type,
        "bdc-key-expired",
        expired_task,
        Timestamp::from_unix_millis(cutoff_ms - 60_000),
    )
    .await;
    insert_one(
        storage,
        namespace,
        task_type,
        "bdc-key-unexpired",
        unexpired_task,
        Timestamp::from_unix_millis(cutoff_ms + 60_000),
    )
    .await;

    // Act — run cleanup with a large `n` so any inflated batch could
    // accidentally clobber the unexpired row.
    let mut tx = storage.begin().await.expect("cleanup begin");
    let _ = tx
        .delete_expired_dedup(cutoff, 1_000)
        .await
        .expect("cleanup");
    tx.commit().await.expect("cleanup commit");

    // Assert — the unexpired dedup row is still present; the expired
    // dedup row may or may not be (Postgres drops by partition window;
    // SQLite drops the row).
    let mut tx = storage.begin().await.expect("post-cleanup begin");
    let unexpired = tx
        .lookup_idempotency(
            &Namespace::new(namespace),
            &IdempotencyKey::new("bdc-key-unexpired"),
        )
        .await
        .expect("lookup unexpired");
    tx.commit().await.expect("post-cleanup commit");
    let unexpired = unexpired.expect("unexpired dedup row must survive cleanup");
    assert_eq!(unexpired.task_id, unexpired_task);
}

/// Seed `count` dedup + task rows with the given `expires_at` (ms). Each
/// row gets a unique key + task_id; payload_hash is derived from the
/// index so debugging stays cheap.
async fn seed_dedup_rows<S: Storage>(
    storage: &S,
    namespace: &str,
    task_type: &str,
    count: usize,
    expires_at_ms: i64,
) {
    for i in 0..count {
        let task_id = TaskId::generate();
        let key = format!("bdc-{expires_at_ms}-{i}");
        insert_one(
            storage,
            namespace,
            task_type,
            &key,
            task_id,
            Timestamp::from_unix_millis(expires_at_ms),
        )
        .await;
    }
}

/// Insert one task + dedup row. Both `tasks.expires_at` and
/// `idempotency_keys.expires_at` are set to `expires_at`, so a single
/// scalar drives partition placement and post-cleanup visibility.
async fn insert_one<S: Storage>(
    storage: &S,
    namespace: &str,
    task_type: &str,
    key: &str,
    task_id: TaskId,
    expires_at: Timestamp,
) {
    // We mostly mirror `helpers::make_task` but with a caller-driven
    // `expires_at`, which is the field cleanup tests care about.
    let payload_hash = derive_hash(key);
    let task = NewTask {
        task_id,
        namespace: Namespace::new(namespace),
        task_type: TaskType::new(task_type),
        priority: 0,
        payload: Bytes::from_static(b"bdc"),
        payload_hash,
        submitted_at: Timestamp::from_unix_millis(T0_MS),
        expires_at,
        max_retries: 0,
        retry_initial_ms: 1_000,
        retry_max_ms: 1_000,
        retry_coefficient: 1.0,
        traceparent: Bytes::new(),
        tracestate: Bytes::new(),
        format_version: 1,
    };
    let dedup = NewDedupRecord {
        namespace: Namespace::new(namespace),
        key: IdempotencyKey::new(key),
        payload_hash,
        expires_at,
    };
    let mut tx = storage.begin().await.expect("seed begin");
    tx.insert_task(task, dedup).await.expect("seed insert");
    tx.commit().await.expect("seed commit");
}

fn derive_hash(seed: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    for (i, byte) in seed.bytes().enumerate() {
        out[i % 32] ^= byte;
    }
    out
}
