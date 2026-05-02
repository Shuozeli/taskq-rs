//! Conformance requirement #2: non-blocking row locking with skip semantics.
//!
//! `design.md` §8.2 #2: "`pick_and_lock_pending` skips locked rows rather
//! than blocking. Vacuously satisfied by single-writer backends (SQLite)
//! since there is nothing to skip; backends supporting concurrent writers
//! must implement true skip-locking (Postgres `FOR UPDATE SKIP LOCKED`,
//! FoundationDB conflict-range retry, etc.)."
//!
//! Backends opting into the vacuous form set
//! [`crate::Options::vacuously_satisfied_skip_locking`] to `true`; the
//! suite then bypasses the concurrent-dispatcher exercise and records it
//! as satisfied by construction.

use std::collections::HashSet;

use taskq_storage::ids::{Namespace, TaskId, TaskType, Timestamp, WorkerId};
use taskq_storage::types::{NamespaceFilter, NewLease, PickCriteria, PickOrdering, TaskTypeFilter};
use taskq_storage::{Storage, StorageError, StorageTx};

use crate::helpers::{make_task, T0_MS};

/// Single-writer backends (SQLite) satisfy skip-locking vacuously: there
/// is nothing to skip because the writer lock is exclusive. This function
/// is a documentation hook — its mere invocation by [`crate::run_all`]
/// records that the carve-out path was exercised.
pub async fn vacuously_satisfied_for_single_writer() {
    // Arrange — nothing.
    // Act       — nothing (single-writer backends cannot exercise skip).
    // Assert    — trivial: requirement is vacuous in this scope.
}

/// Five concurrent dispatchers calling `pick_and_lock_pending` on a
/// namespace seeded with five PENDING tasks must each receive a *distinct*
/// task. The §8.2 #2 invariant: locks skip already-locked rows rather
/// than blocking, so every concurrent dispatcher makes forward progress.
pub async fn pick_and_lock_skip_locked_under_contention<S: Storage>(storage: &S) {
    // Arrange — seed 5 PENDING tasks in one namespace + task type.
    const N: usize = 5;
    let namespace = Namespace::new("skip-locking-ns");
    let task_type = TaskType::new("sl.task");

    let mut seeded: Vec<TaskId> = Vec::with_capacity(N);
    for i in 0..N {
        let task_id = TaskId::generate();
        seeded.push(task_id);
        let (task, dedup) = make_task(
            task_id,
            namespace.as_str(),
            task_type.as_str(),
            &format!("sl-key-{i}"),
            [i as u8; 32],
        );
        let mut tx = storage.begin().await.expect("seed begin");
        tx.insert_task(task, dedup).await.expect("seed insert");
        tx.commit().await.expect("seed commit");
    }

    // Act — race 5 dispatchers, each opening a transaction and calling
    // `pick_and_lock_pending`. We collect each transaction's locked
    // task_id, then commit. If skip-locking is honored every dispatcher
    // pulls a different row.
    //
    // Postgres SSI may surface a 40001 `SerializationConflict` when
    // concurrent dispatchers form a rw-dependency cycle on shared
    // metadata (e.g. partition routing). The CP layer's contract is to
    // transparently retry on `SerializationConflict` (`design.md` §6.4);
    // we mirror that here so this test exercises the §8.2 #2 invariant
    // on the *retry-quiesced* steady state, not transient SSI fires.
    async fn pick_one<S: Storage>(
        storage: &S,
        namespace: &Namespace,
        task_type: &TaskType,
    ) -> Option<TaskId> {
        const MAX_ATTEMPTS: usize = 8;
        for _attempt in 0..MAX_ATTEMPTS {
            let mut tx = storage.begin().await.expect("dispatcher begin");
            let criteria = PickCriteria {
                namespace_filter: NamespaceFilter::Single(namespace.clone()),
                task_types_filter: TaskTypeFilter::AnyOf(vec![task_type.clone()]),
                ordering: PickOrdering::PriorityFifo,
                now: Timestamp::from_unix_millis(T0_MS + 1),
            };
            match tx.pick_and_lock_pending(criteria).await {
                Ok(locked) => {
                    if let Some(ref locked_task) = locked {
                        // Stamp a runtime row so the lock is durable past
                        // the SELECT — matches §6.2 step 3a in prod.
                        if let Err(e) = tx
                            .record_acquisition(NewLease {
                                task_id: locked_task.task_id,
                                attempt_number: locked_task.attempt_number,
                                worker_id: WorkerId::generate(),
                                acquired_at: Timestamp::from_unix_millis(T0_MS + 2),
                                timeout_at: Timestamp::from_unix_millis(T0_MS + 60_000),
                            })
                            .await
                        {
                            if matches!(e, StorageError::SerializationConflict) {
                                let _ = tx.rollback().await;
                                continue;
                            }
                            panic!("record_acquisition: {e:?}");
                        }
                    }
                    match tx.commit().await {
                        Ok(()) => return locked.map(|l| l.task_id),
                        Err(StorageError::SerializationConflict) => continue,
                        Err(e) => panic!("dispatcher commit: {e:?}"),
                    }
                }
                Err(StorageError::SerializationConflict) => {
                    let _ = tx.rollback().await;
                    continue;
                }
                Err(e) => panic!("pick_and_lock_pending: {e:?}"),
            }
        }
        panic!("dispatcher exhausted retry budget");
    }

    let (a, b, c, d, e) = tokio::join!(
        pick_one(storage, &namespace, &task_type),
        pick_one(storage, &namespace, &task_type),
        pick_one(storage, &namespace, &task_type),
        pick_one(storage, &namespace, &task_type),
        pick_one(storage, &namespace, &task_type),
    );
    let picks: Vec<TaskId> = [a, b, c, d, e].into_iter().flatten().collect();

    // Assert — every dispatcher got *some* task and every picked task is
    // unique. The total count equals N (5), demonstrating that no row was
    // double-dispatched and no dispatcher blocked.
    assert_eq!(
        picks.len(),
        N,
        "all {N} dispatchers must receive a task; got {picks:?}"
    );
    let unique: HashSet<&TaskId> = picks.iter().collect();
    assert_eq!(
        unique.len(),
        N,
        "skip-locking violated: a task was dispatched twice across concurrent pickers"
    );
    for picked in &picks {
        assert!(
            seeded.contains(picked),
            "dispatcher returned a task_id we never seeded ({picked})"
        );
    }
}
