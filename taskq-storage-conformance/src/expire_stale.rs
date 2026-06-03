//! Conformance: TTL expiration for un-dispatched / waiting-retry tasks.
//!
//! Cross-backend invariant for Reaper C (`taskq_cp::reapers::reaper_c_tick`).
//! Without it, a PENDING or WAITING_RETRY task whose `expires_at` has
//! elapsed sits in its non-terminal status forever — the dispatcher's
//! `expires_at > now` filter prevents re-dispatch, but no path
//! transitions the row to a terminal state, so a caller polling
//! `GetTaskResult` never sees `EXPIRED`.
//!
//! Two assertions:
//!
//! 1. `expire_stale_tasks` transitions the right rows: only `PENDING`
//!    and `WAITING_RETRY` past TTL, never DISPATCHED / COMPLETED /
//!    CANCELLED / etc.
//! 2. The doomed rows' `idempotency_keys` rows are released — same
//!    semantic as `cancel_task` (the work didn't run, the dedup
//!    window is reusable). Verified via `lookup_idempotency` returning
//!    `None` for the released key.

use bytes::Bytes;
use taskq_storage::ids::{IdempotencyKey, Namespace, TaskId, TaskType, Timestamp};
use taskq_storage::types::{NamespaceFilter, NewLease, PickCriteria, PickOrdering, TaskTypeFilter};
use taskq_storage::{Storage, StorageTx};

use crate::helpers::{make_task, T0_MS};

/// Drive `expire_stale_tasks` against a backend seeded with a mix of
/// stale-PENDING, fresh-PENDING, and DISPATCHED tasks. Asserts that
/// only the stale-PENDING row transitions to EXPIRED, and that its
/// `idempotency_keys` row is released.
pub async fn expire_stale_tasks_only_transitions_pending_or_waiting_retry_past_ttl<S: Storage>(
    storage: &S,
) {
    // Arrange — three tasks in one namespace:
    //
    //   stale_id    : PENDING, expires_at < now      → must transition
    //   fresh_id    : PENDING, expires_at >> now     → must NOT transition
    //   dispatched_id : DISPATCHED, expires_at < now → must NOT transition
    //
    // We pick `now` deliberately past `T0_MS + 1_000ms` so the stale
    // task's expires_at is in the past but the fresh task's is well
    // in the future.
    let namespace = Namespace::new("expire-stale-ns");
    let task_type = TaskType::new("ex.task");
    let stale_id = TaskId::generate();
    let fresh_id = TaskId::generate();
    let dispatched_id = TaskId::generate();
    let stale_key = "expire-stale-key";

    // Stale task: expires_at = T0 + 1ms.
    let (mut stale_task, stale_dedup) = make_task(
        stale_id,
        namespace.as_str(),
        task_type.as_str(),
        stale_key,
        [0xAA; 32],
    );
    stale_task.expires_at = Timestamp::from_unix_millis(T0_MS + 1);
    let mut stale_dedup_owned = stale_dedup;
    stale_dedup_owned.expires_at = Timestamp::from_unix_millis(T0_MS + 1);

    // Fresh task: expires_at = T0 + 1h. Default helper picks 1h.
    let (fresh_task, fresh_dedup) = make_task(
        fresh_id,
        namespace.as_str(),
        task_type.as_str(),
        "expire-fresh-key",
        [0xBB; 32],
    );

    // DISPATCHED task: expires_at past, but dispatched. Must NOT be
    // touched by Reaper C — only the lease-timeout reaper (Reaper A)
    // owns DISPATCHED-row state.
    let (mut dispatched_task, dispatched_dedup) = make_task(
        dispatched_id,
        namespace.as_str(),
        task_type.as_str(),
        "expire-dispatched-key",
        [0xCC; 32],
    );
    dispatched_task.priority = 10;
    dispatched_task.expires_at = Timestamp::from_unix_millis(T0_MS + 1);
    let mut dispatched_dedup_owned = dispatched_dedup;
    dispatched_dedup_owned.expires_at = Timestamp::from_unix_millis(T0_MS + 1);

    let mut tx = storage.begin().await.expect("seed begin");
    tx.insert_task(stale_task, stale_dedup_owned)
        .await
        .expect("insert stale");
    tx.insert_task(fresh_task, fresh_dedup)
        .await
        .expect("insert fresh");
    tx.insert_task(dispatched_task, dispatched_dedup_owned)
        .await
        .expect("insert dispatched");
    tx.commit().await.expect("seed commit");

    // Lock the dispatched task into DISPATCHED.
    let mut tx = storage.begin().await.expect("dispatch begin");
    let locked = tx
        .pick_and_lock_pending(PickCriteria {
            namespace_filter: NamespaceFilter::Single(namespace.clone()),
            task_types_filter: TaskTypeFilter::AnyOf(vec![task_type.clone()]),
            ordering: PickOrdering::PriorityFifo,
            // Pick a `now` that's later than stale/dispatched
            // expires_at (T0+1) so they wouldn't be returned, but
            // still well before fresh's expires_at. The dispatcher's
            // `expires_at > now` filter excludes both stale +
            // dispatched here, so we explicitly bypass by re-seeding
            // the dispatched task with a fresh expires_at first ...
            //
            // Simpler: skip pick_and_lock for dispatched and just
            // record_acquisition directly. The PK is (task_id,
            // attempt_number=0) — neither table cares whether
            // `expires_at` is past at acquisition time.
            now: Timestamp::from_unix_millis(T0_MS - 1),
        })
        .await
        .expect("pick");
    let locked = locked.expect("dispatched fixture should be picked");
    assert_eq!(
        locked.task_id, dispatched_id,
        "priority should force the dispatched fixture to be picked"
    );

    // Record acquisition in the same transaction as pick_and_lock.
    // SQLite is single-writer; opening another transaction before this
    // one commits would block behind the writer lock.
    use taskq_storage::ids::WorkerId;
    tx.record_acquisition(NewLease {
        task_id: dispatched_id,
        attempt_number: 0,
        worker_id: WorkerId::generate(),
        acquired_at: Timestamp::from_unix_millis(T0_MS),
        timeout_at: Timestamp::from_unix_millis(T0_MS + 60_000),
    })
    .await
    .expect("record_acquisition");
    tx.commit().await.expect("dispatch commit");

    // Act — call expire_stale_tasks with `before` past the stale
    // task's expires_at (T0+1) but before the fresh task's
    // expires_at (T0 + 1h). Only the stale PENDING row qualifies.
    let now = Timestamp::from_unix_millis(T0_MS + 100);
    let mut tx = storage.begin().await.expect("expire begin");
    let count = tx
        .expire_stale_tasks(now, 100)
        .await
        .expect("expire_stale_tasks");
    tx.commit().await.expect("expire commit");

    // Assert — exactly one row transitioned.
    assert_eq!(count, 1, "only the stale PENDING task should expire");

    // Stale task is now EXPIRED.
    let mut tx = storage.begin().await.expect("verify begin");
    let stale = tx
        .get_task_by_id(stale_id)
        .await
        .expect("get stale")
        .expect("stale row exists");
    assert_eq!(
        stale.status,
        taskq_storage::TaskStatus::Expired,
        "stale PENDING task must be EXPIRED after the reaper tick"
    );

    // Fresh task is still PENDING (unchanged).
    let fresh = tx
        .get_task_by_id(fresh_id)
        .await
        .expect("get fresh")
        .expect("fresh row exists");
    assert_eq!(
        fresh.status,
        taskq_storage::TaskStatus::Pending,
        "fresh PENDING task must NOT be expired (its TTL hasn't elapsed)"
    );

    // Dispatched task is still DISPATCHED — Reaper C must not touch
    // DISPATCHED rows; that's Reaper A's domain.
    let dispatched = tx
        .get_task_by_id(dispatched_id)
        .await
        .expect("get dispatched")
        .expect("dispatched row exists");
    assert_eq!(
        dispatched.status,
        taskq_storage::TaskStatus::Dispatched,
        "DISPATCHED task with stale expires_at must NOT be touched by Reaper C"
    );

    // The stale task's idempotency key has been released — a fresh
    // submit with the same key would not hit the dedup row.
    let dedup_lookup = tx
        .lookup_idempotency(&namespace, &IdempotencyKey::new(stale_key))
        .await
        .expect("lookup_idempotency");
    assert!(
        dedup_lookup.is_none(),
        "Reaper C must release idempotency_keys for expired tasks (mirrors cancel_task)"
    );
    tx.commit().await.expect("verify commit");

    // Belt-and-suspenders: a brand-new submit with the same key + a
    // different payload hash should now succeed (insert_task would
    // surface ConstraintViolation if the dedup row were still
    // there).
    let mut tx = storage.begin().await.expect("resubmit begin");
    let resubmit_id = TaskId::generate();
    let (resubmit_task, resubmit_dedup) = make_task(
        resubmit_id,
        namespace.as_str(),
        task_type.as_str(),
        stale_key,
        [0xDD; 32],
    );
    let resubmit = tx.insert_task(resubmit_task, resubmit_dedup).await;
    drop(tx); // rollback or commit doesn't matter for this assertion
    assert!(
        resubmit.is_ok(),
        "after expire, the released key must be reusable; insert_task returned {resubmit:?}"
    );
    let _ = Bytes::new(); // keep `bytes` import live across the drop above
}
