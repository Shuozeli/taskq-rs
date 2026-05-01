//! Shared `cancel_internal` helper.
//!
//! `design.md` §6.7: `CancelTask` and `PurgeTasks` both transition the task
//! to `CANCELLED` (terminal) and DELETE the matching `idempotency_keys` row
//! in the same SERIALIZABLE transaction. Per §5, `CANCELLED` releases the
//! key — physical deletion (not a flag) implements that release. The pair
//! lives behind one helper so the two RPC paths cannot drift.
//!
//! Phase 5b implements the helper signature; **the underlying storage trait
//! does not yet expose a `cancel_task_in_tx` method** (see Phase 5c work
//! item in `tasks.md` §5.5). Until that lands, the helper returns a
//! [`CancelOutcome::Unsupported`] variant that callers translate into a
//! `Status::unimplemented(...)` reply. This keeps the call sites in
//! `task_queue::cancel_task` and (Phase 5c) `task_admin::purge_tasks` final-
//! shape so wiring lands once and only the storage trait grows.

use taskq_storage::{StorageError, TaskId};

use crate::state::StorageTxDyn;

/// Outcome of a single `cancel_internal` call.
///
/// `design.md` §6.7 distinguishes "was non-terminal, now CANCELLED" from
/// "already terminal" (idempotent no-op). Phase 5b ships only `Unsupported`
/// to signal that the storage backend does not yet expose the underlying
/// state-transition + dedup-deletion pair as a single atomic operation; the
/// caller surfaces that via `Status::unimplemented(...)`. Phase 5c will
/// extend this enum with `TransitionedToCancelled` and
/// `AlreadyTerminal { state: TerminalState }` once `StorageTx::cancel_task`
/// lands; the `cancel_task` / `purge_tasks` call sites already handle the
/// `Unsupported` arm so the only edit at that point is on the helper body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CancelOutcome {
    /// The storage backend does not yet implement the cancel-state-transition
    /// and dedup-deletion pair as a single atomic operation. The CP returns
    /// `UNIMPLEMENTED` to the caller. Phase 5c flips this to a real outcome.
    Unsupported,
}

// `TerminalState` is referenced by the docstring on `CancelOutcome` for the
// future `AlreadyTerminal { state }` variant. Phase 5c will reintroduce it
// inline; until then the doc-link target lives at the wire layer
// (`taskq.v1.TerminalState`) so we don't carry a dead enum here.

/// Cancel `task_id` inside the caller-owned transaction `tx`.
///
/// Per `design.md` §6.7:
///
/// 1. SELECT task FOR UPDATE.
/// 2. If non-terminal: set status = CANCELLED, delete the dedup row in the
///    same txn, return [`CancelOutcome::TransitionedToCancelled`].
/// 3. If terminal: return [`CancelOutcome::AlreadyTerminal { state }`] —
///    no-op for state, but the caller may still want to delete the dedup
///    row in the `PurgeTasks` path.
///
/// Phase 5b: the storage trait does not yet expose this state-transition
/// pair as one atomic call (the existing `complete_task` path covers the
/// worker-driven terminal transitions but not admin/caller-initiated
/// cancellation). Until Phase 5c grows the trait, this helper returns
/// [`CancelOutcome::Unsupported`] and the caller surfaces
/// `Status::unimplemented(...)`. The shape is otherwise final.
pub(crate) async fn cancel_internal(
    _tx: &mut dyn StorageTxDyn,
    _task_id: TaskId,
) -> Result<CancelOutcome, StorageError> {
    // TODO(phase-5c): once `StorageTx::cancel_task` lands, replace the body
    // with the SELECT-FOR-UPDATE / UPDATE / DELETE FROM idempotency_keys
    // sequence and map storage outcomes to the CancelOutcome variants
    // above. The signature is final; this is the only line that flips.
    Ok(CancelOutcome::Unsupported)
}

#[cfg(test)]
mod tests {
    use super::*;

    use taskq_storage::{
        AuditEntry, CapacityDecision, CapacityKind, DedupRecord, HeartbeatAck, IdempotencyKey,
        LeaseRef, LockedTask, Namespace, NamespaceQuota, NewDedupRecord, NewLease, NewTask,
        PickCriteria, RateDecision, RateKind, TaskOutcome, TaskType, Timestamp, WakeSignal,
        WorkerId,
    };

    use crate::state::{StorageTxFuture, WakeSignalStream};

    /// Stub transaction that satisfies `StorageTxDyn` without touching any
    /// real backend. Every method panics — `cancel_internal` only inspects
    /// the trait object indirectly, never calling any of the storage
    /// methods until Phase 5c lands the cancel-state-transition method.
    struct StubTx;

    impl StorageTxDyn for StubTx {
        fn lookup_idempotency<'a>(
            &'a mut self,
            _ns: &'a Namespace,
            _key: &'a IdempotencyKey,
        ) -> StorageTxFuture<'a, Result<Option<DedupRecord>, StorageError>> {
            unreachable!("cancel_internal does not invoke lookup_idempotency in Phase 5b")
        }
        fn insert_task<'a>(
            &'a mut self,
            _task: NewTask,
            _dedup: NewDedupRecord,
        ) -> StorageTxFuture<'a, Result<TaskId, StorageError>> {
            unreachable!()
        }
        fn pick_and_lock_pending<'a>(
            &'a mut self,
            _criteria: PickCriteria,
        ) -> StorageTxFuture<'a, Result<Option<LockedTask>, StorageError>> {
            unreachable!()
        }
        fn record_acquisition<'a>(
            &'a mut self,
            _lease: NewLease,
        ) -> StorageTxFuture<'a, Result<(), StorageError>> {
            unreachable!()
        }
        fn subscribe_pending<'a>(
            &'a mut self,
            _ns: &'a Namespace,
            _types: &'a [TaskType],
        ) -> StorageTxFuture<'a, Result<WakeSignalStream, StorageError>> {
            unreachable!()
        }
        fn complete_task<'a>(
            &'a mut self,
            _lease: &'a LeaseRef,
            _outcome: TaskOutcome,
        ) -> StorageTxFuture<'a, Result<(), StorageError>> {
            unreachable!()
        }
        fn mark_worker_dead<'a>(
            &'a mut self,
            _worker_id: &'a WorkerId,
            _at: Timestamp,
        ) -> StorageTxFuture<'a, Result<(), StorageError>> {
            unreachable!()
        }
        fn check_capacity_quota<'a>(
            &'a mut self,
            _ns: &'a Namespace,
            _kind: CapacityKind,
        ) -> StorageTxFuture<'a, Result<CapacityDecision, StorageError>> {
            unreachable!()
        }
        fn oldest_pending_age_ms<'a>(
            &'a mut self,
            _ns: &'a Namespace,
        ) -> StorageTxFuture<'a, Result<Option<u64>, StorageError>> {
            unreachable!()
        }
        fn try_consume_rate_quota<'a>(
            &'a mut self,
            _ns: &'a Namespace,
            _kind: RateKind,
            _n: u64,
        ) -> StorageTxFuture<'a, Result<RateDecision, StorageError>> {
            unreachable!()
        }
        fn record_worker_heartbeat<'a>(
            &'a mut self,
            _worker_id: &'a WorkerId,
            _ns: &'a Namespace,
            _at: Timestamp,
        ) -> StorageTxFuture<'a, Result<HeartbeatAck, StorageError>> {
            unreachable!()
        }
        fn extend_lease_lazy<'a>(
            &'a mut self,
            _lease: &'a LeaseRef,
            _new_timeout: Timestamp,
            _last_extended_at: Timestamp,
        ) -> StorageTxFuture<'a, Result<(), StorageError>> {
            unreachable!()
        }
        fn get_namespace_quota<'a>(
            &'a mut self,
            _ns: &'a Namespace,
        ) -> StorageTxFuture<'a, Result<NamespaceQuota, StorageError>> {
            unreachable!()
        }
        fn audit_log_append<'a>(
            &'a mut self,
            _entry: AuditEntry,
        ) -> StorageTxFuture<'a, Result<(), StorageError>> {
            unreachable!()
        }
        fn commit_dyn<'a>(self: Box<Self>) -> StorageTxFuture<'a, Result<(), StorageError>>
        where
            Self: 'a,
        {
            unreachable!()
        }
        fn rollback_dyn<'a>(self: Box<Self>) -> StorageTxFuture<'a, Result<(), StorageError>>
        where
            Self: 'a,
        {
            unreachable!()
        }
    }

    // Keep the unused-import warning quiet for items pulled in only so the
    // stub's signatures parse. Without these, removing one would silently
    // cascade.
    #[allow(dead_code)]
    const _COMPILE_GUARD: Option<WakeSignal> = None;

    #[tokio::test]
    async fn cancel_internal_returns_unsupported_until_storage_method_lands() {
        // Arrange
        let mut tx = StubTx;
        let task_id = TaskId::generate();

        // Act
        let outcome = cancel_internal(&mut tx as &mut dyn StorageTxDyn, task_id)
            .await
            .expect("Phase 5b helper does not propagate storage errors");

        // Assert
        assert_eq!(outcome, CancelOutcome::Unsupported);
    }
}
