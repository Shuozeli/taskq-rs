//! `Dispatcher` strategy trait + three v1 implementations.
//!
//! `design.md` §7.2 / `problems/05`. Strategies translate an `AcquireTask`
//! call into a `PickCriteria` and call `pick_and_lock_pending` on the
//! storage transaction.
//!
//! ## Phase 5b-pre status
//!
//! All three impls (`PriorityFifo`, `AgePromoted`, `RandomNamespace`) are
//! real: they build a `PickCriteria` keyed off the `PullCtx` and the
//! strategy's stored params, then call into the storage shim. The
//! `AcquireTask` handler (Phase 5b) is responsible for the surrounding
//! transaction lifecycle and the long-poll loop; this trait only models
//! the picking decision.

use async_trait::async_trait;

use taskq_storage::{
    Namespace, NamespaceFilter, PickCriteria, PickOrdering, TaskId, TaskType, TaskTypeFilter,
    Timestamp,
};

use crate::state::StorageTxDyn;

/// Acquire-time context handed to every `Dispatcher::pick_next` call.
#[derive(Debug, Clone)]
pub struct PullCtx {
    /// Worker identity is opaque to the dispatcher; only the task-type filter
    /// matters for picking. The handler uses worker_id elsewhere (lease).
    pub task_types: Vec<TaskType>,
    /// Namespace filter -- typically a single namespace, but the
    /// `RandomNamespace` dispatcher samples across multiple.
    pub namespace_filter: Vec<Namespace>,
    pub now: Timestamp,
}

/// Picks the next task to dispatch. Implementations are stateless; whatever
/// they need lives in storage and is read inside the SERIALIZABLE
/// transaction owned by `AcquireTask`.
#[async_trait]
pub trait Dispatcher: Send + Sync + 'static {
    /// Pick the next pending task and return its id, or `None` if no
    /// candidate matches.
    ///
    /// Implementations MUST NOT commit/rollback the transaction; the
    /// handler owns the lifecycle.
    async fn pick_next(&self, ctx: &PullCtx, tx: &mut dyn StorageTxDyn) -> Option<TaskId>;

    /// Static name used by the strategy registry for runtime selection.
    fn name(&self) -> &'static str;
}

/// Translate `PullCtx.namespace_filter` into the storage trait's
/// `NamespaceFilter` enum. The CP layer represents the filter as a flat
/// `Vec<Namespace>`; storage prefers the discriminated form so backends can
/// pick the right SQL shape.
fn namespace_filter_for(ctx: &PullCtx) -> NamespaceFilter {
    match ctx.namespace_filter.len() {
        0 => NamespaceFilter::Any,
        1 => NamespaceFilter::Single(ctx.namespace_filter[0].clone()),
        _ => NamespaceFilter::AnyOf(ctx.namespace_filter.clone()),
    }
}

/// Build the `PickCriteria` shared by every dispatcher. Each impl supplies
/// only the `PickOrdering`; the rest of the criteria comes from `ctx`.
fn build_criteria(ctx: &PullCtx, ordering: PickOrdering) -> PickCriteria {
    PickCriteria {
        namespace_filter: namespace_filter_for(ctx),
        task_types_filter: TaskTypeFilter::AnyOf(ctx.task_types.clone()),
        ordering,
        now: ctx.now,
    }
}

// ---------------------------------------------------------------------------
// `PriorityFifo` -- ORDER BY priority DESC, submitted_at ASC.
// ---------------------------------------------------------------------------

/// Default dispatcher. Maps to `PickOrdering::PriorityFifo`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PriorityFifoDispatcher;

#[async_trait]
impl Dispatcher for PriorityFifoDispatcher {
    async fn pick_next(&self, ctx: &PullCtx, tx: &mut dyn StorageTxDyn) -> Option<TaskId> {
        let criteria = build_criteria(ctx, PickOrdering::PriorityFifo);
        match tx.pick_and_lock_pending(criteria).await {
            Ok(Some(locked)) => Some(locked.task_id),
            Ok(None) | Err(_) => None,
        }
    }

    fn name(&self) -> &'static str {
        "PriorityFifo"
    }
}

// ---------------------------------------------------------------------------
// `AgePromoted` -- effective priority = base + f(age).
// ---------------------------------------------------------------------------

/// `AgePromoted` dispatcher. Maps to `PickOrdering::AgePromoted { age_weight }`.
#[derive(Debug, Clone, Copy)]
pub struct AgePromotedDispatcher {
    /// Multiplier on task age contributing to effective priority. Read from
    /// `NamespaceQuota.dispatcher_params`.
    pub age_weight: f64,
}

#[async_trait]
impl Dispatcher for AgePromotedDispatcher {
    async fn pick_next(&self, ctx: &PullCtx, tx: &mut dyn StorageTxDyn) -> Option<TaskId> {
        let criteria = build_criteria(
            ctx,
            PickOrdering::AgePromoted {
                age_weight: self.age_weight,
            },
        );
        match tx.pick_and_lock_pending(criteria).await {
            Ok(Some(locked)) => Some(locked.task_id),
            Ok(None) | Err(_) => None,
        }
    }

    fn name(&self) -> &'static str {
        "AgePromoted"
    }
}

// ---------------------------------------------------------------------------
// `RandomNamespace` -- sample uniformly from active namespaces.
// ---------------------------------------------------------------------------

/// `RandomNamespace` dispatcher. Maps to
/// `PickOrdering::RandomNamespace { sample_attempts }`.
#[derive(Debug, Clone, Copy)]
pub struct RandomNamespaceDispatcher {
    /// How many candidate namespaces to sample before giving up. Read from
    /// `NamespaceQuota.dispatcher_params`.
    pub sample_attempts: u32,
}

#[async_trait]
impl Dispatcher for RandomNamespaceDispatcher {
    async fn pick_next(&self, ctx: &PullCtx, tx: &mut dyn StorageTxDyn) -> Option<TaskId> {
        let criteria = build_criteria(
            ctx,
            PickOrdering::RandomNamespace {
                sample_attempts: self.sample_attempts,
            },
        );
        match tx.pick_and_lock_pending(criteria).await {
            Ok(Some(locked)) => Some(locked.task_id),
            Ok(None) | Err(_) => None,
        }
    }

    fn name(&self) -> &'static str {
        "RandomNamespace"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use bytes::Bytes;
    use taskq_storage::{
        AuditEntry, CancelOutcome, CapacityDecision, CapacityKind, DeadWorkerRuntime, DedupRecord,
        ExpiredRuntime, HeartbeatAck, IdempotencyKey, LeaseRef, LockedTask, NamespaceQuota,
        NewDedupRecord, NewLease, NewTask, RateDecision, RateKind, RuntimeRef, StorageError, Task,
        TaskId, TaskOutcome, TaskStatus, WorkerId, WorkerInfo,
    };

    use crate::state::{StorageTxDyn, StorageTxFuture, WakeSignalStream};

    /// Test fake recording the last `PickCriteria` it saw and returning a
    /// scripted `LockedTask`. Mirrors the admitter-tests fake; the two
    /// modules don't share since they exercise different shim methods.
    #[derive(Default)]
    struct FakeTx {
        pick_response: Option<LockedTask>,
        last_criteria: Option<PickCriteria>,
    }

    impl FakeTx {
        fn with_pick(mut self, locked: LockedTask) -> Self {
            self.pick_response = Some(locked);
            self
        }
    }

    impl StorageTxDyn for FakeTx {
        fn check_capacity_quota<'a>(
            &'a mut self,
            _ns: &'a Namespace,
            _kind: CapacityKind,
        ) -> StorageTxFuture<'a, Result<CapacityDecision, StorageError>> {
            unreachable!("admitter methods not exercised in dispatcher tests")
        }

        fn oldest_pending_age_ms<'a>(
            &'a mut self,
            _ns: &'a Namespace,
        ) -> StorageTxFuture<'a, Result<Option<u64>, StorageError>> {
            unreachable!("admitter methods not exercised in dispatcher tests")
        }

        fn pick_and_lock_pending<'a>(
            &'a mut self,
            criteria: PickCriteria,
        ) -> StorageTxFuture<'a, Result<Option<LockedTask>, StorageError>> {
            self.last_criteria = Some(criteria);
            let response = self.pick_response.clone();
            Box::pin(async move { Ok(response) })
        }

        fn record_acquisition<'a>(
            &'a mut self,
            _lease: NewLease,
        ) -> StorageTxFuture<'a, Result<(), StorageError>> {
            Box::pin(async move { Ok(()) })
        }

        // --- methods not exercised by dispatcher strategies -------------

        fn lookup_idempotency<'a>(
            &'a mut self,
            _ns: &'a Namespace,
            _key: &'a IdempotencyKey,
        ) -> StorageTxFuture<'a, Result<Option<DedupRecord>, StorageError>> {
            unreachable!("submit-path methods not exercised in dispatcher tests")
        }

        fn insert_task<'a>(
            &'a mut self,
            _task: NewTask,
            _dedup: NewDedupRecord,
        ) -> StorageTxFuture<'a, Result<TaskId, StorageError>> {
            unreachable!("submit-path methods not exercised in dispatcher tests")
        }

        fn subscribe_pending<'a>(
            &'a mut self,
            _ns: &'a Namespace,
            _types: &'a [TaskType],
        ) -> StorageTxFuture<'a, Result<WakeSignalStream, StorageError>> {
            unreachable!("subscribe_pending not exercised in dispatcher tests")
        }

        fn complete_task<'a>(
            &'a mut self,
            _lease: &'a LeaseRef,
            _outcome: TaskOutcome,
        ) -> StorageTxFuture<'a, Result<(), StorageError>> {
            unreachable!("worker-path methods not exercised in dispatcher tests")
        }

        fn mark_worker_dead<'a>(
            &'a mut self,
            _worker_id: &'a WorkerId,
            _at: Timestamp,
        ) -> StorageTxFuture<'a, Result<(), StorageError>> {
            unreachable!("reaper-path methods not exercised in dispatcher tests")
        }

        fn try_consume_rate_quota<'a>(
            &'a mut self,
            _ns: &'a Namespace,
            _kind: RateKind,
            _n: u64,
        ) -> StorageTxFuture<'a, Result<RateDecision, StorageError>> {
            unreachable!("rate-quota methods not exercised in dispatcher tests")
        }

        fn record_worker_heartbeat<'a>(
            &'a mut self,
            _worker_id: &'a WorkerId,
            _ns: &'a Namespace,
            _at: Timestamp,
        ) -> StorageTxFuture<'a, Result<HeartbeatAck, StorageError>> {
            unreachable!("heartbeat methods not exercised in dispatcher tests")
        }

        fn extend_lease_lazy<'a>(
            &'a mut self,
            _lease: &'a LeaseRef,
            _new_timeout: Timestamp,
            _last_extended_at: Timestamp,
        ) -> StorageTxFuture<'a, Result<(), StorageError>> {
            unreachable!("heartbeat methods not exercised in dispatcher tests")
        }

        fn get_namespace_quota<'a>(
            &'a mut self,
            _ns: &'a Namespace,
        ) -> StorageTxFuture<'a, Result<NamespaceQuota, StorageError>> {
            unreachable!("admin methods not exercised in dispatcher tests")
        }

        fn audit_log_append<'a>(
            &'a mut self,
            _entry: AuditEntry,
        ) -> StorageTxFuture<'a, Result<(), StorageError>> {
            unreachable!("admin methods not exercised in dispatcher tests")
        }

        fn cancel_task<'a>(
            &'a mut self,
            _task_id: TaskId,
        ) -> StorageTxFuture<'a, Result<CancelOutcome, StorageError>> {
            unreachable!("Phase 5c admin methods not exercised in dispatcher tests")
        }

        fn get_task_by_id<'a>(
            &'a mut self,
            _task_id: TaskId,
        ) -> StorageTxFuture<'a, Result<Option<Task>, StorageError>> {
            unreachable!("Phase 5c admin methods not exercised in dispatcher tests")
        }

        fn list_expired_runtimes<'a>(
            &'a mut self,
            _before: Timestamp,
            _n: usize,
        ) -> StorageTxFuture<'a, Result<Vec<ExpiredRuntime>, StorageError>> {
            unreachable!("Phase 5c reaper methods not exercised in dispatcher tests")
        }

        fn reclaim_runtime<'a>(
            &'a mut self,
            _runtime: &'a RuntimeRef,
        ) -> StorageTxFuture<'a, Result<(), StorageError>> {
            unreachable!("Phase 5c reaper methods not exercised in dispatcher tests")
        }

        fn list_dead_worker_runtimes<'a>(
            &'a mut self,
            _stale_before: Timestamp,
            _n: usize,
        ) -> StorageTxFuture<'a, Result<Vec<DeadWorkerRuntime>, StorageError>> {
            unreachable!("Phase 5c reaper methods not exercised in dispatcher tests")
        }

        fn count_tasks_by_status<'a>(
            &'a mut self,
            _ns: &'a Namespace,
        ) -> StorageTxFuture<'a, Result<HashMap<TaskStatus, u64>, StorageError>> {
            unreachable!("Phase 5c admin methods not exercised in dispatcher tests")
        }

        fn list_workers<'a>(
            &'a mut self,
            _ns: &'a Namespace,
            _include_dead: bool,
        ) -> StorageTxFuture<'a, Result<Vec<WorkerInfo>, StorageError>> {
            unreachable!("Phase 5c admin methods not exercised in dispatcher tests")
        }

        fn enable_namespace<'a>(
            &'a mut self,
            _ns: &'a Namespace,
        ) -> StorageTxFuture<'a, Result<(), StorageError>> {
            unreachable!("Phase 5c admin methods not exercised in dispatcher tests")
        }

        fn disable_namespace<'a>(
            &'a mut self,
            _ns: &'a Namespace,
        ) -> StorageTxFuture<'a, Result<(), StorageError>> {
            unreachable!("Phase 5c admin methods not exercised in dispatcher tests")
        }

        fn commit_dyn<'a>(self: Box<Self>) -> StorageTxFuture<'a, Result<(), StorageError>>
        where
            Self: 'a,
        {
            unreachable!("lifecycle methods not exercised in dispatcher tests")
        }

        fn rollback_dyn<'a>(self: Box<Self>) -> StorageTxFuture<'a, Result<(), StorageError>>
        where
            Self: 'a,
        {
            unreachable!("lifecycle methods not exercised in dispatcher tests")
        }
    }

    fn pull_ctx_for(ns: &str, types: &[&str]) -> PullCtx {
        PullCtx {
            namespace_filter: vec![Namespace::new(ns)],
            task_types: types.iter().map(|t| TaskType::new(*t)).collect(),
            now: Timestamp::from_unix_millis(0),
        }
    }

    fn make_locked_task(task_id: TaskId, ns: &str) -> LockedTask {
        LockedTask {
            task_id,
            namespace: Namespace::new(ns),
            task_type: TaskType::new("dummy"),
            attempt_number: 0,
            priority: 0,
            payload: Bytes::new(),
            submitted_at: Timestamp::from_unix_millis(0),
            expires_at: Timestamp::from_unix_millis(1_000_000),
            max_retries: 0,
            retry_initial_ms: 0,
            retry_max_ms: 0,
            retry_coefficient: 1.0,
            traceparent: Bytes::new(),
            tracestate: Bytes::new(),
        }
    }

    #[tokio::test]
    async fn priority_fifo_returns_picked_task_id() {
        // Arrange
        let task_id = TaskId::generate();
        let mut tx = FakeTx::default().with_pick(make_locked_task(task_id, "test"));
        let ctx = pull_ctx_for("test", &["dummy"]);
        let dispatcher = PriorityFifoDispatcher;

        // Act
        let picked = dispatcher
            .pick_next(&ctx, &mut tx as &mut dyn StorageTxDyn)
            .await;

        // Assert
        assert_eq!(picked, Some(task_id));
        let criteria = tx.last_criteria.expect("criteria should be recorded");
        assert!(matches!(criteria.ordering, PickOrdering::PriorityFifo));
    }

    #[tokio::test]
    async fn age_promoted_passes_age_weight_to_storage() {
        // Arrange
        let mut tx = FakeTx::default();
        let ctx = pull_ctx_for("test", &["dummy"]);
        let dispatcher = AgePromotedDispatcher { age_weight: 2.5 };

        // Act
        let _ = dispatcher
            .pick_next(&ctx, &mut tx as &mut dyn StorageTxDyn)
            .await;

        // Assert
        let criteria = tx.last_criteria.expect("criteria should be recorded");
        match criteria.ordering {
            PickOrdering::AgePromoted { age_weight } => {
                assert!((age_weight - 2.5).abs() < f64::EPSILON);
            }
            other => panic!("expected AgePromoted ordering, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn random_namespace_passes_sample_attempts_to_storage() {
        // Arrange
        let mut tx = FakeTx::default();
        let ctx = pull_ctx_for("test", &["dummy"]);
        let dispatcher = RandomNamespaceDispatcher { sample_attempts: 7 };

        // Act
        let _ = dispatcher
            .pick_next(&ctx, &mut tx as &mut dyn StorageTxDyn)
            .await;

        // Assert
        let criteria = tx.last_criteria.expect("criteria should be recorded");
        match criteria.ordering {
            PickOrdering::RandomNamespace { sample_attempts } => {
                assert_eq!(sample_attempts, 7);
            }
            other => panic!("expected RandomNamespace ordering, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dispatcher_returns_none_when_no_candidates() {
        // Arrange
        let mut tx = FakeTx::default();
        let ctx = pull_ctx_for("test", &["dummy"]);
        let dispatcher = PriorityFifoDispatcher;

        // Act
        let picked = dispatcher
            .pick_next(&ctx, &mut tx as &mut dyn StorageTxDyn)
            .await;

        // Assert
        assert_eq!(picked, None);
    }

    #[test]
    fn dispatcher_names_are_unique() {
        // Arrange / Act / Assert
        assert_eq!(PriorityFifoDispatcher.name(), "PriorityFifo");
        assert_eq!(
            AgePromotedDispatcher { age_weight: 1.0 }.name(),
            "AgePromoted"
        );
        assert_eq!(
            RandomNamespaceDispatcher { sample_attempts: 4 }.name(),
            "RandomNamespace"
        );
    }
}
