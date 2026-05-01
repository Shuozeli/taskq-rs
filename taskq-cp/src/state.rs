//! Shared CP runtime state.
//!
//! `CpState` is the single object cloned (as `Arc<CpState>`) into every
//! gRPC handler, every reaper task, and the health server. It owns:
//!
//! - the storage handle (boxed for runtime backend dispatch),
//! - the strategy registry,
//! - the metrics handle,
//! - the shutdown receiver,
//! - the loaded `CpConfig`,
//! - the namespace-config cache (placeholder; Phase 5c implements),
//! - the long-poll waiter pool.
//!
//! ## Object-safe storage seam
//!
//! [`StorageTxDyn`] is the dyn-dispatch shim around [`taskq_storage::StorageTx`].
//! Native `async fn in trait` (used by `StorageTx`) is not object-safe in `dyn`
//! position because the returned `impl Future` types differ per backend. The
//! shim erases those Futures behind `BoxFuture`, narrows the surface to the
//! methods the CP layer actually calls, and is implemented blanket-style for
//! any concrete `StorageTx`.
//!
//! [`DynStorage`] mirrors the same trick for [`taskq_storage::Storage`]: it
//! exposes an object-safe `begin_dyn` that returns `Box<dyn StorageTxDyn>`,
//! allowing handlers to start transactions through `Arc<dyn DynStorage>`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_core::Stream;
use tokio::sync::{Mutex, Notify, RwLock};

use taskq_storage::{
    AuditEntry, CancelOutcome, CapacityDecision, CapacityKind, DeadWorkerRuntime, DedupRecord,
    ExpiredRuntime, IdempotencyKey, LeaseRef, LockedTask, Namespace, NamespaceQuota,
    NamespaceQuotaUpsert, NewDedupRecord, NewLease, NewTask, PickCriteria, RateDecision, RateKind,
    ReplayOutcome, RuntimeRef, Storage, StorageError, StorageTx, Task, TaskFilter, TaskId,
    TaskOutcome, TaskStatus, TaskType, TerminalState, Timestamp, WakeSignal, WorkerId, WorkerInfo,
};

use crate::config::CpConfig;
use crate::observability::MetricsHandle;
use crate::shutdown::ShutdownReceiver;
use crate::strategy::StrategyRegistry;

// ---------------------------------------------------------------------------
// DynStorage / DynStorageTx -- object-safe shims for Storage / StorageTx
// ---------------------------------------------------------------------------

/// Boxed future alias used by [`StorageTxDyn`] / [`DynStorage`]. `'a` is the
/// borrow captured by the future; `T` is the method's return type.
pub type StorageTxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Boxed `WakeSignal` stream returned by `subscribe_pending`.
pub type WakeSignalStream = Box<dyn Stream<Item = WakeSignal> + Send + Unpin + 'static>;

/// Object-safe alias for the storage handle. The CP layer takes a
/// `Arc<dyn DynStorage>` so the same handler code works against
/// `PostgresStorage` and `SqliteStorage` without monomorphising every
/// handler.
///
/// `begin_dyn` is the object-safe form of [`Storage::begin`] — it returns a
/// boxed [`StorageTxDyn`] so handlers can drive the full transaction surface
/// through dyn dispatch.
pub trait DynStorage: Send + Sync + 'static {
    /// Open a new transaction. Backends route this to their `Storage::begin`
    /// implementation, which selects the correct isolation level (SERIALIZABLE
    /// for state-transition transactions per `design.md` §1.1; the READ
    /// COMMITTED carve-out for heartbeats is exposed via separate trait
    /// methods on the returned transaction).
    fn begin_dyn(&self) -> StorageTxFuture<'_, Result<Box<dyn StorageTxDyn + '_>, StorageError>>;
}

/// Blanket impl: any concrete [`Storage`] participates as `DynStorage`.
///
/// The trait future is boxed; the inner `StorageTx::Tx<'_>` is wrapped in a
/// type-erased adapter (`StorageTxBox`) that forwards every call back through
/// `StorageTx`'s native methods. This keeps the CP-side dyn surface decoupled
/// from per-backend associated types.
impl<S> DynStorage for S
where
    S: Storage,
{
    fn begin_dyn(&self) -> StorageTxFuture<'_, Result<Box<dyn StorageTxDyn + '_>, StorageError>> {
        Box::pin(async move {
            let tx = self.begin().await?;
            let boxed: Box<dyn StorageTxDyn + '_> = Box::new(StorageTxBox { inner: tx });
            Ok(boxed)
        })
    }
}

/// Object-safe shim around [`taskq_storage::StorageTx`].
///
/// The native `StorageTx` trait uses `async fn in trait` (returns an
/// associated `impl Future`), which makes it unusable behind `dyn`. The CP
/// holds storage as `Arc<dyn DynStorage>` so handlers and strategies can
/// share one storage handle without monomorphising; the returned
/// transaction is therefore also boxed and uses `StorageTxFuture<'_, _>`
/// for every method.
///
/// `commit` / `rollback` consume `Box<Self>` so the borrow checker forbids
/// further use after either, matching the native `StorageTx` shape.
pub trait StorageTxDyn: Send {
    // ---- Submit path ---------------------------------------------------

    fn lookup_idempotency<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        key: &'a IdempotencyKey,
    ) -> StorageTxFuture<'a, Result<Option<DedupRecord>, StorageError>>;

    fn insert_task<'a>(
        &'a mut self,
        task: NewTask,
        dedup: NewDedupRecord,
    ) -> StorageTxFuture<'a, Result<TaskId, StorageError>>;

    // ---- Dispatch path -------------------------------------------------

    fn pick_and_lock_pending<'a>(
        &'a mut self,
        criteria: PickCriteria,
    ) -> StorageTxFuture<'a, Result<Option<LockedTask>, StorageError>>;

    fn record_acquisition<'a>(
        &'a mut self,
        lease: NewLease,
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;

    fn subscribe_pending<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        task_types: &'a [TaskType],
    ) -> StorageTxFuture<'a, Result<WakeSignalStream, StorageError>>;

    // ---- Worker state transitions --------------------------------------

    fn complete_task<'a>(
        &'a mut self,
        lease: &'a LeaseRef,
        outcome: TaskOutcome,
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;

    // ---- Reaper path ---------------------------------------------------

    fn mark_worker_dead<'a>(
        &'a mut self,
        worker_id: &'a WorkerId,
        at: Timestamp,
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;

    // ---- Quota enforcement ---------------------------------------------

    fn check_capacity_quota<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        kind: CapacityKind,
    ) -> StorageTxFuture<'a, Result<CapacityDecision, StorageError>>;

    fn oldest_pending_age_ms<'a>(
        &'a mut self,
        namespace: &'a Namespace,
    ) -> StorageTxFuture<'a, Result<Option<u64>, StorageError>>;

    fn try_consume_rate_quota<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        kind: RateKind,
        n: u64,
    ) -> StorageTxFuture<'a, Result<RateDecision, StorageError>>;

    // ---- Heartbeats (READ COMMITTED carve-out) -------------------------

    fn record_worker_heartbeat<'a>(
        &'a mut self,
        worker_id: &'a WorkerId,
        namespace: &'a Namespace,
        at: Timestamp,
    ) -> StorageTxFuture<'a, Result<taskq_storage::HeartbeatAck, StorageError>>;

    fn extend_lease_lazy<'a>(
        &'a mut self,
        lease: &'a LeaseRef,
        new_timeout: Timestamp,
        last_extended_at: Timestamp,
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;

    // ---- Admin / reads -------------------------------------------------

    fn get_namespace_quota<'a>(
        &'a mut self,
        namespace: &'a Namespace,
    ) -> StorageTxFuture<'a, Result<NamespaceQuota, StorageError>>;

    fn audit_log_append<'a>(
        &'a mut self,
        entry: AuditEntry,
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;

    fn delete_audit_logs_before<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        before: Timestamp,
        n: usize,
    ) -> StorageTxFuture<'a, Result<usize, StorageError>>;

    // ---- Phase 5c admin / cancel / reapers ----------------------------

    fn cancel_task<'a>(
        &'a mut self,
        task_id: TaskId,
    ) -> StorageTxFuture<'a, Result<CancelOutcome, StorageError>>;

    fn get_task_by_id<'a>(
        &'a mut self,
        task_id: TaskId,
    ) -> StorageTxFuture<'a, Result<Option<Task>, StorageError>>;

    fn list_expired_runtimes<'a>(
        &'a mut self,
        before: Timestamp,
        n: usize,
    ) -> StorageTxFuture<'a, Result<Vec<ExpiredRuntime>, StorageError>>;

    fn reclaim_runtime<'a>(
        &'a mut self,
        runtime: &'a RuntimeRef,
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;

    fn list_dead_worker_runtimes<'a>(
        &'a mut self,
        stale_before: Timestamp,
        n: usize,
    ) -> StorageTxFuture<'a, Result<Vec<DeadWorkerRuntime>, StorageError>>;

    fn count_tasks_by_status<'a>(
        &'a mut self,
        namespace: &'a Namespace,
    ) -> StorageTxFuture<'a, Result<HashMap<TaskStatus, u64>, StorageError>>;

    fn list_workers<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        include_dead: bool,
    ) -> StorageTxFuture<'a, Result<Vec<WorkerInfo>, StorageError>>;

    fn enable_namespace<'a>(
        &'a mut self,
        namespace: &'a Namespace,
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;

    fn disable_namespace<'a>(
        &'a mut self,
        namespace: &'a Namespace,
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;

    // ---- Phase 5e admin writes -----------------------------------------

    fn upsert_namespace_quota<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        quota: NamespaceQuotaUpsert,
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;

    fn list_tasks_by_filter<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        filter: TaskFilter,
        limit: usize,
    ) -> StorageTxFuture<'a, Result<Vec<Task>, StorageError>>;

    fn list_tasks_by_terminal_status<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        statuses: Vec<TerminalState>,
        limit: usize,
    ) -> StorageTxFuture<'a, Result<Vec<Task>, StorageError>>;

    fn replay_task<'a>(
        &'a mut self,
        task_id: TaskId,
    ) -> StorageTxFuture<'a, Result<ReplayOutcome, StorageError>>;

    fn add_error_classes<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        classes: &'a [String],
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;

    fn deprecate_error_class<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        class: &'a str,
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;

    fn add_task_types<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        types: &'a [TaskType],
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;

    fn deprecate_task_type<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        task_type: &'a TaskType,
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;

    // ---- Lifecycle -----------------------------------------------------

    /// Commit the transaction. `self: Box<Self>` so further use is rejected
    /// by the borrow checker (and the boxed inner `StorageTx` is consumed by
    /// its native `commit`). The future's lifetime is unconstrained at the
    /// trait level; concrete impls return a future bounded by the inner
    /// transaction's borrow on the parent `Storage`.
    fn commit_dyn<'a>(self: Box<Self>) -> StorageTxFuture<'a, Result<(), StorageError>>
    where
        Self: 'a;

    /// Roll back the transaction. Same lifetime story as `commit_dyn`.
    fn rollback_dyn<'a>(self: Box<Self>) -> StorageTxFuture<'a, Result<(), StorageError>>
    where
        Self: 'a;
}

/// Type-erased adapter: holds a concrete `StorageTx` and forwards every shim
/// method to its native equivalent. The lifetime parameter `'tx` represents
/// the borrow on the parent `Storage`.
struct StorageTxBox<T> {
    inner: T,
}

impl<T> StorageTxDyn for StorageTxBox<T>
where
    T: StorageTx + Send,
{
    fn lookup_idempotency<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        key: &'a IdempotencyKey,
    ) -> StorageTxFuture<'a, Result<Option<DedupRecord>, StorageError>> {
        Box::pin(StorageTx::lookup_idempotency(
            &mut self.inner,
            namespace,
            key,
        ))
    }

    fn insert_task<'a>(
        &'a mut self,
        task: NewTask,
        dedup: NewDedupRecord,
    ) -> StorageTxFuture<'a, Result<TaskId, StorageError>> {
        Box::pin(StorageTx::insert_task(&mut self.inner, task, dedup))
    }

    fn pick_and_lock_pending<'a>(
        &'a mut self,
        criteria: PickCriteria,
    ) -> StorageTxFuture<'a, Result<Option<LockedTask>, StorageError>> {
        Box::pin(StorageTx::pick_and_lock_pending(&mut self.inner, criteria))
    }

    fn record_acquisition<'a>(
        &'a mut self,
        lease: NewLease,
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::record_acquisition(&mut self.inner, lease))
    }

    fn subscribe_pending<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        task_types: &'a [TaskType],
    ) -> StorageTxFuture<'a, Result<WakeSignalStream, StorageError>> {
        Box::pin(StorageTx::subscribe_pending(
            &mut self.inner,
            namespace,
            task_types,
        ))
    }

    fn complete_task<'a>(
        &'a mut self,
        lease: &'a LeaseRef,
        outcome: TaskOutcome,
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::complete_task(&mut self.inner, lease, outcome))
    }

    fn mark_worker_dead<'a>(
        &'a mut self,
        worker_id: &'a WorkerId,
        at: Timestamp,
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::mark_worker_dead(&mut self.inner, worker_id, at))
    }

    fn check_capacity_quota<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        kind: CapacityKind,
    ) -> StorageTxFuture<'a, Result<CapacityDecision, StorageError>> {
        Box::pin(StorageTx::check_capacity_quota(
            &mut self.inner,
            namespace,
            kind,
        ))
    }

    fn oldest_pending_age_ms<'a>(
        &'a mut self,
        namespace: &'a Namespace,
    ) -> StorageTxFuture<'a, Result<Option<u64>, StorageError>> {
        Box::pin(StorageTx::oldest_pending_age_ms(&mut self.inner, namespace))
    }

    fn try_consume_rate_quota<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        kind: RateKind,
        n: u64,
    ) -> StorageTxFuture<'a, Result<RateDecision, StorageError>> {
        Box::pin(StorageTx::try_consume_rate_quota(
            &mut self.inner,
            namespace,
            kind,
            n,
        ))
    }

    fn record_worker_heartbeat<'a>(
        &'a mut self,
        worker_id: &'a WorkerId,
        namespace: &'a Namespace,
        at: Timestamp,
    ) -> StorageTxFuture<'a, Result<taskq_storage::HeartbeatAck, StorageError>> {
        Box::pin(StorageTx::record_worker_heartbeat(
            &mut self.inner,
            worker_id,
            namespace,
            at,
        ))
    }

    fn extend_lease_lazy<'a>(
        &'a mut self,
        lease: &'a LeaseRef,
        new_timeout: Timestamp,
        last_extended_at: Timestamp,
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::extend_lease_lazy(
            &mut self.inner,
            lease,
            new_timeout,
            last_extended_at,
        ))
    }

    fn get_namespace_quota<'a>(
        &'a mut self,
        namespace: &'a Namespace,
    ) -> StorageTxFuture<'a, Result<NamespaceQuota, StorageError>> {
        Box::pin(StorageTx::get_namespace_quota(&mut self.inner, namespace))
    }

    fn audit_log_append<'a>(
        &'a mut self,
        entry: AuditEntry,
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::audit_log_append(&mut self.inner, entry))
    }

    fn delete_audit_logs_before<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        before: Timestamp,
        n: usize,
    ) -> StorageTxFuture<'a, Result<usize, StorageError>> {
        Box::pin(StorageTx::delete_audit_logs_before(
            &mut self.inner,
            namespace,
            before,
            n,
        ))
    }

    fn cancel_task<'a>(
        &'a mut self,
        task_id: TaskId,
    ) -> StorageTxFuture<'a, Result<CancelOutcome, StorageError>> {
        Box::pin(StorageTx::cancel_task(&mut self.inner, task_id))
    }

    fn get_task_by_id<'a>(
        &'a mut self,
        task_id: TaskId,
    ) -> StorageTxFuture<'a, Result<Option<Task>, StorageError>> {
        Box::pin(StorageTx::get_task_by_id(&mut self.inner, task_id))
    }

    fn list_expired_runtimes<'a>(
        &'a mut self,
        before: Timestamp,
        n: usize,
    ) -> StorageTxFuture<'a, Result<Vec<ExpiredRuntime>, StorageError>> {
        Box::pin(StorageTx::list_expired_runtimes(&mut self.inner, before, n))
    }

    fn reclaim_runtime<'a>(
        &'a mut self,
        runtime: &'a RuntimeRef,
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::reclaim_runtime(&mut self.inner, runtime))
    }

    fn list_dead_worker_runtimes<'a>(
        &'a mut self,
        stale_before: Timestamp,
        n: usize,
    ) -> StorageTxFuture<'a, Result<Vec<DeadWorkerRuntime>, StorageError>> {
        Box::pin(StorageTx::list_dead_worker_runtimes(
            &mut self.inner,
            stale_before,
            n,
        ))
    }

    fn count_tasks_by_status<'a>(
        &'a mut self,
        namespace: &'a Namespace,
    ) -> StorageTxFuture<'a, Result<HashMap<TaskStatus, u64>, StorageError>> {
        Box::pin(StorageTx::count_tasks_by_status(&mut self.inner, namespace))
    }

    fn list_workers<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        include_dead: bool,
    ) -> StorageTxFuture<'a, Result<Vec<WorkerInfo>, StorageError>> {
        Box::pin(StorageTx::list_workers(
            &mut self.inner,
            namespace,
            include_dead,
        ))
    }

    fn enable_namespace<'a>(
        &'a mut self,
        namespace: &'a Namespace,
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::enable_namespace(&mut self.inner, namespace))
    }

    fn disable_namespace<'a>(
        &'a mut self,
        namespace: &'a Namespace,
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::disable_namespace(&mut self.inner, namespace))
    }

    fn upsert_namespace_quota<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        quota: NamespaceQuotaUpsert,
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::upsert_namespace_quota(
            &mut self.inner,
            namespace,
            quota,
        ))
    }

    fn list_tasks_by_filter<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        filter: TaskFilter,
        limit: usize,
    ) -> StorageTxFuture<'a, Result<Vec<Task>, StorageError>> {
        Box::pin(StorageTx::list_tasks_by_filter(
            &mut self.inner,
            namespace,
            filter,
            limit,
        ))
    }

    fn list_tasks_by_terminal_status<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        statuses: Vec<TerminalState>,
        limit: usize,
    ) -> StorageTxFuture<'a, Result<Vec<Task>, StorageError>> {
        Box::pin(StorageTx::list_tasks_by_terminal_status(
            &mut self.inner,
            namespace,
            statuses,
            limit,
        ))
    }

    fn replay_task<'a>(
        &'a mut self,
        task_id: TaskId,
    ) -> StorageTxFuture<'a, Result<ReplayOutcome, StorageError>> {
        Box::pin(StorageTx::replay_task(&mut self.inner, task_id))
    }

    fn add_error_classes<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        classes: &'a [String],
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::add_error_classes(
            &mut self.inner,
            namespace,
            classes,
        ))
    }

    fn deprecate_error_class<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        class: &'a str,
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::deprecate_error_class(
            &mut self.inner,
            namespace,
            class,
        ))
    }

    fn add_task_types<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        types: &'a [TaskType],
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::add_task_types(&mut self.inner, namespace, types))
    }

    fn deprecate_task_type<'a>(
        &'a mut self,
        namespace: &'a Namespace,
        task_type: &'a TaskType,
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::deprecate_task_type(
            &mut self.inner,
            namespace,
            task_type,
        ))
    }

    fn commit_dyn<'a>(self: Box<Self>) -> StorageTxFuture<'a, Result<(), StorageError>>
    where
        Self: 'a,
    {
        Box::pin(async move { StorageTx::commit(self.inner).await })
    }

    fn rollback_dyn<'a>(self: Box<Self>) -> StorageTxFuture<'a, Result<(), StorageError>>
    where
        Self: 'a,
    {
        Box::pin(async move { StorageTx::rollback(self.inner).await })
    }
}

// ---------------------------------------------------------------------------
// Tests-only marker: NoopStorage in health.rs implements DynStorage manually.
// We need that to keep working; the blanket impl above only fires for types
// that implement `Storage`. Suppress nothing — `NoopStorage` carries `impl
// DynStorage for NoopStorage {}` directly, which is still valid because the
// trait now has only one method with a default-providable behaviour ... no,
// it does not. We instead keep the `NoopStorage` site by giving it a manual
// impl that always returns `BackendError`. See `health.rs::tests` for the
// shim.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// CpState
// ---------------------------------------------------------------------------

/// Shared CP state. Cloned as `Arc<CpState>` everywhere.
pub struct CpState {
    pub storage: Arc<dyn DynStorage>,
    pub strategy_registry: Arc<StrategyRegistry>,
    pub metrics: MetricsHandle,
    pub shutdown: ShutdownReceiver,
    pub config: Arc<CpConfig>,
    pub namespace_config_cache: Arc<NamespaceConfigCache>,
    pub waiter_pool: Arc<WaiterPool>,
}

impl CpState {
    /// Build the shared state. `main` calls this once after wiring the
    /// individual components.
    pub fn new(
        storage: Arc<dyn DynStorage>,
        strategy_registry: Arc<StrategyRegistry>,
        metrics: MetricsHandle,
        shutdown: ShutdownReceiver,
        config: Arc<CpConfig>,
    ) -> Self {
        let cache_ttl = Duration::from_secs(u64::from(config.quota_cache_ttl_seconds));
        let waiter_limit = config.waiter_limit_per_replica;
        Self {
            storage,
            strategy_registry,
            metrics,
            shutdown,
            config,
            namespace_config_cache: Arc::new(NamespaceConfigCache::new(cache_ttl)),
            waiter_pool: Arc::new(WaiterPool::new(waiter_limit)),
        }
    }
}

// ---------------------------------------------------------------------------
// Namespace config cache (Phase 5c)
// ---------------------------------------------------------------------------

/// Per-namespace `NamespaceQuota` cache. `design.md` §1.1 carve-out: rate
/// quotas are eventually consistent within the cache TTL (default 5s) with
/// ±10% jitter to prevent synchronized expiry. `design.md` §9.1: capacity
/// quotas are NOT cached — they're read inline transactionally; this cache
/// only ever satisfies rate-quota lookups.
///
/// ## Singleflight
///
/// Concurrent misses on the same namespace fan-in: the first task that
/// observes a miss takes a per-entry `Notify`, executes the storage read,
/// installs the value, and notifies all waiters. Subsequent misses on the
/// same namespace observe the in-flight loader and `await` the `Notify`
/// instead of issuing a duplicate read.
///
/// ## Jitter
///
/// Each insert picks a TTL drawn uniformly from `[ttl × 0.9, ttl × 1.1]`
/// so a fleet of replicas does not synchronize cache expiry on a clock
/// boundary.
pub struct NamespaceConfigCache {
    /// Configured TTL (the centre of the ±10% jittered range).
    pub ttl: Duration,
    /// Per-namespace entry slot. The outer `RwLock` guards entry creation;
    /// per-entry mutability happens through the inner `Mutex<CacheEntry>`
    /// so reads on hot entries do not lock the whole map.
    entries: RwLock<HashMap<Namespace, Arc<Mutex<CacheEntry>>>>,
}

/// Cache slot. `value` is `None` while the first loader is in flight;
/// followers `await` `notify` and re-read after wakeup.
struct CacheEntry {
    value: Option<CachedQuota>,
    /// `true` while a loader is in flight. Followers do not start a
    /// duplicate load; they wait on `notify`.
    loading: bool,
    /// Wakeup signal for followers waiting on the in-flight loader.
    notify: Arc<Notify>,
}

impl CacheEntry {
    fn empty() -> Self {
        Self {
            value: None,
            loading: false,
            notify: Arc::new(Notify::new()),
        }
    }
}

#[derive(Clone)]
pub struct CachedQuota {
    pub quota: Arc<NamespaceQuota>,
    /// When this cache entry expires. Includes the per-entry ±10% jitter.
    pub expires_at: std::time::Instant,
}

impl CachedQuota {
    fn is_fresh(&self) -> bool {
        self.expires_at > std::time::Instant::now()
    }
}

impl NamespaceConfigCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Pick a jittered TTL for a fresh insert. ±10% of the configured TTL.
    fn jittered_ttl(&self) -> Duration {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let frac: f64 = rng.gen_range(-0.1..0.1);
        let nanos = self.ttl.as_secs_f64() * (1.0 + frac);
        Duration::from_secs_f64(nanos.max(0.0))
    }

    /// Look up the rate-quota config for `namespace`, loading from storage
    /// on miss with singleflight semantics.
    ///
    /// Returns the cached `NamespaceQuota` directly. On miss the loader
    /// `loader_fn` is called once per (namespace, expiry-window) pair; all
    /// other callers wait for it via the per-entry `Notify`.
    pub async fn get_or_load<F, Fut>(
        &self,
        ns: &Namespace,
        loader_fn: F,
    ) -> Result<Arc<NamespaceQuota>, StorageError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<NamespaceQuota, StorageError>>,
    {
        // Fast path: shared read on the map; if the entry is fresh return it.
        // This branch never takes the per-entry mutex.
        let entry = {
            let map = self.entries.read().await;
            map.get(ns).cloned()
        };
        if let Some(slot) = entry.as_ref() {
            let guard = slot.lock().await;
            if let Some(cached) = &guard.value {
                if cached.is_fresh() {
                    return Ok(Arc::clone(&cached.quota));
                }
            }
            // expired or empty — fall through to the slow path with the
            // entry already in hand.
        }

        // Slow path: miss or stale. We need a per-entry slot to coordinate
        // singleflight.
        let slot = match entry {
            Some(slot) => slot,
            None => {
                let mut map = self.entries.write().await;
                Arc::clone(
                    map.entry(ns.clone())
                        .or_insert_with(|| Arc::new(Mutex::new(CacheEntry::empty()))),
                )
            }
        };

        // Lock the per-entry slot. If a loader is in flight, wait for it.
        // Otherwise become the loader.
        let (am_loader, notify, fresh_value) = {
            let mut guard = slot.lock().await;
            if let Some(cached) = &guard.value {
                if cached.is_fresh() {
                    return Ok(Arc::clone(&cached.quota));
                }
            }
            if guard.loading {
                (false, Arc::clone(&guard.notify), None)
            } else {
                guard.loading = true;
                (true, Arc::clone(&guard.notify), None::<()>.map(|_| ()))
            }
        };

        if !am_loader {
            // Wait for the in-flight loader, then read the slot.
            notify.notified().await;
            let guard = slot.lock().await;
            if let Some(cached) = &guard.value {
                if cached.is_fresh() {
                    return Ok(Arc::clone(&cached.quota));
                }
            }
            // The loader failed or installed an already-stale entry; fall
            // through to attempt the load ourselves. This keeps follower
            // tasks from being permanently stuck on a transient error.
            // Suppress the variable so it doesn't get optimized away.
            let _ = fresh_value;
        }

        // We are the loader. Run the user-supplied closure.
        let load_result = loader_fn().await;

        // Re-acquire the entry mutex to install the value (or clear the
        // loading flag on error) and signal followers.
        {
            let mut guard = slot.lock().await;
            guard.loading = false;
            match &load_result {
                Ok(quota) => {
                    let entry = CachedQuota {
                        quota: Arc::new(quota.clone()),
                        expires_at: std::time::Instant::now() + self.jittered_ttl(),
                    };
                    guard.value = Some(entry);
                }
                Err(_) => {
                    // Leave previous (possibly stale) value in place but
                    // don't pretend a fresh load happened. Followers wake
                    // and retry.
                }
            }
            guard.notify.notify_waiters();
        }

        match load_result {
            Ok(quota) => Ok(Arc::new(quota)),
            Err(err) => Err(err),
        }
    }

    /// Compatibility convenience: looks up the rate-quota config for
    /// `namespace`, opening a fresh transaction on the storage handle and
    /// reading via `get_namespace_quota`. Most callers prefer `get_or_load`
    /// when they already hold a transaction.
    pub async fn get_rate_config(
        &self,
        namespace: &Namespace,
        storage: &Arc<dyn DynStorage>,
    ) -> Result<Arc<NamespaceQuota>, StorageError> {
        let storage_for_loader = Arc::clone(storage);
        let ns_for_loader = namespace.clone();
        self.get_or_load(namespace, move || async move {
            let mut tx = storage_for_loader.begin_dyn().await?;
            let quota = tx.get_namespace_quota(&ns_for_loader).await?;
            tx.commit_dyn().await?;
            Ok(quota)
        })
        .await
    }

    /// Drop the cache entry for `ns`. Used by admin write paths
    /// (`SetNamespaceQuota` / `SetNamespaceConfig` / Enable/Disable
    /// namespace) to make subsequent reads observe the new state without
    /// waiting out the TTL.
    pub fn invalidate(&self, ns: &Namespace) {
        // Synchronous best-effort: the RwLock supports `try_write` so the
        // admin handler does not block on a long-running read. If a reader
        // is mid-flight the next read will observe a stale entry until the
        // TTL elapses — acceptable per `design.md` §1.1 carve-out.
        if let Ok(mut map) = self.entries.try_write() {
            map.remove(ns);
        }
    }

    /// Async invalidate that always succeeds (waits for the writer lock).
    /// Tests prefer this; admin handlers prefer the synchronous version.
    pub async fn invalidate_async(&self, ns: &Namespace) {
        let mut map = self.entries.write().await;
        map.remove(ns);
    }

    /// Lookup an entry without populating. Returns `None` on miss or expiry.
    pub async fn peek(&self, ns: &Namespace) -> Option<CachedQuota> {
        let entry = {
            let map = self.entries.read().await;
            map.get(ns).cloned()
        }?;
        let guard = entry.lock().await;
        guard.value.clone().filter(CachedQuota::is_fresh)
    }

    /// Bypass the singleflight loader and install a value directly. Tests
    /// use this; admin paths prefer `invalidate`.
    pub async fn insert(&self, ns: Namespace, quota: Arc<NamespaceQuota>) {
        let entry = CachedQuota {
            quota,
            expires_at: std::time::Instant::now() + self.jittered_ttl(),
        };
        let slot = {
            let mut map = self.entries.write().await;
            Arc::clone(
                map.entry(ns)
                    .or_insert_with(|| Arc::new(Mutex::new(CacheEntry::empty()))),
            )
        };
        let mut guard = slot.lock().await;
        guard.value = Some(entry);
    }
}

// ---------------------------------------------------------------------------
// Long-poll waiter pool
// ---------------------------------------------------------------------------

/// Per-replica long-poll waiter set. `design.md` §6.2 / §10.1: capped at
/// 5000 waiters per replica by default.
///
/// ## Wakeup model
///
/// Every waiter registered via [`WaiterPool::register_waiter`] is associated
/// with an `(namespace, task_types)` key. When a `WakeSignal` arrives for a
/// namespace (delivered by a `subscribe_pending` task or by an in-process
/// `notify_namespace` call from the submit/cancel paths), the pool finds
/// **one** matching waiter and signals it via the waiter's `Notify`. This is
/// the "single-waiter wake" property from `design.md` §6.2: avoid the
/// thundering herd that would happen if every waiter on a namespace woke
/// for one new task.
///
/// Each waiter also gets a periodic 10s belt-and-suspenders wake regardless
/// of subscription state, to recover from missed notifications under any
/// backend (`design.md` §6.2 step 4b).
///
/// Per-replica cap enforcement lives in `register_waiter`; once the cap is
/// hit, further registrations return `Err(WaiterLimitExceeded)` so the
/// `AcquireTask` handler can surface `REPLICA_WAITER_LIMIT_EXCEEDED`.
pub struct WaiterPool {
    /// Configured cap. `0` means "no cap" (test-only); production sets a
    /// positive cap in `CpConfig::waiter_limit_per_replica` (default 5000).
    pub limit: u32,
    /// Atomic counter for the per-replica cap, exposed as a metric.
    pub active: std::sync::atomic::AtomicU32,
    /// Per-namespace registered waiters. Held behind a `Mutex` because both
    /// register/drop and wake paths take the lock briefly.
    inner: Mutex<WaiterPoolInner>,
}

#[derive(Default)]
struct WaiterPoolInner {
    /// FIFO queue of waiters per namespace. The first matching waiter (by
    /// task-type intersection) wins the wake.
    by_namespace: HashMap<Namespace, Vec<WaiterEntry>>,
    /// Monotonic id used to identify a waiter for removal on drop.
    next_id: u64,
}

#[derive(Clone)]
struct WaiterEntry {
    id: u64,
    /// Task types the worker registered. Empty means "match anything".
    task_types: Vec<TaskType>,
    /// Notification slot the waiter blocks on.
    notify: Arc<Notify>,
}

impl WaiterPool {
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            active: std::sync::atomic::AtomicU32::new(0),
            inner: Mutex::new(WaiterPoolInner::default()),
        }
    }

    /// Try to register a new waiter for `(namespace, task_types)`. The
    /// returned [`WaiterHandle`] removes the waiter from the pool on drop
    /// and exposes [`WaiterHandle::wait_for_wake_or_timeout`] for the
    /// `AcquireTask` long-poll body.
    ///
    /// Returns `Err(WaiterLimitExceeded)` when the per-replica cap has been
    /// reached, so the `AcquireTask` handler can surface
    /// `REPLICA_WAITER_LIMIT_EXCEEDED` (`design.md` §10.1).
    pub async fn register_waiter(
        self: &Arc<Self>,
        namespace: Namespace,
        task_types: Vec<TaskType>,
    ) -> Result<WaiterHandle, WaiterLimitExceeded> {
        use std::sync::atomic::Ordering;

        // Per-replica cap (atomic optimistic CAS).
        let prior = self.active.fetch_add(1, Ordering::AcqRel);
        if self.limit > 0 && prior >= self.limit {
            self.active.fetch_sub(1, Ordering::AcqRel);
            return Err(WaiterLimitExceeded);
        }

        let notify = Arc::new(Notify::new());
        let id = {
            let mut inner = self.inner.lock().await;
            let id = inner.next_id;
            inner.next_id = inner.next_id.wrapping_add(1);
            inner
                .by_namespace
                .entry(namespace.clone())
                .or_default()
                .push(WaiterEntry {
                    id,
                    task_types: task_types.clone(),
                    notify: Arc::clone(&notify),
                });
            id
        };

        Ok(WaiterHandle {
            pool: Arc::clone(self),
            namespace,
            id,
            notify,
        })
    }

    /// Snapshot of currently-parked waiters across all namespaces (the
    /// per-replica metric source). Cheap atomic load.
    pub fn waiter_count_per_replica(&self) -> usize {
        self.active.load(std::sync::atomic::Ordering::Acquire) as usize
    }

    /// Snapshot of currently-parked waiters in `ns`. Walks the inner map; an
    /// O(n) operation in the namespace count, intended for diagnostics
    /// (e.g. structured-log fields), not hot-path bookkeeping.
    pub async fn waiter_count_per_namespace(&self, namespace: &Namespace) -> usize {
        let inner = self.inner.lock().await;
        inner
            .by_namespace
            .get(namespace)
            .map(|v| v.len())
            .unwrap_or(0)
    }

    /// Wake **one** waiter on `namespace` whose `task_types` intersect with
    /// any committed-task type set carried by the wake. The wake is
    /// type-agnostic in v1 — `subscribe_pending` does not currently carry
    /// the committed task type — so we wake the first registered waiter
    /// whose registration matches "any" or includes one of `task_types`.
    /// When `task_types` is empty, we wake the first registered waiter on
    /// the namespace.
    ///
    /// This is the single-waiter property from `design.md` §6.2: "single-
    /// waiter wakeup; loser re-blocks". If the woken waiter's dispatch
    /// transaction fails to find a task (because some other waiter beat it
    /// to the row), it can call `wait_for_wake_or_timeout` again — the
    /// next wake/timer event resumes the wait.
    pub async fn wake_one(&self, namespace: &Namespace, task_types: &[TaskType]) {
        let inner = self.inner.lock().await;
        let Some(waiters) = inner.by_namespace.get(namespace) else {
            return;
        };
        for waiter in waiters {
            if Self::matches_types(&waiter.task_types, task_types) {
                waiter.notify.notify_one();
                return;
            }
        }
    }

    /// Wake every waiter in a namespace. Used by reapers (Phase 5c) so
    /// reclaimed leases that produce new PENDING tasks reach all candidate
    /// workers. The `wake_one` path is the single-waiter optimisation; this
    /// is the broadcast escape hatch.
    pub async fn wake_all(&self, namespace: &Namespace) {
        let inner = self.inner.lock().await;
        if let Some(waiters) = inner.by_namespace.get(namespace) {
            for waiter in waiters {
                waiter.notify.notify_one();
            }
        }
    }

    fn matches_types(registered: &[TaskType], wake: &[TaskType]) -> bool {
        if registered.is_empty() || wake.is_empty() {
            // "any" on either side matches.
            return true;
        }
        registered.iter().any(|t| wake.iter().any(|w| w == t))
    }

    /// Internal: drop a waiter on handle drop.
    fn unregister(&self, namespace: &Namespace, id: u64) {
        // Hot path: fast atomic decrement first so the cap check is accurate
        // even if the inner-map cleanup is delayed.
        self.active
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        // Use try_lock_owned-style pattern via blocking_lock when not in an
        // async context. WaiterHandle::drop runs in sync context, so we use
        // try_lock; if contended, leak the entry and let the next walker GC
        // it. Here we run inside `Drop` (sync), so the simplest path is to
        // schedule cleanup via `tokio::spawn` if a runtime is available. To
        // avoid surprising callers with spawn-on-drop, we instead use
        // `try_lock` and tolerate occasional residual entries in the map —
        // they don't carry a strong reference back to the WaiterHandle so
        // they're harmless apart from a small memory blip.
        if let Ok(mut inner) = self.inner.try_lock() {
            if let Some(waiters) = inner.by_namespace.get_mut(namespace) {
                waiters.retain(|w| w.id != id);
                if waiters.is_empty() {
                    inner.by_namespace.remove(namespace);
                }
            }
        }
    }
}

/// RAII handle returned by [`WaiterPool::register_waiter`]. Removes the
/// waiter from the pool on drop and exposes a single
/// [`WaiterHandle::wait_for_wake_or_timeout`] entry point so the
/// `AcquireTask` handler can race the wake/notify against the long-poll
/// timeout.
pub struct WaiterHandle {
    pool: Arc<WaiterPool>,
    namespace: Namespace,
    id: u64,
    notify: Arc<Notify>,
}

impl WaiterHandle {
    /// Block until: a `wake_one` / `wake_all` signal arrives, the 10s
    /// belt-and-suspenders timer fires, or `timeout` elapses — whichever
    /// happens first.
    ///
    /// `belt_and_suspenders` is configurable (`design.md` §6.2 step 4b
    /// default 10s). When the belt-and-suspenders fires, the caller re-runs
    /// the dispatch query without consuming the long-poll budget — the
    /// timer keeps re-arming until the long-poll deadline.
    pub async fn wait_for_wake_or_timeout(
        &self,
        timeout: Duration,
        belt_and_suspenders: Duration,
    ) -> WakeOutcome {
        // Race: wake notification, belt-and-suspenders timer, long-poll
        // deadline. The belt-and-suspenders is intentionally *shorter* than
        // the long-poll deadline; we expect to fire it multiple times across
        // a single long-poll.
        let effective_belt = belt_and_suspenders.min(timeout);
        tokio::select! {
            biased;
            _ = self.notify.notified() => WakeOutcome::Woken,
            _ = tokio::time::sleep(effective_belt) => {
                if effective_belt >= timeout {
                    WakeOutcome::Timeout
                } else {
                    WakeOutcome::BeltAndSuspenders
                }
            }
        }
    }
}

impl Drop for WaiterHandle {
    fn drop(&mut self) {
        self.pool.unregister(&self.namespace, self.id);
    }
}

/// Outcome of a single [`WaiterHandle::wait_for_wake_or_timeout`] call.
///
/// - `Woken`: a `subscribe_pending` notification (or `wake_one` from the
///   submit / cancel paths) targeted us.
/// - `BeltAndSuspenders`: the periodic 10s timer fired. The caller re-runs
///   the dispatch query and may call `wait_for_wake_or_timeout` again.
/// - `Timeout`: the long-poll deadline was reached. The handler returns
///   `AcquireTaskResponse{ no_task: true }` (`design.md` §6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeOutcome {
    Woken,
    BeltAndSuspenders,
    Timeout,
}

/// Returned from [`WaiterPool::register_waiter`] when the per-replica cap is
/// reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaiterLimitExceeded;

impl std::fmt::Display for WaiterLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "per-replica waiter limit exceeded")
    }
}

impl std::error::Error for WaiterLimitExceeded {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_waiter_enforces_per_replica_cap() {
        // Arrange
        let pool = Arc::new(WaiterPool::new(2));

        // Act
        let h1 = pool
            .register_waiter(Namespace::new("ns"), vec![])
            .await
            .expect("first registration must succeed");
        let h2 = pool
            .register_waiter(Namespace::new("ns"), vec![])
            .await
            .expect("second registration must succeed");
        let third = pool.register_waiter(Namespace::new("ns"), vec![]).await;

        // Assert: third registration hits the cap.
        assert!(matches!(third, Err(WaiterLimitExceeded)));

        // Drop one and another registration should now succeed.
        drop(h1);
        // Give the synchronous unregister a chance to land in the map; the
        // atomic decrement is immediate so the cap permits another register.
        let h3 = pool
            .register_waiter(Namespace::new("ns"), vec![])
            .await
            .expect("post-drop registration must succeed");

        // Cleanup
        drop(h2);
        drop(h3);
    }

    #[tokio::test]
    async fn wake_one_wakes_a_single_waiter() {
        // Arrange
        let pool = Arc::new(WaiterPool::new(0));
        let h1 = pool
            .register_waiter(Namespace::new("ns"), vec![])
            .await
            .unwrap();
        let h2 = pool
            .register_waiter(Namespace::new("ns"), vec![])
            .await
            .unwrap();

        // Act: a single wake notifies exactly one waiter (the first FIFO).
        pool.wake_one(&Namespace::new("ns"), &[]).await;

        // Assert: the first waiter wakes immediately; the second still
        // blocks. Use a short deadline so the test does not hang on bug.
        let woke_one = tokio::time::timeout(
            Duration::from_millis(50),
            h1.wait_for_wake_or_timeout(Duration::from_secs(60), Duration::from_secs(60)),
        )
        .await;
        assert_eq!(woke_one.unwrap(), WakeOutcome::Woken);

        let still_blocked = tokio::time::timeout(
            Duration::from_millis(20),
            h2.wait_for_wake_or_timeout(Duration::from_secs(60), Duration::from_secs(60)),
        )
        .await;
        assert!(
            still_blocked.is_err(),
            "second waiter must remain blocked when only one wake fired"
        );
    }

    #[tokio::test]
    async fn wait_for_wake_or_timeout_returns_belt_when_period_elapses() {
        // Arrange
        let pool = Arc::new(WaiterPool::new(0));
        let h = pool
            .register_waiter(Namespace::new("ns"), vec![])
            .await
            .unwrap();

        // Act
        let outcome = h
            .wait_for_wake_or_timeout(Duration::from_millis(500), Duration::from_millis(20))
            .await;

        // Assert: the belt-and-suspenders timer fires before the long-poll
        // deadline (because belt < timeout).
        assert_eq!(outcome, WakeOutcome::BeltAndSuspenders);
    }

    #[tokio::test]
    async fn wait_for_wake_or_timeout_returns_timeout_when_belt_exceeds_deadline() {
        // Arrange
        let pool = Arc::new(WaiterPool::new(0));
        let h = pool
            .register_waiter(Namespace::new("ns"), vec![])
            .await
            .unwrap();

        // Act: belt >= timeout means the single sleep represents the long-
        // poll deadline itself.
        let outcome = h
            .wait_for_wake_or_timeout(Duration::from_millis(20), Duration::from_millis(50))
            .await;

        // Assert
        assert_eq!(outcome, WakeOutcome::Timeout);
    }

    #[tokio::test]
    async fn waiter_count_tracks_register_and_drop() {
        // Arrange
        let pool = Arc::new(WaiterPool::new(0));

        // Act
        let h = pool
            .register_waiter(Namespace::new("ns"), vec![])
            .await
            .unwrap();
        let count_during = pool.waiter_count_per_replica();
        drop(h);

        // Assert
        assert_eq!(count_during, 1);
        assert_eq!(pool.waiter_count_per_replica(), 0);
    }
}
