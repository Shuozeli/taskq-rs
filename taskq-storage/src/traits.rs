//! `Storage` (connection lifecycle) and `StorageTx` (in-transaction ops).
//!
//! The CP layer interacts with persistence exclusively through these traits.
//! Backends translate every method to native idiom; the CP layer never sees
//! backend-specific types (`design.md` §8).
//!
//! Native `async fn in trait` is used (stable since Rust 1.75) — there is no
//! `async-trait` macro. `Send` is required on `StorageTx` so transactions can
//! cross `tokio` task boundaries; the bound is added explicitly because
//! native-async-fn return-position futures are not `Send` by default.
//!
//! ## Conformance contract
//!
//! Backends conforming to all six requirements from `design.md` §8.2 pass the
//! shared `taskq-storage-conformance` crate. The list:
//!
//! 1. External consistency (strict serializability for state transitions).
//! 2. Non-blocking row locking with skip semantics.
//! 3. Indexed range scans with predicates.
//! 4. Atomic conditional insert.
//! 5. Subscribe-pending ordering invariant.
//! 6. Bounded-cost dedup expiration.

use futures_core::Stream;

use crate::error::Result;
use crate::ids::{IdempotencyKey, Namespace, TaskId, TaskType, Timestamp, WorkerId};
use crate::types::{
    AuditEntry, CapacityDecision, CapacityKind, DedupRecord, ExpiredRuntime, HeartbeatAck,
    LeaseRef, LockedTask, NamespaceQuota, NewDedupRecord, NewLease, NewTask, PickCriteria,
    RateDecision, RateKind, RuntimeRef, TaskOutcome, WakeSignal,
};

/// Connection-lifecycle trait. A `Storage` factory hands out `StorageTx`
/// transactions; everything else lives on the transaction.
///
/// `Send + Sync + 'static` so the CP can hold a single shared instance behind
/// an `Arc` and clone handles into worker tasks.
pub trait Storage: Send + Sync + 'static {
    /// In-transaction handle type. Borrowed from `&self` so connection
    /// pooling is the backend's concern.
    type Tx<'a>: StorageTx + 'a
    where
        Self: 'a;

    /// Open a new transaction. Backends are responsible for choosing the
    /// correct isolation level for the *default* path — SERIALIZABLE for
    /// state transitions per `design.md` §1.1. The READ COMMITTED carve-out
    /// (heartbeats) is exposed via separate trait methods so the backend
    /// can route to a cheaper transaction internally.
    fn begin(&self) -> impl std::future::Future<Output = Result<Self::Tx<'_>>> + Send;
}

/// Operations within a single storage transaction.
///
/// All methods are `async fn`. The trait is `Send` so the transaction
/// future can cross task boundaries. Most operations participate in
/// external consistency; the heartbeat carve-outs (`record_worker_heartbeat`,
/// `extend_lease_lazy`) are explicitly READ COMMITTED per `design.md` §1.1.
///
/// `commit` and `rollback` consume the transaction (`self`) so they cannot
/// be called twice and the borrow checker forbids further use after either.
pub trait StorageTx: Send {
    // ========================================================================
    // Submit path
    // ========================================================================

    /// Used by §6.1 SubmitTask step 2: look up `idempotency_keys[(ns, key)]`.
    ///
    /// Returns `Some(record)` when a row exists (caller compares
    /// `payload_hash` and decides hit vs. mismatch); `None` when no row
    /// exists or the row is past `expires_at` (lazy cleanup).
    ///
    /// SERIALIZABLE: yes (participates in external consistency).
    fn lookup_idempotency(
        &mut self,
        namespace: &Namespace,
        key: &IdempotencyKey,
    ) -> impl std::future::Future<Output = Result<Option<DedupRecord>>> + Send;

    /// Used by §6.1 SubmitTask steps 4-5: insert the new task row plus the
    /// idempotency-key row inside the same transaction.
    ///
    /// Backends MUST perform this as one atomic conditional insert (§8.2 #4):
    /// if the dedup row already exists at commit time, the call returns
    /// `StorageError::ConstraintViolation` and the caller surfaces
    /// `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD` to the user.
    ///
    /// SERIALIZABLE: yes.
    fn insert_task(
        &mut self,
        task: NewTask,
        dedup: NewDedupRecord,
    ) -> impl std::future::Future<Output = Result<TaskId>> + Send;

    // ========================================================================
    // Dispatch path
    // ========================================================================

    /// Used by §6.2 AcquireTask step 2: pick a single PENDING task matching
    /// the strategy's `PickCriteria` and lock it for this transaction.
    ///
    /// Backends supporting concurrent writers MUST satisfy §8.2 #2 — locked
    /// rows are skipped, not blocked on (Postgres `FOR UPDATE SKIP LOCKED`,
    /// FoundationDB conflict-range retry, etc.). Single-writer backends
    /// (SQLite) satisfy the requirement vacuously.
    ///
    /// SERIALIZABLE: yes.
    fn pick_and_lock_pending(
        &mut self,
        criteria: PickCriteria,
    ) -> impl std::future::Future<Output = Result<Option<LockedTask>>> + Send;

    /// Used by §6.2 AcquireTask step 3a: insert the `task_runtime` row in
    /// the same transaction that locked the task. The CP follows up with a
    /// status update on `tasks` and commits.
    ///
    /// SERIALIZABLE: yes.
    fn record_acquisition(
        &mut self,
        lease: NewLease,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Used by §6.2 AcquireTask step 1: subscribe to per-namespace pending
    /// notifications.
    ///
    /// Conformance requirement §8.2 #5 (subscribe-pending ordering invariant):
    /// any transaction committing a row matching `(namespace, task_types)`
    /// strictly after this method returns MUST cause at least one
    /// `WakeSignal` to be observable on the returned stream. Poll-only
    /// backends satisfy this with short polling intervals plus the CP's
    /// 10s belt-and-suspenders fallback (`design.md` §8.4).
    ///
    /// `WakeSignal` is signal-only; the recipient re-runs
    /// `pick_and_lock_pending` to see what is actually available
    /// (`design.md` §8.4, `problems/07`).
    fn subscribe_pending(
        &mut self,
        namespace: &Namespace,
        task_types: &[TaskType],
    ) -> impl std::future::Future<
        Output = Result<Box<dyn Stream<Item = WakeSignal> + Send + Unpin + 'static>>,
    > + Send;

    // ========================================================================
    // Worker state transitions (SERIALIZABLE)
    // ========================================================================

    /// Used by §6.4 CompleteTask / ReportFailure: the unified state-transition
    /// handler. The CP layer pre-computes the `TaskOutcome` (terminal-state
    /// mapping per §6.5) and asks the storage layer to apply it atomically.
    ///
    /// Implementations MUST:
    /// 1. Validate the worker owns the lease (`SELECT … FOR UPDATE` with
    ///    matching `worker_id`); on 0 rows return `StorageError::NotFound`
    ///    so the CP can surface `LEASE_EXPIRED` (§6.4).
    /// 2. Insert the corresponding `task_results` row.
    /// 3. Update `tasks.status` to the terminal/retry state implied by the
    ///    outcome.
    /// 4. Delete the `task_runtime` row.
    ///
    /// Idempotent on `(task_id, attempt_number)` — a second call observes
    /// the existing terminal state and returns Ok without re-running side
    /// effects. The CP layer wraps this in transparent `40001` retries
    /// (§6.4) using `StorageError::SerializationConflict`.
    ///
    /// SERIALIZABLE: yes.
    fn complete_task(
        &mut self,
        lease: &LeaseRef,
        outcome: TaskOutcome,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    // ========================================================================
    // Reaper path
    // ========================================================================

    /// Used by §6.6 Reaper A — per-task timeout: list runtime rows with
    /// `timeout_at <= before` for batch reclaim.
    ///
    /// Backends MUST honor §8.2 #2 skip-locking semantics so concurrent
    /// reapers across replicas don't double-process the same row.
    ///
    /// SERIALIZABLE: yes.
    fn list_expired_runtimes(
        &mut self,
        before: Timestamp,
        n: usize,
    ) -> impl std::future::Future<Output = Result<Vec<ExpiredRuntime>>> + Send;

    /// Used by §6.6 Reaper A & B: reclaim a single runtime row.
    ///
    /// Implementations MUST:
    /// - Increment `task.attempt_number` and set `task.status = PENDING`.
    /// - Delete the `task_runtime` row.
    /// - For Reaper B's dead-worker case the CP additionally calls
    ///   `mark_worker_dead` (see below) in the same transaction so the
    ///   worker cannot transparently resurrect via continued heartbeats.
    ///
    /// SERIALIZABLE: yes.
    fn reclaim_runtime(
        &mut self,
        runtime: &RuntimeRef,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Used by §6.6 Reaper B: stamp `declared_dead_at = NOW()` on a worker's
    /// `worker_heartbeats` row in the same transaction that reclaimed its
    /// leases. The next heartbeat from that worker returns
    /// `HeartbeatAck::WorkerDeregistered` and the SDK re-`Register`s
    /// (`design.md` §6.3 step 2 + §6.6).
    ///
    /// SERIALIZABLE: yes (this write rides the reaper's state-transition
    /// transaction, distinct from the READ COMMITTED heartbeat path).
    fn mark_worker_dead(
        &mut self,
        worker_id: &WorkerId,
        at: Timestamp,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    // ========================================================================
    // Quota enforcement
    // ========================================================================

    /// Used by §6.1 SubmitTask + §6.2 AcquireTask: read a capacity quota
    /// dimension transactionally.
    ///
    /// Per `design.md` §1.1 carve-out, capacity quotas are NOT eventually
    /// consistent — they are read inline INSIDE the same SERIALIZABLE
    /// transaction that consults enforcement state (one cheap PK row lookup
    /// per admit/acquire). The version-fenced cache from round 1 has been
    /// removed (`design.md` §9.1).
    ///
    /// SERIALIZABLE: yes.
    fn check_capacity_quota(
        &mut self,
        namespace: &Namespace,
        kind: CapacityKind,
    ) -> impl std::future::Future<Output = Result<CapacityDecision>> + Send;

    /// Used by the `CoDel` admitter (`design.md` §7.1): return the age in
    /// milliseconds of the oldest PENDING task in `namespace`, or `None` when
    /// the namespace has no PENDING tasks.
    ///
    /// "Age" is `(now − submitted_at)`, computed by the backend so a single
    /// SQL round trip can answer the question without the CP layer having to
    /// page over rows. Returns the age of the *oldest* pending task because
    /// CoDel rejects when the head-of-line latency has already blown past the
    /// configured target.
    ///
    /// SERIALIZABLE: yes (rides the same admit transaction as
    /// `check_capacity_quota`).
    fn oldest_pending_age_ms(
        &mut self,
        namespace: &Namespace,
    ) -> impl std::future::Future<Output = Result<Option<u64>>> + Send;

    /// Used by §6.1 SubmitTask + §6.2 AcquireTask: try to consume `n` rate
    /// tokens for the given dimension.
    ///
    /// Per `design.md` §1.1 carve-out, rate quotas are eventually consistent
    /// within the namespace-config cache TTL (default 5s). The backend
    /// chooses between a per-replica in-memory token bucket (Postgres v1)
    /// and a shared counter (future backends with cheap atomic counters).
    /// Implementations MAY return `RateDecision::Allowed` with stale state
    /// up to the TTL window without violating the contract.
    fn try_consume_rate_quota(
        &mut self,
        namespace: &Namespace,
        kind: RateKind,
        n: u64,
    ) -> impl std::future::Future<Output = Result<RateDecision>> + Send;

    // ========================================================================
    // Heartbeats (READ COMMITTED carve-out, design.md §1.1)
    // ========================================================================

    /// Used by §6.3 Heartbeat step 2: UPSERT into `worker_heartbeats`.
    ///
    /// Carve-out: this method runs at READ COMMITTED, NOT SERIALIZABLE. The
    /// steady-state heartbeat path is one cheap UPSERT per ping; losing a
    /// few writes across a CP crash is consistent with at-least-once
    /// (`design.md` §1.1, §6.3, `problems/01`, `problems/02`).
    ///
    /// Returns `HeartbeatAck::WorkerDeregistered` when 0 rows were affected
    /// because Reaper B set `declared_dead_at` on this worker; the SDK
    /// surfaces `WORKER_DEREGISTERED` and re-`Register`s.
    fn record_worker_heartbeat(
        &mut self,
        worker_id: &WorkerId,
        namespace: &Namespace,
        at: Timestamp,
    ) -> impl std::future::Future<Output = Result<HeartbeatAck>> + Send;

    /// Used by §6.3 Heartbeat step 3 (lazy lease extension).
    ///
    /// Despite living on the heartbeat path, this write is SERIALIZABLE — it
    /// extends `task_runtime.timeout_at` and `last_extended_at`, which Reaper
    /// A reads. The "lazy" qualifier means the CP fires this only when
    /// `(NOW − last_extended_at) ≥ (lease_duration − ε)`, keeping write
    /// volume down to actual extensions, not every keepalive ping
    /// (`design.md` §6.3, §1.1).
    ///
    /// SERIALIZABLE: yes.
    fn extend_lease_lazy(
        &mut self,
        lease: &LeaseRef,
        new_timeout: Timestamp,
        last_extended_at: Timestamp,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    // ========================================================================
    // Cleanup
    // ========================================================================

    /// Used by §6.7 / §8.3 periodic dedup-expiry job: delete up to `n`
    /// idempotency rows whose `expires_at <= before`.
    ///
    /// Conformance requirement §8.2 #6 (bounded-cost dedup expiration):
    /// runtime MUST be bounded by `n`, not by the steady-state size of
    /// `idempotency_keys`. Backends with mutable per-row delete cost
    /// (Postgres without partitioning) MUST implement time-range
    /// partitioning of the dedup table so cleanup is `DROP PARTITION`, not
    /// row-by-row `DELETE`. SQLite is exempt by virtue of single-writer /
    /// dev-only scope.
    ///
    /// Returns the number of rows deleted (or partitions dropped, expressed
    /// as their row counts).
    fn delete_expired_dedup(
        &mut self,
        before: Timestamp,
        n: usize,
    ) -> impl std::future::Future<Output = Result<usize>> + Send;

    // ========================================================================
    // Admin / reads
    // ========================================================================

    /// Used by every admit/acquire flow (§6.1, §6.2) and admin RPCs:
    /// read `namespace_quota` for the given namespace, falling back to
    /// `system_default` for unset fields per `design.md` §9.1.
    ///
    /// Returns `StorageError::NotFound` if neither the requested namespace
    /// nor `system_default` exists (a misconfigured deployment).
    ///
    /// SERIALIZABLE: yes — capacity reads are inline transactional.
    fn get_namespace_quota(
        &mut self,
        namespace: &Namespace,
    ) -> impl std::future::Future<Output = Result<NamespaceQuota>> + Send;

    /// Used by every admin RPC (§6.7, §11.4): append an audit row in the
    /// same transaction as the action it records. Append-only, never
    /// overwritten.
    ///
    /// SERIALIZABLE: yes.
    fn audit_log_append(
        &mut self,
        entry: AuditEntry,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    // ========================================================================
    // Transaction lifecycle
    // ========================================================================

    /// Commit the transaction. Consumes `self` so further use is forbidden
    /// by the borrow checker. On `StorageError::SerializationConflict` the
    /// CP retries the surrounding logical operation per §6.4 / §6.6.
    fn commit(self) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Roll back the transaction. Consumes `self`. Always succeeds in the
    /// happy path; a failure here implies a backend connection issue and is
    /// surfaced as `BackendError`.
    fn rollback(self) -> impl std::future::Future<Output = Result<()>> + Send;
}
