//! Plain-data types crossing the `StorageTx` boundary.
//!
//! These mirror the logical data model from `design.md` §4. Backends
//! translate them to native row representations.

use bytes::Bytes;

use crate::ids::{IdempotencyKey, Namespace, TaskId, TaskType, Timestamp, WorkerId};

// ============================================================================
// Submit
// ============================================================================

/// Existing idempotency record returned by `lookup_idempotency`.
///
/// See `design.md` §6.1: a hit with matching `payload_hash` returns the
/// existing `task_id`; a hit with mismatched hash rejects with
/// `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD` (`§10.1`).
#[derive(Debug, Clone)]
pub struct DedupRecord {
    pub task_id: TaskId,
    /// blake3 hash of the submitted payload (32 bytes).
    pub payload_hash: [u8; 32],
    /// When this dedup row expires. Lazy cleanup deletes rows past this.
    pub expires_at: Timestamp,
}

/// New idempotency record passed to `insert_task` alongside the new task row.
///
/// See `design.md` §6.1 step 5.
#[derive(Debug, Clone)]
pub struct NewDedupRecord {
    pub namespace: Namespace,
    pub key: IdempotencyKey,
    pub payload_hash: [u8; 32],
    pub expires_at: Timestamp,
}

/// New task to insert via `insert_task`.
///
/// Mirrors the `tasks` columns in `design.md` §4. `payload` is opaque bytes;
/// the storage layer never parses the wire format.
#[derive(Debug, Clone)]
pub struct NewTask {
    pub task_id: TaskId,
    pub namespace: Namespace,
    pub task_type: TaskType,
    pub priority: i32,
    pub payload: Bytes,
    pub payload_hash: [u8; 32],
    pub submitted_at: Timestamp,
    pub expires_at: Timestamp,
    pub max_retries: u32,
    pub retry_initial_ms: u64,
    pub retry_max_ms: u64,
    pub retry_coefficient: f32,
    /// W3C `traceparent` header bytes (55 bytes when present).
    pub traceparent: Bytes,
    /// W3C `tracestate` header bytes (variable, ≤ 256 bytes).
    pub tracestate: Bytes,
    pub format_version: u32,
}

// ============================================================================
// Dispatch
// ============================================================================

/// Filter on which namespaces a `pick_and_lock_pending` call considers.
///
/// `RandomNamespace` dispatch may inspect the broader set; `PriorityFifo` is
/// usually scoped to one namespace.
#[derive(Debug, Clone)]
pub enum NamespaceFilter {
    /// Restrict to a single namespace.
    Single(Namespace),
    /// Sample from any of the listed namespaces.
    AnyOf(Vec<Namespace>),
    /// All active namespaces (used by `RandomNamespace` dispatcher).
    Any,
}

/// Filter on which task types a worker is willing to take. Workers declare
/// the types they handle on `Register`; dispatch pre-filters by this set
/// (`design.md` §7.2).
#[derive(Debug, Clone)]
pub enum TaskTypeFilter {
    /// Worker handles only the listed task types.
    AnyOf(Vec<TaskType>),
}

/// Strategy intent for picking the next pending task. The backend translates
/// each variant to its native idiom (Postgres `ORDER BY` + `FOR UPDATE SKIP
/// LOCKED`, FoundationDB conflict-range retry, etc.).
///
/// See `design.md` §7.2 for the strategy framework and §8.1 for the trait
/// shape. `WeightedFair` / `TokenBucket` / `RoundRobin` are deferred to v2.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PickOrdering {
    /// `ORDER BY priority DESC, submitted_at ASC`.
    PriorityFifo,
    /// Effective priority = base priority + `f(age)`. Older tasks bubble up.
    AgePromoted {
        /// Multiplier on task age contributing to effective priority.
        age_weight: f64,
    },
    /// Sample uniformly from active namespaces; within the chosen namespace
    /// fall back to `PriorityFifo`.
    RandomNamespace {
        /// How many candidate namespaces to sample before giving up.
        sample_attempts: u32,
    },
}

/// Criteria passed by the dispatcher strategy to `pick_and_lock_pending`.
///
/// See `problems/12-storage-abstraction.md` for the design discussion of
/// `PickCriteria` as the seam between strategy intent and backend-native SQL.
#[derive(Debug, Clone)]
pub struct PickCriteria {
    pub namespace_filter: NamespaceFilter,
    pub task_types_filter: TaskTypeFilter,
    pub ordering: PickOrdering,
    /// As of when the picker considers tasks "available". Used to filter out
    /// `WAITING_RETRY` tasks whose `retry_after` has not arrived.
    pub now: Timestamp,
}

/// Row returned by `pick_and_lock_pending` once a candidate has been locked
/// for the calling transaction. The caller follows up with
/// `record_acquisition` to write the lease in the same transaction.
#[derive(Debug, Clone)]
pub struct LockedTask {
    pub task_id: TaskId,
    pub namespace: Namespace,
    pub task_type: TaskType,
    pub attempt_number: u32,
    pub priority: i32,
    pub payload: Bytes,
    pub submitted_at: Timestamp,
    pub expires_at: Timestamp,
    pub max_retries: u32,
    pub retry_initial_ms: u64,
    pub retry_max_ms: u64,
    pub retry_coefficient: f32,
    pub traceparent: Bytes,
    pub tracestate: Bytes,
}

/// Lease record to insert via `record_acquisition`. Mirrors `task_runtime`
/// columns from `design.md` §4.
#[derive(Debug, Clone)]
pub struct NewLease {
    pub task_id: TaskId,
    pub attempt_number: u32,
    pub worker_id: WorkerId,
    pub acquired_at: Timestamp,
    pub timeout_at: Timestamp,
}

/// Reference identifying a single lease — `(task_id, attempt_number,
/// worker_id)`. Used by `complete_task`, `extend_lease_lazy`, etc.
#[derive(Debug, Clone)]
pub struct LeaseRef {
    pub task_id: TaskId,
    pub attempt_number: u32,
    pub worker_id: WorkerId,
}

/// Reference to a `task_runtime` row identified for reaping. Returned by
/// `list_expired_runtimes` and consumed by `reclaim_runtime`.
#[derive(Debug, Clone)]
pub struct RuntimeRef {
    pub task_id: TaskId,
    pub attempt_number: u32,
    /// The worker that held the lease — needed by Reaper B which must stamp
    /// `declared_dead_at` on the worker's `worker_heartbeats` row in the
    /// same transaction (`design.md` §6.6).
    pub worker_id: WorkerId,
}

/// Row returned by `list_expired_runtimes` — Reaper A reads this for the
/// per-task timeout path.
#[derive(Debug, Clone)]
pub struct ExpiredRuntime {
    pub task_id: TaskId,
    pub attempt_number: u32,
    pub worker_id: WorkerId,
    pub timeout_at: Timestamp,
    pub namespace: Namespace,
}

/// Signal-only wakeup that `subscribe_pending` yields when a matching row
/// commits. The signal carries no payload — the recipient re-runs
/// `pick_and_lock_pending` to see what's actually available. See
/// `design.md` §8.4 and `problems/07`.
///
/// A struct (rather than `()`) leaves room for non-load-bearing diagnostic
/// fields (e.g. backend identifier) in future minors without breaking the
/// signal-only contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WakeSignal;

// ============================================================================
// Worker state transitions
// ============================================================================

/// Outcome the worker reported on `CompleteTask` / `ReportFailure`. The CP
/// layer combines this with `design.md` §6.5's terminal-state mapping table
/// before calling `complete_task`.
#[derive(Debug, Clone)]
pub enum TaskOutcome {
    /// The worker completed the task successfully.
    Success {
        /// Opaque result payload, persisted to `task_results.result_payload`.
        result_payload: Bytes,
        recorded_at: Timestamp,
    },
    /// Worker reported a failure that the CP classified as a non-retryable
    /// terminal failure. Maps to `task.status = FAILED_NONRETRYABLE`.
    FailedNonretryable {
        error_class: String,
        error_message: String,
        error_details: Bytes,
        recorded_at: Timestamp,
    },
    /// Worker reported a retryable failure but `attempt >= max_retries`.
    /// Maps to `task.status = FAILED_EXHAUSTED`.
    FailedExhausted {
        error_class: String,
        error_message: String,
        error_details: Bytes,
        recorded_at: Timestamp,
    },
    /// Worker reported a retryable failure but `retry_after > task.expires_at`.
    /// Maps to `task.status = EXPIRED`.
    Expired {
        error_class: String,
        error_message: String,
        error_details: Bytes,
        recorded_at: Timestamp,
    },
    /// Worker reported a retryable failure with budget remaining. Maps to
    /// `task.status = WAITING_RETRY` with `retry_after` stamped.
    WaitingRetry {
        error_class: String,
        error_message: String,
        error_details: Bytes,
        retry_after: Timestamp,
        recorded_at: Timestamp,
    },
}

// ============================================================================
// Heartbeats
// ============================================================================

/// Result of `record_worker_heartbeat`. Distinguishes the live-worker case
/// from the dead-worker case (Reaper B has stamped `declared_dead_at`).
///
/// See `design.md` §6.3 step 2: `0 rows affected` means the worker was
/// declared dead; the SDK must re-`Register` before continuing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatAck {
    /// Heartbeat was UPSERTed normally. Worker remains alive.
    Recorded,
    /// `declared_dead_at` is set on this worker's row — Reaper B already
    /// reclaimed its leases. The SDK surfaces `WORKER_DEREGISTERED` and
    /// re-`Register`s.
    WorkerDeregistered,
}

// ============================================================================
// Quota
// ============================================================================

/// Capacity quota dimensions read inline transactionally per `design.md`
/// §9.1 (no cache fence; capacity reads are not eventually consistent).
///
/// Round-2 deferred `MaxWaitersPerNamespace` to v2 — single-replica v1 has
/// no shared-counter need, only the per-replica cap matters
/// (`problems/12` round-2 refinement, `design.md` §6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityKind {
    /// Cap on tasks in `PENDING` per namespace.
    MaxPending,
    /// Cap on tasks in `DISPATCHED` per namespace.
    MaxInflight,
    /// Cap on registered workers per namespace.
    MaxWorkers,
    /// Cap on long-poll waiters held by *this* CP replica per namespace.
    MaxWaitersPerReplica,
}

/// Decision returned by `check_capacity_quota`. Carries the observed counts
/// so the CP can populate `pending_count` / `pending_limit` in
/// `RESOURCE_EXHAUSTED` rejections (`design.md` §10.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityDecision {
    UnderLimit { current: u64, limit: u64 },
    OverLimit { current: u64, limit: u64 },
}

/// Rate quota dimensions. Eventually consistent within the namespace-config
/// cache TTL per `design.md` §1.1 carve-out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateKind {
    /// `max_submit_rpm` — submit-side rate limit.
    SubmitRpm,
    /// `max_dispatch_rpm` — dispatch-side rate limit.
    DispatchRpm,
    /// `max_replay_per_second` — admin DLQ replay rate limit.
    ReplayPerSecond,
}

/// Decision returned by `try_consume_rate_quota`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateDecision {
    /// Tokens consumed; `remaining` reflects the post-consume bucket level.
    Allowed { remaining: u64 },
    /// Rate limit hit; `retry_after_ms` is the smallest backoff that has a
    /// chance of refilling enough tokens. Used to populate `retry_after` in
    /// `RESOURCE_EXHAUSTED` (`design.md` §10.1).
    RateLimited { retry_after_ms: u64 },
}

/// Mirror of `namespace_quota` from `design.md` §4 returned by
/// `get_namespace_quota`. Plain-data shape so the storage layer doesn't
/// depend on protocol enums.
#[derive(Debug, Clone)]
pub struct NamespaceQuota {
    pub namespace: Namespace,
    pub admitter_kind: String,
    pub admitter_params: Bytes,
    pub dispatcher_kind: String,
    pub dispatcher_params: Bytes,

    // Capacity (read inline; design.md §9.1)
    pub max_pending: Option<u64>,
    pub max_inflight: Option<u64>,
    pub max_workers: Option<u32>,
    pub max_waiters_per_replica: Option<u32>,

    // Rate (eventually consistent within cache TTL; design.md §1.1)
    pub max_submit_rpm: Option<u64>,
    pub max_dispatch_rpm: Option<u64>,
    pub max_replay_per_second: Option<u32>,

    // Per-task ceilings
    pub max_retries_ceiling: u32,
    pub max_idempotency_ttl_seconds: u64,
    pub max_payload_bytes: u32,
    pub max_details_bytes: u32,
    pub min_heartbeat_interval_seconds: u32,

    // Lease/heartbeat invariant: ε ≥ 2 × min_heartbeat_interval (design.md §9.1)
    pub lazy_extension_threshold_seconds: u32,

    // Cardinality budget
    pub max_error_classes: u32,
    pub max_task_types: u32,

    // Observability
    pub trace_sampling_ratio: f32,
    pub log_level_override: Option<String>,
    pub audit_log_retention_days: u32,
    pub metrics_export_enabled: bool,
}

// ============================================================================
// Audit
// ============================================================================

/// Append-only audit log entry. Written same-transaction as the admin RPC
/// it records (`design.md` §6.7, §11.4).
#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub timestamp: Timestamp,
    pub actor: String,
    pub rpc: String,
    pub namespace: Option<Namespace>,
    /// Structured JSON-encoded request summary; the storage layer treats
    /// this as opaque bytes. May be truncated by the CP layer per
    /// `design.md` §14 open question.
    pub request_summary: Bytes,
    pub result: String,
    /// sha256 of the original request body for external verification.
    pub request_hash: [u8; 32],
}
