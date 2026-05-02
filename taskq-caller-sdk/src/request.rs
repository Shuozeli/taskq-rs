//! Public request and outcome types for the caller SDK.
//!
//! These wrap the auto-generated FlatBuffers Object-API types
//! (`taskq_proto::SubmitTaskRequest` etc.) with a more ergonomic surface:
//!
//! - Required fields are non-optional in the SDK's `SubmitRequest`.
//! - `payload` is `Bytes` (Arc-backed) so the SDK can hash + encode without
//!   cloning the payload bytes.
//! - Outcome enums collapse the wire's "either `task` or `error`" pattern
//!   into one `match`-able value per RPC.
//!
//! Intentionally not re-exported as type aliases for the wire types: the
//! whole point of the SDK is to hide that `SubmitTaskResponse.error` may
//! be set; callers should consume the outcome enum.

use std::time::SystemTime;

use bytes::Bytes;
use taskq_proto::{RejectReason, TerminalState};

/// Newtype around a `task_id` string. Distinguishes wire identifiers from
/// arbitrary strings in the public API. The SDK does not validate the
/// internal format (UUIDv7 / ULID) — the server is the source of truth.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskId(String);

impl TaskId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl AsRef<str> for TaskId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// Per-task retry configuration override. Mirrors `taskq_proto::RetryConfig`
/// but uses Rust-native primitive types and is non-optional on every field.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfigOverride {
    pub initial_ms: u64,
    pub max_ms: u64,
    pub coefficient: f32,
    pub max_retries: u32,
}

/// A task submission request.
///
/// The SDK fills in `idempotency_key` (UUIDv7) and `traceparent` from the
/// current span if the caller leaves them unset. `payload_hash` is computed
/// internally; it is not part of the wire `SubmitTaskRequest` (the server
/// hashes the payload itself per `design.md` §6.1) but is computed
/// SDK-side so callers see a stable identity in error metadata.
#[derive(Debug, Clone, Default)]
pub struct SubmitRequest {
    /// Logical namespace this task belongs to. Required unless the client
    /// has been configured with `with_default_namespace`. Empty values
    /// surface as [`crate::ClientError::InvalidArgument`].
    pub namespace: String,
    pub task_type: String,
    pub payload: Bytes,
    /// `None` → SDK generates a UUIDv7 string (`design.md` §10.4).
    pub idempotency_key: Option<String>,
    pub priority: Option<i32>,
    pub max_retries: Option<u32>,
    pub expires_at: Option<SystemTime>,
    pub retry_config: Option<RetryConfigOverride>,
}

impl SubmitRequest {
    /// Convenience constructor for the most common shape.
    pub fn new(
        namespace: impl Into<String>,
        task_type: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            task_type: task_type.into(),
            payload: payload.into(),
            idempotency_key: None,
            priority: None,
            max_retries: None,
            expires_at: None,
            retry_config: None,
        }
    }
}

/// Outcome of a `submit` call. Distinguishes a fresh insert from the
/// idempotency-hit and payload-mismatch cases. `Created` and `Existing`
/// share the same status (always `PENDING` on the v1 server) but the
/// distinction is load-bearing for client logic that wants to log
/// "submitted vs. resubmitted" events.
#[derive(Debug, Clone)]
pub enum SubmitOutcome {
    Created {
        task_id: TaskId,
        status: TerminalState,
    },
    Existing {
        task_id: TaskId,
        status: TerminalState,
    },
    PayloadMismatch {
        existing_task_id: TaskId,
        existing_payload_hash: [u8; 32],
    },
}

/// Outcome of a `cancel` call. Mirrors the server-side
/// `CancelTaskResponse` — distinguishes "we just cancelled it" from
/// "it was already terminal at the time of the call".
#[derive(Debug, Clone)]
pub enum CancelOutcome {
    /// The task was non-terminal and we transitioned it to `CANCELLED`.
    Cancelled { task_id: TaskId },
    /// The task was already terminal; the wire returns this state and
    /// the SDK forwards it. May be `CANCELLED` (idempotent re-cancel),
    /// `COMPLETED`, `FAILED_NONRETRYABLE`, `FAILED_EXHAUSTED`, or
    /// `EXPIRED`.
    AlreadyTerminal {
        task_id: TaskId,
        final_status: TerminalState,
    },
}

/// Snapshot of a task at a point in time. Returned by `get_result` and
/// `batch_get_results`. `outcome` and `result_payload` / `failure` are
/// only meaningful when `status` is terminal.
#[derive(Debug, Clone)]
pub struct TaskState {
    pub task_id: TaskId,
    pub status: TerminalState,
    pub outcome: Option<TaskOutcome>,
    pub result_payload: Option<Bytes>,
    pub failure: Option<Failure>,
}

/// Worker-reported outcome on a terminal task. Identical to the wire enum
/// but re-exposed as a plain Rust enum so callers don't reach into
/// `taskq_proto`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOutcome {
    Success,
    RetryableFail,
    NonretryableFail,
    Cancelled,
    Expired,
}

impl TaskOutcome {
    pub(crate) fn from_wire(o: taskq_proto::TaskOutcome) -> Option<Self> {
        match o {
            taskq_proto::TaskOutcome::SUCCESS => Some(Self::Success),
            taskq_proto::TaskOutcome::RETRYABLE_FAIL => Some(Self::RetryableFail),
            taskq_proto::TaskOutcome::NONRETRYABLE_FAIL => Some(Self::NonretryableFail),
            taskq_proto::TaskOutcome::CANCELLED => Some(Self::Cancelled),
            taskq_proto::TaskOutcome::EXPIRED => Some(Self::Expired),
            _ => None,
        }
    }
}

/// Worker-reported failure detail. Mirrors `taskq_proto::Failure` with
/// owned strings/bytes so callers can move them out without lifetime
/// bookkeeping.
#[derive(Debug, Clone)]
pub struct Failure {
    pub error_class: String,
    pub message: String,
    pub details: Bytes,
    pub retryable: bool,
}

/// Outcome of `submit_and_wait`. Either the task settled into a terminal
/// state within the deadline, or it timed out and the caller can poll
/// `get_result` later.
#[derive(Debug, Clone)]
pub enum TerminalOutcome {
    Settled(TaskState),
    TimedOut { task_id: TaskId },
}

/// Internal helper: did the wire `Rejection` encode a permanent vs
/// retryable failure? The wire field `retryable` is authoritative; the
/// SDK only consults `reason` when `retryable` is missing (defaults to
/// `false` per the FlatBuffers schema, which is the safe choice).
pub(crate) fn rejection_is_retryable(reason: RejectReason, retryable: bool) -> bool {
    if retryable {
        return true;
    }
    // Defensive: in case a test or pre-release server returns a structured
    // rejection without setting `retryable`, recompute from the documented
    // reason set in `design.md` §10.1.
    matches!(
        reason,
        RejectReason::SYSTEM_OVERLOAD
            | RejectReason::MAX_PENDING_EXCEEDED
            | RejectReason::LATENCY_TARGET_EXCEEDED
            | RejectReason::NAMESPACE_QUOTA_EXCEEDED
            | RejectReason::REPLICA_WAITER_LIMIT_EXCEEDED
    )
}
