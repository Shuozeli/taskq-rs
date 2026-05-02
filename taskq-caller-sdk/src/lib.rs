//! taskq-caller-sdk: caller-side Rust SDK for the taskq-rs gRPC contract.
//!
//! Wraps the auto-generated `taskq_proto::task_queue_client::TaskQueueClient`
//! with an ergonomic, retry-aware Rust API. Public surface:
//!
//! - [`CallerClient`] — the entry point. Constructed via [`CallerClient::connect`].
//! - [`SubmitRequest`] / [`SubmitOutcome`] — the high-level submit shape.
//! - [`TaskState`], [`CancelOutcome`], [`TerminalOutcome`] — outcome types
//!   for `get_result`, `cancel`, and `submit_and_wait`.
//! - [`ClientError`] — unified error envelope.
//!
//! Three behaviours the SDK adds on top of the raw client:
//!
//! 1. **`RESOURCE_EXHAUSTED` retry loop** (`design.md` §10.4): rejections
//!    with `retryable=true` are retried up to the configured budget,
//!    sleeping until `retry_after_ms` plus ±20% jitter. `retryable=false`
//!    is surfaced immediately.
//! 2. **W3C Trace Context propagation** (`design.md` §11.2): the current
//!    `tracing::Span`'s OTel context (if any) is serialized into the
//!    wire `traceparent` / `tracestate` fields. With no active span, a
//!    fresh W3C-valid context is generated so the server always has
//!    something to persist on the task row.
//! 3. **Auto-generated idempotency keys** (`design.md` §10.4): if the
//!    caller leaves [`SubmitRequest::idempotency_key`] unset, the SDK
//!    generates a UUIDv7 string. The protocol does not let callers
//!    omit the key on the wire.
//!
//! `submit_and_wait` polls `get_result` every 500 ms while the CP-side
//! polling implementation is incomplete (see `design.md` §6.7 + the
//! Phase 5b stub in `taskq-cp/src/handlers/task_queue.rs`).

mod error;
mod hash;
mod request;
mod retry;
mod trace;

pub use error::{ClientError, ConnectError};
pub use request::{
    CancelOutcome, Failure, RetryConfigOverride, SubmitOutcome, SubmitRequest, TaskId, TaskOutcome,
    TaskState, TerminalOutcome,
};
pub use retry::RetryBudget;
pub use taskq_proto::{RejectReason, TerminalState};

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use grpc_client::Channel;
use taskq_proto::task_queue_client::TaskQueueClient;
use taskq_proto::{
    BatchGetTaskResultsRequest, CancelTaskRequest, GetTaskResultRequest, RetryConfig, SemVer,
    SubmitAndWaitRequest, SubmitTaskRequest, Timestamp,
};

/// Wire-format SDK version. Increment when the SDK starts depending on a
/// newer wire field. This goes into `client_version` on every request so
/// the server can detect skew.
const SDK_SEMVER: (u32, u32, u32) = (1, 0, 0);

/// Default polling interval for the client-side `submit_and_wait` fallback.
const SUBMIT_AND_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Caller-side client for the taskq `TaskQueue` service.
///
/// Holds an underlying `TaskQueueClient<Channel>` plus configuration
/// (retry budget, default namespace). `CallerClient` is `Clone` so a
/// single instance can be shared across tasks; the underlying gRPC
/// client multiplexes RPCs over one HTTP/2 connection.
#[derive(Clone)]
pub struct CallerClient {
    inner: TaskQueueClient<Channel>,
    retry_budget: RetryBudget,
    default_namespace: Option<String>,
    poll_interval: Duration,
}

impl CallerClient {
    /// Open a gRPC connection to a taskq control plane. The `uri` is
    /// the canonical `http://host:port/` URI; HTTPS is not yet
    /// supported by the underlying transport (Phase 8).
    pub async fn connect(uri: http::Uri) -> Result<Self, ConnectError> {
        let inner = TaskQueueClient::<Channel>::connect(uri)
            .await
            .map_err(ConnectError::from_boxed)?;
        Ok(Self {
            inner,
            retry_budget: RetryBudget::default(),
            default_namespace: None,
            poll_interval: SUBMIT_AND_WAIT_POLL_INTERVAL,
        })
    }

    /// Wrap an already-constructed `TaskQueueClient<Channel>`. Primarily
    /// useful for tests that hand the SDK a transport that points at an
    /// in-process server. Production code should use [`Self::connect`].
    pub fn from_grpc_client(inner: TaskQueueClient<Channel>) -> Self {
        Self {
            inner,
            retry_budget: RetryBudget::default(),
            default_namespace: None,
            poll_interval: SUBMIT_AND_WAIT_POLL_INTERVAL,
        }
    }

    /// Override the default retry budget (default 5 attempts).
    pub fn with_retry_budget(mut self, budget: u32) -> Self {
        self.retry_budget = RetryBudget::new(budget);
        self
    }

    /// Set a default namespace; submissions that leave
    /// [`SubmitRequest::namespace`] empty will fall back to this value.
    pub fn with_default_namespace(mut self, ns: impl Into<String>) -> Self {
        self.default_namespace = Some(ns.into());
        self
    }

    /// Override the `submit_and_wait` polling interval (default 500 ms).
    /// Test-only knob; production callers should leave the default.
    #[doc(hidden)]
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Submit a task. Generates a UUIDv7 idempotency key if the caller
    /// did not provide one, computes the BLAKE3 payload hash, and
    /// retries `RESOURCE_EXHAUSTED`-with-`retryable=true` rejections
    /// up to the configured budget.
    pub async fn submit(&mut self, req: SubmitRequest) -> Result<SubmitOutcome, ClientError> {
        let namespace = self.resolve_namespace(&req.namespace)?;
        if req.task_type.is_empty() {
            return Err(ClientError::InvalidArgument("task_type is required".into()));
        }
        let idempotency_key = req
            .idempotency_key
            .clone()
            .unwrap_or_else(generate_idempotency_key);

        let wire = build_submit_wire(&req, &namespace, &idempotency_key);
        let mut attempt: u32 = 0;
        loop {
            let resp = self.inner.submit_task(wire.clone()).await?;
            let resp = resp.into_inner();
            if let Some(err) = resp.error.as_ref() {
                let retryable = request::rejection_is_retryable(err.reason, err.retryable);
                let retry_after_ms = err.retry_after.as_ref().map(|t| t.unix_millis).unwrap_or(0);

                if matches!(
                    err.reason,
                    RejectReason::IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD
                ) {
                    return Ok(SubmitOutcome::PayloadMismatch {
                        existing_task_id: TaskId::new(
                            err.existing_task_id.clone().unwrap_or_default(),
                        ),
                        existing_payload_hash: payload_hash_or_zero(
                            err.existing_payload_hash.as_deref(),
                        ),
                    });
                }

                if !retryable || attempt + 1 >= self.retry_budget.max_attempts {
                    return Err(ClientError::Rejected {
                        reason: err.reason,
                        retry_after_ms,
                        retryable,
                        hint: err.hint.clone().unwrap_or_default(),
                    });
                }
                let sleep = retry::sleep_duration_for_retry(retry_after_ms, SystemTime::now());
                tokio::time::sleep(sleep).await;
                attempt += 1;
                continue;
            }

            // Success path. The server returns `existing_task=true` when
            // the dedup row matched and we got the prior task_id back.
            let task_id = TaskId::new(resp.task_id.unwrap_or_default());
            if task_id.as_str().is_empty() {
                return Err(ClientError::Internal(
                    "server returned ok response with empty task_id".into(),
                ));
            }
            let outcome = if resp.existing_task {
                SubmitOutcome::Existing {
                    task_id,
                    status: resp.status,
                }
            } else {
                SubmitOutcome::Created {
                    task_id,
                    status: resp.status,
                }
            };
            return Ok(outcome);
        }
    }

    /// Fetch the current state of a single task. Returns
    /// [`ClientError::Transport`] with `NOT_FOUND` if the task does not
    /// exist.
    pub async fn get_result(&mut self, task_id: &TaskId) -> Result<TaskState, ClientError> {
        let mut req = GetTaskResultRequest::default();
        req.client_version = Some(Box::new(sdk_semver()));
        let trace = trace::extract_or_generate();
        req.traceparent = Some(trace.traceparent);
        req.tracestate = Some(trace.tracestate);
        req.namespace = self.default_namespace.clone();
        req.task_id = Some(task_id.as_str().to_owned());

        let resp = self
            .inner
            .get_result_with_retry(req, self.retry_budget)
            .await?;
        decode_task_state(resp, task_id)
    }

    /// Fetch the current state of multiple tasks in a single RPC.
    /// Returns one `TaskState` per id in the same order as the input;
    /// per-id failures are surfaced as `Internal`-classified errors so
    /// callers see which id was problematic without having to thread
    /// a `Result<TaskState>` per slot.
    pub async fn batch_get_results(
        &mut self,
        task_ids: &[TaskId],
    ) -> Result<Vec<TaskState>, ClientError> {
        let mut req = BatchGetTaskResultsRequest::default();
        req.client_version = Some(Box::new(sdk_semver()));
        let trace = trace::extract_or_generate();
        req.traceparent = Some(trace.traceparent);
        req.tracestate = Some(trace.tracestate);
        req.namespace = self.default_namespace.clone();
        req.task_ids = Some(task_ids.iter().map(|t| t.as_str().to_owned()).collect());

        let resp = self.inner.batch_get_task_results(req).await?.into_inner();
        if let Some(err) = resp.error.as_ref() {
            return Err(ClientError::Rejected {
                reason: err.reason,
                retry_after_ms: err.retry_after.as_ref().map(|t| t.unix_millis).unwrap_or(0),
                retryable: err.retryable,
                hint: err.hint.clone().unwrap_or_default(),
            });
        }
        let results = resp.results.unwrap_or_default();
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            // Per-id error: surface as Internal so the caller can see
            // which id failed without losing the others. v1 of the SDK
            // is conservative here; v2 may switch to `Vec<Result<...>>`.
            if let Some(e) = r.error.as_ref() {
                return Err(ClientError::Rejected {
                    reason: e.reason,
                    retry_after_ms: e.retry_after.as_ref().map(|t| t.unix_millis).unwrap_or(0),
                    retryable: e.retryable,
                    hint: e.hint.clone().unwrap_or_default(),
                });
            }
            let task = r.task.as_ref().ok_or_else(|| {
                ClientError::Internal("BatchTaskResult missing task field with no error".into())
            })?;
            out.push(TaskState {
                task_id: TaskId::new(task.task_id.clone().unwrap_or_default()),
                status: task.status,
                outcome: TaskOutcome::from_wire(r.outcome),
                result_payload: r
                    .result_payload
                    .clone()
                    .map(Bytes::from)
                    .filter(|b| !b.is_empty()),
                failure: r.failure.as_ref().map(|f| Failure {
                    error_class: f.error_class.clone().unwrap_or_default(),
                    message: f.message.clone().unwrap_or_default(),
                    details: f.details.clone().map(Bytes::from).unwrap_or_default(),
                    retryable: f.retryable,
                }),
            });
        }
        Ok(out)
    }

    /// Cancel a task. Idempotent — if the task is already terminal,
    /// returns [`CancelOutcome::AlreadyTerminal`] with the observed
    /// state; if the task does not exist, surfaces the server's
    /// `NOT_FOUND` as [`ClientError::Transport`].
    pub async fn cancel(&mut self, task_id: &TaskId) -> Result<CancelOutcome, ClientError> {
        let mut req = CancelTaskRequest::default();
        req.client_version = Some(Box::new(sdk_semver()));
        let trace = trace::extract_or_generate();
        req.traceparent = Some(trace.traceparent);
        req.tracestate = Some(trace.tracestate);
        req.namespace = self.default_namespace.clone();
        req.task_id = Some(task_id.as_str().to_owned());

        let resp = self.inner.cancel_task(req).await?.into_inner();
        if let Some(err) = resp.error.as_ref() {
            return Err(ClientError::Rejected {
                reason: err.reason,
                retry_after_ms: err.retry_after.as_ref().map(|t| t.unix_millis).unwrap_or(0),
                retryable: err.retryable,
                hint: err.hint.clone().unwrap_or_default(),
            });
        }
        let final_status = resp.final_status;
        let task_id = TaskId::new(resp.task_id.unwrap_or_else(|| task_id.as_str().to_owned()));
        Ok(match final_status {
            TerminalState::CANCELLED => CancelOutcome::Cancelled { task_id },
            other => CancelOutcome::AlreadyTerminal {
                task_id,
                final_status: other,
            },
        })
    }

    /// Submit a task and block until it reaches a terminal state or the
    /// deadline expires. Until the CP's server-side wait is implemented
    /// (Phase 5c), the SDK falls back to client-side polling on
    /// `get_result` every 500 ms (configurable via
    /// [`Self::with_poll_interval`]).
    pub async fn submit_and_wait(
        &mut self,
        req: SubmitRequest,
        timeout: Duration,
    ) -> Result<TerminalOutcome, ClientError> {
        // Try the server-side SubmitAndWait first. The CP's stub returns
        // `timed_out=true` immediately as of Phase 5b; we pick up the
        // `task_id` it gives back and poll from there.
        let namespace = self.resolve_namespace(&req.namespace)?;
        if req.task_type.is_empty() {
            return Err(ClientError::InvalidArgument("task_type is required".into()));
        }
        let idempotency_key = req
            .idempotency_key
            .clone()
            .unwrap_or_else(generate_idempotency_key);

        let submit_wire = build_submit_wire(&req, &namespace, &idempotency_key);
        let mut wire = SubmitAndWaitRequest::default();
        wire.client_version = Some(Box::new(sdk_semver()));
        // Trace context: use the current span — same context propagates
        // to the embedded SubmitTaskRequest so the server stores one
        // unified trace, not two.
        let trace = trace::extract_or_generate();
        wire.traceparent = Some(trace.traceparent.clone());
        wire.tracestate = Some(trace.tracestate.clone());
        wire.submit = Some(Box::new(submit_wire));
        wire.wait_timeout_ms = timeout.as_millis().min(u128::from(u64::MAX)) as u64;

        let resp = self.inner.submit_and_wait(wire).await?.into_inner();
        if let Some(err) = resp.error.as_ref() {
            // Stub server returns SYSTEM_OVERLOAD with `existing_task_id`
            // pointing at the freshly-created task; treat it as a
            // successful submit + we-need-to-poll.
            if matches!(err.reason, RejectReason::SYSTEM_OVERLOAD)
                && err
                    .existing_task_id
                    .as_deref()
                    .is_some_and(|s| !s.is_empty())
            {
                let task_id = TaskId::new(err.existing_task_id.clone().unwrap_or_default());
                return self.poll_until_terminal(&task_id, timeout).await;
            }
            return Err(ClientError::Rejected {
                reason: err.reason,
                retry_after_ms: err.retry_after.as_ref().map(|t| t.unix_millis).unwrap_or(0),
                retryable: err.retryable,
                hint: err.hint.clone().unwrap_or_default(),
            });
        }

        if resp.timed_out {
            // Real timeout: poll one more time so the caller sees a
            // settled state if the task happened to complete in between.
            // If the task has no task_id (server bug), surface as
            // Internal.
            let task = resp.task.as_ref();
            let task_id = task
                .and_then(|t| t.task_id.clone())
                .map(TaskId::new)
                .ok_or_else(|| {
                    ClientError::Internal(
                        "submit_and_wait timed_out=true with no task field".into(),
                    )
                })?;
            return self.poll_until_terminal(&task_id, timeout).await;
        }

        // Server settled in-line.
        let task = resp.task.as_ref().ok_or_else(|| {
            ClientError::Internal("submit_and_wait succeeded with no task field".into())
        })?;
        let task_id = TaskId::new(task.task_id.clone().unwrap_or_default());
        Ok(TerminalOutcome::Settled(TaskState {
            task_id,
            status: task.status,
            outcome: TaskOutcome::from_wire(resp.outcome),
            result_payload: resp
                .result_payload
                .map(Bytes::from)
                .filter(|b| !b.is_empty()),
            failure: resp.failure.as_ref().map(|f| Failure {
                error_class: f.error_class.clone().unwrap_or_default(),
                message: f.message.clone().unwrap_or_default(),
                details: f.details.clone().map(Bytes::from).unwrap_or_default(),
                retryable: f.retryable,
            }),
        }))
    }

    /// Poll `get_result` every `self.poll_interval` until the task settles
    /// or `timeout` elapses.
    async fn poll_until_terminal(
        &mut self,
        task_id: &TaskId,
        timeout: Duration,
    ) -> Result<TerminalOutcome, ClientError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // If `get_result` itself is unimplemented at the server,
            // surface the transport error instead of looping forever.
            let state = match self.get_result(task_id).await {
                Ok(s) => s,
                Err(ClientError::Transport(s)) if s.code() == grpc_core::Code::Unimplemented => {
                    return Err(ClientError::Transport(s));
                }
                Err(e) => return Err(e),
            };
            if is_terminal(state.status) {
                return Ok(TerminalOutcome::Settled(state));
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(TerminalOutcome::TimedOut {
                    task_id: task_id.clone(),
                });
            }
            tokio::time::sleep(self.poll_interval).await;
        }
    }

    fn resolve_namespace(&self, requested: &str) -> Result<String, ClientError> {
        if !requested.is_empty() {
            return Ok(requested.to_owned());
        }
        self.default_namespace.clone().ok_or_else(|| {
            ClientError::InvalidArgument(
                "namespace is required (set on the SubmitRequest or call with_default_namespace)"
                    .into(),
            )
        })
    }
}

/// Wire-shape helper trait: makes `get_result` retry-aware without
/// duplicating the loop body.
trait GetResultWithRetry {
    async fn get_result_with_retry(
        &mut self,
        req: GetTaskResultRequest,
        budget: RetryBudget,
    ) -> Result<taskq_proto::GetTaskResultResponse, ClientError>;
}

impl GetResultWithRetry for TaskQueueClient<Channel> {
    async fn get_result_with_retry(
        &mut self,
        req: GetTaskResultRequest,
        budget: RetryBudget,
    ) -> Result<taskq_proto::GetTaskResultResponse, ClientError> {
        let mut attempt: u32 = 0;
        loop {
            let resp = self.get_task_result(req.clone()).await?.into_inner();
            if let Some(err) = resp.error.as_ref() {
                let retryable = request::rejection_is_retryable(err.reason, err.retryable);
                let retry_after_ms = err.retry_after.as_ref().map(|t| t.unix_millis).unwrap_or(0);
                if !retryable || attempt + 1 >= budget.max_attempts {
                    return Err(ClientError::Rejected {
                        reason: err.reason,
                        retry_after_ms,
                        retryable,
                        hint: err.hint.clone().unwrap_or_default(),
                    });
                }
                let sleep = retry::sleep_duration_for_retry(retry_after_ms, SystemTime::now());
                tokio::time::sleep(sleep).await;
                attempt += 1;
                continue;
            }
            return Ok(resp);
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn sdk_semver() -> SemVer {
    let mut s = SemVer::default();
    s.major = SDK_SEMVER.0;
    s.minor = SDK_SEMVER.1;
    s.patch = SDK_SEMVER.2;
    s
}

fn generate_idempotency_key() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// Build the on-the-wire `SubmitTaskRequest` from the SDK's high-level
/// `SubmitRequest` plus a resolved namespace + idempotency key. This is
/// pulled out of `submit` so `submit_and_wait` reuses the exact same
/// request shape.
fn build_submit_wire(
    req: &SubmitRequest,
    namespace: &str,
    idempotency_key: &str,
) -> SubmitTaskRequest {
    let mut wire = SubmitTaskRequest::default();
    wire.client_version = Some(Box::new(sdk_semver()));
    let trace = trace::extract_or_generate();
    wire.traceparent = Some(trace.traceparent);
    wire.tracestate = Some(trace.tracestate);
    wire.namespace = Some(namespace.to_owned());
    wire.task_type = Some(req.task_type.clone());
    wire.payload = Some(req.payload.to_vec());
    wire.idempotency_key = Some(idempotency_key.to_owned());
    if let Some(p) = req.priority {
        wire.priority = p;
    }
    if let Some(m) = req.max_retries {
        wire.max_retries = m;
    }
    if let Some(t) = req.expires_at {
        let ms = t
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let mut ts = Timestamp::default();
        ts.unix_millis = ms;
        wire.expires_at = Some(Box::new(ts));
    }
    if let Some(rc) = req.retry_config {
        let mut wire_rc = RetryConfig::default();
        wire_rc.initial_ms = rc.initial_ms;
        wire_rc.max_ms = rc.max_ms;
        wire_rc.coefficient = rc.coefficient;
        wire_rc.max_retries = rc.max_retries;
        wire.retry_config = Some(Box::new(wire_rc));
    }
    wire
}

/// Decode a `GetTaskResultResponse` into the SDK's `TaskState` shape.
/// Treats a missing `task` field (with no rejection envelope) as an
/// internal protocol violation — the server should never produce this.
fn decode_task_state(
    resp: taskq_proto::GetTaskResultResponse,
    fallback_id: &TaskId,
) -> Result<TaskState, ClientError> {
    if let Some(err) = resp.error.as_ref() {
        return Err(ClientError::Rejected {
            reason: err.reason,
            retry_after_ms: err.retry_after.as_ref().map(|t| t.unix_millis).unwrap_or(0),
            retryable: err.retryable,
            hint: err.hint.clone().unwrap_or_default(),
        });
    }
    let task = resp
        .task
        .as_ref()
        .ok_or_else(|| ClientError::Internal("GetTaskResultResponse missing task field".into()))?;
    let task_id = TaskId::new(
        task.task_id
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| fallback_id.as_str().to_owned()),
    );
    Ok(TaskState {
        task_id,
        status: task.status,
        outcome: TaskOutcome::from_wire(resp.outcome),
        result_payload: resp
            .result_payload
            .map(Bytes::from)
            .filter(|b| !b.is_empty()),
        failure: resp.failure.as_ref().map(|f| Failure {
            error_class: f.error_class.clone().unwrap_or_default(),
            message: f.message.clone().unwrap_or_default(),
            details: f.details.clone().map(Bytes::from).unwrap_or_default(),
            retryable: f.retryable,
        }),
    })
}

fn payload_hash_or_zero(bytes: Option<&[u8]>) -> [u8; 32] {
    let mut out = [0u8; 32];
    if let Some(b) = bytes {
        let n = b.len().min(32);
        out[..n].copy_from_slice(&b[..n]);
    }
    out
}

fn is_terminal(s: TerminalState) -> bool {
    matches!(
        s,
        TerminalState::COMPLETED
            | TerminalState::CANCELLED
            | TerminalState::FAILED_NONRETRYABLE
            | TerminalState::FAILED_EXHAUSTED
            | TerminalState::EXPIRED
    )
}

// ---------------------------------------------------------------------------
// Re-exports for convenience: callers should not need to depend on
// `taskq-proto` directly for the common types.
// ---------------------------------------------------------------------------

/// Re-exported from `taskq-proto` so callers can compute the canonical
/// payload hash themselves (e.g., for logging or pre-flight checks)
/// without depending on the proto crate.
pub fn payload_hash(bytes: &[u8]) -> [u8; 32] {
    hash::payload_hash(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_idempotency_keys_are_unique() {
        // Arrange / Act
        let a = generate_idempotency_key();
        let b = generate_idempotency_key();

        // Assert: UUIDv7 packs a millisecond timestamp + 74 random bits;
        // collisions in two consecutive calls are vanishingly unlikely.
        assert_ne!(a, b);
        // UUIDv7 string is 36 chars (8-4-4-4-12 hex with hyphens).
        assert_eq!(a.len(), 36);
    }

    #[test]
    fn generated_idempotency_keys_parse_as_uuid_v7() {
        // Arrange / Act
        let key = generate_idempotency_key();
        let parsed = uuid::Uuid::parse_str(&key).expect("UUIDv7 must parse");

        // Assert: get_version_num returns 7 for v7.
        assert_eq!(parsed.get_version_num(), 7);
    }

    #[test]
    fn payload_hash_matches_blake3_directly() {
        // Arrange
        let bytes = b"deadbeef";

        // Act
        let via_sdk = payload_hash(bytes);
        let via_blake3 = *blake3::hash(bytes).as_bytes();

        // Assert
        assert_eq!(via_sdk, via_blake3);
    }

    #[test]
    fn is_terminal_recognizes_all_five_terminal_states() {
        // Arrange / Act / Assert
        assert!(is_terminal(TerminalState::COMPLETED));
        assert!(is_terminal(TerminalState::CANCELLED));
        assert!(is_terminal(TerminalState::FAILED_NONRETRYABLE));
        assert!(is_terminal(TerminalState::FAILED_EXHAUSTED));
        assert!(is_terminal(TerminalState::EXPIRED));
        assert!(!is_terminal(TerminalState::PENDING));
        assert!(!is_terminal(TerminalState::DISPATCHED));
        assert!(!is_terminal(TerminalState::WAITING_RETRY));
    }
}
