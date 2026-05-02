//! User-facing trait + value types delivered to a [`TaskHandler`].
//!
//! The harness invokes [`TaskHandler::handle`] for each acquired task, awaits
//! the returned [`HandlerOutcome`], and translates the outcome into a
//! `CompleteTask` or `ReportFailure` RPC (`design.md` Sec 6.4-6.5).

use std::future::Future;
use std::time::SystemTime;

use bytes::Bytes;

use crate::error_class::ErrorClass;

/// User-supplied trait that processes one acquired task.
///
/// Implementations must be `Send + Sync` because the harness runs up to
/// `concurrency` handlers in parallel from a shared `Arc<H>` (see
/// `WorkerBuilder::with_concurrency`).
pub trait TaskHandler: Send + Sync {
    /// Process one task. The returned future is awaited inside the harness;
    /// once it resolves, the harness submits the outcome to the control
    /// plane.
    ///
    /// Cancelling the harness during shutdown drops the future after the
    /// per-handler drain deadline elapses.
    fn handle(&self, task: AcquiredTask) -> impl Future<Output = HandlerOutcome> + Send;
}

/// Three-way result reported by a [`TaskHandler`].
///
/// Mapping (`design.md` Sec 6.4-6.5):
///
/// - [`HandlerOutcome::Success`] -> `CompleteTaskRequest { result_payload }`
/// - [`HandlerOutcome::RetryableFailure`] -> `ReportFailureRequest { retryable: true }`
/// - [`HandlerOutcome::NonRetryableFailure`] -> `ReportFailureRequest { retryable: false }`
#[derive(Debug, Clone)]
pub enum HandlerOutcome {
    /// The task succeeded. `result_payload` is opaque application bytes.
    Success(
        /// Result payload to ship in `CompleteTaskRequest::result_payload`.
        Bytes,
    ),

    /// The task failed but the failure mode is retryable. The CP will
    /// transition the task into `WAITING_RETRY` and reschedule per the
    /// task's resolved retry config (full jitter).
    RetryableFailure {
        /// Validated `error_class` from [`crate::ErrorClassRegistry`].
        error_class: ErrorClass,
        /// Human-readable failure message.
        message: String,
        /// Optional opaque failure payload (<= namespace.max_details_bytes).
        details: Option<Bytes>,
    },

    /// The task failed with a non-retryable error. The CP will move the task
    /// straight to `FAILED_NONRETRYABLE`.
    NonRetryableFailure {
        /// Validated `error_class` from [`crate::ErrorClassRegistry`].
        error_class: ErrorClass,
        /// Human-readable failure message.
        message: String,
        /// Optional opaque failure payload (<= namespace.max_details_bytes).
        details: Option<Bytes>,
    },
}

/// Task delivered to a [`TaskHandler`].
///
/// Built from `AcquireTaskResponse` (`design.md` Sec 6.2). The handler is
/// expected to inspect `payload`, do work, and return a [`HandlerOutcome`].
/// Trace context is forwarded from the persisted W3C `traceparent`/`tracestate`
/// (Sec 11.2) so that downstream spans link back to the submit-side trace.
#[derive(Debug, Clone)]
pub struct AcquiredTask {
    /// UUIDv7 of the task. Stable across retries.
    pub task_id: String,
    /// 1-indexed retry attempt number (`design.md` Sec 5.4).
    pub attempt_number: u32,
    /// Task type the worker registered for.
    pub task_type: String,
    /// Application-defined payload bytes.
    pub payload: Bytes,
    /// W3C `traceparent` bytes from submit time. Empty if the caller did not
    /// provide one.
    pub traceparent: Bytes,
    /// W3C `tracestate` bytes from submit time. Empty if absent.
    pub tracestate: Bytes,
    /// Best-effort deadline by which the handler should finish before the
    /// CP-assigned lease expires. The harness will continue heartbeating to
    /// extend the lease while the handler runs, but a runaway handler may
    /// see the lease lapse and a sibling worker pick up the next attempt.
    pub deadline_hint: SystemTime,
}
