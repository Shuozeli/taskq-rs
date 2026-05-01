//! Caller-facing `TaskQueue` service handler.
//!
//! `design.md` §3.2 lists the methods on this service. Phase 5a leaves
//! every method body as `todo!()`; Phase 5b implements the lifecycle
//! handlers (`SubmitTask`, `GetTaskResult`, `BatchGetTaskResults`,
//! `CancelTask`, `SubmitAndWait`).
//!
//! ## Why no `pure-grpc-rs` service-trait `impl` in Phase 5a
//!
//! `taskq-proto` does not yet ship the generated `TaskQueueServer` /
//! `TaskQueue` service trait — the FlatBuffers codegen emits message types
//! only, not gRPC service stubs. Phase 5b will either (a) extend the
//! `taskq-proto` `build.rs` to emit service stubs via `grpc-build`'s
//! `flatbuffers` feature, or (b) hand-write the service trait alongside
//! the handler in `taskq-proto::v1`. Either way the *handler struct* lives
//! here, and Phase 5b's only change to this file is wiring the methods.

use std::sync::Arc;

use crate::state::CpState;

/// Caller-facing `TaskQueue` service handler.
///
/// Holds an `Arc<CpState>` so that a single instance can be cloned cheaply
/// into each in-flight request. The actual cloning happens inside the
/// `pure-grpc-rs` `tower::Service` adapter; Phase 5b wires that adapter.
#[derive(Clone)]
pub struct TaskQueueHandler {
    pub state: Arc<CpState>,
}

impl TaskQueueHandler {
    pub fn new(state: Arc<CpState>) -> Self {
        Self { state }
    }

    // -----------------------------------------------------------------
    // Phase 5b lifecycle handlers (tasks.md §5.2)
    // -----------------------------------------------------------------

    /// `SubmitTask` per `design.md` §6.1: admitter → idempotency lookup →
    /// retry-config resolution → insert.
    pub async fn submit_task(&self) {
        // TODO(phase-5b): implement design.md §6.1 lifecycle.
        todo!("phase-5b: TaskQueue::SubmitTask")
    }

    /// `GetTaskResult` — read a task's terminal-state result by `task_id`.
    pub async fn get_task_result(&self) {
        todo!("phase-5b: TaskQueue::GetTaskResult")
    }

    /// `BatchGetTaskResults` — batched form of `GetTaskResult`.
    pub async fn batch_get_task_results(&self) {
        todo!("phase-5b: TaskQueue::BatchGetTaskResults")
    }

    /// `CancelTask` per `design.md` §6.7. Uses the shared `cancel_internal`
    /// helper that `PurgeTasks` also calls.
    pub async fn cancel_task(&self) {
        todo!("phase-5b: TaskQueue::CancelTask")
    }

    /// `SubmitAndWait` — `SubmitTask` followed by an internal long-poll on
    /// the result.
    pub async fn submit_and_wait(&self) {
        todo!("phase-5b: TaskQueue::SubmitAndWait")
    }
}
