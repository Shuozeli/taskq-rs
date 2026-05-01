//! Worker-facing `TaskWorker` service handler.
//!
//! `design.md` §3.2 / §6.2 / §6.3 / §6.4. Phase 5a stubs every method;
//! Phase 5b/5c fill them in (5b for `Register`/`Deregister`/`CompleteTask`/
//! `ReportFailure`, 5c for the `AcquireTask` long-poll machinery and the
//! READ COMMITTED `Heartbeat` carve-out).

use std::sync::Arc;

use crate::state::CpState;

/// Worker-facing `TaskWorker` service handler.
#[derive(Clone)]
pub struct TaskWorkerHandler {
    pub state: Arc<CpState>,
}

impl TaskWorkerHandler {
    pub fn new(state: Arc<CpState>) -> Self {
        Self { state }
    }

    // -----------------------------------------------------------------
    // Phase 5b lifecycle handlers (tasks.md §5.2)
    // -----------------------------------------------------------------

    /// `Register` per `design.md` §6.3 (RegisterWorkerResponse contract):
    /// returns `worker_id`, `lease_duration_ms`, `eps_ms`, and the
    /// namespace's registered `error_classes` set.
    pub async fn register(&self) {
        todo!("phase-5b: TaskWorker::Register")
    }

    /// `AcquireTask` per `design.md` §6.2 (long-poll, single task).
    /// Phase 5c wires the per-replica waiter pool + subscribe-pending
    /// integration.
    pub async fn acquire_task(&self) {
        todo!("phase-5c: TaskWorker::AcquireTask (long-poll)")
    }

    /// `Heartbeat` per `design.md` §6.3 (READ COMMITTED carve-out + lazy
    /// lease extension).
    pub async fn heartbeat(&self) {
        todo!("phase-5b: TaskWorker::Heartbeat")
    }

    /// `CompleteTask` per `design.md` §6.4 (idempotent on (task_id,
    /// attempt_number); transparent 40001 retry).
    pub async fn complete_task(&self) {
        todo!("phase-5b: TaskWorker::CompleteTask")
    }

    /// `ReportFailure` per `design.md` §6.4 + §6.5 (terminal-state mapping,
    /// server-side backoff math, error_class validation).
    pub async fn report_failure(&self) {
        todo!("phase-5b: TaskWorker::ReportFailure")
    }

    /// `Deregister` — clean shutdown for a worker.
    pub async fn deregister(&self) {
        todo!("phase-5b: TaskWorker::Deregister")
    }
}
