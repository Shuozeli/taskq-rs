//! gRPC service handler skeletons.
//!
//! Three modules, one per service group from `design.md` §3.2:
//!
//! - [`task_queue`] — caller-facing service (`TaskQueue`).
//! - [`task_worker`] — worker-facing service (`TaskWorker`).
//! - [`task_admin`] — operator-facing service (`TaskAdmin`) — Phase 5c.
//!
//! Phase 5b implements the lifecycle handlers (`tasks.md` §5.2) and the
//! long-poll machinery (`tasks.md` §5.6). Phase 5c implements the admin
//! handlers (§5.5) and the reapers (§5.4); Phase 5d wires observability
//! emit sites (Phase 6).
//!
//! Two private support modules:
//!
//! - [`cancel`] — `cancel_internal` shared helper used by `CancelTask` and
//!   `PurgeTasks` (`design.md` §6.7).
//! - [`retry`] — transparent `40001` retry helper used by every state-
//!   transition handler (`design.md` §6.4 / §6.6).

pub(crate) mod cancel;
pub(crate) mod retry;
pub mod task_admin;
pub mod task_queue;
pub mod task_worker;

pub use task_admin::TaskAdminHandler;
pub use task_queue::TaskQueueHandler;
pub use task_worker::TaskWorkerHandler;
