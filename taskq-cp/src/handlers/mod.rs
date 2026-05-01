//! gRPC service handler skeletons.
//!
//! Three modules, one per service group from `design.md` §3.2:
//!
//! - [`task_queue`] — caller-facing service (`TaskQueue`).
//! - [`task_worker`] — worker-facing service (`TaskWorker`).
//! - [`task_admin`] — operator-facing service (`TaskAdmin`).
//!
//! Phase 5a: every handler method is a `todo!()`. Phase 5b fills in the
//! lifecycle handlers (5.2 in `tasks.md`); Phase 5b's admin sub-task
//! fills in 5.5; Phase 5c handles long-poll wiring (5.6); Phase 5d wires
//! observability emit sites (Phase 6).

pub mod task_admin;
pub mod task_queue;
pub mod task_worker;

pub use task_admin::TaskAdminHandler;
pub use task_queue::TaskQueueHandler;
pub use task_worker::TaskWorkerHandler;
