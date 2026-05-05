//! taskq-worker-sdk: ergonomic worker harness around `task_worker_client::TaskWorkerClient`.
//!
//! Phase 7 (worker SDK) per `tasks.md` and `design.md` Sec 6.2-6.6, 9.4, 11.2.
//!
//! ## Layered surface
//!
//! - [`WorkerBuilder`] -- configuration entry point. Validates the URI, registers
//!   with the control plane, fetches the namespace's lease window / lazy-extension
//!   threshold (`eps_ms`) / error-class registry, and constructs a [`Worker`].
//! - [`TaskHandler`] -- the user-supplied trait that processes one [`AcquiredTask`]
//!   and returns a [`HandlerOutcome`].
//! - [`Worker`] -- the running harness. Owns the long-poll acquire loop, the
//!   heartbeat task, and the graceful-shutdown sequencer. Run it via
//!   [`Worker::run_until_shutdown`] (catches SIGTERM/SIGINT) or
//!   [`Worker::run_with_signal`] (caller-driven).
//!
//! ## Wire semantics
//!
//! All RPC verbs map 1:1 to `taskq_proto::task_worker_client::TaskWorkerClient`.
//! The harness layers the Sec 5/6/9 invariants on top:
//!
//! | invariant | enforcement                                                              |
//! |-----------|--------------------------------------------------------------------------|
//! | heartbeat | default cadence clamped to `eps_ms / 3`; warn if user override > eps/3   |
//! | LEASE_EXPIRED | `Complete` / `ReportFailure` log + drop, never retry                 |
//! | WORKER_DEREGISTERED | re-`Register`, mint a fresh `worker_id`, resume both loops      |
//! | REPLICA_WAITER_LIMIT_EXCEEDED | acquire backs off 1s, drops + reconnects channel        |
//! | UNKNOWN_ERROR_CLASS | exposed at handler-construction time via `ErrorClass` newtype     |
//! | trace context | persisted `traceparent`/`tracestate` returned by `AcquireTask`     |
//!
//! ## Hello world
//!
//! ```no_run
//! use std::time::Duration;
//! use taskq_worker_sdk::{
//!     AcquiredTask, BuildError, HandlerOutcome, RunError, TaskHandler, WorkerBuilder,
//! };
//!
//! struct EchoHandler;
//!
//! impl TaskHandler for EchoHandler {
//!     async fn handle(&self, task: AcquiredTask) -> HandlerOutcome {
//!         HandlerOutcome::Success(task.payload)
//!     }
//! }
//!
//! async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!     let uri: http::Uri = "http://127.0.0.1:50051".parse()?;
//!     let worker = WorkerBuilder::new(uri, "billing")
//!         .with_task_types(vec!["charge".to_owned()])
//!         .with_concurrency(4)
//!         .with_long_poll_timeout(Duration::from_secs(30))
//!         .with_handler(EchoHandler)
//!         .build()
//!         .await?;
//!     worker.run_until_shutdown().await?;
//!     Ok(())
//! }
//! ```

#![deny(missing_docs)]

mod builder;
mod error;
mod error_class;
mod handler;
mod runtime;
mod shutdown;
mod trace;

pub use builder::WorkerBuilder;
pub use error::{BuildError, RunError};
pub use error_class::{ErrorClass, ErrorClassRegistry, UnknownErrorClass};
pub use handler::{AcquiredTask, HandlerOutcome, TaskHandler};
pub use runtime::Worker;
pub use shutdown::ShutdownSignal;

/// Re-export of the upstream task identifier wrapper. Worker SDK callers
/// receive `TaskId` values inside [`AcquiredTask`] and pass them into log
/// statements / metrics, so we avoid forcing them to depend on `taskq-proto`
/// directly.
pub type TaskId = String;
