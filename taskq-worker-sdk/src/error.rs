//! Public error types for the worker SDK.
//!
//! Two categories:
//!
//! - [`BuildError`] -- raised by [`crate::WorkerBuilder::build`] when the
//!   handshake with the control plane fails (connect, register, schema).
//! - [`RunError`] -- raised by [`crate::Worker::run_until_shutdown`] /
//!   [`crate::Worker::run_with_signal`] when the harness exits abnormally.

use grpc_core::Status;
use thiserror::Error;

/// Failure mode during [`crate::WorkerBuilder::build`].
///
/// `Register` semantics live in `design.md` Sec 6.3. Treat
/// `RegisterRejected { reason: WorkerDeregistered }` as a programmer error -- it
/// can only happen when the SDK is given a previously-tombstoned `worker_id`,
/// which the public surface never exposes.
#[derive(Debug, Error)]
pub enum BuildError {
    /// The supplied [`http::Uri`] is invalid or the gRPC handshake failed.
    #[error("failed to connect to control plane: {0}")]
    Connect(String),

    /// The `Register` RPC returned a non-OK gRPC status.
    #[error("register RPC failed: code={code:?} message={message}")]
    RegisterRpc {
        /// gRPC status code returned by the control plane.
        code: grpc_core::Code,
        /// Human-readable status message.
        message: String,
    },

    /// The `Register` response carried a structured rejection.
    #[error("register rejected: {hint}")]
    RegisterRejected {
        /// Server hint string (free-form).
        hint: String,
    },

    /// `Register` succeeded but the response was missing fields the harness
    /// requires (`worker_id`, `lease_duration_ms`, `eps_ms`).
    #[error("register response is incomplete: {0}")]
    IncompleteRegisterResponse(&'static str),

    /// No [`crate::TaskHandler`] was supplied via
    /// [`crate::WorkerBuilder::with_handler`].
    #[error("WorkerBuilder requires a TaskHandler; call with_handler before build")]
    MissingHandler,
}

impl From<Status> for BuildError {
    fn from(status: Status) -> Self {
        BuildError::RegisterRpc {
            code: status.code(),
            message: status.message().to_owned(),
        }
    }
}

/// Failure mode raised by [`crate::Worker::run_until_shutdown`] /
/// [`crate::Worker::run_with_signal`].
///
/// The acquire loop and heartbeat task tolerate transient errors internally
/// (logged + retried with backoff). `RunError` only surfaces when the harness
/// itself cannot continue -- e.g. the shutdown drain timed out.
#[derive(Debug, Error)]
pub enum RunError {
    /// The acquire-loop runtime panicked or was joined with an error.
    #[error("acquire loop terminated: {0}")]
    AcquireLoop(String),

    /// The heartbeat task panicked or was joined with an error.
    #[error("heartbeat task terminated: {0}")]
    Heartbeat(String),

    /// In-flight handlers did not drain within the configured deadline.
    #[error("graceful shutdown drain timed out after {timeout_seconds}s")]
    DrainTimeout {
        /// The drain deadline in seconds.
        timeout_seconds: u64,
    },

    /// Failed to install the OS signal handler used by `run_until_shutdown`.
    #[error("failed to install signal handler: {0}")]
    SignalSetup(String),
}
