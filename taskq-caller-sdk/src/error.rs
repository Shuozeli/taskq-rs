//! Error types surfaced by the caller SDK.
//!
//! `ClientError` is the unified return type for every public RPC method on
//! `CallerClient`. It distinguishes:
//!
//! - **Connect** — the underlying transport refused at handshake time.
//! - **Transport** — a gRPC `Status` from the wire (network / framing /
//!   server-internal failures); the rejection envelope was *not* present.
//! - **Rejected** — the server returned a structured `Rejection` per
//!   `design.md` §10.1. Callers route on `retryable` to decide whether
//!   to back off or fail fast. The SDK has *already* exhausted its
//!   internal retry budget when this is surfaced.
//! - **InvalidArgument** — caller-side validation failed before any RPC
//!   was issued (e.g., empty namespace with no default set).
//! - **Internal** — a defensive bucket for response shapes the server
//!   should not produce (e.g., `task` field absent on a successful
//!   `GetTaskResult`). Indicates a wire-protocol violation, not a
//!   transient failure.

use std::fmt;

use taskq_proto::RejectReason;

/// Error returned by [`crate::CallerClient::connect`] when the underlying
/// transport handshake fails. Wraps the boxed error from `pure-grpc-rs`'s
/// `Channel::connect` so callers do not have to depend on the transport
/// crate directly.
#[derive(Debug, thiserror::Error)]
#[error("failed to connect to taskq control plane: {message}")]
pub struct ConnectError {
    pub(crate) message: String,
}

impl ConnectError {
    pub(crate) fn from_boxed(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self {
            message: err.to_string(),
        }
    }
}

/// Unified error returned by every `CallerClient` RPC method.
#[derive(Debug)]
pub enum ClientError {
    /// Transport-level connect failure.
    Connect(ConnectError),

    /// gRPC `Status` from the wire — network failure, framing error,
    /// `UNAVAILABLE`, or any non-rejection-envelope server failure.
    Transport(grpc_core::Status),

    /// The server returned a structured rejection. The SDK has already
    /// exhausted its internal retry budget if `retryable` is `true`,
    /// otherwise the SDK never retried.
    Rejected {
        reason: RejectReason,
        /// Unix-epoch milliseconds. The server's "do not retry before
        /// this wall-clock time" floor. Zero when the rejection is
        /// permanent (`retryable = false`).
        retry_after_ms: i64,
        retryable: bool,
        hint: String,
    },

    /// Caller-side argument validation failed before any RPC was issued.
    InvalidArgument(String),

    /// Wire-protocol violation: a response shape the server should not
    /// have produced.
    Internal(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Connect(e) => write!(f, "{e}"),
            ClientError::Transport(s) => {
                write!(f, "transport error: {} ({})", s.message(), s.code())
            }
            ClientError::Rejected {
                reason,
                retry_after_ms,
                retryable,
                hint,
            } => write!(
                f,
                "server rejected request: reason={:?} retryable={} retry_after_ms={} hint={:?}",
                reason, retryable, retry_after_ms, hint
            ),
            ClientError::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
            ClientError::Internal(m) => write!(f, "internal error: {m}"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ClientError::Connect(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ConnectError> for ClientError {
    fn from(e: ConnectError) -> Self {
        ClientError::Connect(e)
    }
}

impl From<grpc_core::Status> for ClientError {
    fn from(s: grpc_core::Status) -> Self {
        ClientError::Transport(s)
    }
}
