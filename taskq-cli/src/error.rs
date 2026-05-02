//! Unified error envelope for the operator CLI.
//!
//! Every command returns [`CliError`]; `main` maps it to a non-zero exit
//! code with a single structured stderr line. The variants mirror the
//! caller SDK's `ClientError` shape so operators see consistent error
//! semantics across the two surfaces:
//!
//! - **Connect** — transport handshake failed before any RPC was sent.
//! - **Transport** — gRPC `Status` from the wire (network, framing,
//!   `UNAVAILABLE`, `PERMISSION_DENIED`, etc.).
//! - **Rejected** — the server returned a structured `Rejection`.
//! - **InvalidArgument** — caller-side validation failed before any RPC
//!   was issued (e.g., non-matching `--confirm-namespace`).
//! - **Aborted** — the operator declined the interactive confirmation
//!   prompt; not a failure but exits non-zero so scripts that wrap the
//!   CLI distinguish "abort" from "success".
//! - **Internal** — wire-protocol violation (server returned a response
//!   shape that the protocol forbids).

use taskq_proto::RejectReason;

/// Error type returned by every CLI command. `main` converts the variants
/// into distinct non-zero exit codes via [`CliError::exit_code`].
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// Transport-level connect failure (DNS, TCP, TLS, HTTP/2 handshake).
    #[error("failed to connect to taskq control plane at {endpoint}: {message}")]
    Connect { endpoint: String, message: String },

    /// gRPC `Status` from the wire — server-internal failure or the
    /// rejection envelope was not present.
    #[error("transport error from {endpoint}: {message} ({code})")]
    Transport {
        endpoint: String,
        code: String,
        message: String,
    },

    /// Server returned a structured rejection. The operator usually wants
    /// to see `reason` + `hint`; `retryable` and `retry_after_ms` are
    /// surfaced for completeness.
    #[error("server rejected request: reason={reason:?} retryable={retryable} hint={hint:?}")]
    Rejected {
        reason: RejectReason,
        retry_after_ms: i64,
        retryable: bool,
        hint: String,
    },

    /// Caller-side validation failed before any RPC was issued.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The operator declined an interactive confirmation prompt.
    #[error("aborted by operator")]
    Aborted,

    /// Wire-protocol violation: the server produced a response shape that
    /// the protocol forbids.
    #[error("internal protocol error: {0}")]
    Internal(String),
}

impl CliError {
    /// Process exit code for this error variant. Distinct codes let
    /// shell scripts that wrap the CLI distinguish failure modes
    /// without parsing stderr.
    pub fn exit_code(&self) -> i32 {
        match self {
            CliError::Connect { .. } => 2,
            CliError::Transport { .. } => 3,
            CliError::Rejected { .. } => 4,
            CliError::InvalidArgument(_) => 64,
            CliError::Aborted => 130,
            CliError::Internal(_) => 70,
        }
    }
}

impl From<grpc_core::Status> for CliError {
    fn from(s: grpc_core::Status) -> Self {
        CliError::Transport {
            endpoint: String::new(),
            code: format!("{:?}", s.code()),
            message: s.message().to_owned(),
        }
    }
}
