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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_code_connect_is_two() {
        let err = CliError::Connect {
            endpoint: "x".into(),
            message: "y".into(),
        };
        assert_eq!(err.exit_code(), 2);
    }

    #[test]
    fn exit_code_transport_is_three() {
        let err = CliError::Transport {
            endpoint: "x".into(),
            code: "Unavailable".into(),
            message: "y".into(),
        };
        assert_eq!(err.exit_code(), 3);
    }

    #[test]
    fn exit_code_rejected_is_four() {
        let err = CliError::Rejected {
            reason: RejectReason::SYSTEM_OVERLOAD,
            retry_after_ms: 0,
            retryable: false,
            hint: "drop".into(),
        };
        assert_eq!(err.exit_code(), 4);
    }

    #[test]
    fn exit_code_invalid_argument_is_sixty_four() {
        // Sixty-four mirrors EX_USAGE from sysexits(3) for CLI usage
        // errors -- shell scripts wrapping the CLI rely on this code
        // to detect "the user typed something wrong" vs server-side
        // failures.
        let err = CliError::InvalidArgument("missing flag".into());
        assert_eq!(err.exit_code(), 64);
    }

    #[test]
    fn exit_code_aborted_is_one_thirty() {
        // 130 == 128 + SIGINT. Treats operator decline as
        // ctrl-C-equivalent so wrapper scripts don't retry.
        let err = CliError::Aborted;
        assert_eq!(err.exit_code(), 130);
    }

    #[test]
    fn exit_code_internal_is_seventy() {
        // 70 == EX_SOFTWARE from sysexits(3). Reserved for
        // wire-protocol violations the CLI cannot recover from.
        let err = CliError::Internal("server returned no fields".into());
        assert_eq!(err.exit_code(), 70);
    }

    #[test]
    fn from_status_drops_endpoint_so_lift_transport_can_supply_it() {
        // Arrange: a raw `grpc_core::Status` only knows code + message.
        // The endpoint lives on the call site (which is what passes
        // it via `lift_transport`). `From<Status>` must leave the
        // endpoint empty so a later `lift_transport` can populate it.
        let s = grpc_core::Status::unavailable("connection lost");

        // Act
        let err: CliError = s.into();

        // Assert
        match err {
            CliError::Transport {
                endpoint, message, ..
            } => {
                assert_eq!(endpoint, "");
                assert_eq!(message, "connection lost");
            }
            other => panic!("expected Transport, got {other:?}"),
        }
    }
}
