//! gRPC server bootstrap.
//!
//! Wires the `pure-grpc-rs` server: builds the [`Router`], installs the
//! version-handshake interceptor + auth interceptor + trace-context
//! extraction, and serves the gRPC handlers under graceful-shutdown.
//!
//! Phase 5a constructs the server and the interceptors. The actual handler
//! `tower::Service` adapters (the `TaskQueueServer` / `TaskWorkerServer` /
//! `TaskAdminServer` wrappers around `pure-grpc-rs`'s `UnaryService`) are
//! Phase 5b — they require gRPC service stubs that are not yet generated
//! by `taskq-proto`. The `serve` function below is the single entry point
//! `main` calls; Phase 5b will plug the wrappers into the `Router`.

use std::sync::Arc;

use grpc_core::{Request, Status};
use grpc_server::{Router, Server};
use taskq_proto::task_admin_server::TaskAdminServer;
use taskq_proto::task_queue_server::TaskQueueServer;
use taskq_proto::task_worker_server::TaskWorkerServer;

use crate::error::{CpError, Result};
use crate::handlers::{TaskAdminHandler, TaskQueueHandler, TaskWorkerHandler};
use crate::shutdown::{wait_for_shutdown, ShutdownReceiver};
use crate::state::CpState;

/// Wire-protocol major version this binary speaks. Mismatched majors are
/// rejected by [`version_handshake_interceptor`].
///
/// Phase 5a hard-codes `1`; Phase 5b will reconcile this with the
/// `taskq-proto` `SemVer` table once the wire-version handshake message
/// type is exchanged on the first RPC.
pub const SUPPORTED_MAJOR: u32 = 1;

/// HTTP header name for the client's wire SemVer string. The value is
/// `MAJOR.MINOR.PATCH`. Per `design.md` §3.3 the version is embedded in
/// the FlatBuffers request body; the header form is used by interceptors
/// that run before the body is decoded so we can short-circuit obviously
/// incompatible clients without paying a decode cost.
pub const CLIENT_VERSION_HEADER: &str = "x-taskq-client-version";

/// HTTP header name for a caller token. v1 default is token-based auth;
/// the interceptor always accepts a present token (the "real" validation
/// is a v0.2 follow-up per the prompt). Phase 5b replaces the placeholder
/// with a pluggable validator.
pub const AUTH_HEADER: &str = "authorization";

/// Phase 9b helper: build the same `Router` that [`serve`] uses but bind to
/// an OS-assigned port (`127.0.0.1:0`), spawn the server task, and return
/// the bound `SocketAddr` plus the `JoinHandle`. Integration tests use this
/// to stand up a CP that listens on a random port without going through the
/// `CpConfig::bind_addr` path (which would force the test to know the port
/// in advance).
///
/// The caller owns the returned `JoinHandle`; aborting it tears the server
/// down.
pub async fn serve_in_process(
    state: Arc<CpState>,
    shutdown: ShutdownReceiver,
) -> Result<(std::net::SocketAddr, tokio::task::JoinHandle<()>)> {
    let task_queue = TaskQueueHandler::new(Arc::clone(&state));
    let task_worker = TaskWorkerHandler::new(Arc::clone(&state));
    let task_admin = TaskAdminHandler::new(Arc::clone(&state));

    let router = Router::new()
        .add_service("taskq.v1.TaskQueue", TaskQueueServer::new(task_queue))
        .add_service("taskq.v1.TaskWorker", TaskWorkerServer::new(task_worker))
        .add_service("taskq.v1.TaskAdmin", TaskAdminServer::new(task_admin));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|err| CpError::transport(err.to_string()))?;
    let addr = listener
        .local_addr()
        .map_err(|err| CpError::transport(err.to_string()))?;

    let handle = tokio::spawn(async move {
        // The pure-grpc-rs server's public surface that takes a pre-bound
        // listener (`serve_with_listener`) does not accept a shutdown
        // signal, so we race the listener-bound serve future against the
        // shutdown receiver and drop it on shutdown. Dropping cancels the
        // accept loop, which is the same effect `serve_with_shutdown`
        // achieves with its internal `tokio::select!`.
        let serve_fut = Server::builder().serve_with_listener(listener, router);
        tokio::select! {
            res = serve_fut => {
                if let Err(err) = res {
                    tracing::warn!(error = %err, "in-process gRPC server exited with error");
                }
            }
            _ = wait_for_shutdown(shutdown) => {
                tracing::info!("in-process gRPC server shutting down");
            }
        }
    });

    Ok((addr, handle))
}

/// Build and serve the gRPC router. Returns when the shutdown signal
/// fires.
///
/// Phase 5a wiring:
///
/// 1. Build a `Router` with no services attached (Phase 5b adds the
///    `TaskQueueServer` / `TaskWorkerServer` / `TaskAdminServer`
///    `tower::Service` wrappers once the FlatBuffers service stubs land).
/// 2. Apply the three interceptors (version-handshake, auth, trace-context)
///    by composing `InterceptedService` for each registered service. Phase
///    5a only wires the version-handshake stub because no services are
///    registered yet; Phase 5b layers auth + trace-context on top.
/// 3. Serve with a graceful-shutdown future plumbed from `shutdown::ShutdownReceiver`.
pub async fn serve(state: Arc<CpState>, shutdown: ShutdownReceiver) -> Result<()> {
    let bind = state.config.bind_addr;

    // Phase 5c: all three services are wired into the router.
    let task_queue = TaskQueueHandler::new(Arc::clone(&state));
    let task_worker = TaskWorkerHandler::new(Arc::clone(&state));
    let task_admin = TaskAdminHandler::new(Arc::clone(&state));

    let router = Router::new()
        .add_service("taskq.v1.TaskQueue", TaskQueueServer::new(task_queue))
        .add_service("taskq.v1.TaskWorker", TaskWorkerServer::new(task_worker))
        .add_service("taskq.v1.TaskAdmin", TaskAdminServer::new(task_admin));

    tracing::info!(addr = %bind, "starting taskq-cp gRPC server");

    Server::builder()
        .serve_with_shutdown(bind, router, wait_for_shutdown(shutdown))
        .await
        .map_err(|err| CpError::transport(err.to_string()))?;

    tracing::info!("taskq-cp gRPC server shut down cleanly");
    Ok(())
}

// ---------------------------------------------------------------------------
// Interceptors
// ---------------------------------------------------------------------------

/// Build the version-handshake interceptor. Reads `CLIENT_VERSION_HEADER`,
/// rejects clients whose major != [`SUPPORTED_MAJOR`].
///
/// Returns `Status::failed_precondition("INCOMPATIBLE_VERSION: ...")` per
/// `design.md` §3.3 / `problems/09`. Within compatible majors the
/// interceptor is a no-op.
pub fn version_handshake_interceptor(
) -> impl Fn(Request<()>) -> std::result::Result<Request<()>, Status> + Clone + Send + Sync + 'static
{
    |req: Request<()>| {
        // The FlatBuffers `SemVer` table is in the request body; we read
        // the convenience header here so we can fail fast before decoding.
        // Clients without the header pass through and Phase 5b will
        // surface the embedded SemVer's mismatch from the body.
        if let Some(value) = req.metadata().get(CLIENT_VERSION_HEADER) {
            let parsed = value.to_str().ok().and_then(parse_semver_major);
            if let Some(major) = parsed {
                if major != SUPPORTED_MAJOR {
                    return Err(Status::failed_precondition(format!(
                        "INCOMPATIBLE_VERSION: server speaks v{SUPPORTED_MAJOR}.x; client claims v{major}.x"
                    )));
                }
            }
        }
        Ok(req)
    }
}

/// Build the auth interceptor. Phase 5a accepts any non-empty
/// `authorization` header value (the real validator is v0.2). Empty/missing
/// metadata is *also* accepted in Phase 5a so a tester can hit the server
/// without bearing a token; Phase 5b will tighten this once a configurable
/// validator lands.
pub fn auth_interceptor(
) -> impl Fn(Request<()>) -> std::result::Result<Request<()>, Status> + Clone + Send + Sync + 'static
{
    |req: Request<()>| {
        // TODO(phase-5b): validate the token via a pluggable trait.
        // Phase 5a accepts everything so end-to-end smoke tests work.
        if let Some(value) = req.metadata().get(AUTH_HEADER) {
            if value.to_str().map(|s| s.is_empty()).unwrap_or(true) {
                return Err(Status::unauthenticated("authorization header is empty"));
            }
        }
        Ok(req)
    }
}

/// Build the trace-context extractor. Reads W3C `traceparent` / `tracestate`
/// off the request metadata and stashes them into the request `Extensions`
/// for handler code to pick up. Phase 5d will wire the actual span
/// continuation (`tracing-opentelemetry`); Phase 5a just pulls the bytes
/// off the wire so handlers see them in `Extensions`.
pub fn trace_context_interceptor(
) -> impl Fn(Request<()>) -> std::result::Result<Request<()>, Status> + Clone + Send + Sync + 'static
{
    |mut req: Request<()>| {
        let traceparent = req
            .metadata()
            .get("traceparent")
            .and_then(|v| v.to_str().ok().map(|s| s.to_owned()));
        let tracestate = req
            .metadata()
            .get("tracestate")
            .and_then(|v| v.to_str().ok().map(|s| s.to_owned()));
        if traceparent.is_some() || tracestate.is_some() {
            req.extensions_mut().insert(WireTraceContext {
                traceparent,
                tracestate,
            });
        }
        Ok(req)
    }
}

/// Lightweight carrier inserted into request `Extensions` by
/// [`trace_context_interceptor`]. Phase 5d turns this into an OTel span
/// link or parent-context resumption.
#[derive(Debug, Clone)]
pub struct WireTraceContext {
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

/// Best-effort SemVer major parser. Only the major component is
/// load-bearing for the handshake; minor / patch are read out of the
/// FlatBuffers body in Phase 5b.
fn parse_semver_major(s: &str) -> Option<u32> {
    let major_part = s.split('.').next()?;
    major_part.parse::<u32>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use grpc_core::Request;

    #[test]
    fn parses_major_from_full_semver() {
        // Arrange / Act / Assert
        assert_eq!(parse_semver_major("1.0.0"), Some(1));
        assert_eq!(parse_semver_major("2.5.7"), Some(2));
        assert_eq!(parse_semver_major("notasemver"), None);
        assert_eq!(parse_semver_major(""), None);
    }

    #[test]
    fn version_interceptor_rejects_major_mismatch() {
        // Arrange
        let interceptor = version_handshake_interceptor();
        let mut req: Request<()> = Request::new(());
        req.metadata_mut()
            .insert(CLIENT_VERSION_HEADER, "2.0.0".parse().unwrap());

        // Act
        let result = interceptor(req);

        // Assert
        let status = result.unwrap_err();
        assert!(status.message().contains("INCOMPATIBLE_VERSION"));
    }

    #[test]
    fn version_interceptor_accepts_matching_major() {
        // Arrange
        let interceptor = version_handshake_interceptor();
        let mut req: Request<()> = Request::new(());
        req.metadata_mut()
            .insert(CLIENT_VERSION_HEADER, "1.99.99".parse().unwrap());

        // Act
        let result = interceptor(req);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn version_interceptor_passes_when_header_absent() {
        // Arrange
        let interceptor = version_handshake_interceptor();
        let req: Request<()> = Request::new(());

        // Act
        let result = interceptor(req);

        // Assert: no header => pass through. Phase 5b will read the body
        // SemVer and reject mismatches there.
        assert!(result.is_ok());
    }

    #[test]
    fn auth_interceptor_rejects_empty_header() {
        // Arrange
        let interceptor = auth_interceptor();
        let mut req: Request<()> = Request::new(());
        req.metadata_mut().insert(AUTH_HEADER, "".parse().unwrap());

        // Act
        let result = interceptor(req);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn trace_context_interceptor_stashes_extension() {
        // Arrange
        let interceptor = trace_context_interceptor();
        let mut req: Request<()> = Request::new(());
        req.metadata_mut().insert(
            "traceparent",
            "00-0af7651916cd43dd8448eb211c80319c-b9c7c989f97918e1-01"
                .parse()
                .unwrap(),
        );

        // Act
        let intercepted = interceptor(req).unwrap();

        // Assert
        let ext = intercepted
            .extensions()
            .get::<WireTraceContext>()
            .expect("traceparent should be stashed in extensions");
        assert!(ext.traceparent.is_some());
        assert!(ext.tracestate.is_none());
    }
}
