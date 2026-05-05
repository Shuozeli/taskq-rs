//! [`WorkerBuilder`] -- constructs a [`crate::Worker`] by performing the
//! `Register` handshake against the control plane.
//!
//! Reference: `design.md` Sec 6.3 (`RegisterWorkerResponse` contract),
//! Sec 9.4 (typed error_class enum at SDK init).

use std::sync::Arc;
use std::time::Duration;

use http::Uri;
use taskq_proto::task_worker_client::TaskWorkerClient;
use taskq_proto::{Label, RegisterWorkerRequest, SemVer};

use crate::error::BuildError;
use crate::error_class::ErrorClassRegistry;
use crate::handler::TaskHandler;
use crate::runtime::{Worker, WorkerConfig, WorkerSession};

/// SemVer reported in every outbound RPC's `client_version` field. v1
/// initial; bumped at the same cadence as the wire schema. The owned wrapper
/// is `#[non_exhaustive]`, so we assemble via `default()` + field assignment.
pub(crate) fn client_semver() -> SemVer {
    let mut v = SemVer::default();
    v.major = 1;
    v.minor = 0;
    v.patch = 0;
    v
}

/// Default long-poll timeout per `tasks.md` Phase 7 ("30s timeout, immediate
/// reconnect on timeout"). Mirrors the CP server's default.
const DEFAULT_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Default per-handler concurrency. v1 ships single-task workers as the
/// least-surprising default; library users opt into more parallelism by
/// calling [`WorkerBuilder::with_concurrency`].
const DEFAULT_CONCURRENCY: usize = 1;

/// Default drain deadline for [`crate::Worker`]'s graceful shutdown. After
/// this many seconds, the harness fires `Deregister` even if handlers are
/// still running and surfaces [`crate::RunError::DrainTimeout`].
const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Builder for [`crate::Worker`]. Mandatory inputs: `uri`, `namespace`, and a
/// [`TaskHandler`]. Everything else has a sensible default.
pub struct WorkerBuilder {
    uri: Uri,
    namespace: String,
    task_types: Vec<String>,
    concurrency: usize,
    long_poll_timeout: Duration,
    heartbeat_interval: Option<Duration>,
    drain_timeout: Duration,
    handler_deadline: Option<Duration>,
    labels: Vec<(String, String)>,
    handler: Option<Arc<dyn HandlerObject>>,
}

impl WorkerBuilder {
    /// Construct a new builder. `uri` is the control-plane endpoint;
    /// `namespace` is the queue namespace this worker will service.
    pub fn new(uri: Uri, namespace: impl Into<String>) -> Self {
        Self {
            uri,
            namespace: namespace.into(),
            task_types: Vec::new(),
            concurrency: DEFAULT_CONCURRENCY,
            long_poll_timeout: DEFAULT_LONG_POLL_TIMEOUT,
            heartbeat_interval: None,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
            handler_deadline: None,
            labels: Vec::new(),
            handler: None,
        }
    }

    /// Set the task types this worker will accept. Unknown types are filtered
    /// server-side at `AcquireTask` time.
    #[must_use]
    pub fn with_task_types(mut self, types: Vec<String>) -> Self {
        self.task_types = types;
        self
    }

    /// Set the maximum concurrent in-flight tasks. Default 1.
    #[must_use]
    pub fn with_concurrency(mut self, n: usize) -> Self {
        // The harness invariant "at least one slot" is enforced by clamping
        // here so callers get the natural behaviour rather than a build-time
        // failure for `0`.
        self.concurrency = n.max(1);
        self
    }

    /// Override the heartbeat cadence. Default is `eps_ms / 3` from the
    /// `Register` response (`design.md` Sec 6.3 SDK clamp). Setting a value
    /// greater than `eps_ms / 3` triggers a startup `tracing::warn!` per
    /// `problems/01-lease-expiration.md` Round 2.
    #[must_use]
    pub fn with_heartbeat_interval(mut self, d: Duration) -> Self {
        self.heartbeat_interval = Some(d);
        self
    }

    /// Override the long-poll deadline. Default 30s; capped server-side at
    /// 60s.
    #[must_use]
    pub fn with_long_poll_timeout(mut self, d: Duration) -> Self {
        self.long_poll_timeout = d;
        self
    }

    /// Override the drain deadline used during graceful shutdown.
    /// Default 30s.
    #[must_use]
    pub fn with_drain_timeout(mut self, d: Duration) -> Self {
        self.drain_timeout = d;
        self
    }

    /// Cap the wall-clock duration the harness will wait on a single
    /// [`TaskHandler::handle`] invocation. When the deadline elapses
    /// the handler future is dropped (cancelled at the next await
    /// point), the runtime logs a warning, and the lease is left to
    /// lapse — Reaper A on the CP side reclaims it after
    /// `lease_window_seconds` and the next attempt redispatches.
    /// Without this knob the harness awaits handlers indefinitely;
    /// the lease still lapses CP-side, but the handler keeps running
    /// (and burning a worker slot) until it finishes on its own.
    ///
    /// Default `None` (no enforcement). Pick a value slightly under
    /// the namespace's `lease_window_seconds` so the wrapper triggers
    /// before the lease itself does. v1 doesn't compute this from
    /// `deadline_hint` automatically — that's a follow-up.
    #[must_use]
    pub fn with_handler_deadline(mut self, d: Duration) -> Self {
        self.handler_deadline = Some(d);
        self
    }

    /// Append a free-form label (key/value) to the `Register` request. Used
    /// by `ListWorkers` introspection.
    #[must_use]
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }

    /// Install the user's [`TaskHandler`].
    #[must_use]
    pub fn with_handler<H: TaskHandler + 'static>(mut self, h: H) -> Self {
        self.handler = Some(Arc::new(HandlerHolder { inner: h }));
        self
    }

    /// Connect, run `Register`, and return a ready-to-run [`Worker`].
    ///
    /// # Errors
    ///
    /// Returns [`BuildError`] when the gRPC connection or the `Register`
    /// RPC fails, when the response is incomplete, or when no
    /// [`TaskHandler`] was installed.
    pub async fn build(self) -> Result<Worker, BuildError> {
        let handler = self.handler.ok_or(BuildError::MissingHandler)?;

        let mut client = TaskWorkerClient::connect(self.uri.clone())
            .await
            .map_err(|e| BuildError::Connect(e.to_string()))?;

        let labels: Vec<Label> = self
            .labels
            .iter()
            .map(|(k, v)| {
                let mut l = Label::default();
                l.key = Some(k.clone());
                l.value = Some(v.clone());
                l
            })
            .collect();

        let mut req = RegisterWorkerRequest::default();
        req.client_version = Some(Box::new(client_semver()));
        req.namespace = Some(self.namespace.clone());
        req.task_types = Some(self.task_types.clone());
        // u32::MAX is reserved as "unspecified"; saturate at u32::MAX - 1
        // to keep the wire field in-band when callers ask for outsized
        // concurrency values.
        req.declared_concurrency = u32::try_from(self.concurrency).unwrap_or(u32::MAX - 1);
        req.labels = Some(labels);

        let response = client.register(req).await?.into_inner();

        if let Some(rejection) = response.error.as_deref() {
            let hint = rejection
                .hint
                .clone()
                .unwrap_or_else(|| format!("rejected: reason={:?}", rejection.reason));
            return Err(BuildError::RegisterRejected { hint });
        }

        let worker_id = response
            .worker_id
            .filter(|s| !s.is_empty())
            .ok_or(BuildError::IncompleteRegisterResponse("worker_id"))?;

        if response.lease_duration_ms == 0 {
            return Err(BuildError::IncompleteRegisterResponse("lease_duration_ms"));
        }
        if response.eps_ms == 0 {
            return Err(BuildError::IncompleteRegisterResponse("eps_ms"));
        }

        let eps = Duration::from_millis(response.eps_ms);
        let lease_duration = Duration::from_millis(response.lease_duration_ms);
        let registry = ErrorClassRegistry::new(response.error_classes.unwrap_or_default());

        // Heartbeat cadence: default eps/3 (Sec 6.3 SDK clamp), warn if
        // user override > eps/3.
        let heartbeat_interval = resolve_heartbeat_interval(eps, self.heartbeat_interval);

        let session = WorkerSession {
            worker_id,
            namespace: self.namespace,
            task_types: self.task_types,
            registry,
            lease_duration,
            eps,
            heartbeat_interval,
            client,
            client_version: client_semver(),
        };

        let config = WorkerConfig {
            concurrency: self.concurrency,
            long_poll_timeout: self.long_poll_timeout,
            drain_timeout: self.drain_timeout,
            handler_deadline: self.handler_deadline,
            handler,
        };

        Ok(Worker::new(session, config))
    }
}

/// Apply the `design.md` Sec 6.3 SDK clamp:
///
/// - When the user did not override the cadence, default to `eps / 3`.
/// - When the user overrode it above `eps / 3`, keep the user's value but
///   emit a startup warning (per `problems/01-lease-expiration.md` Round 2:
///   "operators overriding the cadence above ε/3 receive a startup warning").
///
/// Pure helper so it can be exercised without spinning up the harness.
pub(crate) fn resolve_heartbeat_interval(
    eps: Duration,
    user_override: Option<Duration>,
) -> Duration {
    let cap = eps / 3;
    match user_override {
        None => cap,
        Some(user) => {
            if user > cap {
                tracing::warn!(
                    configured_ms = user.as_millis() as u64,
                    recommended_max_ms = cap.as_millis() as u64,
                    eps_ms = eps.as_millis() as u64,
                    "worker heartbeat cadence exceeds eps/3 (design.md Sec 6.3); \
                     leases may lapse under load"
                );
            }
            user
        }
    }
}

// ---------------------------------------------------------------------------
// Object-safe handler boxing
// ---------------------------------------------------------------------------
//
// `TaskHandler::handle` returns `impl Future`, which is not object-safe.
// `HandlerObject` is the boxed-future trait the harness drives at runtime,
// and `HandlerHolder<H>` adapts any concrete `TaskHandler` to it.

use crate::handler::{AcquiredTask, HandlerOutcome};
use futures::future::BoxFuture;
use futures::FutureExt;

pub(crate) trait HandlerObject: Send + Sync {
    fn handle_boxed(&self, task: AcquiredTask) -> BoxFuture<'_, HandlerOutcome>;
}

struct HandlerHolder<H: TaskHandler> {
    inner: H,
}

impl<H: TaskHandler + 'static> HandlerObject for HandlerHolder<H> {
    fn handle_boxed(&self, task: AcquiredTask) -> BoxFuture<'_, HandlerOutcome> {
        self.inner.handle(task).boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_clamps_to_eps_over_three_when_user_did_not_override() {
        // Arrange
        let eps = Duration::from_millis(900);

        // Act
        let interval = resolve_heartbeat_interval(eps, None);

        // Assert
        assert_eq!(interval, Duration::from_millis(300));
    }

    #[test]
    fn heartbeat_keeps_user_override_below_cap_unchanged() {
        // Arrange
        let eps = Duration::from_millis(900);
        let user = Duration::from_millis(100);

        // Act
        let interval = resolve_heartbeat_interval(eps, Some(user));

        // Assert
        assert_eq!(interval, user);
    }

    #[test]
    fn heartbeat_keeps_user_override_at_cap_unchanged() {
        // Arrange: exact cap is allowed without warning.
        let eps = Duration::from_millis(900);
        let user = Duration::from_millis(300);

        // Act
        let interval = resolve_heartbeat_interval(eps, Some(user));

        // Assert
        assert_eq!(interval, user);
    }

    #[test]
    fn heartbeat_keeps_user_override_above_cap_but_does_not_clamp() {
        // Arrange: per problems/01 Round 2, the SDK warns but does NOT clamp
        // -- the operator's choice wins so they can debug a misconfigured
        // cluster without the harness silently rewriting the value.
        let eps = Duration::from_millis(900);
        let user = Duration::from_millis(500);

        // Act
        let interval = resolve_heartbeat_interval(eps, Some(user));

        // Assert
        assert_eq!(interval, user);
        assert!(interval > eps / 3);
    }

    /// Trivial handler used to exercise the [`HandlerObject`] adapter path.
    struct EchoHandler;
    impl TaskHandler for EchoHandler {
        async fn handle(&self, task: AcquiredTask) -> HandlerOutcome {
            HandlerOutcome::Success(task.payload)
        }
    }

    #[tokio::test]
    async fn handler_holder_dispatches_through_boxed_future() {
        // Arrange
        let holder = HandlerHolder { inner: EchoHandler };
        let task = AcquiredTask {
            task_id: "task-1".to_owned(),
            attempt_number: 1,
            task_type: "echo".to_owned(),
            payload: bytes::Bytes::from_static(b"hi"),
            traceparent: bytes::Bytes::new(),
            tracestate: bytes::Bytes::new(),
            deadline_hint: std::time::SystemTime::now(),
        };

        // Act
        let outcome = holder.handle_boxed(task).await;

        // Assert
        match outcome {
            HandlerOutcome::Success(bytes) => assert_eq!(&bytes[..], b"hi"),
            other => panic!("expected Success, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn build_without_handler_returns_missing_handler() {
        // Arrange: the URI doesn't need to be reachable -- the missing-handler
        // check fires before the connect attempt.
        let uri: http::Uri = "http://127.0.0.1:1".parse().unwrap();
        let builder = WorkerBuilder::new(uri, "ns");

        // Act
        let result = builder.build().await;

        // Assert: the missing-handler check is the first guard inside `build`.
        match result {
            Err(BuildError::MissingHandler) => {}
            Err(other) => panic!("expected MissingHandler, got {other:?}"),
            Ok(_) => panic!("expected MissingHandler, got Ok"),
        }
    }
}
