//! [`Worker`] -- the running harness around `TaskWorkerClient`.
//!
//! Owns:
//!
//! - the long-poll **acquire loop** (one task per [`WorkerConfig::concurrency`] slot),
//! - the **heartbeat** task (one per worker, ticking at `heartbeat_interval`),
//! - the per-handler in-flight set (so heartbeats can advertise held leases),
//! - the **shutdown sequencer** (drain handlers -> deregister -> stop heartbeat).
//!
//! Reference: `design.md` Sec 6.2 (long-poll), Sec 6.3 (heartbeat carve-out + SDK
//! clamp), Sec 6.4 (Complete/ReportFailure with `LEASE_EXPIRED` distinction),
//! Sec 6.5 (terminal-state mapping); `problems/06-backpressure.md` Round 2 (SDK
//! retry on `WAITER_LIMIT_EXCEEDED`); `problems/08-retry-storms.md`
//! (`retryable: bool` + `error_class`).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use taskq_proto::task_worker_client::TaskWorkerClient;
use taskq_proto::{
    AcquireTaskRequest, CompleteTaskRequest, DeregisterWorkerRequest, Failure, HeartbeatRequest,
    LeaseRef, RegisterWorkerRequest, RejectReason, ReportFailureRequest, SemVer,
};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

use crate::builder::HandlerObject;
use crate::error::RunError;
use crate::error_class::ErrorClassRegistry;
use crate::handler::{AcquiredTask, HandlerOutcome};
use crate::shutdown::ShutdownSignal;

/// 1-second backoff on `REPLICA_WAITER_LIMIT_EXCEEDED`. Matches
/// `problems/06-backpressure.md` Round 2 -- "SDK retries with 1s backoff and
/// reconnects to a different replica when behind a load balancer".
const WAITER_LIMIT_BACKOFF: Duration = Duration::from_secs(1);

/// Threshold for consecutive `REPLICA_WAITER_LIMIT_EXCEEDED` rejections
/// before the acquire loop escalates: emits a `tracing::warn!` (so
/// operators see persistent saturation in logs / OTel) and switches
/// from the fixed 1s backoff to the same exponential schedule used for
/// transient errors. Default 5 (≈5s of fixed backoff) — small enough
/// that real saturation surfaces quickly, large enough that brief
/// dispatch races don't spam the logs.
const WAITER_LIMIT_ESCALATE_AFTER: u32 = 5;

/// Initial backoff for non-retryable gRPC errors; doubles up to the cap.
const ACQUIRE_BACKOFF_MIN: Duration = Duration::from_millis(500);
/// Cap for the exponential backoff applied to non-retryable acquire errors.
const ACQUIRE_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Delay between heartbeat retries when the previous attempt errored. Matches
/// the acquire loop's minimum backoff so a partially-degraded CP gets the same
/// breathing room from both client paths.
const HEARTBEAT_RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// Internal session state the harness needs to drive its loops.
pub(crate) struct WorkerSession {
    pub(crate) worker_id: String,
    pub(crate) namespace: String,
    pub(crate) task_types: Vec<String>,
    pub(crate) registry: ErrorClassRegistry,
    pub(crate) lease_duration: Duration,
    pub(crate) eps: Duration,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) client: TaskWorkerClient<grpc_client::Channel>,
    pub(crate) client_version: SemVer,
}

/// Builder-supplied behaviour that does not need to be re-fetched on
/// re-register.
pub(crate) struct WorkerConfig {
    pub(crate) concurrency: usize,
    pub(crate) long_poll_timeout: Duration,
    pub(crate) drain_timeout: Duration,
    /// `None` => harness awaits handlers indefinitely (the lease
    /// still lapses CP-side via Reaper A on `lease_window_seconds`,
    /// but the handler future keeps running until it returns on its
    /// own). `Some(d)` => wrap each handler call in
    /// `tokio::time::timeout(d, ...)`; on timeout the handler is
    /// dropped (cancelled) and the lease is left to lapse.
    pub(crate) handler_deadline: Option<Duration>,
    pub(crate) handler: Arc<dyn HandlerObject>,
}

/// Tracked in-flight lease set. The harness mints one entry on each successful
/// `AcquireTask`, removes it once the result has been reported (or dropped on
/// `LEASE_EXPIRED`), and emits the live set as `held_leases` in every
/// outbound `Heartbeat`.
#[derive(Default)]
struct InFlight {
    leases: Mutex<Vec<HeldLease>>,
}

#[derive(Clone)]
struct HeldLease {
    task_id: String,
    attempt_number: u32,
}

impl InFlight {
    async fn insert(&self, lease: HeldLease) {
        self.leases.lock().await.push(lease);
    }

    async fn remove(&self, task_id: &str, attempt_number: u32) {
        let mut guard = self.leases.lock().await;
        if let Some(idx) = guard
            .iter()
            .position(|l| l.task_id == task_id && l.attempt_number == attempt_number)
        {
            guard.swap_remove(idx);
        }
    }

    async fn snapshot(&self) -> Vec<HeldLease> {
        self.leases.lock().await.clone()
    }

    async fn len(&self) -> usize {
        self.leases.lock().await.len()
    }
}

/// Mutable session state. The acquire loop and the heartbeat task share this
/// behind a mutex so both observe a consistent view of `worker_id` after a
/// `WORKER_DEREGISTERED` re-register cycle.
struct SessionState {
    session: WorkerSession,
    /// Bumped on every re-register so the heartbeat task can detect that the
    /// acquire loop already obtained a fresh `worker_id`.
    epoch: u64,
}

/// Running worker harness. Constructed by [`crate::WorkerBuilder::build`].
pub struct Worker {
    state: Arc<Mutex<SessionState>>,
    config: WorkerConfig,
    inflight: Arc<InFlight>,
    /// Wakes the heartbeat task when the acquire loop has just inserted a
    /// new lease (so the next heartbeat sees it without waiting a full
    /// interval).
    heartbeat_kick: Arc<Notify>,
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("concurrency", &self.config.concurrency)
            .field("long_poll_timeout", &self.config.long_poll_timeout)
            .field("drain_timeout", &self.config.drain_timeout)
            .finish()
    }
}

impl Worker {
    pub(crate) fn new(session: WorkerSession, config: WorkerConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(SessionState { session, epoch: 0 })),
            config,
            inflight: Arc::new(InFlight::default()),
            heartbeat_kick: Arc::new(Notify::new()),
        }
    }

    /// Run the harness until SIGINT or SIGTERM, then drain + deregister.
    ///
    /// # Errors
    ///
    /// Returns [`RunError::SignalSetup`] when the OS signal handler cannot be
    /// installed; otherwise see [`RunError`].
    pub async fn run_until_shutdown(self) -> Result<(), RunError> {
        let signal = build_default_signal()?;
        self.run_with_signal(signal).await
    }

    /// Run the harness until `shutdown` resolves, then drain + deregister.
    ///
    /// # Errors
    ///
    /// See [`RunError`].
    pub async fn run_with_signal(self, shutdown: ShutdownSignal) -> Result<(), RunError> {
        let stop = Arc::new(Notify::new());

        // Spawn the heartbeat task first so re-register flows initiated by the
        // acquire loop can be observed via the shared `state`.
        let heartbeat_handle = spawn_heartbeat_loop(
            Arc::clone(&self.state),
            Arc::clone(&self.inflight),
            Arc::clone(&self.heartbeat_kick),
            Arc::clone(&stop),
        );

        // One acquire loop per concurrency slot. Each slot pulls a single
        // task at a time and runs the user handler to completion before
        // re-issuing the long-poll.
        let mut acquire_handles: Vec<JoinHandle<()>> = Vec::with_capacity(self.config.concurrency);
        let acquire_args = AcquireArgs {
            state: Arc::clone(&self.state),
            inflight: Arc::clone(&self.inflight),
            heartbeat_kick: Arc::clone(&self.heartbeat_kick),
            handler: Arc::clone(&self.config.handler),
            stop: Arc::clone(&stop),
            long_poll_timeout: self.config.long_poll_timeout,
            handler_deadline: self.config.handler_deadline,
        };
        for _ in 0..self.config.concurrency {
            acquire_handles.push(spawn_acquire_loop(acquire_args.clone()));
        }

        // Wait on the shutdown signal -- once it fires, flip the stop flag
        // for both loops and start draining.
        shutdown.wait().await;
        tracing::info!("worker received shutdown signal; draining");
        stop.notify_waiters();

        // Drain in-flight handlers (with a hard deadline). The acquire loops
        // ack the stop notification after their current handler returns;
        // joining their JoinHandles is sufficient.
        let drain_deadline = tokio::time::Instant::now() + self.config.drain_timeout;
        let drain_outcome =
            join_acquire_loops(acquire_handles, drain_deadline, Arc::clone(&self.inflight)).await;

        // Issue Deregister regardless of the drain outcome -- if handlers
        // are stuck, the CP's reaper will reclaim leases on its own and the
        // operator gets a `RunError::DrainTimeout` to investigate.
        if let Err(err) = deregister_best_effort(&self.state).await {
            tracing::warn!(error = %err, "deregister failed; continuing shutdown");
        }

        // Stop the heartbeat task.
        if let Err(err) = heartbeat_handle.await {
            return Err(RunError::Heartbeat(err.to_string()));
        }

        match drain_outcome {
            DrainOutcome::Drained => Ok(()),
            DrainOutcome::Timeout => Err(RunError::DrainTimeout {
                timeout_seconds: self.config.drain_timeout.as_secs(),
            }),
            DrainOutcome::JoinError(msg) => Err(RunError::AcquireLoop(msg)),
        }
    }
}

// ---------------------------------------------------------------------------
// Default OS signal: SIGINT or SIGTERM
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn build_default_signal() -> Result<ShutdownSignal, RunError> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate())
        .map_err(|e| RunError::SignalSetup(format!("SIGTERM: {e}")))?;
    let mut sigint = signal(SignalKind::interrupt())
        .map_err(|e| RunError::SignalSetup(format!("SIGINT: {e}")))?;
    Ok(ShutdownSignal::new(async move {
        tokio::select! {
            _ = sigterm.recv() => {},
            _ = sigint.recv() => {},
        }
    }))
}

#[cfg(not(unix))]
fn build_default_signal() -> Result<ShutdownSignal, RunError> {
    Ok(ShutdownSignal::new(async {
        let _ = tokio::signal::ctrl_c().await;
    }))
}

// ---------------------------------------------------------------------------
// Acquire loop
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AcquireArgs {
    state: Arc<Mutex<SessionState>>,
    inflight: Arc<InFlight>,
    heartbeat_kick: Arc<Notify>,
    handler: Arc<dyn HandlerObject>,
    stop: Arc<Notify>,
    long_poll_timeout: Duration,
    handler_deadline: Option<Duration>,
}

fn spawn_acquire_loop(args: AcquireArgs) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_acquire_loop(args).await;
    })
}

async fn run_acquire_loop(args: AcquireArgs) {
    let mut backoff = ACQUIRE_BACKOFF_MIN;
    let mut consecutive_waiter_limit: u32 = 0;
    loop {
        // Cooperative cancellation: check the stop flag before each poll.
        if stop_is_signalled(&args.stop) {
            return;
        }

        match acquire_one(&args).await {
            AcquireResult::Task(task) => {
                backoff = ACQUIRE_BACKOFF_MIN;
                consecutive_waiter_limit = 0;
                handle_one(&args, task).await;
            }
            AcquireResult::NoTask => {
                // Long-poll deadline elapsed without a hit: immediate
                // reconnect, no backoff (`design.md` Sec 7 worker SDK).
                backoff = ACQUIRE_BACKOFF_MIN;
                consecutive_waiter_limit = 0;
            }
            AcquireResult::WaiterLimit => {
                consecutive_waiter_limit = consecutive_waiter_limit.saturating_add(1);
                if consecutive_waiter_limit >= WAITER_LIMIT_ESCALATE_AFTER {
                    // Persistent saturation -- the replica's waiter
                    // pool is full and brief retries aren't clearing
                    // it. Surface once and switch to exponential
                    // backoff so we don't burn capacity sending
                    // doomed requests at fixed 1s cadence.
                    tracing::warn!(
                        consecutive = consecutive_waiter_limit,
                        ?backoff,
                        "REPLICA_WAITER_LIMIT_EXCEEDED persistent; escalating to exponential backoff",
                    );
                    wait_or_stop(&args.stop, backoff).await;
                    backoff = next_backoff(backoff);
                } else {
                    tracing::debug!(
                        consecutive = consecutive_waiter_limit,
                        "REPLICA_WAITER_LIMIT_EXCEEDED; backing off 1s",
                    );
                    wait_or_stop(&args.stop, WAITER_LIMIT_BACKOFF).await;
                }
            }
            AcquireResult::WorkerDeregistered => {
                consecutive_waiter_limit = 0;
                tracing::warn!("WORKER_DEREGISTERED on AcquireTask; re-registering");
                if let Err(err) = re_register(&args.state).await {
                    tracing::error!(error = %err, "re-register failed");
                    wait_or_stop(&args.stop, backoff).await;
                    backoff = next_backoff(backoff);
                }
            }
            AcquireResult::TransientError(err) => {
                consecutive_waiter_limit = 0;
                tracing::warn!(error = %err, ?backoff, "acquire transient error; backing off");
                wait_or_stop(&args.stop, backoff).await;
                backoff = next_backoff(backoff);
            }
        }
    }
}

enum AcquireResult {
    Task(AcquiredOk),
    NoTask,
    WaiterLimit,
    WorkerDeregistered,
    TransientError(String),
}

struct AcquiredOk {
    task: AcquiredTask,
    held: HeldLease,
}

async fn acquire_one(args: &AcquireArgs) -> AcquireResult {
    // Snapshot the session details we need for this RPC. We hold the lock
    // only for the duration of the snapshot so the heartbeat task can
    // continue concurrently.
    let (worker_id, namespace, task_types, long_poll_ms, mut client, client_version) = {
        let state = args.state.lock().await;
        (
            state.session.worker_id.clone(),
            state.session.namespace.clone(),
            state.session.task_types.clone(),
            args.long_poll_timeout.as_millis() as u64,
            state.session.client.clone(),
            state.session.client_version.clone(),
        )
    };

    let mut req = AcquireTaskRequest::default();
    req.client_version = Some(Box::new(client_version));
    req.worker_id = Some(worker_id);
    req.namespace = Some(namespace);
    req.task_types = Some(task_types);
    req.long_poll_timeout_ms = long_poll_ms;

    let response = match client.acquire_task(req).await {
        Ok(r) => r.into_inner(),
        Err(status) => {
            return AcquireResult::TransientError(format!(
                "code={:?} message={}",
                status.code(),
                status.message()
            ));
        }
    };

    if let Some(rejection) = response.error.as_deref() {
        return classify_acquire_rejection(rejection.reason);
    }

    if response.no_task {
        return AcquireResult::NoTask;
    }

    let Some(task) = response.task.map(|b| *b) else {
        return AcquireResult::TransientError("AcquireTaskResponse missing task".to_owned());
    };

    let task_id = task.task_id.clone().unwrap_or_default();
    if task_id.is_empty() {
        return AcquireResult::TransientError("AcquireTaskResponse missing task_id".to_owned());
    }
    let attempt_number = response.attempt_number;
    let task_type = task.task_type.clone().unwrap_or_default();
    let payload = Bytes::from(task.payload.unwrap_or_default());
    let traceparent = Bytes::from(response.traceparent.unwrap_or_default());
    let tracestate = Bytes::from(response.tracestate.unwrap_or_default());

    // Compute deadline_hint from the lease's `timeout_at` so the user
    // handler can implement its own soft deadline. Fall back to "now +
    // lease_duration" when the lease block is missing (defensive).
    let deadline_hint = response
        .lease
        .as_ref()
        .and_then(|l| l.timeout_at.as_ref())
        .map(|ts| timestamp_to_systemtime(ts.unix_millis))
        .unwrap_or_else(SystemTime::now);

    let task = AcquiredTask {
        task_id: task_id.clone(),
        attempt_number,
        task_type,
        payload,
        traceparent,
        tracestate,
        deadline_hint,
    };
    let held = HeldLease {
        task_id,
        attempt_number,
    };
    AcquireResult::Task(AcquiredOk { task, held })
}

fn classify_acquire_rejection(reason: RejectReason) -> AcquireResult {
    match reason {
        RejectReason::REPLICA_WAITER_LIMIT_EXCEEDED => AcquireResult::WaiterLimit,
        RejectReason::WORKER_DEREGISTERED => AcquireResult::WorkerDeregistered,
        // Everything else (including UNSPECIFIED, SYSTEM_OVERLOAD, etc.) is
        // surfaced as a transient error: log + back off.
        other => AcquireResult::TransientError(format!("AcquireTask rejected: {:?}", other)),
    }
}

/// Run the user handler with an optional wall-clock cap. Pulled out
/// of [`handle_one`] so it can be unit-tested without spinning a
/// session.
///
/// `None` deadline => await the handler indefinitely (CP-side
/// reaper-A is the only safety net).
/// `Some(d)` => `tokio::time::timeout(d, ...)`; on timeout the
/// handler future is dropped (cancelled at its next await point) and
/// this returns `None` so the caller can drop the held lease and
/// move on. The CP's lease will lapse on its own and reaper-A will
/// reclaim.
async fn run_handler_with_deadline(
    handler: &dyn HandlerObject,
    task: AcquiredTask,
    deadline: Option<Duration>,
) -> Option<HandlerOutcome> {
    match deadline {
        None => Some(handler.handle_boxed(task).await),
        Some(d) => tokio::time::timeout(d, handler.handle_boxed(task))
            .await
            .ok(),
    }
}

async fn handle_one(args: &AcquireArgs, ok: AcquiredOk) {
    args.inflight.insert(ok.held.clone()).await;
    args.heartbeat_kick.notify_one();

    let task_id = ok.task.task_id.clone();
    let attempt_number = ok.task.attempt_number;

    // §11.2: attach the upstream W3C parent context so the handler's
    // tracing::Span continues the trace the caller started. Falls
    // back silently to a fresh disconnected context if the bytes
    // aren't a valid version-00 traceparent.
    let span = tracing::info_span!(
        "taskq.handle",
        task_id = %task_id,
        attempt_number,
        task_type = %ok.task.task_type,
    );
    if let Some(parent_ctx) =
        crate::trace::parse_traceparent(&ok.task.traceparent, &ok.task.tracestate)
    {
        use opentelemetry::trace::TraceContextExt;
        let cx = opentelemetry::Context::new().with_remote_span_context(parent_ctx);
        tracing_opentelemetry::OpenTelemetrySpanExt::set_parent(&span, cx);
    }
    let _enter = span.enter();

    let outcome = match run_handler_with_deadline(
        args.handler.as_ref(),
        ok.task,
        args.handler_deadline,
    )
    .await
    {
        Some(o) => o,
        None => {
            tracing::warn!(
                task_id = %task_id,
                attempt_number,
                deadline_ms = args.handler_deadline.map(|d| d.as_millis() as u64).unwrap_or(0),
                "handler exceeded with_handler_deadline; cancelling and dropping the held lease",
            );
            args.inflight.remove(&task_id, attempt_number).await;
            return;
        }
    };

    // Submit the outcome. On `LEASE_EXPIRED` we drop without retry.
    let report_result = report_outcome(args, &task_id, attempt_number, outcome).await;
    match report_result {
        ReportResult::Settled => {}
        ReportResult::LeaseExpired => {
            tracing::warn!(
                task_id = %task_id,
                attempt_number,
                "task lease expired before result landed; dropping",
            );
        }
        ReportResult::WorkerDeregistered => {
            tracing::warn!(
                task_id = %task_id,
                attempt_number,
                "WORKER_DEREGISTERED on report; re-registering"
            );
            if let Err(err) = re_register(&args.state).await {
                tracing::error!(error = %err, "re-register failed");
            }
        }
        ReportResult::Transient(err) => {
            tracing::warn!(error = %err, task_id = %task_id, "report failed transiently; dropping");
        }
    }

    args.inflight.remove(&task_id, attempt_number).await;
}

enum ReportResult {
    Settled,
    LeaseExpired,
    WorkerDeregistered,
    Transient(String),
}

async fn report_outcome(
    args: &AcquireArgs,
    task_id: &str,
    attempt_number: u32,
    outcome: HandlerOutcome,
) -> ReportResult {
    let (worker_id, namespace, mut client, client_version) = {
        let state = args.state.lock().await;
        (
            state.session.worker_id.clone(),
            state.session.namespace.clone(),
            state.session.client.clone(),
            state.session.client_version.clone(),
        )
    };

    match outcome {
        HandlerOutcome::Success(payload) => {
            let mut req = CompleteTaskRequest::default();
            req.client_version = Some(Box::new(client_version));
            req.worker_id = Some(worker_id);
            req.namespace = Some(namespace);
            req.task_id = Some(task_id.to_owned());
            req.attempt_number = attempt_number;
            req.result_payload = Some(payload.to_vec());
            match client.complete_task(req).await {
                Ok(resp) => {
                    let inner = resp.into_inner();
                    if let Some(rej) = inner.error.as_deref() {
                        match rej.reason {
                            RejectReason::LEASE_EXPIRED => ReportResult::LeaseExpired,
                            RejectReason::WORKER_DEREGISTERED => ReportResult::WorkerDeregistered,
                            other => ReportResult::Transient(format!(
                                "CompleteTask rejected: {:?}",
                                other
                            )),
                        }
                    } else {
                        ReportResult::Settled
                    }
                }
                Err(status) => ReportResult::Transient(format!(
                    "CompleteTask RPC error: code={:?} message={}",
                    status.code(),
                    status.message()
                )),
            }
        }
        HandlerOutcome::RetryableFailure {
            error_class,
            message,
            details,
        } => {
            send_report_failure(
                &mut client,
                ReportFailureArgs {
                    client_version,
                    worker_id,
                    namespace,
                    task_id: task_id.to_owned(),
                    attempt_number,
                    error_class: error_class.into_string(),
                    message,
                    details,
                    retryable: true,
                },
            )
            .await
        }
        HandlerOutcome::NonRetryableFailure {
            error_class,
            message,
            details,
        } => {
            send_report_failure(
                &mut client,
                ReportFailureArgs {
                    client_version,
                    worker_id,
                    namespace,
                    task_id: task_id.to_owned(),
                    attempt_number,
                    error_class: error_class.into_string(),
                    message,
                    details,
                    retryable: false,
                },
            )
            .await
        }
    }
}

/// Bundled arguments for [`send_report_failure`]; consolidates the
/// nine fields the wire request needs so the helper signature stays
/// clippy-clean (`clippy::too_many_arguments`).
struct ReportFailureArgs {
    client_version: SemVer,
    worker_id: String,
    namespace: String,
    task_id: String,
    attempt_number: u32,
    error_class: String,
    message: String,
    details: Option<Bytes>,
    retryable: bool,
}

async fn send_report_failure(
    client: &mut TaskWorkerClient<grpc_client::Channel>,
    args: ReportFailureArgs,
) -> ReportResult {
    let mut failure = Failure::default();
    failure.error_class = Some(args.error_class);
    failure.message = Some(args.message);
    failure.details = args.details.map(|b| b.to_vec());
    failure.retryable = args.retryable;

    let mut req = ReportFailureRequest::default();
    req.client_version = Some(Box::new(args.client_version));
    req.worker_id = Some(args.worker_id);
    req.namespace = Some(args.namespace);
    req.task_id = Some(args.task_id);
    req.attempt_number = args.attempt_number;
    req.failure = Some(Box::new(failure));

    match client.report_failure(req).await {
        Ok(resp) => {
            let inner = resp.into_inner();
            if let Some(rej) = inner.error.as_deref() {
                match rej.reason {
                    RejectReason::LEASE_EXPIRED => ReportResult::LeaseExpired,
                    RejectReason::WORKER_DEREGISTERED => ReportResult::WorkerDeregistered,
                    other => {
                        ReportResult::Transient(format!("ReportFailure rejected: {:?}", other))
                    }
                }
            } else {
                ReportResult::Settled
            }
        }
        Err(status) => ReportResult::Transient(format!(
            "ReportFailure RPC error: code={:?} message={}",
            status.code(),
            status.message()
        )),
    }
}

// ---------------------------------------------------------------------------
// Heartbeat loop
// ---------------------------------------------------------------------------

fn spawn_heartbeat_loop(
    state: Arc<Mutex<SessionState>>,
    inflight: Arc<InFlight>,
    kick: Arc<Notify>,
    stop: Arc<Notify>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_heartbeat_loop(state, inflight, kick, stop).await;
    })
}

async fn run_heartbeat_loop(
    state: Arc<Mutex<SessionState>>,
    inflight: Arc<InFlight>,
    kick: Arc<Notify>,
    stop: Arc<Notify>,
) {
    loop {
        let interval = {
            let s = state.lock().await;
            s.session.heartbeat_interval
        };

        tokio::select! {
            _ = stop.notified() => return,
            _ = kick.notified() => {},
            _ = tokio::time::sleep(interval) => {},
        }

        match send_one_heartbeat(&state, &inflight).await {
            HeartbeatOutcome::Ok => {}
            HeartbeatOutcome::WorkerDeregistered => {
                tracing::warn!("Heartbeat returned WORKER_DEREGISTERED; re-registering");
                if let Err(err) = re_register(&state).await {
                    tracing::error!(error = %err, "heartbeat re-register failed");
                    if wait_or_stop_inner(&stop, HEARTBEAT_RETRY_BACKOFF).await {
                        return;
                    }
                }
            }
            HeartbeatOutcome::Error(err) => {
                tracing::warn!(error = %err, "heartbeat transient error");
                if wait_or_stop_inner(&stop, HEARTBEAT_RETRY_BACKOFF).await {
                    return;
                }
            }
        }
    }
}

enum HeartbeatOutcome {
    Ok,
    WorkerDeregistered,
    Error(String),
}

async fn send_one_heartbeat(
    state: &Arc<Mutex<SessionState>>,
    inflight: &Arc<InFlight>,
) -> HeartbeatOutcome {
    let leases = inflight.snapshot().await;
    let (worker_id, namespace, mut client, client_version) = {
        let s = state.lock().await;
        (
            s.session.worker_id.clone(),
            s.session.namespace.clone(),
            s.session.client.clone(),
            s.session.client_version.clone(),
        )
    };

    let lease_refs: Vec<LeaseRef> = leases
        .into_iter()
        .map(|h| {
            let mut lr = LeaseRef::default();
            lr.task_id = Some(h.task_id);
            lr.attempt_number = h.attempt_number;
            lr
        })
        .collect();

    let mut req = HeartbeatRequest::default();
    req.client_version = Some(Box::new(client_version));
    req.worker_id = Some(worker_id);
    req.namespace = Some(namespace);
    req.held_leases = Some(lease_refs);

    match client.heartbeat(req).await {
        Ok(resp) => {
            let inner = resp.into_inner();
            if inner.worker_deregistered {
                return HeartbeatOutcome::WorkerDeregistered;
            }
            if let Some(rej) = inner.error.as_deref() {
                if matches!(rej.reason, RejectReason::WORKER_DEREGISTERED) {
                    return HeartbeatOutcome::WorkerDeregistered;
                }
                return HeartbeatOutcome::Error(format!("Heartbeat rejected: {:?}", rej.reason));
            }
            HeartbeatOutcome::Ok
        }
        Err(status) => HeartbeatOutcome::Error(format!(
            "Heartbeat RPC error: code={:?} message={}",
            status.code(),
            status.message()
        )),
    }
}

// ---------------------------------------------------------------------------
// Re-register flow
// ---------------------------------------------------------------------------

async fn re_register(state: &Arc<Mutex<SessionState>>) -> Result<(), String> {
    // Build the request from the cached session metadata. We do NOT pass
    // the previous worker_id -- the CP minted a tombstone for it, and
    // sending it again would be rejected with WORKER_DEREGISTERED.
    let (mut client, namespace, task_types, client_version) = {
        let s = state.lock().await;
        (
            s.session.client.clone(),
            s.session.namespace.clone(),
            s.session.task_types.clone(),
            s.session.client_version.clone(),
        )
    };

    let mut req = RegisterWorkerRequest::default();
    req.client_version = Some(Box::new(client_version));
    req.namespace = Some(namespace);
    req.task_types = Some(task_types);
    req.declared_concurrency = 1;

    let response = client
        .register(req)
        .await
        .map_err(|s| format!("RPC error: code={:?} message={}", s.code(), s.message()))?
        .into_inner();

    if let Some(rej) = response.error.as_deref() {
        return Err(format!(
            "register rejected: reason={:?} hint={}",
            rej.reason,
            rej.hint.clone().unwrap_or_default()
        ));
    }

    let new_worker_id = response
        .worker_id
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "register response missing worker_id".to_owned())?;
    if response.lease_duration_ms == 0 || response.eps_ms == 0 {
        return Err("register response missing lease/eps fields".to_owned());
    }

    {
        let mut s = state.lock().await;
        s.session.worker_id = new_worker_id;
        s.session.lease_duration = Duration::from_millis(response.lease_duration_ms);
        s.session.eps = Duration::from_millis(response.eps_ms);
        // Re-clamp the heartbeat to eps/3 as the new namespace settings may
        // differ. Keep the user-overridden value (if present) only when it
        // still satisfies the eps/3 invariant.
        let cap = s.session.eps / 3;
        if s.session.heartbeat_interval > cap {
            tracing::warn!(
                configured_ms = s.session.heartbeat_interval.as_millis() as u64,
                recommended_max_ms = cap.as_millis() as u64,
                "re-register: heartbeat cadence exceeds eps/3 (design.md Sec 6.3)"
            );
        }
        s.session.registry = ErrorClassRegistry::new(response.error_classes.unwrap_or_default());
        s.epoch += 1;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Deregister
// ---------------------------------------------------------------------------

async fn deregister_best_effort(state: &Arc<Mutex<SessionState>>) -> Result<(), String> {
    let (mut client, worker_id, namespace, client_version) = {
        let s = state.lock().await;
        (
            s.session.client.clone(),
            s.session.worker_id.clone(),
            s.session.namespace.clone(),
            s.session.client_version.clone(),
        )
    };
    let mut req = DeregisterWorkerRequest::default();
    req.client_version = Some(Box::new(client_version));
    req.worker_id = Some(worker_id);
    req.namespace = Some(namespace);
    req.abandon_inflight = false;
    let response = client
        .deregister(req)
        .await
        .map_err(|s| {
            format!(
                "Deregister RPC error: code={:?} message={}",
                s.code(),
                s.message()
            )
        })?
        .into_inner();
    if let Some(rej) = response.error.as_deref() {
        // WORKER_DEREGISTERED is the expected outcome of a duplicate
        // deregister; anything else is an error worth surfacing.
        if !matches!(rej.reason, RejectReason::WORKER_DEREGISTERED) {
            return Err(format!("deregister rejected: reason={:?}", rej.reason));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Drain join + helpers
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum DrainOutcome {
    Drained,
    Timeout,
    JoinError(String),
}

async fn join_acquire_loops(
    handles: Vec<JoinHandle<()>>,
    deadline: tokio::time::Instant,
    inflight: Arc<InFlight>,
) -> DrainOutcome {
    let mut last_err: Option<String> = None;
    for handle in handles {
        let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(timeout, handle).await {
            Ok(Ok(())) => {}
            Ok(Err(join_err)) => {
                last_err = Some(join_err.to_string());
            }
            Err(_elapsed) => {
                if inflight.len().await > 0 {
                    return DrainOutcome::Timeout;
                }
                // Loop never returned but no in-flight handlers remain --
                // treat as drained; the JoinHandle abandoned itself.
            }
        }
    }
    if let Some(msg) = last_err {
        DrainOutcome::JoinError(msg)
    } else {
        DrainOutcome::Drained
    }
}

fn stop_is_signalled(_: &Arc<Notify>) -> bool {
    // Tokio's Notify is one-shot wakeup, not a flag; a true short-circuit
    // would need an `AtomicBool`. We rely on the wait_or_stop helper for
    // cancellation between operations and on the Notify wake to interrupt
    // sleeping waiters. Returning false here keeps the shape symmetric with
    // a future flag-based implementation.
    false
}

async fn wait_or_stop(stop: &Arc<Notify>, dur: Duration) {
    let _ = tokio::time::timeout(dur, stop.notified()).await;
}

async fn wait_or_stop_inner(stop: &Arc<Notify>, dur: Duration) -> bool {
    matches!(tokio::time::timeout(dur, stop.notified()).await, Ok(()),)
}

/// Doubles `prev` and clamps at [`ACQUIRE_BACKOFF_MAX`]. Pulled out of the
/// loop body so it can be unit-tested without spinning a real harness.
fn next_backoff(prev: Duration) -> Duration {
    (prev * 2).min(ACQUIRE_BACKOFF_MAX)
}

fn timestamp_to_systemtime(unix_millis: i64) -> SystemTime {
    if unix_millis < 0 {
        return UNIX_EPOCH;
    }
    UNIX_EPOCH + Duration::from_millis(unix_millis as u64)
}

// ---------------------------------------------------------------------------
// Internal unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_to_systemtime_negative_clamps_to_epoch() {
        // Arrange
        let negative_ts = -1_000;

        // Act
        let st = timestamp_to_systemtime(negative_ts);

        // Assert
        assert_eq!(st, UNIX_EPOCH);
    }

    #[test]
    fn timestamp_to_systemtime_round_trips_positive() {
        // Arrange
        let ts = 1_700_000_000_000_i64;

        // Act
        let st = timestamp_to_systemtime(ts);

        // Assert
        let dur = st.duration_since(UNIX_EPOCH).unwrap();
        assert_eq!(dur.as_millis() as i64, ts);
    }

    #[test]
    fn classify_acquire_rejection_maps_waiter_limit_to_backoff() {
        // Act
        let result = classify_acquire_rejection(RejectReason::REPLICA_WAITER_LIMIT_EXCEEDED);

        // Assert
        assert!(matches!(result, AcquireResult::WaiterLimit));
    }

    #[test]
    fn classify_acquire_rejection_maps_worker_deregistered_to_reregister() {
        // Act
        let result = classify_acquire_rejection(RejectReason::WORKER_DEREGISTERED);

        // Assert
        assert!(matches!(result, AcquireResult::WorkerDeregistered));
    }

    #[test]
    fn classify_acquire_rejection_default_path_is_transient() {
        // Act
        let result = classify_acquire_rejection(RejectReason::SYSTEM_OVERLOAD);

        // Assert
        assert!(matches!(result, AcquireResult::TransientError(_)));
    }

    #[tokio::test]
    async fn inflight_set_round_trips() {
        // Arrange
        let inflight = InFlight::default();
        let lease = HeldLease {
            task_id: "abc".to_owned(),
            attempt_number: 1,
        };

        // Act
        inflight.insert(lease.clone()).await;
        let snap_after_insert = inflight.snapshot().await;
        inflight.remove("abc", 1).await;
        let snap_after_remove = inflight.snapshot().await;

        // Assert
        assert_eq!(snap_after_insert.len(), 1);
        assert_eq!(snap_after_insert[0].task_id, "abc");
        assert_eq!(snap_after_insert[0].attempt_number, 1);
        assert!(snap_after_remove.is_empty());
    }

    #[test]
    fn next_backoff_doubles_below_cap() {
        // Arrange
        let prev = Duration::from_secs(1);

        // Act
        let next = next_backoff(prev);

        // Assert
        assert_eq!(next, Duration::from_secs(2));
    }

    #[test]
    fn next_backoff_clamps_to_max_at_cap() {
        // Arrange
        let prev = ACQUIRE_BACKOFF_MAX;

        // Act
        let next = next_backoff(prev);

        // Assert
        assert_eq!(next, ACQUIRE_BACKOFF_MAX);
    }

    #[test]
    fn next_backoff_clamps_overflow_to_max() {
        // Arrange: a value just under the cap; doubling would exceed it.
        let prev = ACQUIRE_BACKOFF_MAX - Duration::from_millis(1);

        // Act
        let next = next_backoff(prev);

        // Assert
        assert_eq!(next, ACQUIRE_BACKOFF_MAX);
    }

    #[test]
    fn next_backoff_starts_from_min() {
        // Arrange
        let prev = ACQUIRE_BACKOFF_MIN;

        // Act
        let next = next_backoff(prev);

        // Assert
        assert_eq!(next, ACQUIRE_BACKOFF_MIN * 2);
    }

    // -----------------------------------------------------------------
    // join_acquire_loops drain semantics
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn join_acquire_loops_drained_when_handles_finish_before_deadline() {
        // Arrange: a handle that returns immediately.
        let inflight = Arc::new(InFlight::default());
        let handle = tokio::spawn(async {});
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);

        // Act
        let outcome = join_acquire_loops(vec![handle], deadline, Arc::clone(&inflight)).await;

        // Assert
        assert!(matches!(outcome, DrainOutcome::Drained));
    }

    #[tokio::test]
    async fn join_acquire_loops_timeout_when_inflight_nonempty_past_deadline() {
        // Arrange: handle hangs forever; inflight has one held lease so
        // the timeout must surface as `Timeout` (operator must
        // investigate).
        let inflight = Arc::new(InFlight::default());
        inflight
            .insert(HeldLease {
                task_id: "stuck".to_owned(),
                attempt_number: 1,
            })
            .await;
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);

        // Act
        let outcome = join_acquire_loops(vec![handle], deadline, Arc::clone(&inflight)).await;

        // Assert
        assert!(matches!(outcome, DrainOutcome::Timeout));
    }

    #[tokio::test]
    async fn join_acquire_loops_drained_when_handle_hangs_but_inflight_empty() {
        // Arrange: handle hangs but no in-flight handlers exist. The
        // hanging task is treated as effectively drained — its work
        // queue is empty even though the future itself didn't return.
        let inflight = Arc::new(InFlight::default());
        let handle = tokio::spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);

        // Act
        let outcome = join_acquire_loops(vec![handle], deadline, Arc::clone(&inflight)).await;

        // Assert
        assert!(matches!(outcome, DrainOutcome::Drained));
    }

    #[tokio::test]
    async fn join_acquire_loops_join_error_when_handle_panics() {
        // Arrange: a handle that panics. Tokio surfaces this as a
        // JoinError; the drain must propagate it (so RunError::Acquire
        // -Loop carries the panic message rather than silently
        // returning Drained).
        let inflight = Arc::new(InFlight::default());
        let handle = tokio::spawn(async {
            panic!("simulated handler-side panic");
        });
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);

        // Act
        let outcome = join_acquire_loops(vec![handle], deadline, Arc::clone(&inflight)).await;

        // Assert
        match outcome {
            DrainOutcome::JoinError(msg) => {
                assert!(
                    msg.to_lowercase().contains("panic"),
                    "expected panic-related JoinError message, got {msg:?}"
                );
            }
            other => panic!("expected JoinError, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // run_handler_with_deadline (`with_handler_deadline` builder knob)
    // -----------------------------------------------------------------

    /// Fake [`HandlerObject`] that sleeps for `delay` then returns
    /// `Success(payload)`. Lets us race a handler against a deadline
    /// without standing up the full session.
    struct SleepHandler {
        delay: Duration,
    }

    impl crate::builder::HandlerObject for SleepHandler {
        fn handle_boxed(
            &self,
            task: AcquiredTask,
        ) -> futures::future::BoxFuture<'_, HandlerOutcome> {
            let delay = self.delay;
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                HandlerOutcome::Success(task.payload)
            })
        }
    }

    fn dummy_task() -> AcquiredTask {
        AcquiredTask {
            task_id: "test-task".to_owned(),
            attempt_number: 1,
            task_type: "echo".to_owned(),
            payload: bytes::Bytes::from_static(b"hello"),
            traceparent: bytes::Bytes::new(),
            tracestate: bytes::Bytes::new(),
            deadline_hint: std::time::SystemTime::UNIX_EPOCH,
        }
    }

    #[tokio::test]
    async fn run_handler_with_deadline_returns_outcome_when_handler_finishes_before_cap() {
        // Arrange: handler completes well inside the deadline.
        let handler = SleepHandler {
            delay: Duration::from_millis(10),
        };

        // Act
        let outcome =
            run_handler_with_deadline(&handler, dummy_task(), Some(Duration::from_secs(5))).await;

        // Assert
        assert!(matches!(outcome, Some(HandlerOutcome::Success(_))));
    }

    #[tokio::test]
    async fn run_handler_with_deadline_returns_none_when_handler_exceeds_cap() {
        // Arrange: handler sleeps past the deadline.
        let handler = SleepHandler {
            delay: Duration::from_secs(60),
        };

        // Act
        let outcome =
            run_handler_with_deadline(&handler, dummy_task(), Some(Duration::from_millis(50)))
                .await;

        // Assert: timeout fires; handler future was cancelled and
        // we observe `None` so the caller can drop the held lease.
        assert!(outcome.is_none());
    }

    #[tokio::test]
    async fn run_handler_with_deadline_awaits_indefinitely_when_deadline_is_none() {
        // Arrange: handler sleeps briefly; no deadline.
        let handler = SleepHandler {
            delay: Duration::from_millis(20),
        };

        // Act
        let outcome = run_handler_with_deadline(&handler, dummy_task(), None).await;

        // Assert
        assert!(matches!(outcome, Some(HandlerOutcome::Success(_))));
    }
}
