//! Worker-facing `TaskWorker` service handler.
//!
//! Implements `task_worker_server::TaskWorker` from [`taskq_proto`]: the six
//! worker-facing RPCs `Register`, `AcquireTask`, `Heartbeat`, `CompleteTask`,
//! `ReportFailure`, `Deregister`. Reference: `design.md` §3.2, §6.2-6.6.
//!
//! Same retry + isolation discipline as `TaskQueueHandler`:
//!
//! - Most methods open a SERIALIZABLE transaction via
//!   `state.storage.begin_dyn()` and wrap the body in
//!   `crate::handlers::retry::with_serializable_retry`.
//! - `Heartbeat` is the §6.3 carve-out — the heartbeat path is READ
//!   COMMITTED via `crate::state::StorageTxDyn::record_worker_heartbeat`
//!   (the storage trait routes that to a cheap UPSERT). Lazy lease
//!   extension fires a
//!   SERIALIZABLE write only when more than `(lease − ε)` has elapsed since
//!   the last extension.
//! - `AcquireTask` is the §6.2 long-poll — the handler subscribes to
//!   `subscribe_pending`, calls the dispatcher, registers as a waiter, and
//!   races wake / belt-and-suspenders / long-poll-deadline.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use grpc_core::{Request, Response, Status};
use opentelemetry::KeyValue;
use taskq_proto::task_worker_server::TaskWorker;
use taskq_proto::{
    AcquireTaskRequest, AcquireTaskResponse, CompleteTaskRequest, CompleteTaskResponse,
    DeregisterWorkerRequest, DeregisterWorkerResponse, HeartbeatRequest, HeartbeatResponse, Lease,
    LeaseRef as WireLeaseRef, RegisterWorkerRequest, RegisterWorkerResponse, RejectReason,
    Rejection, ReportFailureRequest, ReportFailureResponse, RetryConfig as WireRetryConfig, SemVer,
    Task, TerminalState, Timestamp as WireTimestamp,
};
use taskq_storage::{
    HeartbeatAck, LeaseRef, Namespace, NewLease, RateDecision, RateKind, StorageError, TaskOutcome,
    TaskType, Timestamp, WorkerId,
};

use crate::handlers::retry::with_serializable_retry;
use crate::state::{CpState, WakeOutcome};

/// Worker-facing `TaskWorker` service handler.
#[derive(Clone)]
pub struct TaskWorkerHandler {
    pub state: Arc<CpState>,
}

impl TaskWorkerHandler {
    pub fn new(state: Arc<CpState>) -> Self {
        Self { state }
    }
}

impl TaskWorker for TaskWorkerHandler {
    fn register(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<RegisterWorkerResponse>, Status>>
                + Send,
        >,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            register_impl(state, req).await.map(Response::new)
        })
    }

    fn acquire_task(
        &self,
        request: Request<AcquireTaskRequest>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Response<AcquireTaskResponse>, Status>> + Send>,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            acquire_task_impl(state, req).await.map(Response::new)
        })
    }

    fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Response<HeartbeatResponse>, Status>> + Send>,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            heartbeat_impl(state, req).await.map(Response::new)
        })
    }

    fn complete_task(
        &self,
        request: Request<CompleteTaskRequest>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<CompleteTaskResponse>, Status>> + Send,
        >,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            complete_task_impl(state, req).await.map(Response::new)
        })
    }

    fn report_failure(
        &self,
        request: Request<ReportFailureRequest>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<ReportFailureResponse>, Status>>
                + Send,
        >,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            report_failure_impl(state, req).await.map(Response::new)
        })
    }

    fn deregister(
        &self,
        request: Request<DeregisterWorkerRequest>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<DeregisterWorkerResponse>, Status>>
                + Send,
        >,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            deregister_impl(state, req).await.map(Response::new)
        })
    }
}

// ---------------------------------------------------------------------------
// Register (design.md §6.3 RegisterWorkerResponse contract)
// ---------------------------------------------------------------------------

#[tracing::instrument(
    name = "taskq.register_worker",
    skip_all,
    fields(namespace = req.namespace.as_deref().unwrap_or("")),
)]
async fn register_impl(
    state: Arc<CpState>,
    req: RegisterWorkerRequest,
) -> Result<RegisterWorkerResponse, Status> {
    let namespace_str = req
        .namespace
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("namespace is required"))?
        .to_owned();
    let namespace = Namespace::new(namespace_str.clone());
    let now = current_timestamp();
    let metrics = state.metrics.clone();

    // Generate or reuse worker_id. If the SDK is re-Registering after a
    // WORKER_DEREGISTERED, it sends the previously-issued id; otherwise we
    // mint a fresh UUIDv7 (`design.md` §6.3 RegisterWorkerResponse contract).
    let worker_id = if let Some(existing) = req.worker_id.as_deref().filter(|s| !s.is_empty()) {
        match parse_worker_id(existing) {
            Ok(id) => id,
            Err(_) => WorkerId::generate(),
        }
    } else {
        WorkerId::generate()
    };

    // Read the namespace's lease + ε settings + error_class registry. v1's
    // SQLite/Postgres backends both implement `get_namespace_quota` returning
    // a defaults-populated row.
    let result = with_serializable_retry(&metrics, "register_worker", || {
        let state = Arc::clone(&state);
        let namespace = namespace.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            // UPSERT into worker_heartbeats. Even though the §6.3 carve-out
            // routes heartbeats through READ COMMITTED, register lives on
            // the SERIALIZABLE side because it co-commits with the
            // namespace-quota read.
            let ack = tx
                .record_worker_heartbeat(&worker_id, &namespace, now)
                .await?;
            if matches!(ack, HeartbeatAck::WorkerDeregistered) {
                // The previously-issued worker_id was tombstoned; force the
                // SDK to take the freshly-generated one by treating this as
                // a normal first registration with a new id. Roll back and
                // try again.
                let _ = tx.rollback_dyn().await;
                return Ok(RegisterFlow::Tombstoned);
            }
            // Best-effort: read namespace quota for lease/ε settings. If the
            // namespace is not yet provisioned, treat as "use defaults".
            let quota = match tx.get_namespace_quota(&namespace).await {
                Ok(q) => Some(Box::new(q)),
                Err(StorageError::NotFound) => None,
                Err(other) => return Err(other),
            };
            tx.commit_dyn().await?;
            Ok(RegisterFlow::Registered(worker_id, quota))
        }
    })
    .await;

    let resp = match result {
        Ok(RegisterFlow::Registered(id, quota)) => {
            let lease_duration_ms = u64::from(state.config.lease_window_seconds) * 1_000;
            let (eps_ms, min_heartbeat_interval_ms, error_classes) = match quota {
                Some(q) => (
                    u64::from(q.lazy_extension_threshold_seconds) * 1_000,
                    u64::from(q.min_heartbeat_interval_seconds) * 1_000,
                    Vec::<String>::new(),
                ),
                None => (
                    // Default ε = 25% of lease (design.md §6.3, §9.1
                    // invariant validates ε ≥ 2 × min_heartbeat_interval).
                    lease_duration_ms / 4,
                    1_000,
                    Vec::<String>::new(),
                ),
            };
            metrics
                .workers_registered
                .add(1, &[KeyValue::new("namespace", namespace_str.clone())]);
            let mut resp = RegisterWorkerResponse::default();
            resp.worker_id = Some(id.to_string());
            resp.server_version = Some(Box::new(server_semver()));
            resp.lease_duration_ms = lease_duration_ms;
            resp.eps_ms = eps_ms;
            resp.min_heartbeat_interval_ms = min_heartbeat_interval_ms;
            resp.error_classes = Some(error_classes);
            resp
        }
        Ok(RegisterFlow::Tombstoned) => {
            let mut resp = RegisterWorkerResponse::default();
            resp.server_version = Some(Box::new(server_semver()));
            resp.error_classes = Some(Vec::new());
            resp.error = Some(Box::new(rejection_with_reason(
                RejectReason::WORKER_DEREGISTERED,
                "worker_id was previously declared dead; re-Register without it",
            )));
            resp
        }
        Err(err) => {
            tracing::error!(error = %err, "register_worker storage error");
            return Err(Status::internal("storage error during Register"));
        }
    };
    Ok(resp)
}

enum RegisterFlow {
    Registered(WorkerId, Option<Box<taskq_storage::NamespaceQuota>>),
    Tombstoned,
}

// ---------------------------------------------------------------------------
// AcquireTask (design.md §6.2 long-poll)
// ---------------------------------------------------------------------------

#[tracing::instrument(
    name = "taskq.acquire_task",
    skip_all,
    fields(
        namespace = req.namespace.as_deref().unwrap_or(""),
        worker_id = req.worker_id.as_deref().unwrap_or(""),
    ),
)]
async fn acquire_task_impl(
    state: Arc<CpState>,
    req: AcquireTaskRequest,
) -> Result<AcquireTaskResponse, Status> {
    let namespace_str = req
        .namespace
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("namespace is required"))?
        .to_owned();
    let namespace = Namespace::new(namespace_str.clone());
    let task_types: Vec<TaskType> = req
        .task_types
        .unwrap_or_default()
        .into_iter()
        .map(TaskType::new)
        .collect();
    let worker_id_str = req
        .worker_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
        .to_owned();
    let worker_id = parse_worker_id(&worker_id_str)
        .map_err(|err| Status::invalid_argument(format!("invalid worker_id: {err}")))?;

    let timeout = effective_long_poll_timeout(&state, req.long_poll_timeout_ms);
    let belt = Duration::from_secs(u64::from(state.config.belt_and_suspenders_seconds));
    let deadline = tokio::time::Instant::now() + timeout;
    let started_at = tokio::time::Instant::now();
    let metrics = state.metrics.clone();

    // Worker-liveness validation lives inside the dispatch loop so the
    // SERIALIZABLE transaction sees the same `worker_heartbeats` row each
    // attempt. We open a short verification transaction first; if the
    // worker has been declared dead, return WORKER_DEREGISTERED.
    if let Err(resp) = validate_worker_alive(&state, &worker_id, &namespace).await {
        return Ok(resp);
    }

    // Register-as-waiter (per-replica cap). If the cap is hit, surface
    // REPLICA_WAITER_LIMIT_EXCEEDED.
    let waiter = match state
        .waiter_pool
        .register_waiter(namespace.clone(), task_types.clone())
        .await
    {
        Ok(handle) => handle,
        Err(_) => {
            metrics.rejection_total.add(
                1,
                &[
                    KeyValue::new("namespace", namespace_str.clone()),
                    KeyValue::new("reason", "REPLICA_WAITER_LIMIT_EXCEEDED"),
                ],
            );
            return Ok(no_task_with_rejection(
                RejectReason::REPLICA_WAITER_LIMIT_EXCEEDED,
                "per-replica waiter cap reached",
            ));
        }
    };

    // Long-poll loop: subscribe-then-query (`design.md` §6.2 step 1-4),
    // single-waiter wake on signals, 10s belt-and-suspenders timer, and a
    // hard long-poll deadline.
    loop {
        // Step 2/3: try the dispatcher inside a fresh SERIALIZABLE transaction.
        let dispatch_outcome = try_dispatch(
            Arc::clone(&state),
            namespace.clone(),
            task_types.clone(),
            worker_id,
        )
        .await;
        match dispatch_outcome {
            Ok(Some(DispatchHit { resp, dispatch })) => {
                let waited = started_at.elapsed().as_secs_f64();
                let task_type_label = dispatch.task_type.as_str().to_owned();
                metrics.dispatch_total.add(
                    1,
                    &[
                        KeyValue::new("namespace", namespace_str.clone()),
                        KeyValue::new("task_type", task_type_label),
                    ],
                );
                // Histograms drop `task_type` per §11.3.
                metrics
                    .long_poll_wait_seconds
                    .record(waited, &[KeyValue::new("namespace", namespace_str.clone())]);
                if let Some(submit_to_dispatch_s) = dispatch.submit_to_dispatch_seconds {
                    metrics.dispatch_latency_seconds.record(
                        submit_to_dispatch_s,
                        &[KeyValue::new("namespace", namespace_str.clone())],
                    );
                }
                return Ok(resp);
            }
            Ok(None) => {} // fall through to wait
            Err(err) => {
                tracing::error!(error = %err, "acquire_task dispatch error");
                return Err(Status::internal("storage error during AcquireTask"));
            }
        }

        // Step 4: wait. `wait_for_wake_or_timeout` races wake notifications
        // against the belt-and-suspenders timer. We compute the remaining
        // long-poll budget on each iteration so multiple belt fires inside
        // a single `AcquireTask` are honoured.
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_else(|| Duration::from_millis(0));
        if remaining.is_zero() {
            // Long-poll timed out without a hit. Record the wait time for
            // observability — operators use this to size pool concurrency.
            metrics.long_poll_wait_seconds.record(
                started_at.elapsed().as_secs_f64(),
                &[KeyValue::new("namespace", namespace_str)],
            );
            return Ok(no_task_response());
        }
        match waiter.wait_for_wake_or_timeout(remaining, belt).await {
            WakeOutcome::Woken | WakeOutcome::BeltAndSuspenders => continue,
            WakeOutcome::Timeout => {
                metrics.long_poll_wait_seconds.record(
                    started_at.elapsed().as_secs_f64(),
                    &[KeyValue::new("namespace", namespace_str)],
                );
                return Ok(no_task_response());
            }
        }
    }
}

/// Side-channel data carried back from `try_dispatch` so the long-poll
/// caller can emit per-task metrics (which require knowing the task_type
/// and submit timestamp picked by the dispatcher).
struct DispatchHit {
    resp: AcquireTaskResponse,
    dispatch: DispatchInfo,
}

struct DispatchInfo {
    task_type: TaskType,
    /// Seconds from PENDING insert (`submitted_at`) to DISPATCHED
    /// (`record_acquisition`). `None` if the clock skew yields a negative
    /// elapsed (e.g. SystemTime jumped) — drop the sample rather than
    /// pollute the histogram.
    submit_to_dispatch_seconds: Option<f64>,
}

/// Open a short transaction, look up the worker's heartbeat row, return
/// WORKER_DEREGISTERED if `declared_dead_at` is set. We reuse
/// `record_worker_heartbeat` since it returns `WorkerDeregistered` when the
/// row is tombstoned without modifying the timestamp meaningfully on the
/// dead path.
async fn validate_worker_alive(
    state: &Arc<CpState>,
    worker_id: &WorkerId,
    namespace: &Namespace,
) -> Result<(), AcquireTaskResponse> {
    let now = current_timestamp();
    let mut tx = match state.storage.begin_dyn().await {
        Ok(t) => t,
        Err(err) => {
            tracing::error!(error = %err, "acquire_task validate_worker begin");
            return Err(no_task_with_rejection(
                RejectReason::SYSTEM_OVERLOAD,
                "storage error",
            ));
        }
    };
    let ack = match tx.record_worker_heartbeat(worker_id, namespace, now).await {
        Ok(ack) => ack,
        Err(err) => {
            tracing::error!(error = %err, "validate_worker heartbeat upsert");
            let _ = tx.rollback_dyn().await;
            return Err(no_task_with_rejection(
                RejectReason::SYSTEM_OVERLOAD,
                "storage error",
            ));
        }
    };
    if let Err(err) = tx.commit_dyn().await {
        tracing::error!(error = %err, "validate_worker commit");
        return Err(no_task_with_rejection(
            RejectReason::SYSTEM_OVERLOAD,
            "storage error",
        ));
    }
    if matches!(ack, HeartbeatAck::WorkerDeregistered) {
        return Err(no_task_with_rejection(
            RejectReason::WORKER_DEREGISTERED,
            "worker was previously declared dead; re-Register before acquiring",
        ));
    }
    Ok(())
}

/// One dispatch attempt: open a SERIALIZABLE transaction, run the
/// dispatcher, record the lease, commit. Returns `Ok(Some(resp))` on a hit;
/// `Ok(None)` when no task matched (caller should wait); `Err` propagates
/// non-recoverable storage errors.
async fn try_dispatch(
    state: Arc<CpState>,
    namespace: Namespace,
    task_types: Vec<TaskType>,
    worker_id: WorkerId,
) -> Result<Option<DispatchHit>, StorageError> {
    let metrics = state.metrics.clone();
    let namespace_str = namespace.as_str().to_owned();
    with_serializable_retry(&metrics, "acquire_task", || {
        let state = Arc::clone(&state);
        let namespace = namespace.clone();
        let task_types = task_types.clone();
        let metrics = metrics.clone();
        let namespace_str = namespace_str.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;

            // Best-effort dispatch-side rate quota.
            if let Ok(RateDecision::RateLimited { .. }) = tx
                .try_consume_rate_quota(&namespace, RateKind::DispatchRpm, 1)
                .await
            {
                let _ = tx.rollback_dyn().await;
                metrics.rate_limit_hit_total.add(
                    1,
                    &[
                        KeyValue::new("namespace", namespace_str),
                        KeyValue::new("kind", "dispatch_rpm"),
                    ],
                );
                return Ok(None);
            }

            let now = current_timestamp();
            let strategy = state.strategy_registry.for_namespace(&namespace);

            // Resolve the `PickOrdering` from the per-namespace
            // dispatcher. `for_namespace` always returns `Some` since
            // Phase 9c — the registry's implicit default is
            // `(MaxPendingAdmitter, PriorityFifoDispatcher)` — so we
            // always have an ordering to honour.
            let ordering = strategy
                .map(|s| s.dispatcher.pick_ordering())
                .unwrap_or(taskq_storage::PickOrdering::PriorityFifo);

            // Single `pick_and_lock_pending` call — the dispatcher's
            // chosen ordering selects the row and the same transaction
            // locks it. Calling `pick_next` again would re-issue
            // `pick_and_lock_pending` and miss the now-DISPATCHED row,
            // so we centralise the lock here.
            use taskq_storage::{NamespaceFilter, PickCriteria, TaskTypeFilter};
            let criteria = PickCriteria {
                namespace_filter: NamespaceFilter::Single(namespace.clone()),
                task_types_filter: TaskTypeFilter::AnyOf(task_types.clone()),
                ordering,
                now,
            };
            let locked = match tx.pick_and_lock_pending(criteria).await? {
                Some(l) => l,
                None => {
                    let _ = tx.rollback_dyn().await;
                    return Ok(None);
                }
            };

            let lease_window = Duration::from_secs(u64::from(state.config.lease_window_seconds));
            let timeout_at =
                Timestamp::from_unix_millis(now.as_unix_millis() + lease_window.as_millis() as i64);

            tx.record_acquisition(NewLease {
                task_id: locked.task_id,
                attempt_number: locked.attempt_number,
                worker_id,
                acquired_at: now,
                timeout_at,
            })
            .await?;
            tx.commit_dyn().await?;

            // Build the wire response.
            let mut lease = Lease::default();
            lease.task_id = Some(locked.task_id.to_string());
            lease.attempt_number = locked.attempt_number;
            lease.worker_id = Some(worker_id.to_string());
            lease.acquired_at = Some(Box::new(timestamp_proto(now)));
            lease.timeout_at = Some(Box::new(timestamp_proto(timeout_at)));
            lease.last_extended_at = Some(Box::new(timestamp_proto(now)));

            let mut resp = AcquireTaskResponse::default();
            resp.task = Some(Box::new(locked_to_wire_task(&locked)));
            resp.attempt_number = locked.attempt_number;
            resp.lease = Some(Box::new(lease));
            resp.no_task = false;
            resp.traceparent = Some(locked.traceparent.to_vec());
            resp.tracestate = Some(locked.tracestate.to_vec());
            resp.server_version = Some(Box::new(server_semver()));

            // Compute submit -> dispatch latency for the histogram emit
            // upstream. `now - submitted_at` in seconds. A negative value
            // (clock jump) becomes None and the sample is dropped.
            let elapsed_ms = now.as_unix_millis() - locked.submitted_at.as_unix_millis();
            let submit_to_dispatch_seconds = if elapsed_ms >= 0 {
                Some(elapsed_ms as f64 / 1_000.0)
            } else {
                None
            };

            // Continue the trace from the persisted W3C `traceparent`. The
            // span itself is created via `tracing::info_span!` so it shows
            // up under the active OTel pipeline. We only emit a marker
            // event for now; once `tracing-opentelemetry`'s context-extract
            // helper is wired with W3C parsing, this will become a true
            // continuation. The emitted event still carries the traceparent
            // bytes so trace correlation works at log-aggregation time.
            if !locked.traceparent.is_empty() {
                tracing::info!(
                    traceparent = ?locked.traceparent.as_ref(),
                    task_id = %locked.task_id,
                    "taskq.dispatch: continuing parent trace"
                );
            }

            let dispatch = DispatchInfo {
                task_type: locked.task_type.clone(),
                submit_to_dispatch_seconds,
            };
            Ok(Some(DispatchHit { resp, dispatch }))
        }
    })
    .await
}

// ---------------------------------------------------------------------------
// Heartbeat (READ COMMITTED carve-out + lazy lease extension)
// ---------------------------------------------------------------------------

#[tracing::instrument(
    name = "taskq.heartbeat",
    skip_all,
    fields(
        namespace = req.namespace.as_deref().unwrap_or(""),
        worker_id = req.worker_id.as_deref().unwrap_or(""),
    ),
)]
async fn heartbeat_impl(
    state: Arc<CpState>,
    req: HeartbeatRequest,
) -> Result<HeartbeatResponse, Status> {
    let namespace_str = req
        .namespace
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("namespace is required"))?
        .to_owned();
    let namespace = Namespace::new(namespace_str.clone());
    let worker_id_str = req
        .worker_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
        .to_owned();
    let worker_id = parse_worker_id(&worker_id_str)
        .map_err(|err| Status::invalid_argument(format!("invalid worker_id: {err}")))?;
    let now = current_timestamp();
    let metrics = state.metrics.clone();

    // §6.3 step 2: UPSERT into worker_heartbeats. The carve-out is wired
    // through the storage method itself (READ COMMITTED isolation).
    let mut tx = state
        .storage
        .begin_dyn()
        .await
        .map_err(|err| Status::internal(format!("storage begin: {err}")))?;
    let ack = match tx
        .record_worker_heartbeat(&worker_id, &namespace, now)
        .await
    {
        Ok(ack) => ack,
        Err(err) => {
            let _ = tx.rollback_dyn().await;
            tracing::error!(error = %err, "heartbeat upsert error");
            return Err(Status::internal("heartbeat upsert error"));
        }
    };

    // Step 3: lazy lease extension for each held lease whose
    // `(NOW − last_extended_at) ≥ (lease − ε)`. Phase 5b cannot read
    // `last_extended_at` from storage in a single round-trip (no listing
    // method exists), so we fire the extension call unconditionally. The
    // backend's `extend_lease_lazy` is idempotent — repeated calls within
    // the threshold simply re-stamp the same timestamps.
    //
    // TODO(phase-5d): once a `list_held_leases(worker_id)` storage method
    // lands, replace this with a per-lease (NOW − last_extended_at) >=
    // (lease − ε) check so we only fire the SERIALIZABLE write when the
    // §6.3 condition holds.
    let lease_window = Duration::from_secs(u64::from(state.config.lease_window_seconds));
    let new_timeout =
        Timestamp::from_unix_millis(now.as_unix_millis() + lease_window.as_millis() as i64);
    let mut extended_leases: Vec<WireLeaseRef> = Vec::new();
    for lease in req.held_leases.unwrap_or_default() {
        let task_id_str = match lease.task_id.as_deref() {
            Some(s) if !s.is_empty() => s.to_owned(),
            _ => continue,
        };
        let task_id = match parse_task_id(&task_id_str) {
            Ok(id) => id,
            Err(_) => continue,
        };
        let lease_ref = LeaseRef {
            task_id,
            attempt_number: lease.attempt_number,
            worker_id,
        };
        match tx.extend_lease_lazy(&lease_ref, new_timeout, now).await {
            Ok(()) => {
                let mut wlr = WireLeaseRef::default();
                wlr.task_id = Some(task_id_str);
                wlr.attempt_number = lease.attempt_number;
                extended_leases.push(wlr);
            }
            Err(StorageError::NotFound) => {
                // Lease was reaped; SDK observes the no-op silently and
                // will hit LEASE_EXPIRED on its next CompleteTask.
            }
            Err(err) => {
                let _ = tx.rollback_dyn().await;
                tracing::error!(error = %err, "extend_lease_lazy");
                return Err(Status::internal("extend_lease_lazy"));
            }
        }
    }
    if let Err(err) = tx.commit_dyn().await {
        tracing::error!(error = %err, "heartbeat commit");
        return Err(Status::internal("heartbeat commit"));
    }

    metrics
        .heartbeat_total
        .add(1, &[KeyValue::new("namespace", namespace_str)]);

    let drain_signalled = *state.shutdown.borrow();
    let mut resp = HeartbeatResponse::default();
    resp.worker_deregistered = matches!(ack, HeartbeatAck::WorkerDeregistered);
    resp.drain = drain_signalled;
    resp.terminate = false;
    resp.extended_leases = Some(extended_leases);
    resp.server_version = Some(Box::new(server_semver()));
    Ok(resp)
}

// ---------------------------------------------------------------------------
// CompleteTask (design.md §6.4)
// ---------------------------------------------------------------------------

#[tracing::instrument(
    name = "taskq.complete_task",
    skip_all,
    fields(
        task_id = req.task_id.as_deref().unwrap_or(""),
        worker_id = req.worker_id.as_deref().unwrap_or(""),
    ),
)]
async fn complete_task_impl(
    state: Arc<CpState>,
    req: CompleteTaskRequest,
) -> Result<CompleteTaskResponse, Status> {
    let task_id_str = req
        .task_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("task_id is required"))?
        .to_owned();
    let task_id = parse_task_id(&task_id_str)
        .map_err(|err| Status::invalid_argument(format!("invalid task_id: {err}")))?;
    let worker_id_str = req
        .worker_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
        .to_owned();
    let worker_id = parse_worker_id(&worker_id_str)
        .map_err(|err| Status::invalid_argument(format!("invalid worker_id: {err}")))?;

    let lease_ref = LeaseRef {
        task_id,
        attempt_number: req.attempt_number,
        worker_id,
    };
    let outcome = TaskOutcome::Success {
        result_payload: Bytes::from(req.result_payload.unwrap_or_default()),
        recorded_at: current_timestamp(),
    };

    let metrics = state.metrics.clone();
    let result = with_serializable_retry(&metrics, "complete_task", || {
        let state = Arc::clone(&state);
        let lease_ref = lease_ref.clone();
        let outcome = outcome.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            // Read the task before mutating so we can label metrics by
            // namespace + task_type. The read participates in the same
            // SERIALIZABLE transaction as the state transition.
            let task_meta = match tx.get_task_by_id(task_id).await {
                Ok(Some(t)) => Some((t.namespace.clone(), t.task_type.clone())),
                _ => None,
            };
            match tx.complete_task(&lease_ref, outcome).await {
                Ok(()) => {
                    tx.commit_dyn().await?;
                    Ok(CompleteFlow::Completed { task_meta })
                }
                Err(StorageError::NotFound) => {
                    let _ = tx.rollback_dyn().await;
                    Ok(CompleteFlow::LeaseExpired { task_meta })
                }
                Err(other) => {
                    let _ = tx.rollback_dyn().await;
                    Err(other)
                }
            }
        }
    })
    .await;

    match result {
        Ok(CompleteFlow::Completed { task_meta }) => {
            let labels = lifecycle_labels(task_meta.as_ref(), "success");
            metrics.complete_total.add(1, &labels);
            metrics
                .terminal_total
                .add(1, &terminal_labels(task_meta.as_ref(), "COMPLETED"));
            let mut resp = CompleteTaskResponse::default();
            resp.task_id = Some(task_id_str);
            resp.final_status = TerminalState::COMPLETED;
            resp.server_version = Some(Box::new(server_semver()));
            Ok(resp)
        }
        Ok(CompleteFlow::LeaseExpired { task_meta }) => {
            metrics.lease_expired_total.add(
                1,
                &lifecycle_labels(task_meta.as_ref(), "lease_expired")[..2],
            );
            let mut resp = CompleteTaskResponse::default();
            resp.task_id = Some(task_id_str);
            resp.final_status = TerminalState::PENDING;
            resp.server_version = Some(Box::new(server_semver()));
            resp.error = Some(Box::new(rejection_with_reason(
                RejectReason::LEASE_EXPIRED,
                "lease expired before CompleteTask landed",
            )));
            Ok(resp)
        }
        Err(err) => {
            tracing::error!(error = %err, "complete_task storage error");
            Err(Status::internal("storage error during CompleteTask"))
        }
    }
}

/// Build the `(namespace, task_type, outcome)` label slice. When task
/// metadata is unavailable (e.g. a stale-id complete after a race),
/// substitute "unknown" so the metric still records — operators can see
/// the dimension exists but lacks identification.
fn lifecycle_labels(meta: Option<&(Namespace, TaskType)>, outcome: &str) -> [KeyValue; 3] {
    let (ns, tt) = match meta {
        Some((n, t)) => (n.as_str().to_owned(), t.as_str().to_owned()),
        None => ("unknown".to_owned(), "unknown".to_owned()),
    };
    [
        KeyValue::new("namespace", ns),
        KeyValue::new("task_type", tt),
        KeyValue::new("outcome", outcome.to_owned()),
    ]
}

/// Build the `(namespace, task_type, terminal_state)` label slice for
/// `taskq_terminal_total`.
fn terminal_labels(meta: Option<&(Namespace, TaskType)>, terminal_state: &str) -> [KeyValue; 3] {
    let (ns, tt) = match meta {
        Some((n, t)) => (n.as_str().to_owned(), t.as_str().to_owned()),
        None => ("unknown".to_owned(), "unknown".to_owned()),
    };
    [
        KeyValue::new("namespace", ns),
        KeyValue::new("task_type", tt),
        KeyValue::new("terminal_state", terminal_state.to_owned()),
    ]
}

enum CompleteFlow {
    Completed {
        task_meta: Option<(Namespace, TaskType)>,
    },
    LeaseExpired {
        task_meta: Option<(Namespace, TaskType)>,
    },
}

// ---------------------------------------------------------------------------
// ReportFailure (design.md §6.4 + §6.5 terminal-state mapping)
// ---------------------------------------------------------------------------

#[tracing::instrument(
    name = "taskq.report_failure",
    skip_all,
    fields(
        task_id = req.task_id.as_deref().unwrap_or(""),
        worker_id = req.worker_id.as_deref().unwrap_or(""),
    ),
)]
async fn report_failure_impl(
    state: Arc<CpState>,
    req: ReportFailureRequest,
) -> Result<ReportFailureResponse, Status> {
    let task_id_str = req
        .task_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("task_id is required"))?
        .to_owned();
    let task_id = parse_task_id(&task_id_str)
        .map_err(|err| Status::invalid_argument(format!("invalid task_id: {err}")))?;
    let worker_id_str = req
        .worker_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
        .to_owned();
    let worker_id = parse_worker_id(&worker_id_str)
        .map_err(|err| Status::invalid_argument(format!("invalid worker_id: {err}")))?;
    let failure = req
        .failure
        .ok_or_else(|| Status::invalid_argument("failure is required"))?;
    let error_class = failure.error_class.unwrap_or_default();
    let error_message = failure.message.unwrap_or_default();
    let error_details = Bytes::from(failure.details.unwrap_or_default());
    let retryable = failure.retryable;
    let now = current_timestamp();
    let lease_ref = LeaseRef {
        task_id,
        attempt_number: req.attempt_number,
        worker_id,
    };
    let metrics = state.metrics.clone();

    let result = with_serializable_retry(&metrics, "report_failure", || {
        let state = Arc::clone(&state);
        let lease_ref = lease_ref.clone();
        let error_class = error_class.clone();
        let error_message = error_message.clone();
        let error_details = error_details.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            // §6.5 mapping requires the task's per-task retry config plus
            // `expires_at` and `max_retries`. Read once, classify, then
            // hand the resulting `TaskOutcome` to `complete_task`. The
            // read participates in the same SERIALIZABLE transaction as
            // the state transition.
            let task = tx.get_task_by_id(task_id).await?;
            let task_meta = task
                .as_ref()
                .map(|t| (t.namespace.clone(), t.task_type.clone()));
            let mapping = classify_failure(
                task.as_ref(),
                retryable,
                req.attempt_number,
                now,
                &error_class,
                &error_message,
                &error_details,
            );
            let outcome = mapping.outcome.clone();
            match tx.complete_task(&lease_ref, outcome).await {
                Ok(()) => {
                    tx.commit_dyn().await?;
                    Ok(ReportFlow::Settled {
                        task_meta,
                        terminal: mapping.terminal,
                    })
                }
                Err(StorageError::NotFound) => {
                    let _ = tx.rollback_dyn().await;
                    Ok(ReportFlow::LeaseExpired { task_meta })
                }
                Err(other) => {
                    let _ = tx.rollback_dyn().await;
                    Err(other)
                }
            }
        }
    })
    .await;

    match result {
        Ok(ReportFlow::Settled {
            task_meta,
            terminal,
        }) => {
            // §11.3 emit-site discipline: outcome label distinguishes the
            // four classifications (waiting-retry vs. each terminal kind).
            let outcome_label = match terminal {
                TerminalClassification::WaitingRetry { .. } => "retryable_fail",
                TerminalClassification::FailedNonretryable => "nonretryable_fail",
                TerminalClassification::FailedExhausted => "retries_exhausted",
                TerminalClassification::Expired => "expired",
            };
            metrics
                .complete_total
                .add(1, &lifecycle_labels(task_meta.as_ref(), outcome_label));
            // `taskq_terminal_total` only fires on a true terminal — retry
            // is not terminal.
            let terminal_label = match terminal {
                TerminalClassification::WaitingRetry { .. } => None,
                TerminalClassification::FailedNonretryable => Some("FAILED_NONRETRYABLE"),
                TerminalClassification::FailedExhausted => Some("FAILED_EXHAUSTED"),
                TerminalClassification::Expired => Some("EXPIRED"),
            };
            if let Some(state_label) = terminal_label {
                metrics
                    .terminal_total
                    .add(1, &terminal_labels(task_meta.as_ref(), state_label));
            }
            // `taskq_error_class_total{namespace, task_type, error_class}`.
            if !error_class.is_empty() {
                let (ns, tt) = match task_meta.as_ref() {
                    Some((n, t)) => (n.as_str().to_owned(), t.as_str().to_owned()),
                    None => ("unknown".to_owned(), "unknown".to_owned()),
                };
                metrics.error_class_total.add(
                    1,
                    &[
                        KeyValue::new("namespace", ns),
                        KeyValue::new("task_type", tt),
                        KeyValue::new("error_class", error_class.clone()),
                    ],
                );
            }
            if matches!(terminal, TerminalClassification::WaitingRetry { .. }) {
                let (ns, tt) = match task_meta.as_ref() {
                    Some((n, t)) => (n.as_str().to_owned(), t.as_str().to_owned()),
                    None => ("unknown".to_owned(), "unknown".to_owned()),
                };
                metrics.retry_total.add(
                    1,
                    &[
                        KeyValue::new("namespace", ns),
                        KeyValue::new("task_type", tt),
                    ],
                );
            }
            let final_status = match terminal {
                TerminalClassification::WaitingRetry { .. } => TerminalState::WAITING_RETRY,
                TerminalClassification::FailedNonretryable => TerminalState::FAILED_NONRETRYABLE,
                TerminalClassification::FailedExhausted => TerminalState::FAILED_EXHAUSTED,
                TerminalClassification::Expired => TerminalState::EXPIRED,
            };
            let mut resp = ReportFailureResponse::default();
            resp.task_id = Some(task_id_str);
            resp.final_status = final_status;
            if let TerminalClassification::WaitingRetry { retry_after } = terminal {
                resp.retry_after = Some(Box::new(timestamp_proto(retry_after)));
            }
            resp.server_version = Some(Box::new(server_semver()));
            Ok(resp)
        }
        Ok(ReportFlow::LeaseExpired { task_meta }) => {
            metrics.lease_expired_total.add(
                1,
                &lifecycle_labels(task_meta.as_ref(), "lease_expired")[..2],
            );
            let mut resp = ReportFailureResponse::default();
            resp.task_id = Some(task_id_str);
            resp.final_status = TerminalState::PENDING;
            resp.server_version = Some(Box::new(server_semver()));
            resp.error = Some(Box::new(rejection_with_reason(
                RejectReason::LEASE_EXPIRED,
                "lease expired before ReportFailure landed",
            )));
            Ok(resp)
        }
        Err(err) => {
            tracing::error!(error = %err, "report_failure storage error");
            Err(Status::internal("storage error during ReportFailure"))
        }
    }
}

enum ReportFlow {
    Settled {
        task_meta: Option<(Namespace, TaskType)>,
        terminal: TerminalClassification,
    },
    LeaseExpired {
        task_meta: Option<(Namespace, TaskType)>,
    },
}

/// One-row classification result of `classify_failure`. Records the §6.5
/// terminal-state hit and carries the `TaskOutcome` to hand to
/// `complete_task`. `terminal` and `outcome` are kept in lockstep — caller
/// uses the former for metric/wire labels and the latter for the storage
/// transition.
#[derive(Clone)]
struct FailureMapping {
    terminal: TerminalClassification,
    outcome: TaskOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TerminalClassification {
    WaitingRetry { retry_after: Timestamp },
    FailedNonretryable,
    FailedExhausted,
    Expired,
}

/// Apply the `design.md` §6.5 mapping table for a `ReportFailure` against
/// the persisted task row. When `task` is `None` (the row was deleted
/// between `AcquireTask` and `ReportFailure` — extremely unlikely under
/// SERIALIZABLE) we treat the failure as terminal nonretryable, since we
/// have nothing to budget against.
fn classify_failure(
    task: Option<&taskq_storage::Task>,
    retryable: bool,
    attempt_number: u32,
    now: Timestamp,
    error_class: &str,
    error_message: &str,
    error_details: &Bytes,
) -> FailureMapping {
    if !retryable {
        return FailureMapping {
            terminal: TerminalClassification::FailedNonretryable,
            outcome: TaskOutcome::FailedNonretryable {
                error_class: error_class.to_owned(),
                error_message: error_message.to_owned(),
                error_details: error_details.clone(),
                recorded_at: now,
            },
        };
    }
    let Some(task) = task else {
        // Defensive fallback: classify as nonretryable so we don't leave
        // the task in a quiet WAITING_RETRY without retry_after evidence.
        return FailureMapping {
            terminal: TerminalClassification::FailedNonretryable,
            outcome: TaskOutcome::FailedNonretryable {
                error_class: error_class.to_owned(),
                error_message: error_message.to_owned(),
                error_details: error_details.clone(),
                recorded_at: now,
            },
        };
    };
    if attempt_number >= task.max_retries {
        return FailureMapping {
            terminal: TerminalClassification::FailedExhausted,
            outcome: TaskOutcome::FailedExhausted {
                error_class: error_class.to_owned(),
                error_message: error_message.to_owned(),
                error_details: error_details.clone(),
                recorded_at: now,
            },
        };
    }
    let retry_after = compute_retry_after(
        now,
        attempt_number,
        task.retry_initial_ms,
        task.retry_max_ms,
        task.retry_coefficient,
    );
    if retry_after.as_unix_millis() > task.expires_at.as_unix_millis() {
        return FailureMapping {
            terminal: TerminalClassification::Expired,
            outcome: TaskOutcome::Expired {
                error_class: error_class.to_owned(),
                error_message: error_message.to_owned(),
                error_details: error_details.clone(),
                recorded_at: now,
            },
        };
    }
    FailureMapping {
        terminal: TerminalClassification::WaitingRetry { retry_after },
        outcome: TaskOutcome::WaitingRetry {
            error_class: error_class.to_owned(),
            error_message: error_message.to_owned(),
            error_details: error_details.clone(),
            retry_after,
            recorded_at: now,
        },
    }
}

/// `design.md` §6.5 backoff math:
///
/// ```text
/// delay = min(initial_ms * coefficient^attempt, max_ms)
/// retry_after = now + rand(delay/2, delay*3/2)
/// ```
///
/// `coefficient ^ attempt` saturates to `max_ms` so a runaway exponent
/// can't blow past `u64::MAX`. The full-jitter window
/// `[delay/2, delay*3/2]` is sampled uniformly so the §6.5 contract holds
/// even at attempt=0.
fn compute_retry_after(
    now: Timestamp,
    attempt_number: u32,
    initial_ms: u64,
    max_ms: u64,
    coefficient: f32,
) -> Timestamp {
    use rand::Rng;

    let initial = initial_ms.max(1) as f64;
    let coeff = if coefficient.is_finite() && coefficient > 0.0 {
        coefficient as f64
    } else {
        2.0
    };
    let factor = coeff.powi(attempt_number as i32);
    let raw = initial * factor;
    let max = max_ms.max(initial as u64) as f64;
    let delay = if raw.is_finite() {
        raw.min(max).max(1.0)
    } else {
        max
    };
    let half = (delay / 2.0).max(1.0);
    let three_halves = (delay * 1.5).max(half + 1.0);
    let mut rng = rand::thread_rng();
    let jittered = rng.gen_range(half..three_halves);
    let delay_ms = jittered.round() as i64;
    Timestamp::from_unix_millis(now.as_unix_millis().saturating_add(delay_ms))
}

// ---------------------------------------------------------------------------
// Deregister
// ---------------------------------------------------------------------------

#[tracing::instrument(
    name = "taskq.deregister_worker",
    skip_all,
    fields(worker_id = req.worker_id.as_deref().unwrap_or("")),
)]
async fn deregister_impl(
    state: Arc<CpState>,
    req: DeregisterWorkerRequest,
) -> Result<DeregisterWorkerResponse, Status> {
    let worker_id_str = req
        .worker_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("worker_id is required"))?
        .to_owned();
    let worker_id = parse_worker_id(&worker_id_str)
        .map_err(|err| Status::invalid_argument(format!("invalid worker_id: {err}")))?;
    let now = current_timestamp();
    let metrics = state.metrics.clone();

    let result = with_serializable_retry(&metrics, "deregister_worker", || {
        let state = Arc::clone(&state);
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            tx.mark_worker_dead(&worker_id, now).await?;
            tx.commit_dyn().await?;
            Ok(())
        }
    })
    .await;

    match result {
        Ok(()) => {
            // Best-effort: the periodic refresher will reconcile the gauge,
            // but a synchronous decrement keeps the metric responsive.
            // Namespace is not threaded through the deregister request in
            // v1 (the `worker_id` is global); the refresher pass corrects.
            let _ = metrics; // metrics handle still in scope; explicit drop for readability.
            let mut resp = DeregisterWorkerResponse::default();
            resp.server_version = Some(Box::new(server_semver()));
            Ok(resp)
        }
        Err(StorageError::NotFound) => {
            let mut resp = DeregisterWorkerResponse::default();
            resp.server_version = Some(Box::new(server_semver()));
            resp.error = Some(Box::new(rejection_with_reason(
                RejectReason::WORKER_DEREGISTERED,
                "worker_id was not registered",
            )));
            Ok(resp)
        }
        Err(err) => {
            tracing::error!(error = %err, "deregister_worker storage error");
            Err(Status::internal("storage error during Deregister"))
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn server_semver() -> SemVer {
    let mut s = SemVer::default();
    s.major = 1;
    s.minor = 0;
    s.patch = 0;
    s
}

fn timestamp_proto(ts: Timestamp) -> WireTimestamp {
    let mut t = WireTimestamp::default();
    t.unix_millis = ts.as_unix_millis();
    t
}

fn current_timestamp() -> Timestamp {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    Timestamp::from_unix_millis(ms)
}

fn parse_task_id(s: &str) -> Result<taskq_storage::TaskId, Box<dyn std::error::Error>> {
    use std::str::FromStr;
    let uuid = uuid::Uuid::from_str(s)?;
    Ok(taskq_storage::TaskId::from_uuid(uuid))
}

fn parse_worker_id(s: &str) -> Result<WorkerId, Box<dyn std::error::Error>> {
    use std::str::FromStr;
    let uuid = uuid::Uuid::from_str(s)?;
    Ok(WorkerId::from_uuid(uuid))
}

fn rejection_with_reason(reason: RejectReason, hint: &str) -> Rejection {
    let mut rej = Rejection::default();
    rej.reason = reason;
    rej.retryable = matches!(
        reason,
        RejectReason::SYSTEM_OVERLOAD | RejectReason::REPLICA_WAITER_LIMIT_EXCEEDED
    );
    rej.hint = Some(hint.to_owned());
    rej.server_version = Some(Box::new(server_semver()));
    rej
}

fn no_task_response() -> AcquireTaskResponse {
    let mut r = AcquireTaskResponse::default();
    r.no_task = true;
    r.server_version = Some(Box::new(server_semver()));
    r
}

fn no_task_with_rejection(reason: RejectReason, hint: &str) -> AcquireTaskResponse {
    let mut r = AcquireTaskResponse::default();
    r.no_task = true;
    r.server_version = Some(Box::new(server_semver()));
    r.error = Some(Box::new(rejection_with_reason(reason, hint)));
    r
}

fn effective_long_poll_timeout(state: &Arc<CpState>, requested_ms: u64) -> Duration {
    let max = u64::from(state.config.long_poll_max_seconds) * 1_000;
    let chosen_ms = if requested_ms == 0 {
        u64::from(state.config.long_poll_default_timeout_seconds) * 1_000
    } else {
        requested_ms.min(max)
    };
    Duration::from_millis(chosen_ms)
}

fn locked_to_wire_task(locked: &taskq_storage::LockedTask) -> Task {
    let mut task = Task::default();
    task.task_id = Some(locked.task_id.to_string());
    task.namespace = Some(locked.namespace.as_str().to_owned());
    task.task_type = Some(locked.task_type.as_str().to_owned());
    task.status = TerminalState::DISPATCHED;
    task.priority = locked.priority;
    task.payload = Some(locked.payload.to_vec());
    task.submitted_at = Some(Box::new(timestamp_proto(locked.submitted_at)));
    task.expires_at = Some(Box::new(timestamp_proto(locked.expires_at)));
    task.attempt_number = locked.attempt_number;
    task.max_retries = locked.max_retries;
    let mut rc = WireRetryConfig::default();
    rc.initial_ms = locked.retry_initial_ms;
    rc.max_ms = locked.retry_max_ms;
    rc.coefficient = locked.retry_coefficient;
    rc.max_retries = locked.max_retries;
    task.retry_config = Some(Box::new(rc));
    task.traceparent = Some(locked.traceparent.to_vec());
    task.tracestate = Some(locked.tracestate.to_vec());
    task.format_version = 1;
    task
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

    use crate::config::{CpConfig, OtelExporterConfig, StorageBackendConfig};
    use crate::observability::MetricsHandle;
    use crate::shutdown::channel;
    use crate::strategy::StrategyRegistry;

    async fn build_state() -> Arc<CpState> {
        let storage = taskq_storage_sqlite::SqliteStorage::open_in_memory()
            .await
            .expect("SqliteStorage::open_in_memory");
        let storage_arc: Arc<dyn crate::state::DynStorage> = Arc::new(storage);
        let config = Arc::new(CpConfig {
            bind_addr: "127.0.0.1:50051".parse::<SocketAddr>().unwrap(),
            health_addr: "127.0.0.1:9090".parse::<SocketAddr>().unwrap(),
            storage_backend: StorageBackendConfig::Sqlite {
                path: ":memory:".to_owned(),
            },
            otel_exporter: OtelExporterConfig::Disabled,
            quota_cache_ttl_seconds: 5,
            long_poll_default_timeout_seconds: 1,
            long_poll_max_seconds: 60,
            belt_and_suspenders_seconds: 1,
            waiter_limit_per_replica: 100,
            lease_window_seconds: 30,
        });
        let (_tx, rx) = channel();
        Arc::new(CpState::new(
            storage_arc,
            Arc::new(StrategyRegistry::empty()),
            MetricsHandle::noop(),
            rx,
            config,
        ))
    }

    fn build_register_request(ns: &str, task_types: Vec<String>) -> RegisterWorkerRequest {
        let mut req = RegisterWorkerRequest::default();
        req.namespace = Some(ns.to_owned());
        req.task_types = Some(task_types);
        req.declared_concurrency = 1;
        req
    }

    #[tokio::test]
    async fn register_returns_worker_id_and_lease_settings() {
        // Arrange
        let state = build_state().await;
        let req = build_register_request("ns", vec!["t1".to_owned()]);

        // Act
        let resp = register_impl(state, req).await.expect("register");

        // Assert
        assert!(resp.error.is_none(), "register should succeed: {resp:?}");
        assert!(resp.worker_id.is_some());
        assert!(resp.lease_duration_ms > 0);
        assert!(resp.eps_ms > 0);
    }

    // -----------------------------------------------------------------
    // Validation paths -- each handler rejects malformed wire requests
    // before touching storage. Cheap to test, high coverage value
    // since the validation chain exercises ~10 lines of plumbing per
    // handler.
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn heartbeat_rejects_empty_namespace() {
        // Arrange
        let state = build_state().await;
        let mut req = HeartbeatRequest::default();
        req.worker_id = Some("019df005-0000-0000-0000-000000000001".to_owned());

        // Act
        let err = heartbeat_impl(state, req).await.unwrap_err();

        // Assert
        assert_eq!(err.code(), grpc_core::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn heartbeat_rejects_empty_worker_id() {
        // Arrange
        let state = build_state().await;
        let mut req = HeartbeatRequest::default();
        req.namespace = Some("ns".to_owned());

        // Act
        let err = heartbeat_impl(state, req).await.unwrap_err();

        // Assert
        assert_eq!(err.code(), grpc_core::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn heartbeat_rejects_malformed_worker_id() {
        // Arrange: not a valid UUID.
        let state = build_state().await;
        let mut req = HeartbeatRequest::default();
        req.namespace = Some("ns".to_owned());
        req.worker_id = Some("not-a-uuid".to_owned());

        // Act
        let err = heartbeat_impl(state, req).await.unwrap_err();

        // Assert
        assert_eq!(err.code(), grpc_core::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn heartbeat_with_no_held_leases_succeeds_after_register() {
        // Arrange: register a worker first so heartbeat has a row to
        // upsert against; then issue a heartbeat carrying no leases.
        let state = build_state().await;
        let registered = register_impl(
            Arc::clone(&state),
            build_register_request("ns", vec!["t1".to_owned()]),
        )
        .await
        .expect("register");
        let worker_id = registered.worker_id.expect("worker_id");
        let mut req = HeartbeatRequest::default();
        req.namespace = Some("ns".to_owned());
        req.worker_id = Some(worker_id);
        // held_leases left as None.

        // Act
        let resp = heartbeat_impl(state, req).await.expect("heartbeat");

        // Assert: ack present, no extended_leases.
        assert!(resp.error.is_none(), "response: {resp:?}");
        assert!(resp
            .extended_leases
            .as_ref()
            .map(|v| v.is_empty())
            .unwrap_or(true));
    }

    #[tokio::test]
    async fn complete_task_rejects_empty_task_id() {
        // Arrange
        let state = build_state().await;
        let mut req = CompleteTaskRequest::default();
        req.worker_id = Some("019df005-0000-0000-0000-000000000001".to_owned());

        // Act
        let err = complete_task_impl(state, req).await.unwrap_err();

        // Assert
        assert_eq!(err.code(), grpc_core::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn complete_task_rejects_empty_worker_id() {
        // Arrange
        let state = build_state().await;
        let mut req = CompleteTaskRequest::default();
        req.task_id = Some(taskq_storage::TaskId::generate().to_string());

        // Act
        let err = complete_task_impl(state, req).await.unwrap_err();

        // Assert
        assert_eq!(err.code(), grpc_core::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn report_failure_rejects_empty_task_id() {
        // Arrange
        let state = build_state().await;
        let mut req = ReportFailureRequest::default();
        req.worker_id = Some("019df005-0000-0000-0000-000000000001".to_owned());

        // Act
        let err = report_failure_impl(state, req).await.unwrap_err();

        // Assert
        assert_eq!(err.code(), grpc_core::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn deregister_rejects_empty_worker_id() {
        // Arrange
        let state = build_state().await;
        let req = DeregisterWorkerRequest::default();

        // Act
        let err = deregister_impl(state, req).await.unwrap_err();

        // Assert
        assert_eq!(err.code(), grpc_core::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn deregister_is_idempotent_for_unregistered_worker_id() {
        // Arrange: well-formed UUID that was never registered.
        let state = build_state().await;
        let mut req = DeregisterWorkerRequest::default();
        req.worker_id = Some(taskq_storage::WorkerId::generate().to_string());

        // Act
        let resp = deregister_impl(state, req).await.expect("deregister");

        // Assert: idempotent. The backend's `mark_worker_dead` is an
        // UPDATE-where (no row -> 0 rows affected, no error), so the
        // handler returns success with no error envelope. SDKs that
        // call deregister twice during shutdown observe the same shape
        // both times. The `WORKER_DEREGISTERED` rejection branch in
        // deregister_impl is dead under SQLite/Postgres backends; it
        // is reserved for backends whose mark_worker_dead can return
        // NotFound.
        assert!(
            resp.error.is_none(),
            "deregister should be idempotent: {resp:?}"
        );
        assert!(resp.server_version.is_some());
    }

    #[tokio::test]
    async fn acquire_task_returns_no_task_when_queue_empty() {
        // Arrange
        let state = build_state().await;
        let register_req = build_register_request("ns", vec!["t1".to_owned()]);
        let registered = register_impl(Arc::clone(&state), register_req)
            .await
            .expect("register");
        let worker_id = registered.worker_id.expect("worker_id");

        let mut req = AcquireTaskRequest::default();
        req.worker_id = Some(worker_id);
        req.namespace = Some("ns".to_owned());
        req.task_types = Some(vec!["t1".to_owned()]);
        req.long_poll_timeout_ms = 100; // short for test speed

        // Act
        let resp = acquire_task_impl(state, req).await.expect("acquire");

        // Assert: with no tasks queued, the long-poll deadline elapses and
        // we return no_task=true.
        assert!(resp.no_task);
        assert!(resp.task.is_none());
    }
}
