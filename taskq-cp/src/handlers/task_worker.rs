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
//!   [`crate::handlers::retry::with_serializable_retry`].
//! - `Heartbeat` is the §6.3 carve-out — the heartbeat path is READ
//!   COMMITTED via [`StorageTxDyn::record_worker_heartbeat`] (the storage
//!   trait routes that to a cheap UPSERT). Lazy lease extension fires a
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
use crate::strategy::dispatcher::PullCtx;

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
            Ok(Some(resp)) => {
                metrics
                    .dispatch_total
                    .add(1, &[KeyValue::new("namespace", namespace_str.clone())]);
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
            return Ok(no_task_response());
        }
        match waiter.wait_for_wake_or_timeout(remaining, belt).await {
            WakeOutcome::Woken | WakeOutcome::BeltAndSuspenders => continue,
            WakeOutcome::Timeout => return Ok(no_task_response()),
        }
    }
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
) -> Result<Option<AcquireTaskResponse>, StorageError> {
    let metrics = state.metrics.clone();
    with_serializable_retry(&metrics, "acquire_task", || {
        let state = Arc::clone(&state);
        let namespace = namespace.clone();
        let task_types = task_types.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;

            // Best-effort dispatch-side rate quota.
            if let Ok(RateDecision::RateLimited { .. }) = tx
                .try_consume_rate_quota(&namespace, RateKind::DispatchRpm, 1)
                .await
            {
                let _ = tx.rollback_dyn().await;
                return Ok(None);
            }

            let now = current_timestamp();
            let strategy = state.strategy_registry.for_namespace(&namespace);

            // Default to PriorityFifo when no strategy is configured for
            // the namespace; this matches the SQLite/Postgres backends'
            // behaviour for the v1 reference deployment.
            let picked = if let Some(strategy) = strategy {
                let ctx = PullCtx {
                    namespace_filter: vec![namespace.clone()],
                    task_types: task_types.clone(),
                    now,
                };
                strategy.dispatcher.pick_next(&ctx, &mut *tx).await
            } else {
                use taskq_storage::{NamespaceFilter, PickCriteria, PickOrdering, TaskTypeFilter};
                let criteria = PickCriteria {
                    namespace_filter: NamespaceFilter::Single(namespace.clone()),
                    task_types_filter: TaskTypeFilter::AnyOf(task_types.clone()),
                    ordering: PickOrdering::PriorityFifo,
                    now,
                };
                tx.pick_and_lock_pending(criteria)
                    .await?
                    .map(|locked| locked.task_id)
            };

            let task_id = match picked {
                Some(id) => id,
                None => {
                    let _ = tx.rollback_dyn().await;
                    return Ok(None);
                }
            };

            // The strategy returned a TaskId only; we need the locked-row
            // payload. Phase 5b's strategy traits drop the `LockedTask` after
            // returning; for the dispatch-record + lease-write step we
            // re-pick (idempotent in the same transaction since the row is
            // now locked). When the strategy already locked, the second
            // call returns the same row immediately.
            //
            // TODO(phase-5d): widen the dispatcher trait to return the full
            // LockedTask so we don't pay a second pick. v1's strategies all
            // forward the LockedTask anyway; the trait collapse just hasn't
            // landed.
            let locked = {
                use taskq_storage::{NamespaceFilter, PickCriteria, PickOrdering, TaskTypeFilter};
                let criteria = PickCriteria {
                    namespace_filter: NamespaceFilter::Single(namespace.clone()),
                    task_types_filter: TaskTypeFilter::AnyOf(task_types.clone()),
                    ordering: PickOrdering::PriorityFifo,
                    now,
                };
                match tx.pick_and_lock_pending(criteria).await? {
                    Some(l) if l.task_id == task_id => l,
                    _ => {
                        // Another waiter beat us to the row in this dispatch
                        // race — return None and let the caller re-wait.
                        let _ = tx.rollback_dyn().await;
                        return Ok(None);
                    }
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
            Ok(Some(resp))
        }
    })
    .await
}

// ---------------------------------------------------------------------------
// Heartbeat (READ COMMITTED carve-out + lazy lease extension)
// ---------------------------------------------------------------------------

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
            match tx.complete_task(&lease_ref, outcome).await {
                Ok(()) => {
                    tx.commit_dyn().await?;
                    Ok(CompleteFlow::Completed)
                }
                Err(StorageError::NotFound) => {
                    let _ = tx.rollback_dyn().await;
                    Ok(CompleteFlow::LeaseExpired)
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
        Ok(CompleteFlow::Completed) => {
            metrics.complete_total.add(1, &[]);
            metrics.terminal_total.add(1, &[]);
            let mut resp = CompleteTaskResponse::default();
            resp.task_id = Some(task_id_str);
            resp.final_status = TerminalState::COMPLETED;
            resp.server_version = Some(Box::new(server_semver()));
            Ok(resp)
        }
        Ok(CompleteFlow::LeaseExpired) => {
            metrics.lease_expired_total.add(1, &[]);
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

enum CompleteFlow {
    Completed,
    LeaseExpired,
}

// ---------------------------------------------------------------------------
// ReportFailure (design.md §6.4 + §6.5 terminal-state mapping)
// ---------------------------------------------------------------------------

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

    // Phase 5b: §6.5 mapping needs the task's `attempt_number`,
    // `max_retries`, `expires_at`, and resolved `RetryConfig` to compute
    // `retry_after`. Without a `get_task_by_id` storage method we cannot
    // read those fields, so we surface the simpler classification: a
    // non-retryable failure goes straight to FAILED_NONRETRYABLE; a
    // retryable failure goes to WAITING_RETRY with `retry_after = now +
    // initial_backoff(1s)` as a conservative seed. The per-task retry
    // config is preserved on the task row at submit time and Phase 5c will
    // read it through.
    let outcome = if retryable {
        let retry_after = Timestamp::from_unix_millis(
            now.as_unix_millis() + 1_000, // conservative 1s initial seed
        );
        TaskOutcome::WaitingRetry {
            error_class: error_class.clone(),
            error_message: error_message.clone(),
            error_details: error_details.clone(),
            retry_after,
            recorded_at: now,
        }
    } else {
        TaskOutcome::FailedNonretryable {
            error_class: error_class.clone(),
            error_message: error_message.clone(),
            error_details: error_details.clone(),
            recorded_at: now,
        }
    };

    let result = with_serializable_retry(&metrics, "report_failure", || {
        let state = Arc::clone(&state);
        let lease_ref = lease_ref.clone();
        let outcome = outcome.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            match tx.complete_task(&lease_ref, outcome).await {
                Ok(()) => {
                    tx.commit_dyn().await?;
                    Ok(ReportFlow::Settled)
                }
                Err(StorageError::NotFound) => {
                    let _ = tx.rollback_dyn().await;
                    Ok(ReportFlow::LeaseExpired)
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
        Ok(ReportFlow::Settled) => {
            let final_status = if retryable {
                TerminalState::WAITING_RETRY
            } else {
                TerminalState::FAILED_NONRETRYABLE
            };
            metrics.terminal_total.add(1, &[]);
            metrics
                .error_class_total
                .add(1, &[KeyValue::new("error_class", error_class.clone())]);
            if retryable {
                metrics.retry_total.add(1, &[]);
            }
            let mut resp = ReportFailureResponse::default();
            resp.task_id = Some(task_id_str);
            resp.final_status = final_status;
            if retryable {
                resp.retry_after = Some(Box::new(timestamp_proto(Timestamp::from_unix_millis(
                    now.as_unix_millis() + 1_000,
                ))));
            }
            resp.server_version = Some(Box::new(server_semver()));
            Ok(resp)
        }
        Ok(ReportFlow::LeaseExpired) => {
            metrics.lease_expired_total.add(1, &[]);
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
    Settled,
    LeaseExpired,
}

// ---------------------------------------------------------------------------
// Deregister
// ---------------------------------------------------------------------------

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
