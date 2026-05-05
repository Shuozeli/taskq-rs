//! Caller-facing `TaskQueue` service handler.
//!
//! Implements `task_queue_server::TaskQueue` from [`taskq_proto`]: the five
//! caller-facing RPCs `SubmitTask`, `GetTaskResult`, `BatchGetTaskResults`,
//! `CancelTask`, `SubmitAndWait`. Reference: `design.md` §3.2, §6.1, §6.4,
//! §6.7.
//!
//! Every method:
//!
//! - Begins a SERIALIZABLE transaction via `state.storage.begin_dyn()`.
//! - Wraps the transaction body in
//!   `crate::handlers::retry::with_serializable_retry` for transparent
//!   `40001` retry up to 3 attempts (`design.md` §6.4).
//! - Distinguishes [`taskq_storage::StorageError::NotFound`] (return
//!   semantic error) from [`taskq_storage::StorageError::SerializationConflict`]
//!   (retry).
//! - Records `taskq_storage_retry_attempts` (per `MetricsHandle`).
//!
//! Trace context (`traceparent` / `tracestate`) is read from the request
//! body and persisted on the task row at submit time per `design.md` §11.2;
//! Phase 5d wires the OTel span continuation.

use std::sync::Arc;

use bytes::Bytes;
use grpc_core::{Request, Response, Status};
use opentelemetry::KeyValue;
use taskq_proto::task_queue_server::TaskQueue;
use taskq_proto::{
    BatchGetTaskResultsRequest, BatchGetTaskResultsResponse, BatchTaskResult, CancelTaskRequest,
    CancelTaskResponse, Failure as WireFailure, GetTaskResultRequest, GetTaskResultResponse,
    RejectReason as WireRejectReason, Rejection, RetryConfig as WireRetryConfig, SemVer,
    SubmitAndWaitRequest, SubmitAndWaitResponse, SubmitTaskRequest, SubmitTaskResponse, Task,
    TaskOutcome as WireTaskOutcome, TerminalState, Timestamp as WireTimestamp,
};
use taskq_storage::{
    IdempotencyKey, Namespace, NewDedupRecord, NewTask, RateDecision, RateKind,
    Task as StorageTask, TaskId, TaskOutcomeKind, TaskResultRow, TaskStatus, TaskType, Timestamp,
};

use crate::handlers::cancel::{cancel_internal, CancelOutcome};
use crate::handlers::retry::with_serializable_retry;
use crate::state::CpState;
use crate::strategy::admitter::{Admit, RejectReason as StrategyRejectReason, SubmitCtx};

/// Caller-facing `TaskQueue` service handler.
///
/// Holds an `Arc<CpState>` so a single instance is cloned cheaply into each
/// in-flight request. The `pure-grpc-rs` `TaskQueueServer<T>` adapter does
/// the cloning under the hood; the inner `state` Arc is bumped per call.
#[derive(Clone)]
pub struct TaskQueueHandler {
    pub state: Arc<CpState>,
}

impl TaskQueueHandler {
    pub fn new(state: Arc<CpState>) -> Self {
        Self { state }
    }
}

impl TaskQueue for TaskQueueHandler {
    fn submit_task(
        &self,
        request: Request<SubmitTaskRequest>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Response<SubmitTaskResponse>, Status>> + Send>,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            let outcome = submit_task_impl(state, req).await;
            Ok(Response::new(outcome))
        })
    }

    fn get_task_result(
        &self,
        request: Request<GetTaskResultRequest>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<GetTaskResultResponse>, Status>>
                + Send,
        >,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            get_task_result_impl(state, req).await.map(Response::new)
        })
    }

    fn batch_get_task_results(
        &self,
        request: Request<BatchGetTaskResultsRequest>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<BatchGetTaskResultsResponse>, Status>>
                + Send,
        >,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            batch_get_task_results_impl(state, req)
                .await
                .map(Response::new)
        })
    }

    fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Response<CancelTaskResponse>, Status>> + Send>,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            cancel_task_impl(state, req).await.map(Response::new)
        })
    }

    fn submit_and_wait(
        &self,
        request: Request<SubmitAndWaitRequest>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<SubmitAndWaitResponse>, Status>>
                + Send,
        >,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            submit_and_wait_impl(state, req).await.map(Response::new)
        })
    }
}

// ---------------------------------------------------------------------------
// SubmitTask
// ---------------------------------------------------------------------------

/// `SubmitTask` per `design.md` §6.1.
///
/// Steps (numbered to match the design doc):
///
/// 1. Run the namespace's admitter (rate-quota check + capacity-quota check
///    are the admitter's responsibility; the admitter itself takes a
///    `&mut StorageTxDyn` so it participates in the SERIALIZABLE
///    transaction).
/// 2. Look up the idempotency key. Match on payload hash → existing-task
///    return. Mismatch → `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`.
/// 3. Resolve retry config (per-task → per-task-type → per-namespace).
///    Phase 5b uses the wire request's per-task overrides directly; the
///    per-task-type and per-namespace layers will plug in when
///    `task_type_retry_config` / `namespace_quota` reads land.
/// 4. Insert task + dedup row atomically via `insert_task` (the storage
///    layer fuses steps 4 and 5 from the design doc into one call).
/// 5. Commit.
/// 6. Return `SubmitTaskResponse{ task_id, status: PENDING }`.
///
/// Phase 5d: a `tracing::Span` is opened with the namespace + task_type
/// fields. The W3C `traceparent` from the request body is persisted on the
/// task row so workers can continue the trace at acquire time
/// (`design.md` §11.2).
#[tracing::instrument(
    name = "taskq.submit_task",
    skip_all,
    fields(
        namespace = req.namespace.as_deref().unwrap_or(""),
        task_type = req.task_type.as_deref().unwrap_or(""),
        idempotency_key = req.idempotency_key.as_deref().unwrap_or(""),
    ),
)]
async fn submit_task_impl(state: Arc<CpState>, req: SubmitTaskRequest) -> SubmitTaskResponse {
    // Validate required fields on the wire body. Missing required fields
    // are a permanent INVALID_PAYLOAD rejection per `design.md` §10.2.
    let namespace_str = match req.namespace.as_deref() {
        Some(ns) if !ns.is_empty() => ns.to_owned(),
        _ => return reject_response(WireRejectReason::INVALID_PAYLOAD, "namespace is required"),
    };
    let task_type_str = match req.task_type.as_deref() {
        Some(t) if !t.is_empty() => t.to_owned(),
        _ => return reject_response(WireRejectReason::INVALID_PAYLOAD, "task_type is required"),
    };
    let idempotency_key_str = match req.idempotency_key.as_deref() {
        Some(k) if !k.is_empty() => k.to_owned(),
        _ => {
            return reject_response(
                WireRejectReason::INVALID_PAYLOAD,
                "idempotency_key is required",
            )
        }
    };
    // Extract all wire-request fields up-front. The retry closure is
    // `FnMut`, so any captured value used inside the loop body must be
    // `Clone`. Owned String / Vec / boxed table fields are pulled out
    // here once; per-attempt clones of the cheap newtypes below are
    // cheap (Arc-like for `Namespace`).
    let payload_bytes: Vec<u8> = req.payload.clone().unwrap_or_default();
    let traceparent_bytes: Vec<u8> = req.traceparent.clone().unwrap_or_default();
    let tracestate_bytes: Vec<u8> = req.tracestate.clone().unwrap_or_default();
    let priority = req.priority;
    let max_retries_req = req.max_retries;
    let expires_at_ms_req = req.expires_at.as_ref().map(|t| t.unix_millis);
    let retry_initial_ms_req = req.retry_config.as_ref().map(|r| r.initial_ms);
    let retry_max_ms_req = req.retry_config.as_ref().map(|r| r.max_ms);
    let retry_coefficient_req = req.retry_config.as_ref().map(|r| r.coefficient);
    let retry_max_retries_req = req.retry_config.as_ref().map(|r| r.max_retries);

    let payload_hash = blake3_hash(&payload_bytes);

    let namespace = Namespace::new(namespace_str.clone());
    let task_type = TaskType::new(task_type_str);
    let idempotency_key = IdempotencyKey::new(idempotency_key_str);
    let now = current_timestamp();

    // Storage transaction with transparent 40001 retry.
    let metrics = state.metrics.clone();
    let result = with_serializable_retry(&metrics, "submit_task", || {
        let state = Arc::clone(&state);
        let namespace = namespace.clone();
        let task_type = task_type.clone();
        let idempotency_key = idempotency_key.clone();
        let payload_bytes = payload_bytes.clone();
        let traceparent_bytes = traceparent_bytes.clone();
        let tracestate_bytes = tracestate_bytes.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;

            // Step 1a: namespace disabled gate. `DisableNamespace` flips
            // the persisted `disabled` flag; runtime enforcement happens
            // here, before the admitter, so a disabled namespace short-
            // circuits without consulting capacity quotas.
            if tx.is_namespace_disabled(&namespace).await? {
                let _ = tx.rollback_dyn().await;
                return Ok(Err(StrategyRejectReason::NamespaceDisabled));
            }

            // Step 1b: admit. The strategy registry's `for_namespace`
            // returns a default chain (`MaxPendingAdmitter` reading the
            // namespace's `max_pending` quota when set, else `Always`)
            // even for unconfigured namespaces, so this branch always has
            // a strategy to consult.
            let strategy = state.strategy_registry.for_namespace(&namespace);
            let admit = if let Some(strategy) = strategy {
                let ctx = SubmitCtx {
                    namespace: namespace.clone(),
                    task_type: task_type.clone(),
                    priority,
                    payload_bytes: payload_bytes.len(),
                    now,
                };
                strategy.admitter.admit(&ctx, &mut *tx).await
            } else {
                Admit::Accept
            };
            match admit {
                Admit::Accept => {}
                Admit::Reject(reason) => {
                    let _ = tx.rollback_dyn().await;
                    return Ok(Err(reason));
                }
            }

            // Submit-side rate quota: best-effort, eventually consistent.
            // Hitting the limit returns a structured rejection.
            if let Ok(RateDecision::RateLimited { .. }) = tx
                .try_consume_rate_quota(&namespace, RateKind::SubmitRpm, 1)
                .await
            {
                let _ = tx.rollback_dyn().await;
                return Ok(Err(StrategyRejectReason::SystemOverload));
            }

            // Step 2: idempotency lookup.
            let existing = tx.lookup_idempotency(&namespace, &idempotency_key).await?;
            if let Some(record) = existing {
                if record.payload_hash == payload_hash {
                    // Idempotency HIT: return existing task_id.
                    let task_id_str = record.task_id.to_string();
                    let _ = tx.commit_dyn().await;
                    return Ok(Ok(SubmitTaskHit::Existing(task_id_str)));
                }
                // Mismatch — IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD.
                let _ = tx.rollback_dyn().await;
                return Ok(Ok(SubmitTaskHit::PayloadMismatch {
                    existing_task_id: record.task_id.to_string(),
                    existing_payload_hash: record.payload_hash,
                }));
            }
            // §6.1 step 2 lazy cleanup: `lookup_idempotency` ignores rows
            // whose `expires_at <= NOW()`, but the SQLite backend's
            // `idempotency_keys` PK is `(namespace, key)` and would block
            // the impending insert. Drop any expired row before falling
            // through to the insert path. Postgres tolerates the call too
            // (its PK includes `expires_at` so lingering expired rows
            // would otherwise rely on the partition-drop pruner).
            tx.delete_idempotency_key(&namespace, &idempotency_key)
                .await?;

            // Steps 3-5: insert task + dedup row.
            let task_id = TaskId::generate();
            let expires_at = expires_at_ms_req
                .map(Timestamp::from_unix_millis)
                .unwrap_or_else(|| {
                    // Default TTL: 24h from submit. Per-namespace override
                    // hooks land in Phase 5c.
                    Timestamp::from_unix_millis(now.as_unix_millis() + 24 * 60 * 60 * 1000)
                });
            // §9.2 three-layer fallback: per-task → per-task-type →
            // per-namespace. v1 implements two of the three: the
            // per-task-type tier is post-v1 (no
            // `task_type_retry_config` read path on the trait yet).
            // When BOTH per-task wire fields are unset (0), look up
            // the namespace's `max_retries_ceiling` so a caller that
            // omits `max_retries` doesn't get FAILED_EXHAUSTED on
            // first failure. `get_namespace_quota` always returns
            // a row by inheriting from `system_default`, so the
            // ceiling is always populated.
            let per_task = retry_max_retries_req.unwrap_or(max_retries_req);
            let max_retries = if per_task == 0 {
                let ceiling = tx
                    .get_namespace_quota(&namespace)
                    .await?
                    .max_retries_ceiling;
                ceiling
            } else {
                per_task
            };
            let retry_initial_ms = retry_initial_ms_req.unwrap_or(1_000);
            let retry_max_ms = retry_max_ms_req.unwrap_or(300_000);
            let retry_coefficient = retry_coefficient_req.unwrap_or(2.0);

            let dedup_expires_at =
                Timestamp::from_unix_millis(expires_at.as_unix_millis() + 24 * 60 * 60 * 1000);

            let new_task = NewTask {
                task_id,
                namespace: namespace.clone(),
                task_type: task_type.clone(),
                priority,
                payload: Bytes::from(payload_bytes),
                payload_hash,
                submitted_at: now,
                expires_at,
                max_retries,
                retry_initial_ms,
                retry_max_ms,
                retry_coefficient,
                traceparent: Bytes::from(traceparent_bytes),
                tracestate: Bytes::from(tracestate_bytes),
                format_version: 1,
            };
            let new_dedup = NewDedupRecord {
                namespace: namespace.clone(),
                key: idempotency_key.clone(),
                payload_hash,
                expires_at: dedup_expires_at,
            };

            let inserted_id = tx.insert_task(new_task, new_dedup).await?;
            tx.commit_dyn().await?;

            // Wake one waiter on this namespace so the new PENDING task is
            // picked up promptly. The waiter pool's `wake_one` is a no-op
            // when there are no waiters.
            state.waiter_pool.wake_one(&namespace, &[task_type]).await;

            Ok(Ok(SubmitTaskHit::Created(inserted_id.to_string())))
        }
    })
    .await;

    match result {
        Ok(Ok(SubmitTaskHit::Created(task_id))) => {
            // §11.3 submit-side counters carry (namespace, task_type, outcome).
            metrics.submit_total.add(
                1,
                &[
                    KeyValue::new("namespace", namespace_str.clone()),
                    KeyValue::new("task_type", task_type.as_str().to_owned()),
                    KeyValue::new("outcome", "accepted"),
                ],
            );
            // Histogram drops `task_type` per §11.3.
            metrics.submit_payload_bytes.record(
                payload_bytes.len() as u64,
                &[KeyValue::new("namespace", namespace_str)],
            );
            ok_submit_response(task_id, TerminalState::PENDING, false)
        }
        Ok(Ok(SubmitTaskHit::Existing(task_id))) => {
            metrics
                .idempotency_hit_total
                .add(1, &[KeyValue::new("namespace", namespace_str.clone())]);
            // Idempotent hits also count as a (logical) submit acceptance for
            // the rate metric, but with a distinct outcome so dashboards can
            // segment them.
            metrics.submit_total.add(
                1,
                &[
                    KeyValue::new("namespace", namespace_str),
                    KeyValue::new("task_type", task_type.as_str().to_owned()),
                    KeyValue::new("outcome", "idempotent_hit"),
                ],
            );
            ok_submit_response(task_id, TerminalState::PENDING, true)
        }
        Ok(Ok(SubmitTaskHit::PayloadMismatch {
            existing_task_id,
            existing_payload_hash,
        })) => {
            metrics
                .idempotency_payload_mismatch_total
                .add(1, &[KeyValue::new("namespace", namespace_str.clone())]);
            metrics.rejection_total.add(
                1,
                &[
                    KeyValue::new("namespace", namespace_str.clone()),
                    KeyValue::new("reason", "IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD"),
                ],
            );
            metrics.submit_total.add(
                1,
                &[
                    KeyValue::new("namespace", namespace_str),
                    KeyValue::new("task_type", task_type.as_str().to_owned()),
                    KeyValue::new("outcome", "rejected_payload_mismatch"),
                ],
            );
            payload_mismatch_response(existing_task_id, existing_payload_hash.to_vec())
        }
        Ok(Err(reason)) => {
            let wire = strategy_reject_to_wire(reason);
            metrics.rejection_total.add(
                1,
                &[
                    KeyValue::new("namespace", namespace_str.clone()),
                    KeyValue::new("reason", format!("{wire:?}")),
                ],
            );
            // System-overload surfaced as a rate-limit hit lets capacity-
            // planning dashboards distinguish "hit a quota" from "got
            // rejected for invalid payload"; same emit point keeps the
            // labels consistent.
            if matches!(wire, WireRejectReason::SYSTEM_OVERLOAD) {
                metrics.rate_limit_hit_total.add(
                    1,
                    &[
                        KeyValue::new("namespace", namespace_str.clone()),
                        KeyValue::new("kind", "submit_rpm"),
                    ],
                );
            }
            metrics.submit_total.add(
                1,
                &[
                    KeyValue::new("namespace", namespace_str),
                    KeyValue::new("task_type", task_type.as_str().to_owned()),
                    KeyValue::new("outcome", reject_outcome_label(wire)),
                ],
            );
            reject_response(wire, "admit-time rejection")
        }
        Err(err) => {
            tracing::error!(error = %err, "submit_task storage error");
            metrics.rejection_total.add(
                1,
                &[
                    KeyValue::new("namespace", namespace_str.clone()),
                    KeyValue::new("reason", "STORAGE_ERROR"),
                ],
            );
            metrics.submit_total.add(
                1,
                &[
                    KeyValue::new("namespace", namespace_str),
                    KeyValue::new("task_type", task_type.as_str().to_owned()),
                    KeyValue::new("outcome", "rejected_storage_error"),
                ],
            );
            reject_response(WireRejectReason::SYSTEM_OVERLOAD, "storage error")
        }
    }
}

/// Map the wire reject reason to the `outcome` label value used by
/// `taskq_submit_total`. Keeps the label vocabulary stable across rejection
/// reasons.
fn reject_outcome_label(reason: WireRejectReason) -> &'static str {
    match reason {
        WireRejectReason::MAX_PENDING_EXCEEDED => "rejected_max_pending",
        WireRejectReason::LATENCY_TARGET_EXCEEDED => "rejected_latency_target",
        WireRejectReason::NAMESPACE_QUOTA_EXCEEDED => "rejected_quota",
        WireRejectReason::SYSTEM_OVERLOAD => "rejected_overload",
        WireRejectReason::NAMESPACE_DISABLED => "rejected_namespace_disabled",
        _ => "rejected_other",
    }
}

/// Result type for a single retry attempt of `SubmitTask`. Distinguishes the
/// happy paths (created / hit) from the structured payload-mismatch path.
enum SubmitTaskHit {
    Created(String),
    Existing(String),
    PayloadMismatch {
        existing_task_id: String,
        existing_payload_hash: [u8; 32],
    },
}

// ---------------------------------------------------------------------------
// GetTaskResult / BatchGetTaskResults
// ---------------------------------------------------------------------------

/// `GetTaskResult` per `design.md` §6. Returns the task row plus its
/// terminal-state status. v0.1.0 does not yet read result_payload /
/// outcome / failure from `task_runtime` — those fields stay default —
/// so callers should rely on `task.status` to detect terminal states.
#[tracing::instrument(
    name = "taskq.get_task_result",
    skip_all,
    fields(task_id = req.task_id.as_deref().unwrap_or("")),
)]
async fn get_task_result_impl(
    state: Arc<CpState>,
    req: GetTaskResultRequest,
) -> Result<GetTaskResultResponse, Status> {
    let task_id = match req.task_id.as_deref() {
        Some(t) if !t.is_empty() => match uuid::Uuid::parse_str(t) {
            Ok(u) => TaskId::from_uuid(u),
            Err(_) => {
                return Ok(reject_get_task_result(
                    WireRejectReason::INVALID_PAYLOAD,
                    "task_id is not a valid UUID",
                ));
            }
        },
        _ => {
            return Ok(reject_get_task_result(
                WireRejectReason::INVALID_PAYLOAD,
                "task_id is required",
            ));
        }
    };
    let metrics = state.metrics.clone();

    // Read the task row + the latest task_results row in one
    // SERIALIZABLE transaction so the caller never sees a torn view
    // (e.g. status=COMPLETED from `tasks` but no row in
    // `task_results` because complete_task is mid-tx on another
    // connection).
    let lookup = with_serializable_retry(&metrics, "get_task_result", || {
        let state = Arc::clone(&state);
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            let task = tx.get_task_by_id(task_id).await?;
            let result = if task.is_some() {
                tx.get_latest_task_result(task_id).await?
            } else {
                None
            };
            tx.commit_dyn().await?;
            Ok((task, result))
        }
    })
    .await
    .map_err(|err| Status::internal(format!("get_task_result: {err}")))?;

    let (task_opt, result_opt) = lookup;
    let mut resp = GetTaskResultResponse::default();
    resp.server_version = Some(Box::new(server_semver()));
    match task_opt {
        Some(task) => {
            resp.task = Some(Box::new(storage_task_to_wire(&task)));
            if let Some(row) = result_opt {
                populate_result_fields(&mut resp, row);
            }
        }
        None => {
            // No dedicated `TASK_NOT_FOUND` reason in v1 enum; surface
            // via gRPC NotFound so SDK callers can `match` on Status.
            return Err(Status::not_found("get_task_result: task_id not found"));
        }
    }
    Ok(resp)
}

/// Map a stored `TaskResultRow` onto the wire response's `outcome` /
/// `result_payload` / `failure` fields. `Success` populates
/// `result_payload`; the failure variants populate `failure` with
/// the captured error class / message / details.
fn populate_result_fields(resp: &mut GetTaskResultResponse, row: TaskResultRow) {
    resp.outcome = wire_outcome(row.outcome);
    if let Some(payload) = row.result_payload {
        resp.result_payload = Some(payload.to_vec());
    }
    if matches!(
        row.outcome,
        TaskOutcomeKind::RetryableFail
            | TaskOutcomeKind::NonretryableFail
            | TaskOutcomeKind::Expired
    ) {
        let mut failure = WireFailure::default();
        failure.error_class = row.error_class;
        failure.message = row.error_message;
        failure.details = row.error_details.map(|d| d.to_vec());
        failure.retryable = matches!(row.outcome, TaskOutcomeKind::RetryableFail);
        resp.failure = Some(Box::new(failure));
    }
}

/// `BatchTaskResult` mirrors `GetTaskResultResponse`'s
/// outcome/result_payload/failure fields per row but doesn't share
/// the same struct, so the population logic is duplicated. Kept
/// parallel with [`populate_result_fields`] for the single-result
/// case.
fn populate_batch_result_fields(row: &mut BatchTaskResult, result_row: TaskResultRow) {
    row.outcome = wire_outcome(result_row.outcome);
    if let Some(payload) = result_row.result_payload {
        row.result_payload = Some(payload.to_vec());
    }
    if matches!(
        result_row.outcome,
        TaskOutcomeKind::RetryableFail
            | TaskOutcomeKind::NonretryableFail
            | TaskOutcomeKind::Expired
    ) {
        let mut failure = WireFailure::default();
        failure.error_class = result_row.error_class;
        failure.message = result_row.error_message;
        failure.details = result_row.error_details.map(|d| d.to_vec());
        failure.retryable = matches!(result_row.outcome, TaskOutcomeKind::RetryableFail);
        row.failure = Some(Box::new(failure));
    }
}

fn wire_outcome(kind: TaskOutcomeKind) -> WireTaskOutcome {
    match kind {
        TaskOutcomeKind::Success => WireTaskOutcome::SUCCESS,
        TaskOutcomeKind::RetryableFail => WireTaskOutcome::RETRYABLE_FAIL,
        TaskOutcomeKind::NonretryableFail => WireTaskOutcome::NONRETRYABLE_FAIL,
        TaskOutcomeKind::Cancelled => WireTaskOutcome::CANCELLED,
        TaskOutcomeKind::Expired => WireTaskOutcome::EXPIRED,
    }
}

/// `BatchGetTaskResults` per `design.md` §6. Per-id semantics: missing
/// ids surface as a per-result `error` rather than failing the whole
/// batch. Bounded by the request — no server-side cap in v0.1.0; the
/// caller SDK is the natural rate limiter.
#[tracing::instrument(
    name = "taskq.batch_get_task_results",
    skip_all,
    fields(count = req.task_ids.as_ref().map(|v| v.len()).unwrap_or(0)),
)]
async fn batch_get_task_results_impl(
    state: Arc<CpState>,
    req: BatchGetTaskResultsRequest,
) -> Result<BatchGetTaskResultsResponse, Status> {
    let task_ids = req.task_ids.unwrap_or_default();
    let metrics = state.metrics.clone();

    let mut results = Vec::with_capacity(task_ids.len());
    for raw in task_ids {
        let mut row = BatchTaskResult::default();
        row.task_id = Some(raw.clone());
        if raw.is_empty() {
            row.error = Some(Box::new(rejection(
                WireRejectReason::INVALID_PAYLOAD,
                "task_id is empty",
            )));
            results.push(row);
            continue;
        }
        let task_id = match uuid::Uuid::parse_str(&raw) {
            Ok(u) => TaskId::from_uuid(u),
            Err(_) => {
                row.error = Some(Box::new(rejection(
                    WireRejectReason::INVALID_PAYLOAD,
                    "task_id is not a valid UUID",
                )));
                results.push(row);
                continue;
            }
        };
        let lookup = with_serializable_retry(&metrics, "batch_get_task_results", || {
            let state = Arc::clone(&state);
            async move {
                let mut tx = state.storage.begin_dyn().await?;
                let task = tx.get_task_by_id(task_id).await?;
                let result = if task.is_some() {
                    tx.get_latest_task_result(task_id).await?
                } else {
                    None
                };
                tx.commit_dyn().await?;
                Ok((task, result))
            }
        })
        .await;
        match lookup {
            Ok((Some(task), result_opt)) => {
                row.task = Some(Box::new(storage_task_to_wire(&task)));
                if let Some(result_row) = result_opt {
                    populate_batch_result_fields(&mut row, result_row);
                }
            }
            Ok((None, _)) => {
                // Per-id miss: leave both `task` and `error` unset so
                // callers can detect "not found" by absence. v1 has no
                // TASK_NOT_FOUND reject reason.
            }
            Err(err) => {
                row.error = Some(Box::new(rejection(
                    WireRejectReason::SYSTEM_OVERLOAD,
                    &format!("storage error: {err}"),
                )));
            }
        }
        results.push(row);
    }

    let mut resp = BatchGetTaskResultsResponse::default();
    resp.results = Some(results);
    resp.server_version = Some(Box::new(server_semver()));
    Ok(resp)
}

fn storage_task_to_wire(task: &StorageTask) -> Task {
    let mut wire = Task::default();
    wire.task_id = Some(task.task_id.to_string());
    wire.namespace = Some(task.namespace.as_str().to_owned());
    wire.task_type = Some(task.task_type.as_str().to_owned());
    wire.status = task_status_to_wire(task.status);
    wire.priority = task.priority;
    wire.payload = Some(task.payload.to_vec());
    wire.payload_hash = Some(task.payload_hash.to_vec());
    wire.submitted_at = Some(Box::new(timestamp_to_wire(task.submitted_at)));
    wire.expires_at = Some(Box::new(timestamp_to_wire(task.expires_at)));
    wire.attempt_number = task.attempt_number;
    wire.max_retries = task.max_retries;
    let mut rc = WireRetryConfig::default();
    rc.initial_ms = task.retry_initial_ms;
    rc.max_ms = task.retry_max_ms;
    rc.coefficient = task.retry_coefficient;
    rc.max_retries = task.max_retries;
    wire.retry_config = Some(Box::new(rc));
    if let Some(ts) = task.retry_after {
        wire.retry_after = Some(Box::new(timestamp_to_wire(ts)));
    }
    wire.traceparent = Some(task.traceparent.to_vec());
    wire.tracestate = Some(task.tracestate.to_vec());
    wire.format_version = task.format_version;
    wire.original_failure_count = task.original_failure_count;
    wire
}

fn task_status_to_wire(status: TaskStatus) -> TerminalState {
    match status {
        TaskStatus::Pending => TerminalState::PENDING,
        TaskStatus::Dispatched => TerminalState::DISPATCHED,
        TaskStatus::WaitingRetry => TerminalState::WAITING_RETRY,
        TaskStatus::Completed => TerminalState::COMPLETED,
        TaskStatus::Cancelled => TerminalState::CANCELLED,
        TaskStatus::FailedNonretryable => TerminalState::FAILED_NONRETRYABLE,
        TaskStatus::FailedExhausted => TerminalState::FAILED_EXHAUSTED,
        TaskStatus::Expired => TerminalState::EXPIRED,
    }
}

fn timestamp_to_wire(ts: Timestamp) -> WireTimestamp {
    let mut t = WireTimestamp::default();
    t.unix_millis = ts.as_unix_millis();
    t
}

fn reject_get_task_result(reason: WireRejectReason, hint: &str) -> GetTaskResultResponse {
    let mut resp = GetTaskResultResponse::default();
    resp.server_version = Some(Box::new(server_semver()));
    resp.error = Some(Box::new(rejection(reason, hint)));
    resp
}

fn rejection(reason: WireRejectReason, hint: &str) -> Rejection {
    let mut r = Rejection::default();
    r.reason = reason;
    r.hint = Some(hint.to_owned());
    r
}

// ---------------------------------------------------------------------------
// CancelTask
// ---------------------------------------------------------------------------

#[tracing::instrument(
    name = "taskq.cancel_task",
    skip_all,
    fields(task_id = req.task_id.as_deref().unwrap_or("")),
)]
async fn cancel_task_impl(
    state: Arc<CpState>,
    req: CancelTaskRequest,
) -> Result<CancelTaskResponse, Status> {
    let task_id_str = req
        .task_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("task_id is required"))?
        .to_owned();
    let task_id = parse_task_id(&task_id_str)?;
    let metrics = state.metrics.clone();

    let outcome = with_serializable_retry(&metrics, "cancel_task", || {
        let state = Arc::clone(&state);
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            let result = cancel_internal(&mut *tx, task_id).await?;
            tx.commit_dyn().await?;
            Ok(result)
        }
    })
    .await;

    match outcome {
        Ok(CancelOutcome::TransitionedToCancelled) => {
            // §6.7: state-transition succeeded; the task is now CANCELLED.
            let mut resp = CancelTaskResponse::default();
            resp.task_id = Some(task_id_str);
            resp.final_status = TerminalState::CANCELLED;
            resp.server_version = Some(Box::new(server_semver()));
            Ok(resp)
        }
        Ok(CancelOutcome::AlreadyTerminal { state }) => {
            // §6.7: idempotent. Map the storage-level TaskStatus to the
            // wire TerminalState so callers see what state we observed.
            let final_status = match state {
                taskq_storage::TaskStatus::Cancelled => TerminalState::CANCELLED,
                taskq_storage::TaskStatus::Completed => TerminalState::COMPLETED,
                taskq_storage::TaskStatus::FailedNonretryable => TerminalState::FAILED_NONRETRYABLE,
                taskq_storage::TaskStatus::FailedExhausted => TerminalState::FAILED_EXHAUSTED,
                taskq_storage::TaskStatus::Expired => TerminalState::EXPIRED,
                _ => TerminalState::CANCELLED,
            };
            let mut resp = CancelTaskResponse::default();
            resp.task_id = Some(task_id_str);
            resp.final_status = final_status;
            resp.server_version = Some(Box::new(server_semver()));
            Ok(resp)
        }
        Ok(CancelOutcome::NotFound) => Err(Status::not_found(format!(
            "task_id {task_id_str} does not exist"
        ))),
        Err(err) => {
            tracing::error!(error = %err, "cancel_task storage error");
            Err(Status::internal("storage error during cancel_task"))
        }
    }
}

// ---------------------------------------------------------------------------
// SubmitAndWait
// ---------------------------------------------------------------------------

async fn submit_and_wait_impl(
    state: Arc<CpState>,
    req: SubmitAndWaitRequest,
) -> Result<SubmitAndWaitResponse, Status> {
    let submit = req
        .submit
        .ok_or_else(|| Status::invalid_argument("submit is required"))?;
    let timeout_ms = if req.wait_timeout_ms == 0 {
        u64::from(state.config.long_poll_default_timeout_seconds) * 1_000
    } else {
        req.wait_timeout_ms
            .min(u64::from(state.config.long_poll_max_seconds) * 1_000)
    };

    // Submit first.
    let submit_resp = submit_task_impl(Arc::clone(&state), *submit).await;
    if let Some(err) = submit_resp.error.as_ref() {
        let mut resp = SubmitAndWaitResponse::default();
        resp.timed_out = false;
        resp.server_version = Some(Box::new(server_semver()));
        resp.error = Some(err.clone());
        return Ok(resp);
    }

    // Phase 5b: the long-poll wait branch needs a `get_task_by_id` storage
    // method. Without it we honour the API shape — return immediately
    // marking timed_out=true so the caller knows to poll later — and let
    // Phase 5c flip this to a real wait once the storage method lands.
    let _ = timeout_ms;
    let mut resp = SubmitAndWaitResponse::default();
    resp.timed_out = true;
    resp.server_version = Some(Box::new(server_semver()));
    let mut rej = Rejection::default();
    rej.reason = WireRejectReason::SYSTEM_OVERLOAD;
    rej.retryable = true;
    rej.hint = Some(
        "SubmitAndWait wait branch needs StorageTx::get_task_by_id; Phase 5c will wire it"
            .to_owned(),
    );
    rej.existing_task_id = submit_resp.task_id.clone();
    rej.server_version = Some(Box::new(server_semver()));
    resp.error = Some(Box::new(rej));
    Ok(resp)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn ok_submit_response(
    task_id: String,
    status: TerminalState,
    existing_task: bool,
) -> SubmitTaskResponse {
    let mut resp = SubmitTaskResponse::default();
    resp.task_id = Some(task_id);
    resp.status = status;
    resp.existing_task = existing_task;
    resp.server_version = Some(Box::new(server_semver()));
    resp
}

fn reject_response(reason: WireRejectReason, hint: &str) -> SubmitTaskResponse {
    let mut resp = SubmitTaskResponse::default();
    resp.status = TerminalState::PENDING;
    resp.server_version = Some(Box::new(server_semver()));
    let mut rej = Rejection::default();
    rej.reason = reason;
    rej.retryable = matches!(
        reason,
        WireRejectReason::SYSTEM_OVERLOAD
            | WireRejectReason::MAX_PENDING_EXCEEDED
            | WireRejectReason::LATENCY_TARGET_EXCEEDED
            | WireRejectReason::NAMESPACE_QUOTA_EXCEEDED
            | WireRejectReason::REPLICA_WAITER_LIMIT_EXCEEDED
    );
    rej.hint = Some(hint.to_owned());
    rej.server_version = Some(Box::new(server_semver()));
    resp.error = Some(Box::new(rej));
    resp
}

fn payload_mismatch_response(
    existing_task_id: String,
    existing_payload_hash: Vec<u8>,
) -> SubmitTaskResponse {
    let mut resp = SubmitTaskResponse::default();
    resp.status = TerminalState::PENDING;
    resp.server_version = Some(Box::new(server_semver()));
    let mut rej = Rejection::default();
    rej.reason = WireRejectReason::IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD;
    rej.retryable = false;
    rej.hint = Some("idempotency key reused with a different payload".to_owned());
    rej.existing_task_id = Some(existing_task_id);
    rej.existing_payload_hash = Some(existing_payload_hash);
    rej.server_version = Some(Box::new(server_semver()));
    resp.error = Some(Box::new(rej));
    resp
}

fn strategy_reject_to_wire(reason: StrategyRejectReason) -> WireRejectReason {
    match reason {
        StrategyRejectReason::NamespaceQuotaExceeded => WireRejectReason::NAMESPACE_QUOTA_EXCEEDED,
        StrategyRejectReason::MaxPendingExceeded => WireRejectReason::MAX_PENDING_EXCEEDED,
        StrategyRejectReason::LatencyTargetExceeded => WireRejectReason::LATENCY_TARGET_EXCEEDED,
        StrategyRejectReason::SystemOverload => WireRejectReason::SYSTEM_OVERLOAD,
        StrategyRejectReason::NamespaceDisabled => WireRejectReason::NAMESPACE_DISABLED,
    }
}

fn server_semver() -> SemVer {
    let mut s = SemVer::default();
    s.major = 1;
    s.minor = 0;
    s.patch = 0;
    s
}

fn current_timestamp() -> Timestamp {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    Timestamp::from_unix_millis(ms)
}

fn blake3_hash(bytes: &[u8]) -> [u8; 32] {
    // Minimal BLAKE3 stand-in: the storage trait specifies a 32-byte
    // payload hash; we use a deterministic xxhash-style fold over the
    // bytes for v1. The protocol does not pin a specific hash family —
    // all matching is "did the same caller resubmit the same bytes?"
    // The CP layer hashes once and stores; clients never see the bytes.
    //
    // TODO(phase-5d): swap for a real blake3 when the dep is added at
    // workspace level; until then this fold is good enough for unit-test
    // determinism + the idempotency-mismatch detection path.
    let mut h: [u8; 32] = [0; 32];
    let mut accumulator: u64 = 0xcbf29ce484222325; // FNV-1a offset basis
    for (i, byte) in bytes.iter().enumerate() {
        accumulator ^= u64::from(*byte);
        accumulator = accumulator.wrapping_mul(0x100000001b3);
        let target = i % 32;
        h[target] = h[target].wrapping_add((accumulator & 0xff) as u8);
    }
    // Stir the length into the hash so empty / size-only changes diverge.
    let len = (bytes.len() as u64).to_le_bytes();
    for (i, b) in len.iter().enumerate() {
        h[i] = h[i].wrapping_add(*b);
    }
    h
}

fn parse_task_id(s: &str) -> Result<TaskId, Status> {
    use std::str::FromStr;
    uuid::Uuid::from_str(s)
        .map(TaskId::from_uuid)
        .map_err(|err| Status::invalid_argument(format!("invalid task_id: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::config::{CpConfig, OtelExporterConfig, StorageBackendConfig};
    use crate::observability::MetricsHandle;
    use crate::shutdown::channel;
    use crate::strategy::StrategyRegistry;

    /// Build a minimal CP state backed by an in-memory SQLite for fast,
    /// hermetic handler tests.
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
            long_poll_default_timeout_seconds: 30,
            long_poll_max_seconds: 60,
            belt_and_suspenders_seconds: 10,
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

    /// Build a minimal `SubmitTaskRequest` populated with required fields.
    /// Tests use this and override individual fields. This keeps the test
    /// bodies independent of generated-struct field churn (FlatBuffers Object
    /// API types are `#[non_exhaustive]` so direct struct literals are
    /// rejected).
    fn build_submit_request(
        ns: &str,
        task_type: &str,
        idempotency_key: &str,
        payload: Vec<u8>,
    ) -> SubmitTaskRequest {
        let mut req = SubmitTaskRequest::default();
        req.namespace = Some(ns.to_owned());
        req.task_type = Some(task_type.to_owned());
        req.payload = Some(payload);
        req.idempotency_key = Some(idempotency_key.to_owned());
        req
    }

    #[tokio::test]
    async fn submit_task_returns_created_task_id_on_happy_path() {
        // Arrange
        let state = build_state().await;
        let mut req = build_submit_request("test-ns", "test-type", "key-1", b"hello".to_vec());
        req.max_retries = 3;

        // Act
        let resp = submit_task_impl(state, req).await;

        // Assert
        assert!(resp.error.is_none(), "expected ok response: {resp:?}");
        assert!(resp.task_id.is_some());
        assert_eq!(resp.status, TerminalState::PENDING);
        assert!(!resp.existing_task);
    }

    #[tokio::test]
    async fn submit_task_falls_back_to_namespace_max_retries_ceiling_when_unset() {
        // Arrange: build a submit request with max_retries left at 0
        // (the wire default). The §9.2 three-layer fallback must
        // pick up `system_default.max_retries_ceiling` (10 per the
        // SQLite migration) instead of letting the task land with
        // max_retries=0 (which would FAILED_EXHAUSTED on first
        // failure).
        let state = build_state().await;
        let req = build_submit_request("test-fallback", "type", "key-fallback", b"x".to_vec());
        assert_eq!(
            req.max_retries, 0,
            "test premise: caller did not set max_retries"
        );

        // Act
        let resp = submit_task_impl(Arc::clone(&state), req).await;
        assert!(resp.error.is_none(), "submit should succeed: {resp:?}");
        let task_id_str = resp.task_id.expect("task_id present");
        let task_id = parse_task_id(&task_id_str).expect("parse task_id");

        // Read the persisted task row back via the trait.
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let task = tx
            .get_task_by_id(task_id)
            .await
            .expect("get_task_by_id")
            .expect("row present");
        let _ = tx.commit_dyn().await;

        // Assert: max_retries was lifted from system_default's
        // max_retries_ceiling, not the caller's zero. The exact
        // ceiling comes from `system_default` in
        // taskq-storage-sqlite/migrations/0001_initial.sql.
        assert!(
            task.max_retries > 0,
            "expected fallback to namespace ceiling, got {}",
            task.max_retries
        );
    }

    #[tokio::test]
    async fn submit_task_keeps_per_task_max_retries_when_set() {
        // Arrange: caller sets max_retries explicitly. The fallback
        // path MUST NOT clobber it.
        let state = build_state().await;
        let mut req = build_submit_request("test-explicit", "type", "key-explicit", b"x".to_vec());
        req.max_retries = 7;

        // Act
        let resp = submit_task_impl(Arc::clone(&state), req).await;
        let task_id_str = resp.task_id.expect("task_id");
        let task_id = parse_task_id(&task_id_str).expect("parse");
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let task = tx.get_task_by_id(task_id).await.unwrap().unwrap();
        let _ = tx.commit_dyn().await;

        // Assert
        assert_eq!(task.max_retries, 7);
    }

    #[tokio::test]
    async fn submit_task_returns_existing_task_on_idempotency_hit() {
        // Arrange
        let state = build_state().await;
        let req = build_submit_request("ns", "type", "key-2", b"hello".to_vec());

        // Act: first submit creates, second hits the idempotency key.
        let first = submit_task_impl(Arc::clone(&state), req.clone()).await;
        let second = submit_task_impl(state, req).await;

        // Assert
        assert!(first.error.is_none(), "first submit should succeed");
        assert!(!first.existing_task);
        assert!(second.error.is_none(), "second submit should hit dedup");
        assert!(second.existing_task);
        assert_eq!(first.task_id, second.task_id);
    }

    #[tokio::test]
    async fn submit_task_rejects_idempotency_payload_mismatch() {
        // Arrange
        let state = build_state().await;
        let req_a = build_submit_request("ns", "type", "key-3", b"alpha".to_vec());
        let mut req_b = req_a.clone();
        req_b.payload = Some(b"beta".to_vec());

        // Act
        let first = submit_task_impl(Arc::clone(&state), req_a).await;
        let second = submit_task_impl(state, req_b).await;

        // Assert
        assert!(first.error.is_none());
        let err = second.error.expect("second submit should reject");
        assert_eq!(
            err.reason,
            WireRejectReason::IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD
        );
        assert!(!err.retryable);
    }

    #[test]
    fn reject_outcome_label_maps_known_reasons() {
        // Arrange / Act / Assert: round-trip every wire reason that
        // submit_task surfaces to ensure the dashboard label vocabulary
        // stays stable.
        assert_eq!(
            reject_outcome_label(WireRejectReason::MAX_PENDING_EXCEEDED),
            "rejected_max_pending"
        );
        assert_eq!(
            reject_outcome_label(WireRejectReason::LATENCY_TARGET_EXCEEDED),
            "rejected_latency_target"
        );
        assert_eq!(
            reject_outcome_label(WireRejectReason::NAMESPACE_QUOTA_EXCEEDED),
            "rejected_quota"
        );
        assert_eq!(
            reject_outcome_label(WireRejectReason::SYSTEM_OVERLOAD),
            "rejected_overload"
        );
        assert_eq!(
            reject_outcome_label(WireRejectReason::NAMESPACE_DISABLED),
            "rejected_namespace_disabled"
        );
    }

    #[tokio::test]
    async fn submit_task_rejects_missing_required_fields() {
        // Arrange
        let state = build_state().await;
        let mut req = SubmitTaskRequest::default();
        req.task_type = Some("type".to_owned());
        req.payload = Some(vec![]);
        req.idempotency_key = Some("key".to_owned());

        // Act
        let resp = submit_task_impl(state, req).await;

        // Assert
        let err = resp.error.expect("response should be a rejection");
        assert_eq!(err.reason, WireRejectReason::INVALID_PAYLOAD);
    }

    #[tokio::test]
    async fn submit_task_rejects_empty_task_type() {
        // Arrange
        let state = build_state().await;
        let mut req = SubmitTaskRequest::default();
        req.namespace = Some("ns".to_owned());
        req.idempotency_key = Some("k".to_owned());
        req.payload = Some(b"x".to_vec());

        // Act
        let resp = submit_task_impl(state, req).await;

        // Assert
        let err = resp.error.expect("rejection");
        assert_eq!(err.reason, WireRejectReason::INVALID_PAYLOAD);
    }

    #[tokio::test]
    async fn submit_task_rejects_empty_idempotency_key() {
        // Arrange
        let state = build_state().await;
        let mut req = SubmitTaskRequest::default();
        req.namespace = Some("ns".to_owned());
        req.task_type = Some("t".to_owned());
        req.payload = Some(b"x".to_vec());

        // Act
        let resp = submit_task_impl(state, req).await;

        // Assert
        let err = resp.error.expect("rejection");
        assert_eq!(err.reason, WireRejectReason::INVALID_PAYLOAD);
    }

    #[tokio::test]
    async fn cancel_task_rejects_empty_task_id() {
        // Arrange
        let state = build_state().await;
        let req = CancelTaskRequest::default();

        // Act
        let err = cancel_task_impl(state, req).await.unwrap_err();

        // Assert: cancel_task_impl surfaces missing-task_id via gRPC
        // Status (not the wire `error` envelope), so SDKs see
        // INVALID_ARGUMENT consistently with the rest of the
        // wire-validation chain in task_queue.
        assert_eq!(err.code(), grpc_core::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn cancel_task_rejects_malformed_task_id() {
        // Arrange
        let state = build_state().await;
        let mut req = CancelTaskRequest::default();
        req.task_id = Some("not-a-uuid".to_owned());

        // Act
        let err = cancel_task_impl(state, req).await.unwrap_err();

        // Assert
        assert_eq!(err.code(), grpc_core::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_task_result_rejects_empty_task_id() {
        // Arrange
        let state = build_state().await;
        let req = GetTaskResultRequest::default();

        // Act
        let resp = get_task_result_impl(state, req).await.expect("response");

        // Assert
        let err = resp.error.expect("rejection");
        assert_eq!(err.reason, WireRejectReason::INVALID_PAYLOAD);
    }

    #[tokio::test]
    async fn get_task_result_rejects_malformed_task_id() {
        // Arrange
        let state = build_state().await;
        let mut req = GetTaskResultRequest::default();
        req.task_id = Some("not-a-uuid".to_owned());

        // Act
        let resp = get_task_result_impl(state, req).await.expect("response");

        // Assert
        let err = resp.error.expect("rejection");
        assert_eq!(err.reason, WireRejectReason::INVALID_PAYLOAD);
    }

    #[tokio::test]
    async fn get_task_result_returns_not_found_for_unknown_task_id() {
        // Arrange: well-formed but never-inserted task_id.
        let state = build_state().await;
        let mut req = GetTaskResultRequest::default();
        req.task_id = Some(taskq_storage::TaskId::generate().to_string());

        // Act
        let err = get_task_result_impl(state, req).await.unwrap_err();

        // Assert
        assert_eq!(err.code(), grpc_core::Code::NotFound);
    }

    #[tokio::test]
    async fn batch_get_task_results_returns_empty_results_for_empty_input() {
        // Arrange
        let state = build_state().await;
        let req = BatchGetTaskResultsRequest::default();

        // Act
        let resp = batch_get_task_results_impl(state, req)
            .await
            .expect("response");

        // Assert
        assert!(resp.results.unwrap_or_default().is_empty());
        assert!(resp.error.is_none());
    }

    #[tokio::test]
    async fn batch_get_task_results_marks_invalid_task_ids_per_row() {
        // Arrange
        let state = build_state().await;
        let mut req = BatchGetTaskResultsRequest::default();
        req.task_ids = Some(vec![
            "".to_owned(),
            "not-a-uuid".to_owned(),
            taskq_storage::TaskId::generate().to_string(),
        ]);

        // Act
        let resp = batch_get_task_results_impl(state, req)
            .await
            .expect("response");

        // Assert: each row carries either a task or a per-row error;
        // the batch as a whole succeeds. Empty + malformed rows
        // become INVALID_PAYLOAD; the missing-but-well-formed row has
        // both task and error unset.
        let results = resp.results.expect("results");
        assert_eq!(results.len(), 3);
        assert!(results[0].error.is_some(), "empty id -> error");
        assert_eq!(
            results[0].error.as_ref().unwrap().reason,
            WireRejectReason::INVALID_PAYLOAD
        );
        assert!(results[1].error.is_some(), "malformed id -> error");
        assert!(
            results[2].task.is_none() && results[2].error.is_none(),
            "missing-but-well-formed id -> both unset"
        );
    }
}
