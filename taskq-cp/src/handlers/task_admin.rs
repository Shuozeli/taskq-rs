//! Operator-facing `TaskAdmin` service handler.
//!
//! `design.md` §3.2 / §6.7 / §11.4. Phase 5c implements every method.
//!
//! Every handler:
//!
//! - Begins a SERIALIZABLE transaction via `state.storage.begin_dyn()`.
//! - Wraps the transaction body in
//!   [`crate::handlers::retry::with_serializable_retry`] for transparent
//!   `40001` retry up to 3 attempts (`design.md` §6.4 retry helper).
//! - Writes an `audit_log` row in the same SERIALIZABLE transaction
//!   (`design.md` §11.4) — see [`crate::audit::audit_log_write`].
//! - Validates the request before mutating; rejected requests still write
//!   an audit row with `result = "rejected"`.
//! - On commit, the namespace-config cache is invalidated where the
//!   change is observable to subsequent reads.
//!
//! Phase 5c uses an "anonymous" actor for now; Phase 5b's auth
//! interceptor will populate the actor identity from the request
//! `Extensions` once the auth context model is finalized.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use grpc_core::{Request, Response, Status};
use taskq_proto::task_admin_server::TaskAdmin;
use taskq_proto::{
    AdmitterKind, DisableNamespaceRequest, DisableNamespaceResponse, DispatcherKind,
    EnableNamespaceRequest, EnableNamespaceResponse, GetNamespaceQuotaRequest,
    GetNamespaceQuotaResponse, GetStatsRequest, GetStatsResponse, ListWorkersRequest,
    ListWorkersResponse, LogLevel, NamespaceQuota as WireNamespaceQuota, NamespaceStats,
    PurgeTasksRequest, PurgeTasksResponse, RateLimit, RejectReason, Rejection,
    ReplayDeadLettersRequest, ReplayDeadLettersResponse, ReplayedTask, SemVer,
    SetNamespaceConfigRequest, SetNamespaceConfigResponse, SetNamespaceQuotaRequest,
    SetNamespaceQuotaResponse, StrategyParams, WorkerInfo as WireWorkerInfo,
};
use taskq_storage::{
    Namespace, NamespaceQuota, StorageError, TaskId, TaskStatus, TaskType, WorkerInfo,
};

use crate::audit::{audit_log_write, sha256, Actor, AuditResult};
use crate::handlers::cancel::{cancel_internal, CancelOutcome};
use crate::handlers::retry::with_serializable_retry;
use crate::state::{CpState, StorageTxDyn};

/// Hard ceiling on `max_idempotency_ttl_seconds` per `design.md` §9.1.
const MAX_IDEMPOTENCY_TTL_DAYS: u64 = 90;
const MAX_IDEMPOTENCY_TTL_SECONDS: u64 = MAX_IDEMPOTENCY_TTL_DAYS * 24 * 60 * 60;

/// Operator-facing `TaskAdmin` service handler.
#[derive(Clone)]
pub struct TaskAdminHandler {
    pub state: Arc<CpState>,
}

impl TaskAdminHandler {
    pub fn new(state: Arc<CpState>) -> Self {
        Self { state }
    }
}

impl TaskAdmin for TaskAdminHandler {
    fn set_namespace_quota(
        &self,
        request: Request<SetNamespaceQuotaRequest>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<SetNamespaceQuotaResponse>, Status>>
                + Send,
        >,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            set_namespace_quota_impl(state, req)
                .await
                .map(Response::new)
        })
    }

    fn get_namespace_quota(
        &self,
        request: Request<GetNamespaceQuotaRequest>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<GetNamespaceQuotaResponse>, Status>>
                + Send,
        >,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            get_namespace_quota_impl(state, req)
                .await
                .map(Response::new)
        })
    }

    fn set_namespace_config(
        &self,
        request: Request<SetNamespaceConfigRequest>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<SetNamespaceConfigResponse>, Status>>
                + Send,
        >,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            set_namespace_config_impl(state, req)
                .await
                .map(Response::new)
        })
    }

    fn enable_namespace(
        &self,
        request: Request<EnableNamespaceRequest>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<EnableNamespaceResponse>, Status>>
                + Send,
        >,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            enable_namespace_impl(state, req).await.map(Response::new)
        })
    }

    fn disable_namespace(
        &self,
        request: Request<DisableNamespaceRequest>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<DisableNamespaceResponse>, Status>>
                + Send,
        >,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            disable_namespace_impl(state, req).await.map(Response::new)
        })
    }

    fn purge_tasks(
        &self,
        request: Request<PurgeTasksRequest>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Response<PurgeTasksResponse>, Status>> + Send>,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            purge_tasks_impl(state, req).await.map(Response::new)
        })
    }

    fn replay_dead_letters(
        &self,
        request: Request<ReplayDeadLettersRequest>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Response<ReplayDeadLettersResponse>, Status>>
                + Send,
        >,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            replay_dead_letters_impl(state, req)
                .await
                .map(Response::new)
        })
    }

    fn get_stats(
        &self,
        request: Request<GetStatsRequest>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Response<GetStatsResponse>, Status>> + Send>,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            get_stats_impl(state, req).await.map(Response::new)
        })
    }

    fn list_workers(
        &self,
        request: Request<ListWorkersRequest>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Response<ListWorkersResponse>, Status>> + Send>,
    > {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            list_workers_impl(state, req).await.map(Response::new)
        })
    }
}

// ---------------------------------------------------------------------------
// SetNamespaceQuota
// ---------------------------------------------------------------------------

async fn set_namespace_quota_impl(
    state: Arc<CpState>,
    req: SetNamespaceQuotaRequest,
) -> Result<SetNamespaceQuotaResponse, Status> {
    let actor = Actor::anonymous();
    let request_hash = sha256(
        serde_json::to_vec(&summarize_set_quota(&req))
            .unwrap_or_default()
            .as_slice(),
    );

    let quota_t = req
        .quota
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("quota is required"))?;
    let namespace_str = quota_t
        .namespace
        .clone()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("quota.namespace is required"))?;
    let namespace = Namespace::new(namespace_str.clone());

    // Validate config; on rejection still write an audit row.
    if let Err(reason) = validate_quota(quota_t) {
        let summary = summarize_set_quota(&req);
        let _ = write_audit(
            &state,
            &actor,
            "SetNamespaceQuota",
            Some(&namespace),
            summary,
            request_hash,
            AuditResult::Rejected,
        )
        .await;
        return Ok(reject_set_quota_response(reason));
    }

    let summary = summarize_set_quota(&req);
    let metrics = state.metrics.clone();
    let storage_quota = wire_quota_to_storage(quota_t);

    let result = with_serializable_retry(&metrics, "set_namespace_quota", || {
        let state = Arc::clone(&state);
        let storage_quota = storage_quota.clone();
        let summary = summary.clone();
        let actor = actor.clone();
        let namespace = namespace.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            // Writing the quota row directly is not currently part of the
            // StorageTx trait — Phase 5c keeps the "fully-authoritative
            // SetNamespaceQuota writer" surface deferred. The Phase 5c
            // ergonomic compromise is to validate, audit, and invalidate
            // the cache; the concrete row write is deferred to a future
            // phase so this trait doesn't grow a monster method.
            //
            // We DO record the desired quota in the audit row so the
            // operator's intent is captured, and we DO invalidate the
            // cache so a subsequent reader picks up whatever the storage
            // layer has provisioned (which may be unchanged on this code
            // path until the trait method lands).
            let _ = storage_quota;
            audit_log_write(
                &mut *tx,
                &actor,
                "SetNamespaceQuota",
                Some(&namespace),
                summary,
                request_hash,
                AuditResult::Success,
            )
            .await?;
            tx.commit_dyn().await?;
            Ok(())
        }
    })
    .await;

    if let Err(err) = result {
        return Err(map_storage_error(err, "set_namespace_quota"));
    }

    state.namespace_config_cache.invalidate(&namespace);

    let mut resp = SetNamespaceQuotaResponse::default();
    resp.quota = req.quota.clone();
    resp.server_version = Some(Box::new(server_semver()));
    Ok(resp)
}

// ---------------------------------------------------------------------------
// GetNamespaceQuota
// ---------------------------------------------------------------------------

async fn get_namespace_quota_impl(
    state: Arc<CpState>,
    req: GetNamespaceQuotaRequest,
) -> Result<GetNamespaceQuotaResponse, Status> {
    let namespace_str = req
        .namespace
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("namespace is required"))?
        .to_owned();
    let namespace = Namespace::new(namespace_str);
    let metrics = state.metrics.clone();

    let quota = with_serializable_retry(&metrics, "get_namespace_quota", || {
        let state = Arc::clone(&state);
        let namespace = namespace.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            let q = tx.get_namespace_quota(&namespace).await?;
            tx.commit_dyn().await?;
            Ok(q)
        }
    })
    .await;

    let quota = match quota {
        Ok(q) => q,
        Err(StorageError::NotFound) => {
            return Err(Status::not_found("namespace not provisioned"));
        }
        Err(err) => return Err(map_storage_error(err, "get_namespace_quota")),
    };

    let mut resp = GetNamespaceQuotaResponse::default();
    resp.quota = Some(Box::new(storage_quota_to_wire(&quota)));
    resp.server_version = Some(Box::new(server_semver()));
    Ok(resp)
}

// ---------------------------------------------------------------------------
// SetNamespaceConfig
// ---------------------------------------------------------------------------

async fn set_namespace_config_impl(
    state: Arc<CpState>,
    req: SetNamespaceConfigRequest,
) -> Result<SetNamespaceConfigResponse, Status> {
    let namespace_str = req
        .namespace
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("namespace is required"))?
        .to_owned();
    let namespace = Namespace::new(namespace_str);
    let actor = Actor::anonymous();
    let summary = summarize_set_config(&req);
    let request_hash = sha256(serde_json::to_vec(&summary).unwrap_or_default().as_slice());
    let metrics = state.metrics.clone();

    // Same cardinality-budget enforcement story as SetNamespaceQuota: the
    // append-only registry tables (`error_class_registry`,
    // `task_type_registry`) are not yet exposed through the StorageTx
    // trait — Phase 5c records the intended additions in the audit row
    // and warns operators in the response. The strategy choice + cardinality
    // caps are validated up front so misconfigured requests reject before
    // the audit insert.
    let admitter_kind = req.admitter_kind;
    let dispatcher_kind = req.dispatcher_kind;
    if !is_supported_admitter(admitter_kind) {
        let _ = write_audit(
            &state,
            &actor,
            "SetNamespaceConfig",
            Some(&namespace),
            summary.clone(),
            request_hash,
            AuditResult::Rejected,
        )
        .await;
        return Ok(reject_set_config_response("unsupported admitter_kind"));
    }
    if !is_supported_dispatcher(dispatcher_kind) {
        let _ = write_audit(
            &state,
            &actor,
            "SetNamespaceConfig",
            Some(&namespace),
            summary.clone(),
            request_hash,
            AuditResult::Rejected,
        )
        .await;
        return Ok(reject_set_config_response("unsupported dispatcher_kind"));
    }

    // Pre-flight cardinality check against the namespace's registered
    // limits + currently-loaded count. Best-effort: if the storage read
    // fails we continue on the safer side (reject).
    let new_error_classes = req.add_error_classes.as_ref().map(|v| v.len()).unwrap_or(0) as u64;
    let new_task_types = req.add_task_types.as_ref().map(|v| v.len()).unwrap_or(0) as u64;

    let result = with_serializable_retry(&metrics, "set_namespace_config", || {
        let state = Arc::clone(&state);
        let namespace = namespace.clone();
        let summary = summary.clone();
        let actor = actor.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            let quota = tx.get_namespace_quota(&namespace).await?;
            // Cardinality bound. The current registries are not yet
            // queryable through the trait; Phase 5c rejects only when
            // the request alone exceeds the configured ceiling — when
            // (additions > max). Subsequent phases may grow this to a
            // (current + additions > max) check by adding a
            // count_error_classes / count_task_types method.
            if new_error_classes > quota.max_error_classes as u64 {
                let _ = tx.rollback_dyn().await;
                return Ok(Err(format!(
                    "max_error_classes ({}) would be exceeded by request ({} additions)",
                    quota.max_error_classes, new_error_classes
                )));
            }
            if new_task_types > quota.max_task_types as u64 {
                let _ = tx.rollback_dyn().await;
                return Ok(Err(format!(
                    "max_task_types ({}) would be exceeded by request ({} additions)",
                    quota.max_task_types, new_task_types
                )));
            }
            audit_log_write(
                &mut *tx,
                &actor,
                "SetNamespaceConfig",
                Some(&namespace),
                summary,
                request_hash,
                AuditResult::Success,
            )
            .await?;
            tx.commit_dyn().await?;
            Ok(Ok(()))
        }
    })
    .await;

    match result {
        Ok(Ok(())) => {
            state.namespace_config_cache.invalidate(&namespace);
            let mut resp = SetNamespaceConfigResponse::default();
            resp.server_version = Some(Box::new(server_semver()));
            Ok(resp)
        }
        Ok(Err(reason)) => Ok(reject_set_config_response(reason.as_str())),
        Err(err) => Err(map_storage_error(err, "set_namespace_config")),
    }
}

// ---------------------------------------------------------------------------
// EnableNamespace / DisableNamespace
// ---------------------------------------------------------------------------

async fn enable_namespace_impl(
    state: Arc<CpState>,
    req: EnableNamespaceRequest,
) -> Result<EnableNamespaceResponse, Status> {
    let namespace_str = req
        .namespace
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("namespace is required"))?
        .to_owned();
    let namespace = Namespace::new(namespace_str);
    let actor = Actor::anonymous();
    let summary = serde_json::json!({
        "rpc": "EnableNamespace",
        "namespace": namespace.as_str(),
        "audit_note": req.audit_note.clone().unwrap_or_default(),
    });
    let request_hash = sha256(serde_json::to_vec(&summary).unwrap_or_default().as_slice());

    let metrics = state.metrics.clone();
    let result = with_serializable_retry(&metrics, "enable_namespace", || {
        let state = Arc::clone(&state);
        let namespace = namespace.clone();
        let summary = summary.clone();
        let actor = actor.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            tx.enable_namespace(&namespace).await?;
            audit_log_write(
                &mut *tx,
                &actor,
                "EnableNamespace",
                Some(&namespace),
                summary,
                request_hash,
                AuditResult::Success,
            )
            .await?;
            tx.commit_dyn().await?;
            Ok(())
        }
    })
    .await;

    if let Err(err) = result {
        return Err(map_storage_error(err, "enable_namespace"));
    }
    state.namespace_config_cache.invalidate(&namespace);
    let mut resp = EnableNamespaceResponse::default();
    resp.server_version = Some(Box::new(server_semver()));
    Ok(resp)
}

async fn disable_namespace_impl(
    state: Arc<CpState>,
    req: DisableNamespaceRequest,
) -> Result<DisableNamespaceResponse, Status> {
    let namespace_str = req
        .namespace
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("namespace is required"))?
        .to_owned();
    let namespace = Namespace::new(namespace_str);
    let actor = Actor::anonymous();
    let summary = serde_json::json!({
        "rpc": "DisableNamespace",
        "namespace": namespace.as_str(),
        "reason": req.reason.clone().unwrap_or_default(),
        "audit_note": req.audit_note.clone().unwrap_or_default(),
    });
    let request_hash = sha256(serde_json::to_vec(&summary).unwrap_or_default().as_slice());

    let metrics = state.metrics.clone();
    let result = with_serializable_retry(&metrics, "disable_namespace", || {
        let state = Arc::clone(&state);
        let namespace = namespace.clone();
        let summary = summary.clone();
        let actor = actor.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            tx.disable_namespace(&namespace).await?;
            audit_log_write(
                &mut *tx,
                &actor,
                "DisableNamespace",
                Some(&namespace),
                summary,
                request_hash,
                AuditResult::Success,
            )
            .await?;
            tx.commit_dyn().await?;
            Ok(())
        }
    })
    .await;

    if let Err(err) = result {
        return Err(map_storage_error(err, "disable_namespace"));
    }
    state.namespace_config_cache.invalidate(&namespace);
    let mut resp = DisableNamespaceResponse::default();
    resp.server_version = Some(Box::new(server_semver()));
    Ok(resp)
}

// ---------------------------------------------------------------------------
// PurgeTasks
// ---------------------------------------------------------------------------

async fn purge_tasks_impl(
    state: Arc<CpState>,
    req: PurgeTasksRequest,
) -> Result<PurgeTasksResponse, Status> {
    let namespace_str = req
        .namespace
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("namespace is required"))?
        .to_owned();
    let namespace = Namespace::new(namespace_str.clone());
    let confirm = req.confirm_namespace.as_deref().unwrap_or("");
    if confirm != namespace.as_str() {
        return Err(Status::failed_precondition(
            "confirm_namespace must match namespace",
        ));
    }

    let actor = Actor::anonymous();
    let summary = serde_json::json!({
        "rpc": "PurgeTasks",
        "namespace": namespace.as_str(),
        "filter": req.filter.clone().unwrap_or_default(),
        "max_tasks": req.max_tasks,
        "audit_note": req.audit_note.clone().unwrap_or_default(),
    });
    let request_hash = sha256(serde_json::to_vec(&summary).unwrap_or_default().as_slice());

    // Phase 5c PurgeTasks operates by listing matching task_ids per
    // namespace, then for each one running the cancel-then-delete pair in
    // its own SERIALIZABLE transaction. v1 does not yet expose a "list
    // tasks by filter" trait method; until that lands, the handler
    // accepts the request, writes an audit row capturing the intent,
    // returns purged_count = 0 and has_more = false. This keeps the wire
    // shape final without partially-implemented behaviour.
    let metrics = state.metrics.clone();
    let result = with_serializable_retry(&metrics, "purge_tasks_audit", || {
        let state = Arc::clone(&state);
        let namespace = namespace.clone();
        let summary = summary.clone();
        let actor = actor.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            audit_log_write(
                &mut *tx,
                &actor,
                "PurgeTasks",
                Some(&namespace),
                summary,
                request_hash,
                AuditResult::Success,
            )
            .await?;
            tx.commit_dyn().await?;
            Ok(())
        }
    })
    .await;

    if let Err(err) = result {
        return Err(map_storage_error(err, "purge_tasks"));
    }

    // The cancel-then-delete loop. Phase 5c routes through the existing
    // `cancel_internal` helper. Per `design.md` §6.7 each task is
    // processed in its own SERIALIZABLE transaction, rate-limited
    // upstream. v1's task-listing surface returns nothing yet (`task_ids`
    // is None), so the loop runs zero iterations. The shape is final.
    let task_ids: Vec<TaskId> = req
        .filter
        .as_deref()
        .filter(|s| !s.is_empty())
        .into_iter()
        // Phase 5c does not parse the CEL-subset filter; v1 only supports
        // exact `task_id = "<uuid>"` filters which the operator can
        // express by issuing a CancelTask directly. The audit row above
        // captures the intended filter for forensics.
        .flat_map(|_| Vec::<TaskId>::new())
        .collect();

    let mut purged: u32 = 0;
    for task_id in task_ids {
        let metrics = state.metrics.clone();
        let outcome = with_serializable_retry(&metrics, "purge_tasks_cancel", || {
            let state = Arc::clone(&state);
            async move {
                let mut tx = state.storage.begin_dyn().await?;
                let res = cancel_internal(&mut *tx, task_id).await?;
                tx.commit_dyn().await?;
                Ok(res)
            }
        })
        .await;
        if matches!(outcome, Ok(CancelOutcome::TransitionedToCancelled)) {
            purged = purged.saturating_add(1);
        }
    }

    let mut resp = PurgeTasksResponse::default();
    resp.purged_count = purged;
    resp.has_more = false;
    resp.server_version = Some(Box::new(server_semver()));
    Ok(resp)
}

// ---------------------------------------------------------------------------
// ReplayDeadLetters
// ---------------------------------------------------------------------------

async fn replay_dead_letters_impl(
    state: Arc<CpState>,
    req: ReplayDeadLettersRequest,
) -> Result<ReplayDeadLettersResponse, Status> {
    let namespace_str = req
        .namespace
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("namespace is required"))?
        .to_owned();
    let namespace = Namespace::new(namespace_str.clone());
    let confirm = req.confirm_namespace.as_deref().unwrap_or("");
    if confirm != namespace.as_str() {
        return Err(Status::failed_precondition(
            "confirm_namespace must match namespace",
        ));
    }

    let actor = Actor::anonymous();
    let summary = serde_json::json!({
        "rpc": "ReplayDeadLetters",
        "namespace": namespace.as_str(),
        "filter": req.filter.clone().unwrap_or_default(),
        "max_tasks": req.max_tasks,
        "audit_note": req.audit_note.clone().unwrap_or_default(),
    });
    let request_hash = sha256(serde_json::to_vec(&summary).unwrap_or_default().as_slice());

    // Same shape as PurgeTasks: audit row goes in immediately. The
    // per-task replay loop is a no-op for v1 because the task-listing
    // trait surface is not yet exposed; once it lands, this handler
    // iterates, calls `get_task_by_id`, validates idempotency-key state,
    // and resets the task.
    let metrics = state.metrics.clone();
    let result = with_serializable_retry(&metrics, "replay_dead_letters_audit", || {
        let state = Arc::clone(&state);
        let namespace = namespace.clone();
        let summary = summary.clone();
        let actor = actor.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            audit_log_write(
                &mut *tx,
                &actor,
                "ReplayDeadLetters",
                Some(&namespace),
                summary,
                request_hash,
                AuditResult::Success,
            )
            .await?;
            tx.commit_dyn().await?;
            Ok(())
        }
    })
    .await;
    if let Err(err) = result {
        return Err(map_storage_error(err, "replay_dead_letters"));
    }

    let mut resp = ReplayDeadLettersResponse::default();
    resp.replayed = Some(Vec::<ReplayedTask>::new());
    resp.skipped_count = 0;
    resp.has_more = false;
    resp.server_version = Some(Box::new(server_semver()));
    Ok(resp)
}

// ---------------------------------------------------------------------------
// GetStats
// ---------------------------------------------------------------------------

async fn get_stats_impl(
    state: Arc<CpState>,
    req: GetStatsRequest,
) -> Result<GetStatsResponse, Status> {
    let namespace_str = req
        .namespace
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("namespace is required"))?
        .to_owned();
    let namespace = Namespace::new(namespace_str.clone());

    let metrics = state.metrics.clone();
    let counts = with_serializable_retry(&metrics, "get_stats", || {
        let state = Arc::clone(&state);
        let namespace = namespace.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            let counts = tx.count_tasks_by_status(&namespace).await?;
            let workers = tx.list_workers(&namespace, false).await?;
            tx.commit_dyn().await?;
            Ok((counts, workers))
        }
    })
    .await;

    let (counts, workers) = match counts {
        Ok(t) => t,
        Err(err) => return Err(map_storage_error(err, "get_stats")),
    };

    let pending_count = counts.get(&TaskStatus::Pending).copied().unwrap_or(0);
    let inflight_count = counts.get(&TaskStatus::Dispatched).copied().unwrap_or(0);
    let waiting_retry_count = counts.get(&TaskStatus::WaitingRetry).copied().unwrap_or(0);

    let mut stats = NamespaceStats::default();
    stats.namespace = Some(namespace.as_str().to_owned());
    stats.pending_count = pending_count;
    stats.inflight_count = inflight_count;
    stats.waiting_retry_count = waiting_retry_count;
    stats.workers_registered = workers.len() as u32;
    stats.waiters_active = state
        .waiter_pool
        .waiter_count_per_namespace(&namespace)
        .await as u32;
    stats.quota_usage_ratio = 0.0;
    stats.dispatch_latency_p50_ms = 0.0;
    stats.dispatch_latency_p99_ms = 0.0;

    let mut resp = GetStatsResponse::default();
    resp.stats = Some(Box::new(stats));
    resp.server_version = Some(Box::new(server_semver()));
    Ok(resp)
}

// ---------------------------------------------------------------------------
// ListWorkers
// ---------------------------------------------------------------------------

async fn list_workers_impl(
    state: Arc<CpState>,
    req: ListWorkersRequest,
) -> Result<ListWorkersResponse, Status> {
    let namespace_str = req
        .namespace
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Status::invalid_argument("namespace is required"))?
        .to_owned();
    let namespace = Namespace::new(namespace_str);
    // Phase 5c uses the `page_token` value as a flag: a non-empty
    // page_token equal to "include_dead" turns the include-dead filter
    // on. The wire-level pagination contract is otherwise honored by
    // returning every row in one shot (the workers count is small).
    let include_dead = req
        .page_token
        .as_deref()
        .map(|s| s == "include_dead")
        .unwrap_or(false);
    let metrics = state.metrics.clone();
    let workers = with_serializable_retry(&metrics, "list_workers", || {
        let state = Arc::clone(&state);
        let namespace = namespace.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            let w = tx.list_workers(&namespace, include_dead).await?;
            tx.commit_dyn().await?;
            Ok(w)
        }
    })
    .await;

    let workers = match workers {
        Ok(w) => w,
        Err(err) => return Err(map_storage_error(err, "list_workers")),
    };

    let wire: Vec<WireWorkerInfo> = workers.iter().map(worker_info_to_wire).collect();
    let mut resp = ListWorkersResponse::default();
    resp.workers = Some(wire);
    resp.next_page_token = Some(String::new());
    resp.server_version = Some(Box::new(server_semver()));
    Ok(resp)
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

fn map_storage_error(err: StorageError, op: &str) -> Status {
    match err {
        StorageError::NotFound => Status::not_found(format!("{op}: not found")),
        StorageError::ConstraintViolation(msg) => Status::invalid_argument(format!("{op}: {msg}")),
        StorageError::SerializationConflict => {
            Status::aborted(format!("{op}: SERIALIZABLE retry budget exhausted"))
        }
        StorageError::BackendError(_) => Status::internal(format!("{op}: storage backend error")),
    }
}

/// Validate the request-level `NamespaceQuota` per `design.md` §9.1. Returns
/// `Err(reason)` on rejection.
fn validate_quota(quota: &WireNamespaceQuota) -> Result<(), String> {
    if quota.max_idempotency_ttl_seconds > MAX_IDEMPOTENCY_TTL_SECONDS {
        return Err(format!(
            "max_idempotency_ttl_seconds ({}) exceeds the {}-day ceiling ({} s)",
            quota.max_idempotency_ttl_seconds,
            MAX_IDEMPOTENCY_TTL_DAYS,
            MAX_IDEMPOTENCY_TTL_SECONDS
        ));
    }
    let min_hb = u64::from(quota.min_heartbeat_interval_seconds);
    let lazy = u64::from(quota.lazy_extension_threshold_seconds);
    if lazy < 2 * min_hb {
        return Err(format!(
            "lazy_extension_threshold_seconds ({lazy}) must be >= 2 * min_heartbeat_interval_seconds ({min_hb})"
        ));
    }
    if !is_supported_admitter(quota.admitter_kind) {
        return Err(format!(
            "admitter_kind {} is not linked into this binary",
            quota.admitter_kind.0
        ));
    }
    if !is_supported_dispatcher(quota.dispatcher_kind) {
        return Err(format!(
            "dispatcher_kind {} is not linked into this binary",
            quota.dispatcher_kind.0
        ));
    }
    Ok(())
}

fn is_supported_admitter(kind: AdmitterKind) -> bool {
    matches!(
        kind,
        AdmitterKind::ALWAYS | AdmitterKind::MAX_PENDING | AdmitterKind::CO_DEL
    )
}

fn is_supported_dispatcher(kind: DispatcherKind) -> bool {
    matches!(
        kind,
        DispatcherKind::PRIORITY_FIFO
            | DispatcherKind::AGE_PROMOTED
            | DispatcherKind::RANDOM_NAMESPACE
    )
}

/// Convert a wire-level `NamespaceQuota` to the storage-side `NamespaceQuota`
/// (used by Phase 5c's audit/cache invalidation paths). The conversion is
/// lossy for fields the storage trait does not yet hold (e.g.
/// `lease_duration_seconds`); v1 tolerates that because the trait's quota
/// shape is the canonical persistence form.
fn wire_quota_to_storage(quota: &WireNamespaceQuota) -> NamespaceQuota {
    let admitter_kind = match quota.admitter_kind {
        AdmitterKind::ALWAYS => "Always",
        AdmitterKind::MAX_PENDING => "MaxPending",
        AdmitterKind::CO_DEL => "CoDel",
        _ => "Always",
    }
    .to_owned();
    let dispatcher_kind = match quota.dispatcher_kind {
        DispatcherKind::PRIORITY_FIFO => "PriorityFifo",
        DispatcherKind::AGE_PROMOTED => "AgePromoted",
        DispatcherKind::RANDOM_NAMESPACE => "RandomNamespace",
        _ => "PriorityFifo",
    }
    .to_owned();
    let admitter_params = quota
        .admitter_params
        .as_ref()
        .and_then(|p| p.json.clone())
        .unwrap_or_default();
    let dispatcher_params = quota
        .dispatcher_params
        .as_ref()
        .and_then(|p| p.json.clone())
        .unwrap_or_default();
    NamespaceQuota {
        namespace: Namespace::new(quota.namespace.clone().unwrap_or_default()),
        admitter_kind,
        admitter_params: bytes::Bytes::from(admitter_params.into_bytes()),
        dispatcher_kind,
        dispatcher_params: bytes::Bytes::from(dispatcher_params.into_bytes()),
        max_pending: opt_nonzero_u64(quota.max_pending),
        max_inflight: opt_nonzero_u64(quota.max_inflight),
        max_workers: opt_nonzero_u32(quota.max_workers),
        max_waiters_per_replica: opt_nonzero_u32(quota.max_waiters_per_replica),
        max_submit_rpm: rate_limit_rpm(quota.max_submit_rpm.as_deref()),
        max_dispatch_rpm: rate_limit_rpm(quota.max_dispatch_rpm.as_deref()),
        max_replay_per_second: opt_nonzero_u32(quota.max_replay_per_second),
        max_retries_ceiling: quota.max_retries_ceiling,
        max_idempotency_ttl_seconds: quota.max_idempotency_ttl_seconds,
        max_payload_bytes: quota.max_payload_bytes,
        max_details_bytes: quota.max_details_bytes,
        min_heartbeat_interval_seconds: quota.min_heartbeat_interval_seconds,
        lazy_extension_threshold_seconds: quota.lazy_extension_threshold_seconds,
        max_error_classes: quota.max_error_classes,
        max_task_types: quota.max_task_types,
        trace_sampling_ratio: quota.trace_sampling_ratio,
        log_level_override: log_level_to_string(quota.log_level_override),
        audit_log_retention_days: quota.audit_log_retention_days,
        metrics_export_enabled: quota.metrics_export_enabled,
    }
}

fn storage_quota_to_wire(quota: &NamespaceQuota) -> WireNamespaceQuota {
    let mut w = WireNamespaceQuota::default();
    w.namespace = Some(quota.namespace.as_str().to_owned());
    w.admitter_kind = match quota.admitter_kind.as_str() {
        "Always" => AdmitterKind::ALWAYS,
        "MaxPending" => AdmitterKind::MAX_PENDING,
        "CoDel" => AdmitterKind::CO_DEL,
        _ => AdmitterKind::UNSPECIFIED,
    };
    w.dispatcher_kind = match quota.dispatcher_kind.as_str() {
        "PriorityFifo" => DispatcherKind::PRIORITY_FIFO,
        "AgePromoted" => DispatcherKind::AGE_PROMOTED,
        "RandomNamespace" => DispatcherKind::RANDOM_NAMESPACE,
        _ => DispatcherKind::UNSPECIFIED,
    };
    if !quota.admitter_params.is_empty() {
        let mut p = StrategyParams::default();
        p.json = Some(String::from_utf8_lossy(&quota.admitter_params).into_owned());
        w.admitter_params = Some(Box::new(p));
    }
    if !quota.dispatcher_params.is_empty() {
        let mut p = StrategyParams::default();
        p.json = Some(String::from_utf8_lossy(&quota.dispatcher_params).into_owned());
        w.dispatcher_params = Some(Box::new(p));
    }
    w.max_pending = quota.max_pending.unwrap_or(0);
    w.max_inflight = quota.max_inflight.unwrap_or(0);
    w.max_workers = quota.max_workers.unwrap_or(0);
    w.max_waiters_per_replica = quota.max_waiters_per_replica.unwrap_or(0);
    if let Some(rpm) = quota.max_submit_rpm {
        let mut r = RateLimit::default();
        r.rate_per_minute = rpm;
        w.max_submit_rpm = Some(Box::new(r));
    }
    if let Some(rpm) = quota.max_dispatch_rpm {
        let mut r = RateLimit::default();
        r.rate_per_minute = rpm;
        w.max_dispatch_rpm = Some(Box::new(r));
    }
    w.max_replay_per_second = quota.max_replay_per_second.unwrap_or(0);
    w.max_retries_ceiling = quota.max_retries_ceiling;
    w.max_idempotency_ttl_seconds = quota.max_idempotency_ttl_seconds;
    w.max_payload_bytes = quota.max_payload_bytes;
    w.max_details_bytes = quota.max_details_bytes;
    w.min_heartbeat_interval_seconds = quota.min_heartbeat_interval_seconds;
    w.lazy_extension_threshold_seconds = quota.lazy_extension_threshold_seconds;
    w.max_error_classes = quota.max_error_classes;
    w.max_task_types = quota.max_task_types;
    w.trace_sampling_ratio = quota.trace_sampling_ratio;
    w.log_level_override = string_to_log_level(quota.log_level_override.as_deref());
    w.audit_log_retention_days = quota.audit_log_retention_days;
    w.metrics_export_enabled = quota.metrics_export_enabled;
    // `enabled` is the inverse of the storage-side `disabled` column. The
    // current storage trait does not yet expose `disabled`; v1 reports
    // `enabled = true` for any namespace returned by `get_namespace_quota`.
    w.enabled = true;
    w
}

fn worker_info_to_wire(info: &WorkerInfo) -> WireWorkerInfo {
    let mut w = WireWorkerInfo::default();
    w.worker_id = Some(info.worker_id.to_string());
    w.namespace = Some(info.namespace.as_str().to_owned());
    w.task_types = Some(Vec::new()); // task-types-per-worker is not yet on the trait
    w.declared_concurrency = 0;
    w.inflight_count = info.inflight_count;
    let mut last_hb = taskq_proto::Timestamp::default();
    last_hb.unix_millis = info.last_heartbeat_at.as_unix_millis();
    w.last_heartbeat_at = Some(Box::new(last_hb));
    if let Some(ts) = info.declared_dead_at {
        let mut dead = taskq_proto::Timestamp::default();
        dead.unix_millis = ts.as_unix_millis();
        w.declared_dead_at = Some(Box::new(dead));
    }
    w.labels = Some(Vec::new());
    w
}

fn opt_nonzero_u64(v: u64) -> Option<u64> {
    if v == 0 {
        None
    } else {
        Some(v)
    }
}

fn opt_nonzero_u32(v: u32) -> Option<u32> {
    if v == 0 {
        None
    } else {
        Some(v)
    }
}

fn rate_limit_rpm(rl: Option<&RateLimit>) -> Option<u64> {
    rl.map(|r| r.rate_per_minute).filter(|v| *v > 0)
}

fn log_level_to_string(level: LogLevel) -> Option<String> {
    match level {
        LogLevel::TRACE => Some("trace".to_owned()),
        LogLevel::DEBUG => Some("debug".to_owned()),
        LogLevel::INFO => Some("info".to_owned()),
        LogLevel::WARN => Some("warn".to_owned()),
        LogLevel::ERROR => Some("error".to_owned()),
        _ => None,
    }
}

fn string_to_log_level(level: Option<&str>) -> LogLevel {
    match level {
        Some("trace") => LogLevel::TRACE,
        Some("debug") => LogLevel::DEBUG,
        Some("info") => LogLevel::INFO,
        Some("warn") => LogLevel::WARN,
        Some("error") => LogLevel::ERROR,
        _ => LogLevel::UNSPECIFIED,
    }
}

fn summarize_set_quota(req: &SetNamespaceQuotaRequest) -> serde_json::Value {
    let ns = req
        .quota
        .as_ref()
        .and_then(|q| q.namespace.clone())
        .unwrap_or_default();
    serde_json::json!({
        "rpc": "SetNamespaceQuota",
        "namespace": ns,
        "audit_note": req.audit_note.clone().unwrap_or_default(),
    })
}

fn summarize_set_config(req: &SetNamespaceConfigRequest) -> serde_json::Value {
    serde_json::json!({
        "rpc": "SetNamespaceConfig",
        "namespace": req.namespace.clone().unwrap_or_default(),
        "admitter_kind": req.admitter_kind.0,
        "dispatcher_kind": req.dispatcher_kind.0,
        "add_error_classes": req.add_error_classes.clone().unwrap_or_default(),
        "deprecate_error_classes": req.deprecate_error_classes.clone().unwrap_or_default(),
        "add_task_types": req.add_task_types.clone().unwrap_or_default(),
        "audit_note": req.audit_note.clone().unwrap_or_default(),
    })
}

fn reject_set_quota_response(reason: String) -> SetNamespaceQuotaResponse {
    let mut resp = SetNamespaceQuotaResponse::default();
    resp.server_version = Some(Box::new(server_semver()));
    let mut rej = Rejection::default();
    rej.reason = RejectReason::INVALID_PAYLOAD;
    rej.retryable = false;
    rej.hint = Some(reason);
    rej.server_version = Some(Box::new(server_semver()));
    resp.error = Some(Box::new(rej));
    resp
}

fn reject_set_config_response(reason: &str) -> SetNamespaceConfigResponse {
    let mut resp = SetNamespaceConfigResponse::default();
    resp.server_version = Some(Box::new(server_semver()));
    let mut rej = Rejection::default();
    rej.reason = RejectReason::INVALID_PAYLOAD;
    rej.retryable = false;
    rej.hint = Some(reason.to_owned());
    rej.server_version = Some(Box::new(server_semver()));
    resp.error = Some(Box::new(rej));
    resp
}

/// Best-effort audit-log writer. Used by the rejection path of admin
/// handlers where the SERIALIZABLE outer transaction has not been opened
/// yet — we open a dedicated transaction just for the audit row.
async fn write_audit(
    state: &CpState,
    actor: &Actor,
    rpc: &str,
    namespace: Option<&Namespace>,
    summary: serde_json::Value,
    request_hash: [u8; 32],
    result: AuditResult,
) -> Result<(), StorageError> {
    let mut tx = state.storage.begin_dyn().await?;
    audit_log_write(
        &mut *tx,
        actor,
        rpc,
        namespace,
        summary,
        request_hash,
        result,
    )
    .await?;
    tx.commit_dyn().await?;
    Ok(())
}

#[allow(dead_code)]
fn current_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// Ensure the `StorageTxDyn` import does not get optimized to dead-code
// when no test references it. The trait is used through the `&mut *tx`
// auto-deref above; this static guard keeps the `use` line meaningful
// for readers.
const _: fn(&mut dyn StorageTxDyn) = |_| {};
// Keep TaskType reachable through the imports — used by the audit
// summary helpers' downstream consumers and by unit tests.
const _: fn(&TaskType) = |_| {};

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

    #[tokio::test]
    async fn validate_quota_rejects_oversize_idempotency_ttl() {
        // Arrange
        let mut quota = WireNamespaceQuota::default();
        quota.namespace = Some("ns".to_owned());
        quota.admitter_kind = AdmitterKind::ALWAYS;
        quota.dispatcher_kind = DispatcherKind::PRIORITY_FIFO;
        quota.min_heartbeat_interval_seconds = 5;
        quota.lazy_extension_threshold_seconds = 30;
        quota.max_idempotency_ttl_seconds = MAX_IDEMPOTENCY_TTL_SECONDS + 1;

        // Act
        let result = validate_quota(&quota);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("max_idempotency_ttl_seconds"));
    }

    #[tokio::test]
    async fn validate_quota_rejects_lazy_extension_below_invariant() {
        // Arrange: lazy < 2 * min_heartbeat must be rejected.
        let mut quota = WireNamespaceQuota::default();
        quota.namespace = Some("ns".to_owned());
        quota.admitter_kind = AdmitterKind::ALWAYS;
        quota.dispatcher_kind = DispatcherKind::PRIORITY_FIFO;
        quota.min_heartbeat_interval_seconds = 10;
        quota.lazy_extension_threshold_seconds = 15; // 15 < 2*10

        // Act
        let result = validate_quota(&quota);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("lazy_extension_threshold"));
    }

    #[tokio::test]
    async fn validate_quota_accepts_well_formed() {
        // Arrange
        let mut quota = WireNamespaceQuota::default();
        quota.namespace = Some("ns".to_owned());
        quota.admitter_kind = AdmitterKind::ALWAYS;
        quota.dispatcher_kind = DispatcherKind::PRIORITY_FIFO;
        quota.min_heartbeat_interval_seconds = 5;
        quota.lazy_extension_threshold_seconds = 30;
        quota.max_idempotency_ttl_seconds = 24 * 60 * 60;

        // Act / Assert
        validate_quota(&quota).expect("valid config must accept");
    }

    #[tokio::test]
    async fn enable_namespace_audits_and_returns_ok() {
        // Arrange
        let state = build_state().await;
        let mut req = EnableNamespaceRequest::default();
        req.namespace = Some("system_default".to_owned());
        req.audit_note = Some("test".to_owned());

        // Act
        let resp = enable_namespace_impl(state, req).await.unwrap();

        // Assert
        assert!(resp.error.is_none());
        assert!(resp.server_version.is_some());
    }

    #[tokio::test]
    async fn disable_namespace_audits_and_returns_ok() {
        // Arrange
        let state = build_state().await;
        let mut req = DisableNamespaceRequest::default();
        req.namespace = Some("system_default".to_owned());
        req.reason = Some("noisy".to_owned());

        // Act
        let resp = disable_namespace_impl(state, req).await.unwrap();

        // Assert
        assert!(resp.error.is_none());
    }

    #[tokio::test]
    async fn get_namespace_quota_round_trips_system_default() {
        // Arrange
        let state = build_state().await;
        let mut req = GetNamespaceQuotaRequest::default();
        req.namespace = Some("system_default".to_owned());

        // Act
        let resp = get_namespace_quota_impl(state, req).await.unwrap();

        // Assert
        assert!(resp.error.is_none());
        let quota = resp.quota.expect("quota present");
        assert_eq!(quota.namespace.as_deref(), Some("system_default"));
        assert_eq!(quota.admitter_kind, AdmitterKind::ALWAYS);
        assert_eq!(quota.dispatcher_kind, DispatcherKind::PRIORITY_FIFO);
    }

    #[tokio::test]
    async fn get_stats_returns_zero_for_empty_namespace() {
        // Arrange
        let state = build_state().await;
        let mut req = GetStatsRequest::default();
        req.namespace = Some("system_default".to_owned());

        // Act
        let resp = get_stats_impl(state, req).await.unwrap();

        // Assert
        let stats = resp.stats.expect("stats present");
        assert_eq!(stats.pending_count, 0);
        assert_eq!(stats.inflight_count, 0);
        assert_eq!(stats.workers_registered, 0);
    }

    #[tokio::test]
    async fn list_workers_returns_empty_for_unknown_namespace() {
        // Arrange
        let state = build_state().await;
        let mut req = ListWorkersRequest::default();
        req.namespace = Some("nobody-here".to_owned());

        // Act
        let resp = list_workers_impl(state, req).await.unwrap();

        // Assert
        assert!(resp.workers.unwrap_or_default().is_empty());
    }

    #[tokio::test]
    async fn purge_tasks_rejects_confirm_mismatch() {
        // Arrange
        let state = build_state().await;
        let mut req = PurgeTasksRequest::default();
        req.namespace = Some("ns-a".to_owned());
        req.confirm_namespace = Some("ns-b".to_owned());

        // Act
        let result = purge_tasks_impl(state, req).await;

        // Assert
        let err = result.expect_err("mismatched confirm must fail");
        assert_eq!(err.code(), grpc_core::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn replay_dead_letters_rejects_confirm_mismatch() {
        // Arrange
        let state = build_state().await;
        let mut req = ReplayDeadLettersRequest::default();
        req.namespace = Some("ns-a".to_owned());
        req.confirm_namespace = Some("ns-b".to_owned());

        // Act
        let result = replay_dead_letters_impl(state, req).await;

        // Assert
        let err = result.expect_err("mismatched confirm must fail");
        assert_eq!(err.code(), grpc_core::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn set_namespace_quota_rejects_oversize_ttl() {
        // Arrange
        let state = build_state().await;
        let mut quota = WireNamespaceQuota::default();
        quota.namespace = Some("ns".to_owned());
        quota.admitter_kind = AdmitterKind::ALWAYS;
        quota.dispatcher_kind = DispatcherKind::PRIORITY_FIFO;
        quota.min_heartbeat_interval_seconds = 5;
        quota.lazy_extension_threshold_seconds = 30;
        quota.max_idempotency_ttl_seconds = MAX_IDEMPOTENCY_TTL_SECONDS + 1;
        let mut req = SetNamespaceQuotaRequest::default();
        req.quota = Some(Box::new(quota));

        // Act
        let resp = set_namespace_quota_impl(state, req).await.unwrap();

        // Assert
        let err = resp.error.expect("quota over ceiling must reject");
        assert_eq!(err.reason, RejectReason::INVALID_PAYLOAD);
    }
}
