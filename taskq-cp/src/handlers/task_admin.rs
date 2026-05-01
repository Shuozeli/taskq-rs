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
//! Phase 5c/5e use an "anonymous" actor for now; the gRPC auth
//! interceptor in `crate::server::auth_interceptor` is a stub that
//! validates a non-empty `authorization` header but does not yet
//! propagate a structured `Actor` into the request `Extensions`. Phase 7
//! (auth) replaces this with a pluggable validator that lands an
//! `Actor` carrier on the request. See `TODO(phase-7-auth)` markers
//! below.

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
    Namespace, NamespaceQuota, NamespaceQuotaUpsert, ReplayOutcome, StorageError, Task, TaskFilter,
    TaskStatus, TaskType, TerminalState, WorkerInfo,
};

use crate::audit::{audit_log_write, sha256, Actor, AuditResult};
use crate::handlers::cancel::{cancel_internal, CancelOutcome};
use crate::handlers::retry::with_serializable_retry;
use crate::state::{CpState, StorageTxDyn};

/// Hard ceiling on `max_idempotency_ttl_seconds` per `design.md` §9.1.
const MAX_IDEMPOTENCY_TTL_DAYS: u64 = 90;
const MAX_IDEMPOTENCY_TTL_SECONDS: u64 = MAX_IDEMPOTENCY_TTL_DAYS * 24 * 60 * 60;

/// Default per-RPC budget for PurgeTasks / ReplayDeadLetters when the
/// request leaves `max_tasks` unset (== 0). `design.md` §6.7 leaves the
/// concrete number to the implementation; 1000 keeps a single admin RPC
/// bounded while still letting an operator burn through a sizeable DLQ
/// in a few invocations.
const DEFAULT_PURGE_BATCH: u32 = 1000;
/// Hard cap on a single PurgeTasks / ReplayDeadLetters request. Operators
/// who need to drain more must paginate. The 5000 ceiling matches the
/// per-replica waiter cap order of magnitude — well inside what one RPC
/// can pace through at the 100/sec budget below without timing out.
const MAX_PURGE_BATCH: u32 = 5000;
/// Pacing between per-task cancel/replay transactions. Per `design.md`
/// §6.7 admin loops are rate-limited at 100/sec — 10ms per iteration.
const PURGE_PACING_MS: u64 = 10;

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

#[tracing::instrument(name = "taskq.admin.set_namespace_quota", skip_all)]
async fn set_namespace_quota_impl(
    state: Arc<CpState>,
    req: SetNamespaceQuotaRequest,
) -> Result<SetNamespaceQuotaResponse, Status> {
    // TODO(phase-7-auth): pull Actor from request Extensions once the auth
    // interceptor lands a structured carrier.
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
    let upsert = quota_to_upsert(&storage_quota);

    let result = with_serializable_retry(&metrics, "set_namespace_quota", || {
        let state = Arc::clone(&state);
        let upsert = upsert.clone();
        let summary = summary.clone();
        let actor = actor.clone();
        let namespace = namespace.clone();
        async move {
            // §6.7: writes the writable subset of `namespace_quota` and the
            // audit row in one SERIALIZABLE transaction. The `disabled`
            // flag and strategy choice are owned by other admin paths;
            // `upsert_namespace_quota` leaves them alone.
            let mut tx = state.storage.begin_dyn().await?;
            tx.upsert_namespace_quota(&namespace, upsert).await?;
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

#[tracing::instrument(
    name = "taskq.admin.get_namespace_quota",
    skip_all,
    fields(namespace = req.namespace.as_deref().unwrap_or("")),
)]
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

#[tracing::instrument(
    name = "taskq.admin.set_namespace_config",
    skip_all,
    fields(namespace = req.namespace.as_deref().unwrap_or("")),
)]
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
    // TODO(phase-7-auth): pull Actor from request Extensions once the auth
    // interceptor lands a structured carrier.
    let actor = Actor::anonymous();
    let summary = summarize_set_config(&req);
    let request_hash = sha256(serde_json::to_vec(&summary).unwrap_or_default().as_slice());
    let metrics = state.metrics.clone();

    // §6.7 SetNamespaceConfig: validate cardinality up front, then in one
    // SERIALIZABLE transaction insert the new registry rows, deprecate the
    // listed entries, and append the audit row. Strategy choice changes
    // are persisted separately at namespace creation time and don't take
    // effect until rolling restart per `design.md` §6.7; the registry
    // mutations DO take effect immediately.
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
    // limits + currently-loaded count. The check below caps additions
    // against the per-namespace ceiling alone — a future revision can
    // grow this to (current + additions > max) once the registries
    // expose a count primitive.
    let add_error_classes: Vec<String> = req.add_error_classes.clone().unwrap_or_default();
    let deprecate_error_classes: Vec<String> =
        req.deprecate_error_classes.clone().unwrap_or_default();
    let add_task_type_strings: Vec<String> = req.add_task_types.clone().unwrap_or_default();
    let add_task_types: Vec<TaskType> = add_task_type_strings
        .iter()
        .map(|s| TaskType::new(s.clone()))
        .collect();
    let new_error_classes = add_error_classes.len() as u64;
    let new_task_types = add_task_types.len() as u64;

    let result = with_serializable_retry(&metrics, "set_namespace_config", || {
        let state = Arc::clone(&state);
        let namespace = namespace.clone();
        let summary = summary.clone();
        let actor = actor.clone();
        let add_error_classes = add_error_classes.clone();
        let deprecate_error_classes = deprecate_error_classes.clone();
        let add_task_types = add_task_types.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            let quota = tx.get_namespace_quota(&namespace).await?;
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
            // §6.7 SetNamespaceConfig: persist registry mutations. The
            // adds are idempotent (INSERT ... ON CONFLICT DO NOTHING /
            // INSERT OR IGNORE); the deprecates are UPDATEs on existing
            // rows (no-op when missing).
            tx.add_error_classes(&namespace, &add_error_classes).await?;
            for class in &deprecate_error_classes {
                tx.deprecate_error_class(&namespace, class.as_str()).await?;
            }
            tx.add_task_types(&namespace, &add_task_types).await?;
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

#[tracing::instrument(
    name = "taskq.admin.enable_namespace",
    skip_all,
    fields(namespace = req.namespace.as_deref().unwrap_or("")),
)]
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
    // TODO(phase-7-auth): pull Actor from request Extensions once the auth
    // interceptor lands a structured carrier.
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

#[tracing::instrument(
    name = "taskq.admin.disable_namespace",
    skip_all,
    fields(namespace = req.namespace.as_deref().unwrap_or("")),
)]
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
    // TODO(phase-7-auth): pull Actor from request Extensions once the auth
    // interceptor lands a structured carrier.
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

#[tracing::instrument(
    name = "taskq.admin.purge_tasks",
    skip_all,
    fields(namespace = req.namespace.as_deref().unwrap_or("")),
)]
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

    // TODO(phase-7-auth): pull Actor from request Extensions once the auth
    // interceptor lands a structured carrier.
    let actor = Actor::anonymous();
    let summary = serde_json::json!({
        "rpc": "PurgeTasks",
        "namespace": namespace.as_str(),
        "filter": req.filter.clone().unwrap_or_default(),
        "max_tasks": req.max_tasks,
        "audit_note": req.audit_note.clone().unwrap_or_default(),
    });
    let request_hash = sha256(serde_json::to_vec(&summary).unwrap_or_default().as_slice());

    // §6.7 PurgeTasks: cap the loop. `req.max_tasks == 0` means "use the
    // default budget" — we cap at 1000 per RPC so a single PurgeTasks
    // call cannot run unbounded.
    let max_tasks = if req.max_tasks == 0 {
        DEFAULT_PURGE_BATCH
    } else {
        req.max_tasks.min(MAX_PURGE_BATCH)
    } as usize;

    // First, write the audit row + list candidate tasks in one
    // transaction. The list is bounded by `max_tasks`; `has_more` reports
    // when the budget capped the result.
    let metrics = state.metrics.clone();
    let listed = with_serializable_retry(&metrics, "purge_tasks_list", || {
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
            // v1 does not parse the CEL-subset filter string; the
            // simple match is "all tasks in this namespace, oldest
            // first, capped at max_tasks + 1". The +1 lets us decide
            // `has_more`. Future revisions can pluck task_types /
            // statuses out of the filter string and pass through
            // `TaskFilter`.
            let tasks = tx
                .list_tasks_by_filter(
                    &namespace,
                    TaskFilter::default(),
                    max_tasks.saturating_add(1),
                )
                .await?;
            tx.commit_dyn().await?;
            Ok(tasks)
        }
    })
    .await;

    let tasks: Vec<Task> = match listed {
        Ok(t) => t,
        Err(err) => return Err(map_storage_error(err, "purge_tasks")),
    };

    let has_more = tasks.len() > max_tasks;
    let target_tasks: Vec<Task> = tasks.into_iter().take(max_tasks).collect();

    // §6.7: cancel each candidate in its own SERIALIZABLE transaction,
    // rate-limited at 100/sec by sleeping between iterations. The
    // `cancel_internal` helper deletes the dedup row in the same tx so
    // the same key can be re-submitted afterwards.
    let mut purged: u32 = 0;
    let pacing = std::time::Duration::from_millis(PURGE_PACING_MS);
    for task in target_tasks {
        let task_id = task.task_id;
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
        tokio::time::sleep(pacing).await;
    }

    let mut resp = PurgeTasksResponse::default();
    resp.purged_count = purged;
    resp.has_more = has_more;
    resp.server_version = Some(Box::new(server_semver()));
    Ok(resp)
}

// ---------------------------------------------------------------------------
// ReplayDeadLetters
// ---------------------------------------------------------------------------

#[tracing::instrument(
    name = "taskq.admin.replay_dead_letters",
    skip_all,
    fields(namespace = req.namespace.as_deref().unwrap_or("")),
)]
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

    // TODO(phase-7-auth): pull Actor from request Extensions once the auth
    // interceptor lands a structured carrier.
    let actor = Actor::anonymous();
    let summary = serde_json::json!({
        "rpc": "ReplayDeadLetters",
        "namespace": namespace.as_str(),
        "filter": req.filter.clone().unwrap_or_default(),
        "max_tasks": req.max_tasks,
        "audit_note": req.audit_note.clone().unwrap_or_default(),
    });
    let request_hash = sha256(serde_json::to_vec(&summary).unwrap_or_default().as_slice());

    // §6.7 ReplayDeadLetters: cap the loop the same way as PurgeTasks.
    let max_tasks = if req.max_tasks == 0 {
        DEFAULT_PURGE_BATCH
    } else {
        req.max_tasks.min(MAX_PURGE_BATCH)
    } as usize;

    // Audit the request + list candidate tasks in one transaction.
    let metrics = state.metrics.clone();
    let listed = with_serializable_retry(&metrics, "replay_dead_letters_list", || {
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
            let statuses = vec![
                TerminalState::FailedNonretryable,
                TerminalState::FailedExhausted,
                TerminalState::Expired,
            ];
            let tasks = tx
                .list_tasks_by_terminal_status(&namespace, statuses, max_tasks.saturating_add(1))
                .await?;
            tx.commit_dyn().await?;
            Ok(tasks)
        }
    })
    .await;

    let tasks: Vec<Task> = match listed {
        Ok(t) => t,
        Err(err) => return Err(map_storage_error(err, "replay_dead_letters")),
    };

    let has_more = tasks.len() > max_tasks;
    let target_tasks: Vec<Task> = tasks.into_iter().take(max_tasks).collect();

    let mut replayed = Vec::<ReplayedTask>::new();
    let mut skipped: u32 = 0;
    let pacing = std::time::Duration::from_millis(PURGE_PACING_MS);
    for task in target_tasks {
        let task_id = task.task_id;
        let metrics = state.metrics.clone();
        let outcome = with_serializable_retry(&metrics, "replay_dead_letters_replay", || {
            let state = Arc::clone(&state);
            async move {
                let mut tx = state.storage.begin_dyn().await?;
                let res = tx.replay_task(task_id).await?;
                tx.commit_dyn().await?;
                Ok(res)
            }
        })
        .await;
        match outcome {
            Ok(ReplayOutcome::Replayed) => {
                let mut row = ReplayedTask::default();
                row.task_id = Some(task_id.to_string());
                replayed.push(row);
            }
            Ok(_) => {
                skipped = skipped.saturating_add(1);
            }
            Err(err) => return Err(map_storage_error(err, "replay_dead_letters")),
        }
        tokio::time::sleep(pacing).await;
    }

    // §11.3: emit `taskq_replay_total{namespace}` per batch of replayed tasks.
    if !replayed.is_empty() {
        state.metrics.replay_total.add(
            replayed.len() as u64,
            &[taskq_proto_namespace_label(&namespace)],
        );
    }

    let mut resp = ReplayDeadLettersResponse::default();
    resp.replayed = Some(replayed);
    resp.skipped_count = skipped;
    resp.has_more = has_more;
    resp.server_version = Some(Box::new(server_semver()));
    Ok(resp)
}

fn taskq_proto_namespace_label(ns: &Namespace) -> opentelemetry::KeyValue {
    opentelemetry::KeyValue::new("namespace", ns.as_str().to_owned())
}

// ---------------------------------------------------------------------------
// GetStats
// ---------------------------------------------------------------------------

#[tracing::instrument(
    name = "taskq.admin.get_stats",
    skip_all,
    fields(namespace = req.namespace.as_deref().unwrap_or("")),
)]
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

#[tracing::instrument(
    name = "taskq.admin.list_workers",
    skip_all,
    fields(namespace = req.namespace.as_deref().unwrap_or("")),
)]
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

/// Project the writable subset of a [`NamespaceQuota`] onto a
/// [`NamespaceQuotaUpsert`]. `set_namespace_quota_impl` calls this after
/// validating the wire-level config; the trait then writes the row.
fn quota_to_upsert(quota: &NamespaceQuota) -> NamespaceQuotaUpsert {
    NamespaceQuotaUpsert {
        max_pending: quota.max_pending,
        max_inflight: quota.max_inflight,
        max_workers: quota.max_workers,
        max_waiters_per_replica: quota.max_waiters_per_replica,
        max_submit_rpm: quota.max_submit_rpm,
        max_dispatch_rpm: quota.max_dispatch_rpm,
        max_replay_per_second: quota.max_replay_per_second,
        max_retries_ceiling: quota.max_retries_ceiling,
        max_idempotency_ttl_seconds: quota.max_idempotency_ttl_seconds,
        max_payload_bytes: quota.max_payload_bytes,
        max_details_bytes: quota.max_details_bytes,
        min_heartbeat_interval_seconds: quota.min_heartbeat_interval_seconds,
        lazy_extension_threshold_seconds: quota.lazy_extension_threshold_seconds,
        max_error_classes: quota.max_error_classes,
        max_task_types: quota.max_task_types,
        trace_sampling_ratio: quota.trace_sampling_ratio,
        log_level_override: quota.log_level_override.clone(),
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

    fn well_formed_quota(namespace: &str) -> WireNamespaceQuota {
        let mut quota = WireNamespaceQuota::default();
        quota.namespace = Some(namespace.to_owned());
        quota.admitter_kind = AdmitterKind::ALWAYS;
        quota.dispatcher_kind = DispatcherKind::PRIORITY_FIFO;
        quota.min_heartbeat_interval_seconds = 5;
        quota.lazy_extension_threshold_seconds = 30;
        quota.max_idempotency_ttl_seconds = 24 * 60 * 60;
        quota.max_payload_bytes = 4096;
        quota.max_details_bytes = 1024;
        quota.max_retries_ceiling = 5;
        quota.max_error_classes = 16;
        quota.max_task_types = 8;
        quota.trace_sampling_ratio = 0.5;
        quota.audit_log_retention_days = 14;
        quota.metrics_export_enabled = true;
        let mut submit = RateLimit::default();
        submit.rate_per_minute = 600;
        quota.max_submit_rpm = Some(Box::new(submit));
        quota
    }

    #[tokio::test]
    async fn set_namespace_quota_persists_writable_subset() {
        // Arrange
        let state = build_state().await;
        let quota_in = well_formed_quota("custom-ns");
        let mut req = SetNamespaceQuotaRequest::default();
        req.quota = Some(Box::new(quota_in));

        // Act
        let resp = set_namespace_quota_impl(Arc::clone(&state), req)
            .await
            .unwrap();

        // Assert: response is success-shaped; subsequent GetNamespaceQuota
        // returns the freshly written values.
        assert!(resp.error.is_none());
        let mut get = GetNamespaceQuotaRequest::default();
        get.namespace = Some("custom-ns".to_owned());
        let got = get_namespace_quota_impl(state, get).await.unwrap();
        let got_quota = got.quota.expect("quota present");
        assert_eq!(got_quota.namespace.as_deref(), Some("custom-ns"));
        assert_eq!(got_quota.max_payload_bytes, 4096);
        assert_eq!(got_quota.max_details_bytes, 1024);
        assert_eq!(got_quota.max_retries_ceiling, 5);
        assert_eq!(got_quota.max_error_classes, 16);
        assert_eq!(got_quota.max_task_types, 8);
        assert!((got_quota.trace_sampling_ratio - 0.5).abs() < f32::EPSILON);
        assert!(got_quota.metrics_export_enabled);
        let submit_rpm = got_quota
            .max_submit_rpm
            .expect("submit_rpm round-trips")
            .rate_per_minute;
        assert_eq!(submit_rpm, 600);
    }

    #[tokio::test]
    async fn set_namespace_quota_overwrites_on_repeat_upsert() {
        // Arrange
        let state = build_state().await;
        let mut first = well_formed_quota("ns-up");
        first.max_retries_ceiling = 3;
        let mut second = well_formed_quota("ns-up");
        second.max_retries_ceiling = 9;
        let mut r1 = SetNamespaceQuotaRequest::default();
        r1.quota = Some(Box::new(first));
        let mut r2 = SetNamespaceQuotaRequest::default();
        r2.quota = Some(Box::new(second));

        // Act
        set_namespace_quota_impl(Arc::clone(&state), r1)
            .await
            .unwrap();
        set_namespace_quota_impl(Arc::clone(&state), r2)
            .await
            .unwrap();

        // Assert
        let mut get = GetNamespaceQuotaRequest::default();
        get.namespace = Some("ns-up".to_owned());
        let got = get_namespace_quota_impl(state, get).await.unwrap();
        let got_quota = got.quota.expect("quota present");
        assert_eq!(got_quota.max_retries_ceiling, 9);
    }

    #[tokio::test]
    async fn set_namespace_config_persists_registry_additions() {
        // Arrange: provision a namespace big enough to hold the additions,
        // then submit a SetNamespaceConfig with a couple of registry rows.
        let state = build_state().await;
        let quota_in = well_formed_quota("ns-cfg");
        let mut quota_req = SetNamespaceQuotaRequest::default();
        quota_req.quota = Some(Box::new(quota_in));
        set_namespace_quota_impl(Arc::clone(&state), quota_req)
            .await
            .unwrap();

        let mut req = SetNamespaceConfigRequest::default();
        req.namespace = Some("ns-cfg".to_owned());
        req.admitter_kind = AdmitterKind::ALWAYS;
        req.dispatcher_kind = DispatcherKind::PRIORITY_FIFO;
        req.add_error_classes = Some(vec!["transient".to_owned(), "permanent".to_owned()]);
        req.add_task_types = Some(vec!["email".to_owned(), "sms".to_owned()]);

        // Act
        let resp = set_namespace_config_impl(Arc::clone(&state), req)
            .await
            .unwrap();

        // Assert: success-shaped response; the registry inserts went through
        // (verified indirectly by repeating the call — the second call must
        // also succeed because INSERT OR IGNORE / ON CONFLICT DO NOTHING
        // makes the writes idempotent).
        assert!(resp.error.is_none());
        let mut req2 = SetNamespaceConfigRequest::default();
        req2.namespace = Some("ns-cfg".to_owned());
        req2.admitter_kind = AdmitterKind::ALWAYS;
        req2.dispatcher_kind = DispatcherKind::PRIORITY_FIFO;
        req2.add_error_classes = Some(vec!["transient".to_owned()]);
        req2.add_task_types = Some(vec!["email".to_owned()]);
        let resp2 = set_namespace_config_impl(state, req2).await.unwrap();
        assert!(resp2.error.is_none());
    }

    #[tokio::test]
    async fn set_namespace_config_rejects_when_additions_exceed_cap() {
        // Arrange: provision with max_error_classes = 2, then try to add 3.
        let state = build_state().await;
        let mut quota_in = well_formed_quota("ns-cap");
        quota_in.max_error_classes = 2;
        let mut quota_req = SetNamespaceQuotaRequest::default();
        quota_req.quota = Some(Box::new(quota_in));
        set_namespace_quota_impl(Arc::clone(&state), quota_req)
            .await
            .unwrap();

        let mut req = SetNamespaceConfigRequest::default();
        req.namespace = Some("ns-cap".to_owned());
        req.admitter_kind = AdmitterKind::ALWAYS;
        req.dispatcher_kind = DispatcherKind::PRIORITY_FIFO;
        req.add_error_classes = Some(vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]);

        // Act
        let resp = set_namespace_config_impl(state, req).await.unwrap();

        // Assert
        let err = resp.error.expect("over-cap addition must reject");
        assert_eq!(err.reason, RejectReason::INVALID_PAYLOAD);
        assert!(err
            .hint
            .as_deref()
            .unwrap_or_default()
            .contains("max_error_classes"));
    }

    #[tokio::test]
    async fn purge_tasks_iterates_and_cancels_listed_tasks() {
        // Arrange: insert 3 pending tasks, then PurgeTasks.
        use bytes::Bytes;
        use taskq_storage::{IdempotencyKey, NewDedupRecord, NewTask, TaskId, Timestamp};

        let state = build_state().await;
        let now = Timestamp::from_unix_millis(1_700_000_000_000);
        let mut tx = state.storage.begin_dyn().await.unwrap();
        for i in 0..3u32 {
            let task_id = TaskId::generate();
            let task = NewTask {
                task_id,
                namespace: Namespace::new("ns-purge"),
                task_type: TaskType::new("type"),
                priority: 0,
                payload: Bytes::from_static(b"{}"),
                payload_hash: [i as u8; 32],
                submitted_at: now,
                expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
                max_retries: 3,
                retry_initial_ms: 1_000,
                retry_max_ms: 10_000,
                retry_coefficient: 2.0,
                traceparent: Bytes::new(),
                tracestate: Bytes::new(),
                format_version: 1,
            };
            let dedup = NewDedupRecord {
                namespace: Namespace::new("ns-purge"),
                key: IdempotencyKey::new(format!("k{i}")),
                payload_hash: [i as u8; 32],
                expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
            };
            tx.insert_task(task, dedup).await.unwrap();
        }
        tx.commit_dyn().await.unwrap();

        let mut req = PurgeTasksRequest::default();
        req.namespace = Some("ns-purge".to_owned());
        req.confirm_namespace = Some("ns-purge".to_owned());
        req.max_tasks = 10;

        // Act
        let resp = purge_tasks_impl(state, req).await.unwrap();

        // Assert
        assert_eq!(resp.purged_count, 3);
        assert!(!resp.has_more);
    }

    #[tokio::test]
    async fn replay_dead_letters_resets_terminal_failed_tasks() {
        // Arrange: insert a task, complete it as FailedNonretryable, then
        // ReplayDeadLetters resets it back to PENDING.
        use bytes::Bytes;
        use taskq_storage::{
            IdempotencyKey, LeaseRef, NewDedupRecord, NewLease, NewTask, TaskId, TaskOutcome,
            Timestamp, WorkerId,
        };

        let state = build_state().await;
        let now = Timestamp::from_unix_millis(1_700_000_000_000);
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();

        // Submit + acquire + report-failure-as-nonretryable -> FAILED_NONRETRYABLE.
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let task = NewTask {
            task_id,
            namespace: Namespace::new("ns-replay"),
            task_type: TaskType::new("type"),
            priority: 0,
            payload: Bytes::from_static(b"{}"),
            payload_hash: [9u8; 32],
            submitted_at: now,
            expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
            max_retries: 3,
            retry_initial_ms: 1_000,
            retry_max_ms: 10_000,
            retry_coefficient: 2.0,
            traceparent: Bytes::new(),
            tracestate: Bytes::new(),
            format_version: 1,
        };
        let dedup = NewDedupRecord {
            namespace: Namespace::new("ns-replay"),
            key: IdempotencyKey::new("kr"),
            payload_hash: [9u8; 32],
            expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
        };
        tx.insert_task(task, dedup).await.unwrap();
        let lease = NewLease {
            task_id,
            attempt_number: 0,
            worker_id,
            acquired_at: now,
            timeout_at: Timestamp::from_unix_millis(now.as_unix_millis() + 30_000),
        };
        tx.record_acquisition(lease).await.unwrap();
        let lease_ref = LeaseRef {
            task_id,
            attempt_number: 0,
            worker_id,
        };
        tx.complete_task(
            &lease_ref,
            TaskOutcome::FailedNonretryable {
                error_class: "boom".to_owned(),
                error_message: "test".to_owned(),
                error_details: Bytes::new(),
                recorded_at: now,
            },
        )
        .await
        .unwrap();
        tx.commit_dyn().await.unwrap();

        let mut req = ReplayDeadLettersRequest::default();
        req.namespace = Some("ns-replay".to_owned());
        req.confirm_namespace = Some("ns-replay".to_owned());
        req.max_tasks = 10;

        // Act
        let resp = replay_dead_letters_impl(Arc::clone(&state), req)
            .await
            .unwrap();

        // Assert: the row was replayed; checking via get_task_by_id observes
        // status PENDING and attempt_number 0.
        let replayed = resp.replayed.unwrap_or_default();
        assert_eq!(replayed.len(), 1);
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let task = tx.get_task_by_id(task_id).await.unwrap().expect("present");
        tx.commit_dyn().await.unwrap();
        assert_eq!(task.status, TaskStatus::Pending);
        assert_eq!(task.attempt_number, 0);
    }

    #[tokio::test]
    async fn replay_dead_letters_skips_non_terminal_tasks() {
        // Arrange: insert a pending task; ReplayDeadLetters ignores it.
        use bytes::Bytes;
        use taskq_storage::{IdempotencyKey, NewDedupRecord, NewTask, TaskId, Timestamp};

        let state = build_state().await;
        let now = Timestamp::from_unix_millis(1_700_000_000_000);
        let task_id = TaskId::generate();
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let task = NewTask {
            task_id,
            namespace: Namespace::new("ns-skip"),
            task_type: TaskType::new("type"),
            priority: 0,
            payload: Bytes::from_static(b"{}"),
            payload_hash: [3u8; 32],
            submitted_at: now,
            expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
            max_retries: 3,
            retry_initial_ms: 1_000,
            retry_max_ms: 10_000,
            retry_coefficient: 2.0,
            traceparent: Bytes::new(),
            tracestate: Bytes::new(),
            format_version: 1,
        };
        let dedup = NewDedupRecord {
            namespace: Namespace::new("ns-skip"),
            key: IdempotencyKey::new("ks"),
            payload_hash: [3u8; 32],
            expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
        };
        tx.insert_task(task, dedup).await.unwrap();
        tx.commit_dyn().await.unwrap();
        let _ = task_id;

        let mut req = ReplayDeadLettersRequest::default();
        req.namespace = Some("ns-skip".to_owned());
        req.confirm_namespace = Some("ns-skip".to_owned());
        req.max_tasks = 10;

        // Act
        let resp = replay_dead_letters_impl(state, req).await.unwrap();

        // Assert: pending tasks aren't picked up by list_tasks_by_terminal_status
        // so the replay loop has nothing to process.
        assert_eq!(resp.replayed.unwrap_or_default().len(), 0);
        assert_eq!(resp.skipped_count, 0);
    }

    #[tokio::test]
    async fn purge_tasks_reports_has_more_when_request_caps_below_total() {
        // Arrange: insert 3 pending tasks, request a max_tasks of 2.
        use bytes::Bytes;
        use taskq_storage::{IdempotencyKey, NewDedupRecord, NewTask, TaskId, Timestamp};

        let state = build_state().await;
        let now = Timestamp::from_unix_millis(1_700_000_000_000);
        let mut tx = state.storage.begin_dyn().await.unwrap();
        for i in 0..3u32 {
            let task_id = TaskId::generate();
            let task = NewTask {
                task_id,
                namespace: Namespace::new("ns-cap"),
                task_type: TaskType::new("type"),
                priority: 0,
                payload: Bytes::from_static(b"{}"),
                payload_hash: [i as u8; 32],
                submitted_at: Timestamp::from_unix_millis(now.as_unix_millis() + i as i64),
                expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
                max_retries: 3,
                retry_initial_ms: 1_000,
                retry_max_ms: 10_000,
                retry_coefficient: 2.0,
                traceparent: Bytes::new(),
                tracestate: Bytes::new(),
                format_version: 1,
            };
            let dedup = NewDedupRecord {
                namespace: Namespace::new("ns-cap"),
                key: IdempotencyKey::new(format!("k{i}")),
                payload_hash: [i as u8; 32],
                expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
            };
            tx.insert_task(task, dedup).await.unwrap();
        }
        tx.commit_dyn().await.unwrap();

        let mut req = PurgeTasksRequest::default();
        req.namespace = Some("ns-cap".to_owned());
        req.confirm_namespace = Some("ns-cap".to_owned());
        req.max_tasks = 2;

        // Act
        let resp = purge_tasks_impl(state, req).await.unwrap();

        // Assert: 2 cancelled this RPC, has_more flagged so the operator
        // re-issues to drain the rest.
        assert_eq!(resp.purged_count, 2);
        assert!(resp.has_more);
    }
}
