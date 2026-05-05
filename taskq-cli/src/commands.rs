//! Subcommand implementations for `taskq-cli`.
//!
//! Each `cmd_*` function:
//!
//! 1. Builds the wire request from the parsed `clap` args.
//! 2. Connects to the CP via [`crate::connect::connect_admin`].
//! 3. Issues the matching admin RPC.
//! 4. Maps the response into a [`crate::output::Renderable`] +
//!    `serde::Serialize` value.
//! 5. Hands the value to [`crate::output::render`] for either human or
//!    JSON output.
//!
//! Destructive commands (`purge`, `replay`) gate the RPC behind an
//! interactive y/N prompt unless the operator passed `--yes` or stdin
//! is not a TTY (e.g., piped input). `--dry-run` short-circuits before
//! the RPC and prints a plan.

use std::io::{self, IsTerminal, Write};

use grpc_core::Status;
use serde::Serialize;
use taskq_proto::{
    AdmitterKind, DisableNamespaceRequest, DispatcherKind, EnableNamespaceRequest, GetStatsRequest,
    ListWorkersRequest, LogLevel, NamespaceQuota, PurgeTasksRequest, RateLimit,
    ReplayDeadLettersRequest, SetNamespaceQuotaRequest, WorkerInfo as WireWorkerInfo,
};

use crate::cli::{
    Cli, ListWorkersArgs, NamespaceCmd, PurgeArgs, ReplayArgs, SetQuotaArgs, StatsArgs,
};
use crate::connect::{auth_request, connect_admin};
use crate::error::CliError;
use crate::output::{render, render_kv, render_status, render_table, OutputFormat, Renderable};

// ---------------------------------------------------------------------------
// `system_default`-derived starter values for `namespace create`.
// ---------------------------------------------------------------------------

// These match the documented defaults in `design.md` §9.1 + the validator's
// invariants in `taskq-cp/src/handlers/task_admin.rs::validate_quota`. The
// CLI never invents quota numbers — operators run `set-quota` afterwards
// to tune.

const DEFAULT_MAX_PENDING: u64 = 100_000;
const DEFAULT_MAX_INFLIGHT: u64 = 10_000;
const DEFAULT_MAX_WORKERS: u32 = 1_000;
const DEFAULT_MAX_WAITERS_PER_REPLICA: u32 = 1_000;
const DEFAULT_MAX_SUBMIT_RPM: u64 = 60_000;
const DEFAULT_MAX_DISPATCH_RPM: u64 = 60_000;
const DEFAULT_MAX_REPLAY_PER_SECOND: u32 = 100;
const DEFAULT_MAX_RETRIES_CEILING: u32 = 32;
const DEFAULT_MAX_IDEMPOTENCY_TTL_SECONDS: u64 = 7 * 24 * 60 * 60;
const DEFAULT_MAX_PAYLOAD_BYTES: u32 = 1_048_576;
const DEFAULT_MAX_DETAILS_BYTES: u32 = 4_096;
const DEFAULT_MIN_HEARTBEAT_INTERVAL_SECONDS: u32 = 5;
const DEFAULT_LEASE_DURATION_SECONDS: u32 = 60;
const DEFAULT_LAZY_EXTENSION_THRESHOLD_SECONDS: u32 =
    2 * DEFAULT_MIN_HEARTBEAT_INTERVAL_SECONDS + 10;
const DEFAULT_MAX_ERROR_CLASSES: u32 = 64;
const DEFAULT_MAX_TASK_TYPES: u32 = 64;
const DEFAULT_TRACE_SAMPLING_RATIO: f32 = 0.1;
const DEFAULT_AUDIT_LOG_RETENTION_DAYS: u32 = 90;

// ---------------------------------------------------------------------------
// Top-level dispatch
// ---------------------------------------------------------------------------

/// Dispatch a parsed [`Cli`] to the correct command implementation. The
/// returned `()` is the success signal; errors flow up to `main` and
/// translate to a non-zero exit code.
pub async fn run(cli: Cli) -> Result<(), CliError> {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let format = cli.format;
    let endpoint = cli.endpoint.clone();
    let token = cli.token.clone();
    let yes = cli.yes;

    match cli.cmd {
        crate::cli::Cmd::Namespace(sub) => match sub {
            NamespaceCmd::Create { name, audit_note } => {
                cmd_namespace_create(
                    &endpoint,
                    token.as_deref(),
                    &name,
                    audit_note,
                    format,
                    &mut out,
                )
                .await
            }
            NamespaceCmd::List { namespace } => {
                cmd_namespace_list(&endpoint, token.as_deref(), &namespace, format, &mut out).await
            }
            NamespaceCmd::Enable { name, audit_note } => {
                cmd_namespace_enable(
                    &endpoint,
                    token.as_deref(),
                    &name,
                    audit_note,
                    format,
                    &mut out,
                )
                .await
            }
            NamespaceCmd::Disable {
                name,
                reason,
                audit_note,
            } => {
                cmd_namespace_disable(
                    &endpoint,
                    token.as_deref(),
                    &name,
                    reason,
                    audit_note,
                    format,
                    &mut out,
                )
                .await
            }
        },
        crate::cli::Cmd::SetQuota(args) => {
            cmd_set_quota(&endpoint, token.as_deref(), args, format, &mut out).await
        }
        crate::cli::Cmd::Purge(args) => {
            cmd_purge(&endpoint, token.as_deref(), args, yes, format, &mut out).await
        }
        crate::cli::Cmd::Replay(args) => {
            cmd_replay(&endpoint, token.as_deref(), args, yes, format, &mut out).await
        }
        crate::cli::Cmd::Stats(args) => {
            cmd_stats(&endpoint, token.as_deref(), args, format, &mut out).await
        }
        crate::cli::Cmd::ListWorkers(args) => {
            cmd_list_workers(&endpoint, token.as_deref(), args, format, &mut out).await
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers shared by every command
// ---------------------------------------------------------------------------

/// Convert a transport-level `Status` into a `CliError::Transport` that
/// carries the operator-visible endpoint string.
fn lift_transport(endpoint: &str, status: Status) -> CliError {
    CliError::Transport {
        endpoint: endpoint.to_owned(),
        code: format!("{:?}", status.code()),
        message: status.message().to_owned(),
    }
}

/// Translate a `Rejection` envelope into a `CliError::Rejected`.
fn lift_rejection(err: &taskq_proto::Rejection) -> CliError {
    CliError::Rejected {
        reason: err.reason,
        retry_after_ms: err.retry_after.as_ref().map(|t| t.unix_millis).unwrap_or(0),
        retryable: err.retryable,
        hint: err.hint.clone().unwrap_or_default(),
    }
}

/// Render `value` using the operator-selected format and flush.
fn emit<T>(value: &T, format: OutputFormat, out: &mut dyn Write) -> Result<(), CliError>
where
    T: Renderable + Serialize,
{
    render(value, format, out).map_err(|e| CliError::Internal(format!("output write failed: {e}")))
}

/// Prompt the operator to confirm a destructive action. Returns
/// `Ok(())` on `y` / `Y` / `yes`; `Err(CliError::Aborted)` otherwise.
/// Skipped when `auto_yes` is set or stdin is not a TTY.
fn confirm_or_abort(prompt: &str, auto_yes: bool) -> Result<(), CliError> {
    let stdin = io::stdin();
    if auto_yes || !stdin.is_terminal() {
        return Ok(());
    }
    let stderr = io::stderr();
    let mut err = stderr.lock();
    write!(err, "{prompt} [y/N] ").ok();
    err.flush().ok();
    let mut buf = String::new();
    if io::stdin().read_line(&mut buf).is_err() {
        return Err(CliError::Aborted);
    }
    let trimmed = buf.trim().to_ascii_lowercase();
    if matches!(trimmed.as_str(), "y" | "yes") {
        Ok(())
    } else {
        Err(CliError::Aborted)
    }
}

// ---------------------------------------------------------------------------
// `namespace create`
// ---------------------------------------------------------------------------

/// Build a freshly-initialised `NamespaceQuota` wire row using the
/// CLI-side defaults.
fn fresh_quota(name: &str) -> NamespaceQuota {
    let mut q = NamespaceQuota::default();
    q.namespace = Some(name.to_owned());
    q.admitter_kind = AdmitterKind::ALWAYS;
    q.dispatcher_kind = DispatcherKind::PRIORITY_FIFO;
    q.max_pending = DEFAULT_MAX_PENDING;
    q.max_inflight = DEFAULT_MAX_INFLIGHT;
    q.max_workers = DEFAULT_MAX_WORKERS;
    q.max_waiters_per_replica = DEFAULT_MAX_WAITERS_PER_REPLICA;
    let mut submit_rpm = RateLimit::default();
    submit_rpm.rate_per_minute = DEFAULT_MAX_SUBMIT_RPM;
    q.max_submit_rpm = Some(Box::new(submit_rpm));
    let mut dispatch_rpm = RateLimit::default();
    dispatch_rpm.rate_per_minute = DEFAULT_MAX_DISPATCH_RPM;
    q.max_dispatch_rpm = Some(Box::new(dispatch_rpm));
    q.max_replay_per_second = DEFAULT_MAX_REPLAY_PER_SECOND;
    q.max_retries_ceiling = DEFAULT_MAX_RETRIES_CEILING;
    q.max_idempotency_ttl_seconds = DEFAULT_MAX_IDEMPOTENCY_TTL_SECONDS;
    q.max_payload_bytes = DEFAULT_MAX_PAYLOAD_BYTES;
    q.max_details_bytes = DEFAULT_MAX_DETAILS_BYTES;
    q.min_heartbeat_interval_seconds = DEFAULT_MIN_HEARTBEAT_INTERVAL_SECONDS;
    q.lease_duration_seconds = DEFAULT_LEASE_DURATION_SECONDS;
    q.lazy_extension_threshold_seconds = DEFAULT_LAZY_EXTENSION_THRESHOLD_SECONDS;
    q.max_error_classes = DEFAULT_MAX_ERROR_CLASSES;
    q.max_task_types = DEFAULT_MAX_TASK_TYPES;
    q.trace_sampling_ratio = DEFAULT_TRACE_SAMPLING_RATIO;
    q.log_level_override = LogLevel::INFO;
    q.audit_log_retention_days = DEFAULT_AUDIT_LOG_RETENTION_DAYS;
    q.metrics_export_enabled = true;
    q.enabled = true;
    q
}

#[derive(Serialize)]
struct ActionReport {
    rpc: &'static str,
    namespace: String,
    status: &'static str,
}

impl Renderable for ActionReport {
    fn render_human(&self, out: &mut dyn Write) -> io::Result<()> {
        render_status(
            &format!(
                "{} {} on namespace {:?}",
                self.rpc, self.status, self.namespace
            ),
            out,
        )
    }
}

async fn cmd_namespace_create(
    endpoint: &str,
    token: Option<&str>,
    name: &str,
    audit_note: Option<String>,
    format: OutputFormat,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    if name.is_empty() {
        return Err(CliError::InvalidArgument("namespace name is empty".into()));
    }
    let mut client = connect_admin(endpoint, token).await?;
    let mut req = SetNamespaceQuotaRequest::default();
    req.quota = Some(Box::new(fresh_quota(name)));
    if let Some(note) = audit_note {
        req.audit_note = Some(note);
    }
    let resp = client
        .set_namespace_quota(auth_request(token, req))
        .await
        .map_err(|s| lift_transport(endpoint, s))?
        .into_inner();
    if let Some(err) = resp.error.as_ref() {
        return Err(lift_rejection(err));
    }
    emit(
        &ActionReport {
            rpc: "namespace create",
            namespace: name.to_owned(),
            status: "succeeded",
        },
        format,
        out,
    )
}

// ---------------------------------------------------------------------------
// `namespace list`
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct NamespaceRow {
    namespace: String,
    pending_count: u64,
    inflight_count: u64,
    waiting_retry_count: u64,
    workers_registered: u32,
    waiters_active: u32,
}

#[derive(Serialize)]
struct NamespaceListReport {
    namespaces: Vec<NamespaceRow>,
}

impl Renderable for NamespaceListReport {
    fn render_human(&self, out: &mut dyn Write) -> io::Result<()> {
        let headers = [
            "NAMESPACE",
            "PENDING",
            "INFLIGHT",
            "WAITING_RETRY",
            "WORKERS",
            "WAITERS",
        ];
        let rows: Vec<Vec<String>> = self
            .namespaces
            .iter()
            .map(|n| {
                vec![
                    n.namespace.clone(),
                    n.pending_count.to_string(),
                    n.inflight_count.to_string(),
                    n.waiting_retry_count.to_string(),
                    n.workers_registered.to_string(),
                    n.waiters_active.to_string(),
                ]
            })
            .collect();
        render_table(&headers, &rows, out)
    }
}

async fn cmd_namespace_list(
    endpoint: &str,
    token: Option<&str>,
    namespace: &str,
    format: OutputFormat,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    // No `ListNamespaces` admin RPC exists in v1; we surface a single
    // row by calling `GetStats` for the requested namespace. When the
    // proto adds `ListNamespaces`, `cmd_namespace_list` switches over
    // to it and the row vector grows.
    let mut client = connect_admin(endpoint, token).await?;
    let mut req = GetStatsRequest::default();
    req.namespace = Some(namespace.to_owned());
    let resp = client
        .get_stats(auth_request(token, req))
        .await
        .map_err(|s| lift_transport(endpoint, s))?
        .into_inner();
    if let Some(err) = resp.error.as_ref() {
        return Err(lift_rejection(err));
    }
    let stats = resp.stats.unwrap_or_default();
    let report = NamespaceListReport {
        namespaces: vec![NamespaceRow {
            namespace: stats.namespace.unwrap_or_else(|| namespace.to_owned()),
            pending_count: stats.pending_count,
            inflight_count: stats.inflight_count,
            waiting_retry_count: stats.waiting_retry_count,
            workers_registered: stats.workers_registered,
            waiters_active: stats.waiters_active,
        }],
    };
    emit(&report, format, out)
}

// ---------------------------------------------------------------------------
// `namespace enable` / `namespace disable`
// ---------------------------------------------------------------------------

async fn cmd_namespace_enable(
    endpoint: &str,
    token: Option<&str>,
    name: &str,
    audit_note: Option<String>,
    format: OutputFormat,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    if name.is_empty() {
        return Err(CliError::InvalidArgument("namespace name is empty".into()));
    }
    let mut client = connect_admin(endpoint, token).await?;
    let mut req = EnableNamespaceRequest::default();
    req.namespace = Some(name.to_owned());
    if let Some(note) = audit_note {
        req.audit_note = Some(note);
    }
    let resp = client
        .enable_namespace(auth_request(token, req))
        .await
        .map_err(|s| lift_transport(endpoint, s))?
        .into_inner();
    if let Some(err) = resp.error.as_ref() {
        return Err(lift_rejection(err));
    }
    emit(
        &ActionReport {
            rpc: "namespace enable",
            namespace: name.to_owned(),
            status: "succeeded",
        },
        format,
        out,
    )
}

async fn cmd_namespace_disable(
    endpoint: &str,
    token: Option<&str>,
    name: &str,
    reason: Option<String>,
    audit_note: Option<String>,
    format: OutputFormat,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    if name.is_empty() {
        return Err(CliError::InvalidArgument("namespace name is empty".into()));
    }
    let mut client = connect_admin(endpoint, token).await?;
    let mut req = DisableNamespaceRequest::default();
    req.namespace = Some(name.to_owned());
    if let Some(r) = reason {
        req.reason = Some(r);
    }
    if let Some(note) = audit_note {
        req.audit_note = Some(note);
    }
    let resp = client
        .disable_namespace(auth_request(token, req))
        .await
        .map_err(|s| lift_transport(endpoint, s))?
        .into_inner();
    if let Some(err) = resp.error.as_ref() {
        return Err(lift_rejection(err));
    }
    emit(
        &ActionReport {
            rpc: "namespace disable",
            namespace: name.to_owned(),
            status: "succeeded",
        },
        format,
        out,
    )
}

// ---------------------------------------------------------------------------
// `set-quota`
// ---------------------------------------------------------------------------

async fn cmd_set_quota(
    endpoint: &str,
    token: Option<&str>,
    args: SetQuotaArgs,
    format: OutputFormat,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    if args.namespace.is_empty() {
        return Err(CliError::InvalidArgument("namespace is empty".into()));
    }
    let mut client = connect_admin(endpoint, token).await?;
    // Start from a baseline quota (so the server has every required
    // field) and overwrite the operator-supplied flags. Defaults match
    // `system_default`; tweaking only the supplied flags is the
    // expected operator workflow per `design.md` §6.7.
    let mut quota = fresh_quota(&args.namespace);
    if let Some(v) = args.max_pending {
        quota.max_pending = v;
    }
    if let Some(v) = args.max_inflight {
        quota.max_inflight = v;
    }
    if let Some(v) = args.max_workers {
        quota.max_workers = v;
    }
    if let Some(v) = args.max_waiters_per_replica {
        quota.max_waiters_per_replica = v;
    }
    if let Some(v) = args.max_submit_rpm {
        let mut r = RateLimit::default();
        r.rate_per_minute = v;
        quota.max_submit_rpm = Some(Box::new(r));
    }
    if let Some(v) = args.max_dispatch_rpm {
        let mut r = RateLimit::default();
        r.rate_per_minute = v;
        quota.max_dispatch_rpm = Some(Box::new(r));
    }
    if let Some(v) = args.max_payload_bytes {
        quota.max_payload_bytes = v;
    }
    if let Some(v) = args.max_idempotency_ttl_seconds {
        quota.max_idempotency_ttl_seconds = v;
    }
    if let Some(v) = args.min_heartbeat_interval_seconds {
        quota.min_heartbeat_interval_seconds = v;
    }
    if let Some(v) = args.lazy_extension_threshold_seconds {
        quota.lazy_extension_threshold_seconds = v;
    }
    if let Some(v) = args.max_error_classes {
        quota.max_error_classes = v;
    }
    if let Some(v) = args.max_task_types {
        quota.max_task_types = v;
    }
    if let Some(v) = args.trace_sampling_ratio {
        quota.trace_sampling_ratio = v;
    }
    if let Some(v) = args.audit_log_retention_days {
        quota.audit_log_retention_days = v;
    }
    if let Some(v) = args.disabled {
        quota.enabled = !v;
    }

    let mut req = SetNamespaceQuotaRequest::default();
    let namespace = args.namespace.clone();
    req.quota = Some(Box::new(quota));
    if let Some(note) = args.audit_note {
        req.audit_note = Some(note);
    }
    let resp = client
        .set_namespace_quota(auth_request(token, req))
        .await
        .map_err(|s| lift_transport(endpoint, s))?
        .into_inner();
    if let Some(err) = resp.error.as_ref() {
        return Err(lift_rejection(err));
    }
    emit(
        &ActionReport {
            rpc: "set-quota",
            namespace,
            status: "succeeded",
        },
        format,
        out,
    )
}

// ---------------------------------------------------------------------------
// `purge`
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct PurgeReport {
    namespace: String,
    purged_count: u32,
    has_more: bool,
    dry_run: bool,
}

impl Renderable for PurgeReport {
    fn render_human(&self, out: &mut dyn Write) -> io::Result<()> {
        let rows = vec![
            ("namespace", self.namespace.clone()),
            ("purged_count", self.purged_count.to_string()),
            ("has_more", self.has_more.to_string()),
            ("dry_run", self.dry_run.to_string()),
        ];
        render_kv(&rows, out)
    }
}

async fn cmd_purge(
    endpoint: &str,
    token: Option<&str>,
    args: PurgeArgs,
    auto_yes: bool,
    format: OutputFormat,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    if args.namespace.is_empty() {
        return Err(CliError::InvalidArgument("namespace is empty".into()));
    }
    if args.confirm_namespace != args.namespace {
        return Err(CliError::InvalidArgument(format!(
            "confirm-namespace {:?} does not match namespace {:?}",
            args.confirm_namespace, args.namespace
        )));
    }
    let plan_summary = format!(
        "will purge up to {} tasks from namespace {:?} (filter={:?}, status={:?})",
        args.max_tasks, args.namespace, args.filter, args.status
    );
    {
        let stderr = io::stderr();
        let mut err = stderr.lock();
        writeln!(err, "{plan_summary}").ok();
    }
    if args.dry_run {
        return emit(
            &PurgeReport {
                namespace: args.namespace.clone(),
                purged_count: 0,
                has_more: false,
                dry_run: true,
            },
            format,
            out,
        );
    }
    confirm_or_abort("Proceed with purge?", auto_yes)?;

    let mut client = connect_admin(endpoint, token).await?;
    let mut req = PurgeTasksRequest::default();
    req.namespace = Some(args.namespace.clone());
    req.confirm_namespace = Some(args.confirm_namespace.clone());
    req.filter = args.filter.clone();
    req.max_tasks = args.max_tasks;
    let composed_note = compose_audit_note(args.audit_note.clone(), &args.status);
    req.audit_note = composed_note;
    let resp = client
        .purge_tasks(auth_request(token, req))
        .await
        .map_err(|s| lift_transport(endpoint, s))?
        .into_inner();
    if let Some(err) = resp.error.as_ref() {
        return Err(lift_rejection(err));
    }
    emit(
        &PurgeReport {
            namespace: args.namespace,
            purged_count: resp.purged_count,
            has_more: resp.has_more,
            dry_run: false,
        },
        format,
        out,
    )
}

/// Append `--status` selectors to an operator-supplied audit note so
/// the audit row preserves intent even though the wire request does
/// not carry a structured status filter (`design.md` §6.7).
fn compose_audit_note(base: Option<String>, statuses: &[String]) -> Option<String> {
    if statuses.is_empty() {
        return base;
    }
    let suffix = format!("status={}", statuses.join(","));
    Some(match base {
        Some(s) if !s.is_empty() => format!("{s}; {suffix}"),
        _ => suffix,
    })
}

// ---------------------------------------------------------------------------
// `replay`
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ReplayReport {
    namespace: String,
    replayed_count: usize,
    skipped_count: u32,
    has_more: bool,
    dry_run: bool,
    failed: Vec<ReplayFailure>,
}

#[derive(Serialize)]
struct ReplayFailure {
    task_id: String,
    reason: String,
}

impl Renderable for ReplayReport {
    fn render_human(&self, out: &mut dyn Write) -> io::Result<()> {
        let rows = vec![
            ("namespace", self.namespace.clone()),
            ("replayed_count", self.replayed_count.to_string()),
            ("skipped_count", self.skipped_count.to_string()),
            ("has_more", self.has_more.to_string()),
            ("dry_run", self.dry_run.to_string()),
            ("failures", self.failed.len().to_string()),
        ];
        render_kv(&rows, out)?;
        if !self.failed.is_empty() {
            writeln!(out)?;
            let headers = ["TASK_ID", "REASON"];
            let table_rows: Vec<Vec<String>> = self
                .failed
                .iter()
                .map(|f| vec![f.task_id.clone(), f.reason.clone()])
                .collect();
            render_table(&headers, &table_rows, out)?;
        }
        Ok(())
    }
}

async fn cmd_replay(
    endpoint: &str,
    token: Option<&str>,
    args: ReplayArgs,
    auto_yes: bool,
    format: OutputFormat,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    if args.namespace.is_empty() {
        return Err(CliError::InvalidArgument("namespace is empty".into()));
    }
    if args.confirm_namespace != args.namespace {
        return Err(CliError::InvalidArgument(format!(
            "confirm-namespace {:?} does not match namespace {:?}",
            args.confirm_namespace, args.namespace
        )));
    }
    let plan_summary = format!(
        "will replay up to {} dead-letter tasks from namespace {:?} (filter={:?})",
        args.max_tasks, args.namespace, args.filter
    );
    {
        let stderr = io::stderr();
        let mut err = stderr.lock();
        writeln!(err, "{plan_summary}").ok();
    }
    if args.dry_run {
        return emit(
            &ReplayReport {
                namespace: args.namespace.clone(),
                replayed_count: 0,
                skipped_count: 0,
                has_more: false,
                dry_run: true,
                failed: Vec::new(),
            },
            format,
            out,
        );
    }
    confirm_or_abort("Proceed with replay?", auto_yes)?;

    let mut client = connect_admin(endpoint, token).await?;
    let mut req = ReplayDeadLettersRequest::default();
    req.namespace = Some(args.namespace.clone());
    req.confirm_namespace = Some(args.confirm_namespace.clone());
    req.filter = args.filter.clone();
    req.max_tasks = args.max_tasks;
    if let Some(note) = args.audit_note {
        req.audit_note = Some(note);
    }
    let resp = client
        .replay_dead_letters(auth_request(token, req))
        .await
        .map_err(|s| lift_transport(endpoint, s))?
        .into_inner();
    if let Some(err) = resp.error.as_ref() {
        return Err(lift_rejection(err));
    }
    let replayed = resp.replayed.unwrap_or_default();
    let mut failed = Vec::new();
    let mut ok_count = 0usize;
    for r in &replayed {
        if let Some(err) = r.error.as_ref() {
            failed.push(ReplayFailure {
                task_id: r.task_id.clone().unwrap_or_default(),
                reason: format!("{:?}", err.reason),
            });
        } else {
            ok_count += 1;
        }
    }
    emit(
        &ReplayReport {
            namespace: args.namespace,
            replayed_count: ok_count,
            skipped_count: resp.skipped_count,
            has_more: resp.has_more,
            dry_run: false,
            failed,
        },
        format,
        out,
    )
}

// ---------------------------------------------------------------------------
// `stats`
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct StatsReport {
    namespace: String,
    pending_count: u64,
    inflight_count: u64,
    waiting_retry_count: u64,
    workers_registered: u32,
    waiters_active: u32,
    quota_usage_ratio: f32,
    dispatch_latency_p50_ms: f32,
    dispatch_latency_p99_ms: f32,
}

impl Renderable for StatsReport {
    fn render_human(&self, out: &mut dyn Write) -> io::Result<()> {
        let rows = vec![
            ("namespace", self.namespace.clone()),
            ("pending_count", self.pending_count.to_string()),
            ("inflight_count", self.inflight_count.to_string()),
            ("waiting_retry_count", self.waiting_retry_count.to_string()),
            ("workers_registered", self.workers_registered.to_string()),
            ("waiters_active", self.waiters_active.to_string()),
            (
                "quota_usage_ratio",
                format!("{:.4}", self.quota_usage_ratio),
            ),
            (
                "dispatch_latency_p50_ms",
                format!("{:.2}", self.dispatch_latency_p50_ms),
            ),
            (
                "dispatch_latency_p99_ms",
                format!("{:.2}", self.dispatch_latency_p99_ms),
            ),
        ];
        render_kv(&rows, out)
    }
}

async fn cmd_stats(
    endpoint: &str,
    token: Option<&str>,
    args: StatsArgs,
    format: OutputFormat,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    if args.namespace.is_empty() {
        return Err(CliError::InvalidArgument("namespace is empty".into()));
    }
    let mut client = connect_admin(endpoint, token).await?;
    let mut req = GetStatsRequest::default();
    req.namespace = Some(args.namespace.clone());
    let resp = client
        .get_stats(auth_request(token, req))
        .await
        .map_err(|s| lift_transport(endpoint, s))?
        .into_inner();
    if let Some(err) = resp.error.as_ref() {
        return Err(lift_rejection(err));
    }
    let s = resp.stats.unwrap_or_default();
    let report = StatsReport {
        namespace: s.namespace.unwrap_or_else(|| args.namespace.clone()),
        pending_count: s.pending_count,
        inflight_count: s.inflight_count,
        waiting_retry_count: s.waiting_retry_count,
        workers_registered: s.workers_registered,
        waiters_active: s.waiters_active,
        quota_usage_ratio: s.quota_usage_ratio,
        dispatch_latency_p50_ms: s.dispatch_latency_p50_ms,
        dispatch_latency_p99_ms: s.dispatch_latency_p99_ms,
    };
    emit(&report, format, out)
}

// ---------------------------------------------------------------------------
// `list-workers`
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct WorkerRow {
    worker_id: String,
    task_types: Vec<String>,
    declared_concurrency: u32,
    inflight_count: u32,
    last_heartbeat_unix_ms: i64,
    declared_dead_unix_ms: i64,
}

#[derive(Serialize)]
struct ListWorkersReport {
    namespace: String,
    workers: Vec<WorkerRow>,
}

impl Renderable for ListWorkersReport {
    fn render_human(&self, out: &mut dyn Write) -> io::Result<()> {
        if self.workers.is_empty() {
            return render_status(
                &format!("no workers registered in namespace {:?}", self.namespace),
                out,
            );
        }
        let headers = [
            "WORKER_ID",
            "TASK_TYPES",
            "CONCURRENCY",
            "INFLIGHT",
            "LAST_HB_MS",
            "DEAD_AT_MS",
        ];
        let rows: Vec<Vec<String>> = self
            .workers
            .iter()
            .map(|w| {
                vec![
                    w.worker_id.clone(),
                    w.task_types.join(","),
                    w.declared_concurrency.to_string(),
                    w.inflight_count.to_string(),
                    w.last_heartbeat_unix_ms.to_string(),
                    if w.declared_dead_unix_ms == 0 {
                        "-".to_string()
                    } else {
                        w.declared_dead_unix_ms.to_string()
                    },
                ]
            })
            .collect();
        render_table(&headers, &rows, out)
    }
}

async fn cmd_list_workers(
    endpoint: &str,
    token: Option<&str>,
    args: ListWorkersArgs,
    format: OutputFormat,
    out: &mut dyn Write,
) -> Result<(), CliError> {
    if args.namespace.is_empty() {
        return Err(CliError::InvalidArgument("namespace is empty".into()));
    }
    let mut client = connect_admin(endpoint, token).await?;
    let mut req = ListWorkersRequest::default();
    req.namespace = Some(args.namespace.clone());
    if args.include_dead {
        // The CP encodes `include_dead` in the page_token slot per
        // `taskq-cp/src/handlers/task_admin.rs::list_workers_impl`.
        req.page_token = Some("include_dead".to_owned());
    }
    let resp = client
        .list_workers(auth_request(token, req))
        .await
        .map_err(|s| lift_transport(endpoint, s))?
        .into_inner();
    if let Some(err) = resp.error.as_ref() {
        return Err(lift_rejection(err));
    }
    let workers = resp
        .workers
        .unwrap_or_default()
        .into_iter()
        .map(worker_info_to_row)
        .collect();
    let report = ListWorkersReport {
        namespace: args.namespace,
        workers,
    };
    emit(&report, format, out)
}

fn worker_info_to_row(w: WireWorkerInfo) -> WorkerRow {
    WorkerRow {
        worker_id: w.worker_id.unwrap_or_default(),
        task_types: w.task_types.unwrap_or_default(),
        declared_concurrency: w.declared_concurrency,
        inflight_count: w.inflight_count,
        last_heartbeat_unix_ms: w
            .last_heartbeat_at
            .as_deref()
            .map(|t| t.unix_millis)
            .unwrap_or(0),
        declared_dead_unix_ms: w
            .declared_dead_at
            .as_deref()
            .map(|t| t.unix_millis)
            .unwrap_or(0),
    }
}

// ---------------------------------------------------------------------------
// Tests — pure helpers only; RPC paths exercised in the integration suite.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use taskq_proto::RejectReason;

    #[test]
    fn fresh_quota_satisfies_lazy_extension_invariant() {
        // Arrange: fresh defaults must already pass the validator's
        // `lazy_extension_threshold >= 2 * min_heartbeat_interval` rule
        // — otherwise `namespace create` would always fail server-side.
        let q = fresh_quota("alpha");

        // Act
        let lazy = u64::from(q.lazy_extension_threshold_seconds);
        let min_hb = u64::from(q.min_heartbeat_interval_seconds);

        // Assert
        assert!(
            lazy >= 2 * min_hb,
            "lazy={lazy} must be >= 2*min_hb={min_hb} per design.md S9.1"
        );
    }

    #[test]
    fn fresh_quota_idempotency_ttl_is_within_ceiling() {
        // Arrange / Act
        let q = fresh_quota("alpha");

        // Assert: 90-day ceiling per design.md S8.3 / S9.1.
        let ninety_days_seconds: u64 = 90 * 24 * 60 * 60;
        assert!(q.max_idempotency_ttl_seconds <= ninety_days_seconds);
    }

    #[test]
    fn compose_audit_note_appends_status_to_existing_note() {
        // Arrange
        let base = Some("intentional cleanup".to_owned());
        let statuses = vec!["EXPIRED".to_owned(), "FAILED_NONRETRYABLE".to_owned()];

        // Act
        let composed = compose_audit_note(base, &statuses);

        // Assert
        assert_eq!(
            composed,
            Some("intentional cleanup; status=EXPIRED,FAILED_NONRETRYABLE".to_owned())
        );
    }

    #[test]
    fn compose_audit_note_returns_status_only_when_base_is_empty() {
        // Arrange / Act
        let composed = compose_audit_note(None, &["EXPIRED".to_owned()]);

        // Assert
        assert_eq!(composed, Some("status=EXPIRED".to_owned()));
    }

    #[test]
    fn compose_audit_note_returns_base_when_statuses_empty() {
        // Arrange / Act
        let composed = compose_audit_note(Some("hello".to_owned()), &[]);

        // Assert
        assert_eq!(composed, Some("hello".to_owned()));
    }

    #[test]
    fn lift_rejection_extracts_retry_after_when_set() {
        // Arrange
        let mut rej = taskq_proto::Rejection::default();
        rej.reason = RejectReason::SYSTEM_OVERLOAD;
        rej.retryable = true;
        let mut ts = taskq_proto::Timestamp::default();
        ts.unix_millis = 1234;
        rej.retry_after = Some(Box::new(ts));
        rej.hint = Some("backoff".to_owned());

        // Act
        let err = lift_rejection(&rej);

        // Assert
        match err {
            CliError::Rejected {
                reason,
                retry_after_ms,
                retryable,
                hint,
            } => {
                assert_eq!(reason, RejectReason::SYSTEM_OVERLOAD);
                assert_eq!(retry_after_ms, 1234);
                assert!(retryable);
                assert_eq!(hint, "backoff");
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }
}
