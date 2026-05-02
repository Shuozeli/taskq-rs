//! `clap` command-line interface definition for `taskq-cli`.
//!
//! Top-level shape:
//!
//! ```text
//! taskq-cli [--endpoint URL] [--token TOKEN] [--format human|json] [--yes] <subcommand>
//! ```
//!
//! Subcommands:
//!
//! - `namespace create|list|enable|disable` — namespace lifecycle
//! - `set-quota` — wraps `SetNamespaceQuota` with one flag per quota field
//! - `purge` — wraps `PurgeTasks`; destructive, gated by confirmation
//! - `replay` — wraps `ReplayDeadLetters`; destructive, gated by confirmation
//! - `stats` — wraps `GetStats`
//! - `list-workers` — wraps `ListWorkers`
//!
//! Every flag mirrors the shape called out in `tasks.md` Phase 8 and
//! `design.md` §6.7. Optional quota flags (`Option<T>`) are not sent on
//! the wire when unset; the server's `system_default` namespace then
//! fills in defaults (`design.md` §9.1).

use clap::{Args, Parser, Subcommand};

use crate::output::OutputFormat;

/// Top-level CLI entry point.
#[derive(Parser, Debug)]
#[command(
    name = "taskq-cli",
    version,
    about = "Operator CLI for taskq-rs (namespace admin, quota, purge, replay, stats).",
    long_about = "Operator CLI for taskq-rs.\n\
                  \n\
                  Connects to a remote taskq-cp control plane via gRPC and wraps the \
                  TaskAdmin admin RPCs with operator-friendly subcommands. Set \
                  TASKQ_ENDPOINT (or pass --endpoint) to point the CLI at a server."
)]
pub struct Cli {
    /// gRPC endpoint of the taskq-cp server.
    #[arg(
        long,
        env = "TASKQ_ENDPOINT",
        default_value = "http://localhost:50051",
        global = true
    )]
    pub endpoint: String,

    /// Auth token (Phase 7 placeholder; v1 ignores it but accepts it for
    /// forward compatibility).
    #[arg(long, env = "TASKQ_TOKEN", global = true)]
    pub token: Option<String>,

    /// Output format.
    #[arg(long, value_enum, default_value_t = OutputFormat::Human, global = true)]
    pub format: OutputFormat,

    /// Skip the interactive confirmation prompt for destructive
    /// operations (`purge`, `replay`). Useful in scripts; equivalent to
    /// piping `y\n` on stdin.
    #[arg(long, global = true)]
    pub yes: bool,

    #[command(subcommand)]
    pub cmd: Cmd,
}

/// Top-level subcommands. Each variant carries its own argument struct.
#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Namespace lifecycle (`create`, `list`, `enable`, `disable`).
    #[command(subcommand)]
    Namespace(NamespaceCmd),

    /// Update namespace quota fields. Flags left unset are omitted from
    /// the wire request; the server merges in `system_default` values.
    SetQuota(SetQuotaArgs),

    /// Purge tasks matching a filter. Destructive — requires
    /// `--confirm-namespace` and an interactive confirmation.
    Purge(PurgeArgs),

    /// Replay terminal-failure tasks back to PENDING. Destructive —
    /// requires `--confirm-namespace` and an interactive confirmation.
    Replay(ReplayArgs),

    /// Snapshot of in-memory counters for a namespace.
    Stats(StatsArgs),

    /// List workers in a namespace.
    ListWorkers(ListWorkersArgs),
}

/// `taskq-cli namespace ...` subcommands.
#[derive(Subcommand, Debug)]
pub enum NamespaceCmd {
    /// Create a namespace by writing a fresh `system_default`-derived
    /// quota row. Existing namespaces are silently overwritten with
    /// defaults; use `set-quota` instead to tune fields.
    Create {
        /// Namespace identifier (non-empty).
        name: String,

        /// Optional audit note threaded into the audit log entry.
        #[arg(long)]
        audit_note: Option<String>,
    },

    /// List namespaces. v1 enumerates the workers RPC for the supplied
    /// namespace; cluster-wide enumeration lands in a future phase
    /// (no `ListNamespaces` admin RPC exists yet).
    List {
        /// Namespace to inspect. Required until a cluster-wide
        /// enumeration RPC lands; the CLI lists the namespace's stats
        /// row as a stand-in.
        #[arg(long)]
        namespace: String,
    },

    /// Mark a namespace `enabled = true`. Reverses a previous `disable`.
    Enable {
        /// Namespace identifier.
        name: String,

        /// Optional audit note.
        #[arg(long)]
        audit_note: Option<String>,
    },

    /// Mark a namespace `enabled = false`. SubmitTask will reject with
    /// `NAMESPACE_DISABLED` until re-enabled.
    Disable {
        /// Namespace identifier.
        name: String,

        /// Operator-visible reason; surfaced in `Rejection.hint` to
        /// callers that hit the disabled namespace.
        #[arg(long)]
        reason: Option<String>,

        /// Optional audit note.
        #[arg(long)]
        audit_note: Option<String>,
    },
}

/// Arguments for `set-quota`.
#[derive(Args, Debug)]
pub struct SetQuotaArgs {
    /// Target namespace.
    pub namespace: String,

    #[arg(long)]
    pub max_pending: Option<u64>,
    #[arg(long)]
    pub max_inflight: Option<u64>,
    #[arg(long)]
    pub max_workers: Option<u32>,
    #[arg(long)]
    pub max_waiters_per_replica: Option<u32>,
    #[arg(long)]
    pub max_submit_rpm: Option<u64>,
    #[arg(long)]
    pub max_dispatch_rpm: Option<u64>,
    #[arg(long)]
    pub max_payload_bytes: Option<u32>,
    #[arg(long)]
    pub max_idempotency_ttl_seconds: Option<u64>,
    #[arg(long)]
    pub min_heartbeat_interval_seconds: Option<u32>,
    #[arg(long)]
    pub lazy_extension_threshold_seconds: Option<u32>,
    #[arg(long)]
    pub max_error_classes: Option<u32>,
    #[arg(long)]
    pub max_task_types: Option<u32>,
    #[arg(long)]
    pub trace_sampling_ratio: Option<f32>,
    #[arg(long)]
    pub audit_log_retention_days: Option<u32>,
    /// Toggle `disabled` on the namespace via the same RPC. v1 prefers
    /// the dedicated `namespace enable|disable` paths; this flag is here
    /// for parity with the request struct.
    #[arg(long)]
    pub disabled: Option<bool>,

    #[arg(long)]
    pub audit_note: Option<String>,
}

/// Arguments for `purge`.
#[derive(Args, Debug)]
pub struct PurgeArgs {
    /// Target namespace.
    #[arg(long)]
    pub namespace: String,

    /// Operator must repeat the namespace to confirm. Server enforces
    /// `confirm_namespace == namespace`.
    #[arg(long)]
    pub confirm_namespace: String,

    /// Server-side filter expression (CEL-subset, see
    /// `design.md` §6.7). Empty matches all tasks in the namespace.
    #[arg(long)]
    pub filter: Option<String>,

    /// Reserved for client-side rendering only; the server's filter
    /// language carries status conditions inside `--filter`. The flag
    /// is accepted (multi-value) so operators can record intent in
    /// audit notes; values are appended to `audit_note`.
    #[arg(long = "status")]
    pub status: Vec<String>,

    /// Maximum tasks to purge per RPC; server caps at 5000.
    #[arg(long, default_value_t = 1000)]
    pub max_tasks: u32,

    /// Print the planned action and exit without invoking the server.
    #[arg(long)]
    pub dry_run: bool,

    /// Optional audit note.
    #[arg(long)]
    pub audit_note: Option<String>,
}

/// Arguments for `replay`.
#[derive(Args, Debug)]
pub struct ReplayArgs {
    /// Target namespace.
    #[arg(long)]
    pub namespace: String,

    /// Operator must repeat the namespace to confirm.
    #[arg(long)]
    pub confirm_namespace: String,

    /// Maximum tasks to replay per RPC; server caps at 5000.
    #[arg(long, default_value_t = 1000)]
    pub max_tasks: u32,

    /// Server-side filter expression (CEL-subset).
    #[arg(long)]
    pub filter: Option<String>,

    /// Optional audit note.
    #[arg(long)]
    pub audit_note: Option<String>,

    /// Print the planned action and exit without invoking the server.
    #[arg(long)]
    pub dry_run: bool,
}

/// Arguments for `stats`.
#[derive(Args, Debug)]
pub struct StatsArgs {
    /// Target namespace.
    pub namespace: String,
}

/// Arguments for `list-workers`.
#[derive(Args, Debug)]
pub struct ListWorkersArgs {
    /// Target namespace.
    pub namespace: String,

    /// Include workers that have been declared dead by Reaper B
    /// (`design.md` §6.6).
    #[arg(long)]
    pub include_dead: bool,
}
