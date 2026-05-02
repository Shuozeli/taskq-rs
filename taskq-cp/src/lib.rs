//! `taskq-cp`: control-plane library entry point.
//!
//! The binary lives in `src/main.rs`; this library exists so integration
//! tests and embedded deployments (where the CP runs in-process beside
//! another service) can import the same types.
//!
//! ## Layout
//!
//! - [`config`] — `CpConfig` loading + env override.
//! - [`error`] — `CpError` for bootstrap-time failures.
//! - [`server`] — gRPC server bootstrap + interceptors.
//! - [`health`] — health/metrics HTTP server (separate port).
//! - [`observability`] — OpenTelemetry pipeline init + standard metric set.
//! - [`shutdown`] — SIGTERM/SIGINT handling and graceful drain plumbing.
//! - [`state`] — `CpState` shared between handlers and reapers.
//! - [`strategy`] — `Admitter` / `Dispatcher` traits + v1 implementations.
//! - [`handlers`] — gRPC service handler skeletons (Phase 5a stubs).
//!
//! Phase 5a is scaffold only. Phase 5b/5c/5d fill in the handler bodies,
//! reaper logic, and observability emit sites.
//!
//! See `design.md` §1, §6, §7, §11 and `tasks.md` Phase 5 for the full
//! plan.

#![forbid(unsafe_code)]

/// Schema version this binary expects to find in `taskq_meta.schema_version`.
///
/// Mirrored from `main.rs::SCHEMA_VERSION`; declared here as well so the
/// observability module can stamp `taskq_schema_version{component=taskq-cp}`
/// at startup without depending on the binary entrypoint.
pub const SCHEMA_VERSION: u32 = 2;

pub mod audit;
pub mod audit_pruner;
pub mod config;
pub mod error;
pub mod handlers;
pub mod health;
pub mod metrics_refresher;
pub mod observability;
pub mod reapers;
pub mod server;
pub mod shutdown;
pub mod state;
pub mod strategy;

pub use crate::config::{CpConfig, OtelExporterConfig, StorageBackendConfig};
pub use crate::error::{CpError, Result};
pub use crate::handlers::{TaskAdminHandler, TaskQueueHandler, TaskWorkerHandler};
pub use crate::observability::{init as init_observability, MetricsHandle, ObservabilityState};
pub use crate::server::{serve, serve_in_process};
pub use crate::shutdown::{
    channel as shutdown_channel, wait_for_signal, ShutdownReceiver, ShutdownSender,
};
pub use crate::state::{
    CpState, DynStorage, NamespaceConfigCache, StorageTxDyn, StorageTxFuture, WaiterHandle,
    WaiterLimitExceeded, WaiterPool, WakeOutcome, WakeSignalStream,
};
pub use crate::strategy::{
    admitter::{
        Admit, Admitter, AlwaysAdmitter, CoDelAdmitter, MaxPendingAdmitter, RejectReason, SubmitCtx,
    },
    build_admitter, build_dispatcher,
    dispatcher::{
        AgePromotedDispatcher, Dispatcher, PriorityFifoDispatcher, PullCtx,
        RandomNamespaceDispatcher,
    },
    AdmitterParams, DispatcherParams, NamespaceStrategy, StrategyMismatch, StrategyRegistry,
    StrategySlot,
};
