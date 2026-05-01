//! Operator-facing `TaskAdmin` service handler.
//!
//! `design.md` §3.2 / §6.7. Phase 5a stubs every method; Phase 5b implements
//! the admin RPCs (tasks.md §5.5). Every admin RPC writes an `audit_log`
//! row in the same SERIALIZABLE transaction.

use std::sync::Arc;

use crate::state::CpState;

/// Operator-facing `TaskAdmin` service handler.
#[derive(Clone)]
pub struct TaskAdminHandler {
    pub state: Arc<CpState>,
}

impl TaskAdminHandler {
    pub fn new(state: Arc<CpState>) -> Self {
        Self { state }
    }

    // -----------------------------------------------------------------
    // Phase 5b admin handlers (tasks.md §5.5)
    // -----------------------------------------------------------------

    /// `SetNamespaceQuota` with full validation (registry caps, TTL ceiling,
    /// ε invariant).
    pub async fn set_namespace_quota(&self) {
        todo!("phase-5b: TaskAdmin::SetNamespaceQuota")
    }

    /// `GetNamespaceQuota` — paired read for `Set*` round-trips.
    pub async fn get_namespace_quota(&self) {
        todo!("phase-5b: TaskAdmin::GetNamespaceQuota")
    }

    /// `SetNamespaceConfig` — strategies, registry additions; rejects writes
    /// that exceed cardinality caps.
    pub async fn set_namespace_config(&self) {
        todo!("phase-5b: TaskAdmin::SetNamespaceConfig")
    }

    /// `EnableNamespace` / `DisableNamespace` — flip the namespace flag.
    pub async fn enable_namespace(&self) {
        todo!("phase-5b: TaskAdmin::EnableNamespace")
    }

    /// See `enable_namespace`.
    pub async fn disable_namespace(&self) {
        todo!("phase-5b: TaskAdmin::DisableNamespace")
    }

    /// `PurgeTasks` per `design.md` §6.7. Uses the shared `cancel_internal`
    /// helper so the cancel-then-delete-dedup pair stays consistent with
    /// `CancelTask`.
    pub async fn purge_tasks(&self) {
        todo!("phase-5b: TaskAdmin::PurgeTasks")
    }

    /// `ReplayDeadLetters` per `design.md` §6.7. Rate-limited; rejects if
    /// the task's idempotency key has been reclaimed.
    pub async fn replay_dead_letters(&self) {
        todo!("phase-5b: TaskAdmin::ReplayDeadLetters")
    }

    /// `GetStats` — aggregated namespace stats for ops dashboards.
    pub async fn get_stats(&self) {
        todo!("phase-5b: TaskAdmin::GetStats")
    }

    /// `ListWorkers` — registered workers per namespace.
    pub async fn list_workers(&self) {
        todo!("phase-5b: TaskAdmin::ListWorkers")
    }
}
