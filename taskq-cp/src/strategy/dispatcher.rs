//! `Dispatcher` strategy trait + three v1 implementations.
//!
//! `design.md` §7.2 / `problems/05`. Strategies translate an `AcquireTask`
//! call into a `PickCriteria` and call `pick_and_lock_pending` on the
//! `StorageTx`.
//!
//! ## Phase 5a status
//!
//! All three impls (`PriorityFifo`, `AgePromoted`, `RandomNamespace`) are
//! `todo!()` stubs. Phase 5b plugs in the call to `pick_and_lock_pending`
//! using the right `PickOrdering` variant.

use async_trait::async_trait;

use taskq_storage::{Namespace, TaskId, TaskType, Timestamp};

use crate::strategy::admitter::StorageTxDyn;

/// Acquire-time context handed to every `Dispatcher::pick_next` call.
#[derive(Debug, Clone)]
pub struct PullCtx {
    /// Worker identity is opaque to the dispatcher; only the task-type filter
    /// matters for picking. The handler uses worker_id elsewhere (lease).
    pub task_types: Vec<TaskType>,
    /// Namespace filter — typically a single namespace, but the
    /// `RandomNamespace` dispatcher samples across multiple.
    pub namespace_filter: Vec<Namespace>,
    pub now: Timestamp,
}

/// Picks the next task to dispatch. Implementations are stateless; whatever
/// they need lives in storage and is read inside the SERIALIZABLE
/// transaction owned by `AcquireTask`.
#[async_trait]
pub trait Dispatcher: Send + Sync + 'static {
    /// Pick the next pending task and return its id, or `None` if no
    /// candidate matches.
    ///
    /// Implementations MUST NOT commit/rollback the transaction; the
    /// handler owns the lifecycle.
    async fn pick_next(&self, ctx: &PullCtx, tx: &mut dyn StorageTxDyn) -> Option<TaskId>;

    /// Static name used by the strategy registry for runtime selection.
    fn name(&self) -> &'static str;
}

// ---------------------------------------------------------------------------
// `PriorityFifo` — ORDER BY priority DESC, submitted_at ASC.
// ---------------------------------------------------------------------------

/// Default dispatcher. Maps to `PickOrdering::PriorityFifo`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PriorityFifoDispatcher;

#[async_trait]
impl Dispatcher for PriorityFifoDispatcher {
    async fn pick_next(&self, _ctx: &PullCtx, _tx: &mut dyn StorageTxDyn) -> Option<TaskId> {
        // TODO(phase-5b): build PickCriteria with PickOrdering::PriorityFifo,
        //                 call tx.pick_and_lock_pending(criteria), return the
        //                 task_id (or None).
        todo!("phase-5b: PriorityFifo dispatcher")
    }

    fn name(&self) -> &'static str {
        "PriorityFifo"
    }
}

// ---------------------------------------------------------------------------
// `AgePromoted` — effective priority = base + f(age).
// ---------------------------------------------------------------------------

/// `AgePromoted` dispatcher. Maps to `PickOrdering::AgePromoted { age_weight }`.
#[derive(Debug, Clone, Copy)]
pub struct AgePromotedDispatcher {
    /// Multiplier on task age contributing to effective priority. Read from
    /// `NamespaceQuota.dispatcher_params`.
    pub age_weight: f64,
}

#[async_trait]
impl Dispatcher for AgePromotedDispatcher {
    async fn pick_next(&self, _ctx: &PullCtx, _tx: &mut dyn StorageTxDyn) -> Option<TaskId> {
        // TODO(phase-5b): build PickCriteria with PickOrdering::AgePromoted
        //                 { age_weight: self.age_weight }, call
        //                 tx.pick_and_lock_pending.
        todo!("phase-5b: AgePromoted dispatcher")
    }

    fn name(&self) -> &'static str {
        "AgePromoted"
    }
}

// ---------------------------------------------------------------------------
// `RandomNamespace` — sample uniformly from active namespaces.
// ---------------------------------------------------------------------------

/// `RandomNamespace` dispatcher. Maps to
/// `PickOrdering::RandomNamespace { sample_attempts }`.
#[derive(Debug, Clone, Copy)]
pub struct RandomNamespaceDispatcher {
    /// How many candidate namespaces to sample before giving up. Read from
    /// `NamespaceQuota.dispatcher_params`.
    pub sample_attempts: u32,
}

#[async_trait]
impl Dispatcher for RandomNamespaceDispatcher {
    async fn pick_next(&self, _ctx: &PullCtx, _tx: &mut dyn StorageTxDyn) -> Option<TaskId> {
        // TODO(phase-5b): build PickCriteria with PickOrdering::RandomNamespace
        //                 { sample_attempts: self.sample_attempts }; call
        //                 tx.pick_and_lock_pending.
        todo!("phase-5b: RandomNamespace dispatcher")
    }

    fn name(&self) -> &'static str {
        "RandomNamespace"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatcher_names_are_unique() {
        // Arrange / Act / Assert
        assert_eq!(PriorityFifoDispatcher.name(), "PriorityFifo");
        assert_eq!(
            AgePromotedDispatcher { age_weight: 1.0 }.name(),
            "AgePromoted"
        );
        assert_eq!(
            RandomNamespaceDispatcher { sample_attempts: 4 }.name(),
            "RandomNamespace"
        );
    }
}
