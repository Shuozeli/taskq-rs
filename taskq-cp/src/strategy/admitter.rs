//! `Admitter` strategy trait + three v1 implementations.
//!
//! `design.md` §7.1 / `problems/05-dispatch-fairness.md`. Strategies run
//! inside the SubmitTask SERIALIZABLE transaction; they read whatever they
//! need (pending count, oldest-pending age) via the `StorageTx` already
//! borrowed by the handler.
//!
//! ## Phase 5a status
//!
//! - `Always`: real (one line) — accepts unconditionally.
//! - `MaxPending`: stub (`todo!()`) — Phase 5b implements via
//!   `check_capacity_quota(MaxPending)`.
//! - `CoDel`: stub (`todo!()`) — Phase 5b implements via the oldest-pending
//!   age read.
//!
//! ## Why `async-trait`?
//!
//! The CP picks per-namespace which `Admitter` to invoke at runtime. That
//! requires `dyn Admitter` dispatch. Native `async fn in trait` (used by
//! the storage trait) is not object-safe in a `dyn` context, so the
//! strategy traits use `async-trait`. This is a deliberate split: the
//! storage trait stays static-dispatch (Phase 2 baseline); strategies are
//! dyn-dispatch.

use async_trait::async_trait;

use taskq_storage::{Namespace, TaskType, Timestamp};

/// Submit-time context handed to every `Admitter::admit` call. Phase 5b
/// will fill in the call site (the `SubmitTask` handler in
/// `handlers/task_queue.rs`); Phase 5a defines the shape so strategy
/// authors can write against it.
#[derive(Debug, Clone)]
pub struct SubmitCtx {
    pub namespace: Namespace,
    pub task_type: TaskType,
    pub priority: i32,
    pub payload_bytes: usize,
    pub now: Timestamp,
}

/// Outcome of an admit decision. The reject reason maps directly to the
/// FlatBuffers `RejectReason` enum at the handler boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admit {
    Accept,
    Reject(RejectReason),
}

/// Reasons an admitter can reject. Mirror of the CP-internal projection
/// of `taskq.v1.RejectReason` — Phase 5b will collapse this enum and the
/// wire enum into one type once handler bodies land.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    NamespaceQuotaExceeded,
    MaxPendingExceeded,
    LatencyTargetExceeded,
    SystemOverload,
    NamespaceDisabled,
}

/// Submit-time admission gate. Implementations are stateless; whatever
/// state they need lives in the storage layer and is read transactionally.
#[async_trait]
pub trait Admitter: Send + Sync + 'static {
    /// Decide whether to admit this submit.
    ///
    /// Implementations MUST NOT commit or roll back the transaction; the
    /// handler owns the transaction lifecycle. Implementations MAY perform
    /// reads against `tx` (capacity-quota, age scans).
    async fn admit(&self, ctx: &SubmitCtx, tx: &mut dyn StorageTxDyn) -> Admit;

    /// Static name used by the strategy registry to map operator config
    /// strings (e.g. `"Always"`) to concrete impls. MUST be unique across
    /// admitter implementations.
    fn name(&self) -> &'static str;
}

/// Object-safe shim around `StorageTx`. The storage trait uses native
/// `async fn` returning `impl Future` which is not object-safe. Phase 5b
/// will define this shim with the methods strategies actually need
/// (`check_capacity_quota`, an "oldest pending" read for CoDel, etc.).
///
/// Phase 5a leaves it as a marker so `dyn Admitter` signatures compile.
/// Implementors of `StorageTx` plug in via a blanket impl in Phase 5b.
pub trait StorageTxDyn: Send {}

// ---------------------------------------------------------------------------
// `Always` — accept everything (default).
// ---------------------------------------------------------------------------

/// Default admitter: accepts unconditionally. `design.md` §7.1.
#[derive(Debug, Default, Clone, Copy)]
pub struct AlwaysAdmitter;

#[async_trait]
impl Admitter for AlwaysAdmitter {
    async fn admit(&self, _ctx: &SubmitCtx, _tx: &mut dyn StorageTxDyn) -> Admit {
        Admit::Accept
    }

    fn name(&self) -> &'static str {
        "Always"
    }
}

// ---------------------------------------------------------------------------
// `MaxPending` — reject if pending_count(namespace) > limit.
// ---------------------------------------------------------------------------

/// `MaxPending` admitter. Reads `check_capacity_quota(MaxPending)`.
///
/// Phase 5b: implement the body using the storage trait. The struct is
/// final-shaped so strategy registry code already knows how to instantiate
/// it from `NamespaceQuota::admitter_params`.
#[derive(Debug, Clone, Copy)]
pub struct MaxPendingAdmitter {
    /// Pending-count ceiling. Read from `NamespaceQuota.max_pending`.
    pub limit: u64,
}

#[async_trait]
impl Admitter for MaxPendingAdmitter {
    async fn admit(&self, _ctx: &SubmitCtx, _tx: &mut dyn StorageTxDyn) -> Admit {
        // TODO(phase-5b): call check_capacity_quota(MaxPending) and compare
        //                 to self.limit; emit Admit::Reject(MaxPendingExceeded)
        //                 if over.
        todo!("phase-5b: MaxPending admitter")
    }

    fn name(&self) -> &'static str {
        "MaxPending"
    }
}

// ---------------------------------------------------------------------------
// `CoDel` — reject if oldest pending has aged past target latency.
// ---------------------------------------------------------------------------

/// `CoDel` admitter. Reads the oldest PENDING task's age in the namespace
/// and compares it to a target latency.
///
/// Phase 5b: implement the body. Storage trait may need a new method
/// `oldest_pending_age(ns)`; that's a Phase 5b decision.
#[derive(Debug, Clone, Copy)]
pub struct CoDelAdmitter {
    /// Target latency in milliseconds. Reject when oldest-pending exceeds.
    pub target_latency_ms: u64,
}

#[async_trait]
impl Admitter for CoDelAdmitter {
    async fn admit(&self, _ctx: &SubmitCtx, _tx: &mut dyn StorageTxDyn) -> Admit {
        // TODO(phase-5b): read oldest-pending age, compare against
        //                 self.target_latency_ms.
        todo!("phase-5b: CoDel admitter")
    }

    fn name(&self) -> &'static str {
        "CoDel"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Concrete dummy impl so we can build a `&mut dyn StorageTxDyn` in tests
    /// without spinning up a real backend.
    struct DummyTx;
    impl StorageTxDyn for DummyTx {}

    fn submit_ctx() -> SubmitCtx {
        SubmitCtx {
            namespace: Namespace::new("test"),
            task_type: TaskType::new("dummy"),
            priority: 0,
            payload_bytes: 0,
            now: Timestamp::from_unix_millis(0),
        }
    }

    #[tokio::test]
    async fn always_admitter_accepts_unconditionally() {
        // Arrange
        let admitter = AlwaysAdmitter;
        let ctx = submit_ctx();
        let mut tx = DummyTx;

        // Act
        let decision = admitter.admit(&ctx, &mut tx).await;

        // Assert
        assert_eq!(decision, Admit::Accept);
    }

    #[test]
    fn admitter_names_are_unique() {
        // Arrange / Act / Assert
        assert_eq!(AlwaysAdmitter.name(), "Always");
        assert_eq!(MaxPendingAdmitter { limit: 0 }.name(), "MaxPending");
        assert_eq!(
            CoDelAdmitter {
                target_latency_ms: 0
            }
            .name(),
            "CoDel"
        );
    }
}
