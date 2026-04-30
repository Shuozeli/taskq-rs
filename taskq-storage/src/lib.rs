//! `taskq-storage`: trait surface and shared types for taskq-rs storage backends.
//!
//! This crate defines the seam between the control plane and any storage
//! backend. Two traits — [`Storage`] (connection lifecycle) and
//! [`StorageTx`] (in-transaction operations) — are the only contract a
//! backend implementor must satisfy. All shared plain-data types, error
//! variants, domain newtypes, and strategy-intent enums live here so the CP
//! and every backend agree on one vocabulary.
//!
//! The design discussion lives in `design.md` §8 and
//! `problems/12-storage-abstraction.md`. Conformance requirements are
//! exercised by the sister crate `taskq-storage-conformance`.
//!
//! ## Native `async fn in trait`
//!
//! Phase 2 uses stable Rust's native `async fn in trait` (≥ 1.75). There is
//! no `async-trait` macro. The trade-off: `Send` bounds are explicit, and
//! the `subscribe_pending` return type uses `Box<dyn Stream<…> + Send +
//! Unpin>` rather than `impl Stream` because the latter cannot appear inside
//! a trait method's return type without `Unpin` constraints that pin every
//! backend to one concrete stream shape.
//!
//! ## Module layout
//!
//! - [`error`] — [`StorageError`] enum and `Result` alias.
//! - [`ids`] — domain newtypes (`Namespace`, `TaskId`, `WorkerId`, …).
//! - [`types`] — plain-data types crossing the trait boundary.
//! - [`traits`] — [`Storage`] and [`StorageTx`] themselves.

pub mod error;
pub mod ids;
pub mod traits;
pub mod types;

// Top-level re-exports keep `use taskq_storage::*;` ergonomic without
// flattening the module tree.

pub use crate::error::{BoxedError, Result, StorageError};
pub use crate::ids::{IdempotencyKey, Namespace, TaskId, TaskType, Timestamp, WorkerId};
pub use crate::traits::{Storage, StorageTx};
pub use crate::types::{
    AuditEntry, CapacityDecision, CapacityKind, DedupRecord, ExpiredRuntime, HeartbeatAck,
    LeaseRef, LockedTask, NamespaceFilter, NamespaceQuota, NewDedupRecord, NewLease, NewTask,
    PickCriteria, PickOrdering, RateDecision, RateKind, RuntimeRef, TaskOutcome, TaskTypeFilter,
    WakeSignal,
};
