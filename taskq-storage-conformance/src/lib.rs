//! `taskq-storage-conformance`: shared conformance tests that every taskq-rs
//! storage backend must pass.
//!
//! The six requirements from `design.md` §8.2 each get one module here:
//!
//! 1. [`external_consistency`] — strict serializability of state-transition
//!    transactions.
//! 2. [`skip_locking`] — non-blocking row locking with skip semantics.
//! 3. [`range_scans`] — indexed range scans with predicates.
//! 4. [`conditional_insert`] — atomic conditional insert (idempotency
//!    races).
//! 5. [`subscribe_ordering`] — subscribe-pending ordering invariant.
//! 6. [`bounded_dedup_cleanup`] — bounded-cost dedup expiration.
//!
//! ## How a backend implementor wires this up
//!
//! ```ignore
//! use taskq_storage_conformance::{run_all, Options};
//!
//! #[tokio::test]
//! async fn pg_conformance() {
//!     let opts = Options {
//!         vacuously_satisfied_skip_locking: false,
//!         vacuously_satisfied_bounded_cleanup: true,
//!         poll_driven_subscribe: false,
//!     };
//!     run_all(&opts, || async { my_backend::Storage::for_test().await }).await;
//! }
//! ```
//!
//! For single-writer backends (SQLite, embedded scope per `design.md`
//! §8.3) the `vacuously_satisfied_skip_locking` flag tells the suite to
//! mark test #2 as trivially satisfied rather than running concurrent
//! dispatchers (which a single-writer backend cannot exercise
//! meaningfully). `poll_driven_subscribe = true` skips the negative
//! direction of the §8.2 #5 ordering invariant for backends whose
//! `subscribe_pending` stream over-delivers wakes (e.g. SQLite's 500 ms
//! polling).
//!
//! ## Test convention: Arrange-Act-Assert
//!
//! Per the user's testing-patterns rule, every test in this suite is
//! structured Arrange / Act / Assert with explicit comments marking each
//! phase. One Act per test; descriptive `test_<behaviour>` names. The
//! tests live as `pub async fn` items so backend test crates can invoke
//! them through [`run_all`] (which constructs a fresh backend per case)
//! without `taskq-storage-conformance` needing to depend on any backend
//! crate.

pub mod bounded_dedup_cleanup;
pub mod conditional_insert;
pub mod external_consistency;
pub mod helpers;
pub mod range_scans;
pub mod skip_locking;
pub mod subscribe_ordering;

use std::future::Future;

use taskq_storage::Storage;

/// Suite-level options a backend implementor passes to [`run_all`].
///
/// `Default` sets `vacuously_satisfied_skip_locking = false` — the
/// concurrent-writer path. Single-writer backends opt in explicitly.
#[derive(Debug, Clone, Copy, Default)]
pub struct Options {
    /// `true` for single-writer backends (SQLite) that satisfy §8.2 #2
    /// vacuously. `false` for backends supporting concurrent writers
    /// (Postgres, Cockroach, FoundationDB, …) — the suite then runs the
    /// concurrent-dispatcher test. See `design.md` §8.2 #2.
    pub vacuously_satisfied_skip_locking: bool,
    /// `true` for backends that satisfy §8.2 #6 vacuously (no time-range
    /// partitioning). SQLite is exempted at the design level — the suite
    /// then asserts only the safety property (does not delete unexpired
    /// rows) without comparing scaling cost. `false` for backends that
    /// implement partition-drop cleanup (Postgres).
    pub vacuously_satisfied_bounded_cleanup: bool,
    /// `true` for poll-only backends whose `subscribe_pending` stream
    /// over-delivers wake signals on a fixed timer (SQLite). The §8.2 #5
    /// contract is "at least one wake on a matching commit"; backends MAY
    /// over-deliver. With this flag set the suite skips the negative
    /// "no wake before subscribe" assertion that only commit-driven
    /// backends (Postgres NOTIFY) can honor strictly.
    pub poll_driven_subscribe: bool,
}

/// Run every conformance test against the supplied backend factory.
///
/// `setup` is called once per test and returns a fresh `Storage`
/// implementation; the suite assumes each invocation produces an isolated
/// backend (e.g. a fresh in-memory SQLite database, a Postgres schema with
/// a unique name). Tests must not share state.
pub async fn run_all<S, F, Fut>(options: &Options, setup: F)
where
    S: Storage,
    F: Fn() -> Fut,
    Fut: Future<Output = S>,
{
    // External consistency (§8.2 #1).
    let s = setup().await;
    external_consistency::concurrent_inserts_serialize_correctly(&s).await;
    let s = setup().await;
    external_consistency::read_after_write_observes_committed_state(&s).await;

    // Skip locking (§8.2 #2). Single-writer backends mark this vacuous.
    if options.vacuously_satisfied_skip_locking {
        skip_locking::vacuously_satisfied_for_single_writer().await;
    } else {
        let s = setup().await;
        skip_locking::pick_and_lock_skip_locked_under_contention(&s).await;
    }

    // Range scans (§8.2 #3).
    let s = setup().await;
    range_scans::dispatch_query_filters_by_namespace_status_priority(&s).await;
    let s = setup().await;
    range_scans::pick_ordering_priority_fifo_orders_by_priority_desc_submitted_at_asc(&s).await;
    let s = setup().await;
    range_scans::pick_ordering_age_promoted_bubbles_old_tasks(&s).await;

    // Conditional insert (§8.2 #4).
    let s = setup().await;
    conditional_insert::idempotency_key_insert_serializes_under_contention(&s).await;
    let s = setup().await;
    conditional_insert::idempotency_key_insert_with_different_payload_returns_constraint_violation(
        &s,
    )
    .await;
    let s = setup().await;
    conditional_insert::idempotency_key_lookup_finds_existing_record(&s).await;

    // Subscribe-pending ordering (§8.2 #5).
    let s = setup().await;
    subscribe_ordering::subscribe_pending_observes_post_subscription_commit(&s).await;
    if !options.poll_driven_subscribe {
        let s = setup().await;
        subscribe_ordering::subscribe_pending_does_not_observe_pre_subscription_commit(&s).await;
    }

    // Bounded dedup cleanup (§8.2 #6).
    if options.vacuously_satisfied_bounded_cleanup {
        bounded_dedup_cleanup::vacuously_satisfied_for_unpartitioned_backend().await;
    } else {
        let s = setup().await;
        bounded_dedup_cleanup::delete_expired_dedup_runs_in_bounded_time_with_n(&s).await;
    }
    let s = setup().await;
    bounded_dedup_cleanup::delete_expired_dedup_only_removes_expired(&s).await;
}
