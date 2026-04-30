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
//!     let opts = Options { vacuously_satisfied_skip_locking: false };
//!     run_all(&opts, || async { my_backend::Storage::for_test().await }).await;
//! }
//! ```
//!
//! For single-writer backends (SQLite, embedded scope per `design.md` §8.3)
//! the `vacuously_satisfied_skip_locking` flag tells the suite to mark test
//! #2 as trivially satisfied rather than running concurrent dispatchers
//! (which a single-writer backend cannot exercise meaningfully).
//!
//! ## Test convention: Arrange-Act-Assert
//!
//! Per the user's testing-patterns rule, every test in this suite is
//! structured Arrange / Act / Assert with explicit comments marking each
//! phase. One Act per test; descriptive `test_<behaviour>` names. The
//! placeholders below stamp the convention so Phase 9 implementations can
//! fill the bodies without restructuring.
//!
//! ## Phase status
//!
//! Phase 2 (this crate) ships only the test scaffolding. Every test is
//! `#[ignore = "implemented in Phase 9 once backends exist"]` and uses
//! `todo!()` (not `unimplemented!()`) so a backend implementor running
//! `cargo test --workspace` sees a panic message that points to "fill this
//! in" rather than "this is permanently missing". The two macros are
//! semantically identical at runtime; the choice signals intent to the
//! reader.

pub mod bounded_dedup_cleanup;
pub mod conditional_insert;
pub mod external_consistency;
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
}

/// Run every conformance test against the supplied backend factory.
///
/// `setup` is called once per test and returns a fresh `Storage`
/// implementation; the suite assumes each invocation produces an isolated
/// backend (e.g. a fresh in-memory SQLite database, a Postgres schema with
/// a unique name). Tests must not share state.
///
/// Phase 2 lands the trait shape only. Each requirement module currently
/// holds `#[ignore]`-annotated `#[tokio::test]` placeholders that backend
/// implementors will lift into their own test files in Phase 9.
pub async fn run_all<S, F, Fut>(_options: &Options, _setup: F)
where
    S: Storage,
    F: Fn() -> Fut,
    Fut: Future<Output = S>,
{
    // Phase 9 will dispatch into each requirement module here. For now the
    // tests live as `#[tokio::test]` items inside the modules so
    // `cargo test --workspace` discovers them; this function exists so the
    // public API for backend implementors is already locked in.
}
