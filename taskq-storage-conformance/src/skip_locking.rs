//! Conformance requirement #2: non-blocking row locking with skip semantics.
//!
//! `design.md` §8.2 #2: "`pick_and_lock_pending` skips locked rows rather
//! than blocking. Vacuously satisfied by single-writer backends (SQLite)
//! since there is nothing to skip; backends supporting concurrent writers
//! must implement true skip-locking (Postgres `FOR UPDATE SKIP LOCKED`,
//! FoundationDB conflict-range retry, etc.)."
//!
//! Backends opting into the vacuous form of this requirement set
//! [`crate::Options::vacuously_satisfied_skip_locking`] to `true`; the
//! suite then bypasses the concurrent-dispatcher test and records it as
//! satisfied by construction.

/// Two concurrent dispatchers calling `pick_and_lock_pending` against the
/// same namespace MUST each receive a different task — neither blocks the
/// other and neither double-dispatches the same row.
///
/// The Phase 9 implementation will:
///   - Seed N PENDING tasks (e.g. N = 8) in one namespace.
///   - Spawn K concurrent dispatcher tasks (e.g. K = 4), each calling
///     `pick_and_lock_pending` and committing.
///   - Assert each dispatcher locked a distinct task; total locks = K;
///     remaining PENDING = N − K; no dispatcher observed
///     `StorageError::SerializationConflict` (skip semantics, not
///     conflict-resolution semantics).
///
/// For backends with `vacuously_satisfied_skip_locking = true` this test
/// is skipped — the test's mere presence (compiled, not run) documents
/// the carve-out.
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_concurrent_dispatchers_skip_lock_distinct_rows() {
    // Arrange — N PENDING tasks, K concurrent dispatchers.
    // Act       — every dispatcher calls pick_and_lock_pending then commits.
    // Assert    — K distinct task_ids locked; no SerializationConflict observed.
    todo!("Phase 9: needs a concrete Storage impl with concurrent writers");
}

/// A locked row must not block a concurrent dispatcher: the second caller
/// returns immediately with `Ok(None)` (no other PENDING row available)
/// rather than waiting on the first transaction's lock.
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_locked_row_does_not_block_concurrent_picker() {
    // Arrange — single PENDING task. T1 locks it but doesn't commit yet.
    // Act       — T2 calls pick_and_lock_pending in parallel.
    // Assert    — T2 returns Ok(None) within a tight time bound (no blocking).
    todo!("Phase 9: needs a concrete Storage impl with concurrent writers");
}
