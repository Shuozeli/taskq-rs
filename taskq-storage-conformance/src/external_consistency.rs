//! Conformance requirement #1: external consistency.
//!
//! `design.md` §8.2 #1: "Strict serializability for state-transition
//! transactions; commit order respects real-time order."
//!
//! The carve-outs from §1.1 (heartbeats, rate quotas) are NOT subject to
//! this requirement and are tested elsewhere (or, for rate quotas, are
//! deliberately left untested for the eventually-consistent path).

/// Two transactions touching the same row must serialize correctly: if T1
/// commits before T2 begins, T2 must observe T1's effect; concurrent
/// conflicting transactions must produce a result equivalent to *some*
/// serial schedule, with at most one committing.
///
/// The Phase 9 implementation will:
///   - Open two transactions that both read-then-update the same task row.
///   - Commit T1, then attempt commit on T2.
///   - Assert T2 either commits with T1's effect visible OR returns
///     `StorageError::SerializationConflict` (caller's transparent retry
///     path, `design.md` §6.4).
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_concurrent_writes_to_same_row_serialize() {
    // Arrange — open two transactions on the same task row.
    // Act       — commit T1, then T2.
    // Assert    — T2 either reflects T1 or surfaces SerializationConflict.
    todo!("Phase 9: needs a concrete Storage impl");
}

/// Read-then-write inside a single transaction must observe a consistent
/// snapshot. Used to validate that capacity-quota inline reads (§9.1) see
/// the same state the same transaction's write will hit.
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_inline_capacity_read_matches_inline_write() {
    // Arrange — seed a namespace with `max_pending = 1` and one PENDING task.
    // Act       — open a SERIALIZABLE tx, read capacity, then attempt a second insert.
    // Assert    — the second insert observes OverLimit and is rejected.
    todo!("Phase 9: needs a concrete Storage impl");
}
