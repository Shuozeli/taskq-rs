//! Conformance requirement #4: atomic conditional insert.
//!
//! `design.md` §8.2 #4: "Idempotency-key insertion is 'insert if not exists,
//! else return existing' within a transaction. Postgres `INSERT ... ON
//! CONFLICT`, FoundationDB read-then-write inside a transaction
//! (conflict-tracked), SQLite `INSERT OR IGNORE` all qualify."
//!
//! The trait method exercising this is `StorageTx::insert_task` paired with
//! `lookup_idempotency` per `design.md` §6.1.

/// Two concurrent submits with the same `(namespace, idempotency_key)` and
/// matching payload hash must produce exactly one task row; the loser must
/// observe the existing dedup record on retry and report success against
/// that task_id.
///
/// The Phase 9 implementation will:
///   - Spawn two concurrent submit transactions with identical key + hash.
///   - Race them through `lookup_idempotency` → `insert_task`.
///   - Assert exactly one task row exists; the loser either receives
///     `StorageError::SerializationConflict` (and on retry observes the
///     winner) or `StorageError::ConstraintViolation` (immediate
///     mismatch surface).
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_concurrent_idempotent_submit_keeps_one_row() {
    // Arrange — namespace seeded; two transactions ready with same (key, hash).
    // Act       — race both through insert_task.
    // Assert    — exactly one tasks row; loser surfaces the existing task_id.
    todo!("Phase 9: needs a concrete Storage impl");
}

/// Concurrent submits with the same key but different payload hashes —
/// design.md §6.1 step 2 demands `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`
/// rejection — must produce exactly one task row. The mismatched submitter
/// must observe a hash mismatch (the CP layer translates this to the
/// rejection reason).
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_concurrent_idempotent_submit_rejects_payload_mismatch() {
    // Arrange — two submitters with same key, different payload_hash.
    // Act       — race both through insert_task.
    // Assert    — one task row; loser sees the existing record with a different
    //             payload_hash and surfaces ConstraintViolation upstream.
    todo!("Phase 9: needs a concrete Storage impl");
}

/// An expired dedup row must not block a fresh submit with the same key:
/// `lookup_idempotency` returns `None` for past-`expires_at` rows so the
/// new submit proceeds (`design.md` §6.1 step 2 lazy cleanup).
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_expired_dedup_does_not_block_fresh_submit() {
    // Arrange — seed a dedup row with expires_at < now.
    // Act       — fresh submit with the same key but new payload.
    // Assert    — submit succeeds; the new task_id is now bound to that key.
    todo!("Phase 9: needs a concrete Storage impl");
}
