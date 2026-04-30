//! Conformance requirement #6: bounded-cost dedup expiration.
//!
//! `design.md` §8.2 #6: "`delete_expired_dedup` must run in time bounded by
//! `n` (the batch size), not by the steady-state size of `idempotency_keys`.
//! Backends with mutable per-row delete cost (e.g. Postgres without
//! partitioning) MUST implement time-range partitioning of the dedup table
//! so cleanup is `DROP PARTITION`, not row-by-row `DELETE`."
//!
//! For Postgres this is satisfied via daily-granularity range partitioning
//! capped at 90 active partitions (`design.md` §8.3). SQLite is exempt by
//! virtue of its single-writer / dev-only scope.

/// Deleting `n` expired rows must cost time proportional to `n`, not to
/// the total table size. The test seeds a wide table and a narrow table
/// and asserts the deletion costs are within a tight ratio.
///
/// The Phase 9 implementation will:
///   - Seed `wide_total = 100_000` dedup rows, all expired, in setup A.
///   - Seed `narrow_total = 1_000` expired dedup rows in setup B.
///   - Time `delete_expired_dedup(now, 1_000)` on each.
///   - Assert the ratio is bounded (e.g. < 3×) — i.e. wide-table cost is
///     not 100× narrow-table cost. Exact factor is tuned per backend in
///     Phase 9; the assertion's purpose is to catch row-by-row DELETE
///     regressions on Postgres without partitioning.
///
/// SQLite-mode runs with `vacuously_satisfied_skip_locking` semantics for
/// requirement #2; for requirement #6 SQLite is also exempted at the
/// design level. The Phase 9 wiring will gate this test off for SQLite.
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_delete_expired_dedup_cost_scales_with_batch_not_table() {
    // Arrange — seed two backends: wide (100K rows) and narrow (1K rows).
    // Act       — time delete_expired_dedup(now, 1000) on each.
    // Assert    — wide-table runtime / narrow-table runtime < bounded ratio.
    todo!("Phase 9: needs a concrete Storage impl supporting partitioning");
}

/// `delete_expired_dedup` must NEVER delete rows whose `expires_at > before`.
/// This is the safety counterpart to the timing requirement: cheap
/// `DROP PARTITION` cleanup is only valid when the partition boundary
/// aligns with `expires_at`.
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_delete_expired_dedup_preserves_unexpired_rows() {
    // Arrange — seed a mix of expired and unexpired dedup rows.
    // Act       — delete_expired_dedup(now, large_n).
    // Assert    — the unexpired rows are still present afterwards.
    todo!("Phase 9: needs a concrete Storage impl");
}
