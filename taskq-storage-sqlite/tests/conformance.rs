//! Conformance suite wiring for the SQLite backend.
//!
//! SQLite is single-process / single-writer per `design.md` §8.3, so
//! [`taskq_storage_conformance::Options::vacuously_satisfied_skip_locking`]
//! is set to `true` (the §8.2 #2 carve-out). Bounded-cost dedup expiration
//! (§8.2 #6) is exempted at the design level for SQLite because the
//! backend has no time-range partitioning; the suite still asserts the
//! safety property (no unexpired rows are deleted).
//!
//! Each test gets a fresh in-memory SQLite database from
//! `SqliteStorage::open_in_memory()`, so per-test isolation is automatic.

use taskq_storage_conformance::{
    bounded_dedup_cleanup, conditional_insert, external_consistency, range_scans,
    retry_progression, run_all, skip_locking, subscribe_ordering, Options,
};
use taskq_storage_sqlite::SqliteStorage;

async fn fresh_storage() -> SqliteStorage {
    SqliteStorage::open_in_memory()
        .await
        .expect("open in-memory sqlite")
}

fn options() -> Options {
    Options {
        vacuously_satisfied_skip_locking: true,
        vacuously_satisfied_bounded_cleanup: true,
        poll_driven_subscribe: true,
    }
}

#[tokio::test]
async fn sqlite_run_all_conformance_passes() {
    run_all(&options(), fresh_storage).await;
}

// ============================================================================
// Per-requirement entry points (each test below dispatches to a single
// conformance function so failures pinpoint the requirement they regressed).
// ============================================================================

#[tokio::test]
async fn external_consistency_concurrent_inserts_serialize_correctly() {
    let s = fresh_storage().await;
    external_consistency::concurrent_inserts_serialize_correctly(&s).await;
}

#[tokio::test]
async fn external_consistency_read_after_write_observes_committed_state() {
    let s = fresh_storage().await;
    external_consistency::read_after_write_observes_committed_state(&s).await;
}

#[tokio::test]
async fn skip_locking_vacuously_satisfied_for_single_writer() {
    skip_locking::vacuously_satisfied_for_single_writer().await;
}

#[tokio::test]
async fn range_scans_dispatch_query_filters_by_namespace_status_priority() {
    let s = fresh_storage().await;
    range_scans::dispatch_query_filters_by_namespace_status_priority(&s).await;
}

#[tokio::test]
async fn range_scans_pick_ordering_priority_fifo_orders_by_priority_desc_submitted_at_asc() {
    let s = fresh_storage().await;
    range_scans::pick_ordering_priority_fifo_orders_by_priority_desc_submitted_at_asc(&s).await;
}

#[tokio::test]
async fn range_scans_pick_ordering_age_promoted_bubbles_old_tasks() {
    let s = fresh_storage().await;
    range_scans::pick_ordering_age_promoted_bubbles_old_tasks(&s).await;
}

#[tokio::test]
async fn conditional_insert_idempotency_key_insert_serializes_under_contention() {
    let s = fresh_storage().await;
    conditional_insert::idempotency_key_insert_serializes_under_contention(&s).await;
}

#[tokio::test]
async fn conditional_insert_idempotency_key_insert_with_different_payload_returns_constraint_violation(
) {
    let s = fresh_storage().await;
    conditional_insert::idempotency_key_insert_with_different_payload_returns_constraint_violation(
        &s,
    )
    .await;
}

#[tokio::test]
async fn conditional_insert_idempotency_key_lookup_finds_existing_record() {
    let s = fresh_storage().await;
    conditional_insert::idempotency_key_lookup_finds_existing_record(&s).await;
}

#[tokio::test]
async fn subscribe_ordering_subscribe_pending_observes_post_subscription_commit() {
    let s = fresh_storage().await;
    subscribe_ordering::subscribe_pending_observes_post_subscription_commit(&s).await;
}

// Note: subscribe_pending_does_not_observe_pre_subscription_commit is gated
// behind `poll_driven_subscribe = true` for SQLite. Poll-only backends are
// permitted to over-deliver wakes per `design.md` §8.2 #5; the contract is
// "at least one wake on a matching commit", not "exactly one". The Postgres
// suite (commit-driven NOTIFY) runs this strictly.

#[tokio::test]
async fn retry_progression_worker_driven_retry_bumps_attempt_and_writes_per_attempt_row() {
    let s = fresh_storage().await;
    retry_progression::worker_driven_retry_bumps_attempt_and_writes_per_attempt_row(&s).await;
}

#[tokio::test]
async fn bounded_dedup_cleanup_vacuously_satisfied_for_unpartitioned_backend() {
    bounded_dedup_cleanup::vacuously_satisfied_for_unpartitioned_backend().await;
}

#[tokio::test]
async fn bounded_dedup_cleanup_delete_expired_dedup_only_removes_expired() {
    let s = fresh_storage().await;
    bounded_dedup_cleanup::delete_expired_dedup_only_removes_expired(&s).await;
}
