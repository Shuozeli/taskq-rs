//! Conformance suite wiring for the Postgres backend.
//!
//! Postgres satisfies §8.2 #1-#5 strictly (SERIALIZABLE + `FOR UPDATE
//! SKIP LOCKED` + range partitioning + LISTEN/NOTIFY). The suite runs
//! against a fresh **database** per test (`taskq_phase9a_<uuid>`),
//! migrates it, drops it on completion. Per-database isolation is used
//! (rather than per-schema) so tests can run in parallel without
//! fighting over the database-level `search_path` setting that
//! `deadpool_postgres` connections inherit at session start.
//!
//! The harness uses `TASKQ_PG_TEST_URL_BASE` for the admin connection
//! string (default: `host=docker.yuacx.com port=5432 user=cyuan
//! password=cyuan dbname=postgres`); the per-test connection string is
//! derived by replacing `dbname=postgres` with the unique test
//! database.
//!
//! ## §8.2 #6 (bounded-cost dedup expiration)
//!
//! Postgres satisfies the requirement via daily-granularity range
//! partitioning of `idempotency_keys`. The cleanup path drops partitions
//! whose entire `expires_at` window is past `before` and returns the
//! number of partitions dropped (NOT rows). The bounded-time test in
//! `taskq-storage-conformance` operates on row counts, so we route
//! Postgres through the `vacuously_satisfied_bounded_cleanup = true`
//! flag — the suite still asserts the universal safety property
//! (`delete_expired_dedup_only_removes_expired`) but skips the row-batch
//! cap check.
//!
//! ## Skip semantics
//!
//! Tests are gated `#[ignore]` so they don't run by default; use
//! `cargo test -p taskq-storage-postgres --test conformance --
//! --ignored`. If the database is unreachable, tests skip cleanly with
//! a printed reason rather than failing the run.

use std::sync::Arc;

use taskq_storage_conformance::{
    bounded_dedup_cleanup, conditional_insert, external_consistency, range_scans, skip_locking,
    subscribe_ordering,
};
use taskq_storage_postgres::{migrate, PostgresConfig, PostgresStorage};
use uuid::Uuid;

const DEFAULT_ADMIN_URL: &str =
    "host=docker.yuacx.com port=5432 user=cyuan password=cyuan dbname=postgres";

fn admin_url() -> String {
    std::env::var("TASKQ_PG_TEST_URL_BASE").unwrap_or_else(|_| DEFAULT_ADMIN_URL.to_owned())
}

/// Replace the `dbname=...` segment with the per-test database name
/// so the connection string targets the freshly created database.
fn url_for_db(dbname: &str) -> String {
    let admin = admin_url();
    let mut parts: Vec<String> = Vec::new();
    let mut found = false;
    for part in admin.split_whitespace() {
        if let Some(_old) = part.strip_prefix("dbname=") {
            parts.push(format!("dbname={dbname}"));
            found = true;
        } else {
            parts.push(part.to_owned());
        }
    }
    if !found {
        parts.push(format!("dbname={dbname}"));
    }
    parts.join(" ")
}

/// Open the admin connection used to CREATE / DROP the per-test
/// databases. Returns `None` if the admin DB is unreachable.
async fn admin_connect() -> Option<(tokio_postgres::Client, tokio::task::JoinHandle<()>)> {
    let (client, conn) = match tokio_postgres::connect(&admin_url(), tokio_postgres::NoTls).await {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("[conformance] skip: cannot reach postgres admin db: {e}");
            return None;
        }
    };
    let handle = tokio::spawn(async move {
        let _ = conn.await;
    });
    Some((client, handle))
}

/// Provision a brand-new database, run migrations, return a connected
/// `Arc<PostgresStorage>` plus the database name. Returns `None` if the
/// admin DB is unreachable.
async fn fresh() -> Option<(Arc<PostgresStorage>, String)> {
    let (admin, admin_handle) = admin_connect().await?;
    let dbname = format!("taskq_phase9a_{}", Uuid::now_v7().simple());
    if let Err(e) = admin
        .batch_execute(&format!("CREATE DATABASE {dbname}"))
        .await
    {
        eprintln!("[conformance] skip: cannot CREATE DATABASE {dbname}: {e}");
        admin_handle.abort();
        return None;
    }
    drop(admin);
    admin_handle.abort();

    let storage = PostgresStorage::connect(PostgresConfig::new(url_for_db(&dbname)))
        .await
        .expect("connect storage");
    migrate(storage.pool()).await.expect("run migrations");
    Some((storage, dbname))
}

/// Drop the per-test database. Best-effort.
async fn drop_db(dbname: &str) {
    let Some((admin, admin_handle)) = admin_connect().await else {
        return;
    };
    // Terminate any leftover sessions (the listener task may still be
    // alive briefly). `WITH (FORCE)` requires Postgres 13+, which we
    // assume.
    let _ = admin
        .batch_execute(&format!("DROP DATABASE IF EXISTS {dbname} WITH (FORCE)"))
        .await;
    drop(admin);
    admin_handle.abort();
}

// ============================================================================
// Per-requirement entry points (each test below dispatches to a single
// conformance function so failures pinpoint the requirement they regressed).
// ============================================================================

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL_BASE to override)"]
async fn external_consistency_concurrent_inserts_serialize_correctly() {
    let Some((storage, dbname)) = fresh().await else {
        return;
    };
    external_consistency::concurrent_inserts_serialize_correctly(&*storage).await;
    drop(storage);
    drop_db(&dbname).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432"]
async fn external_consistency_read_after_write_observes_committed_state() {
    let Some((storage, dbname)) = fresh().await else {
        return;
    };
    external_consistency::read_after_write_observes_committed_state(&*storage).await;
    drop(storage);
    drop_db(&dbname).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432"]
async fn skip_locking_pick_and_lock_skip_locked_under_contention() {
    let Some((storage, dbname)) = fresh().await else {
        return;
    };
    skip_locking::pick_and_lock_skip_locked_under_contention(&*storage).await;
    drop(storage);
    drop_db(&dbname).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432"]
async fn range_scans_dispatch_query_filters_by_namespace_status_priority() {
    let Some((storage, dbname)) = fresh().await else {
        return;
    };
    range_scans::dispatch_query_filters_by_namespace_status_priority(&*storage).await;
    drop(storage);
    drop_db(&dbname).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432"]
async fn range_scans_pick_ordering_priority_fifo_orders_by_priority_desc_submitted_at_asc() {
    let Some((storage, dbname)) = fresh().await else {
        return;
    };
    range_scans::pick_ordering_priority_fifo_orders_by_priority_desc_submitted_at_asc(&*storage)
        .await;
    drop(storage);
    drop_db(&dbname).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432"]
async fn range_scans_pick_ordering_age_promoted_bubbles_old_tasks() {
    let Some((storage, dbname)) = fresh().await else {
        return;
    };
    range_scans::pick_ordering_age_promoted_bubbles_old_tasks(&*storage).await;
    drop(storage);
    drop_db(&dbname).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432"]
async fn conditional_insert_idempotency_key_insert_serializes_under_contention() {
    let Some((storage, dbname)) = fresh().await else {
        return;
    };
    conditional_insert::idempotency_key_insert_serializes_under_contention(&*storage).await;
    drop(storage);
    drop_db(&dbname).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432"]
async fn conditional_insert_idempotency_key_insert_with_different_payload_returns_constraint_violation(
) {
    let Some((storage, dbname)) = fresh().await else {
        return;
    };
    conditional_insert::idempotency_key_insert_with_different_payload_returns_constraint_violation(
        &*storage,
    )
    .await;
    drop(storage);
    drop_db(&dbname).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432"]
async fn conditional_insert_idempotency_key_lookup_finds_existing_record() {
    let Some((storage, dbname)) = fresh().await else {
        return;
    };
    conditional_insert::idempotency_key_lookup_finds_existing_record(&*storage).await;
    drop(storage);
    drop_db(&dbname).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432"]
async fn subscribe_ordering_subscribe_pending_observes_post_subscription_commit() {
    let Some((storage, dbname)) = fresh().await else {
        return;
    };
    subscribe_ordering::subscribe_pending_observes_post_subscription_commit(&*storage).await;
    drop(storage);
    drop_db(&dbname).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432"]
async fn subscribe_ordering_subscribe_pending_does_not_observe_pre_subscription_commit() {
    let Some((storage, dbname)) = fresh().await else {
        return;
    };
    subscribe_ordering::subscribe_pending_does_not_observe_pre_subscription_commit(&*storage).await;
    drop(storage);
    drop_db(&dbname).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432"]
async fn bounded_dedup_cleanup_delete_expired_dedup_only_removes_expired() {
    let Some((storage, dbname)) = fresh().await else {
        return;
    };
    bounded_dedup_cleanup::delete_expired_dedup_only_removes_expired(&*storage).await;
    drop(storage);
    drop_db(&dbname).await;
}
