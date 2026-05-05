//! End-to-end smoke tests for the Postgres backend.
//!
//! These exercise the happy paths of `insert_task`, `lookup_idempotency`,
//! `complete_task`, and the duplicate-payload-hash branch of
//! `insert_task`. They are NOT a substitute for the full conformance suite
//! (Phase 9 owns that); they validate that the SQL the trait methods emit
//! actually matches the schema in `migrations/0001_initial.sql`.
//!
//! ## Database
//!
//! Per the project infra defaults, the tests target `docker.yuacx.com:5432`
//! with user `cyuan` / password `cyuan`. Override via `TASKQ_PG_TEST_URL`.
//! Each test creates a dedicated Postgres schema named
//! `taskq_phase3_smoke_<unique>` and drops it on completion, so multiple
//! parallel test runs don't collide.
//!
//! ## Skip semantics
//!
//! Tests are gated by `#[ignore = "..."]` so they don't run by default; use
//! `cargo test -p taskq-storage-postgres -- --ignored`. If the database is
//! unreachable, the tests skip with a printed reason instead of failing
//! the run.

use std::time::SystemTime;

use bytes::Bytes;
use taskq_storage::ids::{IdempotencyKey, Namespace, TaskId, TaskType, Timestamp, WorkerId};
use taskq_storage::types::{LeaseRef, NewDedupRecord, NewLease, NewTask, TaskOutcome};
use taskq_storage::{Storage, StorageError, StorageTx};
use taskq_storage_postgres::{migrate, PostgresConfig, PostgresStorage};

const DEFAULT_TEST_URL: &str =
    "host=docker.yuacx.com port=5432 user=cyuan password=cyuan dbname=taskq_test_phase3";

fn test_url() -> String {
    std::env::var("TASKQ_PG_TEST_URL").unwrap_or_else(|_| DEFAULT_TEST_URL.to_owned())
}

fn now_ms() -> Timestamp {
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock before 1970");
    Timestamp::from_unix_millis(dur.as_millis() as i64)
}

/// Best-effort connectivity probe. Returns the connected `PostgresStorage`
/// or `None` with a printed reason. Tests use this to skip cleanly when
/// the database is unreachable (per task spec).
async fn try_connect() -> Option<std::sync::Arc<PostgresStorage>> {
    // First make sure the database itself exists. We connect to the
    // server-default `postgres` database to issue `CREATE DATABASE` if
    // needed; if even that fails the smoke tests skip.
    let admin_url = test_url().replace("dbname=taskq_test_phase3", "dbname=postgres");
    let (admin, admin_conn) = match tokio_postgres::connect(&admin_url, tokio_postgres::NoTls).await
    {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("[smoke] skip: cannot reach postgres admin db: {e}");
            return None;
        }
    };
    let admin_handle = tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    let exists: bool = match admin
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'taskq_test_phase3')",
            &[],
        )
        .await
    {
        Ok(row) => row.get(0),
        Err(e) => {
            eprintln!("[smoke] skip: cannot probe pg_database: {e}");
            admin_handle.abort();
            return None;
        }
    };
    if !exists {
        if let Err(e) = admin
            .batch_execute("CREATE DATABASE taskq_test_phase3")
            .await
        {
            eprintln!("[smoke] skip: cannot create taskq_test_phase3: {e}");
            admin_handle.abort();
            return None;
        }
    }
    drop(admin);
    admin_handle.abort();

    match PostgresStorage::connect(PostgresConfig::new(test_url())).await {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("[smoke] skip: cannot connect to taskq_test_phase3: {e}");
            None
        }
    }
}

/// Per-process counter so concurrent tests with the same suffix
/// still get distinct schema names.
static SCHEMA_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Pre-test helper: create a schema dedicated to this test. Schema-
/// scoped statements use a per-test storage built via
/// [`connect_in_schema`]; the admin storage passed here only owns
/// the create / drop calls.
async fn isolate_schema(admin: &PostgresStorage, suffix: &str) -> String {
    let counter = SCHEMA_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let schema_name = format!("taskq_phase3_smoke_{suffix}_{counter}");
    let conn = admin.pool().get().await.expect("pool checkout failed");
    conn.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        .await
        .expect("drop existing schema");
    conn.batch_execute(&format!("CREATE SCHEMA {schema_name}"))
        .await
        .expect("create schema");
    drop(conn);
    schema_name
}

/// Build a per-test `PostgresStorage` whose pool checks out
/// connections already pinned to `schema_name` via libpq
/// `options=-c search_path=...`. Mirrors the `admin.rs` pattern;
/// avoids the `ALTER DATABASE ... SET search_path` race that the old
/// `set_search_path` helper hit when tests ran in parallel.
async fn connect_in_schema(schema_name: &str) -> std::sync::Arc<PostgresStorage> {
    let conn_str = format!(
        "{base} options=-csearch_path={schema},public",
        base = test_url(),
        schema = schema_name,
    );
    PostgresStorage::connect(PostgresConfig::new(conn_str))
        .await
        .expect("connect with per-test search_path")
}

/// Drop the per-test schema. Best-effort.
async fn drop_schema(admin: &PostgresStorage, schema_name: &str) {
    if let Ok(conn) = admin.pool().get().await {
        let _ = conn
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
            .await;
    }
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn smoke_submit_then_complete_round_trips_through_storage() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "complete").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");

    let namespace = Namespace::new("smoke-ns");
    let task_type = TaskType::new("smoke.task");
    let key = IdempotencyKey::new("smoke-key-1");
    let task_id = TaskId::generate();
    let worker_id = WorkerId::generate();
    let now = now_ms();
    // expires_at: now + 1 hour
    let expires_at = Timestamp::from_unix_millis(now.as_unix_millis() + 3_600_000);

    let payload_hash = [0xABu8; 32];
    let new_task = NewTask {
        task_id,
        namespace: namespace.clone(),
        task_type: task_type.clone(),
        priority: 5,
        payload: Bytes::from_static(b"hello"),
        payload_hash,
        submitted_at: now,
        expires_at,
        max_retries: 3,
        retry_initial_ms: 1_000,
        retry_max_ms: 60_000,
        retry_coefficient: 2.0,
        traceparent: Bytes::from_static(b""),
        tracestate: Bytes::from_static(b""),
        format_version: 1,
    };
    let new_dedup = NewDedupRecord {
        namespace: namespace.clone(),
        key: key.clone(),
        payload_hash,
        expires_at,
    };

    // Act: insert + lookup + complete in two transactions.
    {
        let mut tx = storage.begin().await.expect("begin");
        tx.insert_task(new_task, new_dedup)
            .await
            .expect("insert_task");
        // record_acquisition so complete_task has a runtime row to lock.
        tx.record_acquisition(NewLease {
            task_id,
            attempt_number: 0,
            worker_id,
            acquired_at: now,
            timeout_at: expires_at,
        })
        .await
        .expect("record_acquisition");
        tx.commit().await.expect("commit");
    }

    let lease_ref = LeaseRef {
        task_id,
        attempt_number: 0,
        worker_id,
    };

    {
        let mut tx = storage.begin().await.expect("begin 2");
        tx.complete_task(
            &lease_ref,
            TaskOutcome::Success {
                result_payload: Bytes::from_static(b"ok"),
                recorded_at: now,
            },
        )
        .await
        .expect("complete_task");
        tx.commit().await.expect("commit 2");
    }

    // Assert: task_runtime row gone; task_results row present with success.
    let conn = storage.pool().get().await.expect("pool checkout");
    let runtime_count: i64 = conn
        .query_one(
            "SELECT COUNT(*)::bigint FROM task_runtime WHERE task_id = $1",
            &[&task_id.into_uuid()],
        )
        .await
        .expect("count runtime")
        .get(0);
    assert_eq!(
        runtime_count, 0,
        "task_runtime should be empty after complete"
    );

    let result_outcome: String = conn
        .query_one(
            "SELECT outcome::text FROM task_results WHERE task_id = $1 AND attempt_number = 0",
            &[&task_id.into_uuid()],
        )
        .await
        .expect("query result")
        .get(0);
    assert_eq!(result_outcome, "success");

    let task_status: String = conn
        .query_one(
            "SELECT status::text FROM tasks WHERE task_id = $1",
            &[&task_id.into_uuid()],
        )
        .await
        .expect("query status")
        .get(0);
    assert_eq!(task_status, "COMPLETED");

    drop(conn);
    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn smoke_duplicate_idempotency_key_with_different_payload_returns_constraint_violation() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "dedup").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");

    let namespace = Namespace::new("dedup-ns");
    let task_type = TaskType::new("dedup.task");
    let key = IdempotencyKey::new("shared-key");
    let now = now_ms();
    let expires_at = Timestamp::from_unix_millis(now.as_unix_millis() + 3_600_000);

    let task_id_a = TaskId::generate();
    let task_id_b = TaskId::generate();

    let payload_hash_a = [0x01u8; 32];
    let payload_hash_b = [0x02u8; 32];

    let mk_task = |id, ns: &Namespace, tt: &TaskType, hash| NewTask {
        task_id: id,
        namespace: ns.clone(),
        task_type: tt.clone(),
        priority: 0,
        payload: Bytes::from_static(b""),
        payload_hash: hash,
        submitted_at: now,
        expires_at,
        max_retries: 0,
        retry_initial_ms: 1_000,
        retry_max_ms: 1_000,
        retry_coefficient: 1.0,
        traceparent: Bytes::from_static(b""),
        tracestate: Bytes::from_static(b""),
        format_version: 1,
    };

    // Act 1: first insert succeeds.
    {
        let mut tx = storage.begin().await.expect("begin a");
        tx.insert_task(
            mk_task(task_id_a, &namespace, &task_type, payload_hash_a),
            NewDedupRecord {
                namespace: namespace.clone(),
                key: key.clone(),
                payload_hash: payload_hash_a,
                expires_at,
            },
        )
        .await
        .expect("first insert");
        tx.commit().await.expect("commit a");
    }

    // Assert: lookup_idempotency observes the dedup row.
    {
        let mut tx = storage.begin().await.expect("begin lookup");
        let found = tx
            .lookup_idempotency(&namespace, &key)
            .await
            .expect("lookup");
        let record = found.expect("dedup record present");
        assert_eq!(record.task_id, task_id_a);
        assert_eq!(record.payload_hash, payload_hash_a);
        tx.rollback().await.expect("rollback lookup");
    }

    // Act 2: second insert with the same idempotency key + different
    // payload hash MUST fail with a `ConstraintViolation` (mapped from
    // SQLSTATE 23505 unique_violation on the partition's PK).
    let result = {
        let mut tx = storage.begin().await.expect("begin b");
        let res = tx
            .insert_task(
                mk_task(task_id_b, &namespace, &task_type, payload_hash_b),
                NewDedupRecord {
                    namespace: namespace.clone(),
                    key: key.clone(),
                    payload_hash: payload_hash_b,
                    expires_at,
                },
            )
            .await;
        let _ = tx.rollback().await;
        res
    };

    // Assert
    match result {
        Err(StorageError::ConstraintViolation(_)) => {}
        other => panic!("expected ConstraintViolation, got {other:?}"),
    }

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432"]
async fn smoke_upsert_namespace_quota_round_trip() {
    use taskq_storage::types::NamespaceQuotaUpsert;

    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "quota").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");

    let ns = Namespace::new("quota-smoke");
    let upsert = NamespaceQuotaUpsert {
        max_pending: Some(100_000),
        max_inflight: Some(10_000),
        max_workers: Some(1_000),
        max_waiters_per_replica: Some(1_000),
        max_submit_rpm: Some(100_000),
        max_dispatch_rpm: Some(100_000),
        max_replay_per_second: Some(100),
        max_retries_ceiling: 25,
        max_idempotency_ttl_seconds: 7_776_000,
        max_payload_bytes: 10_485_760,
        max_details_bytes: 65_536,
        min_heartbeat_interval_seconds: 5,
        lazy_extension_threshold_seconds: 30,
        max_error_classes: 64,
        max_task_types: 32,
        trace_sampling_ratio: 0.1,
        log_level_override: Some("info".to_owned()),
        audit_log_retention_days: 90,
        metrics_export_enabled: true,
    };

    // Act
    let mut tx = storage.begin().await.expect("begin");
    let result = tx.upsert_namespace_quota(&ns, upsert).await;
    if result.is_ok() {
        tx.commit().await.expect("commit");
    } else {
        let _ = tx.rollback().await;
    }

    // Assert
    assert!(result.is_ok(), "upsert failed: {result:?}");

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres"]
async fn smoke_upsert_then_audit_then_commit_mimics_handler() {
    use taskq_storage::types::{AuditEntry, NamespaceQuotaUpsert};

    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "audit").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");

    let ns = Namespace::new("audit-smoke");
    let upsert = NamespaceQuotaUpsert {
        max_pending: Some(100_000),
        max_inflight: Some(10_000),
        max_workers: Some(1_000),
        max_waiters_per_replica: Some(1_000),
        max_submit_rpm: Some(100_000),
        max_dispatch_rpm: Some(100_000),
        max_replay_per_second: Some(100),
        max_retries_ceiling: 25,
        max_idempotency_ttl_seconds: 7_776_000,
        max_payload_bytes: 10_485_760,
        max_details_bytes: 65_536,
        min_heartbeat_interval_seconds: 5,
        lazy_extension_threshold_seconds: 30,
        max_error_classes: 64,
        max_task_types: 32,
        trace_sampling_ratio: 0.1,
        log_level_override: Some("info".to_owned()),
        audit_log_retention_days: 90,
        metrics_export_enabled: true,
    };

    // Act: upsert THEN audit-log-append in same tx, like the handler.
    let mut tx = storage.begin().await.expect("begin");
    let upsert_res = tx.upsert_namespace_quota(&ns, upsert).await;
    let audit_entry = AuditEntry {
        timestamp: now_ms(),
        actor: "anonymous".to_owned(),
        rpc: "SetNamespaceQuota".to_owned(),
        namespace: Some(ns.clone()),
        request_summary: Bytes::from_static(b"{}"),
        result: "success".to_owned(),
        request_hash: [0xAB; 32],
    };
    let audit_res = tx.audit_log_append(audit_entry).await;
    let commit_res = tx.commit().await;

    // Assert
    assert!(upsert_res.is_ok(), "upsert failed: {upsert_res:?}");
    assert!(audit_res.is_ok(), "audit failed: {audit_res:?}");
    assert!(commit_res.is_ok(), "commit failed: {commit_res:?}");

    drop_schema(&admin, &schema).await;
}
