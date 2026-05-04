//! Admin-path `StorageTx` smoke tests for the Postgres backend.
//!
//! The `taskq-storage-conformance` suite (Phase 9) covers the universal
//! §8.2 properties — skip-locking, conditional-insert dedup, lease
//! ownership, and so on — but it does not exercise the §6.7 admin write
//! and read methods (`enable_namespace` / `disable_namespace`,
//! `cancel_task`, `replay_task`, `add_error_classes` / `add_task_types`
//! and friends, `list_tasks_by_*`, `count_tasks_by_status`, …). Those
//! methods are the bulk of the uncovered Postgres backend code.
//!
//! Each test in this file follows the same shape as `tests/smoke.rs`:
//!
//! 1. `try_connect()` — opens an admin pool against the test database,
//!    skipping the test cleanly when the server is unreachable.
//! 2. `isolate_schema(...)` — creates a fresh schema named
//!    `taskq_phase3_admin_<suffix>` so concurrent test runs do not
//!    collide.
//! 3. Per-test `PostgresStorage::connect(...)` with the schema baked into
//!    the connection's `options=-c search_path=...`. Going through the
//!    connect string (rather than `ALTER DATABASE`) keeps tests isolated
//!    even when the harness runs them in parallel.
//! 4. `migrate(...)` to install the schema.
//! 5. Exercise the trait method through a real `StorageTx`.
//! 6. `drop_schema(...)` at the end.
//!
//! Tests are gated by `#[ignore = "..."]` so a default `cargo test`
//! never tries to reach Postgres; run them with
//! `cargo test -p taskq-storage-postgres --test admin -- --include-ignored`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use taskq_storage::ids::{IdempotencyKey, Namespace, TaskId, TaskType, Timestamp, WorkerId};
use taskq_storage::types::{
    AuditEntry, CancelOutcome, NamespaceQuotaUpsert, NewDedupRecord, NewLease, NewTask,
    ReplayOutcome, TaskFilter, TaskOutcome, TaskStatus, TerminalState,
};
use taskq_storage::{Storage, StorageTx};
use taskq_storage_postgres::{migrate, PostgresConfig, PostgresStorage};

const DEFAULT_TEST_URL_BASE: &str =
    "host=docker.yuacx.com port=5432 user=cyuan password=cyuan dbname=taskq_test_phase3";

/// Per-process counter so two tests in the same binary that pick the same
/// suffix string still get distinct schema names.
static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

fn test_url() -> String {
    std::env::var("TASKQ_PG_TEST_URL").unwrap_or_else(|_| DEFAULT_TEST_URL_BASE.to_owned())
}

fn admin_url() -> String {
    let url = test_url();
    // Probe the same host/credentials but against the server-default
    // `postgres` database so we can `CREATE DATABASE` if needed.
    if let Some(idx) = url.find("dbname=") {
        let mut prefix = url[..idx].to_owned();
        prefix.push_str("dbname=postgres");
        return prefix;
    }
    format!("{url} dbname=postgres")
}

fn target_dbname() -> String {
    let url = test_url();
    for part in url.split_whitespace() {
        if let Some(rest) = part.strip_prefix("dbname=") {
            return rest.to_owned();
        }
    }
    "taskq_test_phase3".to_owned()
}

fn now_ms() -> Timestamp {
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock before 1970");
    Timestamp::from_unix_millis(dur.as_millis() as i64)
}

/// Best-effort connectivity probe matching the smoke.rs flow: ensure the
/// target database exists, then return a `PostgresStorage` connected to it.
/// Returns `None` (with a printed reason) when the server is unreachable.
async fn try_connect() -> Option<Arc<PostgresStorage>> {
    let admin_url = admin_url();
    let dbname = target_dbname();

    let (admin, admin_conn) = match tokio_postgres::connect(&admin_url, tokio_postgres::NoTls).await
    {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("[admin] skip: cannot reach postgres admin db: {e}");
            return None;
        }
    };
    let admin_handle = tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    let exists: bool = match admin
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
            &[&dbname.as_str()],
        )
        .await
    {
        Ok(row) => row.get(0),
        Err(e) => {
            eprintln!("[admin] skip: cannot probe pg_database: {e}");
            admin_handle.abort();
            return None;
        }
    };
    if !exists {
        let create_sql = format!("CREATE DATABASE {dbname}");
        if let Err(e) = admin.batch_execute(&create_sql).await {
            eprintln!("[admin] skip: cannot create {dbname}: {e}");
            admin_handle.abort();
            return None;
        }
    }
    drop(admin);
    admin_handle.abort();

    match PostgresStorage::connect(PostgresConfig::new(test_url())).await {
        Ok(s) => Some(s),
        Err(e) => {
            eprintln!("[admin] skip: cannot connect to {dbname}: {e}");
            None
        }
    }
}

/// Allocate a unique schema name and `CREATE SCHEMA` it via the admin pool.
/// The returned schema name is suitable for both `set_search_path` and
/// `drop_schema`.
async fn isolate_schema(storage: &PostgresStorage, suffix: &str) -> String {
    let counter = SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed);
    let schema_name = format!("taskq_phase3_admin_{suffix}_{counter}");
    let conn = storage.pool().get().await.expect("pool checkout failed");
    conn.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        .await
        .expect("drop existing schema");
    conn.batch_execute(&format!("CREATE SCHEMA {schema_name}"))
        .await
        .expect("create schema");
    drop(conn);
    schema_name
}

/// Build a per-test `PostgresStorage` whose pool checks out connections
/// already pinned to `schema_name` via libpq `options=-c search_path=...`.
/// This gives the test true isolation regardless of what other tests are
/// doing to the database default — the migrations and trait calls inside
/// the test land in the dedicated schema.
async fn connect_in_schema(schema_name: &str) -> Arc<PostgresStorage> {
    // libpq treats `-c key=value` inside `options=` as a startup `SET`.
    // Quoting note: search_path entries must be plain identifiers, which
    // our schema names are by construction, so no escaping is needed.
    let conn_str = format!(
        "{base} options=-csearch_path={schema},public",
        base = test_url(),
        schema = schema_name,
    );
    PostgresStorage::connect(PostgresConfig::new(conn_str))
        .await
        .expect("connect with per-test search_path")
}

/// Drop the per-test schema. Best-effort — uses the admin pool that was
/// used to create it, since the per-test pool is normally already dropped
/// by the time we get here.
async fn drop_schema(storage: &PostgresStorage, schema_name: &str) {
    if let Ok(conn) = storage.pool().get().await {
        let _ = conn
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
            .await;
    }
}

/// Build a [`NamespaceQuotaUpsert`] populated with sensible test defaults
/// — saves every test the boilerplate of spelling out 19 fields.
fn sample_quota_upsert() -> NamespaceQuotaUpsert {
    NamespaceQuotaUpsert {
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
    }
}

/// Build a [`NewTask`] with the listed identity and otherwise-default
/// fields. The expiration is one hour past `submitted_at`.
fn sample_new_task(
    task_id: TaskId,
    namespace: &Namespace,
    task_type: &TaskType,
    submitted_at: Timestamp,
) -> NewTask {
    let expires_at = Timestamp::from_unix_millis(submitted_at.as_unix_millis() + 3_600_000);
    NewTask {
        task_id,
        namespace: namespace.clone(),
        task_type: task_type.clone(),
        priority: 0,
        payload: Bytes::from_static(b""),
        payload_hash: [0xAA; 32],
        submitted_at,
        expires_at,
        max_retries: 0,
        retry_initial_ms: 1_000,
        retry_max_ms: 1_000,
        retry_coefficient: 1.0,
        traceparent: Bytes::from_static(b""),
        tracestate: Bytes::from_static(b""),
        format_version: 1,
    }
}

/// Insert one task with a unique idempotency key. The dedup row's payload
/// hash matches the task's so the §8.2 idempotency invariants hold.
async fn seed_task(
    storage: &PostgresStorage,
    namespace: &Namespace,
    task_type: &TaskType,
    key_str: &str,
) -> TaskId {
    let task_id = TaskId::generate();
    let now = now_ms();
    let task = sample_new_task(task_id, namespace, task_type, now);
    let dedup = NewDedupRecord {
        namespace: namespace.clone(),
        key: IdempotencyKey::new(key_str),
        payload_hash: task.payload_hash,
        expires_at: task.expires_at,
    };
    let mut tx = storage.begin().await.expect("begin seed");
    tx.insert_task(task, dedup).await.expect("insert_task");
    tx.commit().await.expect("commit seed");
    task_id
}

/// Drive the `complete_task` path with the given outcome so we land the
/// `tasks` row in a specific terminal status. The caller is expected to
/// have already inserted the task via `seed_task`.
async fn drive_to_terminal(
    storage: &PostgresStorage,
    task_id: TaskId,
    outcome: TaskOutcome,
) -> WorkerId {
    let worker_id = WorkerId::generate();
    let now = now_ms();
    let timeout_at = Timestamp::from_unix_millis(now.as_unix_millis() + 3_600_000);

    let mut tx = storage.begin().await.expect("begin acquire");
    tx.record_acquisition(NewLease {
        task_id,
        attempt_number: 0,
        worker_id,
        acquired_at: now,
        timeout_at,
    })
    .await
    .expect("record_acquisition");
    tx.commit().await.expect("commit acquire");

    let lease = taskq_storage::types::LeaseRef {
        task_id,
        attempt_number: 0,
        worker_id,
    };
    let mut tx = storage.begin().await.expect("begin complete");
    tx.complete_task(&lease, outcome)
        .await
        .expect("complete_task");
    tx.commit().await.expect("commit complete");

    worker_id
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_add_error_classes_then_deprecate_one_marks_only_that_row() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "errcls").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("errcls-ns");

    let mut tx = storage.begin().await.expect("begin");
    tx.add_error_classes(&ns, &["transient".to_owned(), "permanent".to_owned()])
        .await
        .expect("add_error_classes");
    tx.commit().await.expect("commit");

    // Act
    let mut tx = storage.begin().await.expect("begin deprecate");
    tx.deprecate_error_class(&ns, "transient")
        .await
        .expect("deprecate_error_class");
    tx.commit().await.expect("commit deprecate");

    // Assert: read the registry directly; only `transient` is deprecated.
    let conn = storage.pool().get().await.expect("pool checkout");
    let rows = conn
        .query(
            "SELECT error_class, deprecated FROM error_class_registry \
              WHERE namespace = $1 ORDER BY error_class",
            &[&ns.as_str()],
        )
        .await
        .expect("query registry");
    let observed: Vec<(String, bool)> = rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, bool>(1)))
        .collect();
    assert_eq!(
        observed,
        vec![
            ("permanent".to_owned(), false),
            ("transient".to_owned(), true),
        ]
    );

    drop(conn);
    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_add_task_types_then_deprecate_one_marks_only_that_row() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "tasktypes").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("tasktypes-ns");
    let alpha = TaskType::new("alpha");
    let beta = TaskType::new("beta");

    let mut tx = storage.begin().await.expect("begin");
    tx.add_task_types(&ns, &[alpha.clone(), beta.clone()])
        .await
        .expect("add_task_types");
    tx.commit().await.expect("commit");

    // Act
    let mut tx = storage.begin().await.expect("begin deprecate");
    tx.deprecate_task_type(&ns, &alpha)
        .await
        .expect("deprecate_task_type");
    tx.commit().await.expect("commit deprecate");

    // Assert
    let conn = storage.pool().get().await.expect("pool checkout");
    let rows = conn
        .query(
            "SELECT task_type, deprecated FROM task_type_registry \
              WHERE namespace = $1 ORDER BY task_type",
            &[&ns.as_str()],
        )
        .await
        .expect("query registry");
    let observed: Vec<(String, bool)> = rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, bool>(1)))
        .collect();
    assert_eq!(
        observed,
        vec![("alpha".to_owned(), true), ("beta".to_owned(), false)]
    );

    drop(conn);
    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_add_error_classes_with_empty_slice_is_a_noop() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "errcls_empty").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("empty-ns");

    // Act
    let mut tx = storage.begin().await.expect("begin");
    tx.add_error_classes(&ns, &[])
        .await
        .expect("add_error_classes empty");
    tx.commit().await.expect("commit");

    // Assert: registry contains zero rows for this namespace.
    let conn = storage.pool().get().await.expect("pool checkout");
    let count: i64 = conn
        .query_one(
            "SELECT COUNT(*)::bigint FROM error_class_registry WHERE namespace = $1",
            &[&ns.as_str()],
        )
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 0);

    drop(conn);
    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_disable_then_is_namespace_disabled_returns_true() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "disable").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("disable-ns");
    // Seed a quota row so the disable UPDATE has something to flip.
    let mut tx = storage.begin().await.expect("begin upsert");
    tx.upsert_namespace_quota(&ns, sample_quota_upsert())
        .await
        .expect("upsert_namespace_quota");
    tx.commit().await.expect("commit upsert");

    // Act
    let mut tx = storage.begin().await.expect("begin disable");
    tx.disable_namespace(&ns).await.expect("disable_namespace");
    tx.commit().await.expect("commit disable");

    // Assert
    let mut tx = storage.begin().await.expect("begin read");
    let observed = tx
        .is_namespace_disabled(&ns)
        .await
        .expect("is_namespace_disabled");
    tx.rollback().await.expect("rollback read");
    assert!(observed, "namespace should report as disabled");

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_enable_namespace_clears_disabled_flag() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "enable").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("enable-ns");

    let mut tx = storage.begin().await.expect("begin seed");
    tx.upsert_namespace_quota(&ns, sample_quota_upsert())
        .await
        .expect("upsert");
    tx.disable_namespace(&ns).await.expect("disable_namespace");
    tx.commit().await.expect("commit seed");

    // Act
    let mut tx = storage.begin().await.expect("begin enable");
    tx.enable_namespace(&ns).await.expect("enable_namespace");
    tx.commit().await.expect("commit enable");

    // Assert
    let mut tx = storage.begin().await.expect("begin read");
    let observed = tx
        .is_namespace_disabled(&ns)
        .await
        .expect("is_namespace_disabled");
    tx.rollback().await.expect("rollback read");
    assert!(!observed, "namespace should report as not disabled");

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_is_namespace_disabled_returns_false_for_unknown_namespace() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "disabled_unknown").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");

    // Act
    let mut tx = storage.begin().await.expect("begin");
    let observed = tx
        .is_namespace_disabled(&Namespace::new("never-seen"))
        .await
        .expect("is_namespace_disabled");
    tx.rollback().await.expect("rollback");

    // Assert
    assert!(!observed);

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_list_tasks_by_filter_narrows_by_task_type() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "filter").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("filter-ns");
    let alpha = TaskType::new("alpha");
    let beta = TaskType::new("beta");

    let task_alpha = seed_task(&storage, &ns, &alpha, "filter-alpha-1").await;
    let _task_beta = seed_task(&storage, &ns, &beta, "filter-beta-1").await;

    // Act: filter by task_type=alpha only.
    let mut tx = storage.begin().await.expect("begin");
    let rows = tx
        .list_tasks_by_filter(
            &ns,
            TaskFilter {
                task_types: Some(vec![alpha.clone()]),
                statuses: None,
                submitted_before: None,
                submitted_after: None,
            },
            10,
        )
        .await
        .expect("list_tasks_by_filter");
    tx.rollback().await.expect("rollback");

    // Assert
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].task_id, task_alpha);
    assert_eq!(rows[0].task_type.as_str(), "alpha");

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_list_tasks_by_filter_narrows_by_status() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "filter_status").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("filter-status-ns");
    let task_type = TaskType::new("work");

    // One PENDING task and one we drive to COMPLETED.
    let _pending = seed_task(&storage, &ns, &task_type, "filter-status-pending").await;
    let completed = seed_task(&storage, &ns, &task_type, "filter-status-completed").await;
    drive_to_terminal(
        &storage,
        completed,
        TaskOutcome::Success {
            result_payload: Bytes::from_static(b"ok"),
            recorded_at: now_ms(),
        },
    )
    .await;

    // Act: filter for status = COMPLETED only.
    let mut tx = storage.begin().await.expect("begin");
    let rows = tx
        .list_tasks_by_filter(
            &ns,
            TaskFilter {
                task_types: None,
                statuses: Some(vec![TaskStatus::Completed]),
                submitted_before: None,
                submitted_after: None,
            },
            10,
        )
        .await
        .expect("list_tasks_by_filter");
    tx.rollback().await.expect("rollback");

    // Assert
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].task_id, completed);
    assert_eq!(rows[0].status, TaskStatus::Completed);

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_list_tasks_by_terminal_status_returns_only_requested_terminals() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "terminal").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("terminal-ns");
    let task_type = TaskType::new("work");

    let nonretryable = seed_task(&storage, &ns, &task_type, "term-nonret").await;
    drive_to_terminal(
        &storage,
        nonretryable,
        TaskOutcome::FailedNonretryable {
            error_class: "boom".to_owned(),
            error_message: "no".to_owned(),
            error_details: Bytes::from_static(b""),
            recorded_at: now_ms(),
        },
    )
    .await;

    let exhausted = seed_task(&storage, &ns, &task_type, "term-exh").await;
    drive_to_terminal(
        &storage,
        exhausted,
        TaskOutcome::FailedExhausted {
            error_class: "boom".to_owned(),
            error_message: "no".to_owned(),
            error_details: Bytes::from_static(b""),
            recorded_at: now_ms(),
        },
    )
    .await;

    // A COMPLETED task that should NOT appear in the result.
    let completed = seed_task(&storage, &ns, &task_type, "term-completed").await;
    drive_to_terminal(
        &storage,
        completed,
        TaskOutcome::Success {
            result_payload: Bytes::from_static(b"ok"),
            recorded_at: now_ms(),
        },
    )
    .await;

    // Act
    let mut tx = storage.begin().await.expect("begin");
    let rows = tx
        .list_tasks_by_terminal_status(
            &ns,
            vec![
                TerminalState::FailedNonretryable,
                TerminalState::FailedExhausted,
            ],
            10,
        )
        .await
        .expect("list_tasks_by_terminal_status");
    tx.rollback().await.expect("rollback");

    // Assert: exactly the two failure tasks, no COMPLETED.
    let observed: Vec<TaskId> = rows.iter().map(|t| t.task_id).collect();
    assert_eq!(observed.len(), 2);
    assert!(observed.contains(&nonretryable));
    assert!(observed.contains(&exhausted));
    assert!(!observed.contains(&completed));

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_replay_task_resets_failed_task_to_pending() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "replay").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("replay-ns");
    let task_type = TaskType::new("work");

    let task_id = seed_task(&storage, &ns, &task_type, "replay-key").await;
    drive_to_terminal(
        &storage,
        task_id,
        TaskOutcome::FailedNonretryable {
            error_class: "boom".to_owned(),
            error_message: "no".to_owned(),
            error_details: Bytes::from_static(b""),
            recorded_at: now_ms(),
        },
    )
    .await;

    // Act
    let mut tx = storage.begin().await.expect("begin replay");
    let outcome = tx.replay_task(task_id).await.expect("replay_task");
    tx.commit().await.expect("commit replay");

    // Assert
    assert_eq!(outcome, ReplayOutcome::Replayed);
    let mut tx = storage.begin().await.expect("begin read");
    let task = tx
        .get_task_by_id(task_id)
        .await
        .expect("get_task_by_id")
        .expect("task exists");
    tx.rollback().await.expect("rollback read");
    assert_eq!(task.status, TaskStatus::Pending);
    assert_eq!(task.attempt_number, 0);

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_replay_task_refuses_pending_task() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "replay_pending").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("replay-pending-ns");
    let task_type = TaskType::new("work");
    let task_id = seed_task(&storage, &ns, &task_type, "replay-pending-key").await;

    // Act
    let mut tx = storage.begin().await.expect("begin");
    let outcome = tx.replay_task(task_id).await.expect("replay_task");
    tx.rollback().await.expect("rollback");

    // Assert
    assert_eq!(outcome, ReplayOutcome::NotInTerminalFailureState);

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_replay_task_returns_not_found_for_unknown_task() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "replay_missing").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");

    // Act
    let mut tx = storage.begin().await.expect("begin");
    let outcome = tx
        .replay_task(TaskId::generate())
        .await
        .expect("replay_task");
    tx.rollback().await.expect("rollback");

    // Assert
    assert_eq!(outcome, ReplayOutcome::NotFound);

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_cancel_task_flips_pending_to_cancelled_and_drops_dedup() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "cancel").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("cancel-ns");
    let task_type = TaskType::new("work");
    let key = IdempotencyKey::new("cancel-key");

    let task_id = TaskId::generate();
    let now = now_ms();
    let task = sample_new_task(task_id, &ns, &task_type, now);
    let dedup = NewDedupRecord {
        namespace: ns.clone(),
        key: key.clone(),
        payload_hash: task.payload_hash,
        expires_at: task.expires_at,
    };
    let mut tx = storage.begin().await.expect("begin seed");
    tx.insert_task(task, dedup).await.expect("insert");
    tx.commit().await.expect("commit seed");

    // Act
    let mut tx = storage.begin().await.expect("begin cancel");
    let outcome = tx.cancel_task(task_id).await.expect("cancel_task");
    tx.commit().await.expect("commit cancel");

    // Assert: outcome reports the transition; dedup row gone; status flipped.
    assert_eq!(outcome, CancelOutcome::TransitionedToCancelled);
    let mut tx = storage.begin().await.expect("begin read");
    let dedup_row = tx
        .lookup_idempotency(&ns, &key)
        .await
        .expect("lookup_idempotency");
    assert!(dedup_row.is_none(), "dedup row should be gone");
    let task = tx
        .get_task_by_id(task_id)
        .await
        .expect("get_task_by_id")
        .expect("task exists");
    assert_eq!(task.status, TaskStatus::Cancelled);
    tx.rollback().await.expect("rollback read");

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_cancel_task_returns_not_found_for_unknown_task() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "cancel_missing").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");

    // Act
    let mut tx = storage.begin().await.expect("begin");
    let outcome = tx
        .cancel_task(TaskId::generate())
        .await
        .expect("cancel_task");
    tx.rollback().await.expect("rollback");

    // Assert
    assert_eq!(outcome, CancelOutcome::NotFound);

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_cancel_task_on_terminal_task_reports_already_terminal() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "cancel_terminal").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("cancel-term-ns");
    let task_type = TaskType::new("work");

    let task_id = seed_task(&storage, &ns, &task_type, "cancel-term-key").await;
    drive_to_terminal(
        &storage,
        task_id,
        TaskOutcome::Success {
            result_payload: Bytes::from_static(b"ok"),
            recorded_at: now_ms(),
        },
    )
    .await;

    // Act
    let mut tx = storage.begin().await.expect("begin cancel");
    let outcome = tx.cancel_task(task_id).await.expect("cancel_task");
    tx.commit().await.expect("commit cancel");

    // Assert
    assert_eq!(
        outcome,
        CancelOutcome::AlreadyTerminal {
            state: TaskStatus::Completed,
        }
    );

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_delete_idempotency_key_removes_matching_row() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "del_idemp").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("del-idemp-ns");
    let task_type = TaskType::new("work");
    let key = IdempotencyKey::new("del-idemp-key");

    let task_id = TaskId::generate();
    let now = now_ms();
    let task = sample_new_task(task_id, &ns, &task_type, now);
    let dedup = NewDedupRecord {
        namespace: ns.clone(),
        key: key.clone(),
        payload_hash: task.payload_hash,
        expires_at: task.expires_at,
    };
    let mut tx = storage.begin().await.expect("begin seed");
    tx.insert_task(task, dedup).await.expect("insert");
    tx.commit().await.expect("commit seed");

    // Act
    let mut tx = storage.begin().await.expect("begin delete");
    let deleted = tx
        .delete_idempotency_key(&ns, &key)
        .await
        .expect("delete_idempotency_key");
    tx.commit().await.expect("commit delete");

    // Assert
    assert_eq!(deleted, 1);
    let mut tx = storage.begin().await.expect("begin lookup");
    let observed = tx
        .lookup_idempotency(&ns, &key)
        .await
        .expect("lookup_idempotency");
    tx.rollback().await.expect("rollback lookup");
    assert!(observed.is_none(), "dedup row should be deleted");

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_count_tasks_by_status_aggregates_per_status() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "count").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("count-ns");
    let task_type = TaskType::new("work");

    // 2 PENDING, 1 COMPLETED, 1 FAILED_NONRETRYABLE.
    let _pending_a = seed_task(&storage, &ns, &task_type, "count-p-a").await;
    let _pending_b = seed_task(&storage, &ns, &task_type, "count-p-b").await;

    let completed = seed_task(&storage, &ns, &task_type, "count-c").await;
    drive_to_terminal(
        &storage,
        completed,
        TaskOutcome::Success {
            result_payload: Bytes::from_static(b"ok"),
            recorded_at: now_ms(),
        },
    )
    .await;

    let failed = seed_task(&storage, &ns, &task_type, "count-f").await;
    drive_to_terminal(
        &storage,
        failed,
        TaskOutcome::FailedNonretryable {
            error_class: "boom".to_owned(),
            error_message: "no".to_owned(),
            error_details: Bytes::from_static(b""),
            recorded_at: now_ms(),
        },
    )
    .await;

    // Act
    let mut tx = storage.begin().await.expect("begin");
    let counts = tx
        .count_tasks_by_status(&ns)
        .await
        .expect("count_tasks_by_status");
    tx.rollback().await.expect("rollback");

    // Assert
    assert_eq!(counts.get(&TaskStatus::Pending).copied(), Some(2));
    assert_eq!(counts.get(&TaskStatus::Completed).copied(), Some(1));
    assert_eq!(
        counts.get(&TaskStatus::FailedNonretryable).copied(),
        Some(1)
    );

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_list_workers_returns_alive_workers() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "workers").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("workers-ns");
    let worker_a = WorkerId::generate();
    let worker_b = WorkerId::generate();
    let now = now_ms();

    let mut tx = storage.begin().await.expect("begin");
    tx.record_worker_heartbeat(&worker_a, &ns, now)
        .await
        .expect("heartbeat a");
    tx.record_worker_heartbeat(&worker_b, &ns, now)
        .await
        .expect("heartbeat b");
    tx.commit().await.expect("commit");

    // Act
    let mut tx = storage.begin().await.expect("begin list");
    let workers = tx.list_workers(&ns, false).await.expect("list_workers");
    tx.rollback().await.expect("rollback list");

    // Assert
    let ids: Vec<WorkerId> = workers.iter().map(|w| w.worker_id).collect();
    assert_eq!(workers.len(), 2);
    assert!(ids.contains(&worker_a));
    assert!(ids.contains(&worker_b));
    for w in &workers {
        assert!(
            w.declared_dead_at.is_none(),
            "alive worker has no death stamp"
        );
        assert_eq!(w.namespace.as_str(), "workers-ns");
        assert_eq!(w.inflight_count, 0);
    }

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_list_workers_excludes_dead_when_include_dead_false() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "workers_dead").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("workers-dead-ns");
    let worker_alive = WorkerId::generate();
    let worker_dead = WorkerId::generate();
    let now = now_ms();

    let mut tx = storage.begin().await.expect("begin");
    tx.record_worker_heartbeat(&worker_alive, &ns, now)
        .await
        .expect("heartbeat alive");
    tx.record_worker_heartbeat(&worker_dead, &ns, now)
        .await
        .expect("heartbeat dead");
    tx.mark_worker_dead(&worker_dead, now)
        .await
        .expect("mark_worker_dead");
    tx.commit().await.expect("commit");

    // Act
    let mut tx = storage.begin().await.expect("begin list");
    let alive_only = tx
        .list_workers(&ns, false)
        .await
        .expect("list_workers alive");
    tx.rollback().await.expect("rollback list");

    // Assert
    assert_eq!(alive_only.len(), 1);
    assert_eq!(alive_only[0].worker_id, worker_alive);

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_delete_audit_logs_before_removes_matching_rows() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "audit_del").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("audit-del-ns");
    let now = now_ms();
    let old_ts = Timestamp::from_unix_millis(now.as_unix_millis() - 60_000);
    let new_ts = Timestamp::from_unix_millis(now.as_unix_millis() + 60_000);

    let mk_entry = |ts: Timestamp, rpc: &str| AuditEntry {
        timestamp: ts,
        actor: "anonymous".to_owned(),
        rpc: rpc.to_owned(),
        namespace: Some(ns.clone()),
        request_summary: Bytes::from_static(b"{}"),
        result: "success".to_owned(),
        request_hash: [0xAB; 32],
    };

    let mut tx = storage.begin().await.expect("begin seed");
    tx.audit_log_append(mk_entry(old_ts, "OldRpc"))
        .await
        .expect("append old");
    tx.audit_log_append(mk_entry(new_ts, "NewRpc"))
        .await
        .expect("append new");
    tx.commit().await.expect("commit seed");

    // Act: cutoff = `now`; the old row is before it, the new one is after.
    let mut tx = storage.begin().await.expect("begin delete");
    let deleted = tx
        .delete_audit_logs_before(&ns, now, 100)
        .await
        .expect("delete_audit_logs_before");
    tx.commit().await.expect("commit delete");

    // Assert
    assert_eq!(deleted, 1);
    let conn = storage.pool().get().await.expect("pool checkout");
    let rows = conn
        .query(
            "SELECT rpc FROM audit_log WHERE namespace = $1 ORDER BY rpc",
            &[&ns.as_str()],
        )
        .await
        .expect("query audit");
    let observed: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(observed, vec!["NewRpc".to_owned()]);

    drop(conn);
    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_get_namespace_quota_round_trips_upserted_values() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "quota_read").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");
    let ns = Namespace::new("quota-read-ns");
    let upsert = sample_quota_upsert();

    let mut tx = storage.begin().await.expect("begin upsert");
    tx.upsert_namespace_quota(&ns, upsert.clone())
        .await
        .expect("upsert");
    tx.commit().await.expect("commit upsert");

    // Act
    let mut tx = storage.begin().await.expect("begin read");
    let quota = tx
        .get_namespace_quota(&ns)
        .await
        .expect("get_namespace_quota");
    tx.rollback().await.expect("rollback read");

    // Assert: writable subset matches what we upserted.
    assert_eq!(quota.namespace.as_str(), "quota-read-ns");
    assert_eq!(quota.max_pending, upsert.max_pending);
    assert_eq!(quota.max_inflight, upsert.max_inflight);
    assert_eq!(quota.max_workers, upsert.max_workers);
    assert_eq!(quota.max_submit_rpm, upsert.max_submit_rpm);
    assert_eq!(quota.max_dispatch_rpm, upsert.max_dispatch_rpm);
    assert_eq!(quota.max_replay_per_second, upsert.max_replay_per_second);
    assert_eq!(quota.max_retries_ceiling, upsert.max_retries_ceiling);
    assert_eq!(
        quota.max_idempotency_ttl_seconds,
        upsert.max_idempotency_ttl_seconds
    );
    assert_eq!(quota.max_payload_bytes, upsert.max_payload_bytes);
    assert_eq!(quota.max_details_bytes, upsert.max_details_bytes);
    assert_eq!(quota.max_error_classes, upsert.max_error_classes);
    assert_eq!(quota.max_task_types, upsert.max_task_types);
    assert_eq!(quota.log_level_override, upsert.log_level_override);
    assert_eq!(
        quota.audit_log_retention_days,
        upsert.audit_log_retention_days
    );
    assert_eq!(quota.metrics_export_enabled, upsert.metrics_export_enabled);

    drop_schema(&admin, &schema).await;
}

#[tokio::test]
#[ignore = "requires reachable Postgres at docker.yuacx.com:5432 (set TASKQ_PG_TEST_URL to override)"]
async fn admin_get_task_by_id_returns_none_for_unknown_task() {
    // Arrange
    let Some(admin) = try_connect().await else {
        return;
    };
    let schema = isolate_schema(&admin, "get_missing").await;
    let storage = connect_in_schema(&schema).await;
    migrate(storage.pool()).await.expect("run migrations");

    // Act
    let mut tx = storage.begin().await.expect("begin");
    let observed = tx
        .get_task_by_id(TaskId::generate())
        .await
        .expect("get_task_by_id");
    tx.rollback().await.expect("rollback");

    // Assert
    assert!(observed.is_none());

    drop_schema(&admin, &schema).await;
}
