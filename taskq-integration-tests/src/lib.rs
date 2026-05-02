//! End-to-end test harness for `taskq-rs`.
//!
//! Exposes [`TestHarness`], a self-contained CP + sidecar SQLite connection
//! that integration tests use to spin up a real gRPC server against the
//! caller and worker SDKs. Each `TestHarness::start_in_memory` returns:
//!
//! - `state` -- the [`CpState`] the CP handlers see (so tests can inject
//!   strategies / inspect waiter pools).
//! - `server_addr` -- the OS-assigned `127.0.0.1:<random>` socket the
//!   in-process server is listening on.
//! - `server_handle` -- the spawned task driving the server's accept loop.
//! - `shutdown` -- the `ShutdownSender` the harness flips to drain reapers,
//!   metrics refresher, audit pruner, and the gRPC server.
//! - `_temp` -- the on-disk SQLite directory; dropped on shutdown so the
//!   sidecar reader connection can stay valid for the lifetime of the test.
//!
//! The on-disk path is preferred over `:memory:` because the audit-log read
//! helper opens a sidecar `rusqlite::Connection` against the same file in
//! WAL mode -- SQLite supports any number of read connections concurrently
//! with the single writer the storage crate already owns.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use grpc_client::Channel;
use rusqlite::Connection;
use taskq_caller_sdk::CallerClient;
use taskq_cp::config::{CpConfig, OtelExporterConfig, StorageBackendConfig};
use taskq_cp::observability::MetricsHandle;
use taskq_cp::shutdown::{channel as shutdown_channel, ShutdownSender};
use taskq_cp::state::{CpState, DynStorage};
use taskq_cp::strategy::StrategyRegistry;
use taskq_cp::{audit_pruner, metrics_refresher, reapers, server};
use taskq_proto::task_admin_client::TaskAdminClient;
use taskq_proto::task_queue_client::TaskQueueClient;
use taskq_storage_sqlite::SqliteStorage;
use taskq_worker_sdk::{
    AcquiredTask, BuildError, HandlerOutcome, ShutdownSignal, TaskHandler, Worker, WorkerBuilder,
};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Default lease window applied to the harness CP configuration. Short on
/// purpose: tests that exercise reaper-A want the lease to elapse in a few
/// hundred milliseconds, not the production-default 30 seconds.
pub const HARNESS_LEASE_WINDOW_SECONDS: u32 = 1;

/// Default belt-and-suspenders cadence in the harness. Smaller than
/// production so long-poll waiters re-query within the test budget.
pub const HARNESS_BELT_AND_SUSPENDERS_SECONDS: u32 = 1;

/// Default long-poll timeout the harness writes into `CpConfig`. Smaller
/// than production so workers can shut down quickly.
pub const HARNESS_LONG_POLL_TIMEOUT_SECONDS: u32 = 5;

/// In-process CP harness owning the storage handle, the server, the
/// reapers, and a sidecar SQLite read connection.
pub struct TestHarness {
    /// Shared CP state (cloneable into per-test handlers).
    pub state: Arc<CpState>,
    /// Loopback address the spawned server is listening on.
    pub server_addr: SocketAddr,
    /// JoinHandle for the gRPC server task.
    pub server_handle: JoinHandle<()>,
    /// JoinHandle for Reaper A.
    pub reaper_a_handle: JoinHandle<()>,
    /// JoinHandle for Reaper B.
    pub reaper_b_handle: JoinHandle<()>,
    /// JoinHandle for the metrics refresher.
    pub metrics_handle: JoinHandle<()>,
    /// JoinHandle for the audit pruner.
    pub pruner_handle: JoinHandle<()>,
    /// Watch sender used to fan-out shutdown to every long-running task.
    pub shutdown: ShutdownSender,
    /// Path to the on-disk SQLite file (kept alive by `_temp`).
    pub db_path: PathBuf,
    /// Owns the temp directory that hosts the SQLite file. Dropping the
    /// harness drops this last so the sidecar reader stays valid.
    _temp: TempDir,
}

impl TestHarness {
    /// Build a fresh harness backed by an on-disk SQLite database under a
    /// temporary directory. The CP listens on a random localhost port; the
    /// returned harness exposes that address.
    pub async fn start_in_memory() -> Self {
        // 1. Temp directory + on-disk SQLite path. We avoid `:memory:` so a
        //    sidecar reader can attach to the same file for `audit_log`
        //    introspection. SQLite WAL allows concurrent readers alongside
        //    the storage crate's single writer.
        let temp = tempfile::tempdir().expect("tempdir must succeed");
        let db_path = temp.path().join("taskq-test.sqlite");
        let path_str = db_path
            .to_str()
            .expect("temp path must be utf-8")
            .to_owned();

        let storage = SqliteStorage::open(&db_path)
            .await
            .expect("SqliteStorage::open must succeed");
        let storage_arc: Arc<dyn DynStorage> = Arc::new(storage);

        // 2. CP config -- aggressive timings so tests do not wait on
        //    production-default cadences.
        let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("static bind_addr must parse");
        let health_addr: SocketAddr = "127.0.0.1:0"
            .parse()
            .expect("static health_addr must parse");
        let config = Arc::new(CpConfig {
            bind_addr,
            health_addr,
            storage_backend: StorageBackendConfig::Sqlite { path: path_str },
            otel_exporter: OtelExporterConfig::Disabled,
            quota_cache_ttl_seconds: 1,
            long_poll_default_timeout_seconds: HARNESS_LONG_POLL_TIMEOUT_SECONDS,
            long_poll_max_seconds: HARNESS_LONG_POLL_TIMEOUT_SECONDS * 2,
            belt_and_suspenders_seconds: HARNESS_BELT_AND_SUSPENDERS_SECONDS,
            waiter_limit_per_replica: 100,
            lease_window_seconds: HARNESS_LEASE_WINDOW_SECONDS,
        });

        let (shutdown_tx, shutdown_rx) = shutdown_channel();

        let state = Arc::new(CpState::new(
            Arc::clone(&storage_arc),
            Arc::new(StrategyRegistry::empty()),
            MetricsHandle::noop(),
            shutdown_rx.clone(),
            Arc::clone(&config),
        ));

        // 3. Spawn the in-process gRPC server.
        let (server_addr, server_handle) =
            server::serve_in_process(Arc::clone(&state), shutdown_rx.clone())
                .await
                .expect("serve_in_process must succeed");

        // 4. Spawn reapers + metrics refresher + audit pruner so the
        //    background plumbing matches a production CP.
        let (reaper_a_handle, reaper_b_handle) =
            reapers::spawn_reapers(Arc::clone(&state), shutdown_rx.clone());
        let metrics_handle = metrics_refresher::spawn(Arc::clone(&state), shutdown_rx.clone());
        let pruner_handle = audit_pruner::spawn(Arc::clone(&state), shutdown_rx.clone());

        // 5. Wait until the server is reachable. Connection-refused races
        //    on Linux are short, but explicit polling makes test setup
        //    deterministic.
        wait_for_server(server_addr).await;

        Self {
            state,
            server_addr,
            server_handle,
            reaper_a_handle,
            reaper_b_handle,
            metrics_handle,
            pruner_handle,
            shutdown: shutdown_tx,
            db_path,
            _temp: temp,
        }
    }

    /// Build a [`CallerClient`] connected to the harness's gRPC port with a
    /// short polling interval so `submit_and_wait` settles quickly.
    pub async fn caller(&self) -> CallerClient {
        let uri: http::Uri = format!("http://{}", self.server_addr)
            .parse()
            .expect("loopback URI must parse");
        let client = TaskQueueClient::<Channel>::connect(uri)
            .await
            .expect("TaskQueueClient must connect");
        CallerClient::from_grpc_client(client).with_poll_interval(Duration::from_millis(20))
    }

    /// Open a `TaskAdminClient` for end-to-end admin RPCs.
    pub async fn admin_client(&self) -> TaskAdminClient<Channel> {
        let uri: http::Uri = format!("http://{}", self.server_addr)
            .parse()
            .expect("loopback URI must parse");
        TaskAdminClient::<Channel>::connect(uri)
            .await
            .expect("TaskAdminClient must connect")
    }

    /// Build a worker for `namespace` registered for `task_types`, with the
    /// given handler closure. Returns a ready-to-run [`Worker`] that the
    /// caller can pair with `run_with_signal`.
    pub async fn worker_for<F, Fut>(
        &self,
        namespace: &str,
        task_types: Vec<&str>,
        handler: F,
    ) -> Result<Worker, BuildError>
    where
        F: Fn(AcquiredTask) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandlerOutcome> + Send + 'static,
    {
        let uri: http::Uri = format!("http://{}", self.server_addr)
            .parse()
            .expect("loopback URI must parse");
        let task_types_owned: Vec<String> = task_types.into_iter().map(|s| s.to_owned()).collect();
        WorkerBuilder::new(uri, namespace.to_owned())
            .with_task_types(task_types_owned)
            .with_long_poll_timeout(Duration::from_secs(u64::from(
                HARNESS_LONG_POLL_TIMEOUT_SECONDS,
            )))
            .with_drain_timeout(Duration::from_secs(2))
            .with_handler(FnHandler::new(handler))
            .build()
            .await
    }

    /// Cloneable accessor for the inner `CpState`. Tests use this when they
    /// need to install a strategy, drive a reaper tick, or sample the
    /// waiter pool directly.
    pub fn cp_state(&self) -> Arc<CpState> {
        Arc::clone(&self.state)
    }

    /// Open a fresh sidecar `rusqlite::Connection` against the same SQLite
    /// file the storage layer is writing to. Reads in WAL mode succeed
    /// concurrently with the writer.
    pub fn open_sidecar_connection(&self) -> Connection {
        let conn = Connection::open(&self.db_path).expect("sidecar SQLite open");
        conn.pragma_update(None, "journal_mode", "WAL")
            .expect("WAL pragma");
        conn.pragma_update(None, "busy_timeout", 5000)
            .expect("busy_timeout pragma");
        conn
    }

    /// Count the number of rows currently in `audit_log`. Tests assert on
    /// the delta around an admin RPC.
    pub fn audit_log_count(&self) -> u64 {
        let conn = self.open_sidecar_connection();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM audit_log", [], |row| row.get(0))
            .expect("audit_log count");
        u64::try_from(n).unwrap_or(0)
    }

    /// Return all audit-log entries the database currently holds. Used by
    /// the observability tests to assert on `rpc` / `result` /
    /// `request_summary` after an admin call.
    pub fn audit_log_entries(&self) -> Vec<AuditLogRow> {
        let conn = self.open_sidecar_connection();
        let mut stmt = conn
            .prepare(
                "SELECT timestamp, actor, rpc, namespace, request_summary, result \
                   FROM audit_log ORDER BY id ASC",
            )
            .expect("audit_log prepare");
        let rows = stmt
            .query_map([], |row| {
                Ok(AuditLogRow {
                    timestamp_ms: row.get::<_, i64>(0)?,
                    actor: row.get::<_, String>(1)?,
                    rpc: row.get::<_, String>(2)?,
                    namespace: row.get::<_, Option<String>>(3)?,
                    request_summary: row.get::<_, String>(4)?,
                    result: row.get::<_, String>(5)?,
                })
            })
            .expect("audit_log map");
        rows.filter_map(Result::ok).collect()
    }

    /// Read a task row's `traceparent` directly from SQLite. Used by the
    /// observability test that asserts the wire `traceparent` was
    /// persisted.
    pub fn read_task_traceparent(&self, task_id: &str) -> Vec<u8> {
        let conn = self.open_sidecar_connection();
        conn.query_row(
            "SELECT traceparent FROM tasks WHERE task_id = ?1",
            [task_id],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .expect("traceparent read")
    }

    /// Read a single task row's status string for assertion in tests.
    pub fn read_task_status(&self, task_id: &str) -> Option<String> {
        let conn = self.open_sidecar_connection();
        conn.query_row(
            "SELECT status FROM tasks WHERE task_id = ?1",
            [task_id],
            |row| row.get::<_, String>(0),
        )
        .ok()
    }

    /// Read the `attempt_number` column for `task_id`.
    pub fn read_task_attempt(&self, task_id: &str) -> Option<i64> {
        let conn = self.open_sidecar_connection();
        conn.query_row(
            "SELECT attempt_number FROM tasks WHERE task_id = ?1",
            [task_id],
            |row| row.get::<_, i64>(0),
        )
        .ok()
    }

    /// Read the column `declared_dead_at` for the worker. Used by the
    /// reaper tests to assert that Reaper B stamped a tombstone.
    pub fn read_worker_declared_dead_at(&self, worker_id: &str) -> Option<i64> {
        let conn = self.open_sidecar_connection();
        conn.query_row(
            "SELECT declared_dead_at FROM worker_heartbeats WHERE worker_id = ?1",
            [worker_id],
            |row| row.get::<_, Option<i64>>(0),
        )
        .ok()
        .flatten()
    }

    /// Best-effort namespace seed -- writes the `system_default` quota into
    /// the named namespace so admin paths that read the quota find an
    /// explicit row instead of falling through to the system default.
    pub async fn seed_namespace(&self, namespace: &str) {
        use taskq_storage::{Namespace, NamespaceQuotaUpsert};
        let mut tx = self
            .state
            .storage
            .begin_dyn()
            .await
            .expect("begin_dyn must succeed");
        // Keep the cardinality budgets nonzero and the heartbeat invariants
        // satisfied so set_namespace_config calls can actually exercise
        // additions without tripping the validator.
        let upsert = NamespaceQuotaUpsert {
            max_pending: None,
            max_inflight: None,
            max_workers: None,
            max_waiters_per_replica: Some(100),
            max_submit_rpm: None,
            max_dispatch_rpm: None,
            max_replay_per_second: None,
            max_retries_ceiling: 10,
            max_idempotency_ttl_seconds: 86_400,
            max_payload_bytes: 10 * 1024 * 1024,
            max_details_bytes: 65_536,
            min_heartbeat_interval_seconds: 1,
            lazy_extension_threshold_seconds: 2,
            max_error_classes: 64,
            max_task_types: 32,
            trace_sampling_ratio: 1.0,
            log_level_override: None,
            audit_log_retention_days: 90,
            metrics_export_enabled: true,
        };
        tx.upsert_namespace_quota(&Namespace::new(namespace), upsert)
            .await
            .expect("upsert_namespace_quota");
        tx.commit_dyn().await.expect("commit");
    }

    /// Drive a single Reaper A tick synchronously. Tests prefer this to
    /// waiting on the 5s polling cadence of the spawned reaper task.
    pub async fn tick_reaper_a(&self) {
        reapers::reaper_a_tick(Arc::clone(&self.state))
            .await
            .expect("reaper_a_tick must succeed");
    }

    /// Drive a single Reaper B tick synchronously.
    pub async fn tick_reaper_b(&self) {
        reapers::reaper_b_tick(Arc::clone(&self.state))
            .await
            .expect("reaper_b_tick must succeed");
    }

    /// Cooperative shutdown -- broadcasts to every long-running task and
    /// joins the handles. Tests should call this at the end of their
    /// `Act`/`Assert` block so the runtime tears down cleanly.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(5), self.server_handle).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), self.reaper_a_handle).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), self.reaper_b_handle).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), self.metrics_handle).await;
        let _ = tokio::time::timeout(Duration::from_secs(5), self.pruner_handle).await;
    }
}

/// Wait until the server's loopback port is accepting TCP connections.
async fn wait_for_server(addr: SocketAddr) {
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("in-process gRPC server did not become ready within 2s");
}

/// One row pulled from `audit_log`.
#[derive(Debug, Clone)]
pub struct AuditLogRow {
    pub timestamp_ms: i64,
    pub actor: String,
    pub rpc: String,
    pub namespace: Option<String>,
    pub request_summary: String,
    pub result: String,
}

// ---------------------------------------------------------------------------
// FnHandler -- closure adapter for `TaskHandler`.
// ---------------------------------------------------------------------------

/// Adapter that wraps any `Fn(AcquiredTask) -> impl Future<Output =
/// HandlerOutcome>` in the [`TaskHandler`] trait. Tests prefer a closure
/// over a one-off struct; this stays in the harness rather than the SDK so
/// the SDK's public surface is not bloated with test-only helpers.
pub struct FnHandler<F> {
    inner: F,
}

impl<F> FnHandler<F> {
    /// Wrap a closure as a `TaskHandler`.
    pub fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<F, Fut> TaskHandler for FnHandler<F>
where
    F: Fn(AcquiredTask) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = HandlerOutcome> + Send + 'static,
{
    fn handle(&self, task: AcquiredTask) -> impl Future<Output = HandlerOutcome> + Send {
        (self.inner)(task)
    }
}

// ---------------------------------------------------------------------------
// Public re-exports for tests.
// ---------------------------------------------------------------------------

/// Spawn the worker on the test runtime with a oneshot-driven shutdown.
/// Returns the receiver-side `oneshot::Sender` plus the worker's
/// `JoinHandle`. Tests call `tx.send(()).ok()` to start the drain sequence.
pub fn spawn_worker(worker: Worker) -> (oneshot::Sender<()>, JoinHandle<()>) {
    let (tx, rx) = oneshot::channel::<()>();
    let signal = ShutdownSignal::new(async move {
        let _ = rx.await;
    });
    let handle = tokio::spawn(async move {
        if let Err(err) = worker.run_with_signal(signal).await {
            tracing::warn!(error = %err, "worker exited with error");
        }
    });
    (tx, handle)
}

/// Helper -- count how many `audit_log` rows match `(rpc, namespace)`.
pub fn audit_count_for(entries: &[AuditLogRow], rpc: &str, namespace: &str) -> usize {
    entries
        .iter()
        .filter(|e| e.rpc == rpc && e.namespace.as_deref() == Some(namespace))
        .count()
}

/// Helper -- group entries by `rpc` so tests can assert "saw exactly one
/// SetNamespaceQuota row" without scanning the slice repeatedly.
pub fn audit_count_by_rpc(entries: &[AuditLogRow]) -> HashMap<String, usize> {
    let mut out = HashMap::new();
    for entry in entries {
        *out.entry(entry.rpc.clone()).or_insert(0) += 1;
    }
    out
}
