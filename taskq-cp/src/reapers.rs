//! Background reapers — Phase 5c.
//!
//! Two long-running tasks spawned from `main.rs::run_serve` after the gRPC
//! server starts and before the shutdown wait:
//!
//! - **Reaper A — per-task timeout** (`design.md` §6.6): every 5s, list
//!   every `task_runtime` row whose `timeout_at` has elapsed and reclaim
//!   it (increment attempt_number, flip to PENDING, delete the runtime
//!   row). Operates inside a SERIALIZABLE transaction with transparent
//!   `40001` retry; on commit it pings `WaiterPool::wake_one(ns, [type])`
//!   so a queued waiter has a fair shot at the now-PENDING task.
//!
//! - **Reaper B — dead worker** (`design.md` §6.6): every 10s, list every
//!   runtime row whose worker's `worker_heartbeats.last_heartbeat_at` is
//!   older than the configured lease window AND whose `declared_dead_at
//!   IS NULL`. For each, reclaim the runtime row AND stamp
//!   `declared_dead_at = NOW()` on the worker — same SERIALIZABLE
//!   transaction so the worker's next heartbeat sees the flip and the SDK
//!   re-`Register`s.
//!
//! Both reapers honor shutdown via a watch::Receiver: once the receiver
//! flips to `true` the reaper finishes its current tick and exits.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::KeyValue;
use taskq_storage::{
    DeadWorkerRuntime, ExpiredRuntime, Namespace, RuntimeRef, StorageError, TaskType, Timestamp,
};

use crate::handlers::retry::with_serializable_retry;
use crate::shutdown::ShutdownReceiver;
use crate::state::CpState;

/// Polling cadence for Reaper A. `design.md` §6.6 settles on 5s — below
/// that and the Postgres scan's lock contention starts to dominate; above
/// that and the worker-observable LEASE_EXPIRED latency grows beyond the
/// expected 10s upper bound (`problems/01` round-2).
pub const REAPER_A_TICK: Duration = Duration::from_secs(5);

/// Polling cadence for Reaper B. Dead-worker reclaim is rarer than
/// per-task timeout; 10s amortizes the BRIN scan cost.
pub const REAPER_B_TICK: Duration = Duration::from_secs(10);

/// Default batch size per reaper transaction. Mirrors `design.md` §6.6.
pub const REAPER_BATCH_SIZE: usize = 1000;

/// Spawn both reapers and return the join handles. `main` owns them and
/// awaits them after the gRPC server returns so the reapers cleanly
/// finish their last in-flight tick before the runtime shuts down.
pub fn spawn_reapers(
    state: Arc<CpState>,
    shutdown: ShutdownReceiver,
) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
    let state_a = Arc::clone(&state);
    let shutdown_a = shutdown.clone();
    let handle_a = tokio::spawn(async move {
        run_reaper_a(state_a, shutdown_a).await;
    });

    let state_b = Arc::clone(&state);
    let shutdown_b = shutdown;
    let handle_b = tokio::spawn(async move {
        run_reaper_b(state_b, shutdown_b).await;
    });

    (handle_a, handle_b)
}

/// Reaper A loop. Runs ticks at `REAPER_A_TICK` until shutdown fires.
async fn run_reaper_a(state: Arc<CpState>, mut shutdown: ShutdownReceiver) {
    tracing::info!("reaper-a: per-task timeout reaper started");
    let mut ticker = tokio::time::interval(REAPER_A_TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(err) = reaper_a_tick(Arc::clone(&state)).await {
                    tracing::warn!(error = %err, "reaper-a tick failed");
                }
            }
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    tracing::info!("reaper-a: shutdown signal observed; exiting");
                    return;
                }
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

/// Reaper B loop. Same shape as Reaper A with the dead-worker scan.
async fn run_reaper_b(state: Arc<CpState>, mut shutdown: ShutdownReceiver) {
    tracing::info!("reaper-b: dead-worker reaper started");
    let mut ticker = tokio::time::interval(REAPER_B_TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(err) = reaper_b_tick(Arc::clone(&state)).await {
                    tracing::warn!(error = %err, "reaper-b tick failed");
                }
            }
            changed = shutdown.changed() => {
                if changed.is_ok() && *shutdown.borrow() {
                    tracing::info!("reaper-b: shutdown signal observed; exiting");
                    return;
                }
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

/// Run one Reaper A tick. Reclaim every expired runtime row in a single
/// SERIALIZABLE transaction (with 40001 retry) and wake one waiter per
/// reclaimed task on commit.
async fn reaper_a_tick(state: Arc<CpState>) -> Result<(), StorageError> {
    let now = current_timestamp();
    let metrics = state.metrics.clone();

    let reclaimed_namespaces: Vec<Namespace> =
        with_serializable_retry(&metrics, "reaper_a", || {
            let state = Arc::clone(&state);
            async move {
                let mut tx = state.storage.begin_dyn().await?;
                let expired = tx.list_expired_runtimes(now, REAPER_BATCH_SIZE).await?;
                let mut reclaimed = Vec::with_capacity(expired.len());
                for row in expired {
                    reclaim_one_a(&mut *tx, &row).await?;
                    reclaimed.push(row.namespace);
                }
                tx.commit_dyn().await?;
                Ok(reclaimed)
            }
        })
        .await?;

    // Post-commit: emit metric + wake waiters per reclaimed task. The wake
    // pings WaiterPool::wake_one with an empty type filter so any worker
    // registered on the namespace gets a chance.
    for namespace in &reclaimed_namespaces {
        state.metrics.lease_expired_total.add(
            1,
            &[KeyValue::new("namespace", namespace.as_str().to_owned())],
        );
        state
            .waiter_pool
            .wake_one(namespace, &[] as &[TaskType])
            .await;
    }

    if !reclaimed_namespaces.is_empty() {
        tracing::info!(
            count = reclaimed_namespaces.len(),
            "reaper-a: reclaimed leases"
        );
    }
    Ok(())
}

/// Reclaim one expired runtime row inside `tx`.
async fn reclaim_one_a(
    tx: &mut dyn crate::state::StorageTxDyn,
    row: &ExpiredRuntime,
) -> Result<(), StorageError> {
    let runtime_ref = RuntimeRef {
        task_id: row.task_id,
        attempt_number: row.attempt_number,
        worker_id: row.worker_id,
    };
    tx.reclaim_runtime(&runtime_ref).await
}

/// Run one Reaper B tick. The lease window is configurable per
/// `CpConfig::lease_window_seconds`; rows whose worker's last heartbeat
/// predates `now - lease_window` are reclaimed and the worker is stamped
/// `declared_dead_at = NOW()`.
async fn reaper_b_tick(state: Arc<CpState>) -> Result<(), StorageError> {
    let now = current_timestamp();
    let lease_window = Duration::from_secs(u64::from(state.config.lease_window_seconds));
    let stale_before = Timestamp::from_unix_millis(
        now.as_unix_millis()
            .saturating_sub(lease_window.as_millis() as i64),
    );
    let metrics = state.metrics.clone();

    let reclaimed: Vec<Namespace> = with_serializable_retry(&metrics, "reaper_b", || {
        let state = Arc::clone(&state);
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            let dead = tx
                .list_dead_worker_runtimes(stale_before, REAPER_BATCH_SIZE)
                .await?;
            let mut reclaimed = Vec::with_capacity(dead.len());
            for row in dead {
                reclaim_one_b(&mut *tx, &row, now).await?;
                reclaimed.push(row.namespace);
            }
            tx.commit_dyn().await?;
            Ok(reclaimed)
        }
    })
    .await?;

    for namespace in &reclaimed {
        state.metrics.lease_expired_total.add(
            1,
            &[KeyValue::new("namespace", namespace.as_str().to_owned())],
        );
        state
            .waiter_pool
            .wake_one(namespace, &[] as &[TaskType])
            .await;
    }

    if !reclaimed.is_empty() {
        tracing::info!(
            count = reclaimed.len(),
            "reaper-b: reclaimed dead-worker leases"
        );
    }
    Ok(())
}

/// Reclaim one dead-worker runtime row inside `tx`. Same as Reaper A's
/// `reclaim_runtime` but additionally stamps `declared_dead_at` on the
/// worker's heartbeat row in the same SERIALIZABLE transaction so the
/// worker's next heartbeat returns `WORKER_DEREGISTERED`.
async fn reclaim_one_b(
    tx: &mut dyn crate::state::StorageTxDyn,
    row: &DeadWorkerRuntime,
    now: Timestamp,
) -> Result<(), StorageError> {
    let runtime_ref = RuntimeRef {
        task_id: row.task_id,
        attempt_number: row.attempt_number,
        worker_id: row.worker_id,
    };
    tx.reclaim_runtime(&runtime_ref).await?;
    tx.mark_worker_dead(&row.worker_id, now).await?;
    Ok(())
}

fn current_timestamp() -> Timestamp {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    Timestamp::from_unix_millis(ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

    use bytes::Bytes;
    use taskq_storage::{
        IdempotencyKey, NewDedupRecord, NewLease, NewTask, TaskId, TaskType, WorkerId,
    };

    use crate::config::{CpConfig, OtelExporterConfig, StorageBackendConfig};
    use crate::observability::MetricsHandle;
    use crate::shutdown::channel;
    use crate::state::DynStorage;
    use crate::strategy::StrategyRegistry;

    /// Build an in-memory SQLite-backed CP state for hermetic reaper tests.
    async fn build_state() -> Arc<CpState> {
        let storage = taskq_storage_sqlite::SqliteStorage::open_in_memory()
            .await
            .expect("SqliteStorage::open_in_memory");
        let storage_arc: Arc<dyn DynStorage> = Arc::new(storage);
        let config = Arc::new(CpConfig {
            bind_addr: "127.0.0.1:50051".parse::<SocketAddr>().unwrap(),
            health_addr: "127.0.0.1:9090".parse::<SocketAddr>().unwrap(),
            storage_backend: StorageBackendConfig::Sqlite {
                path: ":memory:".to_owned(),
            },
            otel_exporter: OtelExporterConfig::Disabled,
            quota_cache_ttl_seconds: 5,
            long_poll_default_timeout_seconds: 30,
            long_poll_max_seconds: 60,
            belt_and_suspenders_seconds: 10,
            waiter_limit_per_replica: 100,
            lease_window_seconds: 30,
        });
        let (_tx, rx) = channel();
        Arc::new(CpState::new(
            storage_arc,
            Arc::new(StrategyRegistry::empty()),
            MetricsHandle::noop(),
            rx,
            config,
        ))
    }

    /// Insert a task in PENDING then DISPATCHED with a runtime row whose
    /// timeout has already elapsed. Returns the task_id and worker_id.
    async fn seed_expired_runtime(
        state: &CpState,
        ns: &str,
        task_type: &str,
    ) -> (TaskId, WorkerId) {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let now = current_timestamp();
        let in_past = Timestamp::from_unix_millis(now.as_unix_millis() - 60_000);
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let task = NewTask {
            task_id,
            namespace: Namespace::new(ns),
            task_type: TaskType::new(task_type),
            priority: 0,
            payload: Bytes::from_static(b"{}"),
            payload_hash: [0u8; 32],
            submitted_at: now,
            expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
            max_retries: 3,
            retry_initial_ms: 1_000,
            retry_max_ms: 10_000,
            retry_coefficient: 2.0,
            traceparent: Bytes::new(),
            tracestate: Bytes::new(),
            format_version: 1,
        };
        let dedup = NewDedupRecord {
            namespace: Namespace::new(ns),
            key: IdempotencyKey::new(format!("seed-{task_id}")),
            payload_hash: [0u8; 32],
            expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
        };
        tx.insert_task(task, dedup).await.unwrap();
        let lease = NewLease {
            task_id,
            attempt_number: 0,
            worker_id,
            acquired_at: in_past,
            timeout_at: in_past,
        };
        tx.record_acquisition(lease).await.unwrap();
        tx.commit_dyn().await.unwrap();
        (task_id, worker_id)
    }

    #[tokio::test]
    async fn reaper_a_reclaims_expired_lease() {
        // Arrange
        let state = build_state().await;
        let (task_id, _) = seed_expired_runtime(&state, "ns", "type").await;

        // Act
        reaper_a_tick(Arc::clone(&state)).await.unwrap();

        // Assert: runtime row gone, task back to PENDING with attempt += 1.
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let task = tx.get_task_by_id(task_id).await.unwrap().expect("task row");
        let _ = tx.commit_dyn().await;
        assert_eq!(task.status, taskq_storage::TaskStatus::Pending);
        assert_eq!(task.attempt_number, 1);
    }

    #[tokio::test]
    async fn reaper_b_marks_dead_worker_and_reclaims() {
        // Arrange: insert a runtime row with a recent (not-yet-expired)
        // timeout_at, but a stale heartbeat. Reaper B must reclaim.
        let state = build_state().await;
        let now = current_timestamp();
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();

        // First insert task + non-expired runtime.
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let task = NewTask {
            task_id,
            namespace: Namespace::new("ns"),
            task_type: TaskType::new("type"),
            priority: 0,
            payload: Bytes::from_static(b"{}"),
            payload_hash: [0u8; 32],
            submitted_at: now,
            expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
            max_retries: 3,
            retry_initial_ms: 1_000,
            retry_max_ms: 10_000,
            retry_coefficient: 2.0,
            traceparent: Bytes::new(),
            tracestate: Bytes::new(),
            format_version: 1,
        };
        let dedup = NewDedupRecord {
            namespace: Namespace::new("ns"),
            key: IdempotencyKey::new("hb-test"),
            payload_hash: [0u8; 32],
            expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
        };
        tx.insert_task(task, dedup).await.unwrap();
        let lease = NewLease {
            task_id,
            attempt_number: 0,
            worker_id,
            acquired_at: now,
            timeout_at: Timestamp::from_unix_millis(now.as_unix_millis() + 60_000),
        };
        tx.record_acquisition(lease).await.unwrap();
        tx.commit_dyn().await.unwrap();

        // Stamp a stale heartbeat (older than lease_window_seconds).
        let stale = Timestamp::from_unix_millis(now.as_unix_millis() - 600_000);
        let mut tx = state.storage.begin_dyn().await.unwrap();
        tx.record_worker_heartbeat(&worker_id, &Namespace::new("ns"), stale)
            .await
            .unwrap();
        tx.commit_dyn().await.unwrap();

        // Act
        reaper_b_tick(Arc::clone(&state)).await.unwrap();

        // Assert: task reclaimed and worker stamped declared_dead_at.
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let task = tx.get_task_by_id(task_id).await.unwrap().expect("task row");
        let workers = tx.list_workers(&Namespace::new("ns"), true).await.unwrap();
        let _ = tx.commit_dyn().await;
        assert_eq!(task.status, taskq_storage::TaskStatus::Pending);
        let worker = workers
            .into_iter()
            .find(|w| w.worker_id == worker_id)
            .expect("worker row");
        assert!(worker.declared_dead_at.is_some());
    }
}
