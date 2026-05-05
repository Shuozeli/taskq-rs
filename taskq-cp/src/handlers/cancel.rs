//! Shared `cancel_internal` helper.
//!
//! `design.md` §6.7: `CancelTask` and `PurgeTasks` both transition the task
//! to `CANCELLED` (terminal) and DELETE the matching `idempotency_keys` row
//! in the same SERIALIZABLE transaction. Per §5, `CANCELLED` releases the
//! key — physical deletion (not a flag) implements that release. The pair
//! lives behind one helper so the two RPC paths cannot drift.
//!
//! Phase 5c lifts the storage-level state-transition pair onto the
//! [`taskq_storage::StorageTx::cancel_task`] trait method. The CP-side helper here is a
//! thin re-export of [`taskq_storage::CancelOutcome`] so call sites do not
//! depend on the storage crate directly for outcome matching, and so we can
//! grow the helper later (e.g. wake one waiter on cancel) without touching
//! every caller.

pub(crate) use taskq_storage::CancelOutcome;
use taskq_storage::{StorageError, TaskId};

use crate::state::StorageTxDyn;

/// Cancel `task_id` inside the caller-owned transaction `tx`.
///
/// Per `design.md` §6.7:
///
/// 1. SELECT task FOR UPDATE.
/// 2. If non-terminal: set status = CANCELLED, delete the dedup row in the
///    same txn, return [`CancelOutcome::TransitionedToCancelled`].
/// 3. If terminal: return [`CancelOutcome::AlreadyTerminal { state }`] —
///    no-op for state, but the caller may still want to delete the dedup
///    row in the `PurgeTasks` path.
/// 4. If the task does not exist: return [`CancelOutcome::NotFound`].
///
/// The helper intentionally delegates the entire dance to
/// [`taskq_storage::StorageTx::cancel_task`] so the two backend implementations can keep
/// the SQL contracts they prefer (Postgres `SELECT ... FOR UPDATE` /
/// SQLite `BEGIN IMMEDIATE`-implicit lock) without the CP layer
/// reimplementing them.
pub(crate) async fn cancel_internal(
    tx: &mut dyn StorageTxDyn,
    task_id: TaskId,
) -> Result<CancelOutcome, StorageError> {
    tx.cancel_task(task_id).await
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use taskq_storage::TaskStatus;

    use crate::state::CpState;

    /// Build a minimal CP state backed by an in-memory SQLite. Used by the
    /// test below to exercise the full helper -> backend round trip.
    async fn build_state() -> Arc<CpState> {
        use std::net::SocketAddr;

        use taskq_storage_sqlite::SqliteStorage;

        use crate::config::{CpConfig, OtelExporterConfig, StorageBackendConfig};
        use crate::observability::MetricsHandle;
        use crate::shutdown::channel;
        use crate::state::DynStorage;
        use crate::strategy::StrategyRegistry;

        let storage = SqliteStorage::open_in_memory()
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

    #[tokio::test]
    async fn cancel_internal_returns_not_found_for_unknown_task() {
        // Arrange
        let state = build_state().await;
        let mut tx = state
            .storage
            .begin_dyn()
            .await
            .expect("begin_dyn must succeed");
        let task_id = TaskId::generate();

        // Act
        let outcome = cancel_internal(&mut *tx, task_id).await.unwrap();
        let _ = tx.rollback_dyn().await;

        // Assert
        assert_eq!(outcome, CancelOutcome::NotFound);
    }

    #[tokio::test]
    async fn cancel_internal_idempotent_on_terminal_after_first_cancel() {
        // Arrange: insert a pending task via a normal submit-style insert,
        // cancel it once, then call again to observe AlreadyTerminal.
        use bytes::Bytes;
        use taskq_storage::{
            IdempotencyKey, Namespace, NewDedupRecord, NewTask, TaskType, Timestamp,
        };

        let state = build_state().await;
        let now = Timestamp::from_unix_millis(1_700_000_000_000);
        let task_id = TaskId::generate();

        let mut tx = state.storage.begin_dyn().await.unwrap();
        let new_task = NewTask {
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
            key: IdempotencyKey::new("k1"),
            payload_hash: [0u8; 32],
            expires_at: Timestamp::from_unix_millis(now.as_unix_millis() + 86_400_000),
        };
        tx.insert_task(new_task, dedup).await.unwrap();
        tx.commit_dyn().await.unwrap();

        // Act — first cancel.
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let first = cancel_internal(&mut *tx, task_id).await.unwrap();
        tx.commit_dyn().await.unwrap();

        // Act — second cancel (should be a no-op terminal observation).
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let second = cancel_internal(&mut *tx, task_id).await.unwrap();
        tx.commit_dyn().await.unwrap();

        // Assert
        assert_eq!(first, CancelOutcome::TransitionedToCancelled);
        assert_eq!(
            second,
            CancelOutcome::AlreadyTerminal {
                state: TaskStatus::Cancelled
            }
        );
    }
}
