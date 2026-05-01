//! Audit-log retention pruner — Phase 5d.
//!
//! `design.md` §11.4: every namespace has an `audit_log_retention_days` knob
//! on its `NamespaceQuota` (default 90). Older audit rows MUST be pruned by a
//! periodic, rate-limited background job — same shape as the dedup-expiry
//! job (`design.md` §6.7 / §8.3).
//!
//! Phase 5d ships the pruner as a background tokio task spawned from
//! `main.rs::run_serve`. Each pass:
//!
//! - reads each loaded namespace's `audit_log_retention_days` from
//!   `NamespaceQuota`,
//! - computes `cutoff = NOW − retention_days`,
//! - deletes up to [`PRUNE_BATCH_SIZE`] rows for that namespace whose
//!   `timestamp < cutoff` via [`StorageTxDyn::delete_audit_logs_before`],
//! - emits `taskq_audit_log_pruned_total{namespace}` per delete count.
//!
//! Cadence is hourly by default (`design.md` §11.4 — "rate-limited"; the
//! batch cap bounds the storage spike). Backends with millions of rows per
//! day will want a tighter cadence; that's a config knob in v1.1+.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::KeyValue;
use taskq_storage::{Namespace, StorageError, Timestamp};

use crate::handlers::retry::with_serializable_retry;
use crate::shutdown::ShutdownReceiver;
use crate::state::CpState;

/// Polling cadence. `design.md` §11.4: hourly is the default. Operators
/// override at the config level if they need tighter pruning.
pub const PRUNER_TICK: Duration = Duration::from_secs(60 * 60);

/// Maximum rows pruned per namespace per pass. Bounded to avoid storage
/// spikes; the next pass picks up where this one left off.
pub const PRUNE_BATCH_SIZE: usize = 1_000;

/// Spawn the pruner. Returns the JoinHandle so `main` can await it after the
/// gRPC server exits.
pub fn spawn(state: Arc<CpState>, shutdown: ShutdownReceiver) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run(state, shutdown).await;
    })
}

async fn run(state: Arc<CpState>, mut shutdown: ShutdownReceiver) {
    tracing::info!("audit-pruner: started");
    let mut ticker = tokio::time::interval(PRUNER_TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate-fire — we want at least one tick of warm-up.
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(err) = prune_once(&state).await {
                    tracing::warn!(error = %err, "audit-pruner: tick failed");
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!("audit-pruner: shutdown observed; exiting");
                    return;
                }
            }
        }
    }
}

/// One pruning pass. Walks every loaded namespace, reads its retention,
/// runs one bounded DELETE, emits a metric. Errors per-namespace are logged
/// but do not abort the pass.
pub async fn prune_once(state: &Arc<CpState>) -> Result<(), StorageError> {
    let namespaces = state.strategy_registry.loaded_namespaces();
    for ns in namespaces {
        if let Err(err) = prune_namespace(state, &ns).await {
            tracing::warn!(
                namespace = %ns.as_str(),
                error = %err,
                "audit-pruner: namespace prune failed"
            );
        }
    }
    Ok(())
}

/// Prune one namespace's audit log. Reads the retention setting, computes
/// the cutoff, runs a bounded DELETE inside a SERIALIZABLE transaction.
async fn prune_namespace(state: &Arc<CpState>, namespace: &Namespace) -> Result<(), StorageError> {
    // Read retention config.
    let metrics = state.metrics.clone();
    let retention_days = with_serializable_retry(&metrics, "audit_pruner_read_retention", || {
        let state = Arc::clone(state);
        let namespace = namespace.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            let quota = match tx.get_namespace_quota(&namespace).await {
                Ok(q) => q,
                Err(StorageError::NotFound) => {
                    let _ = tx.rollback_dyn().await;
                    return Ok(0u32);
                }
                Err(other) => {
                    let _ = tx.rollback_dyn().await;
                    return Err(other);
                }
            };
            tx.commit_dyn().await?;
            Ok(quota.audit_log_retention_days)
        }
    })
    .await?;

    if retention_days == 0 {
        // Either the namespace is unprovisioned or the operator opted out
        // of pruning by zeroing the retention. Either way, do nothing.
        return Ok(());
    }

    let cutoff = compute_cutoff(retention_days);
    let deleted = with_serializable_retry(&metrics, "audit_pruner_delete", || {
        let state = Arc::clone(state);
        let namespace = namespace.clone();
        async move {
            let mut tx = state.storage.begin_dyn().await?;
            let n = tx
                .delete_audit_logs_before(&namespace, cutoff, PRUNE_BATCH_SIZE)
                .await?;
            tx.commit_dyn().await?;
            Ok(n)
        }
    })
    .await?;

    if deleted > 0 {
        state.metrics.audit_log_pruned_total.add(
            deleted as u64,
            &[KeyValue::new("namespace", namespace.as_str().to_owned())],
        );
        tracing::info!(
            namespace = %namespace.as_str(),
            deleted,
            "audit-pruner: pruned rows"
        );
    }

    Ok(())
}

/// Compute the cutoff timestamp `(now - retention_days)`. Pulled into a free
/// function so tests can substitute a clock.
fn compute_cutoff(retention_days: u32) -> Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let retention_ms = i64::from(retention_days) * 24 * 60 * 60 * 1_000;
    Timestamp::from_unix_millis(now.saturating_sub(retention_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

    use bytes::Bytes;
    use taskq_storage::{AuditEntry, Namespace};

    use crate::config::{CpConfig, OtelExporterConfig, StorageBackendConfig};
    use crate::observability::MetricsHandle;
    use crate::shutdown::channel;
    use crate::state::DynStorage;
    use crate::strategy::StrategyRegistry;

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

    /// Insert N audit rows with `timestamp = base + i ms`.
    async fn seed_audit_rows(state: &Arc<CpState>, ns: &Namespace, base_ms: i64, count: usize) {
        let mut tx = state.storage.begin_dyn().await.unwrap();
        for i in 0..count {
            let entry = AuditEntry {
                timestamp: Timestamp::from_unix_millis(base_ms + i as i64),
                actor: "test".to_owned(),
                rpc: "TestRpc".to_owned(),
                namespace: Some(ns.clone()),
                request_summary: Bytes::from_static(b"{}"),
                result: "success".to_owned(),
                request_hash: [0u8; 32],
            };
            tx.audit_log_append(entry).await.unwrap();
        }
        tx.commit_dyn().await.unwrap();
    }

    #[tokio::test]
    async fn delete_audit_logs_before_removes_only_matching_namespace() {
        // Arrange: seed two namespaces with 5 rows each, all stamped at ts=0.
        let state = build_state().await;
        let ns_a = Namespace::new("a");
        let ns_b = Namespace::new("b");
        seed_audit_rows(&state, &ns_a, 0, 5).await;
        seed_audit_rows(&state, &ns_b, 0, 5).await;

        // Act: delete rows in ns_a with timestamp < 1000.
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let deleted = tx
            .delete_audit_logs_before(&ns_a, Timestamp::from_unix_millis(1_000), 100)
            .await
            .unwrap();
        tx.commit_dyn().await.unwrap();

        // Assert
        assert_eq!(deleted, 5, "all 5 ns_a rows should be deleted");
    }

    #[tokio::test]
    async fn delete_audit_logs_before_honours_n_cap() {
        // Arrange: 10 rows, all old.
        let state = build_state().await;
        let ns = Namespace::new("ns");
        seed_audit_rows(&state, &ns, 0, 10).await;

        // Act: delete with `n = 3`.
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let deleted = tx
            .delete_audit_logs_before(&ns, Timestamp::from_unix_millis(1_000_000), 3)
            .await
            .unwrap();
        tx.commit_dyn().await.unwrap();

        // Assert
        assert_eq!(deleted, 3);
    }

    #[tokio::test]
    async fn prune_once_with_no_loaded_namespaces_is_noop() {
        // Arrange
        let state = build_state().await;

        // Act
        let result = prune_once(&state).await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn delete_audit_logs_before_skips_recent_rows() {
        // Arrange: seed 5 rows at ts=2_000, ts=2_001, ...
        let state = build_state().await;
        let ns = Namespace::new("ns");
        seed_audit_rows(&state, &ns, 2_000, 5).await;

        // Act: delete with cutoff=1_000 (before all rows).
        let mut tx = state.storage.begin_dyn().await.unwrap();
        let deleted = tx
            .delete_audit_logs_before(&ns, Timestamp::from_unix_millis(1_000), 100)
            .await
            .unwrap();
        tx.commit_dyn().await.unwrap();

        // Assert: no rows match.
        assert_eq!(deleted, 0);
    }

    #[test]
    fn compute_cutoff_subtracts_retention_window() {
        // Arrange: 1 day retention.
        let cutoff = compute_cutoff(1);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Assert: cutoff is roughly (now - 24h). Allow 1s of clock slop.
        let expected = now_ms - 24 * 60 * 60 * 1_000;
        assert!(
            (cutoff.as_unix_millis() - expected).abs() < 1_000,
            "cutoff {} not within 1s of expected {}",
            cutoff.as_unix_millis(),
            expected
        );
    }
}
