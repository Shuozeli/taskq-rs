//! Periodic metrics gauge refresher — Phase 5d.
//!
//! Several metrics in `design.md` §11.3 are gauges that reflect storage state
//! (PENDING / DISPATCHED counts, registered worker counts, quota usage
//! ratios). Cardinality discipline forbids emitting them on every handler
//! call — they're sampled on a periodic cadence (30 s default) by this
//! background task.
//!
//! The refresher iterates over the namespaces it has observed running traffic
//! through and queries `count_tasks_by_status`, `list_workers`, and
//! `get_namespace_quota` to update the in-memory OTel `UpDownCounter`
//! instruments. The waiter-pool gauge (`taskq_waiters_active`) updates from
//! the in-process `WaiterPool::waiter_count_per_replica` snapshot — no
//! storage round-trip needed.
//!
//! In-memory namespace tracking lives in `WaiterPool` and the storage handle.
//! Phase 5d uses the strategy registry's loaded namespace list as the source
//! of truth; namespaces with degraded strategies are still refreshed (their
//! counts feed alerting).
//!
//! The refresher is best-effort: storage errors are logged and the next tick
//! retries.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::KeyValue;
use taskq_storage::{Namespace, TaskStatus};

use crate::shutdown::ShutdownReceiver;
use crate::state::CpState;

/// Refresh cadence per `design.md` §11.3 round-2 verdict (30 s — short
/// enough to react to capacity changes, long enough to keep the storage
/// scan cost negligible).
pub const REFRESH_TICK: Duration = Duration::from_secs(30);

/// Spawn the background refresher. Returns a `JoinHandle` so `main` can
/// await it after the gRPC server returns.
pub fn spawn(state: Arc<CpState>, shutdown: ShutdownReceiver) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run(state, shutdown).await;
    })
}

async fn run(state: Arc<CpState>, mut shutdown: ShutdownReceiver) {
    tracing::info!("metrics-refresher: started");
    let mut tracker = NamespaceGaugeTracker::default();

    let mut ticker = tokio::time::interval(REFRESH_TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate-fire so the very first observation is at least
    // `REFRESH_TICK` after startup; storage may not be populated yet.
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(err) = refresh_once(&state, &mut tracker).await {
                    tracing::warn!(error = %err, "metrics-refresher: refresh tick failed");
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    tracing::info!("metrics-refresher: shutdown observed; exiting");
                    return;
                }
            }
        }
    }
}

/// Run a single refresh tick. Tolerates storage errors per-namespace so a
/// single broken namespace does not poison the rest.
pub async fn refresh_once(
    state: &Arc<CpState>,
    tracker: &mut NamespaceGaugeTracker,
) -> Result<(), taskq_storage::StorageError> {
    // Source of truth: the strategy registry's loaded namespaces (healthy +
    // degraded). The waiter pool's `waiter_count_per_replica` is also
    // refreshed unconditionally below.
    let namespaces = state.strategy_registry.loaded_namespaces();

    // Update the per-replica waiter gauge first — it's the only metric that
    // does not require a storage round-trip.
    let parked = state.waiter_pool.waiter_count_per_replica() as i64;
    tracker.set_waiters_replica(state, parked);

    for ns in namespaces {
        if let Err(err) = refresh_namespace(state, &ns, tracker).await {
            tracing::warn!(
                namespace = %ns.as_str(),
                error = %err,
                "metrics-refresher: namespace refresh failed"
            );
        }
    }
    Ok(())
}

/// Refresh every gauge for one namespace. Opens one transaction per
/// namespace and reads counts + workers + quota.
async fn refresh_namespace(
    state: &Arc<CpState>,
    namespace: &Namespace,
    tracker: &mut NamespaceGaugeTracker,
) -> Result<(), taskq_storage::StorageError> {
    let mut tx = state.storage.begin_dyn().await?;
    let counts = tx.count_tasks_by_status(namespace).await?;
    let workers = tx.list_workers(namespace, false).await?;
    let quota = tx.get_namespace_quota(namespace).await.ok();
    let _ = tx.commit_dyn().await;

    let pending = counts.get(&TaskStatus::Pending).copied().unwrap_or(0) as i64;
    let inflight = counts.get(&TaskStatus::Dispatched).copied().unwrap_or(0) as i64;
    let workers_count = workers.len() as i64;

    tracker.set_pending(state, namespace, pending);
    tracker.set_inflight(state, namespace, inflight);
    tracker.set_workers(state, namespace, workers_count);

    // Quota usage ratio is observed as a histogram so dashboards can see
    // distribution across namespaces / kinds. The `kind` label distinguishes
    // pending-vs-inflight saturation; `design.md` §11.3 leaves the exact
    // dimensions to the implementation.
    if let Some(quota) = quota {
        if let Some(max_pending) = quota.max_pending {
            if max_pending > 0 {
                let ratio = pending as f64 / max_pending as f64;
                state.metrics.quota_usage_ratio.record(
                    ratio,
                    &[
                        KeyValue::new("namespace", namespace.as_str().to_owned()),
                        KeyValue::new("kind", "pending"),
                    ],
                );
            }
        }
        if let Some(max_inflight) = quota.max_inflight {
            if max_inflight > 0 {
                let ratio = inflight as f64 / max_inflight as f64;
                state.metrics.quota_usage_ratio.record(
                    ratio,
                    &[
                        KeyValue::new("namespace", namespace.as_str().to_owned()),
                        KeyValue::new("kind", "inflight"),
                    ],
                );
            }
        }
    }

    Ok(())
}

/// In-memory tracking of the last-observed gauge value per (namespace, gauge).
/// `UpDownCounter::add` accepts a delta, so we apply
/// `(new_value - last_value)` on each refresh so the gauge converges on the
/// observed value without requiring re-registration.
#[derive(Default)]
pub struct NamespaceGaugeTracker {
    pending: HashMap<Namespace, i64>,
    inflight: HashMap<Namespace, i64>,
    workers: HashMap<Namespace, i64>,
    last_waiter_replica: i64,
}

impl NamespaceGaugeTracker {
    fn set_pending(&mut self, state: &Arc<CpState>, ns: &Namespace, value: i64) {
        let last = self.pending.get(ns).copied().unwrap_or(0);
        let delta = value - last;
        if delta != 0 {
            state
                .metrics
                .pending_count
                .add(delta, &[KeyValue::new("namespace", ns.as_str().to_owned())]);
        }
        self.pending.insert(ns.clone(), value);
    }

    fn set_inflight(&mut self, state: &Arc<CpState>, ns: &Namespace, value: i64) {
        let last = self.inflight.get(ns).copied().unwrap_or(0);
        let delta = value - last;
        if delta != 0 {
            state
                .metrics
                .inflight_count
                .add(delta, &[KeyValue::new("namespace", ns.as_str().to_owned())]);
        }
        self.inflight.insert(ns.clone(), value);
    }

    fn set_workers(&mut self, state: &Arc<CpState>, ns: &Namespace, value: i64) {
        let last = self.workers.get(ns).copied().unwrap_or(0);
        let delta = value - last;
        if delta != 0 {
            state
                .metrics
                .workers_registered
                .add(delta, &[KeyValue::new("namespace", ns.as_str().to_owned())]);
        }
        self.workers.insert(ns.clone(), value);
    }

    fn set_waiters_replica(&mut self, state: &Arc<CpState>, value: i64) {
        let delta = value - self.last_waiter_replica;
        if delta != 0 {
            state
                .metrics
                .waiters_active
                .add(delta, &[KeyValue::new("scope", "replica")]);
        }
        self.last_waiter_replica = value;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

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

    #[tokio::test]
    async fn refresh_once_succeeds_with_empty_storage() {
        // Arrange: an in-memory SQLite with no tasks/workers; the refresher
        // tracks zero namespaces because `StrategyRegistry::empty` has none
        // loaded. The tick must be a no-op without panicking.
        let state = build_state().await;
        let mut tracker = NamespaceGaugeTracker::default();

        // Act
        let result = refresh_once(&state, &mut tracker).await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn refresh_once_with_registered_namespace_calls_refresh_namespace() {
        // Arrange: build state with a registered namespace so
        // `refresh_once` actually iterates and calls
        // `refresh_namespace` (the previous test exercised the
        // empty-registry no-op path).
        use crate::strategy::{
            build_admitter, build_dispatcher, AdmitterParams, DispatcherParams, NamespaceStrategy,
        };
        let storage = taskq_storage_sqlite::SqliteStorage::open_in_memory()
            .await
            .expect("SqliteStorage::open_in_memory");
        let storage_arc: Arc<dyn DynStorage> = Arc::new(storage);
        let mut registry = StrategyRegistry::empty();
        let ns = Namespace::new("system_default");
        registry.insert(
            ns,
            NamespaceStrategy {
                admitter: build_admitter("Always", &AdmitterParams::default()).unwrap(),
                dispatcher: build_dispatcher("PriorityFifo", &DispatcherParams::default()).unwrap(),
            },
        );
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
        let state = Arc::new(CpState::new(
            storage_arc,
            Arc::new(registry),
            MetricsHandle::noop(),
            rx,
            config,
        ));
        let mut tracker = NamespaceGaugeTracker::default();

        // Act: empty namespace; refresh_namespace runs through every
        // SQL read (count_tasks_by_status, list_workers,
        // get_namespace_quota) and the quota_usage_ratio branch since
        // system_default has neither max_pending nor max_inflight set
        // (both NULL).
        let result = refresh_once(&state, &mut tracker).await;

        // Assert: pass without error; tracker recorded zero pending /
        // inflight / workers for the namespace (system_default has no
        // tasks).
        assert!(result.is_ok());
        let ns_key = Namespace::new("system_default");
        assert_eq!(tracker.pending.get(&ns_key), Some(&0));
        assert_eq!(tracker.inflight.get(&ns_key), Some(&0));
        assert_eq!(tracker.workers.get(&ns_key), Some(&0));
    }

    #[tokio::test]
    async fn tracker_emits_delta_to_avoid_double_counting() {
        // Arrange
        let state = build_state().await;
        let mut tracker = NamespaceGaugeTracker::default();
        let ns = Namespace::new("t");

        // Act: setting twice with the same value emits no second delta.
        tracker.set_pending(&state, &ns, 5);
        tracker.set_pending(&state, &ns, 5);
        tracker.set_pending(&state, &ns, 7);

        // Assert: tracker remembers the last applied value so the next
        // refresh emits `(new - last)` not `new`.
        assert_eq!(tracker.pending.get(&ns), Some(&7));
    }
}
