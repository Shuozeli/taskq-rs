//! Shared CP runtime state.
//!
//! `CpState` is the single object cloned (as `Arc<CpState>`) into every
//! gRPC handler, every reaper task, and the health server. It owns:
//!
//! - the storage handle (boxed for runtime backend dispatch),
//! - the strategy registry,
//! - the metrics handle,
//! - the shutdown receiver,
//! - the loaded `CpConfig`,
//! - the namespace-config cache (placeholder; Phase 5c implements),
//! - the long-poll waiter pool (placeholder; Phase 5b/5c implement).
//!
//! Phase 5a pins the shape; Phase 5b/c plug in the live behaviour for the
//! placeholders.
//!
//! ## Object-safe storage seam
//!
//! [`StorageTxDyn`] is the dyn-dispatch shim around [`taskq_storage::StorageTx`].
//! Strategies (`Admitter` / `Dispatcher`) hold an `Arc<dyn Admitter>` and need
//! to call into a transaction; native `async fn in trait` (used by
//! `StorageTx`) is not object-safe in `dyn` position because the returned
//! `impl Future` types differ per backend. The shim erases those Futures
//! behind `BoxFuture`, narrows the surface to the methods strategies actually
//! call, and is implemented blanket-style for any concrete `StorageTx`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

use taskq_storage::{
    CapacityDecision, CapacityKind, LockedTask, Namespace, NamespaceQuota, NewLease, PickCriteria,
    Storage, StorageError, StorageTx,
};

use crate::config::CpConfig;
use crate::observability::MetricsHandle;
use crate::shutdown::ShutdownReceiver;
use crate::strategy::StrategyRegistry;

/// Object-safe alias for the storage handle. The CP layer takes a
/// `Arc<dyn DynStorage>` so the same handler code works against
/// `PostgresStorage` and `SqliteStorage` without monomorphising every
/// handler.
///
/// Phase 5b will define the trait body (a method-by-method shim that
/// forwards to `taskq_storage::Storage`) and the blanket impl. For Phase
/// 5a the trait is empty so `Arc<dyn DynStorage>` exists as a type but no
/// handler can call anything on it (handler bodies are `todo!()`).
pub trait DynStorage: Send + Sync + 'static {}

/// Blanket impl: any `Storage` impl participates as `DynStorage`.
impl<S: Storage> DynStorage for S {}

// ---------------------------------------------------------------------------
// StorageTxDyn -- object-safe shim for `StorageTx`
// ---------------------------------------------------------------------------

/// Boxed future alias used by [`StorageTxDyn`]. `'a` is the borrow of the
/// transaction the future captures; `T` is the method's return type.
pub type StorageTxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Object-safe shim around [`taskq_storage::StorageTx`].
///
/// The native `StorageTx` trait uses `async fn in trait` (returns an
/// associated `impl Future`), which makes it unusable behind `dyn`. The
/// strategy framework needs `dyn` dispatch so per-namespace `Arc<dyn Admitter>`
/// / `Arc<dyn Dispatcher>` can be picked at runtime
/// (`design.md` §7, `problems/05`). This shim forwards a narrow set of
/// `StorageTx` methods through `BoxFuture` so strategies can call into the
/// transaction without taking on the underlying `StorageTx`'s associated-type
/// machinery.
///
/// The blanket impl below covers every concrete `StorageTx` type, so the
/// shim is automatic for both `taskq-storage-postgres::PostgresTx` and
/// `taskq-storage-sqlite::SqliteTx`.
///
/// ## What's exposed
///
/// Only the methods the v1 strategies need:
///
/// - [`StorageTxDyn::check_capacity_quota`] -- `MaxPending` admitter.
/// - [`StorageTxDyn::oldest_pending_age_ms`] -- `CoDel` admitter.
/// - [`StorageTxDyn::pick_and_lock_pending`] -- every dispatcher.
/// - [`StorageTxDyn::record_acquisition`] -- every dispatcher's lease step.
///
/// Other `StorageTx` methods (`insert_task`, `complete_task`, ...) are not
/// behind dyn; the gRPC handlers (Phase 5b) drive them through the static
/// `Storage::Tx<'_>` associated type directly.
pub trait StorageTxDyn: Send {
    /// `MaxPending` admit path: ask the backend whether `kind`'s configured
    /// limit has been hit for `ns`. Returns the observed counts so the
    /// handler can populate `pending_count` / `pending_limit` on
    /// `RESOURCE_EXHAUSTED` rejections (`design.md` §10.1).
    fn check_capacity_quota<'a>(
        &'a mut self,
        ns: &'a Namespace,
        kind: CapacityKind,
    ) -> StorageTxFuture<'a, Result<CapacityDecision, StorageError>>;

    /// `CoDel` admit path: head-of-line latency of PENDING tasks in `ns` in
    /// milliseconds. `None` when no PENDING tasks exist; in that case CoDel
    /// always admits.
    fn oldest_pending_age_ms<'a>(
        &'a mut self,
        ns: &'a Namespace,
    ) -> StorageTxFuture<'a, Result<Option<u64>, StorageError>>;

    /// Dispatch path: backend translates `criteria.ordering` to its native
    /// idiom, locks one row, and returns it (or `None` when no candidate
    /// matches the filters).
    fn pick_and_lock_pending<'a>(
        &'a mut self,
        criteria: PickCriteria,
    ) -> StorageTxFuture<'a, Result<Option<LockedTask>, StorageError>>;

    /// Dispatch path: write the `task_runtime` row alongside the locked
    /// task. Caller is the `AcquireTask` handler; the strategy itself does
    /// not call this in v1 (the dispatcher returns the locked task and the
    /// handler records the lease) but the shim exposes it so future
    /// strategies (or test fakes) have access without needing static
    /// dispatch.
    fn record_acquisition<'a>(
        &'a mut self,
        lease: NewLease,
    ) -> StorageTxFuture<'a, Result<(), StorageError>>;
}

/// Blanket impl: any concrete `StorageTx` participates as `StorageTxDyn`.
///
/// Each shim method calls the native `StorageTx` method and `Box::pin`s the
/// returned `impl Future`. The trait bound `T: StorageTx + Send + ?Sized`
/// allows borrowed-mut references (`&mut dyn StorageTx`) as well as owned
/// values.
impl<T> StorageTxDyn for T
where
    T: StorageTx + ?Sized,
{
    fn check_capacity_quota<'a>(
        &'a mut self,
        ns: &'a Namespace,
        kind: CapacityKind,
    ) -> StorageTxFuture<'a, Result<CapacityDecision, StorageError>> {
        Box::pin(StorageTx::check_capacity_quota(self, ns, kind))
    }

    fn oldest_pending_age_ms<'a>(
        &'a mut self,
        ns: &'a Namespace,
    ) -> StorageTxFuture<'a, Result<Option<u64>, StorageError>> {
        Box::pin(StorageTx::oldest_pending_age_ms(self, ns))
    }

    fn pick_and_lock_pending<'a>(
        &'a mut self,
        criteria: PickCriteria,
    ) -> StorageTxFuture<'a, Result<Option<LockedTask>, StorageError>> {
        Box::pin(StorageTx::pick_and_lock_pending(self, criteria))
    }

    fn record_acquisition<'a>(
        &'a mut self,
        lease: NewLease,
    ) -> StorageTxFuture<'a, Result<(), StorageError>> {
        Box::pin(StorageTx::record_acquisition(self, lease))
    }
}

/// Shared CP state. Cloned as `Arc<CpState>` everywhere.
pub struct CpState {
    pub storage: Arc<dyn DynStorage>,
    pub strategy_registry: Arc<StrategyRegistry>,
    pub metrics: MetricsHandle,
    pub shutdown: ShutdownReceiver,
    pub config: Arc<CpConfig>,
    pub namespace_config_cache: Arc<NamespaceConfigCache>,
    pub waiter_pool: Arc<WaiterPool>,
}

impl CpState {
    /// Build the shared state. `main` calls this once after wiring the
    /// individual components.
    pub fn new(
        storage: Arc<dyn DynStorage>,
        strategy_registry: Arc<StrategyRegistry>,
        metrics: MetricsHandle,
        shutdown: ShutdownReceiver,
        config: Arc<CpConfig>,
    ) -> Self {
        let cache_ttl = Duration::from_secs(u64::from(config.quota_cache_ttl_seconds));
        let waiter_limit = config.waiter_limit_per_replica;
        Self {
            storage,
            strategy_registry,
            metrics,
            shutdown,
            config,
            namespace_config_cache: Arc::new(NamespaceConfigCache::new(cache_ttl)),
            waiter_pool: Arc::new(WaiterPool::new(waiter_limit)),
        }
    }
}

// ---------------------------------------------------------------------------
// Namespace config cache (Phase 5c)
// ---------------------------------------------------------------------------

/// Per-namespace `NamespaceQuota` cache. `design.md` §1.1 carve-out says
/// rate quotas are eventually consistent within the cache TTL (default 5s)
/// with ±10% jitter to prevent synchronized expiry. `design.md` §9.1 says
/// capacity quotas are NOT cached — they're read inline transactionally.
///
/// Phase 5a stores the structure; Phase 5c implements singleflight, jitter,
/// and the actual `get_or_load` path.
pub struct NamespaceConfigCache {
    /// Configured TTL. Phase 5c applies ±10% jitter on insert.
    pub ttl: Duration,
    /// Ready slot for cached entries. `RwLock` because reads dominate;
    /// inserts on cache miss are rare.
    entries: RwLock<HashMap<Namespace, CachedQuota>>,
}

#[derive(Clone)]
pub struct CachedQuota {
    pub quota: Arc<NamespaceQuota>,
    /// When this cache entry expires. Phase 5c jitters the ±10% on insert.
    pub expires_at: std::time::Instant,
}

impl NamespaceConfigCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Lookup an entry without populating. Returns `None` on miss or expiry.
    /// Phase 5c will use this from `get_or_load`.
    pub async fn peek(&self, ns: &Namespace) -> Option<CachedQuota> {
        let map = self.entries.read().await;
        map.get(ns).cloned().filter(|entry| {
            // Eviction is lazy in the read path; the write path can prune.
            entry.expires_at > std::time::Instant::now()
        })
    }

    /// Phase 5c implements singleflight + jittered TTL on top of this raw
    /// `insert` primitive. Phase 5a leaves it as a placeholder so the call
    /// shape is wired.
    pub async fn insert(&self, ns: Namespace, quota: Arc<NamespaceQuota>) {
        let entry = CachedQuota {
            quota,
            expires_at: std::time::Instant::now() + self.ttl,
        };
        let mut map = self.entries.write().await;
        map.insert(ns, entry);
    }
}

// ---------------------------------------------------------------------------
// Long-poll waiter pool (Phase 5b/5c)
// ---------------------------------------------------------------------------

/// Per-replica long-poll waiter set. `design.md` §6.2 / §10.1: capped at
/// 5000 waiters per replica by default. Phase 5b/5c implement the actual
/// single-waiter wakeup machinery and the namespace -> waiter index.
///
/// Phase 5a holds only the cap and a counter so the gRPC server can refuse
/// new waiters once `current_count == limit` (Phase 5b will wire the
/// REPLICA_WAITER_LIMIT_EXCEEDED rejection).
pub struct WaiterPool {
    /// Configured cap. `0` means "no cap"; v1 always sets a positive cap.
    pub limit: u32,
    /// Current parked-waiter count, exposed as a metric in Phase 5d.
    /// Atomic so the long-poll path can `fetch_add` / `fetch_sub` without
    /// taking the pool's `RwLock`.
    pub active: std::sync::atomic::AtomicU32,
}

impl WaiterPool {
    pub fn new(limit: u32) -> Self {
        Self {
            limit,
            active: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// Try to claim a waiter slot. Phase 5b will use this from the
    /// `AcquireTask` long-poll path; on `Err(LimitExceeded)` it surfaces
    /// `REPLICA_WAITER_LIMIT_EXCEEDED` per `design.md` §10.1.
    pub fn try_acquire(&self) -> Result<WaiterTicket<'_>, WaiterLimitExceeded> {
        use std::sync::atomic::Ordering;
        // Increment optimistically; if we'd exceed, decrement and refuse.
        let prior = self.active.fetch_add(1, Ordering::AcqRel);
        if self.limit > 0 && prior >= self.limit {
            self.active.fetch_sub(1, Ordering::AcqRel);
            return Err(WaiterLimitExceeded);
        }
        Ok(WaiterTicket { pool: self })
    }
}

/// RAII guard that releases the waiter slot on `Drop`. Held by the
/// `AcquireTask` handler for the duration of the long-poll wait.
pub struct WaiterTicket<'a> {
    pool: &'a WaiterPool,
}

impl Drop for WaiterTicket<'_> {
    fn drop(&mut self) {
        self.pool
            .active
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Returned from `WaiterPool::try_acquire` when the per-replica cap is
/// reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaiterLimitExceeded;

impl std::fmt::Display for WaiterLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "per-replica waiter limit exceeded")
    }
}

impl std::error::Error for WaiterLimitExceeded {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waiter_pool_releases_slot_on_drop() {
        // Arrange
        let pool = WaiterPool::new(2);

        // Act
        let ticket1 = pool.try_acquire().unwrap();
        let ticket2 = pool.try_acquire().unwrap();
        let third = pool.try_acquire();

        // Assert: third call hits the cap.
        assert!(third.is_err());

        // Act: drop one ticket; another acquire should now succeed.
        drop(ticket1);
        let ticket3 = pool.try_acquire().unwrap();

        // Assert
        drop(ticket2);
        drop(ticket3);
        assert_eq!(pool.active.load(std::sync::atomic::Ordering::Acquire), 0);
    }

    #[test]
    fn waiter_pool_with_zero_limit_is_unbounded() {
        // Arrange
        let pool = WaiterPool::new(0);

        // Act
        let _t1 = pool.try_acquire().unwrap();
        let _t2 = pool.try_acquire().unwrap();
        let _t3 = pool.try_acquire().unwrap();

        // Assert: zero limit means we never refuse.
        assert_eq!(pool.active.load(std::sync::atomic::Ordering::Acquire), 3);
    }
}
