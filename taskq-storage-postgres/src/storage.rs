//! `Storage` implementation: pool, listener, rate limiter.

use std::sync::Arc;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use taskq_storage::{Result, Storage, StorageError};
use tokio_postgres::{Config, NoTls};

use crate::errors::{map_db_error, map_pool_error};
use crate::listener::Dispatcher;
use crate::rate::RateLimiter;
use crate::tx::PostgresTx;

/// Connection / pool wiring options.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// libpq-style connection string. The same string is used for both the
    /// transaction pool and the dedicated `LISTEN` connection.
    pub conn_str: String,
    /// Maximum pool size for transactional work.
    pub pool_size: usize,
}

impl PostgresConfig {
    /// Construct from a connection string with a sensible default pool size.
    pub fn new(conn_str: impl Into<String>) -> Self {
        Self {
            conn_str: conn_str.into(),
            pool_size: 16,
        }
    }
}

/// Postgres backend handle. Held inside an `Arc` by the CP layer.
pub struct PostgresStorage {
    pool: Pool,
    rate_limiter: Arc<RateLimiter>,
    dispatcher: Arc<Dispatcher>,
}

impl PostgresStorage {
    /// Connect, build the transaction pool, spawn the `LISTEN` dispatcher.
    /// Migrations are NOT run here — call [`crate::migrate`] explicitly.
    pub async fn connect(config: PostgresConfig) -> Result<Arc<Self>> {
        let pg_config: Config = config.conn_str.parse().map_err(map_db_error)?;

        // Transaction pool. The Custom recycle SQL emits a bare `ROLLBACK`
        // so a dropped-without-commit `PostgresTx` doesn't leave the
        // connection in a "transaction in progress" state for the next
        // checkout. `ROLLBACK` outside a transaction is a no-op (Postgres
        // logs a warning notice), so the recycle is safe on clean
        // connections too. Avoids `DISCARD ALL` to keep the statement
        // cache warm.
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Custom("ROLLBACK".to_owned()),
        };
        let mgr = Manager::from_config(pg_config.clone(), NoTls, mgr_config);
        let pool = Pool::builder(mgr)
            .max_size(config.pool_size)
            .build()
            .map_err(StorageError::backend)?;

        // Listener side: separate connection so its session stays alive
        // independent of pool checkout.
        let (listen_client, listen_conn) = pg_config.connect(NoTls).await.map_err(map_db_error)?;
        let dispatcher = Dispatcher::spawn(listen_client, listen_conn);

        Ok(Arc::new(Self {
            pool,
            rate_limiter: Arc::new(RateLimiter::new()),
            dispatcher,
        }))
    }

    /// Borrow the connection pool. Used by [`crate::migrate`].
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Borrow the rate limiter (per-replica in-memory token buckets).
    pub fn rate_limiter(&self) -> Arc<RateLimiter> {
        Arc::clone(&self.rate_limiter)
    }

    /// Borrow the LISTEN dispatcher.
    pub fn dispatcher(&self) -> Arc<Dispatcher> {
        Arc::clone(&self.dispatcher)
    }
}

impl Storage for PostgresStorage {
    type Tx<'a>
        = PostgresTx<'a>
    where
        Self: 'a;

    async fn begin(&self) -> Result<Self::Tx<'_>> {
        let conn = self.pool.get().await.map_err(map_pool_error)?;
        // SERIALIZABLE per design.md §1.1 — the default for state-transition
        // transactions. Heartbeats run via separate methods that do their
        // own (READ COMMITTED) connection acquisition.
        conn.batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await
            .map_err(map_db_error)?;
        Ok(PostgresTx::new(
            conn,
            Arc::clone(&self.rate_limiter),
            Arc::clone(&self.dispatcher),
            Arc::new(self.pool.clone()),
        ))
    }
}
