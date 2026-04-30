//! `SqliteStorage` — the [`taskq_storage::Storage`] implementation.
//!
//! Holds a single rusqlite `Connection` behind an async `Mutex`. `begin()`
//! takes the mutex, issues `BEGIN IMMEDIATE` (acquiring SQLite's writer
//! lock immediately to avoid `SQLITE_BUSY` deadlocks under contention),
//! and hands ownership of the guard to a [`SqliteTx`]. While the Tx is
//! alive no other caller can call `begin()` — single-writer scope per
//! `design.md` §8.3.

use std::path::Path;
use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::Mutex;

use taskq_storage::{
    error::{Result as StorageResult, StorageError},
    traits::Storage,
};

use crate::convert::map_sql_error;
use crate::migrate;
use crate::tx::SqliteTx;

/// Backend handle. Cheap to clone — internally an `Arc<Mutex<Connection>>`.
#[derive(Clone)]
pub struct SqliteStorage {
    inner: Arc<Mutex<Connection>>,
}

impl SqliteStorage {
    /// Open a new on-disk SQLite database and run all migrations.
    ///
    /// The path is created if it does not exist. Pragmas relevant to the
    /// embedded scope are set:
    ///
    /// - `journal_mode = WAL` — concurrent reads alongside the single writer.
    /// - `synchronous = NORMAL` — safe with WAL, fewer fsync calls.
    /// - `foreign_keys = ON` — exercise the `ON DELETE CASCADE` defined in
    ///   the schema.
    /// - `busy_timeout = 5000` — short bounded wait if WAL contention does
    ///   surface (should not happen under single-writer scope, but cheap
    ///   insurance).
    pub async fn open<P: AsRef<Path>>(path: P) -> StorageResult<Self> {
        let mut connection = Connection::open(path).map_err(map_sql_error)?;
        Self::configure(&connection)?;
        migrate::migrate(&mut connection).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(connection)),
        })
    }

    /// Open an in-memory SQLite database and run migrations. Tests use this.
    pub async fn open_in_memory() -> StorageResult<Self> {
        let mut connection = Connection::open_in_memory().map_err(map_sql_error)?;
        Self::configure(&connection)?;
        migrate::migrate(&mut connection).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(connection)),
        })
    }

    fn configure(connection: &Connection) -> StorageResult<()> {
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(map_sql_error)?;
        connection
            .pragma_update(None, "synchronous", "NORMAL")
            .map_err(map_sql_error)?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(map_sql_error)?;
        connection
            .pragma_update(None, "busy_timeout", 5000)
            .map_err(map_sql_error)?;
        Ok(())
    }

    /// Internal: take exclusive ownership of the connection mutex by
    /// cloning the `Arc` and acquiring the lock. Used by [`Self::begin`].
    fn handle(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.inner)
    }
}

impl Storage for SqliteStorage {
    type Tx<'a>
        = SqliteTx
    where
        Self: 'a;

    async fn begin(&self) -> StorageResult<Self::Tx<'_>> {
        let mutex = self.handle();
        // Owned guard so the Tx can hold the lock across awaits without
        // borrowing from `self`.
        let guard = mutex.lock_owned().await;
        let mut tx = SqliteTx::new(guard);
        // BEGIN IMMEDIATE acquires SQLite's write lock right now rather
        // than promoting from a deferred read lock on the first write.
        // This avoids `SQLITE_BUSY` surprises (`design.md` §8.3 SQLite
        // notes).
        tx.execute_batch("BEGIN IMMEDIATE")
            .await
            .map_err(|err| match err {
                StorageError::BackendError(_) => err,
                other => other,
            })?;
        Ok(tx)
    }
}
