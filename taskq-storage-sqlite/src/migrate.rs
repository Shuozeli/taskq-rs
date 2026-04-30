//! Schema-migration runner for the SQLite backend.
//!
//! Each migration is a `.sql` file embedded with `include_str!`. The runner
//! walks them in lexical order, runs each in a transaction, and records the
//! applied version + filename + epoch-millis in `_taskq_migrations`. The
//! audit table is created on demand; idempotent re-invocation is a no-op.

use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension, Transaction};

use taskq_storage::error::{Result as StorageResult, StorageError};

use crate::convert::map_sql_error;

/// One migration step, embedded at compile time. The version is the integer
/// prefix of the file name (`0001`, `0002`, ...). Adding a new migration
/// means adding a `.sql` file and appending an entry here.
pub struct Migration {
    pub version: i64,
    pub name: &'static str,
    pub sql: &'static str,
}

/// All migrations the SQLite backend ships, in order.
pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "0001_initial.sql",
    sql: include_str!("../migrations/0001_initial.sql"),
}];

/// Apply every migration that has not yet been recorded.
///
/// Each migration runs inside its own transaction. Failure aborts the
/// missing-and-following migrations; previously-applied migrations stay
/// committed. Re-invocation after a partial failure resumes from the first
/// unapplied migration.
pub async fn migrate_blocking(connection: &mut Connection) -> StorageResult<()> {
    ensure_audit_table(connection)?;

    for migration in MIGRATIONS {
        if is_applied(connection, migration.version)? {
            continue;
        }
        apply(connection, migration)?;
    }
    Ok(())
}

/// Async wrapper. The underlying rusqlite calls are blocking; the caller
/// owns the connection and provides it `&mut`, so the simplest correct
/// shape is to perform the migration on the current thread inside a small
/// `spawn_blocking` is not needed — `migrate` is called once at startup,
/// not on the hot path.
///
/// Exposed `pub async fn` per the Phase 4 spec ("`pub async fn migrate(conn:
/// &mut Connection) -> Result<(), StorageError>`").
pub async fn migrate(connection: &mut Connection) -> StorageResult<()> {
    migrate_blocking(connection).await
}

fn ensure_audit_table(connection: &Connection) -> StorageResult<()> {
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS _taskq_migrations (
                version    INTEGER PRIMARY KEY NOT NULL,
                name       TEXT    NOT NULL,
                applied_at INTEGER NOT NULL
             );",
        )
        .map_err(map_sql_error)?;
    Ok(())
}

fn is_applied(connection: &Connection, version: i64) -> StorageResult<bool> {
    connection
        .query_row(
            "SELECT 1 FROM _taskq_migrations WHERE version = ?1",
            params![version],
            |_| Ok(()),
        )
        .optional()
        .map(|opt| opt.is_some())
        .map_err(map_sql_error)
}

fn apply(connection: &mut Connection, migration: &Migration) -> StorageResult<()> {
    let tx = connection.transaction().map_err(map_sql_error)?;
    apply_in_tx(&tx, migration)?;
    tx.commit().map_err(map_sql_error)?;
    Ok(())
}

fn apply_in_tx(tx: &Transaction<'_>, migration: &Migration) -> StorageResult<()> {
    tx.execute_batch(migration.sql).map_err(map_sql_error)?;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    tx.execute(
        "INSERT INTO _taskq_migrations (version, name, applied_at) VALUES (?1, ?2, ?3)",
        params![migration.version, migration.name, now_ms],
    )
    .map_err(map_sql_error)?;

    // Stamp the meta row so taskq_meta.last_migrated_at tracks the most
    // recent migration. taskq_meta is created by 0001_initial.sql; only
    // update it once it exists.
    let meta_exists: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'taskq_meta'",
            [],
            |row| row.get(0),
        )
        .map_err(map_sql_error)?;
    if meta_exists > 0 {
        tx.execute(
            "UPDATE taskq_meta SET schema_version = ?1, last_migrated_at = ?2 WHERE id = 1",
            params![migration.version, now_ms],
        )
        .map_err(map_sql_error)?;
    }

    Ok(())
}

/// Return the current `schema_version` recorded in `taskq_meta`, or
/// `StorageError::NotFound` if the table has not been migrated yet.
pub async fn current_schema_version(connection: &Connection) -> StorageResult<i64> {
    let row: Option<i64> = connection
        .query_row(
            "SELECT schema_version FROM taskq_meta WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(map_sql_error)?;
    row.ok_or(StorageError::NotFound)
}
