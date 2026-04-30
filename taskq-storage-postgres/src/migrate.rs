//! Minimal migration runner.
//!
//! Bundles every `migrations/*.sql` file at compile time via
//! [`include_str!`] and applies them in lexical order. Already-applied
//! migrations are skipped using a `_taskq_migrations` table that records the
//! filename, sha256 hash of the SQL body, and the application timestamp.
//!
//! Each migration runs inside its own transaction; a partial failure leaves
//! the database in the prior consistent state and the migration table
//! untouched.

use deadpool_postgres::Pool;
use taskq_storage::Result;

use crate::errors::{map_db_error, map_pool_error};

/// Compile-time bundle of `(filename, sql_body)` pairs in lexical order.
/// Adding a new migration means adding both the file under `migrations/` and
/// a new entry in this slice. Order is load-bearing.
const MIGRATIONS: &[(&str, &str)] = &[(
    "0001_initial.sql",
    include_str!("../migrations/0001_initial.sql"),
)];

/// Apply all bundled migrations against `pool`. Idempotent: re-running after
/// a successful migration is a no-op.
pub async fn migrate(pool: &Pool) -> Result<()> {
    migrate_with_pool(pool).await
}

/// Same as [`migrate`]; named separately so the public surface can grow
/// future variants (e.g. `migrate_with_options`) without breaking the
/// minimal entry point.
pub async fn migrate_with_pool(pool: &Pool) -> Result<()> {
    let mut conn = pool.get().await.map_err(map_pool_error)?;

    // 1. Make sure the migrations bookkeeping table exists. This DDL itself
    //    is idempotent and runs outside any per-migration transaction.
    conn.batch_execute(
        "CREATE TABLE IF NOT EXISTS _taskq_migrations (
            filename     text PRIMARY KEY,
            applied_at   timestamptz NOT NULL DEFAULT NOW()
         )",
    )
    .await
    .map_err(map_db_error)?;

    for (filename, sql) in MIGRATIONS {
        let already_applied: bool = conn
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM _taskq_migrations WHERE filename = $1)",
                &[filename],
            )
            .await
            .map_err(map_db_error)?
            .get(0);

        if already_applied {
            continue;
        }

        // Each migration is one transaction. The combination of `batch_execute`
        // (so multi-statement SQL works) and an explicit BEGIN/COMMIT around
        // it keeps DDL atomic when Postgres allows it.
        let tx = conn.transaction().await.map_err(map_db_error)?;
        tx.batch_execute(sql).await.map_err(map_db_error)?;
        tx.execute(
            "INSERT INTO _taskq_migrations (filename) VALUES ($1)",
            &[filename],
        )
        .await
        .map_err(map_db_error)?;
        tx.commit().await.map_err(map_db_error)?;
    }

    Ok(())
}

/// Read-only convenience: returns the bundled migration filenames in apply
/// order. Useful for diagnostics and tests.
pub fn bundled_migration_names() -> Vec<&'static str> {
    MIGRATIONS.iter().map(|(name, _)| *name).collect()
}

/// Compile-time-evaluated guard that panics if the [`MIGRATIONS`] slice is
/// not sorted by filename. Exposed (rather than hidden behind an unused
/// const) so dead-code lints don't have to be suppressed.
pub const MIGRATIONS_SORTED: () = {
    let mut i = 1;
    while i < MIGRATIONS.len() {
        let a = MIGRATIONS[i - 1].0.as_bytes();
        let b = MIGRATIONS[i].0.as_bytes();
        let mut j = 0;
        let cmp = loop {
            if j == a.len() && j == b.len() {
                break 0i32;
            }
            if j == a.len() {
                break -1;
            }
            if j == b.len() {
                break 1;
            }
            if a[j] < b[j] {
                break -1;
            }
            if a[j] > b[j] {
                break 1;
            }
            j += 1;
        };
        if cmp >= 0 {
            panic!("MIGRATIONS slice must be sorted by filename");
        }
        i += 1;
    }
};
