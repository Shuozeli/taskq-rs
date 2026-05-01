//! `taskq-cp` -- taskq-rs control-plane gRPC server binary.
//!
//! Phase 5a startup sequence (`tasks.md` §5.x):
//!
//! 1. Parse CLI args (`clap`).
//! 2. Load `CpConfig`.
//! 3. Init the OTel pipeline.
//! 4. Build the storage handle from config.
//! 5. Run migrations iff `--auto-migrate` is passed; otherwise check
//!    schema-version match and refuse to start with a clear error
//!    (`design.md` §12.5).
//! 6. Load the strategy registry from `namespace_quota`.
//! 7. Build `CpState`.
//! 8. Spawn the health server (separate port).
//! 9. Spawn the shutdown listener.
//! 10. Spawn the gRPC server.
//! 11. Wait for shutdown.
//! 12. Drain (graceful).
//! 13. Shut down OTel.
//!
//! Phase 5b/5c/5d fill in the parts that depend on having handler bodies,
//! reaper tasks, and live OTel emit sites.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand};

use taskq_cp::config::{CpConfig, StorageBackendConfig};
use taskq_cp::error::CpError;
use taskq_cp::health::HealthState;
use taskq_cp::state::{CpState, DynStorage};
use taskq_cp::strategy::StrategyRegistry;
use taskq_cp::{health, observability, reapers, server, shutdown};

/// Schema version this binary expects to find in `taskq_meta.schema_version`.
///
/// Bumped any time a migration is added under `taskq-storage-{postgres,sqlite}/migrations/`
/// and corresponds to the highest migration the bundled binary knows how to
/// apply. `design.md` §12.5: production CPs run `taskq-cp migrate` deliberately;
/// `serve` refuses to start when the storage backend's recorded version does
/// not match this constant unless `--auto-migrate` is passed.
const SCHEMA_VERSION: u32 = 2;

/// Command-line interface for `taskq-cp`.
///
/// Default behaviour (no subcommand): `serve`. The `migrate` subcommand
/// applies migrations against the configured backend and exits.
#[derive(Debug, Parser)]
#[command(
    name = "taskq-cp",
    version,
    about = "taskq-rs control-plane gRPC server"
)]
struct Cli {
    /// Path to the TOML config file. Required for both `serve` and
    /// `migrate` so the binary always knows which backend to talk to.
    #[arg(long, value_name = "PATH", global = true)]
    config: Option<PathBuf>,

    /// Optional subcommand. Default behaviour (no subcommand) is `serve`,
    /// matching the previous Phase 5a CLI shape.
    #[command(subcommand)]
    command: Option<Command>,

    /// `serve`-only: if set, run migrations against the configured storage
    /// backend before serving traffic. Otherwise the binary refuses to
    /// start when `taskq_meta.schema_version` does not match this binary's
    /// expected version (`design.md` §12.5: production CPs must run
    /// `taskq-cp migrate` explicitly; `--auto-migrate` is a development
    /// convenience).
    ///
    /// Top-level (rather than nested under `serve`) so `taskq-cp
    /// --auto-migrate --config <path>` keeps working without callers having
    /// to type `serve` explicitly.
    #[arg(long, default_value_t = false)]
    auto_migrate: bool,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the gRPC server. Default if no subcommand is provided.
    Serve,
    /// Apply schema migrations against the configured storage backend and
    /// exit. Operators run this deliberately during deploys (`design.md`
    /// §12.5); the `--auto-migrate` flag on `serve` is the dev shortcut.
    Migrate,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Tracing subscriber up first so config-loading errors get logged.
    install_tracing();

    let cli = Cli::parse();
    let config_path = cli
        .config
        .clone()
        .context("--config <PATH> is required (set the path to the TOML config)")?;
    let config = CpConfig::load_from_file(&config_path)
        .with_context(|| format!("loading config from {}", config_path.display()))?;
    tracing::info!(?config_path, "configuration loaded");

    match cli.command.unwrap_or(Command::Serve) {
        Command::Migrate => run_migrate(&config).await,
        Command::Serve => run_serve(config, cli.auto_migrate).await,
    }
}

/// `migrate` subcommand: apply migrations and exit. Does not start the OTel
/// pipeline or the gRPC server -- this is the operator's deliberate
/// schema-evolution action.
async fn run_migrate(config: &CpConfig) -> anyhow::Result<()> {
    apply_migrations(&config.storage_backend)
        .await
        .context("running migrations")?;
    tracing::info!("migrations applied; exiting");
    Ok(())
}

/// `serve` subcommand: full startup. Runs migrations first iff
/// `--auto-migrate` was passed, then performs the schema-version compatibility
/// check, then proceeds with the rest of Phase 5a's startup sequence.
async fn run_serve(config: CpConfig, auto_migrate: bool) -> anyhow::Result<()> {
    // OTel pipeline before storage init so storage operations can record
    // their first metrics if Phase 5d wires them.
    let observability_state = observability::init(&config.otel_exporter)
        .map_err(anyhow::Error::from)
        .context("initializing observability pipeline")?;

    if auto_migrate {
        tracing::warn!(
            "--auto-migrate is set; running migrations against the configured backend (dev path)"
        );
        apply_migrations(&config.storage_backend)
            .await
            .context("running --auto-migrate migrations")?;
    }

    // Schema-version compatibility gate (`design.md` §12.5). Done after
    // any --auto-migrate so the post-migration version is what gets
    // checked. Fails fast before serving traffic on drift.
    check_schema_version(&config.storage_backend)
        .await
        .context("schema-version compatibility check")?;

    let storage = build_storage(&config.storage_backend)
        .await
        .context("building storage handle")?;

    // Strategy registry -- Phase 5b populates this from the namespace_quota
    // table. Phase 5a starts empty so the rest of the pipeline can wire up.
    let strategy_registry = Arc::new(StrategyRegistry::empty());

    // Shutdown plumbing: a watch channel that every long-running task
    // listens on.
    let (shutdown_tx, shutdown_rx) = shutdown::channel();

    let cp_state = Arc::new(CpState::new(
        Arc::clone(&storage),
        Arc::clone(&strategy_registry),
        observability_state.metrics.clone(),
        shutdown_rx.clone(),
        Arc::new(config),
    ));

    // Spawn the health server on its own task.
    let health_state = HealthState {
        cp: Arc::clone(&cp_state),
        prometheus_registry: observability_state.prometheus_registry.clone(),
    };
    let health_shutdown = shutdown_rx.clone();
    let health_handle =
        tokio::spawn(async move { health::serve(health_state, health_shutdown).await });

    // Spawn the OS-signal listener that flips the shutdown channel.
    let signal_shutdown_tx = shutdown_tx.clone();
    let signal_handle = tokio::spawn(async move {
        shutdown::wait_for_signal().await;
        let _ = signal_shutdown_tx.send(true);
    });

    // Spawn the Phase 5c reapers. They each tick on their own cadence and
    // exit on shutdown; the gRPC server is the runtime's blocking root, so
    // the reapers' join handles are awaited below after the server returns.
    let (reaper_a_handle, reaper_b_handle) =
        reapers::spawn_reapers(Arc::clone(&cp_state), shutdown_rx.clone());

    // Run the gRPC server. Blocks until shutdown fires.
    let serve_result = server::serve(Arc::clone(&cp_state), shutdown_rx).await;
    if let Err(err) = &serve_result {
        tracing::error!(error = %err, "gRPC server exited with error");
    }

    // Make sure the signal listener exits cleanly even if the gRPC server
    // returned for non-signal reasons (e.g. bind failure).
    let _ = shutdown_tx.send(true);
    if let Err(err) = signal_handle.await {
        tracing::warn!(error = %err, "signal listener task panicked");
    }
    if let Err(err) = health_handle.await {
        tracing::warn!(error = %err, "health server task panicked");
    }
    if let Err(err) = reaper_a_handle.await {
        tracing::warn!(error = %err, "reaper-a task panicked");
    }
    if let Err(err) = reaper_b_handle.await {
        tracing::warn!(error = %err, "reaper-b task panicked");
    }

    observability_state.shutdown();

    serve_result.map_err(anyhow::Error::from)
}

/// Run the bundled migrations for the configured backend. Each backend
/// owns its own migration runner; this just dispatches.
async fn apply_migrations(backend: &StorageBackendConfig) -> Result<(), CpError> {
    match backend {
        StorageBackendConfig::Postgres { url, pool_size } => {
            // The Postgres migration runner takes a `deadpool_postgres::Pool`.
            // We connect a transient `PostgresStorage` to acquire the pool;
            // it is dropped at the end of the migration.
            let pg_config = taskq_storage_postgres::PostgresConfig {
                conn_str: url.clone(),
                pool_size: *pool_size,
            };
            let storage = taskq_storage_postgres::PostgresStorage::connect(pg_config)
                .await
                .map_err(CpError::Storage)?;
            taskq_storage_postgres::migrate(storage.pool())
                .await
                .map_err(CpError::Storage)?;
            tracing::info!(backend = "postgres", "migrations applied");
            Ok(())
        }
        StorageBackendConfig::Sqlite { path } => {
            // The SQLite migration runner takes a `&mut rusqlite::Connection`.
            // `SqliteStorage::open*` already runs migrations on construction,
            // so we just open / drop a transient handle. This keeps the
            // entry point uniform across backends without exposing
            // rusqlite's internal `Connection` from the storage crate.
            if path == ":memory:" {
                taskq_storage_sqlite::SqliteStorage::open_in_memory()
                    .await
                    .map_err(CpError::Storage)?;
            } else {
                taskq_storage_sqlite::SqliteStorage::open(path)
                    .await
                    .map_err(CpError::Storage)?;
            }
            tracing::info!(backend = "sqlite", "migrations applied");
            Ok(())
        }
    }
}

/// Read `taskq_meta.schema_version` and compare to the binary's
/// [`SCHEMA_VERSION`]. Refuse to start on drift with a message pointing at
/// `taskq-cp migrate` (`design.md` §12.5).
async fn check_schema_version(backend: &StorageBackendConfig) -> Result<(), CpError> {
    let found = read_schema_version(backend).await?;
    if found != SCHEMA_VERSION {
        return Err(CpError::SchemaVersionMismatch {
            expected: SCHEMA_VERSION,
            found,
        });
    }
    Ok(())
}

/// Backend-specific `taskq_meta.schema_version` read. Returns the value as
/// `u32` so the comparison against [`SCHEMA_VERSION`] is type-aligned.
async fn read_schema_version(backend: &StorageBackendConfig) -> Result<u32, CpError> {
    match backend {
        StorageBackendConfig::Postgres { url, pool_size } => {
            let pg_config = taskq_storage_postgres::PostgresConfig {
                conn_str: url.clone(),
                pool_size: *pool_size,
            };
            let storage = taskq_storage_postgres::PostgresStorage::connect(pg_config)
                .await
                .map_err(CpError::Storage)?;
            let conn = storage
                .pool()
                .get()
                .await
                .map_err(|e| CpError::Storage(taskq_storage::StorageError::backend(e)))?;
            let row = conn
                .query_one(
                    "SELECT schema_version FROM taskq_meta WHERE only_row = TRUE",
                    &[],
                )
                .await
                .map_err(|e| CpError::Storage(taskq_storage::StorageError::backend(e)))?;
            let version: i32 = row.get(0);
            // schema_version is non-negative by construction; cast checked.
            u32::try_from(version).map_err(|_| {
                CpError::Internal(format!(
                    "taskq_meta.schema_version is negative: {version} (corrupt schema?)"
                ))
            })
        }
        StorageBackendConfig::Sqlite { path } => {
            // SqliteStorage::open* runs migrations on connect, so we go
            // through the lower-level `migrate::current_schema_version`
            // helper instead -- it reads the row without performing
            // additional migrations.
            let connection = if path == ":memory:" {
                rusqlite::Connection::open_in_memory()
                    .map_err(|e| CpError::Storage(taskq_storage::StorageError::backend(e)))?
            } else {
                rusqlite::Connection::open(path)
                    .map_err(|e| CpError::Storage(taskq_storage::StorageError::backend(e)))?
            };
            let version =
                taskq_storage_sqlite::migrate::current_schema_version(&connection).await?;
            // SQLite stores schema_version as i64; clamp to u32 for the
            // comparison below.
            u32::try_from(version).map_err(|_| {
                CpError::Internal(format!(
                    "taskq_meta.schema_version is out of u32 range: {version}"
                ))
            })
        }
    }
}

/// Build the configured `Storage` backend wrapped as `Arc<dyn DynStorage>`.
async fn build_storage(backend: &StorageBackendConfig) -> Result<Arc<dyn DynStorage>, CpError> {
    match backend {
        StorageBackendConfig::Postgres { url, pool_size } => {
            let pg_config = taskq_storage_postgres::PostgresConfig {
                conn_str: url.clone(),
                pool_size: *pool_size,
            };
            let storage = taskq_storage_postgres::PostgresStorage::connect(pg_config)
                .await
                .map_err(CpError::Storage)?;
            // PostgresStorage::connect already returns Arc<PostgresStorage>;
            // unwrap that and re-wrap as Arc<dyn DynStorage>.
            let dyn_storage: Arc<dyn DynStorage> = storage;
            Ok(dyn_storage)
        }
        StorageBackendConfig::Sqlite { path } => {
            let storage = if path == ":memory:" {
                taskq_storage_sqlite::SqliteStorage::open_in_memory()
                    .await
                    .map_err(CpError::Storage)?
            } else {
                taskq_storage_sqlite::SqliteStorage::open(path)
                    .await
                    .map_err(CpError::Storage)?
            };
            Ok(Arc::new(storage))
        }
    }
}

/// Install a basic `tracing` subscriber. Phase 5d will replace this with
/// the `tracing-opentelemetry` layered subscriber once the OTel pipeline
/// pieces are fully wired.
fn install_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("taskq_cp=info"));
    let subscriber = fmt::Subscriber::builder()
        .with_env_filter(filter)
        .with_target(true)
        .finish();
    if tracing::subscriber::set_global_default(subscriber).is_err() {
        // Already installed by a parent runtime -- fine.
    }
}
