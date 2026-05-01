//! `taskq-cp` — taskq-rs control-plane gRPC server binary.
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
use clap::Parser;

use taskq_cp::config::{CpConfig, StorageBackendConfig};
use taskq_cp::error::CpError;
use taskq_cp::health::HealthState;
use taskq_cp::state::{CpState, DynStorage};
use taskq_cp::strategy::StrategyRegistry;
use taskq_cp::{health, observability, server, shutdown};

/// Command-line interface for `taskq-cp`.
#[derive(Debug, Parser)]
#[command(
    name = "taskq-cp",
    version,
    about = "taskq-rs control-plane gRPC server"
)]
struct Cli {
    /// Path to the TOML config file. Required.
    #[arg(long, value_name = "PATH")]
    config: PathBuf,

    /// If set, run migrations against the configured storage backend before
    /// serving traffic. Otherwise the binary refuses to start when
    /// `taskq_meta.schema_version` does not match the binary's expected
    /// version (`design.md` §12.5: production CPs must run `taskq-cp
    /// migrate` explicitly; `--auto-migrate` is a development convenience).
    #[arg(long, default_value_t = false)]
    auto_migrate: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Tracing subscriber up first so config-loading errors get logged.
    install_tracing();

    let cli = Cli::parse();
    let config = CpConfig::load_from_file(&cli.config)
        .with_context(|| format!("loading config from {}", cli.config.display()))?;
    tracing::info!(?cli.config, "configuration loaded");

    // OTel pipeline before storage init so storage operations can record
    // their first metrics if Phase 5d wires them.
    let observability_state = observability::init(&config.otel_exporter)
        .map_err(anyhow::Error::from)
        .context("initializing observability pipeline")?;

    let storage = build_storage(&config.storage_backend)
        .await
        .context("building storage handle")?;

    if cli.auto_migrate {
        // TODO(phase-5b): run migrations for the configured backend.
        // For Postgres: taskq_storage_postgres::migrate(...).
        // For SQLite: SqliteStorage::open already runs migrations.
        // Phase 5b will route through the right helper based on the
        // backend variant.
        tracing::warn!("--auto-migrate passed; Phase 5b implements the migration runner");
    } else {
        // TODO(phase-5b): read taskq_meta.schema_version and compare to
        //                 the binary's expected schema_version; fail with
        //                 CpError::SchemaVersionMismatch on drift.
        tracing::debug!("schema-version check is implemented in Phase 5b");
    }

    // Strategy registry — Phase 5b populates this from the namespace_quota
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

    observability_state.shutdown();

    serve_result.map_err(anyhow::Error::from)
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
        // Already installed by a parent runtime — fine.
    }
}
