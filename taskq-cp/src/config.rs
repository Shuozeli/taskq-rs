//! Control-plane configuration.
//!
//! Loads a TOML file from `--config <path>`, with environment-variable
//! overrides under the `TASKQ_*` prefix. Required fields (`storage_backend`,
//! `bind_addr`) have no defaults — missing them is a startup error per
//! `code-standards.md` ("Don't use any default config value. If the config
//! is missed in .env, just fail the server.").
//!
//! Optional operational tunables (long-poll timeouts, waiter caps, lease
//! window, OTel exporter choice) DO have defaults from `design.md` and
//! `tasks.md` Phase 5 numbers — these are not "config values" in the
//! `code-standards.md` sense but operational invariants the protocol commits
//! to.

use std::net::SocketAddr;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{CpError, Result};

/// Top-level CP configuration. Loaded from TOML; serialized to TOML by tests.
///
/// Field ordering follows the priority of operator concerns: where am I
/// running, what storage am I talking to, what does observability look like,
/// then operational knobs.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CpConfig {
    /// gRPC bind address. Required.
    ///
    /// Per global rule #8 (Tailscale binding) the CLI prefers reading the
    /// `TAILSCALE_IP` env var when available; the file is the fallback for
    /// non-Tailscale deployments. Set to `0.0.0.0:50051` for plain local
    /// dev when `TAILSCALE_IP` is unset.
    pub bind_addr: SocketAddr,

    /// Health / metrics HTTP bind address. Optional; defaults to
    /// `0.0.0.0:9090`. Always a separate port from `bind_addr` so cluster
    /// liveness checks do not interleave with gRPC traffic.
    #[serde(default = "default_health_addr")]
    pub health_addr: SocketAddr,

    /// Storage backend selection. Required.
    pub storage_backend: StorageBackendConfig,

    /// OpenTelemetry exporter selection. Defaults to `disabled` so unit
    /// tests of `CpConfig` itself do not need a live OTel collector.
    /// Production deployments must set this explicitly.
    #[serde(default)]
    pub otel_exporter: OtelExporterConfig,

    /// Rate-quota cache TTL in seconds. `design.md` §1.1 carve-out, §9.1.
    #[serde(default = "default_quota_cache_ttl_seconds")]
    pub quota_cache_ttl_seconds: u32,

    /// Default long-poll timeout for `AcquireTask` in seconds. `design.md`
    /// §6.2 default 30s.
    #[serde(default = "default_long_poll_timeout_seconds")]
    pub long_poll_default_timeout_seconds: u32,

    /// Hard cap on long-poll timeout. `design.md` §6.2 caps at 60s.
    #[serde(default = "default_long_poll_max_seconds")]
    pub long_poll_max_seconds: u32,

    /// "Belt-and-suspenders" unconditional re-query cadence inside every
    /// long-poll waiter. `design.md` §6.2 step 4b: 10s.
    #[serde(default = "default_belt_and_suspenders_seconds")]
    pub belt_and_suspenders_seconds: u32,

    /// Per-replica long-poll waiter cap. `design.md` §6.2 / §10.1: 5000.
    /// Namespace-wide cap is deferred to v2.
    #[serde(default = "default_waiter_limit_per_replica")]
    pub waiter_limit_per_replica: u32,

    /// Default lease window for `AcquireTask` in seconds. Per `design.md`
    /// §6.3, namespace can override; this is the seed default.
    #[serde(default = "default_lease_window_seconds")]
    pub lease_window_seconds: u32,
}

/// Required storage backend configuration. The `kind` discriminant determines
/// which sub-struct to deserialize.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum StorageBackendConfig {
    /// Postgres backend (`taskq-storage-postgres`).
    Postgres {
        /// libpq-style connection string.
        url: String,
        /// Transaction-pool size. Defaults to 16, matching the
        /// `PostgresConfig::new` default.
        #[serde(default = "default_postgres_pool_size")]
        pool_size: usize,
    },
    /// SQLite backend (`taskq-storage-sqlite`). Embedded / dev only.
    Sqlite {
        /// On-disk path. Use `:memory:` for an in-memory database (tests).
        path: String,
    },
}

/// OpenTelemetry exporter configuration. `design.md` §11.1 lists the four
/// built-in shapes.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum OtelExporterConfig {
    /// OTLP push to a remote collector.
    Otlp { endpoint: String },
    /// Prometheus pull endpoint exposed on the health server.
    Prometheus,
    /// Stdout exporter for debug.
    Stdout,
    /// No exporter — metric registrations and spans become no-ops.
    #[default]
    Disabled,
}

// ---------------------------------------------------------------------------
// Default helpers
// ---------------------------------------------------------------------------

fn default_health_addr() -> SocketAddr {
    "0.0.0.0:9090"
        .parse()
        .expect("static default health_addr must parse")
}

fn default_quota_cache_ttl_seconds() -> u32 {
    5
}

fn default_long_poll_timeout_seconds() -> u32 {
    30
}

fn default_long_poll_max_seconds() -> u32 {
    60
}

fn default_belt_and_suspenders_seconds() -> u32 {
    10
}

fn default_waiter_limit_per_replica() -> u32 {
    5000
}

fn default_lease_window_seconds() -> u32 {
    30
}

fn default_postgres_pool_size() -> usize {
    16
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

impl CpConfig {
    /// Load the configuration from a TOML file at `path`.
    ///
    /// Environment-variable overrides are applied after the file is parsed:
    ///
    /// - `TAILSCALE_IP` overrides `bind_addr` host iff set (per global rule
    ///   #8). The port is taken from the file value.
    /// - `TASKQ_BIND_ADDR` overrides `bind_addr` outright (host:port).
    /// - `TASKQ_HEALTH_ADDR` overrides `health_addr`.
    ///
    /// The override surface is intentionally narrow in Phase 5a — anything
    /// more complex (per-quota knobs etc.) belongs in the TOML body where it
    /// is auditable.
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = std::fs::read_to_string(&path).map_err(|err| {
            CpError::config(format!(
                "failed to read config file at {}: {err}",
                path.as_ref().display()
            ))
        })?;
        let mut cfg: CpConfig = toml::from_str(&contents).map_err(CpError::config)?;
        cfg.apply_env_overrides()?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Apply env-var overrides in place. See `load_from_file` for the list.
    fn apply_env_overrides(&mut self) -> Result<()> {
        // TASKQ_BIND_ADDR takes precedence; otherwise apply TAILSCALE_IP if
        // present (rebinding the host only, preserving the port from file).
        if let Ok(value) = std::env::var("TASKQ_BIND_ADDR") {
            self.bind_addr = value.parse().map_err(|err| {
                CpError::config(format!(
                    "TASKQ_BIND_ADDR env override is not a valid SocketAddr: {err}"
                ))
            })?;
        } else if let Ok(host) = std::env::var("TAILSCALE_IP") {
            // Tailscale IPs are bare addresses, no port. Combine with the
            // port already in the file's bind_addr so operators only set
            // TAILSCALE_IP once globally.
            let combined = format!("{host}:{}", self.bind_addr.port());
            self.bind_addr = combined.parse().map_err(|err| {
                CpError::config(format!(
                    "TAILSCALE_IP={host} could not be combined with port to form a valid SocketAddr: {err}"
                ))
            })?;
        }
        if let Ok(value) = std::env::var("TASKQ_HEALTH_ADDR") {
            self.health_addr = value.parse().map_err(|err| {
                CpError::config(format!(
                    "TASKQ_HEALTH_ADDR env override is not a valid SocketAddr: {err}"
                ))
            })?;
        }
        Ok(())
    }

    /// Validate cross-field invariants that TOML deserialization cannot
    /// express by itself.
    fn validate(&self) -> Result<()> {
        if self.long_poll_default_timeout_seconds > self.long_poll_max_seconds {
            return Err(CpError::config(format!(
                "long_poll_default_timeout_seconds ({}) must not exceed long_poll_max_seconds ({})",
                self.long_poll_default_timeout_seconds, self.long_poll_max_seconds,
            )));
        }
        if self.belt_and_suspenders_seconds == 0 {
            return Err(CpError::config("belt_and_suspenders_seconds must be > 0"));
        }
        if self.waiter_limit_per_replica == 0 {
            return Err(CpError::config("waiter_limit_per_replica must be > 0"));
        }
        if self.lease_window_seconds == 0 {
            return Err(CpError::config("lease_window_seconds must be > 0"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper that builds a minimal valid TOML body for a SQLite + disabled
    /// OTel deployment. Tests parse this and tweak fields as needed.
    fn minimal_sqlite_toml() -> &'static str {
        r#"
            bind_addr = "127.0.0.1:50051"

            [storage_backend]
            kind = "sqlite"
            path = ":memory:"
        "#
    }

    #[test]
    fn loads_minimal_sqlite_config_with_defaults() {
        // Arrange
        let dir = tempdir_via_env();
        let path = dir.join("cp.toml");
        std::fs::write(&path, minimal_sqlite_toml()).unwrap();

        // Act
        let cfg = CpConfig::load_from_file(&path).unwrap();

        // Assert
        assert_eq!(cfg.bind_addr.port(), 50051);
        assert_eq!(cfg.health_addr.port(), 9090);
        assert_eq!(cfg.quota_cache_ttl_seconds, 5);
        assert_eq!(cfg.long_poll_default_timeout_seconds, 30);
        assert_eq!(cfg.long_poll_max_seconds, 60);
        assert_eq!(cfg.belt_and_suspenders_seconds, 10);
        assert_eq!(cfg.waiter_limit_per_replica, 5000);
        assert_eq!(cfg.lease_window_seconds, 30);
        match &cfg.storage_backend {
            StorageBackendConfig::Sqlite { path } => assert_eq!(path, ":memory:"),
            other => panic!("expected sqlite backend, got {other:?}"),
        }
        assert!(matches!(cfg.otel_exporter, OtelExporterConfig::Disabled));
    }

    #[test]
    fn missing_required_field_errors() {
        // Arrange: TOML missing the required bind_addr field.
        let dir = tempdir_via_env();
        let path = dir.join("cp.toml");
        std::fs::write(
            &path,
            r#"
                [storage_backend]
                kind = "sqlite"
                path = ":memory:"
            "#,
        )
        .unwrap();

        // Act
        let result = CpConfig::load_from_file(&path);

        // Assert
        assert!(matches!(result, Err(CpError::Config(_))));
    }

    #[test]
    fn validates_long_poll_timeout_relationship() {
        // Arrange: default 90 > max 60 must fail.
        let dir = tempdir_via_env();
        let path = dir.join("cp.toml");
        std::fs::write(
            &path,
            r#"
                bind_addr = "127.0.0.1:50051"
                long_poll_default_timeout_seconds = 90
                long_poll_max_seconds = 60

                [storage_backend]
                kind = "sqlite"
                path = ":memory:"
            "#,
        )
        .unwrap();

        // Act
        let result = CpConfig::load_from_file(&path);

        // Assert
        assert!(matches!(result, Err(CpError::Config(_))));
    }

    /// Build a unique scratch directory under `std::env::temp_dir()` without
    /// pulling in the `tempfile` crate. Tests using this clean up via
    /// `Drop` of the returned `TempDir` newtype.
    fn tempdir_via_env() -> std::path::PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("taskq-cp-cfg-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
