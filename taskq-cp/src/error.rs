//! Control-plane error type.
//!
//! `CpError` is the unified error returned by `taskq-cp` machinery (config
//! loading, observability init, server bootstrap, health-server bootstrap).
//!
//! Storage errors map by category:
//!
//! - `StorageError::SerializationConflict` — handled inside the handler with a
//!   transparent retry loop (see `design.md` §6.4) and never surfaced via
//!   `CpError`. If a logical operation exhausts retries, that is mapped to
//!   `Storage(_)` so the caller sees a generic transient `Internal`.
//! - `StorageError::NotFound` — caller-context-dependent. `complete_task`
//!   maps it to `LEASE_EXPIRED`; `get_namespace_quota` maps it to
//!   "namespace not provisioned". The handler decides; this enum is the
//!   transport surface.
//! - `StorageError::ConstraintViolation` / `BackendError` — wrapped here and
//!   surfaced as `Internal` to the gRPC caller (Phase 5b will plumb the
//!   gRPC mapping).

use taskq_storage::StorageError;

/// Error type used by the CP bootstrap path. Handler-level errors that need
/// gRPC `Status` mapping are constructed by Phase 5b inside the handler
/// modules and never travel up the `CpError` path.
#[derive(Debug, thiserror::Error)]
pub enum CpError {
    /// A storage operation failed at startup (migration check, namespace
    /// loading, readiness probe). Routed through `StorageError` so the CP
    /// can pattern-match without losing the original category.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    /// Configuration loading or validation failed. Carries a string so we
    /// can build messages from `toml::de::Error`, `clap` parse errors, or
    /// `SetNamespaceQuota`-style validation failures uniformly.
    #[error("configuration error: {0}")]
    Config(String),

    /// Transport / I/O error from the gRPC or health server (TCP bind,
    /// hyper accept loop, etc.).
    #[error("transport error: {0}")]
    Transport(String),

    /// OpenTelemetry pipeline init or shutdown failed.
    #[error("observability error: {0}")]
    Observability(String),

    /// Schema-version mismatch between this binary and `taskq_meta` in the
    /// storage backend. Surfaced at startup before serving any traffic per
    /// `design.md` §12.5.
    #[error("schema version mismatch: binary expects {expected}, storage has {found}")]
    SchemaVersionMismatch { expected: u32, found: u32 },

    /// Catch-all for unexpected startup failures. Use sparingly; prefer one
    /// of the specific variants above.
    #[error("internal: {0}")]
    Internal(String),
}

impl CpError {
    /// Convenience constructor for `Config(_)` from any displayable error.
    pub fn config<E: std::fmt::Display>(err: E) -> Self {
        Self::Config(err.to_string())
    }

    /// Convenience constructor for `Transport(_)` from any displayable error.
    pub fn transport<E: std::fmt::Display>(err: E) -> Self {
        Self::Transport(err.to_string())
    }

    /// Convenience constructor for `Observability(_)` from any displayable error.
    pub fn observability<E: std::fmt::Display>(err: E) -> Self {
        Self::Observability(err.to_string())
    }
}

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, CpError>;
