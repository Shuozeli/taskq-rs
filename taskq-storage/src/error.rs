//! Shared error model for storage backends.
//!
//! Per `problems/12-storage-abstraction.md` Open Question 3, taskq-rs uses a
//! single unified error enum with a small number of categories that the CP
//! can act on (transparent `40001` retry on `SerializationConflict`, surface
//! `LEASE_EXPIRED` for `NotFound` against a runtime row, etc.). Backend-
//! specific errors that don't fit a category are wrapped in `BackendError`.

use std::error::Error as StdError;

/// Boxed dynamic error used for backend-specific failures that the CP layer
/// does not need to distinguish.
pub type BoxedError = Box<dyn StdError + Send + Sync + 'static>;

/// Unified storage error. The CP layer pattern-matches on these variants to
/// decide retry vs. surface-to-caller.
///
/// See `design.md` §6.4 (`CompleteTask` / `ReportFailure` distinguish
/// `NotFound` from `SerializationConflict`).
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The backend reported a serializable-isolation conflict (Postgres
    /// `40001`, FoundationDB `commit_unknown_result`, CockroachDB SFU
    /// retry, etc.). The CP retries the surrounding transaction transparently
    /// up to a small bound before surfacing.
    #[error("storage transaction must be retried due to serialization conflict")]
    SerializationConflict,

    /// A constraint was violated (unique index, check constraint, foreign
    /// key). Used when a backend reports the violation in a way that maps
    /// cleanly to "the request is invalid against current state" and not
    /// "retry the transaction".
    #[error("storage constraint violation: {0}")]
    ConstraintViolation(String),

    /// The targeted row does not exist. For `complete_task` this maps to
    /// `LEASE_EXPIRED`. For `get_namespace_quota` this maps to "namespace
    /// has not been provisioned".
    #[error("requested storage record not found")]
    NotFound,

    /// Catch-all for backend-specific errors that don't map to the categories
    /// above. The wrapped error preserves the original backend message for
    /// logs.
    #[error("storage backend error: {0}")]
    BackendError(#[source] BoxedError),
}

impl StorageError {
    /// Convenience constructor for wrapping an arbitrary backend error.
    pub fn backend<E>(err: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::BackendError(Box::new(err))
    }

    /// Returns true for transient failures the CP should retry transparently.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::SerializationConflict)
    }
}

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, StorageError>;
