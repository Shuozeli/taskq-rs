//! Map `tokio_postgres::Error` and `deadpool_postgres::PoolError` into the
//! shared [`taskq_storage::StorageError`] variants.

use taskq_storage::StorageError;

const SQLSTATE_SERIALIZATION_FAILURE: &str = "40001";
const SQLSTATE_DEADLOCK_DETECTED: &str = "40P01";
const SQLSTATE_UNIQUE_VIOLATION: &str = "23505";
const SQLSTATE_FOREIGN_KEY_VIOLATION: &str = "23503";
const SQLSTATE_CHECK_VIOLATION: &str = "23514";
const SQLSTATE_NOT_NULL_VIOLATION: &str = "23502";

/// Map a `tokio_postgres::Error` to a `StorageError`. SQLSTATE-aware so the
/// CP layer's transparent retry on `SerializationConflict` works
/// (`design.md` §6.4).
pub fn map_db_error(err: tokio_postgres::Error) -> StorageError {
    if let Some(state) = err.code() {
        match state.code() {
            SQLSTATE_SERIALIZATION_FAILURE | SQLSTATE_DEADLOCK_DETECTED => {
                return StorageError::SerializationConflict;
            }
            SQLSTATE_UNIQUE_VIOLATION
            | SQLSTATE_FOREIGN_KEY_VIOLATION
            | SQLSTATE_CHECK_VIOLATION
            | SQLSTATE_NOT_NULL_VIOLATION => {
                return StorageError::ConstraintViolation(err.to_string());
            }
            _ => {}
        }
    }
    // Surface the underlying SQLSTATE + message so operators can debug
    // backend errors without enabling tokio_postgres trace logging.
    tracing::warn!(error = %err, sqlstate = ?err.code(), "postgres backend error");
    StorageError::backend(err)
}

/// Map a `deadpool_postgres::PoolError` to a `StorageError`. Pool-acquisition
/// failures unwrap to the underlying `tokio_postgres::Error` when present.
pub fn map_pool_error(err: deadpool_postgres::PoolError) -> StorageError {
    use deadpool_postgres::PoolError as P;
    match err {
        P::Backend(inner) => map_db_error(inner),
        other => StorageError::backend(other),
    }
}
