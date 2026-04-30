//! Conversion helpers between the `taskq-storage` trait types and the
//! SQLite-native row representation.
//!
//! Per `migrations/0001_initial.sql`:
//!
//! - UUIDs are stored as TEXT (hyphenated form via `Uuid::to_string`).
//! - Timestamps are stored as INTEGER (Unix epoch milliseconds) matching
//!   `taskq_storage::Timestamp::as_unix_millis`.
//! - Status / outcome enums are stored as TEXT with CHECK constraints.

use rusqlite::Error as SqlError;
use uuid::Uuid;

use taskq_storage::{
    error::StorageError,
    ids::{TaskId, Timestamp, WorkerId},
    types::{CapacityKind, RateKind, TaskOutcome},
};

/// Wire `taskq-storage::Timestamp` to SQLite-native milliseconds.
pub fn ts_to_millis(ts: Timestamp) -> i64 {
    ts.as_unix_millis()
}

/// Reverse of [`ts_to_millis`].
pub fn millis_to_ts(ms: i64) -> Timestamp {
    Timestamp::from_unix_millis(ms)
}

/// Render a `TaskId` as the hyphenated UUID text used in the schema.
pub fn task_id_to_text(id: &TaskId) -> String {
    id.as_uuid().to_string()
}

/// Render a `WorkerId` as the hyphenated UUID text used in the schema.
pub fn worker_id_to_text(id: &WorkerId) -> String {
    id.as_uuid().to_string()
}

/// Parse a UUID-shaped string column into a `TaskId`.
pub fn parse_task_id(text: &str) -> Result<TaskId, StorageError> {
    let uuid = Uuid::parse_str(text).map_err(|err| {
        StorageError::ConstraintViolation(format!("invalid task_id text {text}: {err}"))
    })?;
    Ok(TaskId::from_uuid(uuid))
}

/// Parse a UUID-shaped string column into a `WorkerId`.
pub fn parse_worker_id(text: &str) -> Result<WorkerId, StorageError> {
    let uuid = Uuid::parse_str(text).map_err(|err| {
        StorageError::ConstraintViolation(format!("invalid worker_id text {text}: {err}"))
    })?;
    Ok(WorkerId::from_uuid(uuid))
}

/// Map a rusqlite error to the trait-shape `StorageError` variant.
///
/// `Error::SqliteFailure(code = SQLITE_CONSTRAINT, ...)` becomes
/// `ConstraintViolation`; `Error::QueryReturnedNoRows` becomes `NotFound`.
/// SQLite has no SERIALIZABLE-conflict signal under single-writer scope, so
/// `SerializationConflict` is never returned.
pub fn map_sql_error(err: SqlError) -> StorageError {
    match &err {
        SqlError::QueryReturnedNoRows => StorageError::NotFound,
        SqlError::SqliteFailure(code, msg) => {
            if matches!(code.code, rusqlite::ErrorCode::ConstraintViolation) {
                let detail = msg.clone().unwrap_or_else(|| "constraint violation".into());
                StorageError::ConstraintViolation(detail)
            } else {
                StorageError::backend(err)
            }
        }
        _ => StorageError::backend(err),
    }
}

/// Status string written to `tasks.status`.
pub fn status_pending() -> &'static str {
    "PENDING"
}

/// Status string written to `tasks.status` after acquisition.
pub fn status_dispatched() -> &'static str {
    "DISPATCHED"
}

/// Outcome string + terminal-status string for the `tasks` row.
///
/// Returns `(outcome_text, task_status_text, error_class, error_message,
/// error_details, retry_after_ms, recorded_at_ms)`. The CP layer pre-computes
/// the terminal mapping per `design.md` §6.5; this helper just serializes
/// the discriminant and shared fields.
pub struct OutcomeRow<'a> {
    pub outcome: &'static str,
    pub task_status: &'static str,
    pub error_class: Option<&'a str>,
    pub error_message: Option<&'a str>,
    pub error_details: Option<&'a [u8]>,
    pub result_payload: Option<&'a [u8]>,
    pub retry_after: Option<Timestamp>,
    pub recorded_at: Timestamp,
}

impl<'a> OutcomeRow<'a> {
    /// Project the trait-side `TaskOutcome` into the row shape used by
    /// `task_results` + `tasks.status` updates.
    pub fn from_outcome(outcome: &'a TaskOutcome) -> Self {
        match outcome {
            TaskOutcome::Success {
                result_payload,
                recorded_at,
            } => OutcomeRow {
                outcome: "SUCCESS",
                task_status: "COMPLETED",
                error_class: None,
                error_message: None,
                error_details: None,
                result_payload: Some(result_payload),
                retry_after: None,
                recorded_at: *recorded_at,
            },
            TaskOutcome::FailedNonretryable {
                error_class,
                error_message,
                error_details,
                recorded_at,
            } => OutcomeRow {
                outcome: "NONRETRYABLE_FAIL",
                task_status: "FAILED_NONRETRYABLE",
                error_class: Some(error_class.as_str()),
                error_message: Some(error_message.as_str()),
                error_details: Some(error_details),
                result_payload: None,
                retry_after: None,
                recorded_at: *recorded_at,
            },
            TaskOutcome::FailedExhausted {
                error_class,
                error_message,
                error_details,
                recorded_at,
            } => OutcomeRow {
                outcome: "RETRYABLE_FAIL",
                task_status: "FAILED_EXHAUSTED",
                error_class: Some(error_class.as_str()),
                error_message: Some(error_message.as_str()),
                error_details: Some(error_details),
                result_payload: None,
                retry_after: None,
                recorded_at: *recorded_at,
            },
            TaskOutcome::Expired {
                error_class,
                error_message,
                error_details,
                recorded_at,
            } => OutcomeRow {
                outcome: "EXPIRED",
                task_status: "EXPIRED",
                error_class: Some(error_class.as_str()),
                error_message: Some(error_message.as_str()),
                error_details: Some(error_details),
                result_payload: None,
                retry_after: None,
                recorded_at: *recorded_at,
            },
            TaskOutcome::WaitingRetry {
                error_class,
                error_message,
                error_details,
                retry_after,
                recorded_at,
            } => OutcomeRow {
                outcome: "RETRYABLE_FAIL",
                task_status: "WAITING_RETRY",
                error_class: Some(error_class.as_str()),
                error_message: Some(error_message.as_str()),
                error_details: Some(error_details),
                result_payload: None,
                retry_after: Some(*retry_after),
                recorded_at: *recorded_at,
            },
        }
    }
}

/// Map a `CapacityKind` to the corresponding column in `namespace_quota`.
pub fn capacity_kind_column(kind: CapacityKind) -> &'static str {
    match kind {
        CapacityKind::MaxPending => "max_pending",
        CapacityKind::MaxInflight => "max_inflight",
        CapacityKind::MaxWorkers => "max_workers",
        CapacityKind::MaxWaitersPerReplica => "max_waiters_per_replica",
    }
}

/// Filter for the live count corresponding to a capacity dimension.
pub fn capacity_kind_count_sql(kind: CapacityKind) -> &'static str {
    match kind {
        CapacityKind::MaxPending => {
            "SELECT COUNT(*) FROM tasks WHERE namespace = ?1 AND status = 'PENDING'"
        }
        CapacityKind::MaxInflight => {
            "SELECT COUNT(*) FROM tasks WHERE namespace = ?1 AND status = 'DISPATCHED'"
        }
        CapacityKind::MaxWorkers => {
            "SELECT COUNT(*) FROM worker_heartbeats WHERE namespace = ?1 AND declared_dead_at IS NULL"
        }
        // MaxWaitersPerReplica is per-replica in-process state — capacity
        // reads from storage return the configured limit only; the live
        // count is owned by the CP. We model that as "no rows in storage
        // contribute" by selecting 0 directly.
        CapacityKind::MaxWaitersPerReplica => "SELECT 0",
    }
}

/// Map a `RateKind` to its display name (used by error messages /
/// in-memory rate-bucket keys).
pub fn rate_kind_name(kind: RateKind) -> &'static str {
    match kind {
        RateKind::SubmitRpm => "submit_rpm",
        RateKind::DispatchRpm => "dispatch_rpm",
        RateKind::ReplayPerSecond => "replay_per_second",
    }
}
