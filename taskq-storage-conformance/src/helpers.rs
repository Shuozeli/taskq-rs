//! Shared helpers for the conformance modules.
//!
//! The fixtures here are deliberately tiny — every test gets an isolated
//! backend from the caller's `setup` closure, so helpers focus on building
//! `NewTask` / `NewDedupRecord` payloads with reasonable defaults rather
//! than on persisting state across calls.

use bytes::Bytes;
use taskq_storage::ids::{IdempotencyKey, Namespace, TaskId, TaskType, Timestamp};
use taskq_storage::types::{NewDedupRecord, NewTask};

/// Wall-clock reference timestamp used by every conformance test that
/// needs deterministic time. Tests offset from this rather than from
/// `SystemTime::now` so the fixtures stay deterministic.
///
/// This is intentionally **far in the future** (≈ year 2099) so the
/// SQLite backend's `lookup_idempotency` — which filters by
/// `expires_at` against the current wall clock — still considers fixture
/// rows live regardless of when the suite is executed. Any deterministic
/// timestamp in the past would cause real-time-driven filters to skip
/// the row.
pub const T0_MS: i64 = 4_070_908_800_000;

/// Default expires_at offset: 1 hour from the chosen reference time. Long
/// enough that no test trips dedup or task expiry.
pub const ONE_HOUR_MS: i64 = 3_600_000;

/// Build a `NewTask` + matching `NewDedupRecord` pair with sensible defaults.
///
/// Keeps the per-test boilerplate down: callers override only the fields
/// the test cares about (priority, submitted_at, etc.) by mutating the
/// returned values.
pub fn make_task(
    task_id: TaskId,
    namespace: &str,
    task_type: &str,
    key: &str,
    payload_hash: [u8; 32],
) -> (NewTask, NewDedupRecord) {
    let submitted_at = Timestamp::from_unix_millis(T0_MS);
    let expires_at = Timestamp::from_unix_millis(T0_MS + ONE_HOUR_MS);
    let task = NewTask {
        task_id,
        namespace: Namespace::new(namespace),
        task_type: TaskType::new(task_type),
        priority: 0,
        payload: Bytes::from_static(b"conformance"),
        payload_hash,
        submitted_at,
        expires_at,
        max_retries: 0,
        retry_initial_ms: 1_000,
        retry_max_ms: 1_000,
        retry_coefficient: 1.0,
        traceparent: Bytes::new(),
        tracestate: Bytes::new(),
        format_version: 1,
    };
    let dedup = NewDedupRecord {
        namespace: Namespace::new(namespace),
        key: IdempotencyKey::new(key),
        payload_hash,
        expires_at,
    };
    (task, dedup)
}
