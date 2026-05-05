//! Transparent SERIALIZABLE-retry helper used by every state-transition
//! handler.
//!
//! `design.md` §6.4 / §6.6 commits to "transparent `40001` retry" so workers
//! observe `LEASE_EXPIRED` rather than opaque transient errors under
//! contention. This module provides one helper, [`with_serializable_retry`],
//! that wraps a closure returning a `Future<Result<T, StorageError>>`,
//! retries up to 3 times on [`StorageError::SerializationConflict`] with
//! 5/25/125 ms jittered backoff (per `design.md` §14 open question), and
//! emits the `taskq_storage_retry_attempts` histogram.
//!
//! Other [`StorageError`] variants (`NotFound`, `ConstraintViolation`,
//! `BackendError`) propagate immediately — the caller decides whether
//! `NotFound` becomes `LEASE_EXPIRED` (`CompleteTask` / `ReportFailure`),
//! "namespace not provisioned" (`get_namespace_quota`), etc.

use std::future::Future;
use std::time::Duration;

use opentelemetry::KeyValue;

use taskq_storage::StorageError;

use crate::observability::MetricsHandle;

/// Maximum number of attempts (initial + retries). `design.md` §14: 3
/// attempts with 5/25/125 ms backoff, then surface.
pub const MAX_ATTEMPTS: u32 = 3;

/// Backoff schedule for retry attempt N (1-indexed). Returns the centred
/// delay before the (N+1)-th attempt; the caller adds full ±50% jitter.
fn backoff_for_attempt(attempt: u32) -> Duration {
    // 5ms, 25ms, 125ms - roughly geometric so a thundering herd of conflicts
    // dilates fast without paying high latency for the common single-retry
    // case.
    match attempt {
        1 => Duration::from_millis(5),
        2 => Duration::from_millis(25),
        _ => Duration::from_millis(125),
    }
}

/// Add ±50% jitter to `base` deterministically derived from a small
/// randomness source. Uses rand-free arithmetic on the system clock so
/// this module does not pull in `rand` for one helper.
fn jitter(base: Duration, attempt: u32) -> Duration {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    // Fold attempt into the seed so concurrent retries on different attempts
    // don't synchronize.
    let seed = nanos.wrapping_add(attempt.wrapping_mul(0x9E37_79B9));
    // Map [0, 2^32) to [-0.5, 0.5).
    let frac = (seed as f64) / (u32::MAX as f64) - 0.5;
    let jittered = base.as_secs_f64() * (1.0 + frac); // ±50%
    Duration::from_secs_f64(jittered.max(0.0))
}

/// Run `op` up to [`MAX_ATTEMPTS`] times, retrying transparently on
/// [`StorageError::SerializationConflict`]. Records the attempt count to
/// `metrics.storage_retry_attempts` once the loop exits. Records a counter
/// hit on `storage_serialization_conflict_total` per retry.
///
/// `op_label` populates the metrics' `op` label (e.g. `"submit_task"`).
///
/// Other storage errors (and the `Ok` path) return immediately. The
/// `MAX_ATTEMPTS`-th `SerializationConflict` propagates so the gRPC layer
/// can surface a transient `Internal` to the worker SDK; the SDK then
/// retries at its own layer (per `design.md` §10.4).
pub async fn with_serializable_retry<T, F, Fut>(
    metrics: &MetricsHandle,
    op_label: &'static str,
    mut op: F,
) -> Result<T, StorageError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    let label = [KeyValue::new("op", op_label)];
    let mut attempt: u32 = 1;
    loop {
        match op().await {
            Ok(value) => {
                metrics
                    .storage_retry_attempts
                    .record(u64::from(attempt), &label);
                return Ok(value);
            }
            Err(StorageError::SerializationConflict) if attempt < MAX_ATTEMPTS => {
                metrics.storage_serialization_conflict_total.add(1, &label);
                let delay = jitter(backoff_for_attempt(attempt), attempt);
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            Err(err) => {
                metrics
                    .storage_retry_attempts
                    .record(u64::from(attempt), &label);
                return Err(err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn returns_ok_on_first_attempt_without_retry() {
        // Arrange
        let metrics = MetricsHandle::noop();
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = Arc::clone(&calls);

        // Act
        let result: Result<u32, StorageError> = with_serializable_retry(&metrics, "test", || {
            let calls = Arc::clone(&calls2);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            }
        })
        .await;

        // Assert
        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_on_serialization_conflict_and_succeeds() {
        // Arrange
        let metrics = MetricsHandle::noop();
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = Arc::clone(&calls);

        // Act
        let result: Result<&'static str, StorageError> =
            with_serializable_retry(&metrics, "test", || {
                let calls = Arc::clone(&calls2);
                async move {
                    let n = calls.fetch_add(1, Ordering::SeqCst);
                    if n < 1 {
                        Err(StorageError::SerializationConflict)
                    } else {
                        Ok("won")
                    }
                }
            })
            .await;

        // Assert
        assert_eq!(result.unwrap(), "won");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn surfaces_serialization_conflict_after_max_attempts() {
        // Arrange
        let metrics = MetricsHandle::noop();
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = Arc::clone(&calls);

        // Act
        let result: Result<(), StorageError> = with_serializable_retry(&metrics, "test", || {
            let calls = Arc::clone(&calls2);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(StorageError::SerializationConflict)
            }
        })
        .await;

        // Assert
        assert!(matches!(result, Err(StorageError::SerializationConflict)));
        assert_eq!(calls.load(Ordering::SeqCst), MAX_ATTEMPTS);
    }

    #[tokio::test]
    async fn propagates_non_retryable_errors_immediately() {
        // Arrange
        let metrics = MetricsHandle::noop();
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = Arc::clone(&calls);

        // Act
        let result: Result<(), StorageError> = with_serializable_retry(&metrics, "test", || {
            let calls = Arc::clone(&calls2);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(StorageError::NotFound)
            }
        })
        .await;

        // Assert
        assert!(matches!(result, Err(StorageError::NotFound)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
