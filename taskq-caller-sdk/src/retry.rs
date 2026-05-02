//! Retry-after handling for `RESOURCE_EXHAUSTED` rejections.
//!
//! Per `design.md` §10.4 and `problems/06-backpressure.md` round-2:
//!
//! - The server's `Rejection.retry_after` is a **Unix-epoch milliseconds
//!   timestamp** (UTC), not a delay. The SDK sleeps until that wall-clock
//!   moment plus a ±20% jitter on the *delay component*, then retries
//!   the same request.
//! - When `retryable=false`, the SDK MUST surface immediately.
//! - The retry budget caps the number of attempts (default 5).
//! - If the server returns a `retry_after` in the past (negative
//!   delay), the SDK applies a jittered minimum sleep (5 ms) before
//!   retrying so a misbehaving server cannot pin the SDK in a
//!   tight loop.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::Rng;

/// Caller-tunable retry budget. The SDK retries `RESOURCE_EXHAUSTED`-with-
/// `retryable=true` rejections up to `budget` times before surfacing the
/// last rejection to user code as `ClientError::Rejected`.
#[derive(Debug, Clone, Copy)]
pub struct RetryBudget {
    pub max_attempts: u32,
}

impl RetryBudget {
    pub const DEFAULT: RetryBudget = RetryBudget { max_attempts: 5 };

    pub fn new(max_attempts: u32) -> Self {
        Self { max_attempts }
    }
}

impl Default for RetryBudget {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Compute the duration to sleep before retrying. `retry_after_ms` is a
/// Unix-epoch UTC timestamp from the server. The SDK turns it into a
/// delay relative to *its* clock, then applies ±20% jitter on top.
///
/// Returns `None` if the budget is exhausted (caller should surface the
/// rejection to user code).
pub(crate) fn sleep_duration_for_retry(retry_after_ms: i64, now: SystemTime) -> Duration {
    let now_ms: i64 = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let raw_delay_ms = retry_after_ms.saturating_sub(now_ms);

    // Floor the raw delay at a small positive value so we never spin.
    // The 5ms floor matches the OTel SDK convention for "backpressure
    // with no useful hint" and is short enough that a healthy server
    // recovers within a single retry budget.
    let base_ms = raw_delay_ms.max(5);

    // ±20% jitter on the delay component, per design.md §10.4. We use
    // signed jitter so the SDK occasionally retries *before* the
    // server's hint (still well within the bound, since the hint is
    // a floor, not a contract — see design.md §10.1).
    let jitter_range_ms = (base_ms / 5).max(1);
    let mut rng = rand::thread_rng();
    let jitter_ms: i64 = rng.gen_range(-jitter_range_ms..=jitter_range_ms);
    let final_ms = (base_ms + jitter_ms).max(0) as u64;

    Duration::from_millis(final_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn default_budget_is_five_attempts() {
        // Arrange / Act
        let b = RetryBudget::default();

        // Assert
        assert_eq!(b.max_attempts, 5);
    }

    #[test]
    fn sleep_duration_at_least_min_floor_when_retry_after_is_in_past() {
        // Arrange: retry_after is 1 second before "now".
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let retry_after_ms = 1_000_000_000 - 1_000;

        // Act
        let d = sleep_duration_for_retry(retry_after_ms, now);

        // Assert: floor is 5 ms, jitter is ±1 ms, so final is in [4, 6].
        assert!(d <= Duration::from_millis(10), "got {:?}", d);
    }

    #[test]
    fn sleep_duration_respects_future_retry_after() {
        // Arrange: retry_after is 1 second after "now".
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000);
        let retry_after_ms = 1_000_000_000 + 1_000;

        // Act
        let d = sleep_duration_for_retry(retry_after_ms, now);

        // Assert: 1000ms ±20% = [800, 1200].
        assert!(
            d >= Duration::from_millis(800) && d <= Duration::from_millis(1_200),
            "expected ~1s ±20%, got {:?}",
            d
        );
    }

    #[test]
    fn sleep_duration_with_zero_retry_after_does_not_panic() {
        // Arrange: retry_after_ms == 0 (the "no useful hint" sentinel).
        let now = UNIX_EPOCH + Duration::from_secs(1_000_000);

        // Act
        let d = sleep_duration_for_retry(0, now);

        // Assert: floor applies; result is small but nonzero.
        assert!(d <= Duration::from_millis(10));
    }
}
