//! Per-replica in-memory token-bucket rate limiter.
//!
//! Per `design.md` §1.1 carve-out, rate quotas are eventually consistent
//! within the namespace-config cache TTL. A single replica owns one token
//! bucket per `(Namespace, RateKind)` pair; the bucket replenishes
//! continuously based on the configured rate.
//!
//! The buckets are unmanaged here — they expand as new namespaces submit
//! tasks. v1 has tens of namespaces in scope so unbounded growth is a
//! non-issue; v1.1 / v2 may add LRU eviction.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use taskq_storage::ids::Namespace;
use taskq_storage::types::{RateDecision, RateKind};

/// Discriminant int used as the bucket-map key (`RateKind` is `Copy + Eq`
/// but not `Hash`, so we hash a `u8` instead).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RateKey(u8);

fn rate_key(kind: RateKind) -> RateKey {
    let v = match kind {
        RateKind::SubmitRpm => 0,
        RateKind::DispatchRpm => 1,
        RateKind::ReplayPerSecond => 2,
    };
    RateKey(v)
}

/// One bucket. Refills continuously based on `rate_per_second`.
#[derive(Debug)]
struct Bucket {
    /// Current token count (fractional so refill math doesn't lose granularity).
    tokens: f64,
    /// Absolute capacity. Bucket cannot exceed this even if it sat idle.
    capacity: f64,
    /// Tokens added per second.
    rate_per_second: f64,
    /// Last time we ran the refill calculation.
    last_refill: Instant,
}

impl Bucket {
    fn new(capacity: f64, rate_per_second: f64) -> Self {
        Self {
            tokens: capacity,
            capacity,
            rate_per_second,
            last_refill: Instant::now(),
        }
    }

    /// Refill based on elapsed wall clock since last access, capped at
    /// `capacity`.
    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        self.last_refill = now;
        let added = elapsed.as_secs_f64() * self.rate_per_second;
        self.tokens = (self.tokens + added).min(self.capacity);
    }

    /// Try to take `n` tokens. On success: tokens deducted and the post-take
    /// bucket level is returned. On failure: returns the smallest backoff
    /// that has a chance of refilling enough tokens.
    fn try_take(&mut self, n: u64) -> RateDecision {
        let n = n as f64;
        if self.tokens >= n {
            self.tokens -= n;
            return RateDecision::Allowed {
                remaining: self.tokens.floor() as u64,
            };
        }
        let deficit = n - self.tokens;
        let retry_secs = if self.rate_per_second > 0.0 {
            deficit / self.rate_per_second
        } else {
            // Rate of zero means "never replenish"; backoff is unbounded.
            // Surface a very large window rather than infinity.
            3600.0
        };
        let retry_after_ms = (retry_secs * 1000.0).ceil().max(1.0) as u64;
        RateDecision::RateLimited { retry_after_ms }
    }
}

/// Per-replica bucket map. Cheap to clone — the inner map is in a `Mutex`
/// inside an `Arc` upstream.
#[derive(Debug, Default)]
pub struct RateLimiter {
    inner: Mutex<HashMap<(Namespace, RateKey), Bucket>>,
}

impl RateLimiter {
    /// Construct an empty limiter. Buckets are added on first access via
    /// [`Self::try_consume`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Try to consume `n` tokens against `(namespace, kind)`. Bucket is
    /// instantiated on first sight using `default_per_minute` as both
    /// capacity and per-minute refill rate. Subsequent accesses keep the
    /// existing bucket; capacity changes are picked up only when the
    /// caller calls [`Self::reconfigure`].
    pub fn try_consume(
        &self,
        namespace: &Namespace,
        kind: RateKind,
        n: u64,
        default_per_minute: u64,
    ) -> RateDecision {
        let mut map = self.inner.lock().expect("rate limiter mutex poisoned");
        let now = Instant::now();
        let bucket = map
            .entry((namespace.clone(), rate_key(kind)))
            .or_insert_with(|| {
                Bucket::new(default_per_minute as f64, default_per_minute as f64 / 60.0)
            });
        bucket.refill(now);
        bucket.try_take(n)
    }

    /// Update the bucket parameters for `(namespace, kind)`. Used by the CP
    /// when `SetNamespaceQuota` lowers / raises rate. The old token count is
    /// preserved (clamped to the new capacity).
    pub fn reconfigure(
        &self,
        namespace: &Namespace,
        kind: RateKind,
        capacity: u64,
        rate_per_minute: u64,
    ) {
        let mut map = self.inner.lock().expect("rate limiter mutex poisoned");
        let bucket = map
            .entry((namespace.clone(), rate_key(kind)))
            .or_insert_with(|| Bucket::new(capacity as f64, rate_per_minute as f64 / 60.0));
        let now = Instant::now();
        bucket.refill(now);
        bucket.capacity = capacity as f64;
        bucket.rate_per_second = rate_per_minute as f64 / 60.0;
        bucket.tokens = bucket.tokens.min(bucket.capacity);
    }
}
