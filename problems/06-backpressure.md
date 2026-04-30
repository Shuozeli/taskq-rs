# Problem: Backpressure at submit time

## The problem

Callers can submit faster than workers can drain. The system has to push back somewhere — either reject submissions, accept and queue them indefinitely (which postpones the failure to OOM), or apply some other strategy. Problem #05 already committed the *mechanism*: synchronous rejection via the `Admitter` trait, no implicit blocking, no async overflow queues. This doc resolves the *contract*: what the caller observes when rejected, and how they recover.

The mechanism without the contract creates retry storms. A bare `RESOURCE_EXHAUSTED` with no structure forces every caller to invent their own retry logic, and they amplify each other.

## Why it's hard

- **Retry amplification.** Without backoff guidance, callers retry immediately. Layered retries (caller's HTTP client → user's workflow → user's cron) compound exponentially.
- **Reason discrimination matters.** "Your namespace exceeded its quota" needs different recovery than "the system is overloaded" needs different recovery than "your idempotency key is already in flight." A generic 429 conflates these.
- **Transient vs. permanent rejections.** Some rejections will succeed if retried (`MAX_PENDING_EXCEEDED` once the queue drains). Others will not (`IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`). SDKs must be able to tell them apart.
- **Rejection itself has cost.** Each rejected submit takes a transaction in v1 (the admitter consults DB state). Under heavy backpressure, rejections become load on top of the load.
- **Submit-acceptance is not dispatch-acceptance.** A task admitted at submit time may still wait indefinitely if no worker handles its type or the worker pool starves. The contract must be honest about this.

## The races and failure modes

**1. Retry storms.** N callers all retry on `RESOURCE_EXHAUSTED` with no jitter. CP receives N retries simultaneously when the rejection lifts. Mitigation: structured `retry_after` hint + SDK-side jittered backoff.

**2. Layered retries.** Three layers of independent retry logic on a backpressured system multiply the load. Mitigation: protocol-level retry-after hints that propagate through SDKs; documentation telling callers to surface `retry_after` instead of swallowing it.

**3. Innocent-namespace collateral damage.** A noisy namespace's rejected submits still load the storage layer. Quiet namespaces' submits get slower because of the rejection traffic. Mitigation in v1: deferred. v2 can add a global admitter or in-memory pre-check.

**4. Cliff-edge admission.** A hard `max_pending` cutoff goes from 100% accept to 100% reject in one task. Probabilistic admission smooths this. v1 chooses cliff-edge for simplicity; operators wanting smoothing implement a custom admitter.

**5. Rejection cost cascade.** Under sufficiently heavy load, rejections themselves saturate the storage layer. Detected as: rejection latency growing alongside accept latency. Mitigation deferred to v2 (cheap pre-transaction check).

## What prior art does

| | Faktory | Hatchet | Temporal |
|---|---|---|---|
| Submit-time backpressure | None — PUSH always succeeds | Rate-limited queues (separate table, not rejected) | Namespace-level RPS, returns `RESOURCE_EXHAUSTED` |
| Retry-after hint | N/A | N/A | Yes — in error details |
| Reason discrimination | N/A | N/A | Mostly one reason — RPS exceeded |
| Caller-avoidance hints | N/A | N/A | None |
| Probabilistic admission | N/A | N/A | None |

Hatchet's accept-but-defer approach trades synchronous failure for unbounded delay. We picked the other side: synchronous reject, no hidden delay.

## Direction

**Structured rejection with a typed reason and a retry-after timestamp.**

```
SubmitTaskError {
  code: RESOURCE_EXHAUSTED | FAILED_PRECONDITION | INVALID_ARGUMENT
  reason: enum {
    NAMESPACE_QUOTA_EXCEEDED,        // transient — retry after token bucket refills
    MAX_PENDING_EXCEEDED,            // transient — retry after queue drains
    LATENCY_TARGET_EXCEEDED,         // transient (CoDel) — retry after queue drains
    SYSTEM_OVERLOAD,                 // transient — generic capacity
    NAMESPACE_DISABLED,              // permanent — admin disabled the namespace
    UNKNOWN_TASK_TYPE,               // permanent — no worker handles this type
    IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD,  // permanent — fix the payload
    INVALID_PAYLOAD,                 // permanent
  }
  retry_after: int64                // Unix epoch milliseconds (UTC)
  retryable: bool                   // derived from reason; explicit for SDK convenience
  pending_count: u64                // current namespace pending depth (when relevant)
  pending_limit: u64                // configured limit (when relevant)
  hint: string                      // human-readable, operator-facing
}
```

**Retry-after is a Unix-epoch-millisecond timestamp, not a delay.** Robust to network latency between server send and caller receive — the timestamp is absolute, not relative to receipt. Caller's clock must be NTP-synced (within a second is sufficient); pathological skew causes harmless over- or under-waiting, never correctness violation. Retry-after is a **floor, not a contract** — the server promises "before this time, retrying is futile" but does not promise success at it.

**Retry-after derivation:**

| Reason | Computation |
|---|---|
| `NAMESPACE_QUOTA_EXCEEDED` | `now + (1 / bucket_refill_rate)` rounded up |
| `MAX_PENDING_EXCEEDED` | ETA from current dispatch rate: `now + (pending_count - pending_limit) / dispatch_rate_estimate` |
| `LATENCY_TARGET_EXCEEDED` | `now + age_of_oldest_pending / 2` (heuristic — half the current latency) |
| `SYSTEM_OVERLOAD` | `now + 1s` (minimum sensible default) |
| Permanent reasons | `retry_after = 0`, `retryable = false` |

The dispatch-rate estimate is computed in the admitter from a sliding window of completion timestamps; if no estimate is available (cold start), use a per-namespace configured default.

**`retryable: bool` is derived from `reason` and surfaced explicitly** so SDKs can route to the right path without enumerating the reason variants. SDKs MUST respect `retryable=false` and surface the error to user code without retrying.

**Caller-actionable info on permanent rejections.** When the rejection cause means retry won't help, the response includes enough information to fix it:

- `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`: include the existing task's `task_id` and a hash of the existing payload so the caller can compare and decide whether to use a new key or correct their payload.
- `UNKNOWN_TASK_TYPE`: include the task type the caller submitted — sometimes the bug is a typo.
- `NAMESPACE_DISABLED`: include the disable reason from admin config (e.g., "billing suspended").

**SDK-side retry behavior.** The official Rust worker/caller SDK handles `RESOURCE_EXHAUSTED` automatically:

- If `retryable=true`: sleep until `retry_after` (with jitter ±20%), then retry. Cap retries at a configured budget (default 5).
- If `retryable=false`: surface error to user code immediately.
- Layered backoff: SDK exposes a max-retry-budget configuration so callers can disable internal retries if their layer above already retries.

This is a critical SDK behavior — the protocol's backpressure design is useless without correct client implementation.

**No global admitter in v1.** System-wide overload manifests as every namespace's `MaxPending` triggering. Document this as a known weakness; v2 adds a global admitter on top.

**No probabilistic admission in v1.** Cliff-edge transitions are simpler and operationally predictable. The `Admitter` trait supports custom probabilistic strategies for operators who need smoothing.

**No separate `GetPendingDepth` read RPC.** Callers should rely on rejection feedback, not preemptive polling. Adding a read RPC creates its own load and a polling-driven failure mode.

**Backpressure is application-level only.** No HTTP/2 flow-control conflation. The CP's gRPC server uses default HTTP/2 windowing; backpressure decisions are exclusively in the application layer where they're observable and actionable.

**Submit acceptance is explicitly not a dispatch promise.** A task admitted at submit may wait indefinitely if no worker registers for its type, if the worker pool is starved, or if other namespaces dominate dispatch. The protocol contract is "we accepted your task into the queue"; dispatch latency is a separate observable governed by worker capacity and the dispatcher strategy. This must be documented prominently in the SDK and operator guide — calling code that depends on quick dispatch must observe task status, not just submit acceptance.

## Open questions

1. **Sliding-window size for dispatch-rate estimation.** 1 minute? 5 minutes? Tunable per namespace? Defer until we have benchmarks.
2. **Retry budget defaults in the SDK.** 5 retries with exponential backoff is the convention; let users configure.
3. **What about `Heartbeat` / `CompleteTask` backpressure?** Workers can also overload the CP. v1 punts: workers are trusted infrastructure, not adversarial. v2 adds worker-side rate limits if needed.
4. **`SetQuota` admin RPC and live behavior.** Per #05, strategy choice is locked at startup but parameters can be live-updated via SetQuota. Confirm: when an operator increases `max_pending` while submissions are being rejected, do existing rejected callers' next retry succeed? Yes, but they only retry after their `retry_after` timestamp — there may be a gap between quota increase and observable improvement.
5. **Observability of rejections.** Per-namespace rejection-rate metric is essential. Belongs to problem #11 (observability).

## Forward references

- Problem #05 owns the `Admitter` trait that produces these rejections.
- Problem #03 owns the idempotency-key dedup path that produces `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`.
- Problem #11 (observability) will own the rejection-metrics surface.
