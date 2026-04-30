# Problem: Retry storms & poison pills

(Closes the forward reference from problem #03 about the retryable-vs-nonretryable taxonomy.)

## The problem

Tasks fail. Some failures are recoverable (transient network errors, slow downstreams); some are not (bad input, deterministic bugs). The system has to:

1. Distinguish retryable from non-retryable failures
2. Schedule retries with backoff so they don't hammer the same downstream
3. Cap retries so genuinely-broken tasks don't loop forever
4. Prevent storms — many failed tasks all becoming retry-eligible at the same instant
5. Survive poison pills — tasks that always fail in a way that pins worker capacity

The mechanism has many small decisions; the wrong default in any one of them produces operational pain that surfaces only under failure.

## What we already have

From earlier problems:
- Worker reports failure with `retryable: bool` (#03)
- `FAILED_NONRETRYABLE` and `FAILED_EXHAUSTED` are terminal states; both hold idempotency keys (#03)
- Idempotency on `(task_id, attempt_number)` (#02)
- All state transitions are SERIALIZABLE (#02)

What's left: the math, the storage shape, the configuration model, and the operational semantics around DLQ and replay.

## Why hard

- **"Retryable" is split between worker and system.** Worker says "this failed, retry me"; system says "you've retried N times, stop." The two have to compose without contradiction.
- **N tasks ≠ N timers.** Naively scheduling per-task retry timers doesn't scale. Need a queryable retry-eligibility model.
- **Backoff is per-task; contention is shared.** Without jitter, many simultaneous failures produce many simultaneous retries.
- **Poison pills pin workers.** A task that fails fast, retries fast, fails fast monopolizes a worker for the duration of its retry budget.
- **Worker reports can be wrong or malformed.** A worker that mis-classifies every failure as retryable burns the retry budget on permanent bugs. We need at least the operator-visible signal to spot this.

## The races and failure modes

**A. Retry-eligibility storm.** Service outage causes 10000 tasks to fail simultaneously, all on the same backoff curve. Without jitter, they all become retry-eligible at the same instant. Mitigated by jittered backoff.

**B. Poison-pill loop.** Task fails fast on every attempt; spends ~1s of worker time per attempt, cycling through `max_retries`. Acceptable in isolation, painful in aggregate. v1 mitigation: just `max_retries` ceiling. v2 may add early-escalation.

**C. Lying or buggy worker.** Worker labels every failure as `retryable: true`. Tasks burn their full retry budget before going to DLQ. Caller sees `FAILED_EXHAUSTED` instead of the more diagnostic `FAILED_NONRETRYABLE`. Operationally: spotted via per-task-type metrics on `error_class` distribution.

**D. Backoff exceeds task TTL.** Computed `retry_after > task.expires_at`. Transitioning to `WAITING_RETRY` would violate the caller's TTL contract. Must transition to `EXPIRED` instead.

**E. Mass DLQ replay.** Operator hits `ReplayDeadLetters` for thousands of dead tasks; all become `PENDING` simultaneously. Mitigated by replay being rate-limited at the admin RPC.

**F. Unknown `error_class`.** Worker reports a class string that no namespace has registered. Without validation, garbage classes accumulate and observability becomes meaningless. Mitigated by per-namespace registered class set; unknown classes rejected at the report transaction.

## What prior art does

| | Faktory | Hatchet | Temporal |
|---|---|---|---|
| Backoff formula | `count^4 + 15 + rand(30 * (count+1))` — aggressive, fixed | Configurable per-task: `retry_backoff_factor`, `retry_max_backoff` | Configurable: initial, coefficient, max, max_attempts |
| Storage | Redis Sorted Set "RETRIES" by `retry_after` | `v1_retry_queue_item` table with `retry_after` | Activity record + retry timer in timer queue |
| Worker-side flag | `Retry: -1` for non-retryable | `should_not_retry` flag in StepActionEvent | `non_retryable_error_types` regex list |
| Error-class registry | None | None | Regex list, namespace-level |
| Poison-pill detection | None | None | None — relies on `max_attempts` |
| DLQ | "Dead" set, separate from RETRIES | Same table, status flag | Same table |
| Replay | `MUTATE` admin commands | UI / API | Reset workflow / activity |

None do automatic poison-pill detection or task-type circuit-breaking.

## Direction

### Worker failure report shape

```
ReportFailure {
  task_id,
  attempt_number,
  retryable: bool,
  error_class: string,    // must be registered per-namespace; rejected if unknown
  message: string,        // human-readable, unbounded but truncated server-side
  details: bytes,         // optional structured payload, max 64KB
}
```

`details` larger than 64KB is rejected. Callers with larger payloads should persist them externally (S3, blob store) and reference by URL in `message`.

### `error_class` registry

Each namespace has a registered set of allowed `error_class` strings, configured via the admin API at namespace setup. `ReportFailure` validates the class against this set; unknown classes are rejected with `UNKNOWN_ERROR_CLASS` and the failure report is *not* recorded — the worker must retry the report with a valid class.

The Rust worker SDK exposes the namespace's registered classes as a typed enum at SDK initialization time, so workers can't accidentally emit unknown classes at compile time. Other-language SDKs do runtime validation.

This trades flexibility for observability: operators can guarantee the failure-class histogram is meaningful because the set is closed.

### Backoff math

Server-side. Worker never computes its own retry. Backoff parameters come from a layered config (next section). Curve is exponential with **full jitter**:

```
delay = min(initial_ms * coefficient^attempt, max_ms)
retry_after = now + rand(delay/2, delay*3/2)
```

Full jitter (range `[delay/2, 3delay/2]`) is well-known, simple, and prevents the same-instant retry storm without requiring per-task state beyond `retry_after`.

### Retry config: three layers

Backoff parameters resolve top-down at retry-scheduling time:

1. **Per-task (most specific)** — fields on the task at submit time. Optional.
2. **Per-task-type** — registered configuration per `(namespace, task_type)`. Set via admin API. Optional.
3. **Per-namespace defaults** — always present, set at namespace creation.

```
RetryConfig {
  initial_ms: u64,        // default 1000
  max_ms: u64,            // default 300_000 (5 min)
  coefficient: f32,       // default 2.0
  max_retries: u32,       // default 5; per-namespace hard ceiling enforced
}
```

The middle layer matters because task types within a namespace often have different sensible defaults — `llm.flash` retries should ramp slower than `scrape.browser` retries. Per-namespace alone is too coarse; per-task at submit makes every caller specify it. The task-type layer captures the type-specific knowledge once.

### Storage shape

Same `tasks` table, no separate retry queue. Status `WAITING_RETRY` with a `retry_after` column. The dispatch query naturally includes retry-ready tasks:

```sql
WHERE (status = 'PENDING')
   OR (status = 'WAITING_RETRY' AND retry_after <= NOW())
```

Eliminates the need for a promotion reaper. The query already exists for dispatch; this is just one extra disjunct.

### Terminal-state mapping

| Worker reports | Attempt | Resulting state |
|---|---|---|
| `retryable=false` | any | `FAILED_NONRETRYABLE` |
| `retryable=true` | `attempt < max_retries` | `WAITING_RETRY` with computed `retry_after` |
| `retryable=true` | `attempt >= max_retries` | `FAILED_EXHAUSTED` |
| `retryable=true` but `retry_after > task.expires_at` | any | `EXPIRED` (TTL contract wins) |

Both `FAILED_NONRETRYABLE` and `FAILED_EXHAUSTED` hold their idempotency key (per #03's terminal-state taxonomy). `EXPIRED` also holds the key — TTL expiration is an authoritative system answer.

### DLQ and replay

Dead-letter is not a separate table — it's a status flag (`FAILED_*` / `EXPIRED`). The admin API surfaces it through queries on the `tasks` table.

`ReplayDeadLetters` admin RPC transitions matching tasks back to `PENDING` with **`attempt_number` reset to 0** — treated as fresh attempts. An `original_failure_count` field is preserved on the task for observability ("this task has been DLQ-replayed, here's what happened the first time"). Replay is rate-limited at the admin RPC (default 100 tasks/sec, configurable per-namespace) to prevent storm-on-replay.

Replaying a task whose idempotency key has since been claimed by another submission is rejected — replay cannot collide with a fresh submission using the same key.

### What v1 does NOT include

- **Automatic poison-pill detection.** No "if 5 attempts have the same `error_class`, escalate early." Operators identify poison pills via metrics and use `PurgeTasks`.
- **Task-type circuit breaking.** No automatic shutoff of a task type with high failure rate. Operators can disable types via admin RPC.
- **Pluggable retry strategies.** Unlike `Admitter` and `Dispatcher`, retry math is uniform across the system. Pluggability would cost more than it saves.

These are v2 candidates; the protocol does not preclude them.

## Open questions

1. **Where does the per-namespace `error_class` registry live?** Same DB table as namespace config? Separate? Lean: same.
2. **Migration path for `error_class` schema changes.** If an operator removes a class from the registry, what happens to historical tasks with that class? Lean: registry change only affects future reports; historical data retained.
3. **Replay semantics for `EXPIRED` tasks.** Replay resets state to `PENDING` and resets `attempt_number` — but should it also reset `expires_at`? Lean: yes, compute fresh `expires_at = NOW() + original_ttl`. Otherwise the replayed task immediately re-expires.
4. **Worker access to `attempt_number`.** Confirmed (per discussion) that we don't surface `max_retries` to the worker on `AcquireTask`. Confirm whether `attempt_number` itself is included — workers may want to log differently on later attempts. Lean: yes, include `attempt_number` (4 bytes); `max_retries` not.
5. **Failure-report idempotency.** Worker calls `ReportFailure` twice for the same `(task_id, attempt_number)` — second call observed as already-failed and returns success without re-transitioning. Same model as `CompleteTask` from #02.

## Forward references

- Problem #03 (idempotency) — terminal-state taxonomy is finalized here.
- Problem #11 (observability) — owns the `error_class` histogram and per-task-type failure-rate metrics that operators use for poison-pill identification.
- Problem #06 (backpressure) — `ReplayDeadLetters` rate limit shares the rejection-reason vocabulary.
