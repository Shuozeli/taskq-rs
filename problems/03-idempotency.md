# Problem: Idempotency window race

## The problem

Callers retry. Networks fail, RPCs time out, callers crash mid-`SubmitTask` and restart. Without dedup, every retry creates a duplicate task — which violates the contract callers think they're getting (one logical request → one logical task) even though it doesn't violate at-least-once at the worker level.

An idempotency key gives callers a stable handle for a logical submission: any retry with the same key returns the same task, not a new one. The hard part is not the dedup itself — external consistency from problem #02 makes the dedup-check + task-insert atomic, so concurrent submits with the same key cannot both succeed. The hard part is the *lifecycle*: when does the dedup record stop deduplicating, what does the duplicate caller observe, and what happens when the original task ended in different terminal states?

## Why it's hard

Idempotency is a contract that lives at the seam between caller intent and system state. Several questions all interact:

- The dedup record cannot live forever (table grows unbounded), so it has a TTL — and the TTL boundary is itself a race: a retry after TTL expiry silently creates a duplicate.
- The "duplicate caller" must observe a useful response — but baking the result into the response couples the dedup table to the result schema and bloats it.
- Same key + different payload signals a caller bug or version skew. Detecting it requires storing a body hash; not detecting it lets bugs hide.
- Different terminal states (completed, cancelled, failed) carry different intent about whether the key should still dedup. Without a deliberate taxonomy, callers can't reason about what their key means.

## The races, in order of severity

**1. Window-expiry race.** Caller submits with key K at T0. Task completes at T1. Dedup record expires at T2. Caller's retry budget extends past T2. Retry at T3 (> T2) does not match any dedup record and creates a *new* task with the same key. The caller believes they're idempotent; the system has forgotten. **The TTL must exceed any reasonable retry budget, or the duplicate must be observable.**

**2. In-flight retry race.** Caller times out, retries while the task is still running. The second submit must return the existing `task_id` and current status without re-creating the task. SERIALIZABLE from problem #02 ensures atomicity; the contract question is what shape the response takes.

**3. Same-key-different-payload.** Caller bug, double-deploy, or version skew. Same key arrives with a different payload. Silent first-write-wins hides bugs; explicit rejection catches them. Requires a body hash in the dedup record.

**4. Cross-namespace collision.** Two tenants pick the same key string. If the dedup index is global, they collide. Trivially solved by scoping `(namespace, key)`.

**5. Terminal-state ambiguity.** The original task ended — but in which terminal state? A retry's expectation depends on whether the prior outcome was authoritative (the system's final answer about this work) or abandoned (the caller threw it away). Without a deliberate rule, this becomes implementation-defined.

## What prior art does

| | Faktory | Hatchet | Temporal |
|---|---|---|---|
| Mechanism | None — caller's problem | `v1_idempotency_key` table, `(tenant_id, key)` unique index | `workflow_id` reuse policy |
| Required or optional | N/A | Optional | Workflow ID is required, idempotency policy is optional |
| TTL | N/A | `expires_at` column, configurable | Bound to workflow history retention |
| Loser sees | N/A | Existing task | Configurable per policy: reject / allow / terminate-if-running |
| Body-differs check | N/A | None (first-write-wins) | N/A |
| Stored result | N/A | No (separate table) | No (workflow history) |

## Direction

**Idempotency keys are required on `SubmitTask`.** Callers must always provide one. This is unusual — most systems make it optional — and it is a deliberate cost to push retry-safety onto every caller from the start, preventing silent duplicate floods that only surface in production. SDKs can generate a UUID v7 by default for callers who don't care; the protocol does not let them omit it.

**Scope is `(namespace, key)`.** Both required, both indexed together. Cross-namespace collisions are impossible by construction.

**Default TTL is `task_terminated_at + 24h`,** with a per-task override at submit time and a per-namespace hard ceiling. The 24h floor encodes "longer than any reasonable caller retry budget" and the per-namespace ceiling prevents callers from setting effectively-infinite TTLs that bloat the dedup table.

**Body hash stored in the dedup record.** 32-byte blake3 of the FlatBuffers wire bytes the caller sent. On submit, hash the incoming payload bytes and compare; on mismatch, reject with `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`. Cheap insurance against caller bugs and version skew. The hash is over the literal wire bytes, not over the logical content — if a caller's local FlatBuffers builder produces different vtable ordering across runs for byte-equivalent content, those will hash differently. We accept this; the wire bytes are the authoritative payload identity.

**Loser observes `task_id` + current status, not the result.** Result is fetched via `GetTaskResult`. Keeps the dedup table cold and small; avoids embedding the result schema in submit responses.

**Terminal-state taxonomy** governs whether the key continues to dedup after the task ends. Tasks reach one of four terminal states:

| Terminal state | Meaning | Idempotency key |
|---|---|---|
| `COMPLETED` | Worker reported success | **Held** for TTL — return cached terminal state on retry |
| `CANCELLED` | Caller explicitly abandoned (`CancelTask`) | **Reusable immediately** — caller threw it away, fresh start allowed |
| `FAILED_NONRETRYABLE` | Worker reported failure with `retryable=false` | **Held** for TTL — authoritative "this work doesn't succeed" |
| `FAILED_EXHAUSTED` | Retries exhausted under retryable failures | **Held** for TTL — same as `FAILED_NONRETRYABLE` for idempotency purposes |

Symmetry: a key is **held** when the system's terminal state is authoritative about this work; a key is **reusable** only when the caller explicitly abandoned the prior attempt. Callers who want to retry the same logical work after a non-retryable failure must use a new key — the system is telling them "this didn't work, don't ask again with the same handle."

The retryable-vs-nonretryable distinction is itself an upstream decision that belongs to problem 08 (retry storms / poison pills). What matters here is only that the protocol has a `retryable` flag on worker failure reports and that exhausted retries collapse into the same terminal class as non-retryable failures.

**Cleanup of expired dedup records.** The dedup table grows on every `SubmitTask`. After a record's `expires_at` passes, it is dead weight — still occupies space, still slows the unique-index lookups, but no longer dedups. Postgres `autovacuum` does not help (it reclaims space from `DELETE`d rows, not expired ones); we have to issue the `DELETE` ourselves. Two complementary mechanisms:

- **Lazy on lookup.** When `SubmitTask` finds a matching row whose `expires_at < NOW()`, delete it inline before treating the request as a cache miss. Keeps hot keys clean for free, no separate process required.
- **Periodic reaper.** `DELETE FROM idempotency_keys WHERE expires_at < NOW() LIMIT N` on a schedule (e.g. every 30s). Catches the cold long tail that lazy cleanup never re-touches.

Both run; either alone is sufficient for correctness, together they bound table growth predictably.

**`PurgeTasks` semantics.** Admin-purging a task is uniformly: cancel-if-not-terminal, then delete the task row, then delete the dedup record — all in one transaction. For tasks already in a terminal state (`COMPLETED` / `FAILED_NONRETRYABLE` / `FAILED_EXHAUSTED`), cancellation is a no-op and we just cascade through to the deletes. This collapses the orphan-vs-cascade question: the dedup record never outlives its task. (Orphaned dedup records are a footgun — future submits with the same key would be rejected as duplicates of a task that no longer exists.)

## Open questions

1. **Per-namespace hard ceiling default for TTL.** 7 days? 30 days? Encoded as a quota, configurable via the admin API.
2. **What if the caller wants to "force a new attempt" after a held terminal state?** Adding a `force=true` flag to `SubmitTask` would bypass dedup entirely. Tempting; probably leave it out of v1 — callers can pick a new key, which is the same effect with explicit intent.

## Forward references

- Problem 08 (retry storms / poison pills) owns the full retryable-vs-nonretryable taxonomy on the worker failure path. This doc only consumes its conclusion.
- Problem 02 (crash consistency) provides the SERIALIZABLE foundation that makes the concurrent-submit race trivially correct.

## Post-debate refinement

The scale-skeptic agent flagged that at sustained 1K submits/sec × 24h TTL, `idempotency_keys` reaches ~86M rows / ~10GB on Postgres, with `DELETE WHERE expires_at < NOW()` thrashing autovacuum. The judge ruled this a high-severity issue. Resolution in `design.md`:

- **Postgres backend range-partitions `idempotency_keys` by `expires_at`** (daily granularity). Cleanup becomes `DROP PARTITION`, not row-by-row delete — eliminates the vacuum/bloat cliff. (See `design.md` §8.3.)
- **A new conformance requirement (§8.2 #6) — Bounded-cost dedup expiration** — formalizes this: backends with mutable per-row delete cost MUST implement time-range partitioning of the dedup table. SQLite is exempted by virtue of single-writer / small scope.
- **`CancelTask` explicitly deletes the `idempotency_keys` row** in its SERIALIZABLE transaction (§6.7), implementing the "key reusable immediately on CANCELLED" rule from §5 as physical deletion rather than a status flag.

## Round-2 refinement

Round 2 added three small clarifications:

- **Shared `cancel_internal` helper.** `CancelTask` and `PurgeTasks` both perform the same "transition to CANCELLED + delete dedup row" step. They now share a private helper `cancel_internal(tx, task_id)` so the two paths cannot drift in maintenance changes. See `design.md` §6.7.
- **`CancelTask` ↔ `SubmitTask` race ordering documented.** Under SSI the later-committing transaction wins. If Cancel commits first, the next Submit with the same key creates a fresh task (key released). If Submit commits first, Cancel operates on the new task. Callers should not assume "I cancelled, so my retry is fresh" without observing the cancel reply. See `design.md` §6.7.
- **Hard 90-day TTL ceiling.** `max_idempotency_ttl_seconds ≤ 90 days` is enforced at `SetNamespaceQuota` to bound the active partition count on Postgres at ~90 (partition pruning planning time degrades superlinearly past a few hundred partitions). See `design.md` §8.3 and §9.1.
