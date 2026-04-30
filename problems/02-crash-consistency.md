# Problem: Crash consistency

(Subsumes the original problem #04, "Result delivery": under our chosen direction, result delivery is just another serializable transition with no special crash semantics.)

## The problem

The control plane mediates state transitions on durable task records. At any moment it may crash — process killed, node lost, container redeployed. The contract we promise callers (at-least-once delivery, idempotent retries, observable terminal state) must survive every possible crash point. Workers and callers cannot tell whether a CP crash lost their request, processed it, or processed-and-then-forgot — they will retry blindly.

Crash consistency is the discipline of making sure no crash leaves the system in a state that violates the contract: not "lost work," not "silently duplicated work without an idempotency record," not "two workers think they own the same task," not "task is COMPLETED but result is missing."

## Why it's hard

The CP holds three categories of state:

- **Durable** — tasks, runtime/lease rows, results, idempotency keys. Survives crashes only if the storage commit happens *before* the RPC reply.
- **In-memory** — worker connection map, in-flight long-poll waiters, lease-extension caches, quota token buckets. Lost on crash.
- **Network-in-flight** — RPC bodies traveling between caller↔CP and CP↔worker. Crash here = the other side doesn't know the operation outcome.

For every operation, we have to decide where the storage commit lands relative to the RPC reply, and what retry semantics the client uses if it doesn't get one. Get the order wrong and a single-node crash silently violates the contract.

## The crash points that matter

**A. Crash between "task written to storage" and "reply to SubmitTask."**
Caller doesn't know if it succeeded. Without an idempotency key, retries create a duplicate task. With one, the second submit must observe the existing record and return the same task_id.

**B. Crash between "AcquireTask returned task to worker" and "lease persisted."**
Worker has the task in hand; CP doesn't know it's in flight. On restart, dispatch picks the same task and hands it to a different worker. **Two workers process the same task with no record that this happened.** Avoidable only by persisting the lease before the task leaves the CP on the wire.

**C. Crash during CompleteTask, after CP wrote result, before reply.**
Worker sees gRPC error and retries. The handler must be idempotent on `(task_id, attempt_number)` — the second call observes "already completed for this attempt" and returns success without writing again.

**D. Crash with in-flight lease extensions in memory.**
If heartbeat updates live in memory only (Faktory's lazy-renewal pattern), a crash loses them. On restart the reaper sees expired tasks that were actually being extended and reclaims them. Workers' eventual ACKs fail. Contract is technically held (at-least-once permits this) but observability is poor.

**E. Crash with worker connections held in memory.**
Long-poll waiters all hang. Workers see gRPC stream errors and reconnect. The CP doesn't need to recover them — they re-establish themselves. No state loss.

**F. Concurrent state transitions on the same task.**
Not strictly a "crash" point but in the same family: reaper marking lease-expired while worker concurrently completes; two reapers fighting for the same expired lease; idempotency-key insert racing with another submit using the same key. Without a strong consistency model these are races that surface only at scale and corrupt state silently.

## What prior art does

| | Faktory | Hatchet | Temporal |
|---|---|---|---|
| In-flight state | Redis ZSet (durable) + in-mem extensions (lost on crash) | Postgres `task_runtime` (durable) | In-mem ack manager + persistent timer queue (durable) |
| Restart recovery | `loadWorkingSet` rebuilds map from Redis | Stateless restart; re-read DB | Shard leader takeover, replay log |
| Dup-work risk after CP crash | Yes — lease extensions lost | Low — everything durable | Low — timer queue durable |
| Multi-replica CP | Single server | Multiple engines, shared Postgres | Native — sharded with leader election |
| Concurrency control | Redis atomic ops | Postgres `FOR UPDATE SKIP LOCKED` in reap, optimistic elsewhere | Per-shard single-writer + optimistic mutable-state CAS |

## Direction

**External consistency as the property; single-node Postgres at SERIALIZABLE isolation as the v1 mechanism.**

Every state transition (submit, acquire, heartbeat, complete, reclaim, cancel) is a single serializable transaction. The transaction must commit before the corresponding RPC replies. The protocol commits to external consistency as a property; v1 ships a Postgres reference implementation; future backends (CockroachDB, Spanner, FoundationDB) can implement the same property for multi-region deployment without protocol changes. Backends that cannot provide external consistency (e.g. plain Redis) are not valid storage backends.

**Concrete commitments this implies:**

- **No CP-local in-memory state on the correctness path.** Worker connection maps and long-poll waiters are local to one CP replica because they're inherently connection-bound, but every fact that matters for correctness lives in the DB. CP replicas are interchangeable — a worker can reconnect to any replica mid-task and operations continue.
- **Lease persisted before `AcquireTask` returns.** The transaction reads pending tasks, inserts the runtime/lease row, and only then sends the response. Crash point B is eliminated.
- **Every heartbeat writes `last_heartbeat_at` durably.** Faktory's lazy-renewal trick is off the table — it trades crash consistency for write throughput, and we've chosen the other side. (This resolves problem 01's open question on renewal eager-vs-lazy.)
- **`CompleteTask` is idempotent on `(task_id, attempt_number)`.** Including `attempt_number` distinguishes "completing attempt 2 after attempt 1 was reclaimed" from "completing attempt 1 twice." A second call with the same `(task_id, attempt)` observes the existing row and returns success without re-running side effects.
- **`LEASE_EXPIRED` is a distinct error code.** When a worker's `CompleteTask` lands after the reaper has already reclaimed the lease (and possibly re-dispatched the task as a higher attempt number), the transaction returns `LEASE_EXPIRED` rather than a generic error. The worker logs and drops; the contract has held because the new attempt is in progress.
- **Reaper transaction takes `FOR UPDATE` on the runtime row before checking `timeout_at`.** Avoids serialization-conflict retry pressure on a cold-path query. Multiple CP replicas can run reaper concurrently; `FOR UPDATE SKIP LOCKED` ensures they pick disjoint rows.
- **v1 ships single-replica CP, but writes no code that assumes single-replica.** No in-memory caches that other replicas would need to invalidate, no leader election, no "I am the only writer" assumptions. Multi-replica becomes a deployment decision, not a code change.

**What this costs:**

- One DB write per `AcquireTask`, per `Heartbeat`, per `CompleteTask`, per reaper-reclaimed task. Heartbeat write rate dominates. Mitigation paths (heartbeat coalescing, separate heartbeat table to keep main task row cold) are deferred until problem 06 (backpressure) and 05 (fairness).
- Serialization conflicts under contention surface as transaction retries (Postgres `40001`). Mitigated by row-level `FOR UPDATE` patterns on the relevant runtime row, which serialize naturally without retry pressure.
- We are coupled to a backend that supports `SERIALIZABLE`. Postgres, CockroachDB, Spanner, FoundationDB qualify; SQLite qualifies (single-writer is trivially serializable); Redis without Lua does not.

## Open questions

1. **Heartbeat write pressure.** At what worker count does per-heartbeat DB write become the bottleneck? Decide jointly with problem 06. Possible mitigations: separate `worker_heartbeats` table to keep `tasks` row cold, batch heartbeats from worker SDK, lower default heartbeat cadence.
2. **Reaper cadence.** How often does the reaper poll? Determines minimum useful lease duration. Decide jointly with problem 01.
3. **Idempotency-key TTL.** How long after a task terminates do we retain its idempotency key for dedup? Belongs to problem 03.
4. **What `GetTaskResult` returns during the small window between `CompleteTask` commit and result row visibility on a different replica.** Under SERIALIZABLE this should be zero on a single node, but we should specify the contract: "a successful `CompleteTask` reply is a happens-before edge for any subsequent `GetTaskResult` from any replica."
5. **Reaper concurrency mode.** Multiple replicas can run reaper concurrently with `SKIP LOCKED`; is that what we want, or do we want a single elected reaper to keep the cadence predictable? Defer.

## Post-debate refinement

The multi-agent design debate identified two correctness gaps and resolved them in `design.md`:

- **Heartbeats are now an explicit carve-out from external consistency** (§1.1). Heartbeats write to `worker_heartbeats` at READ COMMITTED with lazy lease extension on `task_runtime`. The carve-out is deliberate; losing heartbeats across a CP crash is consistent with at-least-once. See problem 01 post-debate refinement and design.md §6.3.
- **Capacity quotas are now read transactionally** (§9.1). Earlier framing implied the 5s cache TTL applied to all quota reads, which would have meant admit decisions could compare a stale `max_pending` limit against fresh storage state — a quiet violation of external consistency for the admit predicate. Capacity reads happen inside the transaction (with `version`-fenced cache for the config body); only rate quotas remain cached as eventually consistent (also a §1.1 carve-out).
- **Worker `CompleteTask` / `ReportFailure` transparently retry on Postgres `40001`** (§6.4). Earlier framing committed to a distinct `LEASE_EXPIRED` error code but didn't specify how it survives serialization conflicts under contention with the reaper. The CP now retries up to 3 times before classifying, distinguishing `NotFound` (return `LEASE_EXPIRED`) from `SerializationConflict` (retry).

## Round-2 refinement

Round 2 accepted the round-1 carve-out + transparent-retry direction but tightened two things:

- **Capacity-quota reads are now inline transactional with NO cache.** The round-1 design proposed a `version`-fenced cache to save the row read; round 2 noted that v1's admit target (1K/sec/replica) makes the savings rounding error against the SERIALIZABLE transaction's own cost (1-10ms vs 50-200µs), and the version row itself becomes a hot-contention point at higher rates. Resolved by reading `namespace_quota` inline every admit/acquire — see `design.md` §9.1. The version-fenced cache is moved to `design.md` §14 as a v1.1 candidate, gated on benchmarks showing >2K admits/sec sustained.
- **Retry-budget metrics added.** A 3-attempt CP-side `40001` retry × N replicas × SDK retry budget can produce a ~75-attempt long tail under contention, hidden from operators. `taskq_storage_retry_attempts` (histogram per op) and `taskq_sdk_retry_total` (counter per namespace + reason) make this observable — see `design.md` §11.3.
