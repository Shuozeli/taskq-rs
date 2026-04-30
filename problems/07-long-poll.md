<!-- agent-updated: 2026-04-30T03:35:00Z -->

# Problem: Long-poll connection limits

## The problem

`AcquireTask` is a long-poll RPC: a worker calls and the server holds the connection open until either a matching task becomes available or the long-poll timeout expires. Every idle worker holds an open gRPC stream waiting for work. With N workers, the CP carries N concurrent waiters.

Several concerns flow from this:

- **Per-replica capacity.** N workers × waiter state ≈ N × 10KB. File descriptors, scheduler overhead, and memory all bound how many waiters one replica can hold.
- **Wakeup mechanism.** When a task is submitted, the system has to find a matching waiting worker and unblock it.
- **Cross-replica wakeup.** A worker connects to replica A and waits; a task is submitted to replica B. B has to wake A's waiter without the CP layer assuming any specific transport.
- **Wakeup fairness.** When multiple waiters match an arriving task, exactly one should get it. The others should re-block without thrashing.
- **Long-poll timeout.** Default and bounds. Too short = reconnect overhead. Too long = stale connections.

The load-bearing question is cross-replica wakeup, because we committed to multi-replica-capable design (#02) and storage-pluggability (#12) — which together rule out the obvious "use Postgres LISTEN/NOTIFY directly" approach.

## Why it's hard

- **Storage pluggability vs. native notification primitives.** Postgres has LISTEN/NOTIFY. FoundationDB has watches. SQLite has neither. A wakeup mechanism baked into the CP layer would either be Postgres-specific (breaking #12) or lowest-common-denominator polling (high latency).
- **The register/query race.** Worker calls `AcquireTask`, the dispatcher does a first-pass query (no tasks), then registers as a waiter. A task arriving between the query and the register misses the wakeup. The pattern that fixes this — register-then-query — is well-known but easy to get wrong.
- **Best-effort notifications.** Postgres NOTIFY is delivered only to currently-connected listeners; a connection drop loses the message. We need recovery for missed notifications without falling back to "always poll."
- **Wakeup storms.** N tasks submitted simultaneously, M waiters of matching types — naive "wake all matching waiters" causes M-way contention on the dispatch transaction, only one of which can succeed.

## The races

**A. Worker disappears during long-poll.** Worker crashes; CP's gRPC server detects via context cancellation. Waiter is removed from the per-replica index. Standard tokio pattern.

**B. Task submitted just before subscribe registers.** Mitigated by register-then-query: the waiter subscribes to wakeups *first*, then issues the dispatch query. If a task arrived in the window, the registration catches it.

**C. Multiple waiters wake on one task.** SERIALIZABLE + `FOR UPDATE SKIP LOCKED` ensures exactly one waiter's dispatch transaction succeeds. The others see no available tasks and re-block. Correct but wasteful — mitigated by waking only one matching waiter per task notification.

**D. Missed notification.** Postgres NOTIFY is best-effort; a connection blip drops the message. Mitigated by every waiter also waking on a periodic timer (10s default) to query unconditionally — belt-and-suspenders.

**E. Wakeup storm.** N tasks arrive simultaneously, the notification fan-out floods waiters. Mitigated by waking one matching waiter per notification, not all.

## What prior art does

| | Faktory | Hatchet | Temporal |
|---|---|---|---|
| Mechanism | Redis BRPOP, 2s timeout, worker re-FETCHes | Server-streaming, in-memory subscribed-worker map | gRPC long-poll, in-memory matching service waiters |
| Long-poll timeout | 2s | Indefinite (stream) | ~60s default |
| Cross-replica wakeup | Single Redis = single source of truth | Internal RPC between dispatcher instances | Sharded with leader per shard |
| Missed-notification recovery | None needed (single Redis) | None needed (internal push) | Replay log on shard takeover |

Faktory's 2s is too aggressive for our model (every worker reconnects every 2s = thousands of reconnects/sec at scale). Temporal's 60s is the typical convention for gRPC long-poll.

## Direction

**Wakeup is a storage-trait method.** The CP layer doesn't know how cross-replica wakeup happens; the storage backend implements it natively.

```rust
trait StorageTx {
    async fn subscribe_pending(&mut self, ns: &Namespace, types: &[TaskType])
        -> Result<impl Stream<Item = WakeSignal>>;
}

struct WakeSignal;  // signal-only — no task_id, no payload
```

Subscription is **scoped per-namespace**, with `task_types` as a further filter. Listening on every namespace is wasteful; per-namespace lets workers register only for namespaces they serve. The notification carries no payload — waiters query the storage layer on wake. Keeps notifications small and avoids backend-specific size limits (Postgres NOTIFY caps at 8000 bytes).

**Backend implementations:**

- **Postgres**: `LISTEN taskq_pending_<namespace>`. Submit transactions emit `NOTIFY taskq_pending_<namespace>`. Channel name uses the namespace UUID/identifier so subscriptions don't fan out unnecessarily. The `subscribe_pending` Stream is driven by a single LISTEN connection per CP replica that demuxes channels to per-waiter notification senders.
- **SQLite**: short-interval polling (500ms). Single-process so no cross-replica concern.
- **FoundationDB**: native watches on a per-namespace counter key. Increment on submit, watch fires for all listeners.
- **stonedb / others**: short-interval polling fallback unless backend offers a watch primitive.

**Belt-and-suspenders periodic query.** Every waiter, in addition to its subscribe stream, also wakes on a periodic timer (default 10s, configurable) to query unconditionally. Catches missed notifications under all backends; bounds worst-case dispatch latency to 10s if the notification path completely fails.

**Wake one waiter per notification, not all.** The notification path includes a "next waiter" picker per `(namespace, task_type)` queue. When a task arrives, exactly one matching waiter wakes; if its dispatch transaction fails (someone else got the task first via a different path), the picker wakes the next waiter. This avoids the thundering herd while preserving liveness via the periodic timer fallback.

**Per-(namespace, task_type) waiter ordering: FIFO by registration time.** Predictable, debuggable, easy to implement as an ordered queue. Round-robin or random ordering can be added later if FIFO causes hot-cold worker imbalance.

**Long-poll timeout: 30s default, configurable per call.** Worker SDK can request shorter for more reactive behavior. Server enforces a hard upper bound (60s) to prevent indefinitely-held connections.

**Per-replica waiter limit: 5000 default, configurable.** Beyond the limit, `AcquireTask` returns `RESOURCE_EXHAUSTED` with reason `WAITER_LIMIT_EXCEEDED` (a new reason variant added to the rejection enum from problem #06). Load balancer distributes new connections to other replicas. Math: 5000 × ~10KB ≈ 50MB per replica, acceptable.

**Worker SDK reconnect on timeout: immediate, no backoff.** Long-poll timeout is normal flow control, not error. Backoff applies only on actual gRPC errors (`UNAVAILABLE`, `RESOURCE_EXHAUSTED` with reason `WAITER_LIMIT_EXCEEDED`, etc.).

**Connection multiplexing.** A worker SDK with concurrency > 1 issues multiple parallel `AcquireTask` calls. gRPC over HTTP/2 multiplexes these natively over one connection. No protocol-level concern; this is purely an SDK and transport-layer detail.

**Graceful shutdown.** When a CP replica receives SIGTERM, it stops accepting new `AcquireTask` calls (returns `UNAVAILABLE` so load balancer reroutes), lets existing waiters drain or time out, and exits when all are done or a hard deadline (60s default) elapses. Standard gRPC graceful-shutdown pattern.

**Multi-task pull (batched `AcquireTask`): explicitly deferred to v2.** v1 returns one task per call. Batching reduces transaction overhead at high throughput but complicates lease and cancellation semantics (one lease per task vs. per batch, partial-batch failure handling). Revisit if benchmarks identify the dispatch transaction as the bottleneck.

## Open questions

1. **Postgres LISTEN connection lifecycle.** A single LISTEN connection per CP replica handles notifications for all waiters on that replica. Connection drop = all waiters miss notifications until reconnect. The 10s belt-and-suspenders timer covers correctness, but reconnect should be fast. Defer to implementation.
2. **Default belt-and-suspenders cadence under different backends.** 10s is fine for Postgres (notifications are normally fast). For backends doing pure polling (SQLite), the periodic-query *is* the primary mechanism — should we push to 500ms? Lean: backend reports its preferred default; CP uses it.
3. **What if `subscribe_pending` itself fails?** Waiter falls back to periodic-query-only mode. Functionally correct, just higher latency. Document.
4. **Worker SDK behavior on `WAITER_LIMIT_EXCEEDED`.** Backoff and retry to a different replica? The retry-after timestamp from #06 doesn't apply directly because this isn't task-level overload; it's per-replica capacity. SDK should rely on load balancer to retry with backoff.
5. **Observability for waiters.** Per-namespace, per-type waiter counts, average wait time, wakeup-storm metrics — belongs to problem #11.

## Forward references

- Problem #02 (crash consistency) — register-then-query pattern is correct under SERIALIZABLE.
- Problem #05 (dispatch fairness) — the dispatcher strategy runs inside the woken waiter's transaction.
- Problem #06 (backpressure) — `REPLICA_WAITER_LIMIT_EXCEEDED` and `NAMESPACE_WAITER_LIMIT_EXCEEDED` are reason variants in the rejection enum.
- Problem #11 (observability) — owns metrics for waiter pool health.
- Problem #12 (storage abstraction) — `subscribe_pending` is a trait method backends implement natively.

## Post-debate refinement

Three changes from the multi-agent design debate:

- **Reason-code rename:** `WAITER_LIMIT_EXCEEDED` → `REPLICA_WAITER_LIMIT_EXCEEDED` to distinguish from the new namespace-wide cap. The former cap is local to one CP replica (file-descriptor / memory bound); the latter aggregates across replicas via a storage-trait counter. Two distinct codes mean SDKs (and load balancers) can route appropriately.
- **Real namespace-wide waiter cap:** `max_waiters_per_namespace` added to `NamespaceQuota` and enforced transactionally inside the long-poll register flow (§6.2). The previous design had `max_waiters_per_replica` only, which meant the namespace-wide bound was implicitly `N × per-replica`; that's now a separate, real cap.
- **Subscribe-pending ordering invariant** elevated to a 5th storage-conformance requirement (§8.2 #5): "any transaction committing a row matching the subscription predicate strictly after `subscribe_pending` returns MUST cause at least one `WakeSignal` to be observable on the returned stream." Previously implicit; now explicit and testable in the conformance suite.

## Round-2 refinement

Round 2 reversed the round-1 addition of a namespace-wide waiter cap:

- **`max_waiters_per_namespace` deferred to v2.** v1 ships single-replica deployments by default (per `design.md` §13). With one replica, `max_waiters_per_replica` *is* the namespace-wide cap; the round-1 addition of a separate namespace-wide cap was solving a problem v1 doesn't have. Round 2 also flagged the implementation as underspecified (single hot counter row vs sharded vs approximate) which would have been another correctness pin to add. Resolution: drop `max_waiters_per_namespace` from v1's `NamespaceQuota` schema; remove `NAMESPACE_WAITER_LIMIT_EXCEEDED` from the rejection enum; the only waiter cap in v1 is `REPLICA_WAITER_LIMIT_EXCEEDED`. Revisit alongside multi-replica deployment as the v2 default.
- **`AcquireTask` filters on `worker_heartbeats.declared_dead_at IS NULL`** so a worker that Reaper B has reclaimed cannot pick up new tasks while still appearing alive in `worker_heartbeats` — see problem 01 round-2 refinement.
