# Problem: Multi-tenant noisy neighbor (quota enforcement points)

## The problem

A "noisy neighbor" is any tenant whose behavior degrades the experience of other tenants:

- Tenant A submits at 10000 RPS; tenant B's submits queue behind A's transactions.
- Tenant A holds 4000 idle workers eating connection slots; tenant B's worker registrations are rejected.
- Tenant A's tasks fail with high retry counts; the retry-eligible storm crowds out B's dispatch.
- Tenant A configures a 1-second heartbeat interval; the write rate dominates storage for everyone.
- Tenant A's `PurgeTasks` admin call locks the tasks index; B's submits stall.

Multi-tenancy was a stated goal (#5 in README). Earlier problems added per-namespace controls one at a time. This doc consolidates them into a coherent quota model, identifies what's still missing, and resolves an ambiguity left over from #05 and #06.

## What's already in place

| Problem | Per-namespace control |
|---|---|
| #03 | Idempotency-key TTL ceiling, `(namespace, key)` scoping |
| #05 | Strategy choice (Admitter, Dispatcher); `MaxPending` admitter |
| #06 | `max_pending` enforcement, retry-after derivation per quota |
| #07 | Per-namespace `subscribe_pending`, per-replica waiter limit (global only — *missing per-namespace cut*) |
| #08 | Registered `error_class` set, retry config layers, `max_retries` ceiling, replay rate limit |
| #09 | Strategy mismatch handling per namespace |

What this problem adds:
- Per-namespace waiter limit (global only today)
- Submit/dispatch RPM rate limits (mentioned but not formalized)
- Worker count limits (a namespace registering 10000 workers loads registration paths)
- Min heartbeat interval (a namespace with 1s heartbeats hurts everyone via write amplification)
- Payload size limits (a namespace submitting 100MB payloads dominates storage)
- A coherent `NamespaceQuota` config object that bundles all of this
- Resolution of the live-update question

## The unresolved decision: quota live-update

In #05 we said "strategy choice + parameters are immutable for the process's lifetime." In #06 we asked the same question and didn't resolve it.

This doc resolves it: **strategy *choice* is locked at startup; quota *parameters* live-update.**

- Strategy choice (`Admitter = MaxPending` vs `CoDel`) requires rolling restart, per #05. Code path selection.
- Quota values (`max_pending = 1000`) live-update via `SetNamespaceQuota`. They're parameters, not code.

The two were conflated. They have different deployment characteristics — quota tweaks are routine ops; strategy swaps are deliberate code path changes. They should be treated differently.

## Why hard

- **Quotas are read on every hot-path RPC.** Per-call DB read for "what's namespace X's max_pending?" is expensive at high QPS. Caching is required, but caching introduces propagation-window questions.
- **Lowering a quota below current usage is ambiguous.** Reject the admin call? Accept and let backpressure take over? Cap usage immediately?
- **Quota observability is essential.** An operator who can't see "namespace X is at 95% of its max_pending" can't act. Quota model and metrics are coupled (#11 owns the metrics surface).
- **Quotas vary in cost-to-enforce.** Counting pending tasks is an indexed query. Tracking RPM requires sliding-window or token-bucket state. Different storage backends have different optimal implementations for each.
- **Quotas without defaults force operator burden.** Every new namespace would need explicit configuration. Defaults via inheritance keep onboarding simple.

## What prior art does

| | Faktory | Hatchet | Temporal |
|---|---|---|---|
| Per-tenant quotas | None — single-tenant | Per-tenant separate rate-limit queues | Namespace-level RPS, dispatch limits |
| Quota model | N/A | Per-tenant table, per-queue config | Namespace config |
| Live-update | N/A | API-driven, eventual | API-driven, eventual |
| Defaults | N/A | Per-tenant initialization | System defaults + namespace overrides |
| Enforcement state | N/A | Postgres counters | In-memory shard-leader-owned |

Both Hatchet and Temporal allow live quota updates with eventual consistency.

## Direction

### Two distinct things, two storage paths

**Quota config** — *what's the policy?* — `max_pending = 1000`. Read-mostly. Lives in a `namespace_quota` table. Cached at the CP layer with **5s default TTL, configurable**. Updates take effect within the cache TTL window across replicas.

**Quota enforcement state** — *how much have we used?* — current pending count, current token-bucket fill, current registered worker count. Read on every hot-path RPC, written on every submit/acquire/heartbeat. **Delegated entirely to the storage layer** via the trait from #12.

The CP layer doesn't know or care whether quota state is kept in-memory per replica, in a Postgres advisory lock, in a Redis token bucket, or computed via aggregated DB queries. The storage backend chooses the implementation that's natural for it. The CP just calls into the trait:

```rust
trait StorageTx {
    async fn check_capacity_quota(
        &mut self,
        ns: &Namespace,
        kind: CapacityKind,
    ) -> Result<CapacityDecision>;

    async fn try_consume_rate_quota(
        &mut self,
        ns: &Namespace,
        kind: RateKind,
        n: u64,
    ) -> Result<RateDecision>;
}

enum CapacityKind { MaxPending, MaxInflight, MaxWorkers, MaxWaitersPerReplica }
enum RateKind { SubmitRpm, DispatchRpm, ReplayPerSecond }

enum CapacityDecision { UnderLimit { current: u64, limit: u64 }, OverLimit { current: u64, limit: u64 } }
enum RateDecision { Allowed { remaining: u64 }, RateLimited { retry_after_ms: u64 } }
```

This generalization is the load-bearing decision: it preserves storage pluggability (#12) and lets backends optimize quota enforcement for their natural primitives.

### Unified `NamespaceQuota` config

```rust
struct NamespaceQuota {
  // Capacity quotas
  max_pending:               Option<u64>,
  max_inflight:              Option<u64>,
  max_workers:               Option<u32>,
  max_waiters_per_replica:   Option<u32>,
  
  // Rate quotas (token bucket with burst)
  max_submit_rpm:            Option<RateLimit>,
  max_dispatch_rpm:          Option<RateLimit>,
  max_replay_per_second:     Option<u32>,
  
  // Per-task ceilings
  max_retries_ceiling:       u32,
  max_idempotency_ttl_seconds: u64,
  max_payload_bytes:         u32,           // default 10 MB, configurable
  max_details_bytes:         u32,           // from #08, default 64 KB
  
  // Worker behavior bounds
  min_heartbeat_interval_seconds: u32,
}

struct RateLimit { rate_per_minute: u64, burst: u64 }
```

`Option` fields default to `system_default` quota inherited at namespace creation. The admin API exposes `SetNamespaceQuota` / `GetNamespaceQuota`.

### Five enforcement points

| Point | Quotas applied |
|---|---|
| Submit-time (Admitter) | `max_pending`, `max_payload_bytes`, `max_idempotency_ttl_seconds`, `max_submit_rpm` |
| Acquire-time (Dispatcher) | `max_inflight`, `max_dispatch_rpm` |
| Register-time | `max_workers` |
| Long-poll register | `max_waiters_per_replica` |
| Heartbeat-time | `min_heartbeat_interval_seconds` (rejects requests for shorter intervals) |
| Report-time | `max_details_bytes`, `max_retries_ceiling` (already enforced by #08 retry math) |

Each enforcement point inside a SERIALIZABLE transaction calls into the storage trait's quota methods. Decisions are part of the same transaction as the operation they gate.

### Quota inheritance

A `system_default` row exists with defaults for every field. Per-namespace config overrides specific fields; unset fields inherit defaults. **`system_default` is itself live-updatable** via the same `SetNamespaceQuota` admin RPC; updates propagate within the same 5s cache TTL window.

### Quota validation

`SetNamespaceQuota` rejects nonsensical configs at admin-API time:
- `max_pending=0` (use `NamespaceDisabled` to disable submission)
- `min_heartbeat_interval_seconds=0`
- `RateLimit { rate_per_minute: 0, burst > 0 }`
- `max_idempotency_ttl_seconds` outside the protocol-imposed bounds

Validation rules documented in `protocol/QUOTAS.md`.

### Lowering below current usage

`SetNamespaceQuota` does not reject operations that lower a quota below current usage. The new quota takes effect on the next admission decision; existing over-quota state drains naturally. Operators who want a hard cap can issue `PurgeTasks` after.

### Per-namespace waiter limit precedence

When both global (`max_replica_waiters` from #07) and per-namespace (`max_waiters_per_replica`) limits apply, **whichever rejects first wins**. Reason codes distinguish:
- `WAITER_LIMIT_EXCEEDED` — global per-replica cap
- `NAMESPACE_WAITER_LIMIT_EXCEEDED` — per-namespace cut

SDKs treat both identically (backoff + retry).

### Worker count semantics

- `max_workers` counts registered workers regardless of state (active, idle, draining).
- `max_waiters_per_replica` separately bounds active long-poll connections per replica.
- A worker can be registered without holding a long-poll waiter (between polls). The two counts are independent.

### What v1 does NOT include

- **Per-task-type quotas.** Only per-namespace. Per-task-type retry config exists (#08) but per-task-type capacity quotas are deferred — adds another reasoning layer; real workloads need more design first.
- **Hierarchical namespaces.** Flat namespace model. Multi-level (org → team → service) deferred.
- **Storage partitioning by namespace.** v1 uses a shared `tasks` table indexed by namespace. Per-namespace partitioning is a v2 backend optimization.
- **Billing-grade metering.** Quota enforcement uses approximate counters where the storage backend chooses (e.g. per-replica in-memory token buckets). Not suitable for billing without auxiliary precise tracking. v2 if needed.
- **Quota change audit log.** No persistent record of "who changed what when." `SetNamespaceQuota` writes overwrite the previous value. v2 if compliance demands.

## Open questions

1. **`min_heartbeat_interval_seconds` default.** 5s? 15s? Coupled to the lease/heartbeat semantics from #01. Lean: 5s default, namespace can request longer (more lenient) via `SetNamespaceQuota`, but not shorter.
2. **`max_payload_bytes` enforcement during ingress.** Should the gRPC server reject oversized payloads at the transport layer (HTTP/2 message size limit) or after deserialization? Lean: transport layer for hard rejection, application layer for namespace-specific override (rare path).
3. **Quota cache invalidation across replicas on `SetNamespaceQuota`.** Beyond the 5s TTL, do we want active invalidation (e.g., LISTEN/NOTIFY)? Lean: no — TTL is sufficient and avoids coupling to backend-specific notification primitives. Document the propagation window.
4. **What does `Option<Quota>` = None really mean operationally?** "Unlimited" until system-level resource exhaustion. Documented but operators should generally set explicit limits.
5. **Storage trait additions.** The new quota methods are an addition to the trait from #12. Coordinate the change in #12's trait surface.

## Forward references

- Problem #12 (storage abstraction) — `check_capacity_quota` / `try_consume_rate_quota` are added to the trait. The backend chooses the implementation strategy.
- Problem #11 (observability) — owns per-namespace usage metrics (current/limit gauges, rate-limit-hit counters).
- Problem #05 (dispatch fairness) — the cross-reference for "strategy choice startup-locked, parameters live-update" is here.
- Problem #06 (backpressure) — `NAMESPACE_WAITER_LIMIT_EXCEEDED` is a new reason variant in the rejection enum.
- Problem #07 (long-poll) — global vs per-namespace waiter precedence.
- Problem #08 (retry storms) — replay rate limit lives in this quota model.

## Post-debate refinement

Three changes from the multi-agent design debate:

- **Capacity-quota reads are transactional, not cached.** The original "5s cache TTL applies to all quota reads" wording would have allowed the admit predicate to compare an old cached `max_pending` limit against fresh storage state — a quiet violation of external consistency for the admit path. Now: capacity limits (`max_pending`, `max_inflight`, `max_workers`, `max_waiters_per_*`) are read inside the transaction with a `version`-fenced cache; only rate quotas (`max_*_rpm`, `max_replay_per_second`) remain cached as eventually consistent (§1.1 carve-out in `design.md`).
- **Real namespace-wide waiter cap.** Added `max_waiters_per_namespace` to `NamespaceQuota` (§4 in `design.md`). Backed by a storage-trait counter (`CapacityKind::MaxWaitersPerNamespace`); aggregates correctly across replicas. Distinguished on the wire from `max_waiters_per_replica` via separate rejection codes — see problem 07 post-debate refinement.
- **Cardinality budget caps:** `max_error_classes` (default 64) and `max_task_types` (default 32) added to `NamespaceQuota`. `SetNamespaceConfig` rejects writes that would push the registry beyond these caps. Without them, a tenant registering 200 error classes blew up the metric series count (see problem 11 post-debate refinement).

## Round-2 refinement

Round 2 reversed two of the round-1 additions and added two validation rules:

- **Version-fenced quota cache removed.** Capacity-quota reads are inline transactional, no cache. v1's admit target (1K/sec/replica) makes the row-read overhead irrelevant against the SERIALIZABLE transaction's own cost; the version row itself was emerging as a hot-contention point. The fence + cache machinery is parked at `design.md` §14 as a v1.1 candidate, gated on benchmarks showing >2K admits/sec sustained per replica.
- **`max_waiters_per_namespace` deferred to v2.** Single-replica v1 has no operational gap that a namespace-wide cap closes (see problem 07 round-2 refinement). Removed from `NamespaceQuota` and from the rejection enum.
- **Two new `SetNamespaceQuota` validation rules.** `lazy_extension_threshold_seconds ≥ 2 × min_heartbeat_interval_seconds` (prevents the lazy-extension cadence trap; see problem 01 round-2 refinement). `max_idempotency_ttl_seconds ≤ 90 days` (caps the partition count for Postgres bounded-cost dedup expiration; see problem 03 round-2 refinement).
