<!-- agent-updated: 2026-04-30T03:35:00Z -->

# taskq-rs Architecture

A 10-minute on-ramp. For the full design see [`design.md`](design.md); for the discussion behind each decision see [`problems/`](problems/).

## What it is

A pure-Rust distributed task queue with a standardized contract. Callers submit tasks; workers long-poll, process, and report results. The control plane mediates state, persists everything, and enforces multi-tenant isolation. The protocol is the product — not the storage backend, not the strategies, not the SDK.

## System shape

```
   ┌──────────┐    SubmitTask         ┌─────────────────────┐
   │ Caller   │ ─────────────────────►│   Control Plane     │
   │ (any     │ ◄───── result ─────── │   (Rust, multi-     │
   │  client) │                       │    replica capable) │
   └──────────┘                       │                     │
                                      │  ┌──────────────┐   │
                                      │  │  Strategies  │   │       ┌────────────┐
                                      │  │ (compile-    │   │       │  Storage   │
                                      │  │  time linked)│   │ ◄────►│  backend   │
                                      │  └──────────────┘   │       │ (Postgres, │
                                      │                     │       │  SQLite,   │
                                      │  ┌──────────────┐   │       │  ...)      │
                                      │  │   Reapers    │   │       └────────────┘
                                      │  └──────────────┘   │              ▲
                                      └─────────────────────┘              │
                                          ▲       ▲                        │
                          AcquireTask     │       │  Heartbeat (carve-out) │
                          (long-poll)     │       │                        │
                                          │       │                        │
                                      ┌──────────────────┐                 │
                                      │     Workers      │                 │
                                      │ (Rust SDK; other │                 │
                                      │  langs self-mgr) │                 │
                                      └──────────────────┘                 │
                                                                           │
                  Wire: FlatBuffers over gRPC (pure-grpc-rs)               │
                  Storage: trait-based; backend chosen at compile time ────┘
```

The control plane is **multi-replica capable** by construction — no in-memory state lives on the correctness path. v1 ships single-replica deployments; multi-replica is a deployment decision, not a code change.

## Core concepts

| | |
|---|---|
| **Namespace** | Tenant identity. Quotas, metrics, idempotency, error-class registry, observability — all namespace-scoped. |
| **Task type** | String identifier (`llm.flash`, `scrape.browser`). Workers register the types they handle. |
| **Task** | Unit of work with opaque payload, idempotency key, TTL, retry config, lifecycle state. |
| **Attempt** | One execution of a task by a worker, identified by `(task_id, attempt_number)`. |
| **Lease** | Runtime record: "task X is being processed by worker W until time T." Persisted before `AcquireTask` returns. |
| **Worker** | Process that registers task types, long-polls, heartbeats, reports outcomes. |
| **Idempotency key** | Required on `SubmitTask`, scoped `(namespace, key)`. Bounded duplicate suppression. |

## Two pluggable seams

Everything important is one of these two:

### Storage backend

Backends implement a `Storage` + `StorageTx` trait pair. Strategies and the CP layer pass *intent* (e.g. `PickOrdering::PriorityFifo`); backends translate to native idiom. This keeps the protocol portable across backends without each backend reinventing CP logic.

Conformance has six requirements: external consistency, non-blocking row locking with skip semantics, indexed range scans, atomic conditional insert, subscribe-pending ordering invariant, and bounded-cost dedup expiration.

v1 ships **Postgres** (production) and **SQLite** (single-process embedded/dev only). Postgres requires `SERIALIZABLE` mode; SQLite's single-writer is trivially serializable but precludes multi-replica.

### Strategies

Two traits — `Admitter` (submit-time) and `Dispatcher` (acquire-time) — each with multiple compile-time-linked implementations. Operators select per namespace at CP startup; strategy *parameters* (quotas) live-update via admin RPCs, but strategy *choice* requires a rolling restart.

v1 ships three stateless admitters (`Always`, `MaxPending`, `CoDel`) and three stateless dispatchers (`PriorityFifo`, `AgePromoted`, `RandomNamespace`). Stateful strategies (`WeightedFair`, `TokenBucket`, `RoundRobin`) are deferred to v2 — the trait already supports them.

## Consistency model

Every state transition is a **serializable transaction**. Commit order matches real-time order — externally consistent. v1 reaches this on Postgres `SERIALIZABLE`; future backends (CockroachDB, Spanner, FoundationDB) provide it natively. Backends that can't (Redis without scripting, eventually-consistent KV stores) are explicitly out.

**Three carve-outs** are deliberately weakened for scaling reasons:

1. **Heartbeats** — written at READ COMMITTED to a dedicated `worker_heartbeats` table. Reaper B reads liveness from there. Lease `timeout_at` extends *lazily* — only when more than `lease − ε` has elapsed since the last extension. The steady-state heartbeat path is one cheap UPSERT per ping, not a SERIALIZABLE write.
2. **Rate quotas** (`max_submit_rpm` etc.) — eventually consistent within the cache TTL (default 5s). Capacity quotas (`max_pending` etc.) are read transactionally and are *not* eventually consistent.
3. **Trace parent-span retention** — context bytes are durably persisted on the task row; the parent span itself is exported via OTel best-effort.

Anything not listed here participates in external consistency.

## Task lifecycle

```
PENDING ──► DISPATCHED ──► COMPLETED
                 │      ──► FAILED_NONRETRYABLE
                 │      ──► FAILED_EXHAUSTED   (retries exhausted)
                 │      ──► EXPIRED            (TTL elapsed)
                 │      ──► WAITING_RETRY ──► PENDING (when retry_after ≤ NOW())
                 │      ──► CANCELLED          (admin or caller)
                 ▼
             (reaper reclaims → attempt_number++ → PENDING)
```

Terminal states: `COMPLETED`, `CANCELLED`, `FAILED_NONRETRYABLE`, `FAILED_EXHAUSTED`, `EXPIRED`.

**Idempotency key behavior by terminal state:** held for COMPLETED / FAILED_NONRETRYABLE / FAILED_EXHAUSTED / EXPIRED (the system's authoritative answer); released on CANCELLED (caller threw it away).

Retry math is server-side: `delay = min(initial × coefficient^attempt, max)` with full jitter `± 50%`. Workers never compute their own backoff. Per-task → per-task-type → per-namespace config layering.

## What v1 ships

- Postgres + SQLite backends
- Three admitters, three dispatchers — all stateless
- Rust caller and worker SDKs
- OTel-native observability with W3C trace context persisted per task
- Required idempotency keys (24h-after-termination default TTL)
- Heartbeat carve-out, lazy lease extension, BRIN-indexed liveness scans
- Two reapers (timeout + dead-worker), `40001` transparent retry, `LEASE_EXPIRED` distinct error code
- Per-namespace quotas (capacity + rate + per-task ceilings + cardinality budget)
- Per-replica waiter cap (namespace-wide cap deferred to v2)
- All admin RPCs (`SetNamespaceQuota`, `SetNamespaceConfig`, `PurgeTasks`, `ReplayDeadLetters`, `CancelTask`, etc.)
- `taskq-cp migrate` CLI for explicit migrations
- FlatBuffers schema diff CI (depends on `flatbuffers-rs` shipping the diff tool)

## What v1 does not ship

- Stateful dispatchers (WeightedFair, TokenBucket, RoundRobin)
- Auto poison-pill detection beyond `max_retries`; circuit-breaking
- Per-task-type capacity quotas; hierarchical namespaces
- Cross-region replication
- Multi-task pull (batched `AcquireTask`)
- Real-time event streaming (`SubscribeTaskEvents`)
- Web UI; other-language SDKs (the protocol allows them, we just don't release any)
- Build-time CI cardinality lint (review checklist instead)
- Version-fenced quota cache (capacity reads inline; revisit if benchmarks show >2K admits/sec/replica)
- Namespace-wide waiter cap

## Where to go next

- [`README.md`](README.md) — project goals, status
- [`design.md`](design.md) — full consolidated design (~900 lines, section-numbered)
- [`problems/`](problems/) — 12 problem docs with the discussion trail (failure modes, prior art, alternatives, two rounds of adversarial debate)
- [`tasks.md`](tasks.md) — implementation roadmap for v1
- [`problems/12-storage-abstraction.md`](problems/12-storage-abstraction.md) — start here if you want to add a storage backend
- [`problems/05-dispatch-fairness.md`](problems/05-dispatch-fairness.md) — start here if you want to add a strategy
