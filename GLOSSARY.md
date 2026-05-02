<!-- agent-updated: 2026-05-02T21:35:28Z -->

# Glossary

Terms used across the `taskq-rs` documentation. Sources: [`design.md`](./design.md)
§2 (core concepts), §5 (state machine), §10.1 (rejection reasons), §11.3
(metric labels); the [`problems/`](./problems/) docs for the deeper
discussions; [`protocol/QUOTAS.md`](./protocol/QUOTAS.md) for quota
field semantics. Alphabetical.

---

**Acquire** — The act of a worker calling `AcquireTask` to long-poll for the
next dispatchable task. The CP runs the namespace's `Dispatcher` strategy
and, on success, transitions the task to `DISPATCHED` and inserts a
`task_runtime` row. See [`design.md`](./design.md) §6.2.

**Admin RPC** — Any method on the `TaskAdmin` service. Includes
`SetNamespaceQuota`, `SetNamespaceConfig`, `EnableNamespace`,
`DisableNamespace`, `PurgeTasks`, `ReplayDeadLetters`, `GetStats`,
`ListWorkers`. Every admin RPC writes a same-transaction `audit_log` row.

**Admitter** — Strategy trait gating submit-time admission. Compile-time
linked into the CP binary; runtime-selected per namespace. v1 ships
`Always`, `MaxPending`, `CoDel`. See [`design.md`](./design.md) §7.1.

**Append-only registry** — Per-namespace `error_class` and `task_type`
tables that allow additions and `deprecated` flips but never removals. The
load-bearing defense against orphaned historical data. See
[`design.md`](./design.md) §9.4 / [`protocol/EVOLUTION.md`](./protocol/EVOLUTION.md) §9.

**Attempt** — A single execution of a task by a worker. Identified by
`(task_id, attempt_number)`. Tasks have multiple attempts when retried;
attempts under one task share a parent trace span. See
[`design.md`](./design.md) §2.

**Audit log** — Append-only `audit_log` table written same-transaction as
the admin RPC it records. Carries `actor`, `rpc`, `namespace`,
`request_summary` (JSONB), `result`, `request_hash` (sha256). Pruned by
the periodic retention job per `audit_log_retention_days`. See
[`design.md`](./design.md) §11.4.

**Backoff** — Server-side retry timing, computed with full jitter:
`delay = min(initial_ms * coefficient^attempt, max_ms);
retry_after = now + rand(delay/2, delay*3/2)`. Workers never compute their
own. See [`design.md`](./design.md) §6.5.

**BRIN index** — Block Range Index. Cheap-to-maintain index on
`worker_heartbeats(last_heartbeat_at)` for Reaper B's "scan everything
older than T" pattern. Does not defeat HOT updates. See
[`design.md`](./design.md) §8.3.

**`CANCELLED`** — Terminal state. Caller-driven cancellation. Releases the
idempotency key for immediate reuse (the only terminal state that does
so). See [`design.md`](./design.md) §5, §6.7.

**Capacity quota** — Quota whose enforcement state is read transactionally
inside the SERIALIZABLE transaction. Includes `max_pending`,
`max_inflight`, `max_workers`, `max_waiters_per_replica`. No cache; no
eventual-consistency window. See [`design.md`](./design.md) §9.1.

**Cardinality budget** — Per-namespace caps on registry size
(`max_error_classes` default 64, `max_task_types` default 32) that bound
worst-case Prometheus series count. The load-bearing fix from
[`problems/11`](./problems/11-observability.md) post-debate.

**`COMPLETED`** — Terminal state. Worker reported success.
Idempotency key held for TTL. See [`design.md`](./design.md) §5.

**Conformance suite** — `taskq-storage-conformance` crate with shared tests
every storage backend must pass. Validates external consistency, skip
locking, indexed range scans, atomic conditional inserts, subscribe-pending
ordering, bounded-cost dedup expiration. See [`design.md`](./design.md)
§8.2.

**Control plane (CP)** — The `taskq-cp` server. Stateless re: correctness;
all durable state lives in the storage backend. Multi-replica capable
from day one. See [`design.md`](./design.md) §1.

**`DISPATCHED`** — Non-terminal state. Task has been handed to a worker
and a `task_runtime` row exists. Transitions to `COMPLETED`,
`FAILED_NONRETRYABLE`, `FAILED_EXHAUSTED`, `EXPIRED`, `WAITING_RETRY`,
`CANCELLED`, or back to `PENDING` (via reaper). See
[`design.md`](./design.md) §5.

**Dispatcher** — Strategy trait gating dispatch-time selection.
Compile-time linked; runtime-selected per namespace. v1 ships
`PriorityFifo`, `AgePromoted`, `RandomNamespace`. See
[`design.md`](./design.md) §7.2.

**ε (epsilon)** — `lazy_extension_threshold_seconds`. Heartbeat fires a
SERIALIZABLE lease extension iff `(NOW − task_runtime.last_extended_at) ≥
(lease_duration − ε)`. Validated `≥ 2 × min_heartbeat_interval_seconds`
at `SetNamespaceQuota`. See [`design.md`](./design.md) §6.3.

**External consistency** — Strict serializability for state-transition
transactions; commit order respects real-time order. The protocol
contract; v1 reference impl achieves it via Postgres `SERIALIZABLE`. See
[`design.md`](./design.md) §1.

**`EXPIRED`** — Terminal state. Task TTL elapsed before completion or
retry. Idempotency key held — TTL is the system's authoritative answer.
See [`design.md`](./design.md) §5.

**`FAILED_EXHAUSTED`** — Terminal state. Retryable failure but
`attempt >= max_retries`. Idempotency key held. See
[`design.md`](./design.md) §5.

**`FAILED_NONRETRYABLE`** — Terminal state. Worker reported `retryable=false`.
Idempotency key held — authoritative "this work doesn't succeed". See
[`design.md`](./design.md) §5.

**FlatBuffers** — On-the-wire schema language for `taskq-rs` RPCs.
Append-only field discipline; never reorder; never change types;
deprecation via the `(deprecated)` attribute. See
[`protocol/EVOLUTION.md`](./protocol/EVOLUTION.md) §2.

**`format_version`** — Per-row stored-data version on `tasks` and
`taskq_meta`. Decouples wire compatibility from storage compatibility.
Reader tolerates older formats; writer emits current. Bumped only when
older code cannot read newer rows. See [`design.md`](./design.md) §12.6 /
[`protocol/EVOLUTION.md`](./protocol/EVOLUTION.md) §8.

**Heartbeat** — Worker-to-CP liveness ping. READ COMMITTED upsert into
`worker_heartbeats`; not a state transition. Lazy lease extension fires
when ε threshold elapsed. See [`design.md`](./design.md) §6.3.

**Idempotency key** — Caller-supplied string (required on `SubmitTask`)
for bounded duplicate-suppression. Scoped `(namespace, key)`. Held for
the configured TTL by every terminal state except `CANCELLED`. See
[`design.md`](./design.md) §6.1, §5.

**`IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`** — Permanent rejection
reason. Caller submitted with an existing key but a different
`payload_hash`. Carries the existing `task_id` for caller diagnosis. See
[`design.md`](./design.md) §10.1.

**`INCOMPATIBLE_VERSION`** — Wire-version handshake rejection. Carries
`server_version` + `supported_client_range`. Major-version mismatch
only. See [`design.md`](./design.md) §3.3,
[`protocol/EVOLUTION.md`](./protocol/EVOLUTION.md) §4.

**`INVALID_PAYLOAD`** — Permanent rejection (retryable=false). Submitted
payload exceeds `max_payload_bytes` or otherwise fails request-level
validation. See [`design.md`](./design.md) §10.1.

**Lease** — Runtime record of "task X is being processed by worker W
until time T." Persisted as a `task_runtime` row before `AcquireTask`
returns. Extended lazily via heartbeat. Reclaimed by Reaper A or
Reaper B. See [`design.md`](./design.md) §6.3, §6.6.

**`LEASE_EXPIRED`** — Returned to a worker calling `CompleteTask` /
`ReportFailure` whose lease was already reclaimed by the reaper. Worker
should log and drop. The transparent `40001` retry pattern in §6.4 makes
this a reliable signal rather than an opaque transient error. See
[`design.md`](./design.md) §6.4.

**LISTEN/NOTIFY** — Postgres pub/sub primitive used by `subscribe_pending`
on the Postgres backend. CP runs a dedicated LISTEN connection demuxed via
mpsc to per-namespace subscribers. The 10s belt-and-suspenders re-query
covers connection drops. See [`design.md`](./design.md) §8.4.

**Long-poll** — `AcquireTask`'s waiting model. Worker carries
`long_poll_timeout` (default 30s, max 60s); CP either returns a task or
returns `no_task: true` on timeout. See [`design.md`](./design.md) §6.2.

**`MAX_PENDING_EXCEEDED`** — Rejection reason. Namespace's `max_pending`
quota is exceeded at `SubmitTask`. Carries `pending_count` and
`pending_limit`. See [`design.md`](./design.md) §10.1.

**Multi-replica capable** — No CP-local in-memory state lives on the
correctness path. A worker can reconnect to any replica mid-task and
operations continue regardless of replica count. v1 ships single-replica
deployments by default; multi-replica is supported but not the default.
See [`design.md`](./design.md) §1.

**Namespace** — Tenant identity. Every task carries one. Quotas, metrics,
idempotency, error-class registry, and observability config are
namespace-scoped. See [`design.md`](./design.md) §2.

**`NAMESPACE_DISABLED`** — Permanent rejection (retryable=false).
Operator disabled the namespace via `DisableNamespace`. See
[`design.md`](./design.md) §10.1.

**`NAMESPACE_QUOTA_EXCEEDED`** — Generic rejection reason for
quota-exhausted submits/dispatches. Specific quota types (e.g.
`MAX_PENDING_EXCEEDED`) override this when the storage layer can attribute
the cause. See [`design.md`](./design.md) §10.1.

**`PENDING`** — Initial non-terminal state. Task is awaiting dispatch.
Transitions to `DISPATCHED` (acquire) or `CANCELLED` (cancel). See
[`design.md`](./design.md) §5.

**Rate quota** — Quota whose enforcement state is cached at the CP layer
with 5s default TTL. Eventually consistent within the cache window.
Includes `max_submit_rpm`, `max_dispatch_rpm`, `max_replay_per_second`.
See [`design.md`](./design.md) §1.1, §9.1.

**Reaper** — Background process that reclaims expired leases. Two reapers
run concurrently with `FOR UPDATE SKIP LOCKED`:
- **Reaper A** — per-task timeout. Scans `task_runtime.timeout_at <= NOW()`.
- **Reaper B** — dead-worker reclaim. Scans `worker_heartbeats` joined
  to `task_runtime` for stale heartbeats. Stamps `declared_dead_at` on
  reclaimed workers' heartbeat rows.
See [`design.md`](./design.md) §6.6.

**`REPLICA_WAITER_LIMIT_EXCEEDED`** — Rejection reason. Per-replica
waiter cap exceeded at `AcquireTask`. SDK retries with backoff. (Was
called `WAITER_LIMIT_EXCEEDED` in some pre-round-2 docs;
`max_waiters_per_namespace` is deferred to v2.) See
[`design.md`](./design.md) §10.1, [`problems/10`](./problems/10-noisy-neighbor.md)
round-2.

**`SERIALIZABLE`** — Postgres isolation level used for state-transition
transactions. Achieves the protocol's external-consistency guarantee. The
heartbeat path is the carve-out at `READ COMMITTED`. See
[`design.md`](./design.md) §1.

**SemVer** — Semantic versioning for the protocol. `taskq.v1` is the first
major. MINOR adds backwards-compatible features; MAJOR breaks compat.
**No public stability commitment** — major bumps happen at our discretion.
See [`design.md`](./design.md) §12.1.

**`SKIP LOCKED`** — `FOR UPDATE SKIP LOCKED` clause used by
`pick_and_lock_pending` and the reapers. Skips rows held by concurrent
acquirers rather than blocking. See [`design.md`](./design.md) §8.2 #2.

**Strategy** — Pluggable per-namespace decision logic. Two seams: Admitter
(submit-time) and Dispatcher (dispatch-time). Compile-time linked;
runtime-selected per namespace at CP startup; no hot-swap. See
[`design.md`](./design.md) §7.

**`subscribe_pending`** — Storage-trait method that returns a stream of
wake signals when matching tasks become PENDING. Implementation per
backend: Postgres LISTEN + 500ms fallback poll; SQLite 500ms poll;
FoundationDB native watch. The 10s belt-and-suspenders unconditional
re-query covers missed notifications under any backend. See
[`design.md`](./design.md) §8.4.

**`SYSTEM_OVERLOAD`** — Rejection reason when a non-namespace-specific
overload condition fires (e.g., CP is shedding load due to memory
pressure). See [`design.md`](./design.md) §10.1.

**Task** — Unit of work with an opaque payload, an idempotency key, a
TTL, retry config, and a lifecycle state. See
[`design.md`](./design.md) §2.

**Task type** — String identifier (e.g., `llm.flash`). Workers register
the types they handle; dispatch filters by type. See
[`design.md`](./design.md) §2.

**`taskq_meta`** — Single-row metadata table holding
`(schema_version, format_version, last_migrated_at)`. CP startup
checks compatibility; refuses to start with mismatch. See
[`design.md`](./design.md) §12.5,
[`protocol/EVOLUTION.md`](./protocol/EVOLUTION.md) §6.1.

**Terminal state** — `COMPLETED`, `CANCELLED`, `FAILED_NONRETRYABLE`,
`FAILED_EXHAUSTED`, `EXPIRED`. No further transitions allowed except
admin `ReplayDeadLetters` (which resets terminal-failure tasks back to
`PENDING`). See [`design.md`](./design.md) §5.

**Trace context** — W3C `traceparent` (55 bytes) + `tracestate` (variable,
≤ 256 bytes). Carried on every RPC that crosses a tracing boundary;
persisted on the task row at submit; returned to the worker on `AcquireTask`.
Survives retries, reaper reclaim, CP restart. See
[`design.md`](./design.md) §3.4, §11.2.

**`UNKNOWN_ERROR_CLASS`** — Rejection at `ReportFailure` for an
unregistered `error_class`. Worker SDK MAY refresh by re-`Register`ing.
See [`design.md`](./design.md) §6.3 (RegisterWorkerResponse contract),
[`design.md`](./design.md) §6.4 (ReportFailure validation).

**`UNKNOWN_TASK_TYPE`** — Permanent rejection (retryable=false).
Submitted task type is not registered for the namespace. Carries the
submitted type string. See [`design.md`](./design.md) §10.1.

**v2** — The next major. Bumped only when an append-only fix is
impossible, behavioral semantics change in a way feature flags cannot
model, or accumulated cruft justifies a clean redesign. See
[`protocol/EVOLUTION.md`](./protocol/EVOLUTION.md) §5.

**Waiter** — A worker parked on `AcquireTask` long-poll. Counted against
`max_waiters_per_replica` (and CP-wide `waiter_limit_per_replica`).
Independent of `max_workers` — a worker can be registered without holding
a waiter. See [`design.md`](./design.md) §6.2,
[`protocol/QUOTAS.md`](./protocol/QUOTAS.md) §2.4.

**`WAITING_RETRY`** — Non-terminal state. Worker reported retryable failure
with attempts remaining; CP set `retry_after`. Transitions back to
`PENDING` when `retry_after <= NOW()`. May be `CANCELLED` directly. See
[`design.md`](./design.md) §5.

**Worker** — A process that registers task types, long-polls for work,
processes tasks, heartbeats, and reports outcomes. See
[`design.md`](./design.md) §2.

**`WORKER_DEREGISTERED`** — Returned to a worker whose
`worker_heartbeats.declared_dead_at` was stamped by Reaper B. SDK must
call `Register` again before continuing. Prevents transparent
resurrection of a worker whose tasks were reclaimed. See
[`design.md`](./design.md) §6.3, §6.6.

**Wire-version handshake** — First RPC's negotiation of `client_version`
vs `server_version` + `supported_client_range`. Major-version mismatch
returns `INCOMPATIBLE_VERSION`. Within compatible majors, both sides
ignore unknown fields per FlatBuffers semantics. See
[`design.md`](./design.md) §3.3.
