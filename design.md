<!-- agent-updated: 2026-04-30T03:30:00Z -->

# taskq-rs Design

This document consolidates the design decisions made across `problems/01` through `problems/12`. It captures *what we built and why* in a reader-oriented narrative; the problem docs retain the full discussion (failure modes, prior-art comparisons, alternatives considered) and are cross-referenced throughout.

For project goals see `README.md`. For the problem index see `problems/README.md`.

---

## 1. Architecture at a glance

taskq-rs is a Rust task queue with a FlatBuffers-over-gRPC wire schema, served by a control plane built on `pure-grpc-rs`, with a pluggable storage layer behind it.

Two pluggable seams shape every decision below:

- **Storage backend** (`problems/12`) — backends implement a `Storage` + `StorageTx` trait pair. v1 ships Postgres and SQLite. Backends must provide external consistency, non-blocking row locking with skip semantics, indexed range scans, and atomic conditional inserts.
- **Strategies** (`problems/05`) — `Admitter` and `Dispatcher` traits decide submit-time admission and dispatch-time selection. Compile-time linked into the binary, runtime-selected per namespace at CP startup, no hot-swap.

The control plane is **multi-replica capable from day one**, but v1 ships single-replica deployments. "Multi-replica capable" means no CP-local in-memory state lives on the correctness path — *not* that v1 horizontally scales throughput. A worker can reconnect to any replica mid-task and operations continue, regardless of how many replicas are deployed.

The consistency model is **external consistency** — every state transition is a serializable transaction, commit order matches real-time order. The protocol commits to this property; the v1 reference implementation achieves it via Postgres `SERIALIZABLE` isolation. Future backends (CockroachDB, Spanner, FoundationDB) can implement the same property.

### 1.1 Consistency carve-outs

External consistency is the contract for *state transitions* (submit, acquire, complete, retry, reclaim, cancel, admin actions). Three operational paths are explicitly carved out at weaker semantics for scaling reasons:

- **Worker heartbeats** are written at READ COMMITTED to a dedicated `worker_heartbeats` table. Heartbeats are best-effort liveness signals; losing a few across a CP crash does not violate at-least-once delivery. Reaper B (dead-worker reclaim) reads from `worker_heartbeats`. The lease's `timeout_at` on `task_runtime` is extended *lazily* — only when more than `lease − ε` has elapsed since the last extension (semantics in §6.3) — keeping the SERIALIZABLE write rate down to actual lease extensions, not every keepalive ping. See `problems/01` and `problems/02`.
- **Rate quotas** (`max_submit_rpm`, `max_dispatch_rpm`, `max_replay_per_second`) are eventually consistent within the namespace-config cache TTL (default 5s). Aggregate global rate may briefly exceed the configured rate after a downward `SetNamespaceQuota` write; admission self-corrects within the TTL window. Capacity quotas (`max_pending`, `max_inflight`, `max_workers`) are read transactionally and are NOT eventually consistent — see §9.1.
- **Trace context** bytes (W3C `traceparent` / `tracestate`) are durably persisted on the task row; the *parent span itself* is exported via OTel best-effort and retained per the operator's OTel pipeline configuration. Workers continuing a trace receive the durable context; whether the parent span is queryable in the tracing backend is governed by the OTel buffer/retention, not by taskq-rs.

These carve-outs are deliberate. Anything not listed here participates in external consistency.

Reference: `problems/02`, `problems/12`.

---

## 2. Core concepts

| Concept | Definition |
|---|---|
| **Namespace** | Tenant identity. Every task carries one. Quotas, metrics, idempotency, error-class registry, and observability config are namespace-scoped. |
| **Task type** | String identifier (e.g. `llm.flash`). Workers register the types they handle. Dispatch filters by task type. |
| **Task** | A unit of work with an opaque payload, an idempotency key, a TTL, retry config, and a lifecycle state. |
| **Attempt** | A single execution of a task by a worker. Identified by `(task_id, attempt_number)`. Tasks have multiple attempts when retried. |
| **Lease** | The runtime record of "task X is being processed by worker W until time T." Persisted before `AcquireTask` returns. |
| **Worker** | A process that registers task types, long-polls for work, processes tasks, heartbeats, and reports outcomes. |
| **Idempotency key** | Caller-supplied string (required on `SubmitTask`) for bounded duplicate-suppression. Scoped `(namespace, key)`. |
| **Control plane (CP)** | The taskq-rs server. Stateless re: correctness; all durable state in the storage layer. |

---

## 3. Wire protocol

### 3.1 Schema and transport

- **Schema language:** FlatBuffers (`.fbs`), compiled by [`flatbuffers-rs`](https://github.com/Shuozeli/flatbuffers-rs).
- **Transport:** gRPC over [`pure-grpc-rs`](https://github.com/Shuozeli/pure-grpc-rs).
- **Schema discipline:** append-only, FlatBuffers `deprecated` attribute used for retired fields, never reorder, never change types. Diff tool lives in `flatbuffers-rs`.

Reference: `problems/09` (versioning rules), README (stack choice).

### 3.2 RPC services

```
service TaskQueue {       // caller-facing
  SubmitTask
  GetTaskResult
  BatchGetTaskResults
  CancelTask
  SubmitAndWait
}

service TaskWorker {      // worker-facing
  Register
  AcquireTask              // long-poll, single task
  Heartbeat
  CompleteTask
  ReportFailure
  Deregister
}

service TaskAdmin {        // operator-facing
  SetNamespaceQuota
  GetNamespaceQuota
  SetNamespaceConfig       // strategies, error_class registry
  EnableNamespace / DisableNamespace
  PurgeTasks
  ReplayDeadLetters
  GetStats
  ListWorkers
}
```

### 3.3 Version handshake

Every first RPC from a client (caller or worker) carries `client_version: SemVer`; server responds with `server_version` + `supported_client_range`. Major-version mismatch is rejected with `INCOMPATIBLE_VERSION` carrying the supported range. Within compatible majors, both sides ignore unknown fields per FlatBuffers semantics.

Reference: `problems/09`.

### 3.4 Trace context

Every RPC that crosses a tracing boundary carries W3C Trace Context (`traceparent`, `tracestate`):

- `SubmitTaskRequest` — caller supplies
- `AcquireTaskResponse` — CP returns the *task's* persisted trace context for the worker
- `CompleteTaskRequest` / `ReportFailureRequest` — worker continues

The trace context bytes are stored on the task row at submit time. Workers receive the persisted W3C context on `AcquireTask` and MAY emit spans linking to the parent. Retries are sibling spans under one parent task span; reaper actions emit child spans. Parent-span retention beyond the OTel exporter's buffer is governed by the operator's tracing pipeline, not by taskq-rs (see §1.1 carve-outs).

Reference: `problems/11`.

---

## 4. Logical data model

The CP's logical data model. Physical schemas differ per storage backend (`problems/12`); each backend translates these entities to its native idiom.

### `tasks`

```
task_id            : ULID/UUIDv7  (time-ordered, no coordination)
namespace          : String
task_type          : String
status             : enum         (see §5)
priority           : i32
payload            : bytes        (FlatBuffers wire bytes, opaque to CP)
payload_hash       : bytes(32)    (blake3 of payload, for idempotency mismatch check)
submitted_at       : Timestamp
expires_at         : Timestamp    (TTL)
attempt_number     : u32
max_retries        : u32
retry_initial_ms   : u64
retry_max_ms       : u64
retry_coefficient  : f32
retry_after        : Timestamp?   (when status = WAITING_RETRY)
traceparent        : bytes(55)
tracestate         : bytes        (variable, ≤ 256)
format_version     : u32          (for stored-data evolution)
original_failure_count : u32      (preserved across DLQ replay)
```

### `task_runtime` (lease)

```
task_id, attempt_number   : composite reference
worker_id                 : UUID
acquired_at               : Timestamp
timeout_at                : Timestamp     (per-task lease; extended lazily — see §6.3)
last_extended_at          : Timestamp
```

### `worker_heartbeats`

Carved out of the SERIALIZABLE path (§1.1). Heartbeats write here at READ COMMITTED.

```
worker_id           : UUID (PK)
namespace           : String
last_heartbeat_at   : Timestamp
declared_dead_at    : Timestamp?   // set by Reaper B when this worker's leases get reclaimed
```

Reaper B (dead-worker reclaim) reads `last_heartbeat_at` from this table when computing `task_runtime` rows to reclaim, and stamps `declared_dead_at` for any worker it reclaims. The heartbeat path filters `WHERE declared_dead_at IS NULL` — a worker whose tasks were reclaimed cannot transparently resurrect by continuing to ping; its heartbeat returns `WORKER_DEREGISTERED`, forcing a fresh `Register` call. `AcquireTask` similarly requires a non-dead worker. The lease's `timeout_at` is extended only lazily (semantics in §6.3), so the steady-state heartbeat path is one cheap UPSERT per ping, not a SERIALIZABLE write per ping.

### `task_results`

```
task_id, attempt_number    : composite reference
outcome                    : enum         (success, retryable_fail, nonretryable_fail, cancelled, expired)
result_payload             : bytes        (opaque, only when outcome = success)
error_class                : String?      (from registry; only on failure)
error_message              : String?
error_details              : bytes?       (≤ 64 KB)
recorded_at                : Timestamp
```

### `idempotency_keys`

```
namespace, key       : composite primary key (and unique index)
task_id              : reference
payload_hash         : bytes(32)
expires_at           : Timestamp
```

### `namespace_quota`

```
namespace                          : String  (PK; "system_default" reserved for global default)
admitter_kind                      : enum
admitter_params                    : JSON
dispatcher_kind                    : enum
dispatcher_params                  : JSON

// Capacity (read inline INSIDE the admit/acquire SERIALIZABLE transaction — see §9.1)
max_pending                        : Option<u64>
max_inflight                       : Option<u64>
max_workers                        : Option<u32>
max_waiters_per_replica            : Option<u32>
// (max_waiters_per_namespace deferred to v2 — single-replica v1 has no shared-counter need)

// Rate (token bucket; eventually consistent within cache TTL — see §1.1)
max_submit_rpm                     : Option<RateLimit>
max_dispatch_rpm                   : Option<RateLimit>
max_replay_per_second              : Option<u32>

// Per-task ceilings
max_retries_ceiling                : u32
max_idempotency_ttl_seconds        : u64         // hard ceiling 90 days; partition-count tied to this — see §8.3
max_payload_bytes                  : u32         // default 10 MB
max_details_bytes                  : u32         // default 64 KB
min_heartbeat_interval_seconds     : u32

// Lease/heartbeat
//   ε must satisfy: ε ≥ 2 × min_heartbeat_interval_seconds  (validated at SetNamespaceQuota — see §9.1)
lazy_extension_threshold_seconds   : u32         // default 25% of lease duration

// Cardinality budget (enforced at SetNamespaceConfig)
max_error_classes                  : u32         // default 64
max_task_types                     : u32         // default 32

// Observability
trace_sampling_ratio               : f32
log_level_override                 : Option<LogLevel>
audit_log_retention_days           : u32         // default 90
metrics_export_enabled             : bool
```

### `error_class_registry`

```
namespace, error_class    : composite PK (append-only)
deprecated                : bool
registered_at             : Timestamp
```

### `audit_log`

```
id, timestamp, actor, rpc, namespace, request_summary (JSONB), result, request_hash (sha256)
```

### `task_type_retry_config`

Per-`(namespace, task_type)` retry config layer. Optional. Overlays per-namespace defaults; overlaid by per-task at submit.

### `taskq_meta`

```
schema_version, format_version, last_migrated_at
```

Single-row metadata table.

Reference: `problems/03`, `problems/05`, `problems/08`, `problems/09`, `problems/10`, `problems/11`.

---

## 5. Task state machine

```
PENDING ──────► DISPATCHED ──────► COMPLETED
                     │              ──► FAILED_NONRETRYABLE
                     │              ──► FAILED_EXHAUSTED  (retries exhausted)
                     │              ──► EXPIRED            (TTL elapsed before completion or retry)
                     │              ──► WAITING_RETRY ──► PENDING (when retry_after <= NOW())
                     │              ──► CANCELLED          (admin or caller)
                     ▼
                 (lease reaped → attempt_number++ → PENDING)

PENDING       ──► CANCELLED
WAITING_RETRY ──► CANCELLED
```

**Terminal states:** `COMPLETED`, `CANCELLED`, `FAILED_NONRETRYABLE`, `FAILED_EXHAUSTED`, `EXPIRED`.

**Idempotency-key holding behaviour by terminal state:**

| Terminal | Key behaviour |
|---|---|
| `COMPLETED` | **Held** for TTL (default `terminated_at + 24h`) — return cached terminal state on retry |
| `FAILED_NONRETRYABLE` | **Held** — authoritative "this work doesn't succeed" |
| `FAILED_EXHAUSTED` | **Held** — same idempotency class as nonretryable |
| `EXPIRED` | **Held** — TTL is the system's authoritative answer |
| `CANCELLED` | **Reusable immediately** — caller threw it away, fresh start allowed |

Callers who want a fresh attempt after a non-cancelled terminal state must use a new key.

Reference: `problems/03`, `problems/08`.

---

## 6. Lifecycle flows

State-transition flows below execute as single SERIALIZABLE transactions in the storage layer. Each transaction commits before the corresponding RPC reply is sent. The carve-outs from §1.1 (heartbeats, rate quotas) execute at the weaker semantics described there.

### 6.1 SubmitTask

Required: `namespace`, `task_type`, `payload`, `idempotency_key`, `traceparent`. Optional: per-task retry config overrides, `max_retries`, `priority`, `expires_at`.

```
1.  Run Admitter (per-namespace strategy from #05)
       - Calls storage trait's check_capacity_quota / try_consume_rate_quota
       - On reject: return RESOURCE_EXHAUSTED with structured rejection (#06)
2.  Lookup idempotency_keys[(namespace, idempotency_key)]
       - If present and unexpired: compare payload_hash
            - Match: return existing task_id + current status (no new task)
            - Mismatch: reject IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD (#03)
       - If present but expired: delete the row (lazy cleanup)
3.  Resolve retry config: per-task → per-task-type → per-namespace defaults
4.  Insert tasks row with status PENDING, attempt_number=0, persist traceparent
5.  Insert idempotency_keys row with computed expires_at, payload_hash
6.  Commit transaction
7.  Return SubmitTaskResponse{ task_id, status: PENDING }
```

### 6.2 AcquireTask (long-poll)

Worker call carries `task_types: [...]`, optional `long_poll_timeout` (default 30s, max 60s), and worker identity from prior `Register`.

```
1.  Subscribe to per-namespace pending notifications via storage trait
       (Postgres LISTEN, FoundationDB watch, SQLite poll - per #07/§8.4)
2.  Run Dispatcher (per-namespace strategy from #05)
       - PickCriteria { namespace_filter, task_types, ordering }
       - Storage executes pick_and_lock_pending in native idiom
       - Backends use FOR UPDATE SKIP LOCKED (Postgres) or equivalent
3.  If task found:
       a. Insert task_runtime row with worker_id, acquired_at, timeout_at
       b. Update task.status = DISPATCHED, task.attempt_number unchanged
       c. Commit transaction
       d. Return AcquireTaskResponse{ task, traceparent, attempt_number }
4.  If no task and waiter limit not exceeded:
       a. Register as waiter for (namespace, task_types)
       b. Wait on subscription stream OR 10s belt-and-suspenders timer
       c. On wakeup: re-run from step 2 (single-waiter wakeup; loser re-blocks)
       d. On long_poll_timeout: return AcquireTaskResponse{ no_task: true }
5.  If waiter limit exceeded:
       Return RESOURCE_EXHAUSTED with reason REPLICA_WAITER_LIMIT_EXCEEDED (#10)
```

Worker validation: every `AcquireTask` confirms the worker's row in `worker_heartbeats` has `declared_dead_at IS NULL`. A worker that was previously declared dead by Reaper B (§6.6) cannot acquire new tasks; the call returns `WORKER_DEREGISTERED` and the SDK initiates a fresh `Register`.

v1 has only the per-replica waiter cap. A namespace-wide cap (`max_waiters_per_namespace`, requiring a shared counter across replicas) is deferred to v2 alongside multi-replica deployment as the default — single-replica v1 has no operational gap that the namespace-wide cap would close.

### 6.3 Heartbeat (READ COMMITTED carve-out — see §1.1)

Heartbeats are best-effort liveness signals, not state transitions. They run at READ COMMITTED isolation and write only to `worker_heartbeats`, never to `task_runtime` on the steady-state path.

```
1.  Validate: requested heartbeat interval ≥ namespace.min_heartbeat_interval_seconds
2.  INSERT INTO worker_heartbeats (worker_id, namespace, last_heartbeat_at) VALUES ($1, $2, NOW())
    ON CONFLICT (worker_id) DO UPDATE
        SET last_heartbeat_at = EXCLUDED.last_heartbeat_at
        WHERE worker_heartbeats.declared_dead_at IS NULL
       (READ COMMITTED; UPSERT; rows-affected=0 means the worker was declared dead)
    If 0 rows affected: return WORKER_DEREGISTERED — SDK must re-Register before continuing
3.  Lazy lease extension. For each lease (task_id, attempt) the worker owns, fire SERIALIZABLE extension iff
       (NOW − task_runtime.last_extended_at) ≥ (lease_duration − ε)
    where ε = namespace.lazy_extension_threshold_seconds (default 25% of lease).
    Extension: UPDATE task_runtime SET timeout_at = NOW() + lease_duration, last_extended_at = NOW()
               WHERE (task_id, attempt_number) = ... AND worker_id = $1
4.  Return HeartbeatResponse with optional control signals (drain, terminate)
```

**ε semantics — symmetric on `last_extended_at`, not `timeout_at`.** Earlier framing ("fires when within ε of expiry") had a calibration trap: a worker heartbeating every cadence-interval against a wall-clock-distance trigger could miss the extension window if cadence > ε. The new framing — "fire if more than `(lease − ε)` has elapsed since the last extension" — is symmetric: every heartbeat past the first ε-window from the last extension fires, deterministically.

**ε ≥ 2 × min_heartbeat_interval invariant.** `SetNamespaceQuota` rejects configs where `lazy_extension_threshold_seconds < 2 × min_heartbeat_interval_seconds`. This guarantees that at least two heartbeats per ε-window have a chance to fire extension — covers a single dropped heartbeat without losing the lease.

**SDK heartbeat-cadence guidance.** The Rust worker SDK clamps its default heartbeat cadence to `≤ ε / 3` (read from `Register` response — see below); operators overriding the cadence to a value > ε / 3 receive a startup warning. This is SDK convention, not protocol.

**`RegisterWorkerResponse` contract.** When a worker calls `Register`, the CP returns:
- `lease_duration_ms` and `eps_ms` — so the SDK can compute the correct heartbeat cadence (§6.3 invariants are namespace-resolved) and clamp accordingly
- `error_classes: [string]` — the namespace's currently-registered `error_class` set, so the worker SDK can build a typed enum at init time and reject `ReportFailure` calls with unknown classes at compile time (§9.4)
- `worker_id` (UUID assigned by the CP) — used in subsequent `AcquireTask` / `Heartbeat` / `CompleteTask` / `ReportFailure` calls

If the namespace's error-class registry changes after `Register` (operator runs `SetNamespaceConfig`), the next `ReportFailure` carrying an unknown class returns `UNKNOWN_ERROR_CLASS`; the SDK MAY refresh by re-`Register`ing.

Reaper B (§6.6) reads `worker_heartbeats.last_heartbeat_at` to identify dead workers and stamps `declared_dead_at` on any worker whose tasks it reclaims. Reaper A continues to read `task_runtime.timeout_at` directly. Lazy extension keeps the SERIALIZABLE write rate down to actual lease extensions, not every keepalive ping.

A CP crash that loses a few heartbeat writes between READ COMMITTED commit and the next durable checkpoint is acceptable per at-least-once: at worst Reaper B sees a stale `last_heartbeat_at` and reclaims a still-live task; the worker's eventual `CompleteTask` returns `LEASE_EXPIRED` and the task runs once more on a different worker. The contract holds.

### 6.4 CompleteTask / ReportFailure

Both are idempotent on `(task_id, attempt_number)` — second call observes the existing terminal state and returns success without re-running side effects.

**Transparent serialization-conflict retry.** Under contention with the reaper or other concurrent transitions, these handlers may hit Postgres `40001` (or backend equivalent). The CP retries the transaction transparently up to 3 times with brief jittered backoff before classifying the outcome to the worker. The handler distinguishes:

- **`NotFound`** (no `task_runtime` row matching the worker's claimed `(task_id, attempt_number)`) → return `LEASE_EXPIRED`. The reaper has already reclaimed; the worker should log and drop.
- **`SerializationConflict`** (40001) → retry the transaction; only after exhausted retries surface as transient error.

This is what makes the §6.6 distinct-error-code commitment hold under load: workers reliably see `LEASE_EXPIRED` rather than fishing for it in opaque transient errors.

```
CompleteTask (with up to 3 transparent SerializationConflict retries):
1.  Validate worker owns the (task_id, attempt) lease
       SELECT FROM task_runtime WHERE (task_id, attempt_number) = ($1, $2) AND worker_id = $3 FOR UPDATE
       - 0 rows  → return LEASE_EXPIRED
2.  Insert task_results row with outcome = success
3.  Update task.status = COMPLETED
4.  Delete task_runtime row
5.  Commit transaction
       - 40001 → retry from step 1

ReportFailure (with up to 3 transparent SerializationConflict retries):
1.  Validate worker owns the lease (same FOR UPDATE pattern)
       - 0 rows → return LEASE_EXPIRED
2.  Validate error_class is in namespace's registered set
       (rejected with UNKNOWN_ERROR_CLASS otherwise — #08)
3.  Insert task_results row with outcome and error_details
4.  Decide terminal state (per §6.5)
5.  If WAITING_RETRY: compute retry_after using the task's retry config
6.  Update task.status accordingly
7.  Delete task_runtime row
8.  Commit transaction
       - 40001 → retry from step 1
```

### 6.5 Failure → terminal-state mapping

| Worker reports | Attempt | Resulting state |
|---|---|---|
| `retryable=false` | any | `FAILED_NONRETRYABLE` |
| `retryable=true`, `attempt < max_retries`, `retry_after ≤ task.expires_at` | n/a | `WAITING_RETRY` with `retry_after` set |
| `retryable=true`, `attempt < max_retries`, `retry_after > task.expires_at` | n/a | `EXPIRED` |
| `retryable=true`, `attempt >= max_retries` | n/a | `FAILED_EXHAUSTED` |

Backoff math (server-side, full jitter):

```
delay = min(initial_ms * coefficient^attempt, max_ms)
retry_after = now + rand(delay/2, delay*3/2)
```

Workers never compute their own retry timing.

Reference: `problems/08`.

### 6.6 Lease reclaim (reaper)

Two reapers run concurrently across CP replicas with `FOR UPDATE SKIP LOCKED`:

**Reaper A — per-task timeout:**
```
SELECT ... FROM task_runtime
WHERE timeout_at <= NOW()
FOR UPDATE SKIP LOCKED LIMIT 1000
```
For each: increment `task.attempt_number`, set status `PENDING`, delete runtime row, emit `taskq_lease_expired_total` and a reaper child span.

**Reaper B — dead worker:**
```
SELECT ... FROM task_runtime r JOIN worker_heartbeats h ON h.worker_id = r.worker_id
WHERE h.last_heartbeat_at < NOW() - lease_window
  AND h.declared_dead_at IS NULL
FOR UPDATE SKIP LOCKED LIMIT 1000
```
Reads from `worker_heartbeats` (not `workers`) since heartbeats are written there per §6.3. The query relies on a BRIN index on `worker_heartbeats(last_heartbeat_at)` — see §8.3. Reclaim handling matches Reaper A, plus one additional step: stamp `declared_dead_at = NOW()` on the reclaimed worker's `worker_heartbeats` row in the same transaction. This prevents the worker from transparently resurrecting via continued heartbeats; its next heartbeat receives `WORKER_DEREGISTERED` and the SDK re-`Register`s.

Both reapers tolerate `40001` (serialization conflict with concurrent `CompleteTask` / `Heartbeat` lazy-extension): the reaper transaction retries up to 3 times with brief backoff, then defers the row to the next reaper pass.

Either reaper firing reclaims the lease. Worker `CompleteTask` arriving after reclaim is rejected with `LEASE_EXPIRED` per §6.4.

Reference: `problems/01`, `problems/02`.

### 6.7 Admin actions and CancelTask

**`CancelTask`:** Caller-driven cancellation. Transitions the task to `CANCELLED` (terminal) and **deletes the `idempotency_keys` row** in the same SERIALIZABLE transaction. Per §5, `CANCELLED` releases the key — physical deletion (not a flag) implements that release. Subsequent `SubmitTask` with the same key creates a fresh task with no surprises. If the task is already in a terminal state, `CancelTask` is a no-op for state but still returns success (idempotent).

The state-transition + dedup-row-deletion pair is implemented as a private helper `cancel_internal(tx, task_id)` shared with `PurgeTasks` so the two paths cannot drift.

**`CancelTask` ↔ `SubmitTask` race ordering.** Under SERIALIZABLE, the later-committing transaction wins. If `CancelTask` commits first, the subsequent `SubmitTask` with the same key sees no dedup row and creates a fresh task. If `SubmitTask` commits first, `CancelTask` operates on the new task. Callers observing both commit orders is acceptable — they should not assume "I cancelled, so my retry will be fresh" without observing the cancel reply first.

**`SetNamespaceQuota`:** Validates the config (per §9.1: rejects nonsensical values, registry-cardinality caps, `max_idempotency_ttl_seconds ≤ 90 days`, `lazy_extension_threshold ≥ 2 × min_heartbeat_interval`), writes to `namespace_quota`, returns success. Capacity-quota changes are visible to the next admit/acquire transaction (capacity reads are inline). Rate-quota changes propagate within the rate-cache TTL. Strategy *choice* changes do NOT take effect until rolling restart. Audit log written same-transaction.

**`SetNamespaceConfig`:** Updates strategy choice, error_class registry additions, task_type registry additions. Rejects writes that would push `error_class_registry` or `task_type` count above the namespace's `max_error_classes` / `max_task_types` cap (preventing cardinality blow-up — see §11.3). Audit log written same-transaction.

**`PurgeTasks`:** For each task: cancel-if-not-terminal (which by §6.7 deletes the dedup row), delete task row, delete idempotency_keys row if not already deleted. Single SERIALIZABLE transaction per task. Rate-limited at the admin RPC (default 100/sec, configurable per namespace). Audit log written same-transaction.

**`ReplayDeadLetters`:** For matching tasks in terminal failure states (`FAILED_NONRETRYABLE`, `FAILED_EXHAUSTED`, `EXPIRED`), reset to `PENDING`, set `attempt_number=0`, preserve `original_failure_count`. Rejected if the task's idempotency key has since been claimed by another submission. Rate-limited at the admin RPC (default 100/sec, configurable per namespace). Audit log written.

Reference: `problems/03`, `problems/08`, `problems/10`.

---

## 7. Strategy framework

Two pluggable seams. Both compile-time linked into the CP binary; per-namespace selection at startup; no hot-swap; strategy *parameters* (quotas) live-update via `SetNamespaceQuota`.

### 7.1 Admitter trait

```rust
trait Admitter {
    async fn admit<S: StorageTx>(&self, ctx: &SubmitCtx, tx: &mut S) -> Admit;
}
enum Admit { Accept, Reject(RejectReason) }
```

v1 ships three stateless implementations:

| Strategy | Behaviour |
|---|---|
| `Always` | Accept everything |
| `MaxPending` | Reject if `pending_count(namespace) > limit` |
| `CoDel` | Adaptive — reject if oldest pending in namespace has aged past target latency |

### 7.2 Dispatcher trait

```rust
trait Dispatcher {
    async fn pick_next<S: StorageTx>(&self, ctx: &PullCtx, tx: &mut S) -> Option<TaskId>;
}
```

Worker `task_types` filter is applied as a pre-filter before any strategy logic. v1 ships three stateless implementations:

| Strategy | Behaviour |
|---|---|
| `PriorityFifo` | `ORDER BY priority DESC, submitted_at ASC` |
| `AgePromoted` | Effective priority = base priority + `f(age)`; older tasks bubble up |
| `RandomNamespace` | Sample uniformly from active namespaces, then `PriorityFifo` within |

Stateful strategies (`WeightedFair`, `TokenBucket`, `RoundRobin`) are deferred to v2. The trait already supports them; v1 just doesn't implement them.

Reference: `problems/05`.

---

## 8. Storage backend contract

The control plane interacts with persistence exclusively through the trait pair from `problems/12`. Backends translate intent to native idiom; the CP layer never sees backend-specific types.

### 8.1 Trait shape

```rust
trait Storage: Send + Sync + 'static {
    type Tx<'a>: StorageTx + 'a where Self: 'a;
    async fn begin(&self) -> Result<Self::Tx<'_>, StorageError>;
}

trait StorageTx: Send {
    // Submit
    async fn lookup_idempotency(&mut self, key: &IdempotencyKey) -> Result<Option<DedupRecord>>;
    async fn insert_task(&mut self, t: NewTask, dedup: NewDedupRecord) -> Result<TaskId>;

    // Dispatch
    async fn pick_and_lock_pending(&mut self, c: PickCriteria) -> Result<Option<LockedTask>>;
    async fn record_acquisition(&mut self, lease: NewLease) -> Result<()>;
    async fn subscribe_pending(&mut self, ns: &Namespace, types: &[TaskType])
        -> Result<impl Stream<Item = WakeSignal>>;

    // Worker (state transitions; SERIALIZABLE)
    async fn complete_task(&mut self, lease: &LeaseRef, outcome: TaskOutcome) -> Result<()>;

    // Reaper
    async fn list_expired_runtimes(&mut self, before: Timestamp, n: usize) -> Result<Vec<ExpiredRuntime>>;
    async fn reclaim_runtime(&mut self, runtime: &RuntimeRef) -> Result<()>;
    // mark_worker_dead: Reaper B stamps declared_dead_at on workers it reclaims (§6.6),
    //                   so the heartbeat path's WHERE declared_dead_at IS NULL filter (§6.3)
    //                   surfaces WORKER_DEREGISTERED to the worker SDK on the next ping.
    async fn mark_worker_dead(&mut self, worker_id: &WorkerId, at: Timestamp) -> Result<()>;

    // Quota enforcement
    //   capacity quotas: read transactionally; CapacityKind covers MaxPending, MaxInflight,
    //                    MaxWorkers, MaxWaitersPerReplica
    //   rate quotas: eventually consistent within cache TTL (§1.1 carve-out);
    //                backend chooses in-memory token bucket vs. shared counter
    async fn check_capacity_quota(&mut self, ns: &Namespace, kind: CapacityKind) -> Result<CapacityDecision>;
    async fn try_consume_rate_quota(&mut self, ns: &Namespace, kind: RateKind, n: u64) -> Result<RateDecision>;

    // Heartbeats (READ COMMITTED carve-out, §1.1)
    //   record_worker_heartbeat: UPSERT into worker_heartbeats; returns false if the worker
    //                            row has declared_dead_at != NULL (caller surfaces WORKER_DEREGISTERED)
    async fn record_worker_heartbeat(&mut self, worker_id: &WorkerId, namespace: &Namespace, at: Timestamp) -> Result<HeartbeatAck>;
    async fn extend_lease_lazy(&mut self, lease: &LeaseRef, new_timeout: Timestamp) -> Result<()>;

    // Cleanup
    async fn delete_expired_dedup(&mut self, before: Timestamp, n: usize) -> Result<usize>;

    // Admin / reads
    async fn get_namespace_quota(&mut self, ns: &Namespace) -> Result<NamespaceQuota>;
    async fn audit_log_append(&mut self, entry: AuditEntry) -> Result<()>;
    // ... etc.

    async fn commit(self) -> Result<()>;
    async fn rollback(self) -> Result<()>;
}
```

### 8.2 Conformance requirements

A backend is valid if and only if it provides:

1. **External consistency** — strict serializability for state-transition transactions; commit order respects real-time order. The carve-outs from §1.1 (heartbeats, rate quotas) are NOT part of this requirement.
2. **Non-blocking row locking with skip semantics** — `pick_and_lock_pending` skips locked rows rather than blocking. Vacuously satisfied by single-writer backends (SQLite) since there is nothing to skip; backends supporting concurrent writers must implement true skip-locking (Postgres `FOR UPDATE SKIP LOCKED`, FoundationDB conflict-range retry, etc.).
3. **Indexed range scans with predicates** — efficient filtering by `(namespace, task_type, status, priority)`.
4. **Atomic conditional insert** — idempotency-key insertion is "insert if not exists, else return existing" within a transaction.
5. **Subscribe-pending ordering invariant** — any transaction committing a row matching the subscription predicate strictly after `subscribe_pending` returns MUST cause at least one `WakeSignal` to be observable on the returned stream. Backends that cannot deliver true ordering (e.g. poll-only backends) satisfy this via short polling intervals plus the 10s belt-and-suspenders fallback in the CP.
6. **Bounded-cost dedup expiration** — `delete_expired_dedup` must run in time bounded by `n` (the batch size), not by the steady-state size of `idempotency_keys`. Backends with mutable per-row delete cost (e.g. Postgres without partitioning) MUST implement time-range partitioning of the dedup table so cleanup is `DROP PARTITION`, not row-by-row `DELETE`. See §8.3 Postgres notes.

Backends conforming to all six pass a shared conformance test suite (`taskq-storage-conformance` crate). Adding a backend = trait implementation + passing conformance.

### 8.3 v1 backends

- **Postgres** — `SERIALIZABLE` mode required. Reference implementation. Shipped migrations include:
  - `idempotency_keys` range-partitioned by `expires_at` (daily granularity); cleanup is `DROP PARTITION`, not row-by-row delete. **Dedup TTL hard-capped at 90 days** to bound active partitions per backend at ~90 — partition pruning planning time degrades superlinearly past a few hundred partitions.
  - `worker_heartbeats` as a small dedicated table updated at READ COMMITTED. **BRIN index on `worker_heartbeats(last_heartbeat_at)`** — cheap to maintain on append-mostly time-correlated data, sufficient for Reaper B's "scan everything older than T" pattern, and does not defeat HOT updates the way a B-tree on the same column would.
  - Composite indexes on `tasks(namespace, status, priority DESC, submitted_at ASC)` for the dispatch query.
  - **v1 single-replica heartbeat ceiling: ~10K heartbeats/sec** on a properly-tuned Postgres. Above that, the WAL volume from `worker_heartbeats` UPDATE traffic and the BRIN-summary maintenance start dominating. Operators expecting >10K workers per replica should plan for multi-replica (v2) or hash-partition `worker_heartbeats` by `worker_id` (deferred backend optimization).
  - **Migrations table:** v1 ships an initial migration creating `tasks`, `task_runtime`, `worker_heartbeats`, `task_results`, `idempotency_keys` (partitioned), `namespace_quota`, `error_class_registry`, `audit_log`, `task_type_retry_config`, `taskq_meta`. Operators upgrading from any pre-v1 internal deployment must run `taskq-cp migrate` before starting v1.
- **SQLite** — **single-process, embedded / dev-only.** Multi-process or multi-replica CP deployments on SQLite are NOT supported: SQLite locks at database granularity, not row, and concurrent writers from multiple CP processes block (or return `SQLITE_BUSY`). SQLite is shipped to validate the trait shape against a second backend before v1 freezes and to enable embedded testing — not as a production option.

Deferred to v2: CockroachDB, FoundationDB, stonedb. Disqualified: Redis without Lua, plain key-value stores.

### 8.4 Pending-notification mechanism

The `subscribe_pending` method is implemented natively per backend, satisfying the §8.2 ordering invariant:

- Postgres: `LISTEN taskq_pending_<namespace>` + 500ms fallback poll. NOTIFY-on-commit semantics give the ordering guarantee for crashes-during-commit; the fallback poll covers connection drops.
- SQLite: 500ms poll (single-process, single-writer makes this sufficient).
- FoundationDB: native watch on a per-namespace counter incremented on commit.
- (others): poll-driven; the polling interval governs worst-case wakeup latency.

The CP layer doesn't know which is in use. A 10s belt-and-suspenders unconditional query in every waiter catches missed notifications under any backend, including the case where `subscribe_pending` itself errors.

Reference: `problems/07`, `problems/12`.

---

## 9. Configuration

### 9.1 NamespaceQuota

Single config object (§4) covering capacity, rate, per-task ceilings, worker behaviour bounds, cardinality budget, and observability. Inheritance: per-namespace row overrides specific fields; unset fields inherit from `system_default`. `system_default` is itself live-updatable.

**Capacity-quota reads are transactional, no cache.** Capacity limits (`max_pending`, `max_inflight`, `max_workers`, `max_waiters_per_replica`) are read inline inside the same SERIALIZABLE transaction that consults enforcement state — one cheap PK row lookup (~50-200 µs) per admit/acquire, dwarfed by the transaction's own cost (1-10 ms). The earlier round-2 design proposed a `version`-fenced cache to save the row read; it has been removed because v1's admit target (~1K admits/sec/replica) makes the savings meaningless and the contention on the version row itself becomes a second-order problem at higher rates. Revisit at v1.1 if benchmarks show >2K admits/sec sustained per replica with row-read overhead dominating.

**Rate-quota reads stay cached** (§1.1 carve-out). `max_submit_rpm`, `max_dispatch_rpm`, `max_replay_per_second` propagate to replicas within the cache TTL (default 5s, configurable). Aggregate global rate may briefly exceed the configured rate after a downward write; admission self-corrects within the TTL window. Cache TTL is jittered ±10% to prevent synchronized expiry across replicas.

**Validation at `SetNamespaceQuota`** rejects nonsensical configs at admin-API time:
- Registry-cardinality caps (`max_error_classes`, `max_task_types`) — write that would exceed is rejected
- Per-task ceilings — `max_idempotency_ttl_seconds ≤ 90 days` (partition-count ceiling — see §8.3)
- Heartbeat invariant — `lazy_extension_threshold_seconds ≥ 2 × min_heartbeat_interval_seconds` (prevents lazy-extension cadence trap — see §6.3)
- Strategy parameters — within their type-specific bounds (CoDel target, MaxPending limit, etc.)

Lowering below current usage: not rejected; new submits/acquisitions fail until usage drops naturally (or operator runs `PurgeTasks`).

Reference: `problems/10`.

### 9.2 Retry config: three layers

Resolved top-down at retry-scheduling time:

1. **Per-task** — fields on the task at submit (most specific, optional).
2. **Per-task-type** — registered config per `(namespace, task_type)` (optional middle layer).
3. **Per-namespace defaults** — always present.

```rust
struct RetryConfig {
  initial_ms: u64,        // default 1000
  max_ms: u64,            // default 300_000 (5 min)
  coefficient: f32,       // default 2.0
  max_retries: u32,       // bounded by namespace.max_retries_ceiling
}
```

Reference: `problems/08`.

### 9.3 Strategies

Compile-time linked into the CP binary (build-time choice of which to include). At namespace creation/config time, the operator selects which Admitter and Dispatcher to use; choice is locked at CP startup. Strategy *parameters* live-update via `SetNamespaceQuota`: capacity-quota parameters take effect on the next admit/acquire transaction (read inline, no cache); rate-quota parameters propagate within the rate-cache TTL (default 5s). Strategy *choice* requires a rolling restart.

Strategy mismatch (operator selects a strategy not present in the binary) refuses to serve that namespace at startup, logs clearly, continues serving others.

Reference: `problems/05`, `problems/09`.

### 9.4 Error class registry

Per-namespace registered set of allowed `error_class` strings. Set via admin API. `ReportFailure` rejects unknown classes. Append-only — classes can be marked deprecated but never removed (matching FlatBuffers schema rules).

Rust worker SDK exposes the registered set as a typed enum at SDK initialization time, giving compile-time safety on the worker side. Other-language clients do runtime validation.

Reference: `problems/08`.

---

## 10. Backpressure & error model

### 10.1 Rejection structure

`RESOURCE_EXHAUSTED`-style rejections carry:

```
SubmitTaskError {
  reason: enum {
    NAMESPACE_QUOTA_EXCEEDED,
    MAX_PENDING_EXCEEDED,
    LATENCY_TARGET_EXCEEDED,
    SYSTEM_OVERLOAD,
    NAMESPACE_DISABLED,
    UNKNOWN_TASK_TYPE,
    IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD,
    INVALID_PAYLOAD,
    REPLICA_WAITER_LIMIT_EXCEEDED,    // per-replica waiter cap; v1 has no namespace-wide cap (deferred to v2)
  }
  retry_after: i64                  // Unix epoch milliseconds (UTC)
  retryable: bool                   // derived from reason
  pending_count: u64
  pending_limit: u64
  hint: string
}
```

`retry_after` is a **timestamp, not a delay** — robust to network latency. Floor, not contract: server promises retry before T is futile, does not promise success at T. Caller's clock must be NTP-synced; pathological skew causes harmless over- or under-waiting.

`retryable` is derived from `reason`; SDK MUST respect `false` and surface to user code without retry.

### 10.2 Permanent-rejection actionable info

When `retryable=false`, the rejection includes enough info for the caller to fix without retry:

- `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD` — existing `task_id` + payload hash for comparison
- `UNKNOWN_TASK_TYPE` — the task type the caller submitted
- `NAMESPACE_DISABLED` — disable reason from admin config

### 10.3 Submit acceptance is not a dispatch promise

A task admitted at submit may wait indefinitely if no worker registers for its type or workers starve. The protocol contract is "we accepted your task into the queue." Dispatch latency is a separate observable. Calling code that depends on quick dispatch must observe task status, not just submit acceptance.

### 10.4 SDK retry behaviour

Official Rust SDKs handle `RESOURCE_EXHAUSTED` automatically:

- `retryable=true`: sleep until `retry_after` with ±20% jitter, retry up to budget (default 5)
- `retryable=false`: surface to user code immediately

Configurable retry budget so callers can disable internal retries if their layer above already retries.

Reference: `problems/06`.

---

## 11. Observability

### 11.1 Framework

OpenTelemetry, end-to-end. `opentelemetry`, `tracing`, `tracing-opentelemetry` Rust crates. Exporters use OTel's existing extension model — operators wanting custom backends write OTel-compatible exporters and compile them in.

Built-in exporters in v1: OTLP (gRPC, push), Prometheus (pull, `/metrics` endpoint), stdout (debug).

### 11.2 Trace propagation

W3C Trace Context on the wire. Context bytes (`traceparent` / `tracestate`) are persisted on the task row at submit; returned to the worker on `AcquireTask`. Workers MAY emit spans linking to the parent context across hours and retries; whether the parent span itself is queryable depends on the OTel pipeline (see §3.4 and §1.1 carve-outs). Retries are sibling spans under one parent task span. Reaper / cancellation / admin actions emit child spans.

Sampling decision in the `traceparent` flags is persisted with the task — workers respect it without re-deciding. Per-namespace sampling override lives in `NamespaceQuota.trace_sampling_ratio`.

### 11.3 Standard metric set and cardinality budget

All metrics prefixed `taskq_`. Standard labels: `namespace`, `task_type`, `error_class`, `terminal_state`, `reason`, `kind`, `op`. **`task_id` never appears as a metric label** — traces only.

**Histograms drop `task_type` from labels.** `taskq_dispatch_latency_seconds`, `taskq_long_poll_wait_seconds`, `taskq_submit_payload_bytes`, `taskq_storage_transaction_seconds` are labeled by `namespace` only (plus op/kind for the storage metric). Each histogram bucket is a separate series; `task_type` would multiply by 20× per histogram, dominating total cardinality. Counters retain `task_type` since their per-label cost is constant. Operators who need per-task-type latency breakdowns query traces instead.

**Cardinality is bounded at the source.** Each namespace has caps:
- `max_error_classes` (default 64) — `SetNamespaceConfig` rejects writes that would exceed
- `max_task_types` (default 32) — same enforcement
- These bounds × ~100 namespaces × per-metric structure give the worst-case series count for the cluster.

Worked example with bounds: `100 namespaces × 32 task_types × 64 error_classes` for `taskq_error_class_total` = ~200K series cluster-wide for the highest-cardinality counter. Acceptable for Prometheus. Histograms (no `task_type`): `100 namespaces × ~10 buckets × ~5 histogram metrics` ≈ 5K series — trivial.

**Cardinality review checklist (not a build-time lint in v1).** Metric additions require a review checklist item: identify all labels, multiply by per-namespace caps, confirm the worst-case series count fits the operator's budget. Build-time enforcement (a static lint walking metric registration sites) is appealing but has structural blind spots — dynamic labels in custom strategies, histogram-bucket multipliers, non-Rust SDKs — that make a partial implementation worse than no implementation. The lint is moved to §14 open questions; the *caps themselves* (`max_error_classes`, `max_task_types`) are the load-bearing fix and they remain in v1.

Submit, dispatch, lifecycle, lease, quota, workers/connections, internal health metrics — full list in `problems/11`. New retry-observability metrics:

```
taskq_storage_retry_attempts{op}                        histogram   (40001 retries per logical op)
taskq_sdk_retry_total{namespace, reason}                counter     (SDK-side retries against rejection reasons)
```

Both surface compounding retry budgets that would otherwise hide tail latency under contention.

### 11.4 Audit log

Admin RPCs write to `audit_log` inside the same SERIALIZABLE transaction as the action. Append-only, never overwritten. `request_summary` is structured JSON with documented schema per RPC. `request_hash` (sha256) lets external systems verify request bodies without storing full payloads.

Retention configurable per-namespace via `NamespaceQuota.audit_log_retention_days` (default 90). Periodic pruning, rate-limited.

### 11.5 Health endpoints

- `/healthz` — process liveness
- `/readyz` — storage reachable + schema version match + strategies loaded
- `/metrics` — Prometheus exposition (when Prometheus exporter enabled)

Reference: `problems/11`.

---

## 12. Versioning

### 12.1 SemVer

`taskq.v1` is the first major. MINOR adds backwards-compatible features; MAJOR breaks compat. **No public stability commitment** — major bumps happen at our discretion.

### 12.2 FlatBuffers schema discipline

Append-only field additions; never reorder; never change types. Field removal: mark deprecated, leave the field allocated. Enforced by a CI script that diffs `.fbs` files; the diff tool lives in `flatbuffers-rs`.

### 12.3 Wire-version handshake

First RPC carries client version; server returns `supported_client_range`. Major-version mismatch rejected with `INCOMPATIBLE_VERSION`. Within compatible majors, both sides ignore unknown fields.

### 12.4 Cross-language scope

Rust only. We ship Rust caller and worker SDKs. Other-language clients consuming the wire schema directly self-manage version skew; we don't release them.

### 12.5 Storage migrations

Each backend ships ordered migration files in the CP repo. `taskq_meta` table holds `(schema_version, format_version)`. CP startup checks compatibility; refuses to start with mismatch. Production migrations run via `taskq-cp migrate` subcommand explicitly (no auto-migrate). Development can use `--auto-migrate` flag.

### 12.6 Stored-data versioning

Tasks include `format_version`; CP reads any supported format, writes only current. Namespace config and error_class registry have their own format versions, upgraded in-place at next write.

### 12.7 Rolling upgrades

- Within a minor: required to work; rolling restart is normal upgrade path
- Across minor (same major): required; new optional fields default to old behaviour
- Across major: not supported in-place; drain-and-restart cluster

### 12.8 Two-phase rollouts for behavioural changes

New behaviour gated on per-namespace feature flag, default off. Operators turn on per-namespace; flag becomes default-on next minor; mandatory in major after that. Three-minor-cycle deprecation path minimum.

### 12.9 Deprecation policy

Fields/methods deprecated for ≥2 minor cycles before removal in major. Deprecation surfaces as `taskq_deprecated_field_used_total{field}` counter so operators can identify lingering usage.

Reference: `problems/09`.

---

## 13. v1 scope

### Ships in v1

- Postgres + SQLite storage backends (SQLite scoped to single-process embedded/dev only — see §8.3)
- Three Admitter strategies (`Always`, `MaxPending`, `CoDel`) and three Dispatcher strategies (`PriorityFifo`, `AgePromoted`, `RandomNamespace`), all stateless
- Rust caller SDK and Rust worker SDK
- OpenTelemetry-native observability (metrics + traces + audit log)
- Single-replica deployment (multi-replica capable; not the default — see §1)
- W3C Trace Context bytes persisted per task; parent-span retention via OTel pipeline
- Required idempotency keys with 24h-after-termination default TTL
- Server-side retry math with full jitter and three-layer config
- All admin RPCs (`SetNamespaceQuota`, `SetNamespaceConfig`, `PurgeTasks`, `ReplayDeadLetters`, `CancelTask`, `EnableNamespace`, `DisableNamespace`, `GetStats`, `ListWorkers`)
- FlatBuffers schema diff tool (in `flatbuffers-rs`, coordinated dependency)
- `taskq-cp migrate` CLI for explicit migrations
- **Heartbeats carved out of SERIALIZABLE** to a dedicated `worker_heartbeats` table (READ COMMITTED, UPSERT), with lazy lease extension on `task_runtime` driven by `last_extended_at` semantics and validated `ε ≥ 2 × min_heartbeat_interval` invariant — §1.1, §6.3, §9.1
- **`worker_heartbeats.declared_dead_at` tombstone** — Reaper B stamps it on workers it reclaims; heartbeat path filters `IS NULL`, preventing transparent resurrection — §4, §6.3, §6.6
- **BRIN index on `worker_heartbeats(last_heartbeat_at)`** — keeps Reaper B fast without defeating HOT updates; documented v1 ceiling ~10K heartbeats/sec single-replica — §8.3
- **`idempotency_keys` range-partitioned by `expires_at`** (Postgres) with hard 90-day TTL ceiling so cleanup is `DROP PARTITION` and partition count stays bounded — §8.3
- **Capacity-quota reads inline inside the SERIALIZABLE transaction** (no version-fenced cache); rate quotas remain cached as eventually consistent (§1.1 carve-out); v1 admit target 1K/sec/replica — §9.1
- **Per-replica waiter cap only** — namespace-wide cap deferred to v2 alongside multi-replica deployment defaults — §6.2, §10.1
- **Cardinality budget enforcement**: `max_error_classes` (default 64) and `max_task_types` (default 32) per namespace; `task_type` dropped from histogram labels; cardinality review checklist for new metrics — §11.3
- **Transparent `40001` retry** in `CompleteTask` / `ReportFailure` so workers reliably observe `LEASE_EXPIRED` rather than opaque transient errors — §6.4, §6.6
- **Retry-observability metrics**: `taskq_storage_retry_attempts` histogram + `taskq_sdk_retry_total` counter surface compounding retry budgets — §11.3
- **Subscribe-pending ordering invariant** as a 5th conformance requirement — §8.2
- **Bounded-cost dedup expiration** as a 6th conformance requirement — §8.2
- **Cache singleflight + jittered TTL** for the rate-quota cache, preventing stampedes — §9.1
- **`cancel_internal` shared helper** between `CancelTask` and `PurgeTasks` ensures the cancel-then-delete-dedup pair is implemented once — §6.7
- **`CancelTask` ↔ `SubmitTask` race ordering** documented (later commit wins under SSI) — §6.7

### Deferred to v2+

- Stateful dispatcher strategies (`WeightedFair`, `TokenBucket`, `RoundRobin`)
- Automatic poison-pill detection beyond `max_retries`
- Circuit-breaking on task types
- Per-task-type capacity quotas
- Hierarchical namespaces (org → team → service)
- Cross-region replication (Spanner, CockroachDB backends)
- Multi-task pull (batched `AcquireTask`)
- Real-time event streaming (`SubscribeTaskEvents`)
- Web UI
- Other-language SDKs
- Billing-grade metering
- Storage partitioning by namespace
- Quota change audit log richer than the generic admin audit log

---

## 14. Open questions still on the table

A handful of decisions are explicitly deferred to implementation time:

- **Storage backend instrumentation contract** (`problems/11` Q1) — backend-specific metrics under a backend namespace, plus CP-level wrapper metrics. Confirm shape during impl.
- **Default belt-and-suspenders cadence per backend** (`problems/07`) — 10s for Postgres notification-driven backends, 500ms for poll-driven backends. Backend reports preferred default.
- **Schema-diff tool ownership timeline** (`problems/09`) — coordinated dependency on `flatbuffers-rs` adding the diff tool. If late, ship without CI enforcement and rely on review discipline.
- **`max_payload_bytes` enforcement at transport vs application layer** (`problems/10`) — gRPC-level cap vs. namespace-specific override. Decide during impl.
- **Audit log truncation policy for huge `request_summary`** (`problems/11`) — truncate at 4 KB with `truncated: true` flag. Confirm during impl.
- **Reaper backoff parameters for `40001` retries** — default 3 attempts with 5/25/125ms jitter. Confirm during benchmarks.
- **Build-time CI cardinality lint** (downgraded from v1 commitment per round-2 verdict) — appealing but has structural blind spots (dynamic labels, histogram buckets, non-Rust SDKs). Revisit if/when a more complete mechanism is feasible. v1 ships with the review-checklist substitute and the `max_error_classes` / `max_task_types` caps.
- **Version-fenced quota cache** (deferred from v1 per round-2 verdict) — revisit at v1.1 if benchmarks show >2K admits/sec sustained per replica with capacity-quota row-read overhead dominating. Until then capacity quotas read inline transactionally.
- **`worker_heartbeats` hash-partitioning** — beyond v1's documented ~10K heartbeats/sec single-replica ceiling, hash-partitioning by `worker_id` is the next-step backend optimization. Defer until multi-replica becomes the deployment default.
- **Namespace-wide waiter cap** (`max_waiters_per_namespace`) — deferred to v2 alongside multi-replica deployment defaults. Single-replica v1 has no operational gap that this would close.

Each is small enough that locking them in now would constrain implementation; revisit as concrete decisions land.

**Resolved during the post-debate revision (no longer open):** heartbeat write pressure (split table + lazy extension, §6.3); idempotency table growth (range partitioning + 90-day TTL ceiling, §8.3); cardinality blowup (per-namespace caps + review checklist, §11.3); quota cache vs external consistency (transactional inline reads, §9.1); SQLite scoping (embedded-only, §8.3); `40001` vs `LEASE_EXPIRED` (transparent retry, §6.4); ε vs heartbeat cadence (`last_extended_at` semantics + `ε ≥ 2 × min_heartbeat_interval` validation, §6.3, §9.1); Reaper B → heartbeat resurrection (`declared_dead_at` tombstone, §6.3, §6.6); `worker_heartbeats` indexing (BRIN, §8.3); `CancelTask` / `PurgeTasks` shared helper (§6.7).

---

## Cross-reference index

| Topic | Primary problem doc(s) |
|---|---|
| Storage abstraction & conformance | `problems/12` |
| Lease lifecycle & reclaim | `problems/01` |
| Crash consistency & external consistency | `problems/02` |
| Idempotency & terminal-state taxonomy | `problems/03` |
| Strategies (Admitter, Dispatcher) | `problems/05` |
| Backpressure rejection contract | `problems/06` |
| Long-poll & pending notifications | `problems/07` |
| Retry math & DLQ | `problems/08` |
| Versioning & migrations | `problems/09` |
| Quotas & noisy-neighbor controls | `problems/10` |
| Observability (metrics, traces, audit) | `problems/11` |
