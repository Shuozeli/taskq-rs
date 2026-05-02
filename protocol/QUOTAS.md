<!-- agent-updated: 2026-05-02T21:35:28Z -->

# Namespace Quotas Reference

Comprehensive reference for every field on `NamespaceQuota`: validation
rules at `SetNamespaceQuota`, default values from `system_default`, the
rejection reasons that fire when a quota is exceeded, and operator
guidance for tuning. Companion to [`design.md`](../design.md) §4 / §9.1
and to [`problems/10-noisy-neighbor.md`](../problems/10-noisy-neighbor.md).

The struct shape lives in
[`taskq-storage/src/types.rs::NamespaceQuotaUpsert`](../taskq-storage/src/types.rs).
The validation logic lives in
[`taskq-cp/src/handlers/task_admin.rs::validate_quota`](../taskq-cp/src/handlers/task_admin.rs).
Wire-level rejection reasons live on
[`SubmitTaskError`](../design.md#101-rejection-structure) per
[`design.md`](../design.md) §10.1.

---

## 1. Quick reference

| Field | Unit | Default (`system_default`) | Validation at `SetNamespaceQuota` | Rejection on excess |
|---|---|---|---|---|
| `max_pending` | tasks | unlimited | `>= 1` (Some) | `MAX_PENDING_EXCEEDED` |
| `max_inflight` | tasks | unlimited | `>= 1` (Some) | `NAMESPACE_QUOTA_EXCEEDED` |
| `max_workers` | workers | unlimited | `>= 1` (Some) | `NAMESPACE_QUOTA_EXCEEDED` |
| `max_waiters_per_replica` | waiters | 5000 (CP-wide default) | `>= 1` (Some) | `REPLICA_WAITER_LIMIT_EXCEEDED` |
| `max_submit_rpm` | requests/min | unlimited | `rate >= 1` if Some | `NAMESPACE_QUOTA_EXCEEDED` |
| `max_dispatch_rpm` | requests/min | unlimited | `rate >= 1` if Some | `NAMESPACE_QUOTA_EXCEEDED` |
| `max_replay_per_second` | requests/sec | 100 | `>= 1` (Some) | `NAMESPACE_QUOTA_EXCEEDED` |
| `max_retries_ceiling` | attempts | 10 | (free) | reject at submit if `task.max_retries > ceiling` |
| `max_idempotency_ttl_seconds` | seconds | 86 400 (24 h) | `<= 90 days` (hard) | reject at submit |
| `max_payload_bytes` | bytes | 10 485 760 (10 MiB) | (free) | `INVALID_PAYLOAD` at submit |
| `max_details_bytes` | bytes | 65 536 (64 KiB) | (free) | reject at `ReportFailure` |
| `min_heartbeat_interval_seconds` | seconds | 5 | `>= 1` | reject at `Heartbeat`/`Register` if requested cadence is shorter |
| `lazy_extension_threshold_seconds` (ε) | seconds | ~25% of lease (e.g. 8 for 30 s lease) | `>= 2 * min_heartbeat_interval_seconds` | n/a (internal) |
| `max_error_classes` | classes | 64 | (free) | `SetNamespaceConfig` rejects new class additions past cap |
| `max_task_types` | task types | 32 | (free) | `SetNamespaceConfig` rejects new task-type additions past cap |
| `trace_sampling_ratio` | f32 in `[0, 1]` | 0.01 (1%) | `[0.0, 1.0]` | n/a (informational) |
| `log_level_override` | enum or `None` | `None` | enum-valid | n/a |
| `audit_log_retention_days` | days | 90 | `>= 1` | n/a (pruner-only) |
| `metrics_export_enabled` | bool | `true` | (free) | n/a |

Notes:

- `max_waiters_per_namespace` is **deferred to v2** alongside multi-replica
  default deployment. v1 single-replica has no operational gap that the
  namespace-wide cap would close. See
  [`problems/10`](../problems/10-noisy-neighbor.md) round-2 refinement.
- "Unlimited" means `Option<...> = None` — until system-level resource
  exhaustion. Operators should set explicit limits in production.
- Validation at `SetNamespaceQuota` rejects nonsensical configs (rate of
  zero with non-zero burst, etc.) per [`design.md`](../design.md) §9.1.

---

## 2. Capacity quotas

Capacity quotas are read **inline inside the SERIALIZABLE transaction** of
the operation they gate ([`design.md`](../design.md) §1.1, §9.1). One
cheap `~50-200 µs` PK lookup per admit/acquire, dwarfed by the
transaction's own cost. No cache; no eventual-consistency window.

### 2.1 `max_pending: Option<u64>`

Maximum number of tasks in `PENDING` state for the namespace.

- **Default:** unlimited (None).
- **Enforced at:** `SubmitTask` (Admitter step).
- **Rejection:** `MAX_PENDING_EXCEEDED` per
  [`design.md`](../design.md) §10.1 (also surfaces as
  `NAMESPACE_QUOTA_EXCEEDED` when the storage backend's quota check
  trips). The rejection carries `pending_count` and `pending_limit` so
  the SDK can render an actionable error.
- **Bump it when:** legitimate traffic spikes are being rejected and
  storage / dispatch capacity has headroom.
- **Lower it when:** a noisy neighbor has been exhausting throughput at
  the expense of other namespaces, or storage row-count is approaching
  partition-pruning ceilings (see
  [`design.md`](../design.md) §8.3).

### 2.2 `max_inflight: Option<u64>`

Maximum number of tasks in `DISPATCHED` (with active lease) for the
namespace.

- **Default:** unlimited (None).
- **Enforced at:** `AcquireTask` (Dispatcher step).
- **Rejection:** `NAMESPACE_QUOTA_EXCEEDED`. The acquiring worker sees
  no task on its long-poll and either re-polls or blocks until inflight
  drops.
- **Bump it when:** workers are starved at the dispatch step and the
  pending depth is non-zero.
- **Lower it when:** downstream services are being overloaded by parallel
  task execution.

### 2.3 `max_workers: Option<u32>`

Maximum number of registered workers for the namespace, regardless of
whether they're actively long-polling
([`problems/10`](../problems/10-noisy-neighbor.md) §worker-count
semantics).

- **Default:** unlimited (None).
- **Enforced at:** `Register`.
- **Rejection:** `NAMESPACE_QUOTA_EXCEEDED` returned to the worker SDK on
  `Register`. The SDK surfaces this as a fatal init error — the operator
  must either bump the cap or shrink the worker fleet.
- **Bump it when:** scaling out worker fleet for legitimate throughput
  growth.
- **Lower it when:** a tenant is holding registration slots that other
  namespaces' workers need (each registered worker consumes a row in
  `worker_heartbeats` and contributes to the ~10K heartbeats/sec
  single-replica ceiling per [`design.md`](../design.md) §8.3).

### 2.4 `max_waiters_per_replica: Option<u32>`

Maximum number of long-poll waiters parked on a single CP replica.
Counted independently of `max_workers` — a worker can be registered
without holding a waiter (between polls).

- **Default:** the CP-wide `waiter_limit_per_replica` (default 5000) from
  [`taskq-cp/src/config.rs`](../taskq-cp/src/config.rs); per-namespace
  override via this quota.
- **Enforced at:** `AcquireTask` waiter registration step.
- **Rejection:** `REPLICA_WAITER_LIMIT_EXCEEDED` per
  [`design.md`](../design.md) §10.1. The SDK retries with backoff per
  the standard `RESOURCE_EXHAUSTED` flow.
- **Bump it when:** the namespace has many idle workers that should be
  parked, not actively re-polling.
- **Lower it when:** one tenant is starving others of waiter capacity on
  the replica.

---

## 3. Rate quotas

Rate quotas are **eventually consistent within the rate-cache TTL**
(default 5s, configurable;
[`design.md`](../design.md) §1.1). Aggregate rate may briefly exceed the
configured limit after a downward `SetNamespaceQuota` write; admission
self-corrects within the TTL window. Cache TTL is jittered ±10% to
prevent synchronized expiry across replicas
([`design.md`](../design.md) §9.1).

### 3.1 `max_submit_rpm: Option<RateLimit>` / `max_dispatch_rpm: Option<RateLimit>`

Token-bucket rate limit on `SubmitTask` / `AcquireTask` RPCs per minute.
`RateLimit { rate_per_minute, burst }`.

- **Default:** unlimited (None).
- **Enforced at:** `SubmitTask` / `AcquireTask` Admitter / Dispatcher
  step.
- **Rejection:** `NAMESPACE_QUOTA_EXCEEDED` with `retry_after` set to
  the next refill time per [`design.md`](../design.md) §10.1.
- **Bump it when:** legitimate burst patterns are being throttled (set
  `burst` higher than `rate_per_minute / 60` to absorb spikes).
- **Lower it when:** a tenant's submit rate is dominating storage write
  capacity at the expense of other namespaces.
- **Validation:** `SetNamespaceQuota` rejects `RateLimit { rate_per_minute: 0, burst > 0 }`
  (nonsensical — use `None` for "unlimited" or `Some(RateLimit { rate: N, burst: N })`
  for an actual cap).

### 3.2 `max_replay_per_second: Option<u32>`

Token-bucket rate limit on `ReplayDeadLetters` admin RPC per second.

- **Default:** 100 per [`design.md`](../design.md) §6.7.
- **Enforced at:** `ReplayDeadLetters`.
- **Rejection:** `NAMESPACE_QUOTA_EXCEEDED`.
- **Bump it when:** draining a large DLQ after an upstream fix.
- **Lower it when:** an operator's replay storm is interfering with
  steady-state submit traffic.

---

## 4. Per-task ceilings

These are validated once at `SetNamespaceQuota` (where applicable) and
again per-request at the relevant RPC.

### 4.1 `max_retries_ceiling: u32`

Hard ceiling on the per-task `max_retries` value
([`design.md`](../design.md) §9.2).

- **Default:** 10 (operationally; not enforced by `validate_quota`).
- **Enforced at:** `SubmitTask` — caller's `max_retries` is clamped to
  this ceiling.
- **Rejection:** silent clamp — the persisted task carries
  `min(caller.max_retries, ceiling)`.
- **Bump it when:** retryable failures legitimately need more attempts
  (e.g., long-tail third-party API outages).
- **Lower it when:** a noisy tenant is generating retry storms that
  crowd out other namespaces' dispatch.

### 4.2 `max_idempotency_ttl_seconds: u64`

Hard ceiling on `idempotency_keys.expires_at - submitted_at`
([`design.md`](../design.md) §9.1).

- **Default:** 86 400 (24 h) per the §5 idempotency holding default.
- **Validation:** **`<= 90 days` hard cap** enforced by `validate_quota`
  at `SetNamespaceQuota`. Postgres pre-creates ~96 days of partitions
  and partition-pruning planning time degrades superlinearly past a few
  hundred — the 90-day ceiling is a backend-imposed structural limit,
  not policy.
- **Enforced at:** `SubmitTask`.
- **Rejection:** request-level error (the caller asked for a TTL the
  namespace doesn't allow).
- **Bump it when:** a tenant legitimately needs longer dedup windows
  (subject to the 90-day backend cap).
- **Lower it when:** the tenant's dedup row count is contributing
  excessively to `idempotency_keys` partition pressure.

### 4.3 `max_payload_bytes: u32`

Maximum size of `SubmitTaskRequest.payload`.

- **Default:** 10 485 760 (10 MiB) per
  [`problems/10`](../problems/10-noisy-neighbor.md).
- **Enforced at:** `SubmitTask` — at gRPC transport layer (HTTP/2
  message size cap) for hard rejection, then again at the application
  layer for namespace-specific override.
  ([`problems/10`](../problems/10-noisy-neighbor.md) Open Q2.)
- **Rejection:** `INVALID_PAYLOAD` per
  [`design.md`](../design.md) §10.1 with `retryable=false`.
- **Bump it when:** a tenant legitimately needs to ship larger payloads
  (storage row size and replication cost scale with this).
- **Lower it when:** a tenant is dominating storage WAL volume with
  oversized payloads.

### 4.4 `max_details_bytes: u32`

Maximum size of `ReportFailureRequest.error_details`.

- **Default:** 65 536 (64 KiB) per
  [`problems/08`](../problems/08-retry-storms.md).
- **Enforced at:** `ReportFailure`.
- **Rejection:** request-level error.
- **Bump it when:** workers need to ship richer diagnostic blobs and
  storage capacity allows.
- **Lower it when:** a tenant's failure reports are bloating
  `task_results` storage.

---

## 5. Lease and heartbeat invariants

### 5.1 `min_heartbeat_interval_seconds: u32`

Minimum heartbeat cadence the namespace allows. Workers requesting a
shorter interval are rejected at `Register`.

- **Default:** 5 seconds per
  [`problems/10`](../problems/10-noisy-neighbor.md) Open Q1.
- **Validation:** `>= 1` (enforced implicitly — zero would defeat the
  invariant in §5.2).
- **Enforced at:** `Heartbeat` (rejected if requested interval is
  shorter; [`design.md`](../design.md) §6.3).
- **Rejection:** request-level error to the worker SDK.
- **Bump it when:** the tenant's heartbeat write rate is contributing
  to the ~10K heartbeats/sec single-replica ceiling
  ([`design.md`](../design.md) §8.3).
- **Lower it when:** the tenant's tasks have very short execution
  windows and lease loss must be detected quickly.

### 5.2 `lazy_extension_threshold_seconds: u32` (ε)

The lazy lease-extension window: a heartbeat fires a SERIALIZABLE
extension iff `(NOW − task_runtime.last_extended_at) ≥ (lease_duration − ε)`.

- **Default:** ~25% of lease duration (e.g., 8s for a 30s lease).
- **Validation:** **`>= 2 * min_heartbeat_interval_seconds`** enforced by
  `validate_quota` at `SetNamespaceQuota`. This guarantees that at least
  two heartbeats per ε-window have a chance to fire extension — covers a
  single dropped heartbeat without losing the lease. See
  [`design.md`](../design.md) §6.3 (ε semantics) and §9.1 (validation).
- **Enforced at:** `Heartbeat` (internal calculation; not user-visible).
- **Rejection:** at `SetNamespaceQuota` with explicit error message:
  `lazy_extension_threshold_seconds (X) must be >= 2 * min_heartbeat_interval_seconds (Y)`.
- **Bump it when:** workers run on flaky networks where multiple
  heartbeats may be dropped before recovery.
- **Lower it when:** lease-expiry detection latency must be smaller —
  but never below the `2 * min_heartbeat_interval_seconds` invariant.

The Rust worker SDK clamps its default heartbeat cadence to `<= ε / 3`
([`design.md`](../design.md) §6.3 SDK heartbeat-cadence guidance);
operators overriding the cadence to a value greater than ε / 3 receive
a startup warning.

---

## 6. Cardinality budget

These caps are the load-bearing defense against metric series blow-up
([`design.md`](../design.md) §11.3,
[`problems/11-observability.md`](../problems/11-observability.md)
post-debate refinement).

### 6.1 `max_error_classes: u32`

Maximum number of registered `error_class` strings per namespace.

- **Default:** 64.
- **Enforced at:** `SetNamespaceConfig` — the admin RPC rejects any add
  that would push the registry past the cap. The check sums existing
  registry rows + the request's additions and compares against
  `max_error_classes`. See
  [`taskq-cp/src/handlers/task_admin.rs`](../taskq-cp/src/handlers/task_admin.rs)
  lines 455-462.
- **Rejection:** explicit error message:
  `max_error_classes (N) would be exceeded by request (M additions)`.
- **Bump it when:** a tenant legitimately needs more error
  classification granularity *and* the operator has confirmed the
  metric backend can absorb the resulting series count.
- **Lower it when:** a tenant has accumulated dead error-class strings
  (deprecate first, then lower).

### 6.2 `max_task_types: u32`

Maximum number of registered `task_type` strings per namespace.

- **Default:** 32.
- **Enforced at:** `SetNamespaceConfig` — same enforcement pattern as
  `max_error_classes`.
- **Rejection:** `max_task_types (N) would be exceeded by request (M additions)`.
- **Bump it when:** a tenant legitimately needs more task-type
  granularity.
- **Lower it when:** the tenant's task-type set has accumulated unused
  variants.

### 6.3 Cardinality math

The caps × namespace count × per-metric label structure determines
worst-case Prometheus series count. Worked example from
[`design.md`](../design.md) §11.3:

```
100 namespaces × 32 task_types × 64 error_classes = 204 800 series
   for the highest-cardinality counter (taskq_error_class_total)
100 namespaces × ~10 buckets × ~5 histogram metrics ≈ 5 000 series
   for histograms (drop task_type from labels)
```

Both fit Prometheus comfortably. Operators raising the per-namespace
caps must scale this projection against their metric budget.

`task_id` **never** appears as a metric label — traces only
([`design.md`](../design.md) §11.3, also enforced by review checklist
since the build-time lint was downgraded to v1.1+).

---

## 7. Observability

Observability config lives on `NamespaceQuota` rather than a separate
row ([`problems/11`](../problems/11-observability.md)). The same 5s
rate-cache TTL applies — observability config updates take effect within
that window.

### 7.1 `trace_sampling_ratio: f32`

Per-namespace OTel trace sampling ratio in `[0.0, 1.0]`.

- **Default:** 0.01 (1%).
- **Enforced at:** persisted on submit; the worker SDK respects the
  decision baked into the task's W3C `traceparent` flags
  ([`design.md`](../design.md) §11.2).
- **Rejection:** validated at `SetNamespaceQuota` (must be in `[0.0, 1.0]`).
- **Bump it when:** debugging a tenant-specific issue (set to 1.0
  temporarily).
- **Lower it when:** trace export volume is dominating the operator's
  OTel pipeline cost.

### 7.2 `log_level_override: Option<LogLevel>`

Per-namespace override of the CP log level. `None` inherits the
process-wide level.

- **Default:** `None`.
- **Enforced at:** every CP log site that scopes by namespace.
- **Bump it when:** debugging a specific tenant without flooding
  cluster-wide logs.
- **Lower it when:** the elevated level is no longer needed.

### 7.3 `audit_log_retention_days: u32`

How long to retain `audit_log` rows for the namespace before the periodic
pruner deletes them.

- **Default:** 90 days.
- **Enforced at:** the audit-log pruner (1h cadence, rate-limited).
- **Rejection:** `>= 1` (zero would delete every row immediately).
- **Bump it when:** compliance demands longer retention.
- **Lower it when:** audit-log table size is approaching storage
  pressure.

### 7.4 `metrics_export_enabled: bool`

Whether the CP emits per-namespace metrics for this namespace at all.

- **Default:** `true`.
- **Enforced at:** every metric emission site that takes a namespace
  label.
- **Bump it when:** turning a previously-silenced namespace back on.
- **Lower it when:** a namespace is so high-cardinality that exporting
  its metrics costs more than the visibility is worth (e.g., a sandbox
  namespace).

---

## 8. `system_default` inheritance

A `system_default` row exists in `namespace_quota` carrying defaults for
every field. Per-namespace rows override specific fields; unset fields
inherit from `system_default`. The default itself is live-updatable via
the same `SetNamespaceQuota` RPC, scoped to namespace `system_default`
([`design.md`](../design.md) §9.1, §6.7).

Operators bootstrapping a fresh deployment should review every
`system_default` value — the shipped numbers above are conservative for
"medium-traffic shared cluster" defaults, not the cheapest-possible or
fastest-possible.

---

## 9. Lowering below current usage

`SetNamespaceQuota` does **not** reject operations that lower a quota
below current usage. The new quota takes effect on the next admission /
acquisition decision; existing over-quota state drains naturally as
tasks complete or workers deregister
([`problems/10`](../problems/10-noisy-neighbor.md)).

Operators who want a hard cap immediately can issue `PurgeTasks` or
deregister workers explicitly. The CP refuses to *modify* quota state
without a corresponding admin RPC — there is no implicit "force cap"
behavior.

---

## 10. References

- [`design.md`](../design.md) §4 (data model), §6.7 (admin RPC),
  §9.1 (config validation), §10.1 (rejection structure), §11.3
  (cardinality budget)
- [`problems/10-noisy-neighbor.md`](../problems/10-noisy-neighbor.md) —
  problem statement, prior-art comparison, debate verdicts
- [`problems/11-observability.md`](../problems/11-observability.md) —
  cardinality budget post-debate refinement
- [`taskq-storage/src/types.rs`](../taskq-storage/src/types.rs) —
  `NamespaceQuota` and `NamespaceQuotaUpsert` struct definitions
- [`taskq-cp/src/handlers/task_admin.rs`](../taskq-cp/src/handlers/task_admin.rs) —
  `validate_quota`, `set_namespace_quota_impl`,
  `set_namespace_config_impl`
- [`EVOLUTION.md`](./EVOLUTION.md) — `error_class_registry` and
  `task_type_registry` evolution rules
- [`OPERATIONS.md`](../OPERATIONS.md) — operator-facing quota tuning
  workflow
