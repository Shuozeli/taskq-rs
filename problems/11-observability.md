# Problem: Observability & trace propagation

## The problem

Observability for a multi-tenant task queue has several layers, each with different shape:

- **Metrics** — quantitative aggregates: throughput, latency, error rates, queue depths.
- **Traces** — distributed traces of individual task lifecycles spanning caller → CP → worker, possibly across hours and across retries.
- **Logs** — structured CP logs for debugging.
- **Audit log** — append-only record of who-did-what-when for admin actions.

Most of the design is mechanical (pick a framework, name metrics consistently). The interesting problems are temporal — caller, CP, and worker are decoupled by hours, but the trace must connect them — and operational — multi-tenant deployments need per-namespace overrides for sampling, log level, and retention without code changes.

## What we've already promised this doc owns

Forward references from earlier problems:

| Problem | Metric / observability obligation |
|---|---|
| #03 | Idempotency-key reuse rate, payload-mismatch rate |
| #06 | Per-namespace rejection rate by reason |
| #07 | Per-namespace waiter counts, wait time, wakeup-storm metrics |
| #08 | `error_class` histogram, per-task-type failure rate, replay rate |
| #09 | `taskq_deprecated_field_used`, schema version |
| #10 | Per-namespace quota usage gauges, rate-limit-hit counters |

## Why hard

- **Long temporal gap.** A task submitted at T0 may be picked up by a worker at T+3h. The worker's trace span needs to connect to the caller's. Trace context must be persisted with the task, not just propagated in flight.
- **Retries multiply traces.** Each retry attempt is a separate execution. Convention matters: are they sibling spans under one parent, or separate traces? Tooling depends on it.
- **Cardinality blowup.** `namespace × task_type × error_class × worker_id` explodes. Naive instrumentation produces unusable metrics.
- **Sampling decisions across boundaries.** Caller decides "sample at 100%." Worker, hours later, must respect that choice — but workers have their own SDK initialization and may have their own sampling defaults.
- **Per-namespace overrides.** Operators want to debug specific tenants without flooding for everyone — log level, sampling rate, retention all need namespace-level overrides.
- **Audit log atomicity.** An admin RPC succeeds, audit log write fails: the record diverges from reality. Audit must be in the same transaction as the action.

## The races and failure modes

**A. Lost trace context on retry.** A retry that doesn't carry the original `traceparent` produces an orphan span. Mitigated by persisting trace context on the task row.

**B. Sampling inconsistency.** Caller decides not to sample; worker SDK independently decides to sample. Result: orphan spans with no parent. Mitigated by SDK respecting the persisted sampling decision from the task.

**C. Cardinality explosion.** A label on `task_id` is catastrophic — Prometheus storage grows unboundedly. Mitigated by code-review discipline; `task_id` only in traces (low volume), never in metrics.

**D. Audit log gap.** Admin RPC succeeds, audit log write fails. Without same-transaction semantics, the log diverges from reality. Mitigated by same SERIALIZABLE transaction.

**E. Pluggable exporter failure.** A custom OTel exporter throws on every export. Worst case: exporter blocks CP threads. Mitigated by buffered/async exporter mode required by contract.

## What prior art does

| | Faktory | Hatchet | Temporal |
|---|---|---|---|
| Metrics framework | Custom + Prometheus exporter | Prometheus | OpenTelemetry (metrics + traces) |
| Trace propagation | None | OTel headers | OTel through workflow + activity boundaries |
| Sampling | N/A | Per-tenant configurable | Per-namespace configurable |
| Audit log | Web UI history | Audit events table | Audit history persisted in cluster |
| Custom exporters | N/A | Direct Prometheus only | OTel native — any compatible exporter |

Temporal is the gold standard. We crib from there.

## Direction

### Framework: OpenTelemetry, end-to-end

Metrics, traces, and structured logs flow through OpenTelemetry. The Rust SDK uses `opentelemetry`, `tracing`, and `tracing-opentelemetry` crates. We don't roll our own.

**Pluggable exporters are OTel-native, not a separate taskq-rs trait.** OTel already supports custom exporters via `SpanExporter` / `MetricExporter` traits. Operators who need a custom backend (e.g., a proprietary metrics system) write an OTel-compatible exporter and compile it into their CP binary — same compile-time-linked / runtime-configured model as #05's strategies, but using OTel's existing extension points instead of inventing our own.

Built-in exporters in v1:
- **OTLP** (gRPC) — push to a collector. Default for production.
- **Prometheus** (pull) — `/metrics` endpoint exposed via OTel's Prometheus exporter. For scrape-based deployments.
- **Stdout** — debug only. JSON to stdout.

Operators select one or more via config at startup.

### Trace propagation

**W3C Trace Context** (`traceparent` and `tracestate`) is the on-the-wire format. Both fields are first-class on every RPC that crosses a tracing boundary:

- `SubmitTaskRequest.traceparent` / `.tracestate` — caller-supplied
- `AcquireTaskResponse.traceparent` / `.tracestate` — CP returns the *task's* trace context for the worker to continue
- `ReportFailureRequest.traceparent` / `CompleteTaskRequest.traceparent` — worker continues the trace

**The task row stores the trace context.** When `SubmitTask` lands, `traceparent` and `tracestate` are persisted on the task. `AcquireTask` returns them. The worker continues from the original submission's trace. This survives retries, reaper reclaim, and CP restart.

**Reaper and cancellation events emit spans** as children of the task's parent span. Lease expiry, automatic reclaim, admin cancellation all become first-class trace events — debuggable in any tracing UI.

### Retry as child spans of the task

Each task has a parent span created at submit time. Each attempt is a child span of that parent. Retries 1..N appear as siblings under one parent span. This makes "show me all attempts for this task" trivial in any tracing UI; alternative conventions (separate traces per attempt) lose the linkage.

### Sampling decision is persisted

The sampling flag in the `traceparent` (the `01` bit at byte offset N) is stored with the task. Workers respect it without re-deciding. The sampling decision propagates from the caller through hours of dormancy and across retries with no risk of inconsistency.

### Per-namespace observability config: folded into `NamespaceQuota`

Per #10, observability config lives in the same `NamespaceQuota` struct rather than a separate row. The struct gains:

```rust
struct NamespaceQuota {
  // ... (existing fields from #10) ...
  
  // Observability
  trace_sampling_ratio:        f32,         // [0.0, 1.0], default 0.01 (1%), configurable
  log_level_override:          Option<LogLevel>,    // None = inherit system default
  audit_log_retention_days:    u32,         // default 90, configurable per namespace
  metrics_export_enabled:      bool,        // default true; allows opt-out for cost
}
```

The 5s cache TTL from #10 applies — observability config updates take effect within that window.

### Standard metric set

All metrics prefixed `taskq_`. Counters use `_total` suffix; histograms use base unit suffix (`_seconds`, `_bytes`). Labels are budgeted; cross-product tracked at code review.

```
# Submit-side
taskq_submit_total{namespace, task_type, outcome}              counter
taskq_submit_payload_bytes{namespace, task_type}               histogram
taskq_idempotency_hit_total{namespace}                         counter
taskq_idempotency_payload_mismatch_total{namespace}            counter
taskq_rejection_total{namespace, reason}                       counter

# Dispatch-side
taskq_dispatch_total{namespace, task_type}                     counter
taskq_dispatch_latency_seconds{namespace, task_type}           histogram   (submit -> first dispatch)
taskq_long_poll_wait_seconds{namespace, task_type}             histogram

# Lifecycle
taskq_complete_total{namespace, task_type, outcome}            counter
taskq_retry_total{namespace, task_type}                        counter
taskq_terminal_total{namespace, task_type, terminal_state}     counter      (state ∈ COMPLETED|FAILED_NONRETRYABLE|FAILED_EXHAUSTED|EXPIRED|CANCELLED)
taskq_error_class_total{namespace, task_type, error_class}     counter

# Lease & reaper
taskq_lease_expired_total{namespace, task_type}                counter
taskq_heartbeat_total{namespace}                               counter

# Quota / capacity
taskq_pending_count{namespace}                                 gauge
taskq_inflight_count{namespace}                                gauge
taskq_quota_usage_ratio{namespace, kind}                       gauge        (current / limit)
taskq_rate_limit_hit_total{namespace, kind}                    counter

# Workers / connections
taskq_workers_registered{namespace}                            gauge
taskq_waiters_active{namespace, replica_id}                    gauge

# Internal health
taskq_storage_transaction_seconds{op}                          histogram
taskq_storage_serialization_conflict_total{op}                 counter
taskq_replay_total{namespace}                                  counter
taskq_deprecated_field_used_total{field}                       counter
taskq_schema_version{component}                                gauge
```

**`task_id` never appears as a metric label.** It belongs in traces only. Code review enforces.

Cardinality budget worked example: `~100 namespaces × ~20 task types × ~10 error classes ≈ 20K series` for the worst metric. Operators sizing capacity should plan for this.

### Structured logs via `tracing`

CP logs flow through the `tracing` crate. Output is JSON for production, human-readable for development. Spans carry the same context as OTel traces. Per-namespace `log_level_override` (in `NamespaceQuota`) lets operators bump a specific tenant to DEBUG without changing CP-wide level.

### Audit log

Admin RPCs (`SetNamespaceQuota`, `SetNamespaceConfig`, `PurgeTasks`, `ReplayDeadLetters`, `EnableNamespace`, `DisableNamespace`, etc.) write to an `audit_log` table inside the same SERIALIZABLE transaction as the action they record. Append-only; never overwritten.

```
audit_log {
  id, timestamp, actor (from auth context), rpc, namespace,
  request_summary (JSONB),    -- structured JSON, schema per rpc
  result, request_hash (sha256 of full request body)
}
```

`request_summary` is structured JSON with a documented schema per RPC type — not free text. Operators can query `audit_log` with normal JSON predicates. `request_hash` lets external systems verify request bodies without storing full payloads.

Retention configurable per namespace via `NamespaceQuota.audit_log_retention_days` (default 90). Older rows pruned by a periodic job; the retention job is rate-limited to avoid storage spikes.

### Health endpoints

- `/healthz` — process liveness. 200 always unless about to exit. Container-orchestrator-friendly.
- `/readyz` — readiness. 200 only if storage is reachable, schema version matches, all configured strategies are loaded. Load-balancer-friendly.
- `/metrics` — Prometheus exposition format if Prometheus exporter is enabled.

### What v1 does NOT include

- **Per-task event log table.** Not every state transition is a separate row. Metrics + traces + audit log + storage WAL cover forensic recovery.
- **Bundled dashboards.** Operators bring their own Grafana / Datadog / etc. We may publish reference dashboards as docs but they're not part of the binary.
- **Real-time event streaming to clients.** No `SubscribeTaskEvents` RPC. Callers poll `GetTaskResult`. Real-time deferred.
- **Runtime cardinality enforcement.** Discipline is at code-review time. Static analysis tooling for label cardinality is a v2 candidate.
- **Cross-CP-replica metric aggregation.** Each replica reports its own; aggregation happens in the metrics backend (Prometheus, OTel collector). The CP doesn't aggregate.

## Open questions

1. **Storage backend instrumentation contract.** Storage backends are pluggable (#12). Should they emit their own OTel metrics with backend-specific names (e.g., `taskq_storage_postgres_pool_size`), or only emit through `taskq_storage_*` wrapper metrics that the CP layer adds? Lean: both — backend emits backend-specific metrics under its own namespace; CP wraps with op-level metrics independent of backend. Confirm with backend implementations later. (User explicitly unsure; revisit during impl.)
2. **Export buffer overflow.** What happens if OTLP export can't keep up? OTel SDK has a buffered exporter with overflow policy (drop / block / log). Lean: drop with a `taskq_export_dropped_total` counter; never block CP path.
3. **Trace context size on the task row.** `traceparent` is 55 bytes; `tracestate` is variable up to ~256 bytes. Adds ~300 bytes per task. Acceptable storage cost.
4. **Namespace as a label vs. a metric prefix.** Some operators prefer `taskq_namespace_${ns}_submit_total` over `taskq_submit_total{namespace=$ns}`. Lean: labels (standard practice); operators who want the prefix form can rewrite at the metrics backend.
5. **Default trace sampling for the system span (CP-internal operations).** Independent of namespace ratio. Lean: 1% sampling for system spans, configurable.
6. **Audit log when `request_summary` would be huge.** E.g., `PurgeTasks` with 100k task IDs. Truncate the summary, keep `request_hash` for verification. Lean: yes, truncate at 4KB with a `truncated: true` flag.

## Forward references

- Problem #10 (noisy neighbor) — observability config is folded into `NamespaceQuota`.
- Problem #12 (storage abstraction) — backend instrumentation contract is an open question.
- All previous problems' forward references to "metrics" are resolved by the standard set above.

## Post-debate refinement

The scale-skeptic agent estimated real-world cardinality at 300K-600K series per CP replica (vs. the 20K worked example), driven by histograms × `task_type` × `error_class` × `namespace`. Resolution in `design.md`:

- **Histograms drop `task_type` from labels.** `taskq_dispatch_latency_seconds`, `taskq_long_poll_wait_seconds`, `taskq_submit_payload_bytes`, `taskq_storage_transaction_seconds` are labeled by `namespace` only (plus op/kind for storage). Histogram bucket count would otherwise multiply by 20× per task_type. Counters retain `task_type` since their per-label cost is constant. Per-task-type latency analysis goes through traces.
- **Cardinality is bounded at the source.** `max_error_classes` (default 64) and `max_task_types` (default 32) added to `NamespaceQuota`. `SetNamespaceConfig` rejects registry writes that would exceed the cap. With these bounds, worst-case cluster-wide series for the highest-cardinality counter is ~200K — within Prometheus comfortable range.
- **CI cardinality lint — downgraded to a review checklist for v1.** Round 1 added a build-time CI lint as a v1 commitment. Round 2 noted three structural blind spots: dynamic labels in custom strategies (lint can't see them), histogram-bucket multipliers (lint must charge per bucket), and other-language SDKs (out-of-scope but emit metrics). Resolution: v1 ships a *cardinality review checklist* — every new metric registration is reviewed against the worst-case cardinality computed from the namespace caps. Build-time enforcement is a v1.1+ candidate, parked in `design.md` §14. The *caps themselves* (`max_error_classes`, `max_task_types`) remain in v1 — they're the load-bearing fix.
- **Cache singleflight + jittered TTL** prevents synchronized cache stampedes on `system_default` and namespace creation (§9.1 in `design.md`). (Round-2 note: this now applies only to the rate-quota cache; capacity-quota reads are inline transactional with no cache.)

## Round-2 refinement

Two changes:

- **CI lint downgraded** (see Direction section above). v1 ships a review checklist; build-time lint deferred to v1.1+.
- **Two new retry-observability metrics.** A 3-attempt CP-side `40001` retry × N replicas × SDK retry budget produces a long tail under contention that's invisible to operators today. Added: `taskq_storage_retry_attempts{op}` (histogram) and `taskq_sdk_retry_total{namespace, reason}` (counter). See `design.md` §11.3.
