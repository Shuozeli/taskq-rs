<!-- agent-updated: 2026-05-02T21:35:28Z -->

# OPERATIONS

Production deployment and operations reference for `taskq-rs`. Distinct from
the operator codelab (`codelabs/03-operating-taskq-rs.md`, planned) — this is
a reference, not a tutorial. Walk-throughs of common workflows
(provisioning a namespace, draining a DLQ, rotating quotas) live in the
codelab; this document covers the standing topology, configuration,
observability surface, and failure-mode guidance an operator needs once
the cluster is running.

---

## 1. Deployment topology

### 1.1 Single-replica vs multi-replica CP

`taskq-rs` v1 ships **single-replica deployments** as the default. The CP
is multi-replica capable (no in-memory correctness state), but multi-replica
is not the default deployment shape — see
[`design.md`](./design.md) §1.

Recommended v1 topology:

```
+------------------+      gRPC       +------------------+
|   Worker fleet   |  ============>  |    taskq-cp      |
|   + caller fleet |                 |   (1 replica)    |
+------------------+                 +--------+---------+
                                              |
                                              |  SERIALIZABLE txns
                                              v
                                      +------------------+
                                      |   Postgres 14+   |
                                      |   (primary +     |
                                      |    streaming     |
                                      |    replica)      |
                                      +------------------+
```

Multi-replica CP is supported but adds operational complexity:
`max_waiters_per_namespace` (the namespace-wide waiter cap) is deferred to
v2 ([`problems/10`](./problems/10-noisy-neighbor.md) round-2), so the
single-replica per-namespace cap is the primary defense against waiter
exhaustion in v1. Operators who absolutely need multi-replica today should
load-balance via Postgres-backed coordination only — no shared in-memory
state across replicas.

### 1.2 Postgres setup

- Postgres 14 or newer (range partitioning + BRIN indexes are core).
- `SERIALIZABLE` isolation level for all state-transition transactions.
  Heartbeats run at `READ COMMITTED` on a separate connection per
  [`design.md`](./design.md) §1.1, §6.3.
- Streaming replica recommended for failover; promote-on-primary-loss is
  the operator's responsibility (taskq-rs does not orchestrate Postgres
  failover).

### 1.3 Reaper cadence config

Two reapers run inside the CP process
([`design.md`](./design.md) §6.6):

- **Reaper A** (per-task timeout): scans `task_runtime` for
  `timeout_at <= NOW()`. Default cadence 1s; configurable via
  `taskq-cp` config (Phase 6+).
- **Reaper B** (dead-worker reclaim): scans
  `worker_heartbeats` for stale heartbeats. Default cadence 5s.

Both use `FOR UPDATE SKIP LOCKED` and tolerate `40001` retries. Above
~1000 tasks reclaimed per second sustained, consider raising
`waiter_limit_per_replica` to absorb the resulting requeue burst, or
deploy a second CP replica.

### 1.4 Metrics collector

The CP exposes `/metrics` on `health_addr` (default `0.0.0.0:9090`) when
`otel_exporter = { kind = "prometheus" }`. A Prometheus scraper, OTel
collector, or both can pull from this endpoint. For OTLP push, configure
`otel_exporter = { kind = "otlp", endpoint = "..." }`.

---

## 2. Postgres tuning

### 2.1 Isolation level

`SERIALIZABLE` is the contract. Do **not** lower it for "performance" —
that breaks the external-consistency guarantee
([`design.md`](./design.md) §1). `READ COMMITTED` is the carve-out for
heartbeats; it lives on a *separate connection* in the CP, not on the
state-transition path.

### 2.2 `SKIP LOCKED` performance

`pick_and_lock_pending` uses `FOR UPDATE SKIP LOCKED` to scan past tasks
held by concurrent acquirers ([`design.md`](./design.md) §8.2 #2).
Performance characteristics:

- Postgres does NOT use the same statistics for `SKIP LOCKED` as for
  unqualified `FOR UPDATE`. Verify the planner picks the composite index
  on `tasks(namespace, status, priority DESC, submitted_at ASC)` —
  `EXPLAIN (ANALYZE, BUFFERS)` should report an `Index Scan`, not a
  `Seq Scan`.
- The dispatch query's selectivity matters: highly-selective predicates
  (single namespace + few task types) hit the index cleanly. Less
  selective queries (no task-type filter, many namespaces) may need
  `random_page_cost` tuned down toward `1.1` on SSD storage.

### 2.3 BRIN index on `worker_heartbeats`

```sql
CREATE INDEX worker_heartbeats_last_heartbeat_at_brin
    ON worker_heartbeats USING BRIN (last_heartbeat_at);
```

BRIN is cheap to maintain on append-mostly time-correlated data and does
not defeat HOT updates the way a B-tree on the same column would
([`design.md`](./design.md) §8.3). Verify summarization with:

```sql
SELECT * FROM brin_summarize_new_values('worker_heartbeats_last_heartbeat_at_brin');
```

The auto-summarization setting (`autosummarize`) should be enabled on the
index. Without it, the index becomes useless for the rolling
"everything older than T" predicate.

### 2.4 Partition maintenance for `idempotency_keys`

`idempotency_keys` is range-partitioned by `expires_at` at daily
granularity ([`design.md`](./design.md) §8.3). The Phase 3 schema ships a
helper:

```sql
SELECT taskq_create_idempotency_partitions_through(NOW() + INTERVAL '90 days');
```

Schedule this nightly (e.g., as a cron job, a Postgres `pg_cron` task, or
a manual `taskq-cp migrate` extension) so future partitions exist before
new submits land. The hard 90-day TTL ceiling on `max_idempotency_ttl_seconds`
([`QUOTAS.md`](./protocol/QUOTAS.md) §4.2) bounds the active partition
count at ~90.

Cleanup is `DROP PARTITION`, not row-by-row `DELETE`. Old partitions
should be dropped via the periodic cleanup runner; a manual override is:

```sql
DROP TABLE idempotency_keys_2025_07_15;  -- example daily partition
```

### 2.5 Autovacuum settings

The hot tables benefit from aggressive autovacuum:

```sql
ALTER TABLE tasks SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);
ALTER TABLE task_runtime SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.01
);
ALTER TABLE worker_heartbeats SET (
    autovacuum_vacuum_scale_factor = 0.10,
    autovacuum_analyze_scale_factor = 0.05
);
```

`task_runtime` churns hardest (every acquire / complete / reaper hit) —
keep autovacuum aggressive to prevent index bloat from defeating the
SKIP LOCKED scan.

### 2.6 Connection pool sizing

The Postgres backend uses `deadpool-postgres`; default `pool_size = 16`
per [`taskq-cp/src/config.rs`](./taskq-cp/src/config.rs). Sizing rule of
thumb:

```
pool_size = max(16, expected_concurrent_handler_threads * 1.5)
```

Heartbeats use a *separate* dedicated connection (READ COMMITTED carve-out;
[`design.md`](./design.md) §6.3) so they do not consume from this pool.
LISTEN demuxing also uses a dedicated connection.

---

## 3. Network bind

Per the global Tailscale convention (rule #8), prefer binding to the
Tailscale IP rather than `0.0.0.0` or `127.0.0.1`. The CP's config
honors `TAILSCALE_IP` env var as the host override
([`taskq-cp/src/config.rs`](./taskq-cp/src/config.rs)):

```bash
export TAILSCALE_IP=$(tailscale ip -4)
taskq-cp serve --config /etc/taskq/cp.toml
```

The port from `bind_addr` in the TOML is preserved; only the host is
substituted. If `TAILSCALE_IP` is unset, the file's `bind_addr` is used
as-is — set to `0.0.0.0:50051` for plain local dev.

`TASKQ_BIND_ADDR` env var takes precedence over `TAILSCALE_IP` and
overrides the entire `host:port` if set.

---

## 4. Observability setup

### 4.1 OTLP exporter

```toml
# /etc/taskq/cp.toml
[otel_exporter]
kind = "otlp"
endpoint = "http://otel-collector.internal:4317"
```

Push cadence is 15s; export timeout 10s
([`taskq-cp/src/observability.rs`](./taskq-cp/src/observability.rs)
`init_otlp`). Both metrics and traces are exported. The OTel SDK's
buffered exporter handles transient collector outages — overflow policy
is "drop" with a counter rather than block CP threads.

### 4.2 Prometheus pull endpoint

```toml
[otel_exporter]
kind = "prometheus"
```

The `/metrics` endpoint is served on `health_addr` (default
`0.0.0.0:9090`). Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: 'taskq-cp'
    scrape_interval: 30s
    static_configs:
      - targets: ['taskq-cp.internal:9090']
```

The standard metric set is documented in
[`design.md`](./design.md) §11.3 and emitted by
[`taskq-cp/src/observability.rs`](./taskq-cp/src/observability.rs)
`MetricsHandle::register`. Reference Grafana dashboard:
[`dashboards/grafana-overview.json`](./dashboards/grafana-overview.json).

### 4.3 Audit log retention pruner

The CP runs a periodic audit-log pruner (1h cadence, rate-limited) that
deletes `audit_log` rows older than the namespace's
`audit_log_retention_days` ([`design.md`](./design.md) §11.4). Default
retention is 90 days per namespace. The pruner emits
`taskq_audit_log_pruned_total` per pass.

Operators who need longer retention bump
`audit_log_retention_days` per namespace; operators who want zero
retention (compliance forbids storing the requests at all) should
disable admin RPC handlers entirely rather than relying on aggressive
pruning.

---

## 5. Backup strategy

### 5.1 Critical state

These tables hold authoritative state and should be backed up:

- `tasks` — task metadata + payloads
- `task_results` — terminal outcomes, replay-eligibility
- `audit_log` — compliance / forensic trail
- `namespace_quota` — every operator's tuning history (live-update
  semantics; no separate audit beyond `audit_log`)
- `error_class_registry` / `task_type_registry` — append-only registry
  state that workers depend on

### 5.2 Daily-droppable

`idempotency_keys` is **safe to drop for backup purposes**. The keys
expire on a 24h-after-termination default TTL
([`design.md`](./design.md) §5); losing them means at-most-once dedup
guarantees lapse for the lost window, but at-least-once delivery is
unaffected. Most operators exclude `idempotency_keys` from backups
entirely.

`worker_heartbeats` is also droppable — heartbeats are best-effort
liveness signals ([`design.md`](./design.md) §1.1), not state. After a
restore the reaper will reclaim leases for any worker whose heartbeat
row is missing on the next reaper pass.

### 5.3 Backup cadence

- WAL streaming to a hot standby for instant failover.
- Logical (`pg_dump`) snapshots nightly for disaster-recovery point-in-time
  recovery; exclude `idempotency_keys` and `worker_heartbeats` to keep
  snapshots small.
- Test restore quarterly. A backup that hasn't been restored is not a
  backup.

---

## 6. Capacity planning

### 6.1 Worker fleet size

Per-namespace `max_workers` caps the registered fleet
([`QUOTAS.md`](./protocol/QUOTAS.md) §2.3). System-wide capacity is
bounded by the v1 single-replica heartbeat ceiling: **~10K heartbeats/sec**
on a properly-tuned Postgres ([`design.md`](./design.md) §8.3).

```
worker_count_ceiling = 10_000 * min_heartbeat_interval_seconds
```

For a 5s `min_heartbeat_interval_seconds`, that's ~50K workers per
single-replica deployment before the WAL volume from `worker_heartbeats`
UPDATE traffic and the BRIN-summary maintenance start dominating. Above
this ceiling, plan for multi-replica (v2) or for the deferred backend
optimization of hash-partitioning `worker_heartbeats` by `worker_id`
([`design.md`](./design.md) §14).

### 6.2 Heartbeat write rate

The lazy-extension semantics ([`design.md`](./design.md) §6.3) decouple
heartbeat traffic (READ COMMITTED upserts on `worker_heartbeats`) from
SERIALIZABLE lease-extension writes (`task_runtime` UPDATEs). The latter
fire only when `(NOW − last_extended_at) ≥ (lease_duration − ε)`, i.e.,
roughly once per lease-window per active worker, not once per heartbeat.

A worker with a 30s lease and 5s heartbeat cadence:

- ~12 heartbeats/min as READ COMMITTED upserts
- ~2 SERIALIZABLE extensions/min on `task_runtime`

Plan capacity against the upsert rate (the dominant term).

### 6.3 Waiter pool size

`waiter_limit_per_replica` (CP-wide default 5000;
[`taskq-cp/src/config.rs`](./taskq-cp/src/config.rs)) bounds the in-memory
waiter map. Per-namespace `max_waiters_per_replica` is a sub-cap. Each
waiter holds a tokio task + a small slice of the per-namespace
notification stream; the steady-state cost is dominated by the
notification-demux thread, not the waiter count itself.

For multi-replica deployments, the namespace-wide waiter cap is **deferred
to v2**. Operators running multi-replica today should rely on the
per-replica cap × replica count as a soft cluster-wide cap.

---

## 7. Health endpoints

The CP exposes three endpoints on `health_addr`
([`design.md`](./design.md) §11.5):

- **`/healthz`** — process liveness. 200 always unless the process is
  about to exit. Container-orchestrator-friendly. Does NOT verify storage
  reachability.
- **`/readyz`** — full readiness. 200 only if storage is reachable, the
  schema version matches the binary's expectation, and all configured
  strategies are loaded. Load-balancer-friendly — a CP that returns
  `/readyz != 200` should be drained from the LB.
- **`/metrics`** — Prometheus exposition format. Only enabled when
  `otel_exporter = { kind = "prometheus" }`.

Recommended Kubernetes probes:

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 9090
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /readyz
    port: 9090
  periodSeconds: 5
  failureThreshold: 3
```

---

## 8. Graceful shutdown

The CP handles `SIGTERM` for graceful drain
([`design.md`](./design.md) §6.3 worker drain control signals). On
SIGTERM:

1. `/readyz` starts returning non-200 — the LB drains the replica.
2. New `AcquireTask` waiters are rejected with a `drain` control signal
   that the worker SDK respects (re-poll on a different replica).
3. Existing waiters are released (no task) and reconnect upstream.
4. In-flight state-transition transactions are allowed to complete.
5. The OTel pipeline is flushed.
6. The process exits.

Drain timeout: 30s by default (TODO-tunable in a future config field).
Operators running rolling upgrades should ensure the orchestrator's
`terminationGracePeriodSeconds` is at least drain timeout + 5s.

Lease handoff during drain: leases held on the draining replica are NOT
revoked — workers continue heartbeating and the leases live until either
the worker completes or the lease times out and Reaper A reclaims (any
replica). No special handoff protocol; the storage layer is the
coordination point.

---

## 9. Failure modes

### 9.1 Postgres unreachable

- All state-transition RPCs return `Status::unavailable` after a brief
  retry (transparent `40001` retries do not cover network errors).
- Heartbeats fail to upsert; workers continue executing but their lease
  extensions stop. Eventually leases time out; Reaper A reclaims when
  Postgres returns.
- `/readyz` flips to non-200 within one health-check cycle.
- Operator action: restore Postgres connectivity. The CP requires no
  manual restart — it reconnects via the pool.

### 9.2 Reaper falls behind

Symptoms: `taskq_lease_expired_total` rate climbs faster than the
reaper's reclaim rate; `task_runtime` row count grows monotonically.

- Cause: contention with concurrent state transitions; reaper batches
  exhaust `40001` retries and defer rows.
- Mitigation:
  - Bump reaper batch size (default 1000; configurable).
  - Run a second CP replica to share reaper load (both reapers use
    `SKIP LOCKED` so they do not collide).
  - Investigate the underlying contention with
    `taskq_storage_serialization_conflict_total` and
    `taskq_storage_retry_attempts`.

### 9.3 Quota cache stale

Symptoms: rate-quota changes don't take effect immediately
([`design.md`](./design.md) §1.1, §9.1).

- This is the documented eventual-consistency window (default 5s, jittered
  ±10%). Aggregate rate may briefly exceed the configured limit after a
  downward write; admission self-corrects within the TTL window.
- Operator action: wait one cache TTL. If the change still hasn't
  propagated after `2 * quota_cache_ttl_seconds`, restart the CP — the
  cache is fully reseeded on startup.

### 9.4 LISTEN connection drop (Postgres)

The CP's per-replica LISTEN demux connection may drop. Symptoms:
`taskq_long_poll_wait_seconds` p99 climbs toward the
`belt_and_suspenders_seconds` value (default 10s).

- The 10s belt-and-suspenders unconditional re-query
  ([`design.md`](./design.md) §6.2 step 4b, §8.4) catches missed
  notifications; correctness is preserved, just at higher latency.
- The CP reconnects the LISTEN automatically on next demux loop. If
  reconnection persistently fails, `/readyz` flips on the next health
  check.

### 9.5 OTLP collector unavailable

OTel's buffered exporter drops samples after the buffer fills. The
`taskq_export_dropped_total` counter rises (TODO: confirm name in
implementation; [`problems/11`](./problems/11-observability.md) Open Q2).
CP threads are never blocked by export. Operator action: investigate the
collector; the CP itself is healthy.

---

## 10. Cross-references

- [`README.md`](./README.md) — service overview, build instructions
- [`architecture.md`](./architecture.md) — system architecture diagrams
- [`design.md`](./design.md) — full design specification
- [`tasks.md`](./tasks.md) — phase plan and outstanding work
- [`protocol/EVOLUTION.md`](./protocol/EVOLUTION.md) — schema and protocol
  evolution rules
- [`protocol/QUOTAS.md`](./protocol/QUOTAS.md) — per-quota validation
  rules and operator guidance
- [`GLOSSARY.md`](./GLOSSARY.md) — terms used across the docs
- [`dashboards/README.md`](./dashboards/README.md) — Grafana dashboard
  import instructions
- `codelabs/03-operating-taskq-rs.md` (planned) — hands-on operator
  workflows: provisioning a namespace, draining a DLQ, rotating quotas,
  debugging a stuck task. Pair with this reference for context.
- [`taskq-cli/README.md`](./taskq-cli/README.md) — operator CLI surface
  and exit codes
- [`problems/`](./problems/) — full problem-by-problem design
  discussions
