<!-- agent-updated: 2026-05-02T18:00:00Z -->

# Codelab 3: Operating taskq-rs in production

This codelab is the SRE-facing playbook. It assumes you have read
[Codelab 1](01-build-a-worker-in-50-lines.md) (so you understand what a
worker does) and that you have either followed
[Codelab 2](02-migrating-a-postgres-queue-service.md) or are deploying
green-field. Every section maps to a real `taskq-cp`, `taskq-cli`, or
`design.md` surface; nothing here is aspirational.

## 1. Day-1 deployment

### Storage

Provision a Postgres 14+ instance with `default_transaction_isolation =
serializable` *off* (the CP sets isolation per-transaction; setting it
globally interferes with `worker_heartbeats` writes that intentionally
run at READ COMMITTED -- see [`design.md` Sec 1.1](../design.md)).
Recommended starting tunings:

```ini
max_connections = 200             # CP pool default 16 per replica
shared_buffers = 4GB              # rule of thumb: 25% of RAM
work_mem = 16MB
checkpoint_timeout = 15min
wal_compression = on
default_statistics_target = 200   # the dispatch query benefits
```

Create a database and a role for the CP:

```sql
CREATE DATABASE taskq;
CREATE ROLE taskq_cp WITH LOGIN PASSWORD 'redacted';
GRANT ALL PRIVILEGES ON DATABASE taskq TO taskq_cp;
```

Apply schema:

```bash
taskq-cp migrate --config /etc/taskq/cp.toml
```

`migrate` reads `[storage_backend]` from the config, runs the bundled
SQL migrations idempotently, and exits ([`design.md` Sec 12.5](../design.md)).
Production deploys must run `migrate` before `serve` -- the CP refuses
to start when `taskq_meta.schema_version` does not match the binary's
expected version.

### CP config

Minimum viable `taskq-cp.toml` for a Postgres backend:

```toml
bind_addr = "0.0.0.0:50051"
health_addr = "0.0.0.0:9090"

quota_cache_ttl_seconds = 5
long_poll_default_timeout_seconds = 30
long_poll_max_seconds = 60
belt_and_suspenders_seconds = 10
waiter_limit_per_replica = 5000
lease_window_seconds = 30

[storage_backend]
kind = "postgres"
url = "postgresql://taskq_cp:redacted@db.internal:5432/taskq"
pool_size = 32

[otel_exporter]
kind = "otlp"
endpoint = "http://otel-collector.internal:4317"
```

Every numeric field has a default that matches `design.md`. Set
`pool_size` based on expected concurrent transactions per replica
(rule of thumb: `2 × waiter_limit_per_replica / 100`, plus headroom
for reapers).

### Port binding

Per global rule #8 (Tailscale): set `TAILSCALE_IP` in the systemd unit
or k8s pod spec, and the CP rebinds `bind_addr` to that interface
automatically. The port is taken from the file value, so operators
only need to set `TAILSCALE_IP=$(tailscale ip -4)` once globally.

```ini
# /etc/systemd/system/taskq-cp.service
[Service]
Environment="TAILSCALE_IP=100.64.0.42"
ExecStart=/usr/local/bin/taskq-cp serve --config /etc/taskq/cp.toml
Restart=on-failure
User=taskq
```

`TASKQ_BIND_ADDR` overrides the whole `host:port` if you need it
([`taskq-cp/src/config.rs`](../taskq-cp/src/config.rs)).

### Health endpoints

`taskq-cp` exposes three endpoints on `health_addr`:

- `/healthz` -- process liveness. Returns 200 once the binary is up.
- `/readyz` -- 200 only when storage is reachable, schema version
  matches, and strategies are loaded. Use this for k8s readiness
  probes; the CP will not accept gRPC traffic before this returns 200.
- `/metrics` -- Prometheus exposition (only when `[otel_exporter]
  kind = "prometheus"`).

## 2. Schema migrations

The migration story for both backends:

```bash
# Apply pending migrations.
taskq-cp migrate --config /etc/taskq/cp.toml

# Then start serving.
taskq-cp serve --config /etc/taskq/cp.toml
```

`taskq-cp serve` will refuse to start if `taskq_meta.schema_version`
does not match the binary, with a clear error pointing at `migrate`.
This is the v1 deploy contract -- you do not get auto-migration in
production.

For development you can shortcut with:

```bash
taskq-cp serve --auto-migrate --config /etc/taskq/cp.toml
```

This logs a warning and runs migrations before serving traffic.
Convenience only; do not put `--auto-migrate` in your prod systemd unit.

### Version compatibility

Each `taskq-cp` binary embeds a `SCHEMA_VERSION` constant. Migrations
are append-only and ordered. The compatibility matrix:

| Direction                       | Supported? | Notes                                                    |
|---------------------------------|------------|----------------------------------------------------------|
| Same major, newer minor binary  | Yes        | Rolling upgrade across replicas; new optional fields default to old behaviour ([`design.md` Sec 12.7](../design.md)) |
| Same major, older minor binary  | Yes (within compat window) | Old binaries can read rows written by newer ones |
| Across major                    | No         | Drain-and-restart cluster                                |

### Rollback

If a migration goes wrong, the rollback story is "drop the new tables
and restore from backup." taskq-rs does not ship reversible migrations
-- the schema is small and the rollback path is "scrap the schema, re-
apply from your backup." Practical implication: take a Postgres
snapshot immediately before running `taskq-cp migrate` in production.

## 3. Quota tuning

Per-namespace quotas are the load-bearing knobs. Enumerate them with:

```bash
taskq-cli stats <namespace> --format json
```

The full quota object lives on the `namespace_quota` row
([`design.md` Sec 4](../design.md)). The fields you will tune most:

### `max_pending`

The cap on `PENDING` + `WAITING_RETRY` rows for the namespace.

- **Symptom of "too low"**: callers see
  `RESOURCE_EXHAUSTED { reason: MAX_PENDING_EXCEEDED }`. The Rust
  caller SDK retries with backoff up to its retry budget, then
  surfaces to user code.
- **Symptom of "too high"**: dispatch tail latency creeps up because
  the `pick_and_lock_pending` query is sampling a deeper queue. The
  Postgres backend has indexes for this exact path
  ([`design.md` Sec 8.3](../design.md)) so the slowdown is gentle, not
  cliff-like.
- **Bump to**: peak burst size + a few minutes of typical throughput.
  E.g. for a 1000 RPM submit rate with 10s typical processing, set
  `max_pending = 200` (peak) `+ 1000 / 60 × 600 = 10000` headroom.

### `max_workers`

The cap on registered workers for the namespace.

- **Symptom of "too low"**: workers see `WAITER_LIMIT_EXCEEDED` on
  `Register`. `taskq-cli list-workers <ns>` shows fewer than expected.
- **Bump to**: the actual fleet size. There is no benefit to
  unspecified caps; this is just a sanity guard against runaway
  scale-out.

### `max_submit_rpm` / `max_dispatch_rpm`

Per-minute rate caps. Eventually consistent within `quota_cache_ttl_seconds`
(default 5s) so a downward write may briefly let traffic exceed the
new cap ([`design.md` Sec 1.1](../design.md)).

- **Symptom of "too low"**: spikes get `NAMESPACE_QUOTA_EXCEEDED`
  during normal traffic.
- **Symptom of "too high"**: noisy neighbour can starve the dispatch
  query. Watch `taskq_storage_transaction_seconds`; if a namespace
  drives p99 > 50ms, dial it back.

### `min_heartbeat_interval_seconds`

The minimum heartbeat cadence the CP will accept. Workers cadence
faster than this are rejected on `Heartbeat`.

- **Symptom of "too short"**: heartbeat write storm against
  `worker_heartbeats`. The Postgres backend's documented
  v1 ceiling is ~10K heartbeats/sec single-replica
  ([`design.md` Sec 8.3](../design.md)).
- **Symptom of "too long"**: dead workers go undetected for too long;
  reaper B's reclaim window stretches.
- **Default**: 5s. Bump to 15s if you are at heartbeat-write capacity;
  keep below 10s if you want fast dead-worker detection.

### `lazy_extension_threshold_seconds` (`eps`)

How long the SERIALIZABLE lease extension waits before firing.

- **Invariant**: `eps ≥ 2 × min_heartbeat_interval_seconds`. The CP
  rejects configs that violate this ([`design.md` Sec 9.1](../design.md))
  -- two heartbeats per `eps` window guarantees recovery from a single
  dropped ping.
- **Default**: 25% of the lease (so for a 30s lease, `eps = 7.5s`).

### `max_idempotency_ttl_seconds`

How long the dedup row sticks around after the task hits a terminal
state. Hard-capped at 90 days so the partition count stays bounded.

- **Bump to**: the longest a caller might reasonably retry. 24h
  default is right for most online traffic; bump to 7d for batch jobs
  that may not be observed quickly.

## 4. Observing

### The standard metric set

All metrics prefixed `taskq_`. The full list lives in
[`design.md` Sec 11.3](../design.md); here is the operational starter
pack:

```text
# Submit / dispatch counters
taskq_submit_total{namespace, task_type, outcome}
taskq_dispatch_total{namespace, task_type}
taskq_complete_total{namespace, task_type}
taskq_fail_total{namespace, task_type, error_class}
taskq_terminal_total{namespace, task_type, terminal_state}

# Reaper / lease events
taskq_lease_expired_total{namespace, kind=A|B}
taskq_storage_retry_attempts{op}            # 40001 retries per logical op

# Histograms (label by namespace only -- task_type dropped per Sec 11.3)
taskq_dispatch_latency_seconds{namespace}
taskq_long_poll_wait_seconds{namespace}
taskq_storage_transaction_seconds{namespace, op}

# SDK-side
taskq_sdk_retry_total{namespace, reason}
```

Note `task_id` is **never** a label -- it goes into traces, not
metrics. The cardinality budget is bounded by the
`max_error_classes` (default 64) and `max_task_types` (default 32)
quotas per namespace ([`design.md` Sec 11.3](../design.md)).

### Grafana panel checklist

The first dashboard you build for any namespace:

1. Submit rate by task_type (counter, `rate()` over 5m).
2. Terminal-state breakdown stacked area
   (`taskq_terminal_total` by `terminal_state`).
3. p50 / p95 / p99 dispatch latency (`taskq_dispatch_latency_seconds`).
4. Pending count gauge (`taskq_pending_count`).
5. Workers registered gauge (`taskq_workers_registered`).
6. Lease expirations by kind (`taskq_lease_expired_total{kind="A"|"B"}`).
   A bumps when handlers run too long; B bumps when workers die.
7. SDK retries (`taskq_sdk_retry_total`) by `reason` -- a sustained
   non-zero rate means callers are paying for retries.
8. Storage 40001 retries (`taskq_storage_retry_attempts`) -- a sustained
   p99 > 1 means contention is real, time to look at the dispatcher
   strategy.

### Audit log

Every admin RPC writes a row to `audit_log` inside its transaction
([`design.md` Sec 11.4](../design.md)). Useful queries:

```sql
-- Who changed quotas in the last 24h?
SELECT timestamp, actor, namespace, request_summary
  FROM audit_log
 WHERE rpc = 'SetNamespaceQuota'
   AND timestamp > now() - interval '24 hours'
 ORDER BY timestamp DESC;

-- All replays in the last week.
SELECT timestamp, actor, namespace, request_summary->>'audit_note' AS note
  FROM audit_log
 WHERE rpc = 'ReplayDeadLetters'
   AND timestamp > now() - interval '7 days'
 ORDER BY timestamp DESC;
```

`request_hash` is sha256 of the request body so external systems can
verify what was sent without storing the payload.

## 5. Incident playbooks

### "Tasks aren't being processed"

Run this checklist top-to-bottom. The first hit is usually the answer.

1. Is the namespace enabled?
   ```bash
   taskq-cli stats <ns>
   ```
   Look for `disabled: true` in the output. If yes, re-enable:
   ```bash
   taskq-cli namespace enable <ns>
   ```

2. Are there registered workers for the task type?
   ```bash
   taskq-cli list-workers <ns> --format json | jq '.workers[].task_types'
   ```
   If the task type is missing from every worker's set, dispatch will
   never match. Deploy a worker that registers for it.

3. Are workers alive?
   ```bash
   taskq-cli list-workers <ns> --include-dead
   ```
   Workers with `declared_dead_at` set were tombstoned by Reaper B
   ([`design.md` Sec 6.6](../design.md)). They cannot transparently
   resurrect by continuing to ping; the SDK must re-Register. Check
   the worker process logs for `WORKER_DEREGISTERED` warnings.

4. Are tasks actually `PENDING`?
   ```sql
   SELECT status, count(*) FROM tasks WHERE namespace = '<ns>' GROUP BY status;
   ```
   `WAITING_RETRY` with future `retry_after` is normal -- those tasks
   are scheduled. `PENDING` rows that are not being acquired despite
   live workers point at a dispatcher mismatch (wrong task type set on
   the worker, or wrong namespace).

5. Is the dispatcher quota being hit?
   ```bash
   taskq-cli stats <ns> --format json | jq '.quota_usage'
   ```
   If `max_dispatch_rpm` is at its cap, dispatch is rate-limited.
   Bump it:
   ```bash
   taskq-cli set-quota <ns> --max-dispatch-rpm 600000
   ```

6. Is the namespace's quota row missing the strategy choice you expect?
   The CP picks `Always` admitter and `PriorityFifo` dispatcher when
   left unset. If you wrote a custom strategy, verify
   `taskq-cli stats <ns> | jq '.strategy'` shows the right kinds.

### "Latency p99 spiked"

Walk these in parallel:

- **Dispatch query** -- check
  `taskq_storage_transaction_seconds{op="pick_and_lock_pending"}`. If
  this is the spike, suspect index degradation. `EXPLAIN ANALYZE` the
  dispatch query against your live data; the indexes shipped in the
  initial migration ([`design.md` Sec 8.3](../design.md)) cover
  `(namespace, status, priority DESC, submitted_at ASC)`. If Postgres
  has chosen a sequential scan, run `ANALYZE tasks`.
- **Reaper backlog** -- if `taskq_lease_expired_total{kind="A"}` rate
  drops while pending count rises, Reaper A is behind. Check its log
  for `40001` retry storms; a sustained storm means real contention.
- **Namespace quota saturation** --
  `taskq_namespace_quota_usage_ratio{kind="max_pending"}` near 1.0
  means you are gated, not slow. Bump the quota.
- **Postgres CPU / IO** -- standard suspects. The `worker_heartbeats`
  table writes can dominate if the fleet is large; look at WAL
  generation rate.

### "Audit log is growing too fast"

The audit log is append-only and per-namespace pruned:

```bash
taskq-cli set-quota <noisy-ns> --audit-log-retention-days 14
```

The pruner runs hourly ([`design.md` Sec 11.4](../design.md)),
rate-limited at 1000 rows per pass per namespace. For a one-shot
cleanup of historical rows, do it directly in Postgres:

```sql
DELETE FROM audit_log
 WHERE namespace = 'noisy-ns'
   AND timestamp < now() - interval '30 days';
```

Run inside a transaction; the audit log uses standard MVCC.

## 6. DLQ replay workflow

Tasks in `FAILED_NONRETRYABLE`, `FAILED_EXHAUSTED`, or `EXPIRED` are
candidates for replay. The standard flow:

```bash
# 1. Always preview first.
taskq-cli replay --namespace orders --confirm-namespace orders --dry-run

# Output:
# Would replay 1342 tasks matching:
#   terminal_state IN (FAILED_EXHAUSTED, FAILED_NONRETRYABLE, EXPIRED)

# 2. Look at a sample to make sure these are what you expect.
taskq-cli stats orders --format json | jq '.recent_failures[:5]'

# 3. Run for real, with an audit note.
taskq-cli replay --namespace orders --confirm-namespace orders \
    --max-tasks 500 --audit-note "drain after upstream fix at 14:02"
```

`ReplayDeadLetters` is rate-limited at 100/sec/namespace by default
([`design.md` Sec 6.7](../design.md)); a `--max-tasks 5000` replay
takes about 50 seconds. The CP rejects replay for any task whose
idempotency key has since been claimed by another submission --
idempotency wins.

`original_failure_count` is preserved across replay so your dashboard
can distinguish first-failures from replays of historical failures.

## 7. Worker fleet management

### Adding workers

Just start more processes pointing at the CP. They auto-register on
`build()`. Watch the count:

```bash
watch -n 2 'taskq-cli list-workers orders | wc -l'
```

If `Register` returns `WAITER_LIMIT_EXCEEDED`, bump the namespace's
`max_workers` quota.

### Removing workers (graceful)

The worker SDK handles SIGTERM cleanly:

```rust
// in main.rs
worker.run_until_shutdown().await?;
```

`run_until_shutdown` listens for SIGINT/SIGTERM, drains in-flight
handlers (default 30s, configurable via `with_drain_timeout`), then
sends `Deregister` ([`taskq-worker-sdk/src/runtime.rs`](../taskq-worker-sdk/src/runtime.rs)).
A k8s rolling deploy with `terminationGracePeriodSeconds: 60` works
out of the box.

### Removing workers (kill -9)

The worker dies. Reaper B notices the missing heartbeats, stamps
`declared_dead_at` on the worker's `worker_heartbeats` row, and
reclaims any leases the worker held ([`design.md` Sec 6.6](../design.md)).
The reclaim window is bounded by the lease duration (default 30s).
Tasks that were in flight transition back to `PENDING` with
`attempt_number++`. No data loss; just a delay.

### Draining for deploy

The standard pattern is `kubectl rollout restart` -- the SIGTERM
flow above handles it. If you want to take a worker out of rotation
without killing it (e.g. for live debugging), there is no graceful
"drain and stay alive" command in v1; either kill it or let it run.
A drain RPC is a v2 candidate.

## 8. Backup and restore

### What to back up

The `tasks`, `task_results`, `task_runtime`, and `audit_log` tables
are the durable state. Standard Postgres backups (`pg_basebackup`,
PITR via WAL archiving, or your managed-Postgres provider's snapshot
feature) cover them.

The `idempotency_keys` table is daily-partitioned by `expires_at`
([`design.md` Sec 8.3](../design.md)) and will be cleaned by `DROP
PARTITION` automatically. You can include it in backups, but losing
the dedup history is recoverable -- the worst case is that callers
who retry a stale request get a fresh task instead of the cached
result.

The `worker_heartbeats` table is ephemeral. Do not bother backing it
up; on restore, all workers re-register.

The `namespace_quota`, `error_class_registry`, and `task_type_retry_config`
tables are configuration. Back them up; losing them means re-running
all your `taskq-cli set-quota` and `SetNamespaceConfig` calls.

### Recommended schedule

- Continuous WAL archiving for point-in-time recovery
  (`archive_command` to S3 or equivalent).
- Daily logical dump of `namespace_quota` + `error_class_registry`
  + `task_type_retry_config` so you can rebuild config without a full
  restore.
- Weekly full base backup.

### Restore drill

Once a quarter, restore your backup into a scratch instance, point a
test `taskq-cp` at it, and run a smoke worker against it. The restore
is "stand up Postgres, point CP at it, register a worker, submit a
task, watch it complete" -- if any step fails, your backup story is
broken.

## 9. Where to go from here

- [`OPERATIONS.md`](../OPERATIONS.md) (Phase 10 deliverable, may not
  exist yet) -- the long-form operations guide.
- [`design.md`](../design.md) -- the canonical reference. Sections 6
  (lifecycle), 8 (storage), 9 (configuration), 10 (rejection), 11
  (observability) are the ones you will revisit most.
- [`problems/`](../problems/) -- the discussion behind every design
  decision, including failure modes and rejected alternatives. If a
  knob in this codelab does not behave the way you expect, the answer
  is usually here.
- [Codelab 1](01-build-a-worker-in-50-lines.md) -- the developer-side
  view. Worth re-reading occasionally so you remember what your
  application teams are actually doing.
