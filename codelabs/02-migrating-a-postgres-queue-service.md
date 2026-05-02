<!-- agent-updated: 2026-05-02T18:00:00Z -->

# Codelab 2: Migrating a hand-rolled Postgres queue to taskq-rs

You have a service backed by a `tasks` table in Postgres. A dispatcher
worker `SELECT ... FOR UPDATE SKIP LOCKED`s rows, runs them, then
`UPDATE`s the row to a terminal state. You wrote retry math by hand and
a dead-letter table for rows whose `retry_count` ran past your
threshold. It works -- but it does not scale across teams, the retry
logic is duplicated in three repos, and you would rather not be the
person paged when the cleanup cron falls behind.

This codelab walks you through swapping that homegrown queue out for
`taskq-rs` with no downtime.

## Audience

Teams running:

- a Postgres `tasks` table with a custom dispatcher loop;
- ad-hoc retry / backoff logic per task type;
- a manual dead-letter pattern (a separate table, a status enum, or a
  `retry_count > N` predicate);
- per-call idempotency hacks bolted onto the application layer.

If you have all four, you are the audience. If you have one or two
you can still read on -- the mapping table below is useful even if
you only adopt half of it.

## What you keep, what taskq-rs replaces

| Your homegrown feature                          | taskq-rs equivalent                                                                                                |
|-------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `INSERT INTO tasks (...)`                       | `SubmitTask` RPC ([`design.md` Sec 6.1](../design.md))                                                             |
| Dispatcher worker `SELECT ... FOR UPDATE SKIP LOCKED` | `AcquireTask` long-poll + storage trait `pick_and_lock_pending` ([`design.md` Sec 6.2, Sec 8.2](../design.md))     |
| `retry_count + 1` and ad-hoc backoff            | Server-side retry math with full jitter ([`design.md` Sec 6.5](../design.md)); per-task / per-task-type / per-namespace layers |
| `dead_letter` table or `status = 'DEAD'` rows   | Terminal states `FAILED_NONRETRYABLE`, `FAILED_EXHAUSTED`, `EXPIRED` ([`design.md` Sec 5](../design.md))           |
| Manual dead-letter requeue                      | `ReplayDeadLetters` admin RPC + `taskq-cli replay`                                                                 |
| `process_after TIMESTAMP` column                | `WAITING_RETRY.retry_after` (server-managed); `expires_at` for hard TTL                                            |
| Application-level idempotency hashing           | Required `idempotency_key` on `SubmitTask`, scoped `(namespace, key)`, BLAKE3 payload hash on the row              |
| Custom rate-limiting middleware                 | Per-namespace `max_submit_rpm` / `max_dispatch_rpm` quotas ([`design.md` Sec 9.1](../design.md))                   |
| Hand-rolled OTel spans across enqueue/dequeue   | W3C `traceparent` persisted on the task row, returned to workers on `AcquireTask` ([`design.md` Sec 11.2](../design.md)) |
| `pg_cron` cleanup of old rows                   | Range-partitioned `idempotency_keys` cleaned via `DROP PARTITION`; audit log retention per namespace               |
| The `dispatcher_lock` advisory lock you regret  | Multi-replica capable from day one -- no single dispatcher process; SERIALIZABLE everywhere ([`design.md` Sec 1](../design.md)) |

What you keep:

- Your application-side business logic: nothing in taskq-rs touches
  what your handler actually does.
- Your Postgres operational expertise: taskq-cp ships a Postgres
  backend that uses your existing instance.
- Your observability stack: OTel spans, Prometheus scrapes, and any
  audit-log shipping you already have.

## Migration plan

A four-step strategy. Each step keeps both systems running so you can
roll back at any boundary.

### Step 1: Stand up `taskq-cp` alongside your current queue

Deploy `taskq-cp` against the same Postgres instance, in a dedicated
schema or database so the migrations cannot touch your tables. See
[Codelab 3](03-operating-taskq-rs.md) Day-1 deployment for the full
walkthrough; the short version:

```bash
taskq-cp migrate --config /etc/taskq/cp.toml
taskq-cp serve   --config /etc/taskq/cp.toml
```

Verify the schema landed:

```bash
taskq-cli namespace create migration-test
taskq-cli set-quota migration-test --max-pending 100 --max-workers 4
taskq-cli stats migration-test
```

The CP is now reachable but no callers or workers are pointed at it
yet. Your old queue still owns 100% of traffic.

### Step 2: Migrate one task type at a time

Pick the lowest-risk task type first -- something idempotent, low
volume, with clear failure semantics. For each type:

#### 2a. Dual-write at the caller

Before:

```rust
sqlx::query!(
    "INSERT INTO tasks (task_type, payload, status) VALUES ($1, $2, 'pending')",
    task_type,
    payload
)
.execute(&pool)
.await?;
```

After (writes to both):

```rust
// Write to your existing table -- unchanged.
sqlx::query!(
    "INSERT INTO tasks (task_type, payload, status) VALUES ($1, $2, 'pending')",
    task_type,
    payload
)
.execute(&pool)
.await?;

// Mirror to taskq-rs. Use a deterministic idempotency key so a retried
// outer caller does not double-submit.
let idempotency_key = format!("legacy:{}", legacy_task_id);
let mut req = SubmitRequest::new("orders", task_type, Bytes::from(payload.clone()));
req.idempotency_key = Some(idempotency_key);
let _ = taskq_client.submit(req).await; // log on failure; do not block
```

The idempotency key is the legacy row's primary key -- so a caller
restart that re-runs the legacy insert + the taskq-rs submit gets a
`SubmitOutcome::Existing` from the second submit, not a duplicate task.

While dual-writing, the taskq-rs side has no workers yet. Tasks pile
up in `PENDING`. That is intentional -- you are validating
submission throughput and idempotency before anything tries to run
them. Watch `taskq-cli stats <ns>` and the `taskq_pending_count` gauge
([`design.md` Sec 11.3](../design.md)) until you are convinced.

#### 2b. Dual-read on the worker

Stand up a taskq-rs worker that handles the same task type. While both
queues are live, your worker decides which queue is canonical for a
given task by checking the legacy row first:

```rust
impl TaskHandler for OrderHandler {
    async fn handle(&self, task: AcquiredTask) -> HandlerOutcome {
        // Pull the legacy row by id (we put it in the payload at submit time).
        let legacy_id: String = decode_legacy_id(&task.payload);
        let legacy_status = self
            .legacy_pool
            .query_one("SELECT status FROM tasks WHERE id = $1", &[&legacy_id])
            .await;

        match legacy_status {
            Ok(row) if row.get::<_, String>(0) == "completed" => {
                // The legacy dispatcher already finished it.
                // Mark the taskq-rs side complete with no work; the
                // result payload is empty.
                HandlerOutcome::Success(Bytes::new())
            }
            _ => self.real_work(task).await,
        }
    }
}
```

You also turn off the legacy dispatcher for this task type, or have it
skip rows that already have a corresponding taskq-rs entry. Pick one;
running both real dispatchers concurrently is a recipe for double-
processing.

#### 2c. Cut over the caller

Once the worker has been processing through taskq-rs cleanly for a
soak period (24h is a reasonable starting point):

```rust
// Before
sqlx::query!("INSERT INTO tasks (...) VALUES (...)").execute(&pool).await?;
let _ = taskq_client.submit(req).await;

// After
taskq_client.submit(req).await?;
```

You no longer write to the legacy table for this task type. The
worker can keep its dual-read for one more soak period as a safety
net, then collapse to single-read.

### Step 3: Drain the old queue

For each retired task type:

1. Stop new writes (Step 2c).
2. Let the legacy dispatcher chew through the residual rows. Track
   the count with `SELECT count(*) FROM tasks WHERE task_type = $1 AND
   status IN ('pending', 'in_progress')`.
3. When the count is zero, archive the rows somewhere cold (or just
   leave them -- they are now history) and remove the task type from
   the legacy dispatcher's allowlist.

### Step 4: Decommission

Once every task type is in taskq-rs:

- Delete the legacy `tasks` and `dead_letter` tables (or rename them
  to `tasks_archived_<date>` if you are nervous).
- Delete the legacy dispatcher binary.
- Delete the home-grown retry / backoff / dedup helper crate.
- Remove the legacy connection pool from your callers' DI graph.
- Update your runbooks to point at [Codelab 3](03-operating-taskq-rs.md).

## Mapping common patterns

### "We used `SELECT ... FOR UPDATE SKIP LOCKED`"

You already have the right intuition -- taskq-rs's storage trait is
exactly this pattern, named `pick_and_lock_pending`
([`design.md` Sec 8.1](../design.md)). The Postgres backend translates
it to `FOR UPDATE SKIP LOCKED`; the SQLite backend's single-writer
locking trivially satisfies it; future backends like FoundationDB use
their conflict-range retry primitives. You do not implement it -- you
implement `TaskHandler` and let the SDK do the polling.

### "We used a `retry_count` column"

Built-in. The `tasks` row carries `attempt_number`, `max_retries`,
`retry_initial_ms`, `retry_max_ms`, `retry_coefficient`. The CP
computes the next `retry_after` with full jitter inside the
`ReportFailure` transaction. You set caller-side overrides on
`SubmitRequest`:

```rust
let mut req = SubmitRequest::new("orders", "send_email", payload);
req.max_retries = Some(5);
req.retry_config = Some(taskq_caller_sdk::RetryConfigOverride {
    initial_ms: 1_000,
    max_ms: 60_000,
    coefficient: 2.0,
    max_retries: 5,
});
```

If you do not set them, the resolution is per-task-type → per-namespace
defaults ([`design.md` Sec 9.2](../design.md)). The
`max_retries_ceiling` quota on the namespace caps any per-task override
so a noisy caller cannot pin retries forever.

### "We used a `process_after TIMESTAMP` column"

Two cases:

- **Server-managed retry delay** -- you do not need this. When your
  worker returns `HandlerOutcome::RetryableFailure`, the CP transitions
  the task to `WAITING_RETRY` with a server-computed `retry_after` and
  the dispatcher will not pick it up until that time arrives.
- **Caller-supplied delay (a "send this in 1 hour" job)** -- not in v1.
  Workaround: set `expires_at` on the original submit and have the
  caller resubmit when it wants the work to begin. Real scheduling is
  on the v2 list.

### "We had a `dead_letter` table"

Tasks that exhaust retries land in `FAILED_EXHAUSTED`. Tasks that
report `retryable: false` land in `FAILED_NONRETRYABLE`. Tasks that
hit their TTL land in `EXPIRED`. To re-process them:

```bash
# Preview first.
taskq-cli replay --namespace orders --confirm-namespace orders --dry-run

# Then go.
taskq-cli replay --namespace orders --confirm-namespace orders \
    --max-tasks 500 --audit-note "drain after upstream fix"
```

`ReplayDeadLetters` is rate-limited at 100/sec/namespace by default
([`design.md` Sec 6.7](../design.md)) so a `--max-tasks 50000` replay
will take eight minutes -- this is a feature.

### "We hashed the request body for dedup"

Required idempotency keys *plus* a server-side BLAKE3 payload hash
together replace your homegrown hashing. The contract:

- Every `SubmitTask` carries an `idempotency_key`. The Rust SDK auto-
  generates a UUIDv7 if you leave it unset; for migrations you should
  override with a deterministic key (e.g. your legacy primary key).
- The CP stores the BLAKE3 hash of the payload alongside the key. A
  resubmit with the same key but a different payload is rejected with
  `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`, surfaced to the
  caller as `SubmitOutcome::PayloadMismatch { existing_task_id,
  existing_payload_hash }` so you can reconcile.

## Things to watch for

### Per-namespace quotas need configuring before the cutover

Default quotas are conservative. Resize before traffic arrives:

```bash
taskq-cli set-quota orders \
    --max-pending 100000 \
    --max-workers 64 \
    --max-submit-rpm 60000
```

A submit storm against an unconfigured namespace will get
`MAX_PENDING_EXCEEDED` in production. Tune all three -- pending,
workers, and submit RPM -- in line with your peak load.
[Codelab 3](03-operating-taskq-rs.md) Section 3 walks the full
quota matrix.

### Idempotency keys are mandatory and not optional

Your callers must adopt them. Audit every `SubmitTask` call site:

- For event-driven callers (one event → one task), use the event's
  durable id.
- For HTTP-handler callers, use the request id (`X-Request-Id`) or a
  UUIDv7 minted at the gateway.
- For batch jobs, use `format!("{job_id}:{row_id}")`.

The SDK will mint a UUIDv7 for you if you leave it blank, but that
gives you no dedup -- a caller-restart-then-resubmit flow will create
duplicate tasks. The SDK's `SubmitRequest::idempotency_key` field is
explicitly opt-in for callers who can be smarter about it.

### Heartbeat interval matters

The Rust worker SDK clamps its heartbeat cadence to `eps_ms / 3`
read from the `Register` response ([`design.md` Sec 6.3](../design.md)).
If you call `with_heartbeat_interval(d)` with `d > eps_ms / 3` you get
a startup warning -- and a real risk of leases lapsing under load. Do
not override unless you know what you are doing.

The corollary: if you want shorter leases (faster recovery on dead
workers), tune the namespace's `min_heartbeat_interval_seconds` and
`lazy_extension_threshold_seconds` together. The CP rejects configs
that violate `lazy_extension_threshold ≥ 2 × min_heartbeat_interval`
([`design.md` Sec 9.1](../design.md)).

### Required vs recommended migrations

`taskq-cp serve` refuses to start when `taskq_meta.schema_version` does
not match the binary's expected version. Production deploys should run
`taskq-cp migrate` deliberately ([`design.md` Sec 12.5](../design.md)),
not rely on `--auto-migrate` (which is a dev convenience). Add the
migrate step to your deploy pipeline before the rolling restart.

### Trace context

Your callers must forward W3C `traceparent` if they want unified
traces. The Rust caller SDK extracts the current `tracing::Span`'s
OTel context automatically; if you call `submit` outside of any span,
the SDK mints a fresh W3C-valid context so the server always has
something to persist. Your migration is a great time to set up the
OTel SDK if you have not already.

## Rollback plan

You stay rollback-safe by keeping the dual-write window long. Concrete
fall-back steps if something goes wrong mid-migration:

| At step | Symptom                                | Roll back by                                                                                  |
|---------|----------------------------------------|-----------------------------------------------------------------------------------------------|
| 2a      | taskq-rs submission errors spike       | Wrap the second `submit` in `tokio::spawn(...)` and ignore failures; the legacy queue runs as before |
| 2b      | Tasks stuck in `WAITING_RETRY`         | Disable the taskq-rs worker (kill the process); the legacy dispatcher continues to drain      |
| 2c      | Cutover bleeds errors                  | Revert the caller change to write to both queues; bring the legacy dispatcher back            |
| 3       | Cannot drain residual rows             | Leave them; they are unobservable to taskq-rs and harmless to your business                   |
| 4       | Realize you wanted the old DLQ table   | Restore from your archive. taskq-rs will not touch tables it did not create                   |

The hard rule: you cannot un-submit a task once it is in taskq-rs. If
the cutover sent garbage, use `taskq-cli purge --namespace <ns>
--confirm-namespace <ns> --dry-run` first, then drop the `--dry-run`
([`taskq-cli` README](../taskq-cli/README.md)). `PurgeTasks` is
destructive and audit-logged.

## Checklist before flipping the switch

Before Step 2c on each task type:

- [ ] Quotas sized for peak load (`max_pending`, `max_workers`,
      `max_submit_rpm`)
- [ ] Worker fleet running with `concurrency` matching your old
      dispatcher's parallelism (or higher)
- [ ] Idempotency keys deterministic at every caller site
- [ ] Error classes registered for the task type (`taskq-cli set-quota
      <ns>` plus `SetNamespaceConfig` to add classes; deferring this is
      fine -- you can use the namespace's default set initially)
- [ ] Trace context flowing end-to-end (verify in your tracing UI)
- [ ] Alert on `taskq_failed_exhausted_total{namespace=<ns>}` and
      `taskq_dispatch_latency_seconds_p99` so a regression pages you
- [ ] Postgres has headroom for the new write rate -- the
      `task_runtime` table sees one row per dispatched task and the
      `worker_heartbeats` table writes one cheap UPSERT per ping
      ([`design.md` Sec 8.3](../design.md))

## Next steps

- [Codelab 3](03-operating-taskq-rs.md) -- once you are migrated,
  the day-2 operator playbooks.
- [`design.md` Sec 6, 9, 10](../design.md) -- lifecycle, configuration,
  rejection structure.
- [`problems/03-idempotency.md`](../problems/03-idempotency.md) -- why
  required keys, BLAKE3 payload hashing, and the terminal-state-vs-key
  table look the way they do.
