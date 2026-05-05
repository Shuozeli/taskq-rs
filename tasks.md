<!-- agent-updated: 2026-05-05T00:00:00Z -->

# taskq-rs Implementation Tasks

A living roadmap for the v1 implementation. Translated from [`design.md`](design.md) §13 (v1 scope) into discrete work items with phase ordering. Each task references the design section it implements; the implementer should read that section, not re-derive.

This doc tracks **pending** and **done** work. Completed items move from the relevant phase to the "Done" log at the bottom with a date and commit ref. New tasks discovered during implementation should be appended to the appropriate phase, not silently merged into existing items.

For the system shape see [`architecture.md`](architecture.md). For the discussion behind each decision see [`problems/`](problems/).

## Phase ordering

```
0  scaffold ─► 1  proto ─► 2  storage trait ─► 3  Postgres ─► 5  CP ─► 6  obs ─► 7  SDKs ─► 9  conformance ─► 10  release
                                            └► 4  SQLite ───────────────────────────────► 9  conformance
                                                                              └► 8  CLI ─►
```

Phases 3 and 4 can run in parallel after Phase 2 lands. Phase 6 (observability) is woven into Phase 5 from the start, not bolted on. Phase 7 (SDKs) starts as soon as Phase 1 stabilizes — SDK schema codegen can iterate while the CP is being built.

---

## Phase 0 — Repo scaffolding

- [ ] Cargo workspace at the repo root (`Cargo.toml` virtual manifest)
- [ ] Crate skeletons:
  - [ ] `taskq-proto` — generated FlatBuffers bindings + minor wire helpers
  - [ ] `taskq-storage` — `Storage` + `StorageTx` traits and shared types
  - [ ] `taskq-storage-conformance` — shared test suite all backends must pass
  - [ ] `taskq-storage-postgres` — Postgres backend
  - [ ] `taskq-storage-sqlite` — SQLite backend
  - [ ] `taskq-cp` — control-plane binary
  - [ ] `taskq-caller-sdk` — caller-side Rust SDK
  - [ ] `taskq-worker-sdk` — worker-side Rust SDK
  - [ ] `taskq-cli` — `taskq-cp migrate`, `taskq-cp serve`, etc.
- [ ] `LICENSE` (Apache-2.0 or MIT — confirm with project owner)
- [ ] `CHANGELOG.md` with `## [Unreleased]` section
- [ ] `CONTRIBUTING.md` with build instructions and PR workflow
- [ ] `.github/workflows/ci.yml` — `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test --workspace`, `cargo build --release`
- [ ] Pre-commit config (matching Shuozeli convention) — fmt, clippy, fbs schema lint
- [ ] `rust-toolchain.toml` pinning a stable channel (≥ 1.75 for native `async fn in trait`)
- [ ] Initial `architecture.md` and `tasks.md` ✓ (this file)

---

## Phase 1 — Wire schema (`taskq-proto`)

References: [`design.md`](design.md) §3, §10.1, §11.2; [`problems/09`](problems/09-versioning.md).

- [ ] Define `taskq.v1` namespace in `.fbs` files. One file per service:
  - [ ] `task_queue.fbs` — caller-facing (`SubmitTask`, `GetTaskResult`, `BatchGetTaskResults`, `CancelTask`, `SubmitAndWait`)
  - [ ] `task_worker.fbs` — worker-facing (`Register`, `AcquireTask`, `Heartbeat`, `CompleteTask`, `ReportFailure`, `Deregister`)
  - [ ] `task_admin.fbs` — operator-facing (`SetNamespaceQuota`, `SetNamespaceConfig`, `PurgeTasks`, `ReplayDeadLetters`, `EnableNamespace`, `DisableNamespace`, `GetStats`, `ListWorkers`)
  - [ ] `common.fbs` — shared types: `Task`, `Lease`, `Failure`, `RetryConfig`, `RejectReason` enum, `TerminalState` enum, `TraceContext` (W3C), `SemVer`
- [ ] Wire-version handshake fields on every first RPC
- [ ] `RejectReason` enum populated per [`design.md`](design.md) §10.1 (including `REPLICA_WAITER_LIMIT_EXCEEDED`, `WORKER_DEREGISTERED`, `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`, etc.)
- [ ] FlatBuffers `deprecated` attribute usage documented in `protocol/EVOLUTION.md`
- [ ] Generated bindings via `flatbuffers-rs` build script
- [ ] Schema-diff CI step gating PRs (depends on coordinated dep — see below)

---

## Phase 2 — Storage trait (`taskq-storage`, `taskq-storage-conformance`)

References: [`design.md`](design.md) §8; [`problems/12`](problems/12-storage-abstraction.md).

- [ ] `Storage` trait (connection lifecycle): `begin() -> Tx`
- [ ] `StorageTx` trait with all methods from [`design.md`](design.md) §8.1:
  - [ ] Submit path: `lookup_idempotency`, `insert_task`
  - [ ] Dispatch path: `pick_and_lock_pending`, `record_acquisition`, `subscribe_pending`
  - [ ] Worker path: `complete_task`
  - [ ] Reaper: `list_expired_runtimes`, `reclaim_runtime`
  - [ ] Heartbeat carve-out: `record_worker_heartbeat`, `extend_lease_lazy`
  - [ ] Quota: `check_capacity_quota`, `try_consume_rate_quota`
  - [ ] Cleanup: `delete_expired_dedup`
  - [ ] Admin/reads: `get_namespace_quota`, `audit_log_append`, others as needed
  - [ ] `commit`, `rollback`
- [ ] Supporting types: `PickCriteria`, `PickOrdering` enum, `CapacityKind`, `RateKind`, `TaskOutcome`, `LeaseRef`, `RuntimeRef`, `WakeSignal`, `HeartbeatAck`, `StorageError`
- [ ] `StorageError` enum: `SerializationConflict`, `ConstraintViolation`, `NotFound`, `BackendError(BoxedError)`
- [ ] Conformance test suite scaffolding: parameterizable across backends, organized by the six conformance requirements ([`design.md`](design.md) §8.2)
- [ ] Conformance tests for: external consistency, skip-locking (with single-writer carve-out), indexed range scans, atomic conditional insert, subscribe-pending ordering invariant, bounded-cost dedup expiration

---

## Phase 3 — Postgres backend (`taskq-storage-postgres`)

References: [`design.md`](design.md) §8.3; [`problems/12`](problems/12-storage-abstraction.md) round-2 refinement.

- [ ] Migrations directory with `0001_initial.sql` covering:
  - [ ] `tasks` (with composite index on `(namespace, status, priority DESC, submitted_at ASC)`)
  - [ ] `task_runtime`
  - [ ] `worker_heartbeats` with `declared_dead_at` and **BRIN index on `last_heartbeat_at`**
  - [ ] `task_results`
  - [ ] `idempotency_keys` **range-partitioned by `expires_at`** (daily granularity); supporting partition-creation procedure for the next ~95 days plus a maintenance task to drop expired partitions
  - [ ] `namespace_quota`, `error_class_registry`, `audit_log`, `task_type_retry_config`, `taskq_meta`
- [ ] Postgres-specific implementation of `StorageTx` methods
- [ ] `pick_and_lock_pending` translates `PickOrdering` variants to native SQL (`FOR UPDATE SKIP LOCKED`)
- [ ] `subscribe_pending` via `LISTEN taskq_pending_<namespace>` + 500ms fallback poll
- [ ] Quota implementation: capacity quotas read inline transactionally (no cache fence — round-2 verdict); rate quotas via per-replica in-memory token buckets
- [ ] `record_worker_heartbeat` as `INSERT ... ON CONFLICT DO UPDATE WHERE declared_dead_at IS NULL`, returning `HeartbeatAck::WorkerDeregistered` on 0 rows
- [ ] `extend_lease_lazy` SERIALIZABLE update on `task_runtime`
- [ ] `delete_expired_dedup` as `DROP PARTITION` for partitions whose `expires_at` upper bound is past
- [ ] Pass conformance suite

---

## Phase 4 — SQLite backend (`taskq-storage-sqlite`)

References: [`design.md`](design.md) §8.3.

- [ ] Migrations: same logical schema as Postgres, no partitioning (single-writer scope)
- [ ] Single-writer concurrency: serialize all writes through one connection; readers can use a separate connection pool
- [ ] Skip-locking conformance #2 satisfied vacuously (single writer)
- [ ] `subscribe_pending` via 500ms polling (single-process, no NOTIFY)
- [ ] Pass conformance suite (with the single-writer carve-out marker on test #2)
- [ ] Document explicit "embedded/dev only — not multi-process or multi-replica" warning at backend init

---

## Phase 5 — Control plane (`taskq-cp`)

References: [`design.md`](design.md) §6, §7, §9, §10. Built on `pure-grpc-rs`.

### 5.1 RPC server scaffolding

- [ ] `pure-grpc-rs` server bound to configured address (default Tailscale IP per global rule #8)
- [ ] gRPC interceptor for wire-version handshake
- [ ] Auth context extraction (caller/worker identity, namespace claims) — pluggable, default token-based

### 5.2 Lifecycle handlers

- [ ] `SubmitTask` per [`design.md`](design.md) §6.1: admitter → idempotency lookup → retry-config resolution → insert
- [ ] `AcquireTask` per [`design.md`](design.md) §6.2: subscribe-then-query, register-as-waiter, single-waiter wakeup, `WORKER_DEREGISTERED` validation against `worker_heartbeats.declared_dead_at`
- [ ] `Heartbeat` per [`design.md`](design.md) §6.3: READ COMMITTED carve-out, lazy lease extension based on `last_extended_at` semantics, ε ≥ 2 × min_heartbeat_interval invariant
- [ ] `CompleteTask` per [`design.md`](design.md) §6.4: idempotent on `(task_id, attempt_number)`, transparent `40001` retry distinguishing `NotFound` from `SerializationConflict`
- [ ] `ReportFailure` per [`design.md`](design.md) §6.4 + §6.5 (terminal-state mapping): `error_class` validation, server-side backoff math with full jitter, `EXPIRED` short-circuit when `retry_after > task.expires_at`
- [ ] `CancelTask` per [`design.md`](design.md) §6.7: shared `cancel_internal` helper with `PurgeTasks`
- [ ] `GetTaskResult`, `BatchGetTaskResults`, `SubmitAndWait`
- [ ] `Register`, `Deregister`

### 5.3 Strategies (compile-time linked)

- [ ] `Admitter` trait dispatch + per-namespace selection at startup
- [ ] `Always`, `MaxPending`, `CoDel` admitter implementations
- [ ] `Dispatcher` trait dispatch + per-namespace selection at startup
- [ ] `PriorityFifo`, `AgePromoted`, `RandomNamespace` dispatcher implementations
- [ ] Strategy mismatch handling: namespace-level refusal at startup with health-endpoint reporting

### 5.4 Reapers

- [ ] Reaper A — per-task timeout: `WHERE timeout_at <= NOW() FOR UPDATE SKIP LOCKED LIMIT 1000`
- [ ] Reaper B — dead worker: `JOIN worker_heartbeats` with `declared_dead_at IS NULL` filter, stamps `declared_dead_at` on reclaim
- [ ] Both: tolerate `40001` with up to 3 retries, then defer
- [ ] Periodic dedup-expiry job (lazy + periodic) per [`design.md`](design.md) §6.7 / §8.3

### 5.5 Admin RPCs

- [ ] `SetNamespaceQuota` with full validation (registry caps, TTL ceiling, ε invariant)
- [ ] `SetNamespaceConfig` (strategies, registry additions; rejects writes that exceed cardinality caps)
- [ ] `PurgeTasks` (cancel-then-delete-then-delete-dedup via `cancel_internal`; rate-limited)
- [ ] `ReplayDeadLetters` (reset to `PENDING`, reset `attempt_number`, preserve `original_failure_count`; rate-limited; reject if key reclaimed)
- [ ] `EnableNamespace` / `DisableNamespace`
- [ ] `GetStats`, `ListWorkers`
- [ ] All admin RPCs write to `audit_log` in same SERIALIZABLE transaction

### 5.6 Long-poll machinery

- [ ] Per-replica waiter set (in-process; not on correctness path)
- [ ] Single-waiter-per-notification dispatch (avoid thundering herd)
- [ ] 30s default long-poll timeout (60s hard cap)
- [ ] 10s belt-and-suspenders timer in every waiter
- [ ] 5000-default per-replica waiter cap with `REPLICA_WAITER_LIMIT_EXCEEDED`

### 5.7 Health endpoints

- [ ] `/healthz` — process liveness
- [ ] `/readyz` — storage reachable + schema version match + strategies loaded; reports degraded namespaces by name
- [ ] `/metrics` — Prometheus exposition (when Prometheus exporter enabled)

### 5.8 Graceful shutdown

- [ ] SIGTERM handler stops accepting new `AcquireTask`, drains existing waiters, exits when done or hard 60s deadline elapses

---

## Phase 6 — Observability (`taskq-cp` + SDKs)

References: [`design.md`](design.md) §11; [`problems/11`](problems/11-observability.md) round-2 refinement.

- [ ] OpenTelemetry SDK integration (`opentelemetry`, `tracing`, `tracing-opentelemetry`)
- [ ] Built-in exporters: OTLP (gRPC push), Prometheus (pull), stdout (debug). Operators can compile in custom OTel exporters
- [ ] Standard metric set per [`design.md`](design.md) §11.3 — full list emitted from CP and SDKs:
  - [ ] Submit-side: `taskq_submit_total`, `taskq_submit_payload_bytes`, `taskq_idempotency_hit_total`, `taskq_idempotency_payload_mismatch_total`, `taskq_rejection_total`
  - [ ] Dispatch-side: `taskq_dispatch_total`, `taskq_dispatch_latency_seconds`, `taskq_long_poll_wait_seconds`
  - [ ] Lifecycle: `taskq_complete_total`, `taskq_retry_total`, `taskq_terminal_total`, `taskq_error_class_total`
  - [ ] Lease & reaper: `taskq_lease_expired_total`, `taskq_heartbeat_total`
  - [ ] Quota / capacity: `taskq_pending_count`, `taskq_inflight_count`, `taskq_quota_usage_ratio`, `taskq_rate_limit_hit_total`
  - [ ] Workers / connections: `taskq_workers_registered`, `taskq_waiters_active`
  - [ ] Internal health: `taskq_storage_transaction_seconds`, `taskq_storage_serialization_conflict_total`, `taskq_storage_retry_attempts`, `taskq_replay_total`, `taskq_deprecated_field_used_total`, `taskq_schema_version`
  - [ ] SDK-side retry: `taskq_sdk_retry_total`
- [ ] Histograms drop `task_type` from labels (per round-2)
- [ ] Cardinality review checklist in `CONTRIBUTING.md` (build-time lint deferred)
- [ ] W3C trace context persistence on task row + propagation through `AcquireTask` response
- [ ] Reaper / cancellation actions emit child spans of the parent task span
- [ ] Structured JSON logging via `tracing` crate
- [ ] Audit log writes for every admin RPC, same-transaction, with structured `request_summary`

---

## Phase 7 — SDKs (`taskq-caller-sdk`, `taskq-worker-sdk`)

References: [`design.md`](design.md) §6.4, §10.4, §11; [`problems/06`](problems/06-backpressure.md), [`problems/08`](problems/08-retry-storms.md).

### Caller SDK

- [ ] `submit(task)` with required idempotency-key generation (UUID v7 default)
- [ ] `RESOURCE_EXHAUSTED` handling: respect `retry_after` timestamp, `retryable=true` → sleep + retry with ±20% jitter, `retryable=false` → surface to caller
- [ ] Configurable retry budget (default 5)
- [ ] `get_result`, `batch_get_results`, `cancel`, `submit_and_wait`
- [ ] Trace context propagation from caller's tracing context

### Worker SDK

- [ ] `Worker::register(task_types, concurrency)` with version handshake and namespace error-class registry fetch
- [ ] Typed enum for `error_class` generated at SDK init from registry response (compile-time safety on `ReportFailure`)
- [ ] Long-poll `acquire` loop with 30s timeout, immediate reconnect on timeout, exponential backoff on `UNAVAILABLE`
- [ ] Heartbeat task: cadence clamped to `≤ ε / 3` from `Register` response; warns on operator override above that
- [ ] `WORKER_DEREGISTERED` handling: re-`Register` and resume
- [ ] Trace context continuation from `AcquireTaskResponse`
- [ ] `complete(result)`, `report_failure(retryable, error_class, ...)`
- [ ] Graceful shutdown: stop acquiring, drain in-flight, deregister

---

## Phase 8 — CLI tooling (`taskq-cli` / `taskq-cp` binary)

- [ ] `taskq-cp serve --config <path>` to launch the control plane
- [ ] `taskq-cp migrate --backend postgres --auto` for explicit schema migrations (no auto in production)
- [ ] `taskq-cp namespace create|list|enable|disable`
- [ ] `taskq-cp set-quota <namespace> --max-pending <n> ...` (thin wrapper over admin RPC)
- [ ] `taskq-cp purge --namespace <ns> --filter <expr>` (admin)
- [ ] `taskq-cp replay --namespace <ns> --filter <expr>` (admin)
- [ ] Configuration: TOML file, env vars override, `--flag` overrides env

---

## Phase 9 — Conformance and integration tests

- [ ] Conformance suite passes for Postgres
- [ ] Conformance suite passes for SQLite (with single-writer carve-out)
- [ ] Integration tests: full submit → dispatch → complete cycle
- [ ] Integration tests: retry path with all four terminal states
- [ ] Integration tests: lease reclaim by both reapers
- [ ] Integration tests: idempotency-key holding/release per terminal state
- [ ] Integration tests: `CancelTask` ↔ `SubmitTask` race (under SSI)
- [ ] Integration tests: `WORKER_DEREGISTERED` flow (Reaper B reclaims, worker heartbeats, gets dereg)
- [ ] Integration tests: rolling restart with in-flight tasks survives
- [ ] Performance benchmark: validate the ~10K heartbeats/sec single-replica ceiling on Postgres
- [ ] Performance benchmark: 1K admits/sec sustained under contention
- [ ] Performance benchmark: dispatch latency p50/p99 under load

---

## Phase 10 — Polish + release prep

- [ ] `codelabs/01-build-a-worker-in-50-lines.md`
- [ ] `codelabs/02-migrating-a-postgres-queue-service.md`
- [ ] `codelabs/03-operating-taskq-rs.md` (quotas, DLQ replay, debugging)
- [ ] `protocol/EVOLUTION.md` (schema evolution rules)
- [ ] `protocol/QUOTAS.md` (validation rules summary)
- [ ] `OPERATIONS.md` (deployment guide, recommended Postgres tuning, observability setup)
- [ ] Glossary in `architecture.md` or `GLOSSARY.md`
- [ ] Reference dashboards (Grafana JSON) for the standard metric set
- [ ] Tag `v0.1.0` after conformance + benchmarks pass
- [ ] Add `taskq-rs` to the `pidx`-generated index in the Shuozeli superproject (per global rule #4: libcli index maintenance — verify whether taskq-rs counts as libcli or just gets a README entry)

---

## Coordinated dependencies

These are tracked here because they're external to taskq-rs but block specific tasks:

- [ ] **`flatbuffers-rs` schema-diff tool** — needed for the schema-diff CI step (Phase 1). Until landed, ship without CI enforcement and rely on review discipline. See [`problems/09`](problems/09-versioning.md) Open Q5.
- [ ] **`pure-grpc-rs` long-poll support** — confirm the gRPC framework supports server-side long-poll cleanly (deadline-bounded blocking, cancellation propagation). If a feature is missing, file upstream.

---

## Known bugs

_(none currently — see Done log for resolved issues.)_

---

## Done

- [x] Initial commit: README, design.md, problems/, .gitignore, .claude/rules/shared submodule (`2da9b86`, 2026-04-30)
- [x] `architecture.md` — mid-density on-ramp doc (2026-04-30)
- [x] `tasks.md` — this file (2026-04-30)
- [x] Two rounds of adversarial design debate; verdicts applied to design.md (2026-04-29 / -04-30)
- [x] 12 problem docs covering Architecture, Correctness, Operability written, with Round-1 and Round-2 refinement sections (2026-04-26 → -04-30)
- [x] **Phase 0** — Cargo workspace, 9 crate skeletons, LICENSE (MIT), CHANGELOG, CONTRIBUTING, CI workflow (4 jobs), pre-commit, rust-toolchain.toml. CI green on `faf0d0d`. (2026-04-30)
- [x] **Phase 1** — `taskq-proto` wire schema: 4 `.fbs` files (common, task_queue, task_worker, task_admin) under `taskq.v1`; codegen via `flatc-rs-compiler`+`flatc-rs-codegen` from `Shuozeli/flatbuffers-rs`; smoke tests pin enum tag values. Service-trait stub generation deferred to Phase 5. (`c53ba09`, 2026-04-30)
- [x] **Phase 2** — `taskq-storage` trait pair (native `async fn in trait` via `impl Future + Send` desugar); `taskq-storage-conformance` harness with 14 `#[ignore]`'d tests across 6 modules. Added `mark_worker_dead` (surfaced an under-specification in design.md §6.6 → §8.1, since folded back in `5fc45e0`). (`d8ddf87`, 2026-04-30)
- [x] **Design clarifications** — `mark_worker_dead` added explicitly to `StorageTx` in §8.1; `RegisterWorkerResponse` contract documented in §6.3 (`lease_duration_ms`, `eps_ms`, `error_classes`, `worker_id`). (`5fc45e0`, 2026-04-30)
- [x] **Doc freshness sweep** — `<!-- agent-updated: ... -->` line added to all 17 markdown docs per `.claude/rules/shared/api/docsguide.md`. (`63b5fd4`, 2026-04-30)
- [x] **Phase 3** — `taskq-storage-postgres` Postgres backend: `tokio-postgres` + `deadpool-postgres`, SERIALIZABLE state-transition txns, READ COMMITTED carve-out for heartbeats on a separate connection, `idempotency_keys` range-partitioned by `expires_at` (daily) with `taskq_create_idempotency_partitions_through()` SQL function pre-creating ~96 days of runway, BRIN index on `worker_heartbeats(last_heartbeat_at)`, dedicated LISTEN connection demuxed via mpsc to per-namespace subscribers, AFTER INSERT/UPDATE-status-PENDING trigger calling `pg_notify`, in-memory per-replica token-bucket rate limiter, smoke tests against `docker.yuacx.com:5432`. (`01fe7f7`, 2026-04-30)
- [x] **Phase 4** — `taskq-storage-sqlite` SQLite backend (embedded/dev only per design.md §8.3): rusqlite (bundled) with `Arc<Mutex<Connection>>` single-writer model, WAL journal mode + busy_timeout=5000, `BEGIN IMMEDIATE` for trivially-serializable transactions, B-tree (not BRIN) on `last_heartbeat_at`, `tokio::time::interval(500ms)` polling for `subscribe_pending`, smoke tests against in-memory SQLite. Rate-quota and waiter-cap methods return safe no-op values per the agent's read of §1.1. (`8511d55`, 2026-04-30)
- [x] **Phase 5e** — `StorageTx` admin-write follow-ups: `upsert_namespace_quota`, `list_tasks_by_filter`, `list_tasks_by_terminal_status`, `replay_task`, `add_error_classes` / `deprecate_error_class`, `add_task_types` / `deprecate_task_type` on the trait. `task_admin::set_namespace_quota_impl` now actually writes the row; `set_namespace_config_impl` persists registry adds + deprecates; `purge_tasks_impl` and `replay_dead_letters_impl` iterate (rate-limited 100/sec) instead of returning empty. New `NamespaceQuotaUpsert`, `TaskFilter`, `TerminalState`, `ReplayOutcome` types. `taskq-cp` test count: 67 → 75; SQLite smoke test count: 2 → 9. Auth wiring left at Phase 7 with `TODO(phase-7-auth)` markers. (`9e8a9fc`, 2026-04-30)
- [x] **Phase 5a** — control-plane scaffold (`taskq-cp`): `pure-grpc-rs` server + version-handshake interceptor, OTel pipeline scaffold (OTLP + Prometheus + stdout), axum health endpoints, strategy registry with object-safe `dyn Admitter` / `dyn Dispatcher` traits via `async-trait`, graceful shutdown, 22 metric instrument declarations, three handler modules with `todo!()` bodies. 27 unit tests. (`21f8029`, 2026-04-30)
- [x] **Phase 5b-pre** — codegen rewire to `grpc-build`; `StorageTxDyn` shim expanded from 4 to 16 methods; all five strategies populated; `taskq-cp migrate` subcommand + `--auto-migrate` flag with schema-version gate. 36 unit tests. (`cabeb5a`, 2026-04-30)
- [x] **`taskq-proto` codegen finalization** — bumped `pure-grpc-rs` pin to `58c35d9` after extending `compile_fbs` upstream to emit complete FlatBuffers gRPC stubs (owned wrappers via Object API, `FlatBufferGrpcMessage` impls, server + client modules). `taskq-proto/src/lib.rs` now `include!`s the generated `common_grpc.rs` alongside the existing `common_generated.rs`. (`170f9e3`, 2026-04-30)
- [x] **Phase 5b** — caller + worker handler bodies (11 RPCs); long-poll machinery with `WaiterPool` (single-waiter wake, per-replica cap); `cancel_internal` shared helper; transparent `40001` retry helper. `TaskQueueServer` + `TaskWorkerServer` registered in Router. 50 unit tests. (`05fc260`, 2026-05-01)
- [x] **Phase 5c** — admin handler bodies (8 RPCs); both reapers (timeout + dead-worker) spawned from `main.rs`; `NamespaceConfigCache` singleflight + jittered TTL; `audit_log_write` helper; storage trait growth (`cancel_task`, `get_task_by_id`, `list_dead_worker_runtimes`, `count_tasks_by_status`, `list_workers`, `enable_namespace`, `disable_namespace`); migration `0002_phase5c_admin.sql` adding `namespace_quota.disabled` + `task_type_registry`; SCHEMA_VERSION → 2. 67 unit tests. (`d3b2e3c`, 2026-05-01)
- [x] **Phase 5d** — full observability emit sites for all 22 metrics; OTLP gRPC exporter wired (tonic coexists with pure-grpc-rs); W3C trace context propagated through reapers and admin actions; `audit_pruner` (1h tick) and `metrics_refresher` (30s tick) background tasks; `delete_audit_logs_before` trait method on both backends. Folded shadow-variable bugfix in `replay_dead_letters_impl` pre-push. 76 unit tests. (`1d1f943`, 2026-05-01)
- [x] **Phase 7-caller** — `taskq-caller-sdk` ergonomic `CallerClient` with auto-generated UUIDv7 keys, BLAKE3 payload hashing, W3C trace propagation, retry on `RESOURCE_EXHAUSTED` honoring Unix-ms `retry_after` with ±20% jitter, configurable retry budget. 27 tests (15 unit + 12 in-process integration). (`64a3944`, 2026-05-01)
- [x] **Phase 7-worker** — `taskq-worker-sdk` `WorkerBuilder` + `TaskHandler` trait + `Worker::run_until_shutdown`; `ErrorClassRegistry` typed newtype validates `ReportFailure`; heartbeat clamps to ε/3 with operator-warning on override; re-register flow on `WORKER_DEREGISTERED` from acquire / report / heartbeat sites. 22 tests (20 unit + 2 doctest). (`0c06891`, 2026-05-01)
- [x] **Phase 8** — CLI admin subcommands (`taskq-cli`): `namespace create|list|enable|disable`, `set-quota`, `purge`, `replay`, `stats`, `list-workers`. Global `--format human|json`, `--endpoint`, `--token`, `--yes`. Destructive-op safeguards via `--confirm-namespace`, `--dry-run`, interactive TTY prompt. 27 tests (10 unit + 17 integration). (`ab41eb7`, 2026-05-01)
- [x] **Phase 9a** — conformance test bodies for all 6 §8.2 requirements; per-backend test harnesses. SQLite: 13 tests always-runnable via in-memory backend. Postgres: 12 tests gated `#[ignore]` with per-test database isolation against `docker.yuacx.com:5432`. Surfaced 4 conformance gaps documented inline (SQLite poll over-delivery, Postgres `delete_expired_dedup` n-param ignored, SSI 40001 retries, per-database isolation). (`a6fc3a9`, 2026-05-02)
- [x] **Phase 9b** — end-to-end integration tests (`taskq-integration-tests`): in-process CP against file-backed SQLite; 22 tests across lifecycle / retry / reaper / idempotency / admin / observability. Surfaced and fixed a real production bug in `try_dispatch` (no-strategy fallback double-picking + losing the row); surfaced 4 production gaps for Phase 9c. Added `serve_in_process` helper and made reaper ticks `pub` for deterministic driving. (`28d4795`, 2026-05-02)
- [x] **Phase 9c** — production-gap fixes: full §6.5 retry-state mapping (`FAILED_EXHAUSTED`, retry-past-TTL → `EXPIRED`); submit-time lazy dedup cleanup; `disable_namespace` runtime enforcement; `MaxPending` default chain when strategy registry is empty; `Dispatcher::pick_ordering()` to centralise the lock call. 208 workspace tests passing. (`f3a3b88`, 2026-05-02)
- [x] **WAITING_RETRY progression bugs** — `complete_task` now bumps `tasks.attempt_number` on the WAITING_RETRY transition (both backends), so the next dispatch reads the new value and the follow-up `INSERT INTO task_results` no longer collides on PK(task_id, attempt_number). The Postgres dispatcher's `pick_and_lock_pending` filter widened to `status IN ('PENDING', 'WAITING_RETRY')` to match SQLite. Removed the masked Reaper-A latency cost (~lease window per retry) and unstuck Postgres retryable failures that previously sat in WAITING_RETRY indefinitely. New `retry_progression` conformance module locks the contract for both backends; lifecycle suite went 7.15s → 2.63s. (2026-05-05)
