<!-- agent-updated: 2026-05-02T21:47:00Z -->

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-05-02

Initial public release. Pure-Rust distributed task queue over a
FlatBuffers / `pure-grpc-rs` wire contract; Postgres backend for
production, SQLite backend for embedded/dev. 208 workspace tests
passing.

### Added

- **Phase 0** scaffolding: Cargo workspace, nine empty crate skeletons
  (`taskq-proto`, `taskq-storage`, `taskq-storage-conformance`,
  `taskq-storage-postgres`, `taskq-storage-sqlite`, `taskq-cp`,
  `taskq-caller-sdk`, `taskq-worker-sdk`, `taskq-cli`), `LICENSE` (MIT),
  `CONTRIBUTING.md`, GitHub Actions CI workflow, pre-commit configuration,
  pinned stable Rust toolchain.
- **Phase 1** wire schema: four `.fbs` files under `taskq.v1` (common,
  task_queue, task_worker, task_admin); FlatBuffers codegen via `grpc-build`
  from `Shuozeli/pure-grpc-rs`; smoke tests pin enum tag values as wire-
  stability regression guards.
- **Phase 2** storage trait pair (`Storage` + `StorageTx`) with native
  `async fn in trait` via `impl Future + Send` desugar; conformance
  harness scaffolding with 14 `#[ignore]`'d test placeholders.
- **Phase 3** Postgres backend: `tokio-postgres` + `deadpool-postgres`,
  SERIALIZABLE state-transition transactions, READ COMMITTED carve-out
  for heartbeats on a separate connection, range-partitioned
  `idempotency_keys` (daily) with `taskq_create_idempotency_partitions_-
  through()` SQL function pre-creating ~96 days of runway, BRIN index on
  `worker_heartbeats(last_heartbeat_at)`, dedicated LISTEN connection
  demuxed via `mpsc` to per-namespace subscribers.
- **Phase 4** SQLite backend (embedded/dev only): `rusqlite` (bundled),
  `Arc<Mutex<Connection>>` single-writer model, WAL journal mode +
  busy_timeout=5000, 500ms polling for `subscribe_pending`.
- **Phase 5a** control-plane scaffold: `pure-grpc-rs` server with version
  handshake interceptor, OTel pipeline (OTLP + Prometheus + stdout
  exporters), axum health endpoints, strategy registry with object-safe
  `dyn Admitter`/`dyn Dispatcher` traits via `async-trait`, graceful
  shutdown, 22 metric instrument declarations.
- **Phase 5b-pre** rewired `taskq-proto` codegen via `grpc-build`; expanded
  `StorageTxDyn` shim from 4 to 16 methods; populated all five strategies
  (`Always`, `MaxPending`, `CoDel`, `PriorityFifo`, `AgePromoted`,
  `RandomNamespace`); added `taskq-cp migrate` subcommand and
  `--auto-migrate` flag with schema-version gate.
- **Phase 5b** caller + worker handler bodies: 11 RPCs (5 caller + 6
  worker); long-poll machinery with single-waiter wake and per-replica
  cap (`WaiterPool`); `cancel_internal` shared helper; transparent
  `40001` retry for `CompleteTask` / `ReportFailure`.
- **Phase 5c** admin handler bodies (8 RPCs); both reapers (timeout +
  dead-worker) with deterministic-tick driving; `NamespaceConfigCache`
  singleflight + jittered TTL; `audit_log_write` helper with
  same-transaction guarantee and 4KB summary truncation; storage trait
  growth (`cancel_task`, `get_task_by_id`, `list_dead_worker_runtimes`,
  `count_tasks_by_status`, `list_workers`, `enable_namespace`,
  `disable_namespace`); migration `0002_phase5c_admin.sql` adding
  `namespace_quota.disabled` column and `task_type_registry` table;
  SCHEMA_VERSION bumped to 2.
- **Phase 5d** full observability emit sites for all 22 metrics; OTLP
  gRPC exporter wired (tonic coexists with `pure-grpc-rs`); W3C trace
  context persisted on task row, propagated through reapers and admin
  spans; `audit_pruner` (1h tick) and `metrics_refresher` (30s tick)
  background tasks; `delete_audit_logs_before` trait method on both
  backends.
- **Phase 5e** storage trait follow-ups: `upsert_namespace_quota`,
  `list_tasks_by_filter`, `list_tasks_by_terminal_status`, `replay_task`,
  `add_error_classes` / `deprecate_error_class`, `add_task_types` /
  `deprecate_task_type`. `SetNamespaceQuota` now writes the row;
  `SetNamespaceConfig` persists registry adds; `PurgeTasks` and
  `ReplayDeadLetters` iterate via rate-limited (100/sec) loops.
- **Phase 7-caller** (`taskq-caller-sdk`): ergonomic `CallerClient` with
  auto-generated UUIDv7 idempotency keys, BLAKE3 payload hashing, W3C
  trace-context propagation from `tracing-opentelemetry`, retry on
  `RESOURCE_EXHAUSTED` honoring `retry_after` Unix-ms timestamp with
  ±20% jitter, configurable retry budget (default 5).
- **Phase 7-worker** (`taskq-worker-sdk`): `WorkerBuilder` + `TaskHandler`
  trait + `Worker::run_until_shutdown`. `ErrorClassRegistry` typed
  newtype validates `ReportFailure` against the registry. Heartbeat
  cadence clamps to ε/3 with operator-warning on override. Re-register
  flow on `WORKER_DEREGISTERED` from acquire / report / heartbeat sites.
- **Phase 8** CLI admin subcommands (`taskq-cli`): `namespace
  create|list|enable|disable`, `set-quota`, `purge`, `replay`, `stats`,
  `list-workers`. Global `--format human|json`, `--endpoint`, `--token`,
  `--yes`. Destructive-op safeguards: required `--confirm-namespace`,
  `--dry-run`, interactive `Proceed? [y/N]` prompt when stdin is a TTY.
- **Phase 9a** conformance test bodies for all 6 §8.2 requirements; per-
  backend test harnesses (`taskq-storage-sqlite/tests/conformance.rs`
  always-runnable; `taskq-storage-postgres/tests/conformance.rs` gated
  `#[ignore]` with per-test database isolation via `CREATE DATABASE
  taskq_phase9a_<uuid>`). 13 SQLite + 12 Postgres tests passing.
- **Phase 9b** end-to-end integration tests (`taskq-integration-tests`):
  `TestHarness::start_in_memory` boots an in-process CP against file-
  backed SQLite. 22 tests across lifecycle / retry / reaper / idempotency
  / admin / observability. Adds `serve_in_process` helper and exposes
  `reaper_a_tick` / `reaper_b_tick` for deterministic testing.
- **Phase 9c** production-gap fixes: full §6.5 retry-state mapping
  (`FAILED_EXHAUSTED`, retry-past-TTL → `EXPIRED`); submit-time lazy
  dedup cleanup (`delete_idempotency_key`); `disable_namespace` runtime
  enforcement (`is_namespace_disabled`); `MaxPending` default chain when
  strategy registry is empty; `Dispatcher::pick_ordering()` to centralise
  the lock call.
- **Phase 10a** codelab walkthroughs (`codelabs/`): `01-build-a-worker-
  in-50-lines.md` (handler + main one-screener), `02-migrating-a-
  postgres-queue-service.md` (dual-write/dual-read/cutover/decommission
  for hand-rolled `SELECT FOR UPDATE SKIP LOCKED` services), `03-
  operating-taskq-rs.md` (SRE deploy + incident playbooks). ~1330 lines
  total.
- **Phase 10b** reference docs and starter Grafana dashboard:
  `GLOSSARY.md` (taskq vocabulary), `OPERATIONS.md` (alerts off the
  standard metric set, oncall playbooks, DLQ replay), `protocol/
  EVOLUTION.md` (SemVer + wire-stability policy), `protocol/QUOTAS.md`
  (capacity vs rate, cardinality caps), `dashboards/grafana-overview.
  json` (8-panel dashboard parameterised on `${DS_PROMETHEUS}`).

### Changed

- **Round-2 design refinement** (pre-implementation): heartbeats carved
  out of SERIALIZABLE to a dedicated `worker_heartbeats` table at READ
  COMMITTED with lazy lease extension on `task_runtime`; capacity-quota
  reads transactional with no cache; SQLite scoped to single-process
  embedded/dev only; cardinality budget caps (`max_error_classes`,
  `max_task_types`) added to `NamespaceQuota`; `idempotency_keys`
  range-partitioned by `expires_at` on Postgres for bounded-cost cleanup.
- **`pure-grpc-rs` upstream extension** (`Shuozeli/pure-grpc-rs` commit
  `58c35d9`): `compile_fbs` now emits FlatBuffers gRPC stubs end-to-end
  (owned wrappers via Object API, `FlatBufferGrpcMessage` impls, server
  + client modules per `rpc_service` block); `taskq-proto` consumes
  these directly.

### Fixed

- **`task_worker.rs::try_dispatch` no-strategy fallback** was double-
  picking and losing the just-flipped row when no strategy was registered
  for a namespace. Phase 9b found and fixed.
- **`replay_dead_letters_impl` shadow-variable bug** introduced during
  Phase 5d/5e merge — the response's `replayed` Vec was reset to empty
  right before serialization. Folded into Phase 5d commit pre-push.
