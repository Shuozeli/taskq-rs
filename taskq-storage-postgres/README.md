<!-- agent-updated: 2026-04-30T18:00:00Z -->

# taskq-storage-postgres

Postgres backend for the taskq-rs `Storage` / `StorageTx` trait pair
(`design.md` §8.3). Implements every requirement from `design.md` §8.2,
including range-partitioned `idempotency_keys`, BRIN-indexed
`worker_heartbeats`, and `LISTEN/NOTIFY`-driven pending notifications.

## Prerequisites

- Postgres 14+ (declarative range partitioning, BRIN, JSONB).
- `SERIALIZABLE` isolation supported (mandatory for state-transition
  transactions per `design.md` §1.1).
- A user with `CREATE TABLE`, `CREATE TYPE`, `CREATE FUNCTION`, and
  partition-management privileges on the target database.

## Quick start

```rust
use taskq_storage_postgres::{migrate, PostgresConfig, PostgresStorage};

let storage = PostgresStorage::connect(PostgresConfig::new(
    "host=localhost user=app dbname=taskq",
)).await?;
migrate(storage.pool()).await?;
```

`migrate` is idempotent — calling it on an already-migrated database is a
no-op. Production deployments should run `taskq-cp migrate` (Phase 8) rather
than running migrations from the server process.

## Running smoke tests

The smoke tests (`tests/smoke.rs`) require a reachable Postgres. Per the
project's infra defaults they default to `docker.yuacx.com:5432`,
user/password `cyuan`/`cyuan`, database `taskq_test_phase3` (created on
demand). Override via env:

```bash
export TASKQ_PG_TEST_URL="host=localhost user=app password=app dbname=taskq_test_phase3"
cargo test -p taskq-storage-postgres -- --include-ignored
```

Without that env var (and when the default host is unreachable), every
smoke test is `#[ignore]`d with a clear message naming the missing
dependency.

## What this crate does NOT do

- No CP-side retry on `SerializationConflict` — that lives in the
  control-plane layer (`design.md` §6.4). This crate surfaces
  `StorageError::SerializationConflict` and lets the caller decide.
- No conformance test bodies — those land in Phase 9 inside
  `taskq-storage-conformance`.
