<!-- agent-updated: 2026-04-30T04:30:00Z -->

# taskq-storage-sqlite

SQLite implementation of the `taskq-storage` `Storage` and `StorageTx`
traits.

> **Embedded / dev only.** Per `design.md` §8.3, SQLite is single-process
> embedded scope. Multi-process or multi-replica control-plane deployments
> on SQLite are NOT supported. The backend ships to validate the trait
> shape against a second implementation and to enable embedded testing.

## Overview

- **Driver:** [`rusqlite`] with the `bundled` feature so the build does
  not depend on a system SQLite version.
- **Concurrency:** single connection behind an async `Mutex`.
  `begin()` issues `BEGIN IMMEDIATE` to acquire the writer lock up front,
  avoiding `SQLITE_BUSY` surprises.
- **Schema:** see `migrations/0001_initial.sql`. UUIDs as `TEXT`,
  timestamps as `INTEGER` Unix epoch milliseconds, JSON as `TEXT`,
  enums as `TEXT` with `CHECK` constraints.
- **subscribe_pending:** 500 ms polling. SQLite has no LISTEN/NOTIFY
  equivalent; the CP layer's 10 s belt-and-suspenders timer covers any
  missed wakeup.
- **Skip-locking:** §8.2 #2 satisfied vacuously. There is one writer.
- **Bounded-cost dedup expiration:** §8.2 #6 honored via
  `DELETE ... WHERE rowid IN (SELECT rowid ... LIMIT n)` (SQLite's
  `LIMIT` on `DELETE` is a build-time feature flag).

## Running tests

```bash
cargo test -p taskq-storage-sqlite
```

The smoke tests use in-memory SQLite databases and require no external
infrastructure.

## Trade-offs

- **No BRIN equivalent on `worker_heartbeats(last_heartbeat_at)`.** SQLite
  has only B-tree indexes; the index churn is acceptable at single-process
  scale.
- **No `idempotency_keys` partitioning.** SQLite lacks declarative
  partitioning; cleanup is a plain row-bounded `DELETE`.

[`rusqlite`]: https://crates.io/crates/rusqlite
