//! `taskq-storage-sqlite`: SQLite backend for the taskq-rs `Storage` trait.
//!
//! **Embedded / dev only.** Per `design.md` §8.3, SQLite is single-process
//! embedded scope. Multi-process or multi-replica control-plane deployments
//! on SQLite are not supported: SQLite locks at database granularity, not
//! row, and concurrent writers from multiple CP processes block (or return
//! `SQLITE_BUSY`). The backend ships to validate the trait shape against a
//! second implementation and to enable embedded testing — not as a
//! production option.
//!
//! ## Driver choice
//!
//! Built on [`rusqlite`] with the `bundled` feature so the build does not
//! depend on a system SQLite version. rusqlite is synchronous; every call
//! that touches the database is wrapped in [`tokio::task::spawn_blocking`]
//! to satisfy the async `StorageTx` contract.
//!
//! ## Concurrency model
//!
//! A single `Mutex<Connection>` is shared between [`SqliteStorage`] and the
//! transactions it hands out. SQLite's own database-level write lock would
//! make this redundant in theory, but the `Mutex` keeps the rusqlite handle
//! `Send`-safe between blocking calls and prevents opportunistic
//! `SQLITE_BUSY` errors. Per `design.md` §8.2 #2 this satisfies the
//! skip-locking conformance vacuously — there is only one writer.
//!
//! ## Module layout
//!
//! - [`migrate`] — migration runner.
//! - [`storage`] — [`SqliteStorage`] impl wrapping a connection.
//! - [`tx`]      — [`SqliteTx`] impl.
//! - [`convert`] — translation between trait types and SQLite-native rows.
//!
//! ## Conformance carve-outs
//!
//! Single-writer scope means:
//!
//! - §8.2 #2 (skip-locking) satisfied vacuously. No concurrent writers, so
//!   `pick_and_lock_pending` uses plain `SELECT ... LIMIT 1` followed by
//!   `UPDATE ... SET status='DISPATCHED'`.
//! - §8.2 #5 (subscribe-pending ordering) satisfied via 500ms polling per
//!   `design.md` §8.4 — SQLite has no LISTEN/NOTIFY equivalent. The CP
//!   layer's 10s belt-and-suspenders timer covers anything missed.
//! - §8.2 #6 (bounded-cost dedup expiration) satisfied with row-bounded
//!   `DELETE`. SQLite's `LIMIT` on `DELETE` is a build-time feature flag,
//!   so we route through `rowid IN (SELECT ... LIMIT n)` instead.

pub mod convert;
pub mod migrate;
pub mod storage;
pub mod tx;

pub use crate::migrate::{migrate, MIGRATIONS};
pub use crate::storage::SqliteStorage;
pub use crate::tx::SqliteTx;
