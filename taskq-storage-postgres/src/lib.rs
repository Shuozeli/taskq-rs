//! taskq-storage-postgres: Postgres backend implementation of the taskq-rs
//! `Storage` / `StorageTx` trait pair.
//!
//! ## What lives here
//!
//! - [`PostgresStorage`] — the [`taskq_storage::Storage`] implementation. Owns
//!   a `deadpool_postgres::Pool` plus the long-lived `LISTEN` connection that
//!   demuxes notifications to per-waiter channels.
//! - [`PostgresTx`] — a single SERIALIZABLE transaction. Implements
//!   [`taskq_storage::StorageTx`].
//! - [`mod@migrate`] — applies the bundled migration SQL files in lexical order.
//! - [`PostgresConfig`] — minimal connection/pool wiring options.
//!
//! ## Isolation
//!
//! Per `design.md` §1.1 the default path is SERIALIZABLE; only
//! `record_worker_heartbeat` is carved out to READ COMMITTED. The
//! `record_worker_heartbeat` call therefore runs on a side connection from
//! the same pool rather than inside the transaction the caller opened — this
//! keeps the heartbeat path off the SERIALIZABLE retry stack.
//!
//! ## Rate quotas
//!
//! Per the §1.1 carve-out, rate quotas are eventually consistent within the
//! cache TTL. v1 implements this as a per-replica in-memory token bucket
//! map; see [`rate`].

#![forbid(unsafe_code)]

pub mod errors;
pub mod listener;
pub mod migrate;
pub mod rate;
pub mod storage;
pub mod tx;

pub use crate::errors::map_db_error;
pub use crate::migrate::{migrate, migrate_with_pool};
pub use crate::storage::{PostgresConfig, PostgresStorage};
pub use crate::tx::PostgresTx;
