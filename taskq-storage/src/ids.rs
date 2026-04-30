//! Domain newtypes used across the storage trait surface.
//!
//! Strings (`Namespace`, `TaskType`, `IdempotencyKey`) wrap an owned `String`
//! for v1 simplicity. v1's submit target is ~1K/sec/replica per
//! `design.md` §9.1; the per-call `String` allocation is rounding error
//! against the SERIALIZABLE transaction's own cost. We can revisit
//! `Arc<str>` when benchmarks justify it (v1.1 candidate).
//!
//! IDs (`TaskId`, `WorkerId`) wrap UUID v7 — time-ordered, no coordination,
//! per `design.md` §4 (`task_id : ULID/UUIDv7`).
//!
//! `Timestamp` is a thin wrapper over Unix-epoch milliseconds (UTC). The CP
//! layer uses one canonical clock; backends translate to native types
//! (Postgres `timestamptz`, SQLite integer, etc.).

use std::fmt;

use uuid::Uuid;

macro_rules! string_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            /// Wrap an owned `String`.
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }

            /// Borrow the inner string.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consume the wrapper and return the inner `String`.
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_owned())
            }
        }
    };
}

string_newtype! {
    /// Tenant identity. Every task carries one. Quotas, metrics, idempotency,
    /// and observability are namespace-scoped (`design.md` §2).
    Namespace
}

string_newtype! {
    /// Task type identifier (e.g. `llm.flash`). Workers register types they
    /// handle; dispatch filters by type (`design.md` §2).
    TaskType
}

string_newtype! {
    /// Caller-supplied idempotency key. Scoped `(namespace, key)`.
    /// Required on every `SubmitTask` (`design.md` §2, §6.1).
    IdempotencyKey
}

macro_rules! uuid_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
        pub struct $name(Uuid);

        impl $name {
            /// Generate a new time-ordered UUID v7.
            pub fn generate() -> Self {
                Self(Uuid::now_v7())
            }

            /// Wrap an existing UUID. Caller is responsible for v7-ness if
            /// time ordering matters.
            pub fn from_uuid(u: Uuid) -> Self {
                Self(u)
            }

            /// Borrow the inner UUID.
            pub fn as_uuid(&self) -> &Uuid {
                &self.0
            }

            /// Consume the wrapper and return the inner UUID.
            pub fn into_uuid(self) -> Uuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl From<Uuid> for $name {
            fn from(u: Uuid) -> Self {
                Self(u)
            }
        }
    };
}

uuid_newtype! {
    /// Identifier for a task. Time-ordered UUID v7 per `design.md` §4.
    TaskId
}

uuid_newtype! {
    /// Identifier for a worker process. UUID v7 per `design.md` §4.
    WorkerId
}

/// Wall-clock timestamp expressed as Unix epoch milliseconds (UTC).
///
/// The storage layer uses milliseconds to match `design.md` §10.1's
/// `retry_after: i64 // Unix epoch milliseconds (UTC)`. Backends translate
/// to native time types as needed.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Construct from raw Unix-epoch milliseconds.
    pub fn from_unix_millis(ms: i64) -> Self {
        Self(ms)
    }

    /// Borrow the inner Unix-epoch millisecond value.
    pub fn as_unix_millis(&self) -> i64 {
        self.0
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ms", self.0)
    }
}
