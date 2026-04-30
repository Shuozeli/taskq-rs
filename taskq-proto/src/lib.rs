//! taskq-proto: generated FlatBuffers bindings and minor wire helpers for the
//! taskq-rs gRPC contract.
//!
//! The wire schema lives in `schema/*.fbs`; `build.rs` compiles every file
//! through the pure-Rust `flatc-rs` pipeline into a single Rust source file
//! under `OUT_DIR`. The four FlatBuffers files share `namespace taskq.v1;` so
//! every generated type lands in [`v1`].
//!
//! ## Layout
//!
//! - [`v1`] -- all generated readers, builders, and Object API types under the
//!   `taskq.v1` namespace, plus root-type helpers (`root_as_*`).
//! - [`common`] -- thin alias re-export of [`v1`] for code sites that conceptually
//!   speak in terms of `common.fbs` (`Task`, `Lease`, `RetryConfig`, ...).
//! - [`task_queue`] -- alias for code sites speaking the caller-facing service.
//! - [`task_worker`] -- alias for the worker-facing service.
//! - [`task_admin`] -- alias for the operator-facing service.
//!
//! All four aliases point at the same module because FlatBuffers namespaces
//! are flat; the per-file split lives at the schema layer (where it bounds the
//! reviewer's blast radius) rather than in the Rust module tree.
//!
//! ## Schema discipline
//!
//! See `design.md` Sec 12.2 and `problems/09-versioning.md`. In short:
//! append-only fields, never reorder, never change types; field removal uses
//! the FlatBuffers `(deprecated)` attribute and leaves the slot occupied.

// The generated code does not follow Rust idioms (it mirrors C++ flatc output
// for binary compatibility). Suppress the noise here so user code stays clean.
#[allow(
    unused_imports,
    dead_code,
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    unused_variables,
    unused_mut,
    deprecated,
    elided_lifetimes_in_paths,
    mismatched_lifetime_syntaxes,
    clippy::all,
    clippy::pedantic
)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/taskq_v1_generated.rs"));
}

/// Every generated type lives here under `taskq.v1`. Importing as
/// `use taskq_proto::v1::Task;` is the canonical access path.
pub use generated::taskq::v1;

/// Alias for shared types declared in `schema/common.fbs`
/// (`Task`, `Lease`, `RetryConfig`, `Failure`, `RejectReason`, `TerminalState`,
/// `TraceContext`, `SemVer`, `Timestamp`, `IdempotencyKey`, `Rejection`,
/// `RateLimit`, `TaskOutcome`).
pub use generated::taskq::v1 as common;

/// Alias for caller-facing types declared in `schema/task_queue.fbs`
/// (`SubmitTaskRequest`, `SubmitTaskResponse`, ...).
pub use generated::taskq::v1 as task_queue;

/// Alias for worker-facing types declared in `schema/task_worker.fbs`
/// (`RegisterWorkerRequest`, `AcquireTaskRequest`, ...).
pub use generated::taskq::v1 as task_worker;

/// Alias for operator-facing types declared in `schema/task_admin.fbs`
/// (`SetNamespaceQuotaRequest`, `NamespaceQuota`, ...).
pub use generated::taskq::v1 as task_admin;

#[cfg(test)]
mod tests {
    use super::v1;

    /// Smoke-test that core types from each `.fbs` survived codegen and
    /// the per-file aliases in `lib.rs` resolve back to the same module.
    /// Failure here means a renamed type in a schema file or a missing
    /// re-export -- both of which are wire-compat regressions.
    #[test]
    fn generated_types_are_reachable() {
        // Arrange: pick one representative type from each of the four `.fbs` files.

        // Act: name the types via the public API surface. If any are missing,
        // this test fails to compile.
        let _: v1::TerminalState = v1::TerminalState::PENDING;
        let _: v1::TaskOutcome = v1::TaskOutcome::SUCCESS;
        let _: v1::RejectReason = v1::RejectReason::UNSPECIFIED;
        let _: v1::AdmitterKind = v1::AdmitterKind::ALWAYS;
        let _: v1::DispatcherKind = v1::DispatcherKind::PRIORITY_FIFO;
        let _: v1::LogLevel = v1::LogLevel::INFO;

        // Assert: enum tags must match the wire-stable values declared in the
        // schema. Reordering would silently break stored data; this guards it.
        assert_eq!(v1::TerminalState::PENDING.0, 0);
        assert_eq!(v1::TerminalState::COMPLETED.0, 3);
        assert_eq!(v1::TerminalState::EXPIRED.0, 7);
        assert_eq!(
            v1::RejectReason::IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD.0,
            7
        );
        assert_eq!(v1::RejectReason::WORKER_DEREGISTERED.0, 10);
        assert_eq!(v1::RejectReason::LEASE_EXPIRED.0, 12);
    }

    /// Verify the per-file aliases in `lib.rs` agree with `v1`.
    /// They must point at the same module so callers can pick the spelling
    /// that matches their mental model without diverging type identity.
    #[test]
    fn per_file_aliases_agree_with_v1() {
        // Arrange / Act: same type via different aliases.
        let a: v1::TerminalState = v1::TerminalState::PENDING;
        let b: super::common::TerminalState = super::common::TerminalState::PENDING;
        let c: super::task_queue::TerminalState = super::task_queue::TerminalState::PENDING;
        let d: super::task_worker::TerminalState = super::task_worker::TerminalState::PENDING;
        let e: super::task_admin::TerminalState = super::task_admin::TerminalState::PENDING;

        // Assert: equality across aliases is well-typed -- proves they alias.
        assert_eq!(a, b);
        assert_eq!(b, c);
        assert_eq!(c, d);
        assert_eq!(d, e);
    }
}
