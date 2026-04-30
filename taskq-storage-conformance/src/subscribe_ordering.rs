//! Conformance requirement #5: subscribe-pending ordering invariant.
//!
//! `design.md` §8.2 #5: "Any transaction committing a row matching the
//! subscription predicate strictly after `subscribe_pending` returns MUST
//! cause at least one `WakeSignal` to be observable on the returned
//! stream. Backends that cannot deliver true ordering (e.g. poll-only
//! backends) satisfy this via short polling intervals plus the 10s
//! belt-and-suspenders fallback in the CP."
//!
//! This is the load-bearing property that prevents the long-poll path from
//! losing wakeups in `design.md` §6.2 step 4. See `problems/07` for the
//! original discussion.

/// The basic ordering check: subscribe, then commit a matching row. The
/// stream MUST yield at least one `WakeSignal`.
///
/// The Phase 9 implementation will:
///   - Open subscription for (namespace, [task_type]).
///   - In a separate transaction, insert + commit a PENDING task in that
///     namespace with that task_type.
///   - Assert the subscription stream yields ≥ 1 `WakeSignal` within a
///     bounded wait (e.g. 1s for notify-driven backends, 600ms for
///     poll-driven — accommodating the 500ms default poll interval per
///     `design.md` §8.4).
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_commit_after_subscribe_produces_wake_signal() {
    // Arrange — subscribe to (ns, [task_type]).
    // Act       — commit a matching PENDING task in another transaction.
    // Assert    — at least one WakeSignal observed within the bounded wait.
    todo!("Phase 9: needs a concrete Storage impl");
}

/// A commit of a NON-matching row (different namespace OR task_type not in
/// the filter) MUST NOT (be required to) wake the subscription. Backends
/// MAY over-deliver wakeups, but the test asserts the *positive* form is
/// only required for matches; the suite leaves a permissive "we got an
/// extra wake; that's fine" assertion since the CP layer's contract is
/// "at least one signal" not "exactly one signal".
///
/// In practice this test exists to catch backends that DROP wakeups for
/// matching commits while delivering them for non-matching ones — the
/// unambiguous bug.
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_matching_commit_always_wakes_even_if_others_also_wake() {
    // Arrange — subscribe to (ns_a, [t1]).
    // Act       — commit (ns_b, t1) and then (ns_a, t1) in that order.
    // Assert    — at least one WakeSignal observed AFTER the (ns_a, t1) commit.
    todo!("Phase 9: needs a concrete Storage impl");
}
