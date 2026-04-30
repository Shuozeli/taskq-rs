//! Conformance requirement #3: indexed range scans with predicates.
//!
//! `design.md` §8.2 #3: "Efficient filtering by `(namespace, task_type,
//! status, priority)`. Backends without secondary indexes must provide
//! equivalent (e.g., explicit secondary subspaces in FoundationDB)."
//!
//! Postgres satisfies this via the composite index `(namespace, status,
//! priority DESC, submitted_at ASC)` on `tasks` (`design.md` §8.3); the
//! conformance test exercises the operations that index serves rather
//! than asserting on `EXPLAIN` output (which would couple the test to a
//! specific backend's planner).

/// `pick_and_lock_pending` with `PriorityFifo` ordering returns the highest-
/// priority oldest PENDING task whose `task_type` is in the worker's filter
/// set, restricted to the requested namespace.
///
/// The Phase 9 implementation will:
///   - Seed PENDING tasks across two namespaces and three task_types with
///     varying priorities.
///   - Pick with namespace_filter = Single(ns_a), task_types = [t1, t2],
///     ordering = PriorityFifo.
///   - Assert the locked task has the highest priority among (ns_a, t1|t2)
///     PENDING rows; ties broken by `submitted_at ASC`.
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_priority_fifo_picks_highest_priority_oldest_first() {
    // Arrange — seed mixed (namespace, task_type, priority) PENDING tasks.
    // Act       — call pick_and_lock_pending(PriorityFifo, ns_a, [t1, t2]).
    // Assert    — locked task matches the expected (priority, submitted_at) pair.
    todo!("Phase 9: needs a concrete Storage impl");
}

/// Tasks outside the requested namespace MUST NOT be returned, even when
/// they would otherwise rank first by the ordering. This is the "scoping
/// is honored" smoke test for the dispatch query.
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_dispatch_query_honors_namespace_filter() {
    // Arrange — high-priority PENDING task in ns_b, low-priority in ns_a.
    // Act       — pick with namespace_filter = Single(ns_a).
    // Assert    — the returned task is from ns_a, not ns_b.
    todo!("Phase 9: needs a concrete Storage impl");
}

/// Tasks whose `task_type` is not in the worker's filter MUST NOT be
/// returned. This is the symmetric "task_type filter is honored" check.
#[tokio::test]
#[ignore = "implemented in Phase 9 once backends exist"]
async fn test_dispatch_query_honors_task_type_filter() {
    // Arrange — PENDING tasks of types t1, t2, t3 in one namespace.
    // Act       — pick with task_types = [t1] only.
    // Assert    — the returned task has task_type = t1.
    todo!("Phase 9: needs a concrete Storage impl");
}
