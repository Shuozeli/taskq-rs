//! Conformance requirement #3: indexed range scans with predicates.
//!
//! `design.md` §8.2 #3: "Efficient filtering by `(namespace, task_type,
//! status, priority)`. Backends without secondary indexes must provide
//! equivalent (e.g., explicit secondary subspaces in FoundationDB)."
//!
//! Postgres satisfies this via the composite index `(namespace, status,
//! priority DESC, submitted_at ASC)` on `tasks` (`design.md` §8.3). The
//! tests below exercise the operations the index serves rather than
//! asserting on `EXPLAIN` output (which would couple the test to a
//! specific backend's planner).

use taskq_storage::ids::{Namespace, TaskId, TaskType, Timestamp};
use taskq_storage::types::{NamespaceFilter, PickCriteria, PickOrdering, TaskTypeFilter};
use taskq_storage::{Storage, StorageTx};

use crate::helpers::{make_task, T0_MS};

/// `pick_and_lock_pending` MUST honor the namespace + task_type filters.
/// Tasks outside the worker's filter set must not be returned even when
/// they would otherwise rank first.
pub async fn dispatch_query_filters_by_namespace_status_priority<S: Storage>(storage: &S) {
    // Arrange — seed three tasks:
    //   ns_a + t_target  + priority 1   (eligible)
    //   ns_b + t_target  + priority 100 (different namespace; excluded)
    //   ns_a + t_other   + priority 100 (different task_type; excluded)
    let ns_a = "rs-ns-a";
    let ns_b = "rs-ns-b";
    let t_target = "rs.target";
    let t_other = "rs.other";

    let id_a = TaskId::generate();
    let id_b = TaskId::generate();
    let id_other = TaskId::generate();

    let mut tx = storage.begin().await.expect("begin seed");
    let (mut ta, da) = make_task(id_a, ns_a, t_target, "k-a", [1u8; 32]);
    ta.priority = 1;
    tx.insert_task(ta, da).await.expect("insert a");
    let (mut tb, db) = make_task(id_b, ns_b, t_target, "k-b", [2u8; 32]);
    tb.priority = 100;
    tx.insert_task(tb, db).await.expect("insert b");
    let (mut to, do_) = make_task(id_other, ns_a, t_other, "k-o", [3u8; 32]);
    to.priority = 100;
    tx.insert_task(to, do_).await.expect("insert other");
    tx.commit().await.expect("commit seed");

    // Act — pick scoped to (ns_a, [t_target]).
    let mut tx = storage.begin().await.expect("begin pick");
    let locked = tx
        .pick_and_lock_pending(PickCriteria {
            namespace_filter: NamespaceFilter::Single(Namespace::new(ns_a)),
            task_types_filter: TaskTypeFilter::AnyOf(vec![TaskType::new(t_target)]),
            ordering: PickOrdering::PriorityFifo,
            now: Timestamp::from_unix_millis(T0_MS + 1),
        })
        .await
        .expect("pick");
    tx.commit().await.expect("commit pick");

    // Assert — the only eligible row is (ns_a, t_target). Despite higher
    // priority elsewhere, namespace and type filters constrain the pick.
    let locked = locked.expect("an eligible task exists");
    assert_eq!(locked.task_id, id_a);
    assert_eq!(locked.namespace.as_str(), ns_a);
    assert_eq!(locked.task_type.as_str(), t_target);
}

/// `PriorityFifo` ordering MUST return the highest-priority task first;
/// ties broken by `submitted_at ASC`.
pub async fn pick_ordering_priority_fifo_orders_by_priority_desc_submitted_at_asc<S: Storage>(
    storage: &S,
) {
    // Arrange — four tasks with mixed priorities and submit times. The
    // expected pick order is: highest priority first; within the highest
    // priority bucket, oldest submitted_at first.
    let ns = "rs-pf";
    let tt = "rs.pf";

    let id_high_old = TaskId::generate(); // priority 10, submitted_at T0 + 100
    let id_high_new = TaskId::generate(); // priority 10, submitted_at T0 + 500
    let id_low_old = TaskId::generate(); // priority 1, submitted_at T0 + 50
    let id_low_new = TaskId::generate(); // priority 1, submitted_at T0 + 1000

    let mut tx = storage.begin().await.expect("begin seed");
    for (id, prio, submitted_offset, key, hash) in [
        (id_low_old, 1, 50i64, "k-lo", [10u8; 32]),
        (id_high_new, 10, 500, "k-hn", [11u8; 32]),
        (id_high_old, 10, 100, "k-ho", [12u8; 32]),
        (id_low_new, 1, 1_000, "k-ln", [13u8; 32]),
    ] {
        let (mut t, d) = make_task(id, ns, tt, key, hash);
        t.priority = prio;
        t.submitted_at = Timestamp::from_unix_millis(T0_MS + submitted_offset);
        tx.insert_task(t, d).await.expect("insert");
    }
    tx.commit().await.expect("commit seed");

    // Act — pick once. The first pick must be the highest-priority oldest
    // task: id_high_old.
    let mut tx = storage.begin().await.expect("begin pick");
    let locked = tx
        .pick_and_lock_pending(PickCriteria {
            namespace_filter: NamespaceFilter::Single(Namespace::new(ns)),
            task_types_filter: TaskTypeFilter::AnyOf(vec![TaskType::new(tt)]),
            ordering: PickOrdering::PriorityFifo,
            now: Timestamp::from_unix_millis(T0_MS + 5_000),
        })
        .await
        .expect("pick")
        .expect("at least one eligible task");
    tx.commit().await.expect("commit pick");

    // Assert — first pick is the oldest of the high-priority bucket.
    assert_eq!(locked.task_id, id_high_old);
    assert_eq!(locked.priority, 10);
}

/// `AgePromoted` ordering MUST bubble older tasks ahead of newer ones with
/// equal base priority once `(now - submitted_at) * age_weight` outranks
/// the priority delta. The test seeds tasks with the same base priority
/// but very different ages so any positive `age_weight` orders the older
/// task first.
pub async fn pick_ordering_age_promoted_bubbles_old_tasks<S: Storage>(storage: &S) {
    // Arrange — two tasks at the same priority. `id_old` is much older
    // (submitted long before `id_new`); with a positive age_weight the
    // effective priority of `id_old` exceeds that of `id_new`.
    let ns = "rs-age";
    let tt = "rs.age";
    let id_old = TaskId::generate();
    let id_new = TaskId::generate();

    let mut tx = storage.begin().await.expect("begin seed");
    let (mut t_old, d_old) = make_task(id_old, ns, tt, "k-old", [20u8; 32]);
    t_old.priority = 5;
    t_old.submitted_at = Timestamp::from_unix_millis(T0_MS - 1_000_000);
    tx.insert_task(t_old, d_old).await.expect("insert old");

    let (mut t_new, d_new) = make_task(id_new, ns, tt, "k-new", [21u8; 32]);
    t_new.priority = 5;
    t_new.submitted_at = Timestamp::from_unix_millis(T0_MS);
    tx.insert_task(t_new, d_new).await.expect("insert new");
    tx.commit().await.expect("commit seed");

    // Act — pick with AgePromoted ordering. The age weight is positive,
    // and `id_old` is older by 1_000s of ms, so its effective priority
    // beats id_new's.
    let mut tx = storage.begin().await.expect("begin pick");
    let locked = tx
        .pick_and_lock_pending(PickCriteria {
            namespace_filter: NamespaceFilter::Single(Namespace::new(ns)),
            task_types_filter: TaskTypeFilter::AnyOf(vec![TaskType::new(tt)]),
            ordering: PickOrdering::AgePromoted { age_weight: 0.001 },
            now: Timestamp::from_unix_millis(T0_MS + 1),
        })
        .await
        .expect("pick")
        .expect("at least one eligible task");
    tx.commit().await.expect("commit pick");

    // Assert — older task is picked first.
    assert_eq!(locked.task_id, id_old);
}
