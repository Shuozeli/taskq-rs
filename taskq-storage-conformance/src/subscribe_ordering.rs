//! Conformance requirement #5: subscribe-pending ordering invariant.
//!
//! `design.md` §8.2 #5: "Any transaction committing a row matching the
//! subscription predicate strictly after `subscribe_pending` returns MUST
//! cause at least one `WakeSignal` to be observable on the returned
//! stream. Backends that cannot deliver true ordering (e.g. poll-only
//! backends) satisfy this via short polling intervals plus the 10s
//! belt-and-suspenders fallback in the CP."
//!
//! This is the load-bearing property that prevents the long-poll path
//! from losing wakeups in `design.md` §6.2 step 4. See `problems/07` for
//! the original discussion.
//!
//! ## Backend semantics
//!
//! - Postgres delivers true commit-time signals via `LISTEN`/`NOTIFY`
//!   (`design.md` §8.4): a wake fires only on a matching commit.
//! - SQLite uses 500ms polling (no LISTEN equivalent): the stream emits
//!   a `WakeSignal` on every tick. This still satisfies the
//!   "post-subscribe commit yields a wake" property — the next tick is
//!   the wake — but note that polling-driven backends will *also* wake
//!   without a matching commit. The negative-direction test below caps
//!   the wait at 100 ms, which is shorter than the 500 ms tick, so the
//!   poller has not fired yet either way.

use std::time::Duration;

use futures_core::Stream;
use futures_util::stream::StreamExt;
use taskq_storage::ids::{Namespace, TaskId, TaskType};
use taskq_storage::types::WakeSignal;
use taskq_storage::{Storage, StorageTx};
use tokio::time::timeout;

use crate::helpers::make_task;

/// Open a subscription on a fresh transaction, then drop the transaction
/// (commit) so SQLite's writer lock is released and another transaction
/// can run. Returns the subscription stream.
async fn open_subscription<S: Storage>(
    storage: &S,
    namespace: &Namespace,
    task_types: &[TaskType],
) -> Box<dyn Stream<Item = WakeSignal> + Send + Unpin + 'static> {
    let mut tx = storage.begin().await.expect("begin subscribe");
    let stream = tx
        .subscribe_pending(namespace, task_types)
        .await
        .expect("subscribe_pending");
    tx.commit().await.expect("commit subscribe tx");
    stream
}

/// Subscribing first and then committing a matching task MUST surface at
/// least one `WakeSignal` on the stream.
pub async fn subscribe_pending_observes_post_subscription_commit<S: Storage>(storage: &S) {
    // Arrange — subscribe to (ns, [tt]).
    let namespace = Namespace::new("so-after-subscribe");
    let task_type = TaskType::new("so.task");
    let mut stream = open_subscription(storage, &namespace, std::slice::from_ref(&task_type)).await;

    // Act — commit a matching task in a separate transaction. Then poll
    // the stream for at most 2 seconds (accommodates Postgres NOTIFY
    // latency and SQLite's 500 ms polling cadence).
    let task_id = TaskId::generate();
    let (task, dedup) = make_task(
        task_id,
        namespace.as_str(),
        task_type.as_str(),
        "so-key-after",
        [0x77u8; 32],
    );
    let mut tx = storage.begin().await.expect("begin insert");
    tx.insert_task(task, dedup).await.expect("insert");
    tx.commit().await.expect("commit insert");

    let signal = timeout(Duration::from_secs(2), stream.next()).await;

    // Assert — within the bounded wait we observe at least one wake.
    match signal {
        Ok(Some(WakeSignal)) => {}
        Ok(None) => panic!("subscription stream ended unexpectedly"),
        Err(_) => panic!("no WakeSignal observed within 2 s of post-subscribe commit"),
    }
}

/// Subscribing AFTER a matching commit MUST NOT (be required to) wake
/// the stream within a tight window. The §8.2 #5 contract is one-way:
/// only post-subscribe commits are required to fire. This test asserts
/// that the negative direction is honored within a 100 ms window — short
/// enough that even SQLite's 500 ms polling stream has not fired its
/// first tick yet.
pub async fn subscribe_pending_does_not_observe_pre_subscription_commit<S: Storage>(storage: &S) {
    // Arrange — commit a matching task FIRST, then subscribe.
    let namespace = Namespace::new("so-before-subscribe");
    let task_type = TaskType::new("so.task");
    let task_id = TaskId::generate();
    let (task, dedup) = make_task(
        task_id,
        namespace.as_str(),
        task_type.as_str(),
        "so-key-before",
        [0x88u8; 32],
    );
    let mut tx = storage.begin().await.expect("begin insert");
    tx.insert_task(task, dedup).await.expect("insert");
    tx.commit().await.expect("commit insert");

    let mut stream = open_subscription(storage, &namespace, &[task_type]).await;

    // Act — wait briefly for any wake on the freshly opened subscription.
    let signal = timeout(Duration::from_millis(100), stream.next()).await;

    // Assert — within 100 ms NO wake fires (the pre-subscribe commit is
    // not observable). Poll-driven backends with > 100 ms tick periods
    // satisfy this trivially; commit-driven backends satisfy it because
    // the commit happened before the LISTEN was issued.
    match signal {
        Err(_) => {}
        Ok(Some(WakeSignal)) => {
            panic!("pre-subscribe commit must not produce a wake within 100 ms")
        }
        Ok(None) => panic!("subscription stream ended unexpectedly"),
    }
}
