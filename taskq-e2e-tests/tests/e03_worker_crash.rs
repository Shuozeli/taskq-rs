//! E2E worker crash → reaper-B reclaim.
//!
//! Worker A acquires a task and "crashes" (we abort its tokio task so
//! it stops heartbeating). After the lease window plus the reaper
//! tick, reaper-B should mark worker A dead and re-dispatch the task.
//! Worker B then acquires it and completes. Exercises the dead-worker
//! reaper on real wall-clock timing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use taskq_caller_sdk::{SubmitOutcome, SubmitRequest};
use taskq_e2e_tests::{require_docker, wait_for, ComposeFile, ComposeStack};
use taskq_proto::TerminalState;
use taskq_worker_sdk::{AcquiredTask, HandlerOutcome, TaskHandler, WorkerBuilder};

const NS: &str = "e2e_crash";

/// Sleep for ages so the lease lapses before the handler returns. The
/// CP will reclaim before Worker A gets a chance to call CompleteTask.
struct StuckHandler {
    seen: Arc<AtomicUsize>,
}

impl TaskHandler for StuckHandler {
    async fn handle(&self, task: AcquiredTask) -> HandlerOutcome {
        self.seen.fetch_add(1, Ordering::SeqCst);
        // Sleep beyond the lease window. Worker B picks up the
        // reclaimed task and returns Success.
        tokio::time::sleep(Duration::from_secs(60)).await;
        HandlerOutcome::Success(task.payload)
    }
}

struct EchoHandler;
impl TaskHandler for EchoHandler {
    async fn handle(&self, task: AcquiredTask) -> HandlerOutcome {
        HandlerOutcome::Success(task.payload)
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn dead_worker_lease_gets_reclaimed() -> Result<()> {
    // Arrange
    require_docker()?;
    let stack = ComposeStack::up(ComposeFile::Base)?;
    stack.seed_namespace(NS).await?;
    let endpoint = stack.primary_endpoint();

    let stuck_seen = Arc::new(AtomicUsize::new(0));
    let stuck_seen_clone = Arc::clone(&stuck_seen);
    let worker_a = WorkerBuilder::new(endpoint.clone(), NS)
        .with_task_types(vec!["echo".to_owned()])
        .with_concurrency(1)
        .with_long_poll_timeout(Duration::from_secs(2))
        .with_heartbeat_interval(Duration::from_millis(500))
        .with_handler(StuckHandler {
            seen: stuck_seen_clone,
        })
        .build()
        .await
        .map_err(|e| anyhow!("worker A build: {e}"))?;
    let worker_a_handle = tokio::spawn(async move {
        let _ = worker_a.run_until_shutdown().await;
    });

    // Wait for worker A to register its long-poll waiter before
    // submitting so the wake hits a parked waiter.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut caller = stack.caller().await?;
    let mut submit_req = SubmitRequest::new(NS, "echo", Bytes::from_static(b"crash"));
    // Allow at least one retry so the reaper-reclaimed lease bounces
    // back to WAITING_RETRY/PENDING instead of straight to
    // FAILED_EXHAUSTED.
    submit_req.max_retries = Some(3);
    let outcome = caller
        .submit(submit_req)
        .await
        .map_err(|e| anyhow!("submit: {e}"))?;
    let task_id = match outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => return Err(anyhow!("expected Created, got {other:?}")),
    };

    // Wait until worker A actually picked up the task (so its lease is
    // alive and we know the reaper has something to reclaim).
    wait_for(Duration::from_secs(10), Duration::from_millis(200), || {
        let seen = Arc::clone(&stuck_seen);
        async move {
            if seen.load(Ordering::SeqCst) >= 1 {
                Ok(Some(()))
            } else {
                Ok(None)
            }
        }
    })
    .await?;

    // Act: kill worker A. Heartbeats stop; reaper B will mark it dead
    // after `lease_window_seconds` (cp-config: 2s) plus reaper tick.
    worker_a_handle.abort();

    // Bring up worker B to compete for the reclaimed lease.
    let worker_b = WorkerBuilder::new(endpoint.clone(), NS)
        .with_task_types(vec!["echo".to_owned()])
        .with_concurrency(1)
        .with_long_poll_timeout(Duration::from_secs(2))
        .with_heartbeat_interval(Duration::from_millis(500))
        .with_handler(EchoHandler)
        .build()
        .await
        .map_err(|e| anyhow!("worker B build: {e}"))?;
    let worker_b_handle = tokio::spawn(async move {
        let _ = worker_b.run_until_shutdown().await;
    });

    // Assert: task lands COMPLETED (worker B handles it after reaper-B
    // reclaims worker A's lease).
    let final_state = wait_for(Duration::from_secs(60), Duration::from_millis(500), || {
        let task_id = Arc::new(task_id.clone());
        let mut caller = caller.clone();
        async move {
            let st = caller
                .get_result(&task_id)
                .await
                .map_err(|e| anyhow!("get_result: {e}"))?;
            if st.status == TerminalState::COMPLETED {
                Ok(Some(st))
            } else {
                Ok(None)
            }
        }
    })
    .await?;
    assert_eq!(final_state.status, TerminalState::COMPLETED);

    worker_b_handle.abort();
    Ok(())
}
