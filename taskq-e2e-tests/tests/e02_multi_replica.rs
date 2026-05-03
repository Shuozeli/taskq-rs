//! E2E multi-replica LISTEN/NOTIFY fan-out.
//!
//! Worker connects to `cp-a`'s long-poll. Caller submits via `cp-b`.
//! Postgres `NOTIFY taskq_pending_<ns>` fires; cp-a's listener
//! demuxes to the worker's waiter pool entry; the task gets handled
//! within ε seconds. Exercises the cross-process LISTEN path that the
//! in-process `TestHarness` cannot reach.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use taskq_caller_sdk::{SubmitOutcome, SubmitRequest};
use taskq_e2e_tests::{require_docker, wait_for, ComposeFile, ComposeStack};
use taskq_proto::TerminalState;
use taskq_worker_sdk::{AcquiredTask, HandlerOutcome, TaskHandler, WorkerBuilder};

const NS: &str = "e2e_fanout";

struct EchoHandler;
impl TaskHandler for EchoHandler {
    async fn handle(&self, task: AcquiredTask) -> HandlerOutcome {
        HandlerOutcome::Success(task.payload)
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn submit_via_b_wakes_worker_on_a() -> Result<()> {
    // Arrange
    require_docker()?;
    let stack = ComposeStack::up(ComposeFile::MultiCp)?;
    stack.seed_namespace(NS).await?;
    let endpoint_a = stack.primary_endpoint();
    let endpoint_b = stack
        .secondary_endpoint()
        .ok_or_else(|| anyhow!("multi-cp stack must expose secondary endpoint"))?;

    let worker = WorkerBuilder::new(endpoint_a.clone(), NS)
        .with_task_types(vec!["echo".to_owned()])
        .with_concurrency(1)
        .with_long_poll_timeout(Duration::from_secs(3))
        .with_handler(EchoHandler)
        .build()
        .await
        .map_err(|e| anyhow!("worker build: {e}"))?;
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run_until_shutdown().await;
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut caller = stack.caller_at(endpoint_b).await?;

    // Act: submit via replica B; replica A's worker must wake.
    let outcome = caller
        .submit(SubmitRequest::new(NS, "echo", Bytes::from_static(b"x")))
        .await
        .map_err(|e| anyhow!("submit: {e}"))?;
    let task_id = match outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => return Err(anyhow!("expected Created, got {other:?}")),
    };

    // Assert
    let final_state = wait_for(Duration::from_secs(15), Duration::from_millis(200), || {
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

    worker_handle.abort();
    Ok(())
}
