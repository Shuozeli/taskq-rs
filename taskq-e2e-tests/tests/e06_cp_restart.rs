//! E2E CP restart preserves committed state.
//!
//! Submit a task (committed to Postgres). Stop+restart the CP
//! container. Bring up a new worker. Assert the worker dispatches and
//! completes the task — i.e. CP restart didn't lose any state, and a
//! fresh worker connecting after the restart picks up where the
//! pre-restart system left off.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use taskq_caller_sdk::{SubmitOutcome, SubmitRequest};
use taskq_e2e_tests::{require_docker, wait_for, ComposeFile, ComposeStack};
use taskq_proto::TerminalState;
use taskq_worker_sdk::{AcquiredTask, HandlerOutcome, TaskHandler, WorkerBuilder};

const NS: &str = "e2e_restart";

struct EchoHandler;
impl TaskHandler for EchoHandler {
    async fn handle(&self, task: AcquiredTask) -> HandlerOutcome {
        HandlerOutcome::Success(task.payload)
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn cp_restart_preserves_pending_task() -> Result<()> {
    // Arrange
    require_docker()?;
    let stack = ComposeStack::up(ComposeFile::Base)?;
    stack.seed_namespace(NS).await?;
    let endpoint = stack.primary_endpoint();

    // Submit BEFORE bringing up any worker so the row sits in PENDING
    // across the restart.
    let mut caller = stack.caller_at(endpoint.clone()).await?;
    let outcome = caller
        .submit(SubmitRequest::new(
            NS,
            "echo",
            Bytes::from_static(b"survive"),
        ))
        .await
        .map_err(|e| anyhow!("submit: {e}"))?;
    let task_id = match outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => return Err(anyhow!("expected Created, got {other:?}")),
    };
    drop(caller);

    // Act: restart the CP container. The Postgres volume is untouched.
    stack.stop_service("cp")?;
    stack.start_service("cp")?;

    // Assert: a fresh worker + fresh caller after restart drains the
    // pre-restart task to COMPLETED.
    let worker = WorkerBuilder::new(endpoint.clone(), NS)
        .with_task_types(vec!["echo".to_owned()])
        .with_concurrency(1)
        .with_long_poll_timeout(Duration::from_secs(2))
        .with_handler(EchoHandler)
        .build()
        .await
        .map_err(|e| anyhow!("worker build: {e}"))?;
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run_until_shutdown().await;
    });

    let caller2 = stack.caller_at(endpoint).await?;
    let final_state = wait_for(Duration::from_secs(20), Duration::from_millis(300), || {
        let task_id = Arc::new(task_id.clone());
        let mut caller = caller2.clone();
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
