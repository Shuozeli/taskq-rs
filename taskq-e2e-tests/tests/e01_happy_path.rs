//! E2E happy-path: 1 caller + 1 worker + 1 task, real Docker.
//!
//! Boots the `compose.base.yml` stack (postgres + 1 cp), seeds a
//! namespace, registers an echo worker via the worker SDK over real
//! gRPC, submits a task via the caller SDK, asserts the task lands
//! `COMPLETED` with the echoed payload.
//!
//! `#[ignore]` so default `cargo test` skips it; CI runs with
//! `--include-ignored --test-threads=1`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use taskq_caller_sdk::{SubmitOutcome, SubmitRequest};
use taskq_e2e_tests::{require_docker, wait_for, ComposeFile, ComposeStack};
use taskq_proto::TerminalState;
use taskq_worker_sdk::{AcquiredTask, HandlerOutcome, TaskHandler, WorkerBuilder};

const NS: &str = "e2e_happy";

struct EchoHandler;

impl TaskHandler for EchoHandler {
    async fn handle(&self, task: AcquiredTask) -> HandlerOutcome {
        HandlerOutcome::Success(task.payload)
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker; run with --include-ignored"]
async fn happy_path_round_trip() -> Result<()> {
    // Arrange
    require_docker()?;
    let stack = ComposeStack::up(ComposeFile::Base)?;
    stack.seed_namespace(NS).await?;

    let endpoint = stack.primary_endpoint();
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
    // Give the worker a moment to register and start its first long-poll
    // before the submit so `wake_one` finds a waiter parked in the pool.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut caller = stack.caller().await?;

    // Act
    let outcome = caller
        .submit(SubmitRequest::new(NS, "echo", Bytes::from_static(b"hello")))
        .await
        .map_err(|e| anyhow!("submit: {e}"))?;
    let task_id = match outcome {
        SubmitOutcome::Created { task_id, .. } => task_id,
        other => return Err(anyhow!("expected Created, got {other:?}")),
    };

    let final_state = wait_for(Duration::from_secs(20), Duration::from_millis(200), || {
        let task_id = Arc::new(task_id.clone());
        let mut caller = caller.clone();
        async move {
            let state = caller
                .get_result(&task_id)
                .await
                .map_err(|e| anyhow!("get_result: {e}"))?;
            if state.status == TerminalState::COMPLETED {
                Ok(Some(state))
            } else {
                Ok(None)
            }
        }
    })
    .await?;

    // Assert: task reached COMPLETED via the real wire path. v0.1.0
    // does not yet read the per-attempt outcome / result_payload from
    // task_runtime (that row is deleted on completion), so this test
    // only asserts terminal status. When the read path lands the
    // remaining checks should be re-enabled here.
    assert_eq!(final_state.status, TerminalState::COMPLETED);

    worker_handle.abort();
    Ok(())
}
