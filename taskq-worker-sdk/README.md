<!-- agent-updated: 2026-05-02T02:59:45Z -->

# taskq-worker-sdk

Worker-side Rust SDK for the [`taskq-rs`](../README.md) gRPC contract.

Wraps the generated `task_worker_client::TaskWorkerClient` with an ergonomic
harness that handles `Register`, the long-poll `AcquireTask` loop, `Heartbeat`,
`CompleteTask` / `ReportFailure`, and graceful `Deregister`.

## Hello world

```rust,no_run
use std::time::Duration;
use taskq_worker_sdk::{
    AcquiredTask, HandlerOutcome, TaskHandler, WorkerBuilder,
};

struct EchoHandler;

impl TaskHandler for EchoHandler {
    async fn handle(&self, task: AcquiredTask) -> HandlerOutcome {
        HandlerOutcome::Success(task.payload)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let uri: http::Uri = "http://127.0.0.1:50051".parse()?;
    let worker = WorkerBuilder::new(uri, "billing")
        .with_task_types(vec!["charge".to_owned()])
        .with_concurrency(4)
        .with_long_poll_timeout(Duration::from_secs(30))
        .with_handler(EchoHandler)
        .build()
        .await?;
    worker.run_until_shutdown().await?;
    Ok(())
}
```

## Lifecycle

`build()` runs `Register` synchronously and caches `worker_id`,
`lease_duration_ms`, `eps_ms`, and the namespace's `error_classes` registry.
`run_until_shutdown` (or `run_with_signal`) spawns:

- one **acquire loop** per `concurrency` slot -- long-polls `AcquireTask`,
  hands tasks off to the user's `TaskHandler`, then ships
  `CompleteTask` / `ReportFailure`.
- one **heartbeat task** -- ticks at `eps_ms / 3` by default
  ([`design.md` Sec 6.3](../design.md) SDK clamp). Overrides above `eps/3`
  trigger a startup warning.

Rejections handled internally:

| reason                        | SDK action                                  |
|-------------------------------|---------------------------------------------|
| `REPLICA_WAITER_LIMIT_EXCEEDED` | back off 1s and retry                     |
| `WORKER_DEREGISTERED`         | re-`Register` with a fresh `worker_id`      |
| `LEASE_EXPIRED`               | log + drop -- another worker owns the task  |
| `UNKNOWN_ERROR_CLASS`         | guarded at compile time via `ErrorClass`    |

Graceful shutdown stops the acquire loops, drains in-flight handlers (default
30s), then issues `Deregister`. `RunError::DrainTimeout` surfaces if the
deadline elapses with handlers still running.

## See also

- [`design.md`](../design.md) Sec 6.2-6.6 -- wire semantics.
- [`problems/01-lease-expiration.md`](../problems/01-lease-expiration.md) -- heartbeat carve-out.
- [`problems/06-backpressure.md`](../problems/06-backpressure.md) -- waiter-limit retry behaviour.
- [`problems/08-retry-storms.md`](../problems/08-retry-storms.md) -- typed `error_class`.
