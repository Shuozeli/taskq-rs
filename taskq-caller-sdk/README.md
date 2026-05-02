<!-- agent-updated: 2026-04-26T00:00:00Z -->
# taskq-caller-sdk

Caller-side Rust SDK for the [taskq-rs](../README.md) gRPC contract.
Wraps the auto-generated `task_queue_client::TaskQueueClient` (from
[`taskq-proto`](../taskq-proto)) with retry-aware ergonomics, automatic
idempotency keys, and W3C trace context propagation.

## Hello World

```rust
use bytes::Bytes;
use std::time::Duration;
use taskq_caller_sdk::{CallerClient, SubmitRequest, TerminalOutcome};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CallerClient::connect("http://127.0.0.1:50051".parse()?)
        .await?
        .with_default_namespace("orders")
        .with_retry_budget(5);

    let outcome = client
        .submit_and_wait(
            SubmitRequest::new("orders", "send_email", Bytes::from_static(b"...")),
            Duration::from_secs(30),
        )
        .await?;

    match outcome {
        TerminalOutcome::Settled(state) => println!("done: {:?}", state.status),
        TerminalOutcome::TimedOut { task_id } => println!("still pending: {task_id}"),
    }
    Ok(())
}
```

## Error handling

Every RPC returns `Result<_, ClientError>`. The variants split on intent:

- `Connect` / `Transport` — the network or gRPC framing layer failed.
  Retry the operation at the caller's discretion.
- `Rejected { reason, retryable, retry_after_ms, hint }` — the server
  returned a structured rejection. The SDK has *already* exhausted its
  internal retry budget if `retryable` was `true`. When `retryable` is
  `false` (e.g. `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD`),
  surface the error to user code; do not retry.
- `InvalidArgument` — caller-side validation failed before any RPC.
- `Internal` — wire-protocol violation. Indicates a server bug.

Idempotency keys default to a freshly-generated UUIDv7. Provide your own
on `SubmitRequest::idempotency_key` for explicit dedup control.

## Retry budget

`with_retry_budget(n)` caps the number of `RESOURCE_EXHAUSTED`-with-
`retryable=true` retries. Default is 5. Set to 1 to disable internal
retry when an outer layer already retries (per `design.md` §10.4).

## Links

- [`design.md` §10.1, §10.4, §11.2](../design.md) — wire contract for
  rejections, retry-after semantics, and W3C trace propagation.
- [`problems/03-idempotency.md`](../problems/03-idempotency.md) —
  payload hash + dedup record contract.
- [`problems/06-backpressure.md`](../problems/06-backpressure.md) —
  retry-after timestamp semantics.
