<!-- agent-updated: 2026-05-02T18:00:00Z -->

# Codelab 1: Build a worker in 50 lines

You will write a working `taskq-rs` worker in roughly fifty lines of Rust.
The worker registers with a control plane, long-polls for `echo` tasks,
returns the payload bytes back as the result, and shuts down cleanly on
SIGTERM. Every piece is real `taskq-worker-sdk` code -- no pseudocode.

## What you will build

```
+----------+   SubmitTask("echo", b"hi")   +-------------+
| caller   | ----------------------------> | taskq-cp    |
| (CLI/SDK)|                               | (gRPC)      |
+----------+ <-------- task_id ----------- +-------------+
                                                |
                         AcquireTask (long-poll)|
                         CompleteTask           |
                                                v
                                        +---------------+
                                        | your worker   |
                                        | (this guide)  |
                                        +---------------+
```

The worker is the bottom box. It:

1. opens a gRPC connection to `taskq-cp`,
2. calls `Register` to obtain a `worker_id`, the namespace's lease window
   (`lease_duration_ms`), the lazy-extension threshold (`eps_ms`), and
   the namespace's registered `error_classes`,
3. spawns a long-poll `AcquireTask` loop that hands each task to your
   `TaskHandler`,
4. ships `CompleteTask` / `ReportFailure` per the handler's outcome,
5. heartbeats at `eps_ms / 3` so the lease never lapses while the
   handler is running,
6. on SIGTERM, drains in-flight handlers, then sends `Deregister`.

Reference: [`design.md` Sec 6.2--6.6](../design.md) for the wire-level
flow.

## Prerequisites

You need a running `taskq-cp` and a registered namespace.

### Run the control plane against in-memory SQLite

`taskq-cp` ships with a SQLite backend that is fine for tutorials. Save
this as `taskq-cp.toml`:

```toml
bind_addr = "127.0.0.1:50051"
health_addr = "127.0.0.1:9090"

[storage_backend]
kind = "sqlite"
path = ":memory:"

[otel_exporter]
kind = "disabled"
```

Then start the CP with `--auto-migrate` so the schema is created on
the fly:

```bash
cargo run -p taskq-cp -- --auto-migrate --config taskq-cp.toml
```

You should see `serving gRPC on 127.0.0.1:50051` in the logs.

### Create a namespace with `taskq-cli`

In a second shell:

```bash
export TASKQ_ENDPOINT=http://127.0.0.1:50051
cargo run -p taskq-cli -- namespace create demo
cargo run -p taskq-cli -- set-quota demo --max-pending 1000 --max-workers 16
```

`namespace create demo` writes a fresh quota row derived from
`system_default` (see [`taskq-cli` README](../taskq-cli/README.md)).
The `set-quota` call wires capacity numbers low enough to bounce over
in a tutorial without hitting noisy-neighbour caps.

## Step 1: dependencies

Create a new binary crate next to your other code, then add this to
`Cargo.toml`:

```toml
[package]
name = "echo-worker"
version = "0.1.0"
edition = "2021"

[dependencies]
taskq-worker-sdk = { git = "https://github.com/Shuozeli/taskq-rs", branch = "main" }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
bytes = "1"
http = "1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

The git URL is the canonical Shuozeli mirror; pin a `rev` once you go
to production. `taskq-worker-sdk` re-exports `bytes::Bytes` through
its `AcquiredTask::payload` and `HandlerOutcome::Success` variants, so
keep that crate in your direct deps.

## Step 2: define your handler

Implement `TaskHandler` for any `Send + Sync` type. The handler takes one
[`AcquiredTask`](../taskq-worker-sdk/src/handler.rs) and returns a
[`HandlerOutcome`](../taskq-worker-sdk/src/handler.rs):

```rust
use bytes::Bytes;
use taskq_worker_sdk::{AcquiredTask, HandlerOutcome, TaskHandler};

struct EchoHandler;

impl TaskHandler for EchoHandler {
    async fn handle(&self, task: AcquiredTask) -> HandlerOutcome {
        tracing::info!(
            task_id = %task.task_id,
            attempt = task.attempt_number,
            bytes = task.payload.len(),
            "echo: returning payload",
        );
        HandlerOutcome::Success(task.payload)
    }
}
```

Three things to know about `HandlerOutcome`:

- `Success(Bytes)` -- ships `CompleteTask { result_payload }`. The bytes
  go straight onto the wire; size capped by the namespace's
  `max_payload_bytes` quota.
- `RetryableFailure { error_class, message, details }` -- ships
  `ReportFailure { retryable: true }`. The CP applies server-side
  backoff math (see [`design.md` Sec 6.5](../design.md)) and sets
  `WAITING_RETRY`.
- `NonRetryableFailure { ... }` -- ships `ReportFailure { retryable:
  false }`. The task transitions to `FAILED_NONRETRYABLE` immediately.

`error_class` is a typed [`ErrorClass`](../taskq-worker-sdk/src/error_class.rs)
minted from the `ErrorClassRegistry` returned by `Register`. The SDK's
public surface refuses to construct one for a class the namespace has
not registered, so unknown classes cannot leak onto the wire.

## Step 3: build the worker

`WorkerBuilder` runs `Register` synchronously in `build().await`, caches
the response, and returns a [`Worker`](../taskq-worker-sdk/src/runtime.rs)
ready to run:

```rust
use std::time::Duration;
use taskq_worker_sdk::{BuildError, WorkerBuilder};

async fn build() -> Result<taskq_worker_sdk::Worker, BuildError> {
    let uri: http::Uri = "http://127.0.0.1:50051".parse().unwrap();
    WorkerBuilder::new(uri, "demo")
        .with_task_types(vec!["echo".to_owned()])
        .with_concurrency(2)
        .with_long_poll_timeout(Duration::from_secs(30))
        .with_handler(EchoHandler)
        .build()
        .await
}
```

The arguments map to `RegisterWorkerRequest` fields (see
[`design.md` Sec 6.3](../design.md)):

- `namespace` -- the tenant identity. Quotas, metrics, and the error-
  class registry are scoped here.
- `with_task_types(...)` -- the strings this worker handles. The CP
  filters dispatch by this set.
- `with_concurrency(n)` -- how many tasks the harness is willing to
  process in parallel; spawns `n` acquire loops.
- `with_long_poll_timeout(d)` -- per-call deadline for the long-poll;
  the server caps at 60s (see `taskq-cp/src/config.rs::long_poll_max_seconds`).
  When a poll returns empty, the SDK reconnects immediately.

If you skip `with_handler(...)`, `build()` returns
`BuildError::MissingHandler` *before* attempting the network call.

## Step 4: run it

`run_until_shutdown()` listens for SIGINT/SIGTERM, drains in-flight
handlers (default 30s), then issues `Deregister`:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter("info,taskq_worker_sdk=debug")
        .init();

    let worker = build().await?;
    worker.run_until_shutdown().await?;
    Ok(())
}
```

Run it:

```bash
cargo run -p echo-worker
```

You should see one log line per heartbeat tick (every `eps_ms / 3`) and
a long-poll log line every 30 seconds while the queue is empty.

The full file:

```rust
use std::time::Duration;
use taskq_worker_sdk::{
    AcquiredTask, BuildError, HandlerOutcome, TaskHandler, Worker, WorkerBuilder,
};

struct EchoHandler;

impl TaskHandler for EchoHandler {
    async fn handle(&self, task: AcquiredTask) -> HandlerOutcome {
        tracing::info!(task_id = %task.task_id, "echo");
        HandlerOutcome::Success(task.payload)
    }
}

async fn build() -> Result<Worker, BuildError> {
    let uri: http::Uri = "http://127.0.0.1:50051".parse().unwrap();
    WorkerBuilder::new(uri, "demo")
        .with_task_types(vec!["echo".to_owned()])
        .with_concurrency(2)
        .with_long_poll_timeout(Duration::from_secs(30))
        .with_handler(EchoHandler)
        .build()
        .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let worker = build().await?;
    worker.run_until_shutdown().await?;
    Ok(())
}
```

That is 26 non-blank lines of Rust. You promised fifty.

## Step 5: send it work

Pick either approach.

### From a Rust caller

```rust
use bytes::Bytes;
use taskq_caller_sdk::{CallerClient, SubmitOutcome, SubmitRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CallerClient::connect("http://127.0.0.1:50051".parse()?)
        .await?
        .with_default_namespace("demo");

    let outcome = client
        .submit(SubmitRequest::new("demo", "echo", Bytes::from_static(b"hello")))
        .await?;

    match outcome {
        SubmitOutcome::Created { task_id, .. } => println!("submitted {task_id}"),
        SubmitOutcome::Existing { task_id, .. } => println!("dedup hit: {task_id}"),
        SubmitOutcome::PayloadMismatch { existing_task_id, .. } => {
            println!("idempotency-key reuse with different payload; existing {existing_task_id}");
        }
    }
    Ok(())
}
```

The SDK auto-generates a UUIDv7 idempotency key when you do not pass
one. See [`taskq-caller-sdk/tests/smoke.rs`](../taskq-caller-sdk/tests/smoke.rs)
for the wire-level expectations.

### Or with `submit_and_wait` for synchronous flows

```rust
use std::time::Duration;
use taskq_caller_sdk::{CallerClient, SubmitRequest, TerminalOutcome};

let outcome = client
    .submit_and_wait(
        SubmitRequest::new("demo", "echo", Bytes::from_static(b"hi")),
        Duration::from_secs(30),
    )
    .await?;

match outcome {
    TerminalOutcome::Settled(state) => println!("status={:?}", state.status),
    TerminalOutcome::TimedOut { task_id } => println!("still pending: {task_id}"),
}
```

## What just happened

The lifecycle ([`design.md` Sec 6.2--6.6](../design.md)):

1. **Register** -- your worker's `build().await` opened a gRPC channel,
   sent `RegisterWorkerRequest { namespace: "demo", task_types: ["echo"],
   declared_concurrency: 2 }`, and received back the `worker_id`,
   `lease_duration_ms`, `eps_ms`, and the namespace's error-class
   registry. The SDK clamps the heartbeat cadence to `eps_ms / 3` so
   leases never lapse.

2. **Long-poll** -- two acquire loops (because of `with_concurrency(2)`)
   each block on `AcquireTask` for up to 30s. The CP holds the long-poll
   open until a matching task is committed, at which point one waiter
   wakes (single-waiter wakeup -- the loser re-blocks).

3. **Dispatch** -- the CP runs the configured `Dispatcher` (default
   `PriorityFifo` per [`design.md` Sec 7.2](../design.md)) inside a
   SERIALIZABLE transaction, inserts a `task_runtime` row with `worker_id`
   and `timeout_at = NOW + lease_duration`, transitions the task to
   `DISPATCHED`, and returns the task to your worker.

4. **Handler** -- the harness calls `EchoHandler::handle`. The handler
   returns `HandlerOutcome::Success(payload)`.

5. **Complete** -- the harness sends `CompleteTaskRequest { result_payload
   }`. The CP commits a SERIALIZABLE transaction that inserts a
   `task_results` row, transitions the task to `COMPLETED`, deletes the
   `task_runtime` row, and replies. On internal `40001`
   serialization-conflict the CP retries up to three times transparently
   so the worker never sees opaque transient errors
   ([`design.md` Sec 6.4](../design.md)).

6. **Heartbeat** -- in parallel with all of the above, the heartbeat
   task pings `Heartbeat` at `eps_ms / 3`. The CP's READ COMMITTED
   carve-out (see [`design.md` Sec 1.1](../design.md)) means most pings
   are a single cheap UPSERT into `worker_heartbeats`; the SERIALIZABLE
   `task_runtime.timeout_at` extension only fires lazily, when more than
   `lease_duration - eps` has elapsed since the last extension.

7. **Shutdown** -- on SIGTERM the harness flips a stop flag, lets in-
   flight handlers finish (default 30s drain), then sends `Deregister`.

## Common errors

The harness handles the common rejection codes for you (see
[`taskq-worker-sdk/README.md`](../taskq-worker-sdk/README.md)):

| Reason                          | What it means                                | What the SDK does                                 |
|---------------------------------|----------------------------------------------|---------------------------------------------------|
| `WORKER_DEREGISTERED`           | Reaper B tombstoned this worker (heartbeat gap exceeded the lease window, see [`design.md` Sec 6.6](../design.md)) | Re-`Register`, mint a fresh `worker_id`, resume polling |
| `LEASE_EXPIRED`                 | The reaper reclaimed the lease before your `CompleteTask` landed -- another worker may already be running the task | Log + drop the result. `design.md` Sec 6.4 promises a distinct error code so you do not have to fish for it in opaque transients |
| `REPLICA_WAITER_LIMIT_EXCEEDED` | The CP's per-replica long-poll waiter cap was hit | Back off 1s and retry (see `problems/06-backpressure.md`) |
| `UNKNOWN_ERROR_CLASS`           | You sent a `ReportFailure` with an `error_class` not in the namespace's registry | Cannot happen with the SDK's `ErrorClass` newtype unless the operator removed a class after `Register`; refresh by re-`Register`ing |

Things that can go wrong from your side:

- **Payload parse errors** -- if your handler decodes `task.payload`
  with `serde_json::from_slice` and the bytes are not valid JSON,
  return
  `HandlerOutcome::NonRetryableFailure { error_class: registry.error_class("InvalidPayload")?, ... }`.
  Retrying a parse failure is wasted work.
- **Transient downstream calls** -- map them to
  `HandlerOutcome::RetryableFailure`. The CP will compute the next
  `retry_after` with full jitter (`design.md` Sec 6.5).
- **Handler runs longer than the lease** -- the CP's reaper will
  reclaim and another worker will pick up attempt N+1 while your
  original handler is still running. Use `task.deadline_hint` as a
  soft deadline inside your handler.

## Next steps

- [Codelab 2](02-migrating-a-postgres-queue-service.md) -- migrating an
  existing Postgres-backed queue to taskq-rs.
- [Codelab 3](03-operating-taskq-rs.md) -- day-2 operator playbooks.
- [`design.md` Sec 6](../design.md) -- the lifecycle in full.
- [`problems/01-lease-expiration.md`](../problems/01-lease-expiration.md)
  -- the heartbeat carve-out's failure modes.
