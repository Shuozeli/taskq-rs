<!-- agent-updated: 2026-05-01T00:00:00Z -->

# taskq-cp

The taskq-rs control-plane gRPC server binary (and supporting library).

## What this crate is

- The `taskq-cp` binary serves the `TaskQueue`, `TaskWorker`, and
  `TaskAdmin` gRPC services described in `design.md` §3.2.
- It owns the lifecycle: configuration loading, OpenTelemetry pipeline,
  storage handle, strategy registry, gRPC server bootstrap, health
  endpoints, graceful shutdown.
- It is **multi-replica capable** — no in-memory state on the correctness
  path. v1 ships single-replica deployments.

## Phase status

This crate is currently at the Phase 5a scaffold milestone:

- gRPC handler bodies are `todo!()` — Phase 5b implements the lifecycle.
- Reaper tasks are not spawned — Phase 5b implements them.
- OTel emit sites are declared but not invoked — Phase 5d wires them.
- The OTLP exporter pipeline is not wired — Phase 5d implements it.
- The migration check is a `tracing::debug!` — Phase 5b reads
  `taskq_meta.schema_version`.

See `tasks.md` Phase 5 for the full breakdown.

## Module layout

```
src/
  main.rs                     # binary entry point
  lib.rs                      # library entry point + re-exports
  config.rs                   # CpConfig: TOML + env override
  error.rs                    # CpError + Result alias
  server.rs                   # gRPC server bootstrap + interceptors
  observability.rs            # OTel pipeline + standard metric set
  health.rs                   # /healthz, /readyz, /metrics
  shutdown.rs                 # SIGTERM/SIGINT broadcast plumbing
  state.rs                    # CpState shared across handlers/reapers
  strategy/
    mod.rs                    # StrategyRegistry + build_admitter / build_dispatcher
    admitter.rs               # Admitter trait + Always / MaxPending / CoDel
    dispatcher.rs             # Dispatcher trait + PriorityFifo / AgePromoted / RandomNamespace
  handlers/
    mod.rs
    task_queue.rs             # caller-facing service skeleton
    task_worker.rs            # worker-facing service skeleton
    task_admin.rs             # admin service skeleton
```

## Running

```bash
# Print the CLI help (always works, no config needed):
cargo run --bin taskq-cp -- --help

# Run with a config (handler bodies still todo!()):
cargo run --bin taskq-cp -- --config etc/taskq-cp.toml
```

A minimal SQLite + Prometheus config:

```toml
bind_addr = "0.0.0.0:50051"
health_addr = "0.0.0.0:9090"

[storage_backend]
kind = "sqlite"
path = "./taskq.db"

[otel_exporter]
kind = "prometheus"
```

Environment overrides applied after the file is parsed:

| Variable | Effect |
|---|---|
| `TAILSCALE_IP` | rebinds `bind_addr` host (preserves port) per global rule #8 |
| `TASKQ_BIND_ADDR` | full host:port override for `bind_addr` |
| `TASKQ_HEALTH_ADDR` | full host:port override for `health_addr` |

## Verification

```bash
cargo build --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --no-run
```

End-to-end gRPC traffic is not part of Phase 5a — handlers are `todo!()`
and a real RPC will panic. Phase 5b lights them up.
