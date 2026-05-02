<!-- agent-updated: 2026-04-26T00:00:00Z -->

# taskq-integration-tests

End-to-end integration tests for `taskq-rs`. Each test spins up a real
control-plane gRPC server in-process, points the caller and worker SDKs at
it, and exercises one path from `design.md` Sec 6 (lifecycle) / Sec 10
(rejection model) / Sec 11 (observability).

## Layout

| File | What it covers |
|------|---------------|
| `src/lib.rs` | `TestHarness` -- shared in-process CP + sidecar SQLite reader. |
| `tests/lifecycle.rs` | Submit -> dispatch -> complete round-trip, multi-worker dispatch, cancel. |
| `tests/retry.rs` | Retryable / non-retryable failure paths (`HandlerOutcome`). |
| `tests/reaper.rs` | Reaper A (per-task timeout) + Reaper B (dead-worker reclaim). |
| `tests/idempotency.rs` | Per-terminal-state key holding/release, payload-mismatch rejection. |
| `tests/admin.rs` | `SetNamespaceQuota`, `DisableNamespace`, `PurgeTasks`, `ReplayDeadLetters`. |
| `tests/observability.rs` | Audit-log persistence + W3C `traceparent` survival. |

## Running

```
cargo test -p taskq-integration-tests
```

Each test starts a fresh on-disk SQLite database under `tempfile::tempdir`,
binds the gRPC server to a random localhost port, and spawns the reapers,
metrics refresher, and audit pruner. Tearing the harness down via
`harness.shutdown().await` flips a `watch::Sender<bool>` and joins every
background task with a 5s timeout each.

## Notes

- The harness uses a sidecar `rusqlite::Connection` against the same
  database file the storage crate writes to. SQLite WAL mode supports
  concurrent readers alongside the storage crate's single writer, so
  tests can sample the `tasks`, `worker_heartbeats`, and `audit_log`
  tables without going through the unfinished `GetTaskResult` RPC.
- `TestHarness::tick_reaper_a` / `tick_reaper_b` drive a single Reaper
  iteration on demand so retry/expiry tests do not have to wait on the
  production polling cadences (5s / 10s respectively).
- The `taskq-cp` `serve_in_process` helper is a thin wrapper around the
  same `Router` `main` builds; it binds to `127.0.0.1:0` and returns the
  assigned `SocketAddr`. Phase-9b tests connect via `http://<addr>`.
