<!-- agent-updated: 2026-05-02T16:55:39Z -->

# taskq-cli

Operator CLI for `taskq-rs`. Wraps the `TaskAdmin` gRPC service exposed by
`taskq-cp` with friendly subcommands for namespace lifecycle, quota tuning,
DLQ replay, task purge, and live stats. Pair with `taskq-cp serve` for the
server side; this binary speaks gRPC, never the database directly.

## Install

From within the workspace:

```sh
cargo build -p taskq-cli --release
./target/release/taskq-cli --help
```

## Connect

The CLI talks to a running `taskq-cp` over gRPC. Configure once via env:

```sh
export TASKQ_ENDPOINT=http://taskq-cp.internal:50051
export TASKQ_TOKEN=...   # optional; v1 accepts but ignores (Phase 7)
```

Or pass `--endpoint`/`--token` per invocation.

## Command reference

| Command | Admin RPC | Notes |
|---------|-----------|-------|
| `namespace create <name>` | `SetNamespaceQuota` | seeds `system_default`-derived row |
| `namespace list --namespace <ns>` | `GetStats` | one-row stand-in until `ListNamespaces` lands |
| `namespace enable <name>` | `EnableNamespace` | reverses a previous `disable` |
| `namespace disable <name>` | `DisableNamespace` | sets `--reason` for caller-visible hint |
| `set-quota <ns> [flags...]` | `SetNamespaceQuota` | one flag per quota field |
| `purge --namespace <ns> --confirm-namespace <ns>` | `PurgeTasks` | destructive; `--dry-run` and y/N gate |
| `replay --namespace <ns> --confirm-namespace <ns>` | `ReplayDeadLetters` | destructive; `--dry-run` and y/N gate |
| `stats <ns>` | `GetStats` | snapshot of in-memory counters |
| `list-workers <ns> [--include-dead]` | `ListWorkers` | tabular output |

Global flags: `--endpoint`, `--token`, `--format human|json`, `--yes` (skip
prompts), and any subcommand's `--help`.

## Examples

Tune a quota then verify:

```sh
taskq-cli set-quota crawler --max-pending=200000 --max-submit-rpm=120000
taskq-cli stats crawler --format json | jq '.pending_count'
```

List all workers, including ones Reaper B has tombstoned:

```sh
taskq-cli list-workers crawler --include-dead
```

Drain a namespace's dead-letter queue (interactive prompt unless `--yes`):

```sh
taskq-cli replay --namespace crawler --confirm-namespace crawler --max-tasks 500 \
    --audit-note "drain after upstream fix"
```

Preview a destructive purge first:

```sh
taskq-cli purge --namespace crawler --confirm-namespace crawler --dry-run
```

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | success |
| 2 | connect failure (DNS / TCP / handshake) |
| 3 | gRPC transport error |
| 4 | server returned a structured `Rejection` |
| 64 | invalid arguments |
| 70 | internal protocol violation |
| 130 | aborted by operator at confirmation prompt |
