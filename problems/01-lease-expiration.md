<!-- agent-updated: 2026-04-30T03:35:00Z -->

# Problem: Lease acquisition & expiration

## The problem

When a worker pulls a task, the control plane must mark the task as "owned by worker W until time T." If the worker dies, hangs, or has a network partition before completing, the task must eventually be reclaimed and re-dispatched. If the worker is still alive but slow, the lease must be extendable. None of this can use real DB row locks — the connection holding the lock would be the worker, and gRPC is request/response, not a long-held transaction.

This is the central correctness problem. Get it wrong and you either:
- **Lose work** (lease expires too aggressively, task fails before retry kicks in)
- **Duplicate work** (lease expires while worker is still running, task is re-dispatched, both copies complete)
- **Stall work** (worker dies, but reaper never runs, task sits forever)

## Why it's hard

- "Worker died" and "worker is just slow" are indistinguishable from the control plane's perspective.
- Clocks drift. A reaper comparing `now()` to a stored `expires_at` from a different host is at the mercy of NTP.
- Heartbeats can themselves be lost — losing N heartbeats in a row should not always mean "kill the task."
- Reclaim is eventually consistent: between "lease expires" and "reaper notices," the task is in limbo.
- Extending a lease on every heartbeat doubles write traffic. Not extending it means the timeout has to be large enough to cover slow tasks, which means dead workers take longer to detect.

## What prior art does

| | Faktory | Hatchet | Temporal |
|---|---|---|---|
| Lock state | Redis ZSet `working` scored by expiry + in-mem `workingMap` | Postgres `v1_task_runtime` row with `timeout_at` | In-mem ack manager + persistent timer queue |
| Acquisition | LPOP queue → ZADD working | INSERT runtime row, DELETE queue row | Atomic state transition + write timer |
| Expiry detection | Periodic `ZREMRANGEBYSCORE` < now | `SELECT ... WHERE timeout_at <= NOW() FOR UPDATE SKIP LOCKED` | Timer fires at `now + heartbeat_timeout` |
| Renewal | In-mem `extension` field; Redis updated lazily on reap | `RefreshTimeout` RPC updates `timeout_at` in place | Heartbeat resets the timer |
| Failure of reaper itself | Tasks stuck in `working` until reaper resumes | Tasks stuck in `task_runtime` | Tasks stuck — timer subsystem must be HA |

Notable patterns:
- **Hatchet uses two reapers.** One on per-task `timeout_at`, one on `worker.last_heartbeat_at < NOW() - 30s`. Either can fire reclaim. More robust than a single timestamp.
- **Faktory's lazy renewal trick.** Heartbeat extensions are in-memory only; durable store is updated only when the reaper would have killed the task. Avoids write amplification.
- **Temporal is event-driven, not polled.** Lower steady-state cost, but requires a reliable timer subsystem (a whole project of its own).

## Open questions for taskq-rs

1. **One reaper or two?** Per-task `timeout_at` only, or also a `worker_last_heartbeat` reaper like Hatchet?
2. **Where does lease state live?** Status flag on the task row, or a separate `task_runtime` table? Separate table makes the "in-flight" set cheap to scan; status flag keeps writes co-located.
3. **Renewal: eager or lazy?** Update `timeout_at` on every heartbeat, or only on reap (Faktory-style)?
4. **Heartbeat cadence and timeout multiplier.** Faktory: 15s / 30min. Hatchet: ~4s / 30s. Pick defaults; let task type override.
5. **Lease token vs. opaque task token.** Should the worker get a separate lease nonce that proves ownership on every subsequent RPC, or rely on `(task_id, attempt)` matching?
6. **Reaper cadence vs. shortest allowable lease.** If reaper runs every 5s, leases below 5s are meaningless. Document the floor.
7. **Reaper at-most-one or concurrent?** If multiple control-plane replicas, do we use `FOR UPDATE SKIP LOCKED` (concurrent reapers) or leader election (single reaper)?
8. **Clock skew tolerance.** Do we add a grace period on top of `timeout_at` before reclaiming, in case the reaper's clock is ahead of the worker's?
9. **What does the worker observe on lease loss?** If the worker eventually phones home with a result for a task that's already been reclaimed, do we accept it (idempotency wins), reject it, or log + drop?

## Decisions deferred

No decisions made yet. This doc captures the problem so we can decide deliberately after thinking through the other problems.

## Post-debate refinement

After the multi-agent design debate (see `/design.md` §13 for full list), the heartbeat path is **carved out of the SERIALIZABLE state-transition path**:

- Heartbeats write to a dedicated `worker_heartbeats` table at READ COMMITTED isolation. They are best-effort liveness signals, not state transitions; losing a few across a CP crash is consistent with at-least-once delivery.
- Reaper B (dead-worker reclaim) reads `worker_heartbeats.last_heartbeat_at` rather than chasing `task_runtime` updates.
- The lease's `timeout_at` on `task_runtime` is extended **lazily** — only when a heartbeat arrives within ε of expiry (default ε = 25% of lease duration). Steady-state heartbeat is one cheap row UPDATE; only actual lease extensions touch `task_runtime` under SERIALIZABLE.

This resolves Q3 of the original open questions ("Renewal: eager or lazy?") and Q4 ("heartbeat cadence and timeout multiplier") concretely. See `design.md` §1.1 (carve-outs), §6.3 (heartbeat flow), and §6.6 (reaper). Eager Postgres-write per heartbeat would have hit a write-storm wall around 2-5K concurrent workers; the carve-out moves that ceiling to wherever READ COMMITTED + lazy extension lets it land, which is significantly higher.

## Round-2 refinement

The first round-2 debate found two gaps in the round-1 carve-out:

- **Lazy-extension cadence trap.** The original ε semantics ("fires when within ε of expiry") allowed a calibration trap: a healthy worker heartbeating at cadence > ε would skip every extension and lose its lease at expiry. Resolved in `design.md` §6.3 by redefining the trigger against `last_extended_at` ("fires if `(NOW − last_extended_at) ≥ (lease − ε)`"), which is symmetric and cadence-invariant, plus a validated invariant `ε ≥ 2 × min_heartbeat_interval_seconds` enforced at `SetNamespaceQuota`. The Rust SDK additionally clamps default heartbeat cadence to `≤ ε / 3` and warns on overrides.
- **Reaper-B → heartbeat resurrection.** A worker whose tasks Reaper B reclaims could continue heartbeating successfully because the heartbeat UPDATE was keyed only on `worker_id`, with no link to the reclaim. Resolved by adding `declared_dead_at` to `worker_heartbeats`: Reaper B stamps it on reclaim; heartbeat path filters `WHERE declared_dead_at IS NULL` and returns `WORKER_DEREGISTERED` if the row was dead-stamped. SDK responds by re-`Register`ing.
- **Indexing the heartbeat table.** Reaper B's `WHERE last_heartbeat_at < NOW() - lease_window` query needed a stated indexing policy. Resolved by committing to BRIN on `worker_heartbeats(last_heartbeat_at)` (`design.md` §8.3) — cheap to maintain on append-mostly time-correlated data, doesn't defeat HOT updates, and the v1 single-replica heartbeat ceiling is documented as ~10K/sec.
