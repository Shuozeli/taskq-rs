# Problem: Dispatch fairness & starvation

## The problem

When pending work exceeds worker capacity, the control plane must choose which task to dispatch next. The choice is multi-dimensional and the wrong choice silently starves some callers.

Four independent starvation modes:

1. **Inter-namespace.** Namespace A submits 10000 tasks, namespace B submits 1. Without active fairness, B waits behind A's whole backlog.
2. **Intra-namespace, across task types.** One namespace, two task types. The high-volume type starves the low-volume type.
3. **Priority vs. age.** A pending low-priority task submitted hours ago still loses to every fresh high-priority task. Strict priority means low-priority can wait forever.
4. **Worker capability skew.** A worker registered for a rare task type long-polls indefinitely while abundant work in other types goes undispatched. Inversely, tasks of a rare type pile up because most workers only handle the common types.

Each is independent. A scheme that solves (1) doesn't necessarily solve (2). A scheme that solves both might still allow (3).

## Why it's hard

Pull-based dispatch reframes "what's the next task" from a global to a per-pull decision. A worker arrives saying "I handle types X and Y; give me work." The dispatcher's query is filtered by the worker's `task_types` set *before* fairness is considered. Two pulls from differently-capable workers may make different fairness choices simultaneously.

Worse, the "right" fairness algorithm is workload-dependent. Latency-sensitive callers want strict priority. Multi-tenant SaaS wants weighted fair sharing. Throughput-sensitive batch wants FIFO. There is no universal answer — which means committing to one in v1 forces the wrong choice on some users.

## What prior art does

| | Faktory | Hatchet | Temporal |
|---|---|---|---|
| Cross-tenant fairness | None — single-tenant | None — `(tenant_id, queue, priority DESC, id ASC)`, big tenants dominate | Per-namespace task queues are independent; no auto-balance across them |
| Within-queue ordering | Worker FETCHes from a list of queues *in order* (queue 1 always first) | Strict priority + FIFO | FIFO per task queue |
| Anti-starvation | None | None | None |
| Quota / rate limit | None | Per-tenant RL queues (separate table) | Namespace rate limits in matching service |

Bottom line: production task queues mostly punt on cross-tenant fairness and rely on rate limits to prevent any single tenant from saturating capacity. Real WFQ-style fairness is rare.

## Direction

**Pluggable strategies at two seams: an `Admitter` trait at submit time and a `Dispatcher` trait at acquire time.** Both traits are compile-time linked (no dynamic loading), runtime-selected per namespace at CP startup (no hot-swap), and run inside the SERIALIZABLE transaction so any state they consult or update lives in the durable storage layer.

```rust
trait Admitter {
    async fn admit<S: StorageTx>(&self, ctx: &SubmitCtx, tx: &mut S) -> Admit;
}
enum Admit { Accept, Reject(RejectReason) }

trait Dispatcher {
    async fn pick_next<S: StorageTx>(&self, ctx: &PullCtx, tx: &mut S) -> Option<TaskId>;
}
```

A worker's `task_types` filter is part of `PullCtx` and is always applied as a pre-filter — it is not the strategy's responsibility. Strategies decide ordering and tenancy fairness given a candidate set.

**v1 ships the moderate set: three admitters, three dispatchers, all stateless** (computable from `tasks` table alone, no schema additions for per-strategy state):

| Admitter | Behavior |
|---|---|
| `Always` | Accept everything. Default. |
| `MaxPending` | Reject if `pending_count(namespace) > limit`. Limit configured per namespace. |
| `CoDel` | Adaptive — reject if oldest pending task in namespace has aged past target latency. CoDel-style backpressure. |

| Dispatcher | Behavior |
|---|---|
| `PriorityFifo` | `ORDER BY priority DESC, submitted_at ASC`. Default. Mirrors Hatchet/Temporal. |
| `AgePromoted` | Effective priority = base priority + `f(age)`; older tasks bubble up. Mitigates starvation mode (3) without per-namespace state. |
| `RandomNamespace` | Sample uniformly from active namespaces, then `PriorityFifo` within. Approximate inter-namespace fairness without per-namespace deficit tracking. |

Stateful strategies (`WeightedFair`, `TokenBucket`, `RoundRobin` with last-served pointer) are explicitly **deferred to v2**. They require schema additions for per-namespace deficit/state, complicate the storage trait, and we don't yet know which kind of fairness real workloads will need. The trait will already exist; v2 adds implementations.

**Configuration model:**

- Each namespace has a config row: `(namespace, admitter_kind, admitter_params_json, dispatcher_kind, dispatcher_params_json)`.
- Loaded at CP startup, cached for the life of the process.
- `SetQuota` / `SetStrategy` admin RPCs write to DB but take effect on next restart. Operators do rolling restarts to apply changes.
- A global default applies when no per-namespace config exists.

**Storage interaction.** Strategies are backend-agnostic — they pass *intent* (an ordering enum, a filter set) to the storage trait, which translates to native idiom. `PickOrdering::PriorityFifo` becomes `ORDER BY priority DESC, submitted_at ASC ... FOR UPDATE SKIP LOCKED` on Postgres, a subspace scan with row-locks on FoundationDB, an index iterator on stonedb. See problem #12 for the storage abstraction in detail.

**Anti-starvation guarantees in v1.** Honest answer: weak.

- (1) Inter-namespace: only `RandomNamespace` actively addresses. `MaxPending` admitter caps the worst case; relies on operator setting limits.
- (2) Intra-namespace cross-type: not addressed. Operators can configure per-task-type quotas in v2.
- (3) Priority starvation: `AgePromoted` dispatcher addresses if selected.
- (4) Worker capability skew: not addressed. Workers register the types they want; if no work matches, they long-poll until timeout. Operator visibility via worker stats.

This is acceptable for v1 because the trait surface lets us add stronger strategies in v2 without protocol changes.

## Open questions

1. **Multi-task pull (batched `AcquireTask`).** Should a worker request "up to N tasks" in one RPC? Batching reduces transaction overhead under high throughput but complicates lease semantics (one lease per task or one lease per batch?). Defer to a follow-up problem doc.
2. **Default `CoDel` target latency.** Needs a sensible default (5s? 30s?) and per-namespace override.
3. **`AgePromoted` aging function.** Linear, exponential, sigmoid? A sigmoid avoids unbounded promotion of stuck tasks. Default and tuning knobs TBD.
4. **What happens when no candidate matches the worker's `task_types`?** Long-poll continues, no work returned. How long is the long-poll timeout (default), and how does the worker SDK retry? Belongs to problem #07 (long-poll connection limits).
5. **Strategy + `SKIP LOCKED` interaction.** A `RandomNamespace` dispatcher that picks namespace N might find all of N's tasks locked by other concurrent dispatchers. Skip to the next sampled namespace, or return empty? Lean: skip up to K times, then return empty. Document the exact behavior in the strategy.
6. **Strategy versioning across CP replica restarts.** During a rolling restart, half the replicas have strategy v1 active, half have v2. Tasks submitted under v1's `Admitter` may be dispatched under v2's `Dispatcher`. Acceptable as long as both produce valid (just different) decisions; document.
