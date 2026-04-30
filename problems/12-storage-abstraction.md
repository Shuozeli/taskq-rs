<!-- agent-updated: 2026-04-30T03:35:00Z -->

# Problem: Storage backend abstraction

## The problem

The taskq-rs control plane needs to persist tasks, leases, idempotency keys, results, worker metadata, and namespace config. The natural temptation is to bind directly to one storage system — write hand-tuned Postgres SQL throughout the CP, get the best possible performance, ship it. This works until the second user wants SQLite for embedded testing, or the third user wants CockroachDB for multi-region, or we want to dogfood `stonedb`. Then the entire CP needs rewriting.

Storage abstraction is the discipline of putting a trait between the CP and its persistence layer so that swapping backends is a build-time decision, not a fork. Done well, it preserves performance and lets backends use their native idioms. Done poorly, it imposes a lowest-common-denominator query surface that's slow on every backend and prevents adopting new ones.

## Why it's hard

Three forces pull in different directions:

- **Consistency requirements are non-negotiable.** Problem #02 commits to external consistency. Backends that cannot provide it (plain Redis, DynamoDB without transactions, eventually-consistent KV stores) are out — the trait must encode this requirement so an unsuitable backend cannot even compile.
- **Backends have wildly different native query primitives.** Postgres has `FOR UPDATE SKIP LOCKED`, secondary indexes, `INSERT ... ON CONFLICT`. FoundationDB has range scans inside transactions and conflict ranges. SQLite has single-writer serialization. A trait that exposes only the lowest common denominator forces every backend through narrow operations and loses the optimizations each could naturally provide.
- **Strategies are part of the CP, not the backend.** The dispatcher trait from problem #05 is generic — `PriorityFifo`, `AgePromoted`, `RandomNamespace` are pure CP logic that must work on every backend. They cannot write SQL. They must express intent at a level that any conforming backend can translate.

The trait shape determines whether new strategies can be added without backend changes, and whether new backends can be added without strategy changes. Both must compose freely.

## What prior art does

| | Faktory | Hatchet | Temporal |
|---|---|---|---|
| Storage abstraction | Yes — `Store` interface (`storage/types.go`) with Redis impl | None — Postgres-only, sqlc-generated typed queries | Yes — Persistence interface, Cassandra/MySQL/Postgres/SQLite plugins |
| Query level | High — domain methods (`Retries()`, `Working()`, `Queues()`) | Backend-specific SQL | Mid — typed CRUD on persistence rows |
| Conformance test suite | Yes (interface satisfied by impl) | N/A | Yes — extensive shared test suite |
| Strategies on top | None (Faktory has no strategy framework) | Hatchet's queue logic uses Postgres directly | Some matching/dispatch logic is per-persistence |

Notable: Temporal is the closest model — multiple backend plugins, conformance tests, strategy logic above the persistence layer. The persistence interface is large (~50 methods) but stable; new backends can be added without changing the surface above it.

## Direction

**Two-layer trait: `Storage` (connection lifecycle) + `StorageTx` (operations within a transaction).** Backends implement both. The CP and all strategies operate against `StorageTx` only — they never see backend-specific types.

```rust
trait Storage: Send + Sync + 'static {
    type Tx<'a>: StorageTx + 'a where Self: 'a;
    async fn begin(&self) -> Result<Self::Tx<'_>, StorageError>;
}

trait StorageTx: Send {
    // Submit path
    async fn lookup_idempotency(&mut self, key: &IdempotencyKey) -> Result<Option<DedupRecord>>;
    async fn insert_task(&mut self, t: NewTask, dedup: NewDedupRecord) -> Result<TaskId>;

    // Dispatch path
    async fn pick_and_lock_pending(&mut self, c: PickCriteria) -> Result<Option<LockedTask>>;
    async fn record_acquisition(&mut self, lease: NewLease) -> Result<()>;

    // Worker path
    async fn record_heartbeat(&mut self, lease: &LeaseRef, at: Timestamp) -> Result<HeartbeatAck>;
    async fn complete_task(&mut self, lease: &LeaseRef, outcome: TaskOutcome) -> Result<()>;

    // Reaper path
    async fn list_expired_runtimes(&mut self, before: Timestamp, n: usize) -> Result<Vec<ExpiredRuntime>>;
    async fn reclaim_runtime(&mut self, runtime: &RuntimeRef) -> Result<()>;

    // Cleanup
    async fn delete_expired_dedup(&mut self, before: Timestamp, n: usize) -> Result<usize>;

    // Admin
    async fn get_namespace_config(&mut self, ns: &Namespace) -> Result<NamespaceConfig>;
    // ... etc.

    async fn commit(self) -> Result<()>;
    async fn rollback(self) -> Result<()>;
}

struct PickCriteria {
    namespaces: NamespaceFilter,
    task_types: TaskTypeFilter,
    ordering: PickOrdering,
}

enum PickOrdering {
    PriorityFifo,
    AgePromoted { age_weight: f64 },
    RandomNamespace { sample_attempts: u32 },
    // future: WeightedFair { ... }, etc.
}
```

Strategies pass `PickOrdering` enum variants to the backend; the backend translates each variant to its native idiom. New strategies require new variants and corresponding backend implementations — a coordinated change, but localized.

**Backend conformance requirements** (encoded in trait contracts, validated by a shared test suite all backends must pass):

1. **External consistency.** Transactions are serializable in the strict sense — commit order respects real-time order. Not just "snapshot isolation."
2. **Non-blocking row locking with skip semantics.** `pick_and_lock_pending` must skip rows already locked by concurrent transactions, not block on them. Postgres `FOR UPDATE SKIP LOCKED`, FoundationDB conflict ranges + retry, CockroachDB SFU all qualify. A backend that can only block-and-wait fails throughput targets and is rejected.
3. **Indexed range scans with predicates.** The dispatch query needs efficient filtering by `(namespace, task_type, status, priority)`. Backends without secondary indexes must provide equivalent (e.g., explicit secondary subspaces in FoundationDB).
4. **Atomic conditional insert.** Idempotency-key insertion is "insert if not exists, else return existing" within a transaction. Postgres `INSERT ... ON CONFLICT`, FoundationDB read-then-write inside a transaction (conflict-tracked), SQLite `INSERT OR IGNORE` all qualify.

**Backends in scope:**

- **Postgres** (v1 reference impl, `SERIALIZABLE` mode required)
- **SQLite** (v1 — for embedded testing, single-writer is trivially serializable)
- **CockroachDB** (v2 — for multi-region external consistency without changing protocol)
- **FoundationDB** (v2 — for very-high-throughput single-region)
- **stonedb** (v2 — dogfood the Shuozeli LSM-tree)

**Backends explicitly out:**

- Redis without Lua scripting (no SERIALIZABLE)
- DynamoDB without transactions API
- Plain key-value stores (no secondary indexes, no conditional inserts)
- Any eventually-consistent backend

**Conformance test suite.** A shared crate (`taskq-storage-conformance`) exercises every operation under the four required properties, including:

- Concurrent dispatcher pulls don't double-dispatch
- Reaper + worker `CompleteTask` race resolves cleanly (one wins, other gets `LEASE_EXPIRED`)
- Idempotency-key inserts under contention serialize correctly
- All backends produce identical results for the same operation sequence

Adding a backend = implementing the trait + passing the conformance suite. No CP changes needed.

## v1 scope

Ship the trait + Postgres backend + SQLite backend. SQLite makes the embedded-testing story trivial and validates the trait shape against a second implementation before v1 freezes. CockroachDB / FoundationDB / stonedb come later.

## Open questions

1. **Async trait flavor.** `async_trait` macro vs. native `async fn in trait` (stable in 1.75)? Native is preferable but constrains MSRV.
2. **Trait stability vs. evolvability.** Once v1 ships, adding a method breaks every external backend implementor. Options: default-method implementations (with sensible fallbacks), trait versioning (`StorageTxV2: StorageTxV1`), or just accept that this is a small ecosystem and we'll coordinate.
3. **Error model.** A unified `StorageError` enum vs. backend-specific errors wrapped in `Box<dyn Error>`. Lean: unified enum with categories (`SerializationConflict`, `ConstraintViolation`, `NotFound`, `BackendError(BoxedError)`).
4. **Read-only vs. read-write transactions.** Some backends optimize read-only paths. Worth distinguishing in the trait? Lean: not for v1; revisit if the optimization matters.
5. **Connection pooling.** Owned by the `Storage` impl, not exposed in the trait. Each backend handles pooling internally (Postgres via `deadpool-postgres`, SQLite single-connection per process).
6. **Schema migration story per backend.** Each backend ships its own migrations; the CP doesn't manage them. Backends must document their schema and provide migration tooling.
7. **Conformance test suite as a separate crate or inside the Storage trait crate?** Lean: separate. Backends depend on it as a dev-dependency.

## Forward references

- Problem #02 (crash consistency) defines the external-consistency property that conformance test #1 enforces.
- Problem #05 (dispatch fairness) defines the strategy framework whose `PickOrdering` variants the backend must translate.
- Problem #01 (lease expiration) drives the `record_heartbeat` and `list_expired_runtimes` shape.
- Problem #03 (idempotency) drives the `lookup_idempotency` and `delete_expired_dedup` shape.

## Post-debate refinement

The multi-agent design debate added two conformance requirements and two trait methods, and tightened the SQLite scope.

**Two new conformance requirements (now §8.2 #5 and #6 in `design.md`):**

- **Subscribe-pending ordering invariant.** Any transaction committing a row matching the subscription predicate strictly after `subscribe_pending` returns MUST cause at least one `WakeSignal` to be observable on the returned stream. Previously implicit; now explicit and testable in the conformance suite.
- **Bounded-cost dedup expiration.** `delete_expired_dedup` must run in time bounded by the batch size, not by the steady-state size of `idempotency_keys`. Backends with mutable per-row delete cost (Postgres without partitioning) MUST implement time-range partitioning of the dedup table so cleanup is `DROP PARTITION`. SQLite is exempted by virtue of its single-writer / dev-only scope.

**Skip-locking conformance (§8.2 #2) clarified:** "Vacuously satisfied by single-writer backends since there is nothing to skip; backends supporting concurrent writers must implement true skip-locking." This honest framing prevents the misimpression that SQLite "conforms" the same way Postgres does.

**Two new trait methods** for the heartbeat carve-out:

- `record_worker_heartbeat(worker_id, at)` — READ COMMITTED write to dedicated `worker_heartbeats` table.
- `extend_lease_lazy(lease, new_timeout)` — SERIALIZABLE write to `task_runtime`, called only when the heartbeat path detects the lease is within ε of expiry.

**SQLite scoping tightened (§8.3):** SQLite is single-process embedded / dev-only — multi-process or multi-replica CP deployments on SQLite are NOT supported. SQLite ships to validate trait shape and enable embedded testing, not as a production option.

## Round-2 refinement

Round 2 added two Postgres-specific implementation notes to §8.3 and dropped one trait surface:

- **BRIN index on `worker_heartbeats(last_heartbeat_at)`.** Reaper B's `WHERE last_heartbeat_at < NOW() - lease_window` query needed an indexing decision. BRIN was chosen over B-tree because it's cheap to maintain on append-mostly time-correlated data and does not defeat HOT updates the way B-tree on a frequently-updated column would. v1 single-replica heartbeat ceiling documented as ~10K/sec; hash-partitioning by `worker_id` is the next-step optimization beyond that, deferred to multi-replica defaults.
- **Hard 90-day TTL ceiling on idempotency partitions.** Daily granularity × 90 days = ~90 active partitions; planning time degrades superlinearly past a few hundred. The cap is enforced at `SetNamespaceQuota` validation.
- **Capacity-quota cache fence machinery dropped.** Round 1 added a `version` column to `namespace_quota` for cache fencing. Round 2 found this was over-engineering for v1's admit target (1K/sec/replica); capacity quotas are now read inline transactionally. The `version` column is removed from the schema. The trait method `check_capacity_quota` remains, just without cache plumbing above it.
