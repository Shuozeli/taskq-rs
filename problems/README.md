# Problems

Problems we expect to face designing taskq-rs. One file per problem, written as we discuss.

A problem doc captures: what the problem is, why it's hard, what prior art does, and the open questions. Decisions made during discussion are captured in a "Direction" section at the bottom of the relevant problem doc, so they stay attached to the problem they answer. These will be promoted into `design.md` (not yet written) once we've thought through all the problems.

## Index

Problem numbers are stable IDs, not ordering. Categories below reflect logical grouping.

### Architecture (foundational; other problems reference these)

| # | Problem | Status |
|---|---------|--------|
| 12 | [Storage backend abstraction](12-storage-abstraction.md) | Written |

### Correctness (must get right)

| # | Problem | Status |
|---|---------|--------|
| 01 | [Lease acquisition & expiration](01-lease-expiration.md) | Written |
| 02 | [Crash consistency (control plane restart mid-dispatch)](02-crash-consistency.md) | Written — subsumes #04 |
| 03 | [Idempotency window race](03-idempotency.md) | Written |
| 04 | _(Folded into #02 — result delivery is just another serializable transition)_ | — |
| 05 | [Dispatch fairness & starvation](05-dispatch-fairness.md) | Written |
| 06 | [Backpressure at submit time](06-backpressure.md) | Written |
| 07 | [Long-poll connection limits](07-long-poll.md) | Written |

### Operability (need a story before v1 ships)

| # | Problem | Status |
|---|---------|--------|
| 08 | [Retry storms & poison pills](08-retry-storms.md) | Written |
| 09 | [Schema & protocol versioning](09-versioning.md) | Written |
| 10 | [Multi-tenant noisy neighbor (quota enforcement points)](10-noisy-neighbor.md) | Written |
| 11 | [Observability & trace propagation](11-observability.md) | Written |

### Deferred (tempting but v1-distractions)

- Worker autoscaling signals (Temporal-style poll-response hints)
- Cross-region replication
- Web UI for ops

## Discussion order

Suggested: 01 (done) → 02 (done) → 03 (done) → 05 (done) → 12 (done) → 06 (done) → 07 (done) → 08 (done) → 09 (done) → 10 (done) → 11 (done). All problems written.

Correctness first because design choices for fairness/backpressure/versioning are constrained by the consistency model we pick. Architecture (#12) was opened during the #05 discussion because pluggable strategies forced the storage abstraction question; it sits above Correctness in the index because everything else references it.
