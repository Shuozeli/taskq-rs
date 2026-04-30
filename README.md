<!-- agent-updated: 2026-04-30T03:35:00Z -->

# taskq-rs

A pure-Rust distributed task queue with a standard gRPC contract. Schema is FlatBuffers (compiled with [`flatbuffers-rs`](https://github.com/Shuozeli/flatbuffers-rs)); transport is gRPC over [`pure-grpc-rs`](https://github.com/Shuozeli/pure-grpc-rs); idempotency keys are required and the contract is at-least-once with externally-consistent state transitions.

## Goals

1. **Standardize the contract, not the storage.** A single, versioned schema (FlatBuffers `.fbs`) served over gRPC that any caller, worker, or operator tool can speak. Storage backend is an implementation detail behind the protocol.

2. **Polyglot workers, Rust-first reference impls.** The protocol is the product. Workers in any language with a FlatBuffers + gRPC client must be able to participate. v1 ships a Rust control plane (built on `pure-grpc-rs`) and Rust worker SDK; other-language SDKs follow once the contract is stable.

3. **Pull-based, worker-controlled concurrency.** Workers long-poll and declare their own concurrency. The control plane never tracks worker addresses or pushes tasks.

4. **At-least-once with required idempotency keys.** Every `SubmitTask` carries an idempotency key (scoped `(namespace, key)`). At-least-once is the worker-execution contract; the key gives callers bounded duplicate-suppression at the submission layer.

5. **Multi-tenant by default.** Every task carries a namespace. Quotas, metrics, and rate limiting are namespace-scoped from day one.

6. **Externally consistent.** Every state transition is a serializable transaction; commit order matches real-time order. v1 reference implementation runs on Postgres `SERIALIZABLE` (single-node); the protocol does not preclude future CockroachDB / Spanner / FoundationDB backends for multi-region.

7. **Operable from the start.** Stats, worker listing, dead-letter replay, and quota changes are part of v1, not bolted on.

8. **Small surface, no workflow engine.** v1 is a task queue. Not a DAG engine, not a saga coordinator, not a cron scheduler.

9. **Boringly reliable.** Crashes, worker death mid-task, control-plane restarts, and network partitions are first-class scenarios — not edge cases discovered in production.

## Non-Goals (v1)

- Workflow orchestration (DAGs, dependencies, fan-out/join)
- Exactly-once delivery
- Cross-datacenter replication
- Web UI

## Status

Goals only. No design yet.
