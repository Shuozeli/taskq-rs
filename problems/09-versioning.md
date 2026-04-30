<!-- agent-updated: 2026-04-30T03:35:00Z -->

# Problem: Schema & protocol versioning

## The problem

Five distinct surfaces share the word "version" but evolve under different constraints:

1. **Wire schema** — FlatBuffers `.fbs` definitions for gRPC requests/responses.
2. **RPC method set** — methods on `TaskQueue`, `TaskWorker`, `TaskAdmin`.
3. **Storage schema** — per-backend tables, columns, indexes (different per backend, sometimes absent).
4. **Stored data** — namespace configs, error_class registries, retry policies, and tasks that persist across CP versions.
5. **Behavioral semantics** — what an RPC actually does, even when its wire shape is unchanged.

A single version number can't cover all five; each needs its own discipline. The protocol's long-term liveness depends on whether we set those disciplines correctly now or accumulate scar tissue we can't shed.

## What we already have

- FlatBuffers wire format, compiled by `flatbuffers-rs` (problem #12, README).
- `pure-grpc-rs` as the transport layer.
- Pluggable storage backends with schema-version trait method (#12).
- Compile-time linked strategies (#05).
- Per-namespace registered `error_class` set (#08).
- All state transitions are SERIALIZABLE (#02).

## Why hard

- **FlatBuffers has strict evolution rules.** Fields must be appended; reordering breaks reads; type changes are forbidden; removal corrupts older readers. Discipline must be enforced mechanically, not by convention.
- **Workers run in the wild.** Even though we ship only the Rust SDK, customer-operated worker fleets can't be synchronously upgraded with the CP. Long-tail-old-clients is the default, not the exception.
- **Storage migrations are backend-specific.** Postgres `ALTER TABLE`, SQLite `ALTER`, FoundationDB schemaless. Migration tooling per backend; CP doesn't impose a uniform mechanism.
- **Mid-rolling-upgrade state.** During CP rolling upgrade, half the replicas run v1.5 and half run v1.6. Any task or stored record produced by one must be readable by the other.
- **"Deprecated" almost never means "removed."** Once a field exists in a stable wire schema, it persists essentially forever — at least until a major version bump. That makes deprecation a labeling concept, not a runtime change.
- **Behavior change without wire change is invisible to compat checks.** A method that now interprets the same bytes differently is the most dangerous kind of update — it passes every schema-diff tool but breaks callers silently.

## The races and failure modes

**A. Skewed rolling upgrade.** Replica A on v1.6 writes a task with a new field; replica B on v1.5 reads it and ignores the field. Informational additions are safe; semantic additions are not. Mitigated by feature-flag gating behavioral changes.

**B. Worker downgrade.** Worker upgraded to v1.6 SDK and uses a v1.6-only feature; CP rolls back to v1.5 because of a bug. CP doesn't recognize the request. Mitigated by version handshake — server tells client its supported range.

**C. Storage schema mismatch.** CP v1.6 expects a new column; operator forgot to migrate. CP starts, queries fail at runtime. Mitigated by startup-time schema-version check; refuse to start with clear error.

**D. Strategy not present in binary.** Operator's namespace config selects a strategy that was excluded from this CP build (compile-time link). Should refuse to serve that namespace, log clearly, continue with others.

**E. Permanent field bloat.** Each minor version adds optional fields; over many releases the wire schema grows monotonically. Without an exit plan, v1 lives forever and accretes scar tissue. Mitigated by explicit major bumps when accumulated cruft is large enough to justify a clean redesign.

## What prior art does

| | Faktory | Hatchet | Temporal |
|---|---|---|---|
| Wire versioning | Custom TCP, `HELLO {"v":2}` handshake; server can refuse old | gRPC protobuf, field numbers give natural compat | Protobuf, multi-version SDK negotiation via headers |
| RPC additions | Manual coordination | Add new method, old clients don't call | Add new method; SDK declares server-version range |
| Storage migrations | None — Redis schemaless | SQL files in repo, manual run | Per-backend tooling |
| Behavioral versioning | Server-version-checked | Implicit | Explicit version flags on workflow definitions |
| Deprecation | Bump major | Implicit, leave in proto | Explicit deprecation in proto comments |

Temporal is the most disciplined model. We crib the posture (handshake, explicit deprecation, multi-version readiness) but skip the polyglot-SDK coordination — we ship only Rust.

## Direction

### SemVer for the protocol

`taskq.v1` is the first major. MINOR adds backwards-compatible features (optional fields, new RPCs); PATCH is bugfix. MAJOR breaks compat and requires coordinated upgrade.

**No public stability commitment.** We do not promise any specific lifetime for v1. Major bumps happen when we judge them necessary; SemVer labels what a bump means, not when one happens. Operators integrating with taskq-rs should track our release notes, not assume long-term stability.

### FlatBuffers schema discipline

- Append-only field additions; never reorder; never change types.
- Field removal: mark deprecated in the schema (FlatBuffers `deprecated` attribute), leave the field allocated, never reuse the slot. The stored data and the wire bytes remain valid.
- All evolution rules documented in `protocol/EVOLUTION.md` and enforced by a CI script that diffs `.fbs` files against the previous tag.

**Diff tooling.** `flatc` does not provide adequate schema-diff for compat enforcement. A custom diff tool is needed and should live in [`flatbuffers-rs`](https://github.com/Shuozeli/flatbuffers-rs) so any FlatBuffers project can reuse it. This is a coordinated dependency on a sister Shuozeli project; tracked as a v1 prerequisite.

### Wire-version handshake

First RPC from any client (caller or worker) carries `client_version: SemVer`; server responds with its `server_version` and `supported_client_range`. CP rejects clients with major-version mismatch using a typed error:

```
INCOMPATIBLE_VERSION
  server_version: SemVer
  supported_range: { min: SemVer, max: SemVer }
  hint: string
```

Within compatible majors, both sides ignore unknown fields per FlatBuffers semantics.

### Cross-language scope: Rust only

We ship the Rust worker SDK and Rust caller SDK. Other-language clients consuming the wire schema are out-of-scope for our release coordination — they self-manage version skew against the published `.fbs`. The protocol does not preclude them; we just don't release them.

This eliminates a whole category of multi-SDK release-coupling problems. Future polyglot SDKs may emerge but their compat story is theirs.

### Storage schema migrations

- Each backend ships ordered migration files in the CP repo (`storage/postgres/migrations/`, `storage/sqlite/migrations/`).
- A single `taskq_meta` table holds `(schema_version, format_version, last_migrated_at)` — one row, present in every backend that supports schema. Backends without schema (FoundationDB) store this in a known key.
- CP startup checks `schema_version`. If the binary expects newer than what's stored, refuse to start with a clear error pointing to the migration runbook. The CP process never auto-migrates in production.
- A `taskq-cp migrate` subcommand runs migrations explicitly. Operators run this as a deliberate ops action.
- For development, an `--auto-migrate` flag is available; not for production.

### Stored-data versioning

- Tasks include a `format_version` field. CP reads any format version it knows; writes only the current format.
- Namespace config and error_class registry have their own `format_version`. On read of an old version, CP upgrades in-place at next write.
- This decouples wire compatibility from storage compatibility — they evolve at different cadences.

### Rolling upgrades

- **Within a minor version**: required to work. New code must be backwards-readable by previous-minor replicas during rollout.
- **Across minor versions, same major**: required. New optional fields must default to old behavior when absent. Behavioral changes are gated on feature flags.
- **Across major versions**: not supported in-place. Operator drains the cluster, runs migrations, restarts on the new major. Document the runbook.

### Two-phase rollouts for behavioral changes

New behavior that affects task processing semantics is gated on a per-namespace feature flag, default off. Operators turn it on per-namespace once both code is deployed and they've verified rollout. Flag becomes default-on in the next minor; flag becomes mandatory in the major after that.

This pattern lets us deploy code first, then enable behavior, then remove the old path — three minor cycles minimum for any semantic change. The cost is real (lots of dead code in the interim) but the alternative — rip-and-replace deploys — is unacceptable for a system that holds task state.

### Strategy-mismatch handling

If a CP binary doesn't link a strategy referenced in a namespace's config, refuse to serve that namespace at startup, log a clear error (`Namespace "foo" requires dispatcher "WeightedFair" not present in this binary`), and continue serving other namespaces. Health endpoint reports degraded namespaces by name. Operators must either deploy a binary that includes the strategy or update the namespace config.

### Deprecation policy

- A field or method marked deprecated must remain functional for **at least two minor cycles** before removal in a major bump.
- Deprecation surfaces as a metric (`taskq_deprecated_field_used{field="..."}`) so operators can see who's still using it.
- Wire-schema deprecations follow FlatBuffers conventions: never remove the field slot, just stop reading/writing from new code.

### Registry evolution

Per-namespace `error_class` registries follow the same "never remove, only deprecate" rule as wire fields. Removing a class would break historical observability — old failure records carry the class string and queries against them would silently lose data. Registry edits are append-only; deprecated classes are marked but stay in the set indefinitely.

## Open questions

1. **What's the cadence target for minor releases?** Driven by feature volume, not calendar. Document in `RELEASE.md`; no commitment.
2. **`format_version` for tasks: bump on every change, or only when read-incompatible?** Lean: only when older code can't read newer records. Additions of fields with safe defaults don't bump format.
3. **How do feature flags get cleaned up?** Each flag has a target minor version for default-on and a target major for mandatory. Tracked in a `FLAGS.md` doc with a kill date for each. Otherwise flags accumulate.
4. **Migration ordering for multiple backends.** If a feature requires schema changes on Postgres and SQLite, do both migrations ship in the same CP release? Lean: yes, always — the feature is unavailable until all supported backends have migrated.
5. **Schema-diff tool ownership timeline.** Coordinated dependency on `flatbuffers-rs` adding a diff tool. If that lands later than v1.0, we ship without CI enforcement and rely on review discipline. Acceptable but risky.

## Forward references

- Problem #12 (storage abstraction) — owns the per-backend migration mechanism.
- Problem #08 (retry storms) — `error_class` registry follows the same never-remove rule.
- Problem #05 (dispatch fairness) — strategy mismatch handling.
- Problem #11 (observability) — owns the `taskq_deprecated_field_used` metric.
