<!-- agent-updated: 2026-05-02T21:35:28Z -->

# Schema and Protocol Evolution

Reference document for how the wire schema, the RPC method set, the storage
schema, the stored data, and the per-namespace registries evolve over time
inside the `taskq.v1` major. Companion to [`design.md`](../design.md) §3.1,
§3.3, §12 and to [`problems/09-versioning.md`](../problems/09-versioning.md).

This document is reference material for contributors writing schema diffs,
adding RPC methods, and shipping storage migrations. The runbook for
*operating* a deployed cluster across upgrades lives in
[`OPERATIONS.md`](../OPERATIONS.md).

---

## 1. Surfaces and disciplines

Five surfaces share the word "version" and each has its own rule. The matrix
below is the canonical summary; the rest of this document expands each row.

| Surface | Tracked in | Rule |
|---|---|---|
| Wire schema (FlatBuffers `.fbs`) | `taskq-proto/schema/*.fbs` | Append-only fields; never reorder; never change types; deprecate via attribute |
| RPC method set | `taskq-proto/schema/task_*.fbs` `service` decls | Append new methods; never rename; never change signatures |
| Storage schema | `taskq-storage-{postgres,sqlite}/migrations/*.sql` | Numbered ordered files; CP refuses to start on mismatch |
| Stored data | `tasks.format_version`, `taskq_meta.format_version` | Reader tolerates older formats; writer emits current |
| Per-namespace registries | `error_class_registry`, `task_type_registry` | Append-only; deprecation flips `deprecated`; never remove |

External consistency for state transitions ([`design.md`](../design.md) §1)
constrains all evolution: any change that could be observed differently by
two CP replicas mid-upgrade is gated on the two-phase rollout pattern in
§7 below.

---

## 2. FlatBuffers schema discipline

### 2.1 Append-only fields

FlatBuffers identifies fields by ordinal slot. Adding a field at the end of a
table is binary-compatible with old readers (they ignore it) and old writers
(they don't emit it; the new reader sees the field's default).

```fbs
// taskq.v1.SubmitTaskRequest, before
table SubmitTaskRequest {
  namespace: string;
  task_type: string;
  payload:   [ubyte];
  idempotency_key: string;
  traceparent: [ubyte];
}

// After: append max_retries_override, do not insert.
table SubmitTaskRequest {
  namespace: string;
  task_type: string;
  payload:   [ubyte];
  idempotency_key: string;
  traceparent: [ubyte];
  max_retries_override: uint32 = 0;   // appended at end, default-zero
}
```

The new field MUST come last. Inserting it between `task_type` and `payload`
shifts every subsequent slot and silently corrupts every reader on the old
schema.

### 2.2 Never reorder, never retype

These two operations are forbidden, period:

- Reordering existing fields. Slots are positional in the wire bytes.
- Changing a field's type. `uint32` to `uint64` *looks* harmless and breaks
  every reader that decodes 4 bytes where 8 are now written.

If a field needs a new type, append a new field with a new name and deprecate
the old one. Per §2.3.

### 2.3 Deprecation

Mark retired fields with FlatBuffers' `(deprecated)` attribute. The field
slot stays allocated; old wire bytes carrying that field still parse; new
code refuses to read or write the field.

```fbs
table SubmitTaskRequest {
  namespace: string;
  task_type: string;
  payload:   [ubyte];
  idempotency_key: string;
  traceparent: [ubyte];
  max_retries_override: uint32 = 0 (deprecated);   // do not read or write
  retry_policy_override: RetryPolicy;              // replacement
}
```

Deprecated fields stay in the schema indefinitely — at minimum across the
two-minor-cycle deprecation window from [`design.md`](../design.md) §12.9 —
and never have their slot reused. Operators see lingering usage via the
`taskq_deprecated_field_used_total{field}` counter
([`design.md`](../design.md) §11.3).

### 2.4 Diff tooling

`flatc` does not provide adequate schema-diff for compatibility enforcement.
A custom diff tool lives in [`flatbuffers-rs`](https://github.com/Shuozeli/flatbuffers-rs)
([`design.md`](../design.md) §12.2) and is invoked from the `taskq-rs` CI
workflow on every PR that touches `taskq-proto/schema/`. It diffs the new
`.fbs` files against the previous tag and rejects:

- Field additions that are not at the end of the table
- Field removals (use deprecation instead)
- Type changes
- Enum value renumbering or removal
- `union` member removal or reordering

Until that tool lands upstream the rule is enforced by review discipline,
per [`tasks.md`](../tasks.md) coordinated dependency note.

---

## 3. RPC method set

The three services (`TaskQueue`, `TaskWorker`, `TaskAdmin`) are open for
appending methods. Adding a method is binary-compatible — old clients never
call it, new clients can. Removing or renaming a method is a major version
bump.

### 3.1 Adding a method

```fbs
// task_admin.fbs, before
service TaskAdmin {
  SetNamespaceQuota(SetNamespaceQuotaRequest):SetNamespaceQuotaResponse;
  GetNamespaceQuota(GetNamespaceQuotaRequest):GetNamespaceQuotaResponse;
  // ...
}

// After: add ListNamespaces. Existing methods unchanged.
service TaskAdmin {
  SetNamespaceQuota(SetNamespaceQuotaRequest):SetNamespaceQuotaResponse;
  GetNamespaceQuota(GetNamespaceQuotaRequest):GetNamespaceQuotaResponse;
  // ...
  ListNamespaces(ListNamespacesRequest):ListNamespacesResponse;
}
```

Old `taskq-cli` builds keep working; they simply don't surface
`list-namespaces`. New CLI builds against an old CP receive `UNIMPLEMENTED`
from `pure-grpc-rs` and surface a clear "server too old" error.

### 3.2 Changing a method's signature

Forbidden. The method name + request/response message names is the contract.
If the request needs a new optional field, append it on the request message
per §2.1. If the response shape needs to change in a way that old callers
cannot tolerate, add a new method with a new name and deprecate the old.

---

## 4. Wire-version handshake

Every first RPC from a client (caller or worker) carries
`client_version: SemVer`. The server returns its own `server_version` plus
`supported_client_range: { min: SemVer, max: SemVer }`. Within the supported
range both sides ignore unknown fields per FlatBuffers semantics.

A major-version mismatch returns the typed error:

```
INCOMPATIBLE_VERSION
  server_version:  SemVer
  supported_range: { min: SemVer, max: SemVer }
  hint:            string    // human-readable upgrade pointer
```

The SDK surfaces this as a fatal initialization error — there is no retry
that fixes a major-version skew. Operators upgrading must coordinate the
client fleet against the deployed CP.

Within a major (`v1.x`), the handshake is informational: it lets new SDKs
gate features against the server's reported version, and lets the
`taskq_deprecated_field_used_total` counter be enriched with the client
version that emitted the field.

---

## 5. Migration cadence: when to bump `taskq.v1` to `v2`

`v2` is a deliberate decision, not an incremental drift. Bump only when:

- An append-only fix is impossible. E.g., the protocol requires a *removal*
  of a method or message, not just deprecation.
- Behavioral semantics change in a way the two-phase feature-flag rollout
  pattern (§7) cannot model. E.g., the wire interpretation of a field
  changes inconsistently across replicas.
- Accumulated cruft from many minor cycles justifies a clean redesign.
  Subjective; this is the "stop accreting scar tissue" exit valve from
  [`problems/09`](../problems/09-versioning.md).

Until then, every change must be expressible as one of:

- Append a new field (§2.1)
- Deprecate a field (§2.3)
- Append a new RPC method (§3.1)
- Add a storage migration (§6)
- Bump a stored-data `format_version` and teach the reader the new shape (§8)
- Append to a per-namespace registry (§9)

If none of these works, that's the v2 trigger.

There is **no public stability commitment** for v1 lifetime
([`design.md`](../design.md) §12.1). Operators integrating with `taskq-rs`
should track release notes, not assume a calendar.

---

## 6. Storage schema migrations

Each backend ships ordered SQL migration files in the CP repo:

```
taskq-storage-postgres/migrations/0001_initial.sql
taskq-storage-postgres/migrations/0002_phase5c_admin.sql
taskq-storage-sqlite/migrations/0001_initial.sql
taskq-storage-sqlite/migrations/0002_phase5c_admin.sql
```

A new migration is a new numbered file. Files are append-only; never edit a
shipped migration in place. If a previous migration was wrong, ship a new
migration that corrects it.

### 6.1 Schema versions in `taskq_meta`

Every backend stores its current `schema_version` and `format_version` in a
single-row `taskq_meta` table ([`design.md`](../design.md) §4):

```sql
CREATE TABLE taskq_meta (
    only_row         boolean PRIMARY KEY DEFAULT TRUE,
    schema_version   integer NOT NULL,
    format_version   integer NOT NULL,
    last_migrated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT taskq_meta_singleton CHECK (only_row = TRUE)
);
```

Each migration file ends with an `UPDATE taskq_meta SET schema_version = N`
to advance the marker. Backends without a SQL schema (FoundationDB, future
KV stores) store the same fields under a known key.

### 6.2 The `taskq-cp migrate` runner

`taskq-cp migrate` is the canonical entry point. It:

1. Connects to the configured `storage_backend`.
2. Reads the current `schema_version` from `taskq_meta`.
3. Runs every migration file with a higher version number, in order, each in
   its own transaction.
4. Updates `taskq_meta.last_migrated_at` after each file.

Operators run this as a deliberate ops action before deploying a CP that
requires a newer schema.

### 6.3 Refuse-to-start on mismatch

CP startup checks `taskq_meta.schema_version` against the binary's expected
version. On mismatch the process refuses to start with a clear error
pointing at the migration runbook.

This is the safe default. Auto-migrating in production hides operator
mistakes — a CP that silently runs `ALTER TABLE` during pod startup turns a
clean rolling upgrade into a flapping deployment. The `--auto-migrate` flag
on `taskq-cp serve` overrides the gate for development; production must run
`taskq-cp migrate` explicitly.

### 6.4 Adding a storage migration: example

A new feature requires a `task_priority_class` column on `tasks`:

1. Author `taskq-storage-postgres/migrations/0003_priority_class.sql`:

   ```sql
   ALTER TABLE tasks
       ADD COLUMN IF NOT EXISTS priority_class text NULL;

   CREATE INDEX IF NOT EXISTS tasks_priority_class_ix
       ON tasks (namespace, priority_class)
       WHERE priority_class IS NOT NULL;

   UPDATE taskq_meta
       SET schema_version = 3, last_migrated_at = NOW()
       WHERE only_row = TRUE;
   ```

2. Author the matching `taskq-storage-sqlite/migrations/0003_priority_class.sql`
   with SQLite-flavored DDL. **Both backends ship in the same release** — see
   [`problems/09`](../problems/09-versioning.md) Open Q4: features are
   unavailable until every supported backend has migrated.

3. Bump `SCHEMA_VERSION` in `taskq-cp` so the startup gate trips for any
   operator running the new binary against an un-migrated database.

4. Update [`OPERATIONS.md`](../OPERATIONS.md) with the migration note if
   the change has operational consequences (downtime window, vacuum cost,
   etc.).

---

## 7. Two-phase rollouts for behavioral changes

Wire-compatible behavioral changes (the "behavior change without wire
change" failure mode from [`problems/09`](../problems/09-versioning.md))
must be gated behind a per-namespace feature flag. The pattern from
[`design.md`](../design.md) §12.8:

1. **Minor `N`** — Ship the new behavior behind a flag, default off.
   Operators turn it on per-namespace once they verify rollout.
2. **Minor `N+1`** — Flag becomes default on. Operators who want the old
   behavior must explicitly disable it per-namespace.
3. **Major `N+M`** — Flag is removed; new behavior is mandatory.

Three-minor-cycle deprecation path minimum. The cost is real (lots of dead
code in the interim) but the alternative — rip-and-replace deploys — is
unacceptable for a system that holds task state across hours.

Each flag has a documented kill-date target tracked in `FLAGS.md` (created
on first use). Flags without a kill-date target accumulate scar tissue;
reviewers should reject new flags that don't declare one.

---

## 8. Stored-data versioning

The `tasks.format_version` column ([`design.md`](../design.md) §4) tracks
the format of the row's payload-adjacent fields (retry config, trace
context layout, future per-task metadata). The CP reads any supported
format version; it always writes the current version.

```rust
match task.format_version {
    1 => decode_v1(&task),
    2 => decode_v2(&task),
    other => return Err(StorageError::Unsupported(format!(
        "tasks.format_version={other} is newer than this binary supports"
    ))),
}
```

Bump `format_version` only when older code cannot read newer rows. Adding a
field with a safe default does not bump the format ([`problems/09`](../problems/09-versioning.md)
Open Q2). The `taskq_meta.format_version` column tracks the data format
the binary writes; older readers refuse to start when this is newer than
they understand.

Namespace config and the registry tables (`namespace_quota`,
`error_class_registry`, `task_type_registry`) carry their own format
markers and upgrade in place at next write.

---

## 9. `error_class_registry` evolution

Per-namespace error classes are append-only. The same rule as wire schema
deprecation applies: classes can be marked deprecated via the `deprecated`
boolean but never removed. Removing a class would break historical
observability — old `task_results` rows carry the class string and queries
against them would silently lose data.

### 9.1 Adding a class

```rust
// SetNamespaceConfig admin RPC, conceptually:
add_error_classes(namespace, &["network.timeout", "auth.expired"]);
```

The CP rejects writes that would push the registry past
`namespace.max_error_classes` (default 64;
[`design.md`](../design.md) §11.3). The cardinality cap is the load-bearing
defense against metric blow-up from `taskq_error_class_total`.

### 9.2 Deprecating a class

```rust
deprecate_error_class(namespace, "network.timeout");
```

The row stays. New `ReportFailure` calls naming a deprecated class still
succeed — the class is registered, just discouraged — but operators can
filter the metric series and migrate workers to a replacement class.

### 9.3 Cardinality budget interaction

Each new class consumes one slot against the namespace's
`max_error_classes` cap. When the cap is reached, `SetNamespaceConfig`
rejects the write with a clear error (see
[`QUOTAS.md`](./QUOTAS.md)). Operators either bump the cap (subject to
overall metric budget) or deprecate an existing class.

The same model applies to `task_type_registry` against `max_task_types`
(default 32).

---

## 10. Worked examples

### 10.1 Add an optional field to `SubmitTaskRequest`

Goal: let callers tag a task with a `priority_class` string for downstream
analytics.

1. Edit `taskq-proto/schema/task_queue.fbs`:

   ```fbs
   table SubmitTaskRequest {
     namespace: string;
     task_type: string;
     payload:   [ubyte];
     idempotency_key: string;
     traceparent: [ubyte];
     // ... existing fields ...
     priority_class: string;   // appended at end
   }
   ```

2. Run `cargo build -p taskq-proto`. The codegen picks up the new field;
   no other crate needs to change yet.

3. CP reads the new field optionally; missing values default to empty
   string and are treated as "no class". No storage migration needed if
   the value is just persisted on the existing `tasks.payload` blob; if
   the value should be queryable, add a storage migration per §6.4.

4. CI runs the FlatBuffers diff tool; the new field passes because it's
   appended at the end with a default value.

### 10.2 Deprecate a field

Goal: retire `SubmitTaskRequest.max_retries_override` in favor of a richer
`retry_policy_override: RetryPolicy`.

1. Add `retry_policy_override` per the §10.1 pattern.

2. Edit `task_queue.fbs` to mark the old field deprecated:

   ```fbs
   max_retries_override: uint32 = 0 (deprecated);
   ```

3. CP code stops reading `max_retries_override`. Callers passing it now hit
   `taskq_deprecated_field_used_total{field="max_retries_override"}`.

4. Track the kill-date target in `FLAGS.md` (or `EVOLUTION.md` if there's
   no behavioral flag) — at minimum two minor cycles before the slot can
   be relabeled `(deprecated, never-reuse)` in v2.

### 10.3 Add a new RPC method

Goal: add `ListNamespaces` to `TaskAdmin`.

1. Edit `taskq-proto/schema/task_admin.fbs` to declare the new method per
   §3.1, plus the new request/response messages.

2. Implement the handler in `taskq-cp/src/handlers/task_admin.rs`.

3. Add a `taskq-cli` subcommand wrapping the new RPC.

4. Old `taskq-cli` builds keep working; old `taskq-cp` builds receive
   `UNIMPLEMENTED` and the CLI surfaces a clear error.

### 10.4 Add a storage migration

See §6.4 for the worked example with `priority_class`. Key points:

- Both Postgres and SQLite migration files ship in the same release.
- Each migration ends with `UPDATE taskq_meta SET schema_version = N`.
- The CP's `SCHEMA_VERSION` constant bumps in lockstep.
- `taskq-cp migrate` runs the migration; `taskq-cp serve` refuses to start
  until the migration has run (unless `--auto-migrate` is passed in dev).

---

## 11. References

- [`design.md`](../design.md) §3 (wire), §11.4 (audit), §12 (versioning)
- [`problems/09-versioning.md`](../problems/09-versioning.md) — full
  problem statement, prior-art comparison, debate verdicts
- [`OPERATIONS.md`](../OPERATIONS.md) — operator-facing migration runbook
- [`QUOTAS.md`](./QUOTAS.md) — cardinality caps and validation rules that
  bound the registry growth path
- [`tasks.md`](../tasks.md) — coordinated dependencies (FlatBuffers diff
  tool)
