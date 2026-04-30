<!-- agent-updated: 2026-04-30T03:35:00Z -->

# Contributing to taskq-rs

Thanks for your interest in contributing. taskq-rs is a pure-Rust distributed
task queue with a standardized FlatBuffers-over-gRPC contract. This guide
covers what you need to build, test, and ship a change.

Before working on anything substantive, read [`README.md`](README.md),
[`architecture.md`](architecture.md), and the relevant sections of
[`design.md`](design.md). Open work items are tracked in
[`tasks.md`](tasks.md). The discussion behind each design decision lives in
[`problems/`](problems/).

## Build

```bash
cargo build --workspace
```

The workspace pins a stable Rust toolchain via `rust-toolchain.toml`
(>= 1.75, for native `async fn in trait`). `rustup` will pick it up
automatically.

## Test

```bash
cargo test --workspace
```

Tests follow the **Arrange / Act / Assert** pattern. Each test must:

- Mark the three phases with `// Arrange`, `// Act`, `// Assert` comments.
- Exercise exactly one behavior per test (one Act per test).
- Use a descriptive name that names the behavior, not the function under test
  (`test_session_returns_none_for_nonexistent_id`, not `test_get_session`).
- Be deterministic and isolated — no shared mutable state, no time or random
  dependencies.

Phase 0 scaffolding ships without tests; tests land alongside their crates
starting in Phase 2 (`taskq-storage`).

## CI requirements

Every PR must pass GitHub Actions before merge. The CI workflow runs:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --no-run` (compile-only until tests are written)
- `cargo build --workspace --release`

Run those commands locally before pushing; CI is a backstop, not a substitute
for local verification.

## Pre-commit hooks

Install [`pre-commit`](https://pre-commit.com/) and enable the hooks:

```bash
pre-commit install
```

The hooks enforce `cargo fmt`, `cargo clippy`, trailing-whitespace cleanup,
and end-of-file fixes. They mirror the CI gates so you find problems before
pushing.

## Adding a new crate to the workspace

1. Create the crate directory at the workspace root: `mkdir taskq-<name>`.
2. Add a `Cargo.toml` with `edition = "2021"`, `version = "0.0.0"`,
   `license = "MIT"`, a one-line `description`, and (until publishing)
   `publish = false`.
3. Create `src/lib.rs` (or `src/main.rs` for binaries) with a single
   crate-level doc comment describing the crate's role. No code beyond what
   is needed to compile.
4. Append the crate to `members` in the root `Cargo.toml`.
5. Run `cargo build --workspace` and `cargo clippy --workspace --all-targets
   -- -D warnings` — both must succeed with zero warnings.
6. Update [`architecture.md`](architecture.md) if the crate adds a new seam
   or boundary, and [`tasks.md`](tasks.md) if it lands a tracked Phase item.

## Cardinality review checklist (metrics)

When adding a metric (counter, gauge, or histogram) — particularly during
Phase 6 work — review [`design.md`](design.md) §11.3 and verify:

- [ ] Total label cardinality stays under the namespace cardinality budget
      (`max_metric_label_cardinality`, default 1000 per namespace).
- [ ] Histograms drop high-cardinality dimensions (specifically `task_type`,
      per the round-2 verdict in §11.3). Counters may keep `task_type` but
      only if the namespace operator has provisioned for the cardinality.
- [ ] No unbounded labels (free-form strings, error messages, request IDs).
- [ ] The metric appears in the standard set listed in §11.3 *or* an ADR
      explains why a new metric is needed.

Build-time cardinality lint is deferred for v1; this checklist is the
substitute (per [`tasks.md`](tasks.md) Phase 6).

## Pull request process

1. **Reference the design.** Every PR description must cite the
   [`design.md`](design.md) section it implements (e.g., "implements §6.2").
   PRs that touch the wire schema must also cite the relevant
   [`problems/`](problems/) doc.
2. **Keep PRs small.** Phase boundaries in [`tasks.md`](tasks.md) are good
   commit boundaries. Don't fold Phase 3 work into a Phase 2 PR.
3. **Move task items to the Done log.** When a [`tasks.md`](tasks.md) item
   lands, move its bullet to the bottom "Done" section with a date and
   commit hash.
4. **Don't bypass clippy.** No `#[allow(clippy::...)]`. Fix the underlying
   code instead. (See `.claude/rules/shared/common/rust-quality.md`.)
5. **Follow Conventional Commits** for the commit subject when feasible
   (`feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`). Body should
   explain the *why*, not the *what*.

## Code style

- No emojis in code, comments, or commit messages.
- No `unwrap()` outside tests and documented invariants. Propagate errors
  with `Result`.
- Encode invariants in the type system (newtypes, enums, `NonZero*`) rather
  than runtime checks.
- Prefer composition over inheritance.
- Storage interactions are always within a transaction — including reads.

See `.claude/rules/shared/common/` for the full rule set.
