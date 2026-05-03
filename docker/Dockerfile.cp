# Two-stage build for taskq-cp + taskq-cli.
#
# Stage 1 (builder): vanilla `cargo build --release` of just the two
# binaries we ship. Stage 2: `debian:bookworm-slim` with ca-certificates
# and the two binaries copied in. Same image runs `taskq-cp` (default
# entrypoint) or `taskq-cli` (override entrypoint).
#
# The image expects the CP TOML config to be mounted at
# `/etc/taskq/cp.toml`. The compose files in `docker/` do this.

FROM rust:1-slim AS builder

RUN apt-get update \
 && apt-get install -y --no-install-recommends pkg-config libssl-dev git \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cargo cache priming: copy lockable manifests first so dependency
# compilation caches across source-only changes.
COPY Cargo.toml ./
COPY taskq-proto/Cargo.toml             taskq-proto/Cargo.toml
COPY taskq-storage/Cargo.toml           taskq-storage/Cargo.toml
COPY taskq-storage-conformance/Cargo.toml taskq-storage-conformance/Cargo.toml
COPY taskq-storage-postgres/Cargo.toml  taskq-storage-postgres/Cargo.toml
COPY taskq-storage-sqlite/Cargo.toml    taskq-storage-sqlite/Cargo.toml
COPY taskq-cp/Cargo.toml                taskq-cp/Cargo.toml
COPY taskq-caller-sdk/Cargo.toml        taskq-caller-sdk/Cargo.toml
COPY taskq-worker-sdk/Cargo.toml        taskq-worker-sdk/Cargo.toml
COPY taskq-cli/Cargo.toml               taskq-cli/Cargo.toml
COPY taskq-integration-tests/Cargo.toml taskq-integration-tests/Cargo.toml
COPY taskq-e2e-tests/Cargo.toml         taskq-e2e-tests/Cargo.toml

# Real source tree.
COPY . .

RUN cargo build --release -p taskq-cp -p taskq-cli

FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/taskq-cp  /usr/local/bin/taskq-cp
COPY --from=builder /build/target/release/taskq-cli /usr/local/bin/taskq-cli

EXPOSE 50051 9090

ENTRYPOINT ["/usr/local/bin/taskq-cp"]
CMD ["--config", "/etc/taskq/cp.toml", "--auto-migrate", "serve"]
