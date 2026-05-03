//! Compose-driven E2E test harness.
//!
//! Each test creates a [`ComposeStack`] pointing at one of the
//! `docker/compose.*.yml` files. The stack:
//!
//! 1. Generates a random project name so parallel runs (or stale state
//!    from a prior run) can't collide.
//! 2. Runs `docker compose -p <project> -f <file> up --wait -d`. The
//!    `--wait` flag blocks until every service with a healthcheck is
//!    healthy, so tests don't need their own readiness loops.
//! 3. Exposes typed handles: connect to a CP via `caller_at()` /
//!    `worker_at()`, run admin operations via `exec_cli()`, force a
//!    crash via `kill_service()` / `restart_service()`.
//! 4. On `Drop`, runs `docker compose -p <project> down -v` so the
//!    Postgres volume is wiped and the project's containers are
//!    removed.
//!
//! The harness shells out to the local `docker` binary; tests `#[ignore]`
//! themselves so a default `cargo test` doesn't try to invoke Docker on
//! a developer machine that doesn't have it. CI runs with
//! `--include-ignored` and supplies Docker.

#![forbid(unsafe_code)]

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use grpc_client::Channel;
use http::Uri;
use taskq_caller_sdk::CallerClient;
use taskq_proto::task_admin_client::TaskAdminClient;
use taskq_proto::task_queue_client::TaskQueueClient;
use taskq_proto::task_worker_client::TaskWorkerClient;
use uuid::Uuid;

/// Absolute path to the `docker/` directory at the workspace root.
fn docker_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root is a parent of taskq-e2e-tests")
        .join("docker")
}

/// One of the canned compose files in `docker/`.
#[derive(Debug, Clone, Copy)]
pub enum ComposeFile {
    /// `compose.base.yml` — postgres + 1 cp.
    Base,
    /// `compose.multi-cp.yml` — postgres + 2 cp replicas.
    MultiCp,
}

impl ComposeFile {
    fn filename(self) -> &'static str {
        match self {
            ComposeFile::Base => "compose.base.yml",
            ComposeFile::MultiCp => "compose.multi-cp.yml",
        }
    }
}

/// Per-test compose stack. `Drop` tears it down with `docker compose down -v`.
pub struct ComposeStack {
    project: String,
    file: PathBuf,
    /// Host port the primary CP is reachable on. Tests using `MultiCp`
    /// should additionally use [`Self::secondary_cp_port`].
    cp_port: u16,
    secondary_cp_port: Option<u16>,
    pg_port: u16,
}

impl ComposeStack {
    /// Boot a fresh stack. Picks free host ports dynamically (so parallel
    /// test invocations and dev-machine port reservations don't collide)
    /// and waits for every healthcheck to go green.
    pub fn up(file: ComposeFile) -> Result<Self> {
        let project = format!("taskqe2e_{}", Uuid::now_v7().simple());
        let file_path = docker_dir().join(file.filename());

        let cp_port = pick_free_port()?;
        let pg_port = pick_free_port()?;
        let secondary_cp_port = match file {
            ComposeFile::Base => None,
            ComposeFile::MultiCp => Some(pick_free_port()?),
        };
        let secondary_health_port = match file {
            ComposeFile::Base => None,
            ComposeFile::MultiCp => Some(pick_free_port()?),
        };
        let health_port = pick_free_port()?;

        let stack = Self {
            project,
            file: file_path,
            cp_port,
            secondary_cp_port,
            pg_port,
        };

        let mut cmd = Command::new("docker");
        cmd.envs([
            ("TASKQ_E2E_CP_PORT", cp_port.to_string()),
            ("TASKQ_E2E_HEALTH_PORT", health_port.to_string()),
            ("TASKQ_E2E_PG_PORT", pg_port.to_string()),
        ]);
        if let Some(p) = secondary_cp_port {
            cmd.env("TASKQ_E2E_CPA_PORT", cp_port.to_string());
            cmd.env("TASKQ_E2E_CPB_PORT", p.to_string());
            cmd.env("TASKQ_E2E_HEALTHA_PORT", health_port.to_string());
            if let Some(hb) = secondary_health_port {
                cmd.env("TASKQ_E2E_HEALTHB_PORT", hb.to_string());
            }
        }
        cmd.args([
            "compose",
            "-p",
            &stack.project,
            "-f",
            stack.file.to_str().expect("path is utf-8"),
            "up",
            "-d",
            "--wait",
        ]);
        let status = cmd
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .context("spawn docker compose up")?;
        if !status.success() {
            bail!("docker compose up failed with {status}");
        }
        Ok(stack)
    }

    /// Primary CP gRPC port on the host.
    pub fn cp_port(&self) -> u16 {
        self.cp_port
    }

    /// Secondary CP gRPC port (only set on [`ComposeFile::MultiCp`]).
    pub fn secondary_cp_port(&self) -> Option<u16> {
        self.secondary_cp_port
    }

    /// Postgres port on the host.
    pub fn pg_port(&self) -> u16 {
        self.pg_port
    }

    /// libpq-style admin connection string for the Postgres in this stack.
    pub fn pg_admin_url(&self) -> String {
        format!(
            "host=127.0.0.1 port={} user=postgres password=postgres dbname=taskq",
            self.pg_port,
        )
    }

    /// `http://127.0.0.1:<cp_port>` URI for the primary CP.
    pub fn primary_endpoint(&self) -> Uri {
        format!("http://127.0.0.1:{}", self.cp_port)
            .parse()
            .expect("primary endpoint must parse")
    }

    /// `http://127.0.0.1:<secondary_cp_port>` URI for the second CP
    /// replica (multi-CP stacks only).
    pub fn secondary_endpoint(&self) -> Option<Uri> {
        self.secondary_cp_port.map(|p| {
            format!("http://127.0.0.1:{p}")
                .parse()
                .expect("secondary endpoint must parse")
        })
    }

    /// Connect a caller SDK to the primary CP.
    pub async fn caller(&self) -> Result<CallerClient> {
        self.caller_at(self.primary_endpoint()).await
    }

    /// Connect a caller SDK to a specific endpoint (for multi-replica
    /// scenarios where the test fans across replicas).
    pub async fn caller_at(&self, endpoint: Uri) -> Result<CallerClient> {
        let queue = TaskQueueClient::<Channel>::connect(endpoint)
            .await
            .map_err(|err| anyhow!("connect TaskQueueClient: {err}"))?;
        Ok(CallerClient::from_grpc_client(queue))
    }

    /// Raw `TaskQueueClient` (caller side) on the given endpoint.
    pub async fn raw_queue_client(&self, endpoint: Uri) -> Result<TaskQueueClient<Channel>> {
        TaskQueueClient::<Channel>::connect(endpoint)
            .await
            .map_err(|err| anyhow!("connect TaskQueueClient: {err}"))
    }

    /// Raw `TaskWorkerClient` on the given endpoint.
    pub async fn raw_worker_client(&self, endpoint: Uri) -> Result<TaskWorkerClient<Channel>> {
        TaskWorkerClient::<Channel>::connect(endpoint)
            .await
            .map_err(|err| anyhow!("connect TaskWorkerClient: {err}"))
    }

    /// Raw `TaskAdminClient` on the given endpoint.
    pub async fn raw_admin_client(&self, endpoint: Uri) -> Result<TaskAdminClient<Channel>> {
        TaskAdminClient::<Channel>::connect(endpoint)
            .await
            .map_err(|err| anyhow!("connect TaskAdminClient: {err}"))
    }

    /// Invoke the `taskq-cli` binary inside the named compose service
    /// (`cp`, `cp-a`, `cp-b`). Returns combined stdout+stderr on success.
    pub fn exec_cli(&self, service: &str, args: &[&str]) -> Result<String> {
        let mut full = vec![
            "compose",
            "-p",
            &self.project,
            "-f",
            self.file.to_str().expect("path is utf-8"),
            "exec",
            "-T",
            service,
            "/usr/local/bin/taskq-cli",
            "--endpoint",
            "http://127.0.0.1:50051",
        ];
        full.extend_from_slice(args);
        let out = Command::new("docker")
            .args(&full)
            .output()
            .context("spawn docker compose exec")?;
        if !out.status.success() {
            bail!(
                "taskq-cli {args:?} failed (status={}): stdout={} stderr={}",
                out.status,
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr),
            );
        }
        let mut combined = String::from_utf8_lossy(&out.stdout).into_owned();
        combined.push_str(&String::from_utf8_lossy(&out.stderr));
        Ok(combined)
    }

    /// Hard-kill a named service (`docker compose kill -s SIGKILL`).
    /// Use this to simulate a crash that bypasses graceful shutdown.
    pub fn kill_service(&self, service: &str) -> Result<()> {
        self.run_compose(&["kill", "-s", "SIGKILL", service])
    }

    /// Stop a service gracefully (`docker compose stop`).
    pub fn stop_service(&self, service: &str) -> Result<()> {
        self.run_compose(&["stop", service])
    }

    /// Bring a previously-stopped service back up. Polls Docker for
    /// the service's healthy status before returning. Avoids
    /// `docker compose up --wait` because that re-binds host ports
    /// from the compose file's defaults rather than the per-stack
    /// env-var overrides set in `up()`.
    pub fn start_service(&self, service: &str) -> Result<()> {
        self.run_compose(&["start", service])?;
        // Poll `docker compose ps --format json` for the service to
        // become healthy. ~30s budget; covers normal restart timing.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let out = Command::new("docker")
                .args([
                    "compose",
                    "-p",
                    &self.project,
                    "-f",
                    self.file.to_str().expect("path is utf-8"),
                    "ps",
                    "--format",
                    "{{.Service}}\t{{.Health}}",
                ])
                .output()
                .context("docker compose ps")?;
            let stdout = String::from_utf8_lossy(&out.stdout);
            let healthy = stdout
                .lines()
                .filter(|l| l.starts_with(service))
                .any(|l| l.contains("healthy"));
            if healthy {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                bail!("service {service} did not become healthy within 60s; ps:\n{stdout}");
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    /// Run a libpq SQL statement against the stack's Postgres as the
    /// admin user. Useful for forcing schema-version downgrades and
    /// other tests that need to reach behind the CP's back.
    pub async fn pg_exec(&self, sql: &str) -> Result<()> {
        let (client, conn) = tokio_postgres::connect(&self.pg_admin_url(), tokio_postgres::NoTls)
            .await
            .with_context(|| format!("connect to pg at {}", self.pg_admin_url()))?;
        let handle = tokio::spawn(async move {
            let _ = conn.await;
        });
        let result = client
            .batch_execute(sql)
            .await
            .with_context(|| format!("exec SQL: {sql}"));
        handle.abort();
        result
    }

    /// Seed a namespace by direct SQL insert into `namespace_quota`.
    ///
    /// `taskq-cli namespace create` exercises the same admin path and
    /// works equivalently — the SQL seed is just a per-test perf shave
    /// (no docker exec round-trip) for tests that don't care to
    /// validate the CLI specifically. Tests like `e05_cli_golden`
    /// exercise the wire path on purpose.
    pub async fn seed_namespace(&self, ns: &str) -> Result<()> {
        let template = "INSERT INTO namespace_quota \
             SELECT $$<NS>$$ AS namespace, admitter_kind, admitter_params, \
                    dispatcher_kind, dispatcher_params, max_pending, max_inflight, \
                    max_workers, max_waiters_per_replica, max_submit_rpm, \
                    max_dispatch_rpm, max_replay_per_second, max_retries_ceiling, \
                    max_idempotency_ttl_seconds, max_payload_bytes, max_details_bytes, \
                    min_heartbeat_interval_seconds, lazy_extension_threshold_seconds, \
                    max_error_classes, max_task_types, trace_sampling_ratio, \
                    log_level_override, audit_log_retention_days, \
                    metrics_export_enabled, false AS disabled \
             FROM namespace_quota WHERE namespace = 'system_default' \
             ON CONFLICT (namespace) DO NOTHING";
        let sql = template.replace("<NS>", ns);
        self.pg_exec(&sql).await
    }

    /// Run a one-off `taskq-cp` container against this stack (sharing
    /// the postgres). `cmd_args` replaces the image's CMD (the ENTRYPOINT
    /// is `/usr/local/bin/taskq-cp`). Returns (exit_code, stdout+stderr).
    /// Useful for the schema-mismatch test where we want to invoke `cp
    /// serve` WITHOUT `--auto-migrate` and observe the failure.
    pub fn run_cp_oneoff(&self, service: &str, cmd_args: &[&str]) -> Result<(i32, String)> {
        let mut full: Vec<&str> = vec![
            "compose",
            "-p",
            &self.project,
            "-f",
            self.file.to_str().expect("path is utf-8"),
            "run",
            "--rm",
            "--no-deps",
            service,
        ];
        full.extend_from_slice(cmd_args);
        let out = Command::new("docker")
            .args(&full)
            .output()
            .context("spawn docker compose run")?;
        let code = out.status.code().unwrap_or(-1);
        let mut combined = String::from_utf8_lossy(&out.stdout).into_owned();
        combined.push_str(&String::from_utf8_lossy(&out.stderr));
        Ok((code, combined))
    }

    /// Run an arbitrary `docker compose <args...>` against this stack's
    /// project + file.
    fn run_compose(&self, args: &[&str]) -> Result<()> {
        let mut full: Vec<&str> = vec![
            "compose",
            "-p",
            &self.project,
            "-f",
            self.file.to_str().expect("path is utf-8"),
        ];
        full.extend_from_slice(args);
        let status = Command::new("docker")
            .args(&full)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .context("spawn docker compose")?;
        if !status.success() {
            bail!("docker compose {args:?} failed with {status}");
        }
        Ok(())
    }
}

impl Drop for ComposeStack {
    fn drop(&mut self) {
        let _ = self.run_compose(&["down", "-v", "--remove-orphans"]);
    }
}

/// Wait up to `total` for `f` to return `Ok(Some(_))`. Returns the
/// resolved value or `Err` if the deadline elapses. The poll cadence is
/// `step` (default 250ms).
pub async fn wait_for<T, F, Fut>(total: Duration, step: Duration, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>>>,
{
    let deadline = std::time::Instant::now() + total;
    loop {
        if let Some(value) = f().await? {
            return Ok(value);
        }
        if std::time::Instant::now() >= deadline {
            bail!("wait_for timed out after {:?}", total);
        }
        tokio::time::sleep(step).await;
    }
}

/// Bind to `127.0.0.1:0` to let the kernel pick a free port, then drop
/// the listener and return the port number. Race-y in theory but the
/// 64k ephemeral range plus per-test compose project names mean
/// collisions are vanishingly rare in practice.
fn pick_free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Sanity helper: assert the workspace has a built docker image with the
/// given tag. Intended for compose's `build:` step which is a no-op when
/// the image exists.
pub fn require_docker() -> Result<()> {
    let out = Command::new("docker").arg("--version").output();
    match out {
        Ok(o) if o.status.success() => Ok(()),
        Ok(o) => bail!(
            "docker not usable: status={} stderr={}",
            o.status,
            String::from_utf8_lossy(&o.stderr)
        ),
        Err(err) => bail!("docker binary not found on PATH: {err}"),
    }
}
