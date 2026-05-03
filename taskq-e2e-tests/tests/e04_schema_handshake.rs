//! E2E schema-version handshake.
//!
//! After the base stack auto-migrates to schema v2, force-poke
//! `taskq_meta.schema_version = 99` (a future version unknown to this
//! binary). Run a one-off `cp serve` (no `--auto-migrate`) and assert
//! it exits non-zero with a schema-mismatch message — operators must
//! explicitly run `migrate` rather than have the CP silently start
//! against a mismatched DB.

use anyhow::{anyhow, Result};
use taskq_e2e_tests::{require_docker, ComposeFile, ComposeStack};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn cp_refuses_to_serve_with_mismatched_schema_version() -> Result<()> {
    // Arrange: full stack up; auto-migrate brought DB to current
    // SCHEMA_VERSION (currently 2, see taskq-cp/src/lib.rs).
    require_docker()?;
    let stack = ComposeStack::up(ComposeFile::Base)?;

    // Stop the running CP so the connection slot is free for our
    // one-off probe. Postgres stays up.
    stack.stop_service("cp")?;

    // Act 1: poke a future schema_version into taskq_meta.
    stack
        .pg_exec("UPDATE taskq_meta SET schema_version = 99 WHERE only_row = TRUE")
        .await?;

    // Act 2: run a one-off `cp serve` (no --auto-migrate). Should fail.
    let (code, output) = stack.run_cp_oneoff("cp", &["--config", "/etc/taskq/cp.toml", "serve"])?;

    // Assert
    assert_ne!(
        code, 0,
        "cp serve should refuse to start; output:\n{output}"
    );
    let lower = output.to_lowercase();
    if !(lower.contains("schema") && lower.contains("version")) {
        return Err(anyhow!(
            "expected schema-version mismatch message, got:\n{output}"
        ));
    }

    Ok(())
}
