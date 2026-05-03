//! E2E CLI golden path.
//!
//! Drives `taskq-cli` inside the cp container against the running CP:
//! `namespace create / set-quota / stats / list-workers`. Validates the
//! shipped binary, end-to-end, against a real Postgres backend.

use anyhow::{anyhow, Result};
use taskq_e2e_tests::{require_docker, ComposeFile, ComposeStack};

const NS: &str = "e2e_cli";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn cli_admin_round_trip() -> Result<()> {
    // Arrange
    require_docker()?;
    let stack = ComposeStack::up(ComposeFile::Base)?;

    // Act + Assert: each verb succeeds and prints a sensible token.
    let create_out = stack.exec_cli("cp", &["namespace", "create", NS])?;
    if !create_out.contains("succeeded") {
        return Err(anyhow!("namespace create unexpected output: {create_out}"));
    }

    let setquota_out = stack.exec_cli("cp", &["set-quota", NS, "--max-pending", "50"])?;
    if !setquota_out.contains("succeeded") {
        return Err(anyhow!("set-quota unexpected output: {setquota_out}"));
    }

    let stats_out = stack.exec_cli("cp", &["stats", NS])?;
    // Stats render either tabular or JSON; both should at least
    // reference the namespace name in the body.
    if !stats_out.contains(NS) {
        return Err(anyhow!("stats output missing namespace: {stats_out}"));
    }

    let workers_out = stack.exec_cli("cp", &["list-workers", NS])?;
    // No workers registered -- output should still be valid (table
    // header or empty json array).
    if workers_out.is_empty() {
        return Err(anyhow!("list-workers produced empty output"));
    }

    Ok(())
}
