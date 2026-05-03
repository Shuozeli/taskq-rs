//! E2E admin purge: submit several tasks, drop them via `taskq-cli
//! purge`, verify the rows go away.
//!
//! Originally scoped as DLQ-replay but the failure path that produces
//! dead-letters is harder to drive cleanly without registering error
//! classes via SetNamespaceConfig. `purge` exercises the same admin
//! verb shape (CLI -> wire -> admin handler -> SQL) on a verb that
//! is straightforward to set up.

use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use taskq_caller_sdk::SubmitRequest;
use taskq_e2e_tests::{require_docker, ComposeFile, ComposeStack};

const NS: &str = "e2e_purge";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn purge_pending_clears_namespace() -> Result<()> {
    // Arrange
    require_docker()?;
    let stack = ComposeStack::up(ComposeFile::Base)?;
    stack.seed_namespace(NS).await?;
    stack.exec_cli("cp", &["set-quota", NS, "--max-pending", "100"])?;

    // Submit 3 tasks; no worker, so they sit in PENDING.
    let mut caller = stack.caller().await?;
    for i in 0..3 {
        let mut req = SubmitRequest::new(NS, "echo", Bytes::from_static(b"x"));
        req.idempotency_key = Some(format!("purge-{i}"));
        caller
            .submit(req)
            .await
            .map_err(|e| anyhow!("submit {i}: {e}"))?;
    }
    drop(caller);

    // Verify rows landed in PENDING.
    let (pending_pre, cancelled_pre) = counts(&stack, NS).await?;
    assert_eq!(pending_pre, 3, "expected 3 PENDING rows pre-purge");
    assert_eq!(cancelled_pre, 0, "expected 0 CANCELLED rows pre-purge");

    // Act: purge via the CLI. v1 implements PurgeTasks as
    // "transition to CANCELLED" (not DELETE), so the rows stay in the
    // `tasks` table with status=CANCELLED.
    let purge_out = stack.exec_cli(
        "cp",
        &["purge", "--namespace", NS, "--confirm-namespace", NS],
    )?;
    if !purge_out.contains("purged_count") {
        return Err(anyhow!("purge unexpected output: {purge_out}"));
    }

    // Allow the rate-limited (100/sec) cancel loop to drain.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Assert: PENDING -> 0, CANCELLED -> 3.
    let (pending_post, cancelled_post) = counts(&stack, NS).await?;
    assert_eq!(
        pending_post, 0,
        "expected 0 PENDING rows post-purge, got {pending_post}"
    );
    assert_eq!(
        cancelled_post, 3,
        "expected 3 CANCELLED rows post-purge, got {cancelled_post}"
    );

    Ok(())
}

async fn counts(stack: &ComposeStack, namespace: &str) -> Result<(i64, i64)> {
    use anyhow::Context;
    let (client, conn) =
        tokio_postgres::connect(&stack.pg_admin_url(), tokio_postgres::NoTls).await?;
    let h = tokio::spawn(async move {
        let _ = conn.await;
    });
    let row = client
        .query_one(
            "SELECT \
                COUNT(*) FILTER (WHERE status = 'PENDING')::bigint, \
                COUNT(*) FILTER (WHERE status = 'CANCELLED')::bigint \
             FROM tasks WHERE namespace = $1",
            &[&namespace],
        )
        .await
        .context("count tasks")?;
    let pending: i64 = row.get(0);
    let cancelled: i64 = row.get(1);
    h.abort();
    Ok((pending, cancelled))
}
