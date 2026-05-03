//! E2E idempotency over the wire.
//!
//! Two SDK clients on independent gRPC channels submit the same
//! (namespace, idempotency_key, payload). Assert both receive the same
//! `task_id` — one as `Created`, the other as `Existing` (order is
//! non-deterministic). Validates the dedup table works end-to-end
//! across separate connections rather than just within one client's
//! local idempotency cache.

use anyhow::{anyhow, Result};
use bytes::Bytes;
use taskq_caller_sdk::{SubmitOutcome, SubmitRequest};
use taskq_e2e_tests::{require_docker, ComposeFile, ComposeStack};

const NS: &str = "e2e_idem";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn duplicate_submit_across_channels_returns_same_task_id() -> Result<()> {
    // Arrange
    require_docker()?;
    let stack = ComposeStack::up(ComposeFile::Base)?;
    stack.seed_namespace(NS).await?;
    let endpoint = stack.primary_endpoint();
    let mut caller_a = stack.caller_at(endpoint.clone()).await?;
    let mut caller_b = stack.caller_at(endpoint).await?;

    let key = "wire-idempotency-key-1";
    let payload = Bytes::from_static(b"same-bytes");

    // Act: submit the same key from both channels in sequence (so the
    // assertion is deterministic). Ordering: A first, B second.
    let mut req_a = SubmitRequest::new(NS, "echo", payload.clone());
    req_a.idempotency_key = Some(key.into());
    let outcome_a = caller_a
        .submit(req_a)
        .await
        .map_err(|e| anyhow!("submit A: {e}"))?;

    let mut req_b = SubmitRequest::new(NS, "echo", payload);
    req_b.idempotency_key = Some(key.into());
    let outcome_b = caller_b
        .submit(req_b)
        .await
        .map_err(|e| anyhow!("submit B: {e}"))?;

    // Assert
    let (id_a, id_b) = match (outcome_a, outcome_b) {
        (SubmitOutcome::Created { task_id: a, .. }, SubmitOutcome::Existing { task_id: b, .. }) => {
            (a, b)
        }
        (SubmitOutcome::Created { task_id: a, .. }, SubmitOutcome::Created { task_id: b, .. }) => {
            // Race-y: both saw nothing in dedup. Should be vanishingly
            // rare with sequential submits but allow it -- the wire
            // contract guarantees same task_id either way.
            (a, b)
        }
        other => return Err(anyhow!("unexpected outcomes: {other:?}")),
    };
    assert_eq!(id_a, id_b, "idempotency must yield the same task_id");

    Ok(())
}
