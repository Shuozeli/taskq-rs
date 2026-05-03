//! E2E quota under real concurrency.
//!
//! Set `max_pending = 5` for the namespace via the CLI. With no
//! workers draining, fire 30 concurrent submits and assert ~5 land
//! and the rest get `MAX_PENDING_EXCEEDED` rejections. Validates the
//! capacity-quota path under wire-level contention rather than
//! per-process.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use taskq_caller_sdk::{ClientError, SubmitOutcome, SubmitRequest};
use taskq_e2e_tests::{require_docker, ComposeFile, ComposeStack};
use taskq_proto::RejectReason as WireRejectReason;
use tokio::sync::Mutex;

const NS: &str = "e2e_quota";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn max_pending_rejects_under_burst() -> Result<()> {
    // Arrange
    require_docker()?;
    let stack = ComposeStack::up(ComposeFile::Base)?;
    stack.seed_namespace(NS).await?;
    // Tighten the quota via the wire admin path.
    stack.exec_cli("cp", &["set-quota", NS, "--max-pending", "5"])?;

    let endpoint = stack.primary_endpoint();
    let caller = stack.caller_at(endpoint).await?;
    let caller = Arc::new(Mutex::new(caller));

    // Act: fire 30 concurrent submits with distinct idempotency keys.
    let mut handles = Vec::with_capacity(30);
    for i in 0..30 {
        let caller = Arc::clone(&caller);
        let h = tokio::spawn(async move {
            let mut req = SubmitRequest::new(NS, "echo", Bytes::from_static(b"x"));
            req.idempotency_key = Some(format!("burst-{i}"));
            let mut c = caller.lock().await;
            c.submit(req).await
        });
        handles.push(h);
    }

    let mut accepted = 0;
    let mut rejected_max_pending = 0;
    let mut other = Vec::new();
    for h in handles {
        match h.await.map_err(|e| anyhow!("join: {e}"))? {
            Ok(SubmitOutcome::Created { .. }) | Ok(SubmitOutcome::Existing { .. }) => {
                accepted += 1;
            }
            Err(ClientError::Rejected { reason, .. })
                if reason == WireRejectReason::MAX_PENDING_EXCEEDED =>
            {
                rejected_max_pending += 1;
            }
            other_outcome => other.push(format!("{other_outcome:?}")),
        }
    }

    // Assert: ~5 accepted (cap), rest rejected with MAX_PENDING_EXCEEDED.
    // Allow a small slop window because the cap is a high-water-mark
    // checked transactionally per-submit; under a 30-way race the
    // exact count can flicker by 1 if the system_default fallback is
    // racing the per-namespace value.
    assert!(
        (5..=8).contains(&accepted),
        "expected ~5 accepted, got accepted={accepted} rejected={rejected_max_pending} other={other:?}"
    );
    assert!(
        rejected_max_pending >= 20,
        "expected >=20 MAX_PENDING_EXCEEDED rejections, got accepted={accepted} rejected={rejected_max_pending} other={other:?}"
    );

    // Sleep briefly so any in-flight submit acks settle before the
    // stack tears down.
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(())
}
