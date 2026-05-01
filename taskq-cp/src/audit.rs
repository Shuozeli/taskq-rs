//! Same-transaction audit-log writer for admin RPCs.
//!
//! `design.md` §11.4: every admin RPC writes an [`AuditEntry`] to the
//! `audit_log` table inside the same SERIALIZABLE transaction as the action
//! it records. The entry carries:
//!
//! - structured `request_summary` (truncated at 4 KB with a `truncated:true`
//!   flag), and
//! - `request_hash` = sha256 of the full request body, so external systems
//!   can verify request bytes without storing the payload.
//!
//! `design.md` §11.4 also says retention is per-namespace via
//! `NamespaceQuota.audit_log_retention_days` (default 90); pruning is a
//! periodic job — not this module's concern.

use bytes::Bytes;
use sha2::{Digest, Sha256};
use taskq_storage::{AuditEntry, Namespace, StorageError, Timestamp};

use crate::state::StorageTxDyn;

/// Hard cap on the persisted `request_summary` size, in bytes. Bodies
/// larger than this are truncated and the JSON gains a `truncated: true`
/// flag at the top level.
pub const MAX_SUMMARY_BYTES: usize = 4 * 1024;

/// Outcome of the audited action. The audit row records this verbatim so
/// operators can search by result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditResult {
    Success,
    Rejected,
    Error,
}

impl AuditResult {
    fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Rejected => "rejected",
            Self::Error => "error",
        }
    }
}

/// Caller identity threaded through the admin handlers. Phase 5c is
/// auth-context-light; the actor string lands in `audit_log.actor` for
/// retroactive correlation. The exact wire-level extraction lives in the
/// gRPC interceptor (Phase 5b's territory).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Actor {
    pub identity: String,
}

impl Actor {
    pub fn new(identity: impl Into<String>) -> Self {
        Self {
            identity: identity.into(),
        }
    }

    /// Sentinel actor used when the caller did not present an identity
    /// (Phase 5b auth interceptor accepts unauthenticated requests).
    pub fn anonymous() -> Self {
        Self::new("anonymous")
    }

    pub fn as_str(&self) -> &str {
        &self.identity
    }
}

/// Compute sha256 over `bytes`. Re-exported convenience around `sha2` so
/// admin handlers don't all import the digest crate directly.
pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let out = hasher.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    arr
}

/// Append one audit row inside `tx` (the caller's SERIALIZABLE transaction).
///
/// `request_summary` is a `serde_json::Value` produced by the admin handler
/// — it should describe the request shape minus any sensitive payload (e.g.
/// `{ "namespace": "ns", "filter": "..." }`). The helper:
///
/// 1. Serializes `request_summary` to a JSON string.
/// 2. If the JSON exceeds [`MAX_SUMMARY_BYTES`], replaces it with a JSON
///    object carrying `{ "truncated": true, "original_size": N, "summary":
///    "<head of body, char-boundary safe>" }`.
/// 3. Writes the row via [`StorageTxDyn::audit_log_append`].
///
/// `request_hash` is the sha256 of the full unredacted request body the
/// caller transmitted (the wire bytes); the helper does not compute it
/// itself because admin handlers have access to the raw FlatBuffers body
/// before unpacking.
pub(crate) async fn audit_log_write(
    tx: &mut dyn StorageTxDyn,
    actor: &Actor,
    rpc: &str,
    namespace: Option<&Namespace>,
    request_summary: serde_json::Value,
    request_hash: [u8; 32],
    result: AuditResult,
) -> Result<(), StorageError> {
    let summary_bytes = encode_summary(request_summary);
    let entry = AuditEntry {
        timestamp: now_timestamp(),
        actor: actor.as_str().to_owned(),
        rpc: rpc.to_owned(),
        namespace: namespace.cloned(),
        request_summary: Bytes::from(summary_bytes),
        result: result.as_str().to_owned(),
        request_hash,
    };
    tx.audit_log_append(entry).await
}

/// JSON-encode `summary`, truncating to a fixed cap if necessary. The
/// truncated form is itself valid JSON so the storage column's `jsonb`
/// (Postgres) typing does not reject it.
fn encode_summary(summary: serde_json::Value) -> Vec<u8> {
    let raw = serde_json::to_vec(&summary).unwrap_or_else(|_| b"{}".to_vec());
    if raw.len() <= MAX_SUMMARY_BYTES {
        return raw;
    }
    // Walk back to a char boundary; serde_json::to_vec emits UTF-8 already
    // but a hard cut may land mid-codepoint when nested strings push the
    // body over.
    let original = String::from_utf8_lossy(&raw).into_owned();
    let mut head_end = MAX_SUMMARY_BYTES.min(original.len());
    while head_end > 0 && !original.is_char_boundary(head_end) {
        head_end -= 1;
    }
    let head = &original[..head_end];
    let truncated = serde_json::json!({
        "truncated": true,
        "original_size": raw.len(),
        "summary": head,
    });
    serde_json::to_vec(&truncated).unwrap_or_else(|_| b"{}".to_vec())
}

/// Wall-clock now as a `Timestamp`. Centralized so tests can swap the
/// helper if a deterministic clock is needed (Phase 5c does not — admin
/// audit writes always use the system clock).
fn now_timestamp() -> Timestamp {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    Timestamp::from_unix_millis(ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_matches_known_vector() {
        // Arrange: SHA-256("") -> e3b0c44...
        let expected_hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        // Act
        let hash = sha256(b"");

        // Assert
        let got_hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(got_hex, expected_hex);
    }

    #[test]
    fn encode_summary_passes_short_bodies_through() {
        // Arrange
        let body = serde_json::json!({"namespace": "ns", "filter": "foo"});

        // Act
        let bytes = encode_summary(body.clone());

        // Assert
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed, body);
    }

    #[test]
    fn encode_summary_truncates_oversize_bodies() {
        // Arrange: build a body > 4 KB.
        let huge = "x".repeat(MAX_SUMMARY_BYTES * 2);
        let body = serde_json::json!({"big": huge});

        // Act
        let bytes = encode_summary(body);

        // Assert: the persisted form must be a well-formed JSON object
        // carrying truncated:true.
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["truncated"], serde_json::Value::Bool(true));
        assert!(parsed["original_size"].as_u64().unwrap() > MAX_SUMMARY_BYTES as u64);
        assert!(parsed["summary"].is_string());
    }
}
