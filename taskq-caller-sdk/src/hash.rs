//! Payload hashing.
//!
//! Per `design.md` §6.1 and `problems/03-idempotency.md`, the canonical
//! payload identity for the `(namespace, idempotency_key)` dedup record
//! is a 32-byte BLAKE3 digest of the FlatBuffers wire bytes the caller
//! sent. The CP independently hashes incoming bytes and compares to the
//! stored value; the SDK computes the same hash so callers can
//! (a) include it in instrumentation and
//! (b) verify by hand against the `existing_payload_hash` field of an
//! `IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD` rejection without
//! having to re-hash externally.
//!
//! BLAKE3 is chosen over SHA-2 for two reasons documented in
//! `problems/03`: it is the algorithm the storage layer's CP-side hashing
//! commits to, so the SDK and CP produce identical 32-byte digests for
//! identical input bytes. Using SHA-256 here would require the CP to
//! also store the SHA-256, doubling write amplification on the
//! `idempotency_keys` row for no benefit.

/// Compute the canonical 32-byte payload hash for an idempotency record.
pub fn payload_hash(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_hash_is_deterministic_for_same_bytes() {
        // Arrange
        let payload = b"hello world";

        // Act
        let h1 = payload_hash(payload);
        let h2 = payload_hash(payload);

        // Assert
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 32);
    }

    #[test]
    fn payload_hash_diverges_for_different_bytes() {
        // Arrange / Act
        let h_alpha = payload_hash(b"alpha");
        let h_beta = payload_hash(b"beta");

        // Assert
        assert_ne!(h_alpha, h_beta);
    }

    #[test]
    fn payload_hash_handles_empty_input() {
        // Arrange / Act
        let h = payload_hash(&[]);

        // Assert: BLAKE3 of empty input is well-defined (not all-zeros).
        assert_eq!(h.len(), 32);
        assert_ne!(h, [0u8; 32]);
    }
}
