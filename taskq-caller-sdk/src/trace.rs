//! W3C Trace Context extraction.
//!
//! Per `design.md` §11.2, every taskq RPC carries the caller's W3C
//! `traceparent` (and optionally `tracestate`) bytes. The SDK reads
//! these from the *current* `tracing::Span` so that user code that
//! has already wired up an OTel pipeline (subscriber + exporter)
//! propagates context "for free" — no manual plumbing.
//!
//! On the wire `traceparent` is the **55-byte ASCII representation**
//! of the W3C version-00 string:
//!
//! ```text
//! 00-{trace_id_32_hex}-{span_id_16_hex}-{flags_2_hex}
//! ```
//!
//! When the caller has no active OTel context (no subscriber, or the
//! current span is sampled-out / disconnected), the SDK generates a
//! fresh `traceparent` with random IDs so the server still records a
//! W3C-valid value on the task row.
//!
//! `tracestate` is opaque vendor data; the SDK currently does not
//! extract it from the `tracing` span (the Rust `tracing-opentelemetry`
//! crate has no public extractor for it as of `0.25`). When/if the
//! upstream API adds a hook, this is the single point to plug it in.

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId};
use rand::RngCore;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// W3C Trace Context bytes that go onto every outbound RPC.
#[derive(Debug, Clone)]
pub(crate) struct TraceContextBytes {
    pub traceparent: Vec<u8>,
    pub tracestate: Vec<u8>,
}

/// Extract a `TraceContextBytes` from the current `tracing::Span` (if
/// it carries an OTel context) or generate a fresh, randomly-rooted
/// one. Always returns 55-byte W3C traceparent; never empty.
pub(crate) fn extract_or_generate() -> TraceContextBytes {
    if let Some(bytes) = extract_current_span() {
        return bytes;
    }
    generate_fresh()
}

fn extract_current_span() -> Option<TraceContextBytes> {
    // `Span::current().context()` returns an `opentelemetry::Context`.
    // When no OTel-aware subscriber is installed (e.g., the caller
    // never called `tracing::subscriber::set_global_default` with a
    // `tracing-opentelemetry` layer) this resolves to the empty
    // context with an invalid SpanContext (`is_valid() == false`).
    let cx = tracing::Span::current().context();
    let span_ref = cx.span();
    let span_ctx: &SpanContext = span_ref.span_context();
    if !span_ctx.is_valid() {
        return None;
    }
    Some(TraceContextBytes {
        traceparent: format_traceparent(
            span_ctx.trace_id(),
            span_ctx.span_id(),
            span_ctx.trace_flags(),
        )
        .into_bytes(),
        // tracestate carries vendor-specific data. The Rust OTel API
        // exposes it as `TraceState` on `SpanContext`, but the
        // wire-bytes encoding is just the ASCII serialization; v1 of
        // the SDK forwards it verbatim if non-empty.
        tracestate: span_ctx.trace_state().header().into_bytes(),
    })
}

fn generate_fresh() -> TraceContextBytes {
    let mut rng = rand::thread_rng();
    let mut trace_bytes = [0u8; 16];
    rng.fill_bytes(&mut trace_bytes);
    let mut span_bytes = [0u8; 8];
    rng.fill_bytes(&mut span_bytes);

    // Zero IDs are reserved by W3C as "invalid"; if RNG produced one,
    // bump the first byte. Probability is 2^-128 / 2^-64 so this is
    // defensive only.
    if trace_bytes == [0u8; 16] {
        trace_bytes[0] = 1;
    }
    if span_bytes == [0u8; 8] {
        span_bytes[0] = 1;
    }

    let traceparent = format_traceparent(
        TraceId::from_bytes(trace_bytes),
        SpanId::from_bytes(span_bytes),
        // Sampled = 0 (not sampled): the SDK has no opinion on whether
        // this synthetic context should be exported. Operators control
        // sampling via the namespace's `trace_sampling_ratio`.
        TraceFlags::default(),
    );
    TraceContextBytes {
        traceparent: traceparent.into_bytes(),
        tracestate: Vec::new(),
    }
}

fn format_traceparent(trace_id: TraceId, span_id: SpanId, flags: TraceFlags) -> String {
    // W3C version-00 format: `00-{trace_id_32_hex}-{span_id_16_hex}-{flags_2_hex}`.
    // 2 + 1 + 32 + 1 + 16 + 1 + 2 == 55 bytes.
    format!(
        "00-{:032x}-{:016x}-{:02x}",
        trace_id,
        span_id,
        flags.to_u8()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_traceparent_is_55_bytes() {
        // Arrange / Act
        let bytes = generate_fresh();

        // Assert
        assert_eq!(bytes.traceparent.len(), 55);
        assert!(bytes.tracestate.is_empty());
    }

    #[test]
    fn generated_traceparent_starts_with_version_00() {
        // Arrange / Act
        let bytes = generate_fresh();

        // Assert: per W3C, the version field is two hex chars; v1 SDK
        // always emits "00" (the only published version as of 2025).
        let s = std::str::from_utf8(&bytes.traceparent).unwrap();
        assert!(s.starts_with("00-"), "traceparent: {s:?}");
    }

    #[test]
    fn generated_traceparent_has_three_dashes_at_known_offsets() {
        // Arrange
        let bytes = generate_fresh();

        // Act
        let s = std::str::from_utf8(&bytes.traceparent).unwrap();

        // Assert: the W3C wire layout is fixed-width; if any of these
        // shift, the format string above is wrong.
        assert_eq!(&s[2..3], "-");
        assert_eq!(&s[35..36], "-");
        assert_eq!(&s[52..53], "-");
    }

    #[test]
    fn extract_or_generate_returns_a_value_with_no_active_span() {
        // Arrange: no global subscriber installed.
        // Act
        let bytes = extract_or_generate();

        // Assert: the SDK never returns an empty traceparent — it falls
        // back to a freshly-generated one so the server always has a
        // valid W3C value to persist.
        assert_eq!(bytes.traceparent.len(), 55);
    }
}
