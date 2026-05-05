//! W3C Trace Context decoder.
//!
//! Inverse of `taskq-caller-sdk`'s `trace.rs` extractor. The caller
//! SDK serializes the current `tracing::Span` into 55-byte W3C
//! `traceparent` bytes and persists them on the task row. When the
//! worker SDK acquires the task it gets those bytes back from the
//! CP; we parse them into an `opentelemetry::trace::SpanContext` so
//! the harness can attach the upstream parent to the
//! handler-wrapping `tracing::Span`. Without this, the handler runs
//! in a disconnected trace and operators see two separate timelines
//! for what is logically one workflow.
//!
//! W3C `traceparent` wire format (version-00 only, 55 bytes ASCII):
//!
//! ```text
//! 00-{trace_id_32_hex}-{span_id_16_hex}-{flags_2_hex}
//! ```
//!
//! `tracestate` is opaque vendor data — we forward it onto the
//! `SpanContext`'s `TraceState` when present so downstream exporters
//! see the same vendor headers the caller produced.

use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};

/// Parse W3C `traceparent` + `tracestate` bytes into a `SpanContext`
/// suitable for attaching as a parent via
/// `tracing_opentelemetry::OpenTelemetrySpanExt::set_parent`.
///
/// Returns `None` for any malformed input — callers fall back to the
/// implicit (disconnected) span context. Reasons for None:
/// - traceparent isn't 55 bytes
/// - any field isn't valid hex
/// - trace_id or span_id is the W3C "invalid" all-zeros value
/// - version != "00" (we don't speak future versions yet)
pub(crate) fn parse_traceparent(traceparent: &[u8], tracestate: &[u8]) -> Option<SpanContext> {
    if traceparent.len() != 55 {
        return None;
    }
    let s = std::str::from_utf8(traceparent).ok()?;
    let mut parts = s.splitn(4, '-');
    let version = parts.next()?;
    let trace_hex = parts.next()?;
    let span_hex = parts.next()?;
    let flags_hex = parts.next()?;
    if version != "00" || trace_hex.len() != 32 || span_hex.len() != 16 || flags_hex.len() != 2 {
        return None;
    }

    let trace_id_bytes = decode_hex_array::<16>(trace_hex)?;
    let span_id_bytes = decode_hex_array::<8>(span_hex)?;
    let flags_byte = decode_hex_array::<1>(flags_hex)?[0];

    if trace_id_bytes == [0u8; 16] || span_id_bytes == [0u8; 8] {
        // W3C reserves the all-zeros trace_id / span_id as "invalid"
        // — propagating them would corrupt downstream exporters.
        return None;
    }

    // tracestate is best-effort: parse failures fall through to an
    // empty TraceState so the parent context still attaches.
    use std::str::FromStr;
    let trace_state = std::str::from_utf8(tracestate)
        .ok()
        .and_then(|s| TraceState::from_str(s).ok())
        .unwrap_or_default();

    Some(SpanContext::new(
        TraceId::from_bytes(trace_id_bytes),
        SpanId::from_bytes(span_id_bytes),
        TraceFlags::new(flags_byte),
        // `is_remote = true` — the parent originated outside this
        // process. Downstream `tracing-opentelemetry` exporters
        // mark it as REMOTE_PARENT in OTel span links.
        true,
        trace_state,
    ))
}

fn decode_hex_array<const N: usize>(s: &str) -> Option<[u8; N]> {
    if s.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        let hi = hex_digit(s.as_bytes()[i * 2])?;
        let lo = hex_digit(s.as_bytes()[i * 2 + 1])?;
        *byte = (hi << 4) | lo;
    }
    Some(out)
}

fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "00-0af7651916cd43dd8448eb211c80319c-b9c7c989f97918e1-01";

    #[test]
    fn parse_round_trips_canonical_w3c_string() {
        // Arrange
        let traceparent = SAMPLE.as_bytes();

        // Act
        let ctx = parse_traceparent(traceparent, b"").expect("valid input parses");

        // Assert
        assert_eq!(
            format!("{:032x}", ctx.trace_id()),
            "0af7651916cd43dd8448eb211c80319c"
        );
        assert_eq!(format!("{:016x}", ctx.span_id()), "b9c7c989f97918e1");
        assert_eq!(ctx.trace_flags().to_u8(), 0x01);
        assert!(ctx.is_remote());
    }

    #[test]
    fn parse_returns_none_for_short_input() {
        assert!(parse_traceparent(b"00-too-short", b"").is_none());
    }

    #[test]
    fn parse_returns_none_for_unsupported_version() {
        // Arrange: same length and shape but version "ff" instead of "00".
        let bad = "ff-0af7651916cd43dd8448eb211c80319c-b9c7c989f97918e1-01";

        // Act / Assert
        assert!(parse_traceparent(bad.as_bytes(), b"").is_none());
    }

    #[test]
    fn parse_returns_none_for_invalid_trace_id_all_zeros() {
        // Arrange: well-formed-but-invalid (W3C reserves all-zeros).
        let bad = "00-00000000000000000000000000000000-b9c7c989f97918e1-01";

        // Act / Assert
        assert!(parse_traceparent(bad.as_bytes(), b"").is_none());
    }

    #[test]
    fn parse_returns_none_for_non_hex_chars() {
        let bad = "00-zzf7651916cd43dd8448eb211c80319c-b9c7c989f97918e1-01";
        assert!(parse_traceparent(bad.as_bytes(), b"").is_none());
    }

    #[test]
    fn parse_returns_none_for_empty_input() {
        assert!(parse_traceparent(b"", b"").is_none());
    }

    #[test]
    fn parse_with_tracestate_attaches_vendor_state() {
        // Arrange
        let trace_state = b"vendor1=opaque,vendor2=more";

        // Act
        let ctx = parse_traceparent(SAMPLE.as_bytes(), trace_state).unwrap();

        // Assert: the TraceState.header() serializes back to roughly
        // the same set of vendor entries (order is preserved per
        // W3C).
        let header = ctx.trace_state().header();
        assert!(header.contains("vendor1=opaque"));
        assert!(header.contains("vendor2=more"));
    }

    #[test]
    fn parse_tolerates_garbage_tracestate() {
        // Arrange: invalid UTF-8 in tracestate -- traceparent is
        // still good and should yield a context with empty
        // TraceState rather than failing the whole parse.
        let bad_state = &[0xff, 0xfe, 0xfd];

        // Act
        let ctx = parse_traceparent(SAMPLE.as_bytes(), bad_state).unwrap();

        // Assert: parent context still resolved; tracestate empty.
        assert!(ctx.trace_state().header().is_empty());
    }
}
