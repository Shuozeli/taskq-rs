//! Output rendering for the operator CLI.
//!
//! Two modes:
//!
//! - `human` — operator-friendly text. Single-row results render as
//!   key-value pairs; multi-row results render as a tabular list with
//!   header underline.
//! - `json` — machine-readable JSON via `serde_json::to_writer_pretty`.
//!   Suitable for scripting (`taskq-cli stats foo --format json | jq`).
//!
//! Every command's response type implements both [`Renderable`] (for
//! `human`) and `serde::Serialize` (for `json`). The dispatcher picks
//! one based on the global `--format` flag.

use std::fmt::Write as _;
use std::io::{self, Write};

use clap::ValueEnum;
use serde::Serialize;

/// Output format selector for the global `--format` flag.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "lower")]
pub enum OutputFormat {
    /// Operator-friendly key-value / tabular text.
    #[default]
    Human,
    /// `serde_json` pretty-printed JSON (suitable for `jq`).
    Json,
}

/// Trait for types that know how to render themselves as human-readable
/// text. JSON rendering goes through `serde::Serialize` directly.
pub trait Renderable {
    /// Render this value to `out` in human-readable form.
    fn render_human(&self, out: &mut dyn Write) -> io::Result<()>;
}

/// Top-level entry point used by every command. Picks `human` vs.
/// `json` based on the format flag, writes to `out`, and flushes.
pub fn render<T>(value: &T, format: OutputFormat, out: &mut dyn Write) -> io::Result<()>
where
    T: Renderable + Serialize,
{
    match format {
        OutputFormat::Human => {
            value.render_human(out)?;
        }
        OutputFormat::Json => {
            serde_json::to_writer_pretty(&mut *out, value).map_err(io::Error::other)?;
            writeln!(out)?;
        }
    }
    out.flush()
}

/// Render a single-row key-value report.
pub fn render_kv(rows: &[(&str, String)], out: &mut dyn Write) -> io::Result<()> {
    let key_width = rows.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
    for (k, v) in rows {
        writeln!(out, "{:<width$}  {}", k, v, width = key_width)?;
    }
    Ok(())
}

/// Render a tabular result with a header row underlined by dashes.
/// Each column's width is the max of the header and the cell values.
pub fn render_table(headers: &[&str], rows: &[Vec<String>], out: &mut dyn Write) -> io::Result<()> {
    if headers.is_empty() {
        return Ok(());
    }
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() && cell.len() > widths[i] {
                widths[i] = cell.len();
            }
        }
    }
    let mut line = String::new();
    for (i, h) in headers.iter().enumerate() {
        if i > 0 {
            line.push_str("  ");
        }
        let _ = write!(&mut line, "{:<width$}", h, width = widths[i]);
    }
    writeln!(out, "{line}")?;
    line.clear();
    for (i, w) in widths.iter().enumerate() {
        if i > 0 {
            line.push_str("  ");
        }
        line.push_str(&"-".repeat(*w));
    }
    writeln!(out, "{line}")?;
    for row in rows {
        line.clear();
        for (i, cell) in row.iter().enumerate() {
            if i > 0 {
                line.push_str("  ");
            }
            let w = widths.get(i).copied().unwrap_or(cell.len());
            let _ = write!(&mut line, "{:<width$}", cell, width = w);
        }
        writeln!(out, "{line}")?;
    }
    Ok(())
}

/// Render a one-line "X succeeded" message — used by side-effecting
/// commands like `namespace enable` that don't return a payload.
pub fn render_status(msg: &str, out: &mut dyn Write) -> io::Result<()> {
    writeln!(out, "{msg}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct Row {
        name: String,
        count: u32,
    }

    impl Renderable for Row {
        fn render_human(&self, out: &mut dyn Write) -> io::Result<()> {
            render_kv(
                &[
                    ("name", self.name.clone()),
                    ("count", self.count.to_string()),
                ],
                out,
            )
        }
    }

    #[test]
    fn render_human_emits_key_value_lines() {
        // Arrange
        let row = Row {
            name: "alpha".into(),
            count: 7,
        };
        let mut buf: Vec<u8> = Vec::new();

        // Act
        render(&row, OutputFormat::Human, &mut buf).unwrap();

        // Assert: both the key and value land on the same line; key is
        // padded to the max-key width.
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("name"), "missing key: {s}");
        assert!(s.contains("alpha"), "missing value: {s}");
        assert!(s.contains("count"), "missing key: {s}");
        assert!(s.contains('7'), "missing value: {s}");
    }

    #[test]
    fn render_json_emits_valid_json() {
        // Arrange
        let row = Row {
            name: "alpha".into(),
            count: 7,
        };
        let mut buf: Vec<u8> = Vec::new();

        // Act
        render(&row, OutputFormat::Json, &mut buf).unwrap();

        // Assert: round-trip through serde_json::Value to verify the
        // bytes are well-formed JSON.
        let s = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&s).expect("valid json");
        assert_eq!(parsed["name"], "alpha");
        assert_eq!(parsed["count"], 7);
    }

    #[test]
    fn render_table_aligns_columns() {
        // Arrange
        let headers = ["name", "count"];
        let rows = vec![
            vec!["alpha".to_owned(), "7".to_owned()],
            vec!["bravo-long".to_owned(), "100".to_owned()],
        ];
        let mut buf: Vec<u8> = Vec::new();

        // Act
        render_table(&headers, &rows, &mut buf).unwrap();

        // Assert
        let s = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines.len(), 4); // header, sep, two rows
        assert!(lines[1].contains("----"), "separator missing: {s}");
        assert!(lines[2].contains("alpha"));
        assert!(lines[3].contains("bravo-long"));
    }

    #[test]
    fn render_table_emits_only_header_and_separator_when_rows_empty() {
        // Arrange: tabular outputs (e.g. `list-workers` against an
        // empty namespace) MUST still render the header so operators
        // can confirm the right command ran. Empty rows means just
        // header + separator.
        let headers = ["worker_id", "namespace"];
        let rows: Vec<Vec<String>> = Vec::new();
        let mut buf: Vec<u8> = Vec::new();

        // Act
        render_table(&headers, &rows, &mut buf).unwrap();

        // Assert
        let s = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines.len(), 2, "expected header + separator only: {s:?}");
        assert!(lines[0].contains("worker_id"));
        assert!(lines[1].contains("---"));
    }

    #[test]
    fn render_table_with_empty_headers_is_a_noop() {
        // Arrange
        let headers: [&str; 0] = [];
        let rows = vec![vec!["foo".to_owned()]];
        let mut buf: Vec<u8> = Vec::new();

        // Act
        render_table(&headers, &rows, &mut buf).unwrap();

        // Assert
        assert!(buf.is_empty());
    }

    #[test]
    fn render_kv_with_empty_rows_emits_nothing() {
        // Arrange
        let rows: Vec<(&str, String)> = Vec::new();
        let mut buf: Vec<u8> = Vec::new();

        // Act
        render_kv(&rows, &mut buf).unwrap();

        // Assert
        assert!(buf.is_empty());
    }

    #[test]
    fn render_status_appends_newline() {
        // Arrange
        let mut buf: Vec<u8> = Vec::new();

        // Act
        render_status(
            "namespace create succeeded on namespace \"alpha\"",
            &mut buf,
        )
        .unwrap();

        // Assert
        let s = String::from_utf8(buf).unwrap();
        assert!(
            s.ends_with('\n'),
            "render_status must terminate the line: {s:?}"
        );
    }

    #[test]
    fn render_json_terminates_with_newline() {
        // Arrange
        let row = Row {
            name: "alpha".into(),
            count: 7,
        };
        let mut buf: Vec<u8> = Vec::new();

        // Act
        render(&row, OutputFormat::Json, &mut buf).unwrap();

        // Assert: callers that pipe to `jq` rely on a trailing newline
        // so the stream is valid newline-delimited JSON if multiple
        // commands chain together.
        let s = String::from_utf8(buf).unwrap();
        assert!(s.ends_with('\n'), "json output must terminate with newline");
    }

    #[test]
    fn render_kv_pads_keys_to_max_width() {
        // Arrange
        let rows = vec![("short", "1".to_owned()), ("longer-key", "2".to_owned())];
        let mut buf: Vec<u8> = Vec::new();

        // Act
        render_kv(&rows, &mut buf).unwrap();

        // Assert
        let s = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines.len(), 2);
        // Key column is padded to "longer-key".len() == 10.
        assert!(lines[0].starts_with("short     "), "got: {:?}", lines[0]);
    }
}
