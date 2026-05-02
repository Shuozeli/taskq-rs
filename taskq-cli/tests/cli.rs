//! Integration tests for the `taskq-cli` binary.
//!
//! Drive the built executable as a subprocess via `assert_cmd` and
//! verify operator-visible behaviour:
//!
//! - `--help` works for the top level and every subcommand.
//! - Invalid argument combinations exit non-zero with a clear message.
//! - `--format json` produces valid JSON for code paths that exit
//!   without making an RPC (e.g., `--dry-run`).
//! - Destructive operations (`purge`, `replay`) honour `--dry-run` and
//!   the `--confirm-namespace` mismatch guard.
//!
//! End-to-end tests against a running CP land in Phase 9b — the goal
//! here is to lock in the CLI surface (flags, exit codes, formatting)
//! without standing up a server.

use assert_cmd::Command;
use predicates::prelude::*;

/// Construct a `Command` for the built CLI binary. `cargo_bin` resolves
/// to `target/<profile>/taskq-cli` so tests pick up edits without
/// polluting the user's `$PATH`.
fn cli() -> Command {
    Command::cargo_bin("taskq-cli").expect("taskq-cli binary built by `cargo test`")
}

#[test]
fn top_level_help_lists_every_subcommand() {
    // Arrange / Act
    let assert = cli().arg("--help").assert();

    // Assert
    assert
        .success()
        .stdout(predicate::str::contains("namespace"))
        .stdout(predicate::str::contains("set-quota"))
        .stdout(predicate::str::contains("purge"))
        .stdout(predicate::str::contains("replay"))
        .stdout(predicate::str::contains("stats"))
        .stdout(predicate::str::contains("list-workers"));
}

#[test]
fn namespace_help_lists_all_lifecycle_subcommands() {
    // Arrange / Act
    let assert = cli().args(["namespace", "--help"]).assert();

    // Assert
    assert
        .success()
        .stdout(predicate::str::contains("create"))
        .stdout(predicate::str::contains("list"))
        .stdout(predicate::str::contains("enable"))
        .stdout(predicate::str::contains("disable"));
}

#[test]
fn purge_help_documents_destructive_flags() {
    // Arrange / Act
    let assert = cli().args(["purge", "--help"]).assert();

    // Assert
    assert
        .success()
        .stdout(predicate::str::contains("--namespace"))
        .stdout(predicate::str::contains("--confirm-namespace"))
        .stdout(predicate::str::contains("--max-tasks"))
        .stdout(predicate::str::contains("--dry-run"));
}

#[test]
fn replay_help_documents_audit_note_flag() {
    // Arrange / Act
    let assert = cli().args(["replay", "--help"]).assert();

    // Assert
    assert
        .success()
        .stdout(predicate::str::contains("--audit-note"));
}

#[test]
fn stats_help_takes_namespace_positional() {
    // Arrange / Act
    let assert = cli().args(["stats", "--help"]).assert();

    // Assert
    assert
        .success()
        .stdout(predicate::str::contains("<NAMESPACE>"));
}

#[test]
fn list_workers_help_documents_include_dead() {
    // Arrange / Act
    let assert = cli().args(["list-workers", "--help"]).assert();

    // Assert
    assert
        .success()
        .stdout(predicate::str::contains("--include-dead"));
}

#[test]
fn missing_subcommand_exits_with_usage_error() {
    // Arrange / Act
    let assert = cli().assert();

    // Assert: clap prints a usage hint and exits with code 2 by default.
    assert
        .failure()
        .stderr(predicate::str::contains("Usage: taskq-cli"));
}

#[test]
fn unknown_subcommand_exits_with_usage_error() {
    // Arrange / Act
    let assert = cli().arg("tickle-the-tiger").assert();

    // Assert
    assert
        .failure()
        .stderr(predicate::str::contains("unrecognized subcommand"));
}

#[test]
fn purge_without_required_namespace_fails() {
    // Arrange / Act: purge needs both --namespace and --confirm-namespace.
    let assert = cli().args(["purge", "--max-tasks=10"]).assert();

    // Assert
    assert.failure().stderr(predicate::str::contains(
        "the following required arguments were not provided",
    ));
}

#[test]
fn purge_with_mismatched_confirm_exits_invalid_argument() {
    // Arrange / Act: confirm-namespace doesn't match namespace.
    let assert = cli()
        .args([
            "purge",
            "--namespace=alpha",
            "--confirm-namespace=beta",
            "--dry-run",
        ])
        .assert();

    // Assert: caller-side guard exits with InvalidArgument code (64),
    // before any RPC. The plan summary prints to stderr regardless.
    assert
        .failure()
        .code(64)
        .stderr(predicate::str::contains("confirm-namespace"));
}

#[test]
fn purge_dry_run_emits_human_summary_without_rpc() {
    // Arrange / Act: `--dry-run` short-circuits before `connect_admin`,
    // so the test can run with no server reachable.
    let assert = cli()
        .args([
            "purge",
            "--namespace=alpha",
            "--confirm-namespace=alpha",
            "--max-tasks=42",
            "--dry-run",
        ])
        .assert();

    // Assert
    assert
        .success()
        .stdout(predicate::str::contains("namespace"))
        .stdout(predicate::str::contains("alpha"))
        .stdout(predicate::str::contains("dry_run"))
        .stdout(predicate::str::contains("true"));
}

#[test]
fn purge_dry_run_json_output_is_valid_json() {
    // Arrange / Act
    let output = cli()
        .args([
            "--format=json",
            "purge",
            "--namespace=alpha",
            "--confirm-namespace=alpha",
            "--max-tasks=42",
            "--dry-run",
        ])
        .output()
        .expect("CLI should run");

    // Assert
    assert!(output.status.success(), "expected success: {output:?}");
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("dry-run json must be valid");
    assert_eq!(parsed["namespace"], "alpha");
    assert_eq!(parsed["dry_run"], true);
    assert_eq!(parsed["purged_count"], 0);
}

#[test]
fn replay_dry_run_emits_summary_without_rpc() {
    // Arrange / Act
    let assert = cli()
        .args([
            "replay",
            "--namespace=alpha",
            "--confirm-namespace=alpha",
            "--max-tasks=10",
            "--dry-run",
        ])
        .assert();

    // Assert
    assert
        .success()
        .stdout(predicate::str::contains("namespace"))
        .stdout(predicate::str::contains("alpha"))
        .stdout(predicate::str::contains("dry_run"));
}

#[test]
fn replay_dry_run_json_output_is_valid_json() {
    // Arrange / Act
    let output = cli()
        .args([
            "--format=json",
            "replay",
            "--namespace=alpha",
            "--confirm-namespace=alpha",
            "--max-tasks=5",
            "--dry-run",
        ])
        .output()
        .expect("CLI should run");

    // Assert
    assert!(output.status.success(), "expected success: {output:?}");
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("dry-run replay json must be valid");
    assert_eq!(parsed["namespace"], "alpha");
    assert_eq!(parsed["dry_run"], true);
    assert_eq!(parsed["replayed_count"], 0);
}

#[test]
fn replay_with_mismatched_confirm_exits_invalid_argument() {
    // Arrange / Act
    let assert = cli()
        .args([
            "replay",
            "--namespace=alpha",
            "--confirm-namespace=beta",
            "--dry-run",
        ])
        .assert();

    // Assert
    assert.failure().code(64);
}

#[test]
fn unreachable_endpoint_exits_nonzero_with_clear_message() {
    // Arrange: pick a port that is almost certainly not listening.
    let endpoint = "http://127.0.0.1:1";

    // Act: invoking a non-destructive RPC against a dead endpoint
    // should fail before producing stdout. `pure-grpc-rs` lazily
    // connects on the first RPC, so the failure surfaces either as
    // `Connect` (exit 2) when DNS / TCP fail at handshake time, or as
    // `Transport` (exit 3) when the framework wraps the IO error
    // inside a `Status::unavailable`. Either is correct; both leave a
    // diagnostic on stderr.
    let output = cli()
        .args(["--endpoint", endpoint, "stats", "alpha"])
        .timeout(std::time::Duration::from_secs(10))
        .output()
        .expect("CLI should run");

    // Assert
    assert!(!output.status.success(), "expected non-zero exit");
    let code = output.status.code().expect("exit code present");
    assert!(
        code == 2 || code == 3,
        "expected connect (2) or transport (3) exit code, got {code}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to connect") || stderr.contains("transport error"),
        "expected diagnostic in stderr, got: {stderr}"
    );
}

#[test]
fn invalid_endpoint_uri_exits_invalid_argument() {
    // Arrange / Act
    let assert = cli()
        .args(["--endpoint", "::not a uri::", "stats", "alpha"])
        .timeout(std::time::Duration::from_secs(10))
        .assert();

    // Assert: caller-side validation fires before any I/O.
    assert
        .failure()
        .code(64)
        .stderr(predicate::str::contains("not a valid http::Uri"));
}
