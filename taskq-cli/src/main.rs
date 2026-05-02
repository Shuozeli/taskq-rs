//! taskq-cli: operator CLI for taskq-rs.
//!
//! Connects to a remote `taskq-cp` control plane and wraps the
//! `TaskAdmin` admin RPCs with operator-friendly subcommands. The CLI
//! is the thin friendly face on top of the admin contract documented
//! in `design.md` §3.2 / §6.7.
//!
//! Subcommand tree:
//!
//! ```text
//! taskq-cli namespace create <name>             SetNamespaceQuota (defaults)
//! taskq-cli namespace list   --namespace <ns>   GetStats (single-row stand-in)
//! taskq-cli namespace enable <name>             EnableNamespace
//! taskq-cli namespace disable <name>            DisableNamespace
//! taskq-cli set-quota <ns> [--max-pending=N ...]  SetNamespaceQuota
//! taskq-cli purge --namespace <ns> ...          PurgeTasks
//! taskq-cli replay --namespace <ns> ...         ReplayDeadLetters
//! taskq-cli stats <ns>                          GetStats
//! taskq-cli list-workers <ns> [--include-dead]  ListWorkers
//! ```
//!
//! See `taskq-cli/README.md` for usage examples.

mod cli;
mod commands;
mod connect;
mod error;
mod output;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::cli::Cli;

/// CLI entry point. Returns a process exit code; non-zero values come
/// from [`CliError::exit_code`] so shell scripts can distinguish
/// connect / transport / rejection / abort failures.
fn main() -> std::process::ExitCode {
    // Logs go to stderr only when `RUST_LOG` is set, so the default
    // human / json output on stdout stays uncluttered.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("failed to start tokio runtime: {err}");
            return std::process::ExitCode::from(70);
        }
    };

    let result = runtime.block_on(commands::run(cli));
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            // Convert i32 → u8 for ExitCode. Non-fitting codes (e.g.
            // 130) clamp at 255; the operator still sees stderr.
            let code = err.exit_code().clamp(1, 255) as u8;
            std::process::ExitCode::from(code)
        }
    }
}
