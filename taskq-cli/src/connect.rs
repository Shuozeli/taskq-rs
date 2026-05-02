//! gRPC connection helper for the operator CLI.
//!
//! Mirrors `taskq-caller-sdk`'s `CallerClient::connect` shape: we build
//! an `http::Uri` from the operator-supplied endpoint string, hand it to
//! `TaskAdminClient::connect`, and surface failures through
//! [`crate::error::CliError`]. Auth-token wiring is a Phase 7 concern;
//! we accept and stash the token so the CLI is forward-compatible, but
//! v1 does not yet attach it as gRPC request metadata (the auth
//! interceptor in the CP is a stub — see `taskq-cp/src/handlers/task_admin.rs`
//! `TODO(phase-7-auth)` markers).

use grpc_client::Channel;
use taskq_proto::task_admin_client::TaskAdminClient;

use crate::error::CliError;

/// Open a gRPC channel to a `taskq-cp` server and return a typed
/// `TaskAdminClient`. The `endpoint` is parsed as an `http::Uri`; the
/// underlying transport supports `http://` only (HTTPS is a Phase 8
/// follow-up tracked in `taskq-caller-sdk` per `design.md` §11).
///
/// `_token` is accepted but unused in v1 — see the module docstring.
pub async fn connect_admin(
    endpoint: &str,
    _token: Option<&str>,
) -> Result<TaskAdminClient<Channel>, CliError> {
    // Parse the endpoint into a `Uri`. The `Display` form of the parse
    // error tends to be opaque ("invalid uri") so we lift the operator's
    // string into the message for a useful diagnostic.
    let uri: http::Uri = endpoint.parse().map_err(|err: http::uri::InvalidUri| {
        CliError::InvalidArgument(format!(
            "endpoint {endpoint:?} is not a valid http::Uri: {err}"
        ))
    })?;

    let client = TaskAdminClient::<Channel>::connect(uri)
        .await
        .map_err(|err| CliError::Connect {
            endpoint: endpoint.to_owned(),
            message: err.to_string(),
        })?;
    Ok(client)
}
