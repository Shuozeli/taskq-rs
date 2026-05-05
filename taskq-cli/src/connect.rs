//! gRPC connection helper for the operator CLI.
//!
//! Mirrors `taskq-caller-sdk`'s `CallerClient::connect` shape: we build
//! an `http::Uri` from the operator-supplied endpoint string, hand it
//! to `TaskAdminClient::connect`, and surface failures through
//! [`crate::error::CliError`].
//!
//! `--token` is accepted but not yet attached as `authorization`
//! metadata on outbound requests. The CP-side auth interceptor (in
//! `taskq-cp::server::auth_interceptor`) reads that header and stamps
//! the actor identity into `audit_log.actor`; until pure-grpc-rs
//! grows a per-call interceptor or this helper builds raw `Request`s
//! to attach metadata, all CLI requests show up with
//! `Actor::anonymous`. Tracked separately.

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
