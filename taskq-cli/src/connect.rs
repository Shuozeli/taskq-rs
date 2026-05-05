//! gRPC connection helper for the operator CLI.
//!
//! Mirrors `taskq-caller-sdk`'s `CallerClient::connect` shape: we build
//! an `http::Uri` from the operator-supplied endpoint string, hand it
//! to `TaskAdminClient::connect`, and surface failures through
//! [`crate::error::CliError`].
//!
//! `--token` is wrapped onto every outbound request via
//! [`auth_request`] from each command call site. The CP-side
//! `auth_interceptor` reads the `authorization` header and stamps
//! the actor identity into `audit_log.actor`. Without `--token` the
//! requests arrive as `Actor::anonymous`.

use grpc_client::Channel;
use grpc_core::Request;
use taskq_proto::task_admin_client::TaskAdminClient;

use crate::error::CliError;

/// Wrap a request body with the `authorization: Bearer <token>`
/// metadata header when `token` is present. Used by every command in
/// `commands.rs` to pass the operator-supplied `--token` along on
/// outbound requests so the CP-side `auth_interceptor` can stamp the
/// audit-log actor.
///
/// `None` returns a plain request with no auth metadata, which the CP
/// interceptor maps to `Actor::anonymous`.
pub fn auth_request<T>(token: Option<&str>, body: T) -> Request<T> {
    let mut req = Request::new(body);
    if let Some(token) = token {
        // `parse` only fails for non-ASCII; tokens in practice are
        // ASCII-safe (UUIDs, JWTs, base64). On failure we fall through
        // and the server treats the request as anonymous -- safer than
        // panicking on a malformed `--token`.
        if let Ok(value) = format!("Bearer {token}").parse() {
            req.metadata_mut().insert("authorization", value);
        }
    }
    req
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_request_omits_header_when_token_absent() {
        // Arrange / Act
        let req = auth_request::<u32>(None, 42);

        // Assert
        assert!(req.metadata().get("authorization").is_none());
        assert_eq!(*req.get_ref(), 42);
    }

    #[test]
    fn auth_request_attaches_bearer_prefixed_token() {
        // Arrange / Act
        let req = auth_request::<u32>(Some("svc-account-7"), 42);

        // Assert
        let value = req
            .metadata()
            .get("authorization")
            .expect("authorization metadata must be set when token is present");
        assert_eq!(value.to_str().unwrap(), "Bearer svc-account-7");
    }
}
