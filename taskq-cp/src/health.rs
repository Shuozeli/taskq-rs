//! Health and metrics HTTP server.
//!
//! `design.md` §11.5 / `tasks.md` §5.7.
//!
//! Routes:
//!
//! - `GET /healthz` — process liveness. 200 unless shutdown was signalled.
//! - `GET /readyz` — storage reachable + schema version match + strategies
//!   loaded. Lists degraded namespaces by name in the body.
//! - `GET /metrics` — Prometheus exposition (only when the operator
//!   selected `OtelExporterConfig::Prometheus`).
//!
//! The health server runs on a separate port from the gRPC server so
//! cluster liveness checks never compete with worker traffic.
//!
//! Built on `axum` for ergonomic routing without dragging in a second
//! gRPC framework.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use prometheus::{Encoder, Registry, TextEncoder};

use crate::error::{CpError, Result};
use crate::shutdown::{wait_for_shutdown, ShutdownReceiver};
use crate::state::CpState;

/// Shared state for the health router. Carries the CP state plus the
/// optional Prometheus registry — `None` for non-Prometheus exporters
/// (in which case `/metrics` returns 404).
#[derive(Clone)]
pub struct HealthState {
    pub cp: Arc<CpState>,
    pub prometheus_registry: Option<Registry>,
}

/// Build the axum `Router`. `main` calls this and hands the router to
/// `axum::serve`.
pub fn build_router(state: HealthState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Spawn the health server. Returns when the shutdown signal fires.
pub async fn serve(state: HealthState, shutdown: ShutdownReceiver) -> Result<()> {
    let bind = state.cp.config.health_addr;
    let router = build_router(state);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|err| CpError::transport(format!("health server bind {bind}: {err}")))?;
    tracing::info!(addr = %bind, "starting taskq-cp health server");

    axum::serve(listener, router)
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await
        .map_err(|err| CpError::transport(format!("health server: {err}")))?;
    tracing::info!("taskq-cp health server shut down cleanly");
    Ok(())
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

async fn healthz(State(state): State<HealthState>) -> Response {
    if *state.cp.shutdown.borrow() {
        // Once we've started draining we still want to be reachable for
        // log scraping but Kubernetes-style readiness should return 503.
        // For a process-liveness check we keep returning 200 — a process
        // that is draining is still "alive". /readyz is the right place
        // to flip during drain.
        (StatusCode::OK, "draining").into_response()
    } else {
        (StatusCode::OK, "ok").into_response()
    }
}

async fn readyz(State(state): State<HealthState>) -> Response {
    // 1. Shutdown gate — if we've been signalled, fail readiness so the
    //    load balancer drains us.
    if *state.cp.shutdown.borrow() {
        return (StatusCode::SERVICE_UNAVAILABLE, "shutting down").into_response();
    }

    // 2. Storage reachability + schema-version + strategy-load checks.
    //    Phase 5b will populate this body — we need a `Storage::ping` or
    //    equivalent and a `taskq_meta` read. Phase 5a returns 200 with
    //    the degraded-namespace summary already populated from the
    //    strategy registry.
    let degraded = state.cp.strategy_registry.degraded();
    let healthy = state.cp.strategy_registry.healthy_count();
    let body = if degraded.is_empty() {
        format!("ready: {healthy} namespace(s) loaded\n")
    } else {
        let mut body = format!(
            "ready (degraded): {healthy} namespace(s) loaded; {} degraded:\n",
            degraded.len()
        );
        for entry in degraded {
            body.push_str(&format!("- {}: {}\n", entry.namespace, entry.reason));
        }
        body
    };

    // Degraded namespaces don't fail readiness: per `design.md` §9.3 we
    // continue serving the rest. The body lists them so operators see it.
    (StatusCode::OK, body).into_response()
}

async fn metrics(State(state): State<HealthState>) -> Response {
    let Some(registry) = state.prometheus_registry else {
        return (
            StatusCode::NOT_FOUND,
            "metrics endpoint disabled; configure otel_exporter = \"prometheus\"\n",
        )
            .into_response();
    };

    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    let mut buf = Vec::new();
    if let Err(err) = encoder.encode(&metric_families, &mut buf) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to encode metrics: {err}"),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, encoder.format_type())],
        buf,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use std::net::SocketAddr;
    use taskq_storage::Namespace;
    use tower::ServiceExt;

    /// Build a `HealthState` whose CpState does the bare minimum so the
    /// router can answer requests. The storage is a never-used stub —
    /// `/healthz` and `/readyz` don't currently invoke it (Phase 5b will).
    fn fake_state(register_degraded: bool) -> HealthState {
        let (_tx, rx) = crate::shutdown::channel();
        let metrics = crate::observability::MetricsHandle::noop();
        let config = Arc::new(crate::config::CpConfig {
            bind_addr: "127.0.0.1:50051".parse::<SocketAddr>().unwrap(),
            health_addr: "127.0.0.1:9090".parse::<SocketAddr>().unwrap(),
            storage_backend: crate::config::StorageBackendConfig::Sqlite {
                path: ":memory:".to_owned(),
            },
            otel_exporter: crate::config::OtelExporterConfig::Disabled,
            quota_cache_ttl_seconds: 5,
            long_poll_default_timeout_seconds: 30,
            long_poll_max_seconds: 60,
            belt_and_suspenders_seconds: 10,
            waiter_limit_per_replica: 5_000,
            lease_window_seconds: 30,
        });
        let mut registry = crate::strategy::StrategyRegistry::empty();
        if register_degraded {
            registry.mark_degraded(Namespace::new("broken"), "missing strategy");
        }

        let storage: Arc<dyn crate::state::DynStorage> = Arc::new(NoopStorage);
        let cp = Arc::new(CpState::new(
            storage,
            Arc::new(registry),
            metrics,
            rx,
            config,
        ));
        HealthState {
            cp,
            prometheus_registry: None,
        }
    }

    /// Empty stand-in implementing `DynStorage` for tests that don't touch
    /// storage. The `begin_dyn` impl always errors; the health-server tests
    /// never start a transaction, so the path is unreachable in practice.
    struct NoopStorage;
    impl crate::state::DynStorage for NoopStorage {
        fn begin_dyn(
            &self,
        ) -> crate::state::StorageTxFuture<
            '_,
            std::result::Result<
                Box<dyn crate::state::StorageTxDyn + '_>,
                taskq_storage::StorageError,
            >,
        > {
            Box::pin(async move {
                Err(taskq_storage::StorageError::backend(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "NoopStorage::begin_dyn — health-server tests do not exercise transactions",
                )))
            })
        }
    }

    #[tokio::test]
    async fn healthz_returns_ok_in_steady_state() {
        // Arrange
        let router = build_router(fake_state(false));

        // Act
        let resp = router
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Assert
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(body.as_ref(), b"ok");
    }

    #[tokio::test]
    async fn readyz_lists_degraded_namespaces() {
        // Arrange
        let router = build_router(fake_state(true));

        // Act
        let resp = router
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Assert
        assert_eq!(resp.status(), StatusCode::OK);
        let body =
            String::from_utf8(to_bytes(resp.into_body(), 4096).await.unwrap().to_vec()).unwrap();
        assert!(body.contains("degraded"));
        assert!(body.contains("broken"));
    }

    #[tokio::test]
    async fn metrics_returns_404_when_prometheus_disabled() {
        // Arrange
        let router = build_router(fake_state(false));

        // Act
        let resp = router
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Assert
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
