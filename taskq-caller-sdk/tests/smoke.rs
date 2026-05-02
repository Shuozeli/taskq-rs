//! Smoke tests for `taskq-caller-sdk`.
//!
//! These tests stand up an in-process `pure-grpc-rs` server backed by a
//! programmable `TaskQueue` fake, point a `CallerClient` at it, and
//! verify the SDK's behaviour end-to-end:
//!
//! 1. `submit` auto-generates a UUIDv7 idempotency key when none is
//!    provided, and it is forwarded on the wire.
//! 2. `submit` retries `RESOURCE_EXHAUSTED`-with-`retryable=true`
//!    rejections and surfaces `retryable=false` immediately.
//! 3. `submit_and_wait` polls until the fake reports a terminal state.
//! 4. Trace context propagation: the wire `traceparent` is non-empty.
//!
//! The fake exposes its observed inputs through a shared `Arc<Mutex<>>`
//! so each test can assert on what the SDK actually sent.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use grpc_core::{Request, Response, Status};
use grpc_server::{NamedService, Router, Server};
use taskq_caller_sdk::{
    CallerClient, ClientError, RejectReason, SubmitOutcome, SubmitRequest, TaskId, TerminalOutcome,
    TerminalState,
};
use taskq_proto::task_queue_client::TaskQueueClient;
use taskq_proto::task_queue_server::{TaskQueue, TaskQueueServer};
use taskq_proto::{
    BatchGetTaskResultsRequest, BatchGetTaskResultsResponse, CancelTaskRequest, CancelTaskResponse,
    GetTaskResultRequest, GetTaskResultResponse, Rejection, SubmitAndWaitRequest,
    SubmitAndWaitResponse, SubmitTaskRequest, SubmitTaskResponse, Task, TaskOutcome, Timestamp,
};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'static>>;

// ---------------------------------------------------------------------------
// Fake server
// ---------------------------------------------------------------------------

/// Programmable behaviours for the fake `TaskQueue` server. Each test
/// crafts a `FakeTaskQueue` with the desired responses queued up.
#[derive(Default)]
struct FakeState {
    submit_responses: Vec<SubmitTaskResponse>,
    /// Captured `SubmitTaskRequest`s in arrival order. Tests assert on
    /// idempotency keys, traceparent bytes, etc.
    seen_submits: Vec<SubmitTaskRequest>,
    /// Per-attempt overrides for `get_task_result`. Index by attempt.
    get_responses: Vec<GetTaskResultResponse>,
    seen_gets: Vec<GetTaskResultRequest>,
    cancel_response: Option<CancelTaskResponse>,
    submit_and_wait_response: Option<SubmitAndWaitResponse>,
}

#[derive(Clone)]
struct FakeTaskQueue {
    state: Arc<Mutex<FakeState>>,
}

impl FakeTaskQueue {
    fn new() -> (Self, Arc<Mutex<FakeState>>) {
        let state = Arc::new(Mutex::new(FakeState::default()));
        (
            Self {
                state: Arc::clone(&state),
            },
            state,
        )
    }
}

impl TaskQueue for FakeTaskQueue {
    fn submit_task(
        &self,
        request: Request<SubmitTaskRequest>,
    ) -> BoxFuture<Result<Response<SubmitTaskResponse>, Status>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            let mut guard = state.lock().await;
            guard.seen_submits.push(req);
            let resp = if guard.submit_responses.is_empty() {
                SubmitTaskResponse::default()
            } else {
                guard.submit_responses.remove(0)
            };
            Ok(Response::new(resp))
        })
    }

    fn get_task_result(
        &self,
        request: Request<GetTaskResultRequest>,
    ) -> BoxFuture<Result<Response<GetTaskResultResponse>, Status>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let req = request.into_inner();
            let mut guard = state.lock().await;
            guard.seen_gets.push(req);
            let resp = if guard.get_responses.is_empty() {
                GetTaskResultResponse::default()
            } else {
                guard.get_responses.remove(0)
            };
            Ok(Response::new(resp))
        })
    }

    fn batch_get_task_results(
        &self,
        _request: Request<BatchGetTaskResultsRequest>,
    ) -> BoxFuture<Result<Response<BatchGetTaskResultsResponse>, Status>> {
        Box::pin(async move { Ok(Response::new(BatchGetTaskResultsResponse::default())) })
    }

    fn cancel_task(
        &self,
        _request: Request<CancelTaskRequest>,
    ) -> BoxFuture<Result<Response<CancelTaskResponse>, Status>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let guard = state.lock().await;
            let resp = guard.cancel_response.clone().unwrap_or_default();
            Ok(Response::new(resp))
        })
    }

    fn submit_and_wait(
        &self,
        _request: Request<SubmitAndWaitRequest>,
    ) -> BoxFuture<Result<Response<SubmitAndWaitResponse>, Status>> {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            let guard = state.lock().await;
            let resp = guard.submit_and_wait_response.clone().unwrap_or_default();
            Ok(Response::new(resp))
        })
    }
}

async fn start_server(fake: FakeTaskQueue) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = TaskQueueServer::new(fake);
    let router = Router::new().add_service(TaskQueueServer::<FakeTaskQueue>::NAME, server);

    tokio::spawn(async move {
        Server::builder()
            .serve_with_listener(listener, router)
            .await
            .unwrap();
    });

    // Give the server a moment to bind. Connecting too early racy with
    // the bind, so retry briefly.
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return addr;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("server did not become ready");
}

async fn connect_client(addr: SocketAddr) -> CallerClient {
    let uri: http::Uri = format!("http://{addr}").parse().unwrap();
    let client = TaskQueueClient::connect(uri).await.unwrap();
    CallerClient::from_grpc_client(client)
}

fn make_ok_submit_response(task_id: &str, existing: bool) -> SubmitTaskResponse {
    let mut r = SubmitTaskResponse::default();
    r.task_id = Some(task_id.to_owned());
    r.status = TerminalState::PENDING;
    r.existing_task = existing;
    r
}

fn make_rejection(reason: RejectReason, retryable: bool, retry_after_ms: i64) -> Rejection {
    let mut rej = Rejection::default();
    rej.reason = reason;
    rej.retryable = retryable;
    let mut ts = Timestamp::default();
    ts.unix_millis = retry_after_ms;
    rej.retry_after = Some(Box::new(ts));
    rej
}

fn make_rejected_submit_response(rej: Rejection) -> SubmitTaskResponse {
    let mut r = SubmitTaskResponse::default();
    r.status = TerminalState::PENDING;
    r.error = Some(Box::new(rej));
    r
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn submit_auto_generates_uuid_v7_idempotency_key() {
    // Arrange
    let (fake, state) = FakeTaskQueue::new();
    {
        let mut guard = state.lock().await;
        guard
            .submit_responses
            .push(make_ok_submit_response("task-1", false));
    }
    let addr = start_server(fake).await;
    let mut client = connect_client(addr).await;
    let req = SubmitRequest::new("ns", "type", Bytes::from_static(b"hello"));

    // Act
    let outcome = client.submit(req).await.unwrap();

    // Assert
    match outcome {
        SubmitOutcome::Created { task_id, status } => {
            assert_eq!(task_id.as_str(), "task-1");
            assert_eq!(status, TerminalState::PENDING);
        }
        other => panic!("expected Created, got {other:?}"),
    }
    let guard = state.lock().await;
    let seen = &guard.seen_submits[0];
    let key = seen
        .idempotency_key
        .as_deref()
        .expect("SDK should have populated idempotency_key");
    assert_eq!(key.len(), 36, "expected UUIDv7 string length");
    let parsed = uuid::Uuid::parse_str(key).expect("must parse as UUID");
    assert_eq!(parsed.get_version_num(), 7, "must be UUIDv7");
}

#[tokio::test]
async fn submit_returns_existing_outcome_when_dedup_hits() {
    // Arrange
    let (fake, state) = FakeTaskQueue::new();
    {
        let mut guard = state.lock().await;
        guard
            .submit_responses
            .push(make_ok_submit_response("task-7", true));
    }
    let addr = start_server(fake).await;
    let mut client = connect_client(addr).await;

    // Act
    let outcome = client
        .submit(SubmitRequest::new("ns", "type", Bytes::from_static(b"x")))
        .await
        .unwrap();

    // Assert
    assert!(matches!(outcome, SubmitOutcome::Existing { .. }));
}

#[tokio::test]
async fn submit_retries_retryable_rejection_then_succeeds() {
    // Arrange: first response is RESOURCE_EXHAUSTED with retryable=true,
    // second is success. The SDK must retry once.
    let (fake, state) = FakeTaskQueue::new();
    {
        let mut guard = state.lock().await;
        // retry_after = 0 means "now or in the past" — SDK floors to
        // 5 ms. Test runs sub-second.
        let rej = make_rejection(RejectReason::SYSTEM_OVERLOAD, true, 0);
        guard
            .submit_responses
            .push(make_rejected_submit_response(rej));
        guard
            .submit_responses
            .push(make_ok_submit_response("task-after-retry", false));
    }
    let addr = start_server(fake).await;
    let mut client = connect_client(addr).await.with_retry_budget(3);

    // Act
    let outcome = client
        .submit(SubmitRequest::new("ns", "type", Bytes::from_static(b"x")))
        .await
        .unwrap();

    // Assert
    match outcome {
        SubmitOutcome::Created { task_id, .. } => {
            assert_eq!(task_id.as_str(), "task-after-retry");
        }
        other => panic!("expected Created after retry, got {other:?}"),
    }
    let guard = state.lock().await;
    assert_eq!(guard.seen_submits.len(), 2, "SDK should have retried once");
}

#[tokio::test]
async fn submit_surfaces_non_retryable_rejection_immediately() {
    // Arrange: NAMESPACE_DISABLED is permanent (retryable=false).
    let (fake, state) = FakeTaskQueue::new();
    {
        let mut guard = state.lock().await;
        let rej = make_rejection(RejectReason::NAMESPACE_DISABLED, false, 0);
        guard
            .submit_responses
            .push(make_rejected_submit_response(rej));
    }
    let addr = start_server(fake).await;
    let mut client = connect_client(addr).await;

    // Act
    let err = client
        .submit(SubmitRequest::new("ns", "type", Bytes::from_static(b"x")))
        .await
        .unwrap_err();

    // Assert
    match err {
        ClientError::Rejected {
            reason, retryable, ..
        } => {
            assert_eq!(reason, RejectReason::NAMESPACE_DISABLED);
            assert!(!retryable);
        }
        other => panic!("expected Rejected, got {other:?}"),
    }
    let guard = state.lock().await;
    assert_eq!(
        guard.seen_submits.len(),
        1,
        "SDK must not retry retryable=false"
    );
}

#[tokio::test]
async fn submit_exhausts_budget_and_surfaces_last_rejection() {
    // Arrange: every response is retryable. The SDK should give up
    // after `budget` attempts.
    let (fake, state) = FakeTaskQueue::new();
    {
        let mut guard = state.lock().await;
        for _ in 0..5 {
            let rej = make_rejection(RejectReason::SYSTEM_OVERLOAD, true, 0);
            guard
                .submit_responses
                .push(make_rejected_submit_response(rej));
        }
    }
    let addr = start_server(fake).await;
    let mut client = connect_client(addr).await.with_retry_budget(2);

    // Act
    let err = client
        .submit(SubmitRequest::new("ns", "type", Bytes::from_static(b"x")))
        .await
        .unwrap_err();

    // Assert: 2 attempts max → 2 RPCs observed.
    assert!(matches!(err, ClientError::Rejected { .. }));
    let guard = state.lock().await;
    assert_eq!(guard.seen_submits.len(), 2);
}

#[tokio::test]
async fn submit_returns_payload_mismatch_outcome() {
    // Arrange: server returns IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD.
    let (fake, state) = FakeTaskQueue::new();
    {
        let mut guard = state.lock().await;
        let mut rej = make_rejection(
            RejectReason::IDEMPOTENCY_KEY_REUSE_WITH_DIFFERENT_PAYLOAD,
            false,
            0,
        );
        rej.existing_task_id = Some("existing-task-123".into());
        rej.existing_payload_hash = Some(vec![0xAB; 32]);
        guard
            .submit_responses
            .push(make_rejected_submit_response(rej));
    }
    let addr = start_server(fake).await;
    let mut client = connect_client(addr).await;

    // Act
    let outcome = client
        .submit(SubmitRequest::new("ns", "type", Bytes::from_static(b"x")))
        .await
        .unwrap();

    // Assert
    match outcome {
        SubmitOutcome::PayloadMismatch {
            existing_task_id,
            existing_payload_hash,
        } => {
            assert_eq!(existing_task_id.as_str(), "existing-task-123");
            assert_eq!(existing_payload_hash, [0xAB; 32]);
        }
        other => panic!("expected PayloadMismatch, got {other:?}"),
    }
}

#[tokio::test]
async fn submit_propagates_traceparent_on_the_wire() {
    // Arrange
    let (fake, state) = FakeTaskQueue::new();
    {
        let mut guard = state.lock().await;
        guard
            .submit_responses
            .push(make_ok_submit_response("t-1", false));
    }
    let addr = start_server(fake).await;
    let mut client = connect_client(addr).await;

    // Act: no active OTel context — SDK should still emit a fresh
    // 55-byte W3C traceparent.
    client
        .submit(SubmitRequest::new("ns", "type", Bytes::from_static(b"x")))
        .await
        .unwrap();

    // Assert
    let guard = state.lock().await;
    let seen = &guard.seen_submits[0];
    let traceparent = seen
        .traceparent
        .as_deref()
        .expect("SDK must populate traceparent");
    assert_eq!(
        traceparent.len(),
        55,
        "W3C version-00 traceparent is 55 bytes"
    );
    let s = std::str::from_utf8(traceparent).unwrap();
    assert!(s.starts_with("00-"));
}

#[tokio::test]
async fn submit_uses_default_namespace_when_request_namespace_is_empty() {
    // Arrange
    let (fake, state) = FakeTaskQueue::new();
    {
        let mut guard = state.lock().await;
        guard
            .submit_responses
            .push(make_ok_submit_response("t-1", false));
    }
    let addr = start_server(fake).await;
    let mut client = connect_client(addr)
        .await
        .with_default_namespace("default-ns");
    let mut req = SubmitRequest::new("", "type", Bytes::from_static(b"x"));
    req.namespace.clear();

    // Act
    client.submit(req).await.unwrap();

    // Assert
    let guard = state.lock().await;
    let seen = &guard.seen_submits[0];
    assert_eq!(seen.namespace.as_deref(), Some("default-ns"));
}

#[tokio::test]
async fn submit_rejects_empty_namespace_when_no_default() {
    // Arrange
    let (fake, _state) = FakeTaskQueue::new();
    let addr = start_server(fake).await;
    let mut client = connect_client(addr).await;
    let req = SubmitRequest::new("", "type", Bytes::from_static(b"x"));

    // Act
    let err = client.submit(req).await.unwrap_err();

    // Assert
    assert!(matches!(err, ClientError::InvalidArgument(_)));
}

#[tokio::test]
async fn submit_and_wait_polls_until_terminal() {
    // Arrange: submit_and_wait stub returns SYSTEM_OVERLOAD with the
    // task_id surfaced via existing_task_id (matches the Phase 5b CP
    // stub's behaviour). The SDK then polls get_result; we queue up
    // a non-terminal then a COMPLETED response.
    let (fake, state) = FakeTaskQueue::new();
    {
        let mut guard = state.lock().await;
        let mut saw = SubmitAndWaitResponse::default();
        let mut rej = Rejection::default();
        rej.reason = RejectReason::SYSTEM_OVERLOAD;
        rej.retryable = true;
        rej.existing_task_id = Some("polled-task".into());
        saw.error = Some(Box::new(rej));
        guard.submit_and_wait_response = Some(saw);

        // Two get_result responses: PENDING then COMPLETED.
        let mut pending = GetTaskResultResponse::default();
        let mut t1 = Task::default();
        t1.task_id = Some("polled-task".into());
        t1.status = TerminalState::PENDING;
        pending.task = Some(Box::new(t1));
        guard.get_responses.push(pending);

        let mut done = GetTaskResultResponse::default();
        let mut t2 = Task::default();
        t2.task_id = Some("polled-task".into());
        t2.status = TerminalState::COMPLETED;
        done.task = Some(Box::new(t2));
        done.outcome = TaskOutcome::SUCCESS;
        done.result_payload = Some(b"result-bytes".to_vec());
        guard.get_responses.push(done);
    }
    let addr = start_server(fake).await;
    let mut client = connect_client(addr)
        .await
        .with_poll_interval(Duration::from_millis(10));
    let req = SubmitRequest::new("ns", "type", Bytes::from_static(b"x"));

    // Act
    let outcome = client
        .submit_and_wait(req, Duration::from_secs(5))
        .await
        .unwrap();

    // Assert
    match outcome {
        TerminalOutcome::Settled(state) => {
            assert_eq!(state.status, TerminalState::COMPLETED);
            assert_eq!(state.task_id.as_str(), "polled-task");
            assert_eq!(
                state.result_payload.as_deref(),
                Some(b"result-bytes" as &[u8])
            );
        }
        TerminalOutcome::TimedOut { .. } => panic!("expected Settled"),
    }
}

#[tokio::test]
async fn cancel_returns_cancelled_outcome_for_active_task() {
    // Arrange
    let (fake, state) = FakeTaskQueue::new();
    {
        let mut guard = state.lock().await;
        let mut resp = CancelTaskResponse::default();
        resp.task_id = Some("t-cancel".into());
        resp.final_status = TerminalState::CANCELLED;
        guard.cancel_response = Some(resp);
    }
    let addr = start_server(fake).await;
    let mut client = connect_client(addr).await;
    let task_id = TaskId::new("t-cancel");

    // Act
    let outcome = client.cancel(&task_id).await.unwrap();

    // Assert
    use taskq_caller_sdk::CancelOutcome;
    match outcome {
        CancelOutcome::Cancelled { task_id } => {
            assert_eq!(task_id.as_str(), "t-cancel");
        }
        other => panic!("expected Cancelled, got {other:?}"),
    }
}

#[tokio::test]
async fn cancel_returns_already_terminal_for_completed_task() {
    // Arrange
    let (fake, state) = FakeTaskQueue::new();
    {
        let mut guard = state.lock().await;
        let mut resp = CancelTaskResponse::default();
        resp.task_id = Some("t-done".into());
        resp.final_status = TerminalState::COMPLETED;
        guard.cancel_response = Some(resp);
    }
    let addr = start_server(fake).await;
    let mut client = connect_client(addr).await;

    // Act
    let outcome = client.cancel(&TaskId::new("t-done")).await.unwrap();

    // Assert
    use taskq_caller_sdk::CancelOutcome;
    match outcome {
        CancelOutcome::AlreadyTerminal { final_status, .. } => {
            assert_eq!(final_status, TerminalState::COMPLETED);
        }
        other => panic!("expected AlreadyTerminal, got {other:?}"),
    }
}
