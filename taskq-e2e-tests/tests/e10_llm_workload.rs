//! E2E mixed-LLM workload: n requesters → m namespaces → k workers,
//! where each worker is registered for a specific subset of
//! (namespace, task_type) pairs that simulate "this worker can run
//! these models". Verifies that the CP's dispatch filter routes each
//! task to a worker that actually advertised its task_type, and that
//! the whole batch settles to COMPLETED inside a reasonable wall
//! budget.
//!
//! No real LLMs — handlers sleep a few ms based on task_type to
//! simulate per-model latency variance and return canned bytes.
//!
//! Topology
//! --------
//!
//! Namespaces (m=3): `chat`, `embedding`, `summarize`.
//! Task types (6 total) representing model identifiers.
//! Workers (k=3):
//!   - `chat-fast`        : chat=[gpt-4]                          c=3
//!   - `claude-multi`     : chat=[claude-3], summarize=[claude-3] c=2
//!   - `embedding-pool`   : embedding=[ada-002, bge-large]        c=4
//!
//! Submits 26 tasks across the namespaces with mixed priorities,
//! drains, then asserts:
//! 1. Every task lands COMPLETED.
//! 2. Every task was handled by a worker registered for its
//!    (namespace, task_type) — i.e. CP dispatch never picked a task
//!    for a worker that didn't advertise the task_type.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use taskq_caller_sdk::{SubmitOutcome, SubmitRequest, TaskId};
use taskq_e2e_tests::{require_docker, wait_for, ComposeFile, ComposeStack};
use taskq_proto::TerminalState;
use taskq_worker_sdk::{AcquiredTask, HandlerOutcome, TaskHandler, WorkerBuilder};
use tokio::sync::Mutex;

/// Per-task recording the handler stamps when it picks the task up.
#[derive(Debug, Clone)]
struct Stamp {
    worker_label: &'static str,
    namespace: String,
    task_type: String,
}

type Recorder = Arc<Mutex<HashMap<String, Stamp>>>;

struct LlmHandler {
    label: &'static str,
    recorder: Recorder,
}

impl TaskHandler for LlmHandler {
    async fn handle(&self, task: AcquiredTask) -> HandlerOutcome {
        // Simulate per-model latency. Cheap so the test stays fast.
        let sleep_ms = match task.task_type.as_str() {
            "gpt-4" => 80,
            "claude-3" => 120,
            "ada-002" => 30,
            "bge-large" => 60,
            _ => 50,
        };
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

        let stamp = Stamp {
            worker_label: self.label,
            namespace: namespace_from_label(self.label, &task.task_type).to_owned(),
            task_type: task.task_type.clone(),
        };
        self.recorder
            .lock()
            .await
            .insert(task.task_id.clone(), stamp);

        HandlerOutcome::Success(Bytes::from(format!(
            "ok-{label}-{tt}",
            label = self.label,
            tt = task.task_type
        )))
    }
}

/// Per-worker the test wires up: which namespace it watches and
/// which task_types it advertises. The worker SDK only lets a
/// builder register against ONE namespace, so a worker that wants to
/// span namespaces (like `claude-multi` here) needs one builder per
/// namespace running concurrently. That's a deliberate v1 design
/// choice — namespaces are the dispatch unit.
struct WorkerSpec {
    label: &'static str,
    namespace: &'static str,
    task_types: Vec<&'static str>,
    concurrency: usize,
}

fn worker_specs() -> Vec<WorkerSpec> {
    vec![
        WorkerSpec {
            label: "chat-fast",
            namespace: "chat",
            task_types: vec!["gpt-4"],
            concurrency: 3,
        },
        // claude-multi spans two namespaces; we'll spin up two
        // builders sharing the same label + recorder.
        WorkerSpec {
            label: "claude-multi",
            namespace: "chat",
            task_types: vec!["claude-3"],
            concurrency: 2,
        },
        WorkerSpec {
            label: "claude-multi",
            namespace: "summarize",
            task_types: vec!["claude-3"],
            concurrency: 2,
        },
        WorkerSpec {
            label: "embedding-pool",
            namespace: "embedding",
            task_types: vec!["ada-002", "bge-large"],
            concurrency: 4,
        },
    ]
}

/// Static map: which (namespace, task_type) is each worker label
/// allowed to handle. Used by the assertion sweep.
fn allowed_for(label: &str) -> Vec<(&'static str, &'static str)> {
    match label {
        "chat-fast" => vec![("chat", "gpt-4")],
        "claude-multi" => vec![("chat", "claude-3"), ("summarize", "claude-3")],
        "embedding-pool" => vec![("embedding", "ada-002"), ("embedding", "bge-large")],
        _ => vec![],
    }
}

/// Small lookup so the handler can stamp the namespace it ran
/// against — `AcquiredTask` does not carry the namespace, so we
/// derive it from (worker_label, task_type).
fn namespace_from_label(label: &str, task_type: &str) -> &'static str {
    for (ns, tt) in allowed_for(label) {
        if tt == task_type {
            return ns;
        }
    }
    "<unknown>"
}

/// Submit-spec: which namespace, task_type, priority, and how many.
/// Mixed priorities so the run exercises both the
/// default-priority and high-priority dispatch paths.
fn submit_plan() -> Vec<(&'static str, &'static str, i32, usize)> {
    vec![
        // chat namespace: 8 gpt-4 + 4 claude-3 (mix of low/high pri)
        ("chat", "gpt-4", 1, 6),
        ("chat", "gpt-4", 10, 2),
        ("chat", "claude-3", 1, 3),
        ("chat", "claude-3", 10, 1),
        // embedding namespace: 10 mixed across two models, all bg pri
        ("embedding", "ada-002", 1, 6),
        ("embedding", "bge-large", 1, 4),
        // summarize namespace: 4 claude-3 (some interactive)
        ("summarize", "claude-3", 1, 2),
        ("summarize", "claude-3", 10, 2),
    ]
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires docker"]
async fn llm_workload_routes_tasks_by_specialization() -> Result<()> {
    // Arrange: fresh stack, three namespaces seeded.
    require_docker()?;
    let stack = ComposeStack::up(ComposeFile::Base)?;
    for ns in ["chat", "embedding", "summarize"] {
        stack.seed_namespace(ns).await?;
    }
    let endpoint = stack.primary_endpoint();

    let recorder: Recorder = Arc::new(Mutex::new(HashMap::new()));

    // Spin up every (worker_label x namespace) registration as its
    // own SDK Worker. Each registration shares the recorder so the
    // post-run audit sees every handled task in one map.
    let mut worker_handles = Vec::new();
    for spec in worker_specs() {
        let task_types: Vec<String> = spec.task_types.iter().map(|s| (*s).to_owned()).collect();
        let worker = WorkerBuilder::new(endpoint.clone(), spec.namespace)
            .with_task_types(task_types)
            .with_concurrency(spec.concurrency)
            .with_long_poll_timeout(Duration::from_secs(2))
            .with_handler(LlmHandler {
                label: spec.label,
                recorder: Arc::clone(&recorder),
            })
            .build()
            .await
            .map_err(|e| anyhow!("worker build {label}: {e}", label = spec.label))?;
        worker_handles.push(tokio::spawn(async move {
            let _ = worker.run_until_shutdown().await;
        }));
    }

    // Let the workers register their long-poll waiters before any
    // tasks land so the first round of NOTIFYs hits parked waiters.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Act: fan out the submit plan over multiple caller channels so
    // the wire path actually races (mirrors a real fleet of
    // requesters submitting concurrently).
    let mut all_task_ids: Vec<TaskId> = Vec::new();
    let mut submit_handles = Vec::new();
    let mut idem_counter = 0u32;
    for (namespace, task_type, priority, count) in submit_plan() {
        for _ in 0..count {
            idem_counter += 1;
            let stack_endpoint = endpoint.clone();
            let key = format!("llm-e2e-{idem_counter}");
            submit_handles.push(tokio::spawn(async move {
                let mut caller = caller_at(stack_endpoint).await?;
                let mut req = SubmitRequest::new(
                    namespace,
                    task_type,
                    Bytes::from(format!("payload-for-{key}")),
                );
                req.idempotency_key = Some(key);
                req.priority = Some(priority);
                let outcome = caller
                    .submit(req)
                    .await
                    .map_err(|e| anyhow!("submit: {e}"))?;
                match outcome {
                    SubmitOutcome::Created { task_id, .. } => Ok::<TaskId, anyhow::Error>(task_id),
                    other => Err(anyhow!("expected Created, got {other:?}")),
                }
            }));
        }
    }
    for h in submit_handles {
        all_task_ids.push(h.await.map_err(|e| anyhow!("join: {e}"))??);
    }
    let expected: usize = submit_plan().iter().map(|(_, _, _, n)| n).sum();
    assert_eq!(
        all_task_ids.len(),
        expected,
        "every submit_plan entry should yield a task_id"
    );

    // Drain: poll one shared caller for every task's terminal status.
    let drain_caller = caller_at(endpoint.clone()).await?;
    let drain_caller = Arc::new(Mutex::new(drain_caller));
    let task_ids = all_task_ids.clone();
    wait_for(Duration::from_secs(60), Duration::from_millis(500), || {
        let drain_caller = Arc::clone(&drain_caller);
        let ids = task_ids.clone();
        async move {
            let mut caller = drain_caller.lock().await;
            for id in &ids {
                let st = caller
                    .get_result(id)
                    .await
                    .map_err(|e| anyhow!("get: {e}"))?;
                if st.status != TerminalState::COMPLETED {
                    return Ok(None);
                }
            }
            Ok(Some(()))
        }
    })
    .await?;

    // Assert: every task was handled by a worker registered for its
    // (namespace, task_type).
    let recorded = recorder.lock().await.clone();
    assert_eq!(
        recorded.len(),
        expected,
        "every task should have been handled exactly once"
    );

    let mut routing_violations: Vec<String> = Vec::new();
    for id in &all_task_ids {
        let key = id.as_str();
        let stamp = recorded
            .get(key)
            .ok_or_else(|| anyhow!("task {key} was not handled by any worker"))?;
        let allowed = allowed_for(stamp.worker_label);
        if !allowed
            .iter()
            .any(|(ns, tt)| *ns == stamp.namespace && *tt == stamp.task_type)
        {
            routing_violations.push(format!(
                "task {id}: worker {label} handled ({ns}, {tt}) which it did not register for",
                label = stamp.worker_label,
                ns = stamp.namespace,
                tt = stamp.task_type,
            ));
        }
    }
    assert!(
        routing_violations.is_empty(),
        "dispatch routing violations: {routing_violations:#?}"
    );

    // Sanity: each worker label actually saw at least one task.
    let mut by_label: HashMap<&'static str, usize> = HashMap::new();
    for stamp in recorded.values() {
        *by_label.entry(stamp.worker_label).or_insert(0) += 1;
    }
    for label in ["chat-fast", "claude-multi", "embedding-pool"] {
        assert!(
            by_label.get(label).copied().unwrap_or(0) > 0,
            "worker {label} handled zero tasks; routing imbalance"
        );
    }

    for h in worker_handles {
        h.abort();
    }
    Ok(())
}

/// Standalone caller-builder so `tokio::spawn` blocks (which require
/// `'static` futures) don't borrow the `ComposeStack`. Equivalent to
/// `stack.caller_at(uri)` minus the `&self` borrow.
async fn caller_at(uri: http::Uri) -> Result<taskq_caller_sdk::CallerClient> {
    use grpc_client::Channel;
    use taskq_proto::task_queue_client::TaskQueueClient;
    let client = TaskQueueClient::<Channel>::connect(uri)
        .await
        .map_err(|err| anyhow!("connect TaskQueueClient: {err}"))?;
    Ok(taskq_caller_sdk::CallerClient::from_grpc_client(client))
}
