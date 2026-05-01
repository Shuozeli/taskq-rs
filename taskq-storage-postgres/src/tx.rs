//! `StorageTx` implementation for Postgres.
//!
//! One [`PostgresTx`] wraps one `BEGIN ISOLATION LEVEL SERIALIZABLE`
//! connection checked out of the pool. `commit` / `rollback` consume the
//! handle and return the connection to the pool. The READ COMMITTED
//! heartbeat carve-out (`design.md` §1.1, §6.3) is implemented by
//! `record_worker_heartbeat` checking out a separate connection from the
//! pool — it never rides the SERIALIZABLE transaction.
//!
//! The implementation favours one SQL statement per trait method when
//! possible; the comments above each block point at the design.md section
//! the SQL implements.

use std::sync::Arc;

use std::collections::HashMap;

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use deadpool_postgres::{Object as PgConn, Pool};
use futures_core::Stream;
use taskq_storage::error::Result;
use taskq_storage::ids::{IdempotencyKey, Namespace, TaskId, TaskType, Timestamp, WorkerId};
use taskq_storage::types::{
    AuditEntry, CancelOutcome, CapacityDecision, CapacityKind, DeadWorkerRuntime, DedupRecord,
    ExpiredRuntime, HeartbeatAck, LeaseRef, LockedTask, NamespaceFilter, NamespaceQuota,
    NamespaceQuotaUpsert, NewDedupRecord, NewLease, NewTask, PickCriteria, PickOrdering,
    RateDecision, RateKind, ReplayOutcome, RuntimeRef, Task, TaskFilter, TaskOutcome, TaskStatus,
    TaskTypeFilter, TerminalState, WakeSignal, WorkerInfo,
};
use taskq_storage::{StorageError, StorageTx};
use tokio::sync::mpsc;
use tokio_postgres::types::ToSql;
use tokio_postgres::Row;
use uuid::Uuid;

use crate::errors::{map_db_error, map_pool_error};
use crate::listener::Dispatcher;
use crate::rate::RateLimiter;

/// One in-flight SERIALIZABLE transaction.
///
/// The transaction is opened with raw `BEGIN`/`COMMIT` rather than
/// `tokio_postgres::Transaction` so that the lifetime of the transaction is
/// tied to this struct alone — the deadpool `Object` is held until
/// `commit` / `rollback`.
pub struct PostgresTx<'a> {
    conn: PgConn,
    rate_limiter: Arc<RateLimiter>,
    dispatcher: Arc<Dispatcher>,
    /// Connection pool used for the heartbeat carve-out (separate
    /// READ COMMITTED side connection).
    pool: Arc<Pool>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> PostgresTx<'a> {
    pub(crate) fn new(
        conn: PgConn,
        rate_limiter: Arc<RateLimiter>,
        dispatcher: Arc<Dispatcher>,
        pool: Arc<Pool>,
    ) -> Self {
        Self {
            conn,
            rate_limiter,
            dispatcher,
            pool,
            _marker: std::marker::PhantomData,
        }
    }
}

// `Drop` for `PostgresTx` is intentionally implicit: the wrapped deadpool
// `Object` returns the connection to the pool on drop. The pool's
// `RecyclingMethod::Custom("ROLLBACK")` (configured in
// [`crate::storage::PostgresStorage::connect`]) issues `ROLLBACK` at the
// next checkout, so a dropped-without-commit transaction does not poison
// subsequent pool users.

// Helper: convert taskq Timestamp <-> chrono DateTime<Utc>.
fn timestamp_to_chrono(ts: Timestamp) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ts.as_unix_millis())
        .single()
        .expect("storage Timestamp must convert to UTC chrono")
}

fn chrono_to_timestamp(dt: DateTime<Utc>) -> Timestamp {
    Timestamp::from_unix_millis(dt.timestamp_millis())
}

// SQL fragments that translate `PickOrdering` to native ORDER BY clauses.
//
// These are tested implicitly by the smoke tests / Phase 9 conformance.
fn pick_ordering_sql(ordering: PickOrdering) -> String {
    match ordering {
        PickOrdering::PriorityFifo => {
            "ORDER BY priority DESC, submitted_at ASC".to_owned()
        }
        PickOrdering::AgePromoted { age_weight } => format!(
            "ORDER BY (priority + LEAST(2147483647, GREATEST(-2147483648, (EXTRACT(EPOCH FROM (NOW() - submitted_at)) * {age_weight})::bigint))) DESC, submitted_at ASC",
        ),
        // RandomNamespace is dispatched separately in `pick_and_lock_pending`
        // because it requires sampling a namespace first; the SQL fragment
        // below is the inner per-namespace ordering.
        PickOrdering::RandomNamespace { .. } => {
            "ORDER BY priority DESC, submitted_at ASC".to_owned()
        }
    }
}

// `serial_status` rows are uppercase-strings matching the `taskq_task_status`
// enum from migrations/0001_initial.sql.
const STATUS_PENDING: &str = "PENDING";
const STATUS_DISPATCHED: &str = "DISPATCHED";
const STATUS_COMPLETED: &str = "COMPLETED";
const STATUS_FAILED_NONRETRYABLE: &str = "FAILED_NONRETRYABLE";
const STATUS_FAILED_EXHAUSTED: &str = "FAILED_EXHAUSTED";
const STATUS_EXPIRED: &str = "EXPIRED";
const STATUS_WAITING_RETRY: &str = "WAITING_RETRY";

// Outcome strings matching the `taskq_outcome` enum.
const OUTCOME_SUCCESS: &str = "success";
const OUTCOME_RETRYABLE_FAIL: &str = "retryable_fail";
const OUTCOME_NONRETRYABLE_FAIL: &str = "nonretryable_fail";
const OUTCOME_EXPIRED: &str = "expired";

impl<'a> StorageTx for PostgresTx<'a> {
    // ========================================================================
    // Submit path
    // ========================================================================

    async fn lookup_idempotency(
        &mut self,
        namespace: &Namespace,
        key: &IdempotencyKey,
    ) -> Result<Option<DedupRecord>> {
        // §6.1 step 2: read dedup row, ignoring already-expired rows.
        let row = self
            .conn
            .query_opt(
                "SELECT task_id, payload_hash, expires_at \
                   FROM idempotency_keys \
                  WHERE namespace = $1 AND key = $2 AND expires_at > NOW() \
                  LIMIT 1",
                &[&namespace.as_str(), &key.as_str()],
            )
            .await
            .map_err(map_db_error)?;

        let Some(row) = row else {
            return Ok(None);
        };
        let task_id: Uuid = row.get(0);
        let payload_hash_bytes: Vec<u8> = row.get(1);
        let expires_at: DateTime<Utc> = row.get(2);

        let payload_hash: [u8; 32] = payload_hash_bytes.as_slice().try_into().map_err(|_| {
            StorageError::ConstraintViolation(
                "idempotency_keys.payload_hash != 32 bytes".to_owned(),
            )
        })?;

        Ok(Some(DedupRecord {
            task_id: TaskId::from_uuid(task_id),
            payload_hash,
            expires_at: chrono_to_timestamp(expires_at),
        }))
    }

    async fn insert_task(&mut self, task: NewTask, dedup: NewDedupRecord) -> Result<TaskId> {
        // §6.1 steps 4-5: insert tasks + idempotency_keys atomically.
        let submitted_at = timestamp_to_chrono(task.submitted_at);
        let expires_at = timestamp_to_chrono(task.expires_at);
        let dedup_expires_at = timestamp_to_chrono(dedup.expires_at);
        let task_id = task.task_id.into_uuid();
        let payload_hash_vec = task.payload_hash.to_vec();
        let payload_bytes = task.payload.to_vec();
        let traceparent = task.traceparent.to_vec();
        let tracestate = task.tracestate.to_vec();

        self.conn
            .execute(
                "INSERT INTO tasks (\
                    task_id, namespace, task_type, status, priority, \
                    payload, payload_hash, submitted_at, expires_at, \
                    attempt_number, max_retries, retry_initial_ms, \
                    retry_max_ms, retry_coefficient, traceparent, \
                    tracestate, format_version) \
                 VALUES ($1, $2, $3, $4::text::taskq_task_status, $5, $6, $7, $8, $9, 0, $10, $11, $12, $13, $14, $15, $16)",
                &[
                    &task_id,
                    &task.namespace.as_str(),
                    &task.task_type.as_str(),
                    &STATUS_PENDING,
                    &task.priority,
                    &payload_bytes,
                    &payload_hash_vec,
                    &submitted_at,
                    &expires_at,
                    &(task.max_retries as i32),
                    &(task.retry_initial_ms as i64),
                    &(task.retry_max_ms as i64),
                    &task.retry_coefficient,
                    &traceparent,
                    &tracestate,
                    &(task.format_version as i32),
                ],
            )
            .await
            .map_err(map_db_error)?;

        let dedup_payload_hash = dedup.payload_hash.to_vec();
        self.conn
            .execute(
                "INSERT INTO idempotency_keys (\
                    namespace, key, task_id, payload_hash, expires_at) \
                 VALUES ($1, $2, $3, $4, $5)",
                &[
                    &dedup.namespace.as_str(),
                    &dedup.key.as_str(),
                    &task_id,
                    &dedup_payload_hash,
                    &dedup_expires_at,
                ],
            )
            .await
            .map_err(map_db_error)?;

        Ok(task.task_id)
    }

    // ========================================================================
    // Dispatch path
    // ========================================================================

    async fn pick_and_lock_pending(
        &mut self,
        criteria: PickCriteria,
    ) -> Result<Option<LockedTask>> {
        // §6.2 step 2: SELECT with FOR UPDATE SKIP LOCKED.
        // RandomNamespace dispatch first samples a namespace; for
        // simplicity v1 implements that as a single random pick.
        let now = timestamp_to_chrono(criteria.now);

        // Resolve namespace filter into a parameterized list.
        let namespace_clause = match &criteria.namespace_filter {
            NamespaceFilter::Single(ns) => Some(vec![ns.as_str().to_owned()]),
            NamespaceFilter::AnyOf(namespaces) => Some(
                namespaces
                    .iter()
                    .map(|n| n.as_str().to_owned())
                    .collect::<Vec<_>>(),
            ),
            NamespaceFilter::Any => None,
        };

        // Resolve task-type filter.
        let TaskTypeFilter::AnyOf(task_types) = &criteria.task_types_filter;
        let task_types: Vec<String> = task_types.iter().map(|t| t.as_str().to_owned()).collect();

        // Special-case RandomNamespace: pick a random pending namespace
        // first, then run PriorityFifo within it.
        let chosen_namespaces: Option<Vec<String>> =
            if let PickOrdering::RandomNamespace { .. } = criteria.ordering {
                let ns_row = self
                    .conn
                    .query_opt(
                        "SELECT DISTINCT namespace FROM tasks WHERE status = 'PENDING' \
                     ORDER BY random() LIMIT 1",
                        &[],
                    )
                    .await
                    .map_err(map_db_error)?;
                match ns_row {
                    Some(row) => {
                        let n: String = row.get(0);
                        Some(vec![n])
                    }
                    None => return Ok(None),
                }
            } else {
                namespace_clause
            };

        let order_sql = pick_ordering_sql(criteria.ordering);

        // Build the WHERE clause. Use $1 = task_types, $2 = now, $3 = namespaces.
        let mut sql = String::from(
            "SELECT task_id, namespace, task_type, attempt_number, priority, \
                    payload, submitted_at, expires_at, max_retries, retry_initial_ms, \
                    retry_max_ms, retry_coefficient, traceparent, tracestate \
               FROM tasks \
              WHERE status = 'PENDING' \
                AND task_type = ANY($1::text[]) \
                AND (retry_after IS NULL OR retry_after <= $2) ",
        );
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![&task_types, &now];

        let chosen_namespaces_owned: Vec<String>;
        if let Some(ns_list) = chosen_namespaces.as_ref() {
            chosen_namespaces_owned = ns_list.clone();
            sql.push_str("AND namespace = ANY($3::text[]) ");
            params.push(&chosen_namespaces_owned);
        }

        sql.push_str(&order_sql);
        sql.push_str(" LIMIT 1 FOR UPDATE SKIP LOCKED");

        let row = self
            .conn
            .query_opt(&sql, &params)
            .await
            .map_err(map_db_error)?;

        let Some(row) = row else { return Ok(None) };
        Ok(Some(locked_task_from_row(&row)?))
    }

    async fn record_acquisition(&mut self, lease: NewLease) -> Result<()> {
        // §6.2 step 3a + 3b: insert task_runtime, update task status.
        let acquired_at = timestamp_to_chrono(lease.acquired_at);
        let timeout_at = timestamp_to_chrono(lease.timeout_at);
        let task_id = lease.task_id.into_uuid();
        let worker_id = lease.worker_id.into_uuid();
        let attempt_number = lease.attempt_number as i32;

        self.conn
            .execute(
                "INSERT INTO task_runtime (\
                    task_id, attempt_number, worker_id, acquired_at, timeout_at, last_extended_at) \
                 VALUES ($1, $2, $3, $4, $5, $4)",
                &[
                    &task_id,
                    &attempt_number,
                    &worker_id,
                    &acquired_at,
                    &timeout_at,
                ],
            )
            .await
            .map_err(map_db_error)?;

        self.conn
            .execute(
                "UPDATE tasks SET status = $1::text::taskq_task_status WHERE task_id = $2",
                &[&STATUS_DISPATCHED, &task_id],
            )
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn subscribe_pending(
        &mut self,
        namespace: &Namespace,
        _task_types: &[TaskType],
    ) -> Result<Box<dyn Stream<Item = WakeSignal> + Send + Unpin + 'static>> {
        // §6.2 step 1: subscribe to per-namespace LISTEN, demultiplexed by
        // the long-lived listener. Per design.md §8.4 task_types isn't part
        // of the channel name (it's a shared per-namespace channel); the
        // belt-and-suspenders 10s poll on the CP side filters to the
        // worker's actual task types.
        let receiver = self.dispatcher.subscribe(namespace).await?;
        Ok(Box::new(ReceiverStream { rx: receiver }))
    }

    // ========================================================================
    // Worker state transitions
    // ========================================================================

    async fn complete_task(&mut self, lease: &LeaseRef, outcome: TaskOutcome) -> Result<()> {
        // §6.4: validate ownership, insert results, update status, delete runtime.
        let task_id = lease.task_id.into_uuid();
        let attempt = lease.attempt_number as i32;
        let worker_id = lease.worker_id.into_uuid();

        // Lock the runtime row.
        let runtime = self
            .conn
            .query_opt(
                "SELECT 1 FROM task_runtime \
                  WHERE task_id = $1 AND attempt_number = $2 AND worker_id = $3 \
                    FOR UPDATE",
                &[&task_id, &attempt, &worker_id],
            )
            .await
            .map_err(map_db_error)?;
        if runtime.is_none() {
            return Err(StorageError::NotFound);
        }

        // Insert task_results.
        let (
            outcome_str,
            result_payload,
            error_class,
            error_message,
            error_details,
            recorded_at,
            next_status,
            retry_after,
        ) = outcome_to_row(outcome);
        let recorded_at_chrono = timestamp_to_chrono(recorded_at);
        let retry_after_chrono = retry_after.map(timestamp_to_chrono);

        self.conn
            .execute(
                "INSERT INTO task_results (\
                    task_id, attempt_number, outcome, result_payload, error_class, error_message, error_details, recorded_at) \
                 VALUES ($1, $2, $3::text::taskq_outcome, $4, $5, $6, $7, $8)",
                &[
                    &task_id,
                    &attempt,
                    &outcome_str,
                    &result_payload,
                    &error_class,
                    &error_message,
                    &error_details,
                    &recorded_at_chrono,
                ],
            )
            .await
            .map_err(map_db_error)?;

        // Update task status (and retry_after when WAITING_RETRY).
        self.conn
            .execute(
                "UPDATE tasks \
                    SET status = $1::text::taskq_task_status, retry_after = $2 \
                  WHERE task_id = $3",
                &[&next_status, &retry_after_chrono, &task_id],
            )
            .await
            .map_err(map_db_error)?;

        // Delete runtime row.
        self.conn
            .execute(
                "DELETE FROM task_runtime WHERE task_id = $1 AND attempt_number = $2",
                &[&task_id, &attempt],
            )
            .await
            .map_err(map_db_error)?;

        Ok(())
    }

    // ========================================================================
    // Reaper path
    // ========================================================================

    async fn list_expired_runtimes(
        &mut self,
        before: Timestamp,
        n: usize,
    ) -> Result<Vec<ExpiredRuntime>> {
        // §6.6 Reaper A: scan expired runtimes with SKIP LOCKED.
        let cutoff = timestamp_to_chrono(before);
        let limit = n as i64;
        let rows = self
            .conn
            .query(
                "SELECT r.task_id, r.attempt_number, r.worker_id, r.timeout_at, t.namespace \
                   FROM task_runtime r \
                   JOIN tasks t ON t.task_id = r.task_id \
                  WHERE r.timeout_at <= $1 \
                  ORDER BY r.timeout_at ASC \
                  LIMIT $2 \
                    FOR UPDATE OF r SKIP LOCKED",
                &[&cutoff, &limit],
            )
            .await
            .map_err(map_db_error)?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id: Uuid = row.get(0);
            let attempt_number: i32 = row.get(1);
            let worker_id: Uuid = row.get(2);
            let timeout_at: DateTime<Utc> = row.get(3);
            let namespace: String = row.get(4);

            out.push(ExpiredRuntime {
                task_id: TaskId::from_uuid(task_id),
                attempt_number: attempt_number as u32,
                worker_id: WorkerId::from_uuid(worker_id),
                timeout_at: chrono_to_timestamp(timeout_at),
                namespace: Namespace::new(namespace),
            });
        }
        Ok(out)
    }

    async fn reclaim_runtime(&mut self, runtime: &RuntimeRef) -> Result<()> {
        // §6.6: increment attempt_number, set status PENDING, delete runtime.
        let task_id = runtime.task_id.into_uuid();
        let attempt = runtime.attempt_number as i32;

        self.conn
            .execute(
                "UPDATE tasks SET status = $1::text::taskq_task_status, attempt_number = attempt_number + 1 \
                  WHERE task_id = $2",
                &[&STATUS_PENDING, &task_id],
            )
            .await
            .map_err(map_db_error)?;

        self.conn
            .execute(
                "DELETE FROM task_runtime WHERE task_id = $1 AND attempt_number = $2",
                &[&task_id, &attempt],
            )
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn mark_worker_dead(&mut self, worker_id: &WorkerId, at: Timestamp) -> Result<()> {
        // §6.6 Reaper B step: stamp declared_dead_at on the worker's row.
        let worker_id_uuid = worker_id.into_uuid();
        let at = timestamp_to_chrono(at);
        self.conn
            .execute(
                "UPDATE worker_heartbeats SET declared_dead_at = $1 WHERE worker_id = $2",
                &[&at, &worker_id_uuid],
            )
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    // ========================================================================
    // Quota enforcement
    // ========================================================================

    async fn check_capacity_quota(
        &mut self,
        namespace: &Namespace,
        kind: CapacityKind,
    ) -> Result<CapacityDecision> {
        // §9.1: read namespace_quota inline transactionally; system_default
        // fallback for unset fields.
        let (limit_col, count_sql) = match kind {
            CapacityKind::MaxPending => (
                "max_pending",
                "SELECT COUNT(*)::bigint FROM tasks WHERE namespace = $1 AND status = 'PENDING'",
            ),
            CapacityKind::MaxInflight => (
                "max_inflight",
                "SELECT COUNT(*)::bigint FROM tasks WHERE namespace = $1 AND status = 'DISPATCHED'",
            ),
            CapacityKind::MaxWorkers => (
                "max_workers",
                "SELECT COUNT(*)::bigint FROM worker_heartbeats WHERE namespace = $1 AND declared_dead_at IS NULL",
            ),
            CapacityKind::MaxWaitersPerReplica => {
                // Per-replica waiter cap is held in CP-level memory, not in
                // the database. The trait method exists for symmetry; the
                // CP layer maintains the count locally and only consults
                // this method for the configured limit. v1 returns
                // UnderLimit using only the configured ceiling; the CP
                // counts waiters itself.
                let limit = read_capacity_limit(&self.conn, namespace, "max_waiters_per_replica")
                    .await?
                    .unwrap_or(u64::MAX);
                return Ok(CapacityDecision::UnderLimit { current: 0, limit });
            }
        };

        let limit = read_capacity_limit(&self.conn, namespace, limit_col).await?;
        let Some(limit) = limit else {
            // Unset on both per-namespace and system_default => unlimited.
            return Ok(CapacityDecision::UnderLimit {
                current: 0,
                limit: u64::MAX,
            });
        };

        let row = self
            .conn
            .query_one(count_sql, &[&namespace.as_str()])
            .await
            .map_err(map_db_error)?;
        let current: i64 = row.get(0);
        let current = current as u64;

        if current >= limit {
            Ok(CapacityDecision::OverLimit { current, limit })
        } else {
            Ok(CapacityDecision::UnderLimit { current, limit })
        }
    }

    async fn oldest_pending_age_ms(&mut self, namespace: &Namespace) -> Result<Option<u64>> {
        // §7.1 CoDel: head-of-line age of PENDING tasks in this namespace.
        // Postgres computes the age in milliseconds server-side so the CP
        // layer's clock skew never enters the picture.
        let row = self
            .conn
            .query_opt(
                "SELECT (EXTRACT(EPOCH FROM (NOW() - MIN(submitted_at))) * 1000)::bigint \
                   FROM tasks \
                  WHERE namespace = $1 AND status = 'PENDING'",
                &[&namespace.as_str()],
            )
            .await
            .map_err(map_db_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        // The aggregate returns one row even when no tasks match; the column
        // is NULL in that case. Treat NULL and "no rows" the same.
        let age: Option<i64> = row.try_get::<_, Option<i64>>(0).map_err(map_db_error)?;
        Ok(age.and_then(|v| if v < 0 { None } else { Some(v as u64) }))
    }

    async fn try_consume_rate_quota(
        &mut self,
        namespace: &Namespace,
        kind: RateKind,
        n: u64,
    ) -> Result<RateDecision> {
        // §1.1 carve-out: rate quotas are eventually consistent within
        // cache TTL. v1 implementation: per-replica in-memory token bucket.
        // Default capacity = configured rate (so a fresh bucket starts
        // full). If unconfigured, treat as "unbounded" and always allow.
        let limit_col = match kind {
            RateKind::SubmitRpm => "max_submit_rpm",
            RateKind::DispatchRpm => "max_dispatch_rpm",
            RateKind::ReplayPerSecond => "max_replay_per_second",
        };
        let limit = read_capacity_limit(&self.conn, namespace, limit_col).await?;
        let Some(limit) = limit else {
            return Ok(RateDecision::Allowed {
                remaining: u64::MAX,
            });
        };
        // For RateKind::ReplayPerSecond the unit is per-second; treat the
        // RateLimiter "default_per_minute" as `60 * per_second`.
        let per_minute = match kind {
            RateKind::SubmitRpm | RateKind::DispatchRpm => limit,
            RateKind::ReplayPerSecond => limit.saturating_mul(60),
        };
        Ok(self
            .rate_limiter
            .try_consume(namespace, kind, n, per_minute))
    }

    // ========================================================================
    // Heartbeats (READ COMMITTED carve-out)
    // ========================================================================

    async fn record_worker_heartbeat(
        &mut self,
        worker_id: &WorkerId,
        namespace: &Namespace,
        at: Timestamp,
    ) -> Result<HeartbeatAck> {
        // §6.3 step 2: side connection at READ COMMITTED.
        let mut side = self.pool.get().await.map_err(map_pool_error)?;
        let at_chrono = timestamp_to_chrono(at);
        let worker_id_uuid = worker_id.into_uuid();

        // tokio_postgres connects with READ COMMITTED isolation by default.
        // Wrap in an explicit transaction so the UPSERT is atomic.
        let txn = side.transaction().await.map_err(map_db_error)?;
        let updated = txn
            .execute(
                "INSERT INTO worker_heartbeats (worker_id, namespace, last_heartbeat_at) \
                  VALUES ($1, $2, $3) \
                 ON CONFLICT (worker_id) DO UPDATE \
                       SET last_heartbeat_at = EXCLUDED.last_heartbeat_at \
                     WHERE worker_heartbeats.declared_dead_at IS NULL",
                &[&worker_id_uuid, &namespace.as_str(), &at_chrono],
            )
            .await
            .map_err(map_db_error)?;
        txn.commit().await.map_err(map_db_error)?;

        if updated == 0 {
            Ok(HeartbeatAck::WorkerDeregistered)
        } else {
            Ok(HeartbeatAck::Recorded)
        }
    }

    async fn extend_lease_lazy(
        &mut self,
        lease: &LeaseRef,
        new_timeout: Timestamp,
        last_extended_at: Timestamp,
    ) -> Result<()> {
        // §6.3 step 3: SERIALIZABLE update on task_runtime.
        let new_timeout = timestamp_to_chrono(new_timeout);
        let last_extended_at = timestamp_to_chrono(last_extended_at);
        let task_id = lease.task_id.into_uuid();
        let attempt = lease.attempt_number as i32;
        let worker_id = lease.worker_id.into_uuid();

        let updated = self
            .conn
            .execute(
                "UPDATE task_runtime \
                    SET timeout_at = $1, last_extended_at = $2 \
                  WHERE task_id = $3 AND attempt_number = $4 AND worker_id = $5",
                &[
                    &new_timeout,
                    &last_extended_at,
                    &task_id,
                    &attempt,
                    &worker_id,
                ],
            )
            .await
            .map_err(map_db_error)?;
        if updated == 0 {
            return Err(StorageError::NotFound);
        }
        Ok(())
    }

    // ========================================================================
    // Cleanup
    // ========================================================================

    async fn delete_expired_dedup(&mut self, before: Timestamp, _n: usize) -> Result<usize> {
        // §8.3 / §8.2 #6: bounded-cost cleanup via DROP PARTITION.
        //
        // Strategy: list every partition of `idempotency_keys` whose upper
        // range bound is <= `before`; for each, DETACH then DROP. Returns
        // partitions dropped, NOT rows.
        let cutoff = timestamp_to_chrono(before);

        // pg_partition_tree gives the partition list; pg_get_expr renders
        // the FOR VALUES clause we can parse to find upper bounds. The
        // simpler path: pg_class.relname matches `idempotency_keys_YYYYMMDD`
        // for daily partitions, so we can compare against the cutoff
        // without having to parse partition expressions.
        let rows = self
            .conn
            .query(
                "SELECT c.relname \
                   FROM pg_inherits i \
                   JOIN pg_class p ON p.oid = i.inhparent \
                   JOIN pg_class c ON c.oid = i.inhrelid \
                  WHERE p.relname = 'idempotency_keys' \
                    AND c.relname ~ '^idempotency_keys_[0-9]{8}$' \
                  ORDER BY c.relname",
                &[],
            )
            .await
            .map_err(map_db_error)?;

        let cutoff_date = cutoff.format("%Y%m%d").to_string();
        let mut dropped = 0usize;
        for row in rows {
            let name: String = row.get(0);
            // partition name is `idempotency_keys_YYYYMMDD`; the date is
            // its lower bound. Upper bound = lower_bound + 1 day, so the
            // partition is fully past `cutoff` iff upper_bound <= cutoff,
            // i.e. lower_bound < (cutoff - 1 day). Approximate by string
            // comparison since YYYYMMDD lexical order matches date order.
            let suffix = name.trim_start_matches("idempotency_keys_");
            // Skip if suffix is not within range.
            if suffix < cutoff_date.as_str() {
                let detach_sql = format!("ALTER TABLE idempotency_keys DETACH PARTITION {name}");
                let drop_sql = format!("DROP TABLE {name}");
                // Defense in depth: validate the partition name shape.
                if !is_safe_partition_name(&name) {
                    continue;
                }
                self.conn
                    .batch_execute(&detach_sql)
                    .await
                    .map_err(map_db_error)?;
                self.conn
                    .batch_execute(&drop_sql)
                    .await
                    .map_err(map_db_error)?;
                dropped += 1;
            }
        }

        Ok(dropped)
    }

    // ========================================================================
    // Admin / reads
    // ========================================================================

    async fn get_namespace_quota(&mut self, namespace: &Namespace) -> Result<NamespaceQuota> {
        // Merge per-namespace row with system_default fallback.
        let default = read_quota_row(&self.conn, "system_default").await?;
        let specific = read_quota_row(&self.conn, namespace.as_str()).await?;

        let base = match (specific, default) {
            (Some(s), Some(d)) => merge_quotas(s, d),
            (Some(s), None) => s,
            (None, Some(d)) => NamespaceQuota {
                namespace: namespace.clone(),
                ..d
            },
            (None, None) => return Err(StorageError::NotFound),
        };

        Ok(base)
    }

    async fn audit_log_append(&mut self, entry: AuditEntry) -> Result<()> {
        // §11.4: append-only insert in same transaction.
        //
        // `request_summary` arrives as opaque bytes per the trait contract.
        // We pass them as a UTF-8 string with `::jsonb` so Postgres rejects
        // non-JSON input via SQLSTATE 22P02 (mapped to `BackendError`).
        let timestamp = timestamp_to_chrono(entry.timestamp);
        let summary_str = std::str::from_utf8(entry.request_summary.as_ref()).map_err(|e| {
            StorageError::ConstraintViolation(format!("audit_log.request_summary not UTF-8: {e}"))
        })?;
        let request_hash = entry.request_hash.to_vec();
        let namespace_str = entry.namespace.as_ref().map(|n| n.as_str().to_owned());

        self.conn
            .execute(
                "INSERT INTO audit_log (\
                    timestamp, actor, rpc, namespace, request_summary, result, request_hash) \
                 VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)",
                &[
                    &timestamp,
                    &entry.actor.as_str(),
                    &entry.rpc.as_str(),
                    &namespace_str,
                    &summary_str,
                    &entry.result.as_str(),
                    &request_hash,
                ],
            )
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    // ========================================================================
    // Phase 5c admin / cancel / reaper-B
    // ========================================================================

    async fn cancel_task(&mut self, task_id: TaskId) -> Result<CancelOutcome> {
        // §6.7: SELECT FOR UPDATE -> if non-terminal, flip to CANCELLED and
        // delete the dedup row in the same transaction. Idempotent on
        // already-terminal rows (returns AlreadyTerminal).
        let task_id_uuid = task_id.into_uuid();
        let row = self
            .conn
            .query_opt(
                "SELECT status::text, namespace FROM tasks WHERE task_id = $1 FOR UPDATE",
                &[&task_id_uuid],
            )
            .await
            .map_err(map_db_error)?;

        let Some(row) = row else {
            return Ok(CancelOutcome::NotFound);
        };
        let status_str: String = row.get(0);
        let namespace_str: String = row.get(1);
        let Some(status) = TaskStatus::from_db_str(&status_str) else {
            return Err(StorageError::ConstraintViolation(format!(
                "tasks.status has unknown value: {status_str}"
            )));
        };

        if status.is_terminal() {
            return Ok(CancelOutcome::AlreadyTerminal { state: status });
        }

        // Non-terminal: flip to CANCELLED and delete the dedup row.
        self.conn
            .execute(
                "UPDATE tasks SET status = $1::text::taskq_task_status WHERE task_id = $2",
                &[&"CANCELLED", &task_id_uuid],
            )
            .await
            .map_err(map_db_error)?;

        // §6.7: physically delete the dedup row so the same key can be reused.
        self.conn
            .execute(
                "DELETE FROM idempotency_keys WHERE namespace = $1 AND task_id = $2",
                &[&namespace_str, &task_id_uuid],
            )
            .await
            .map_err(map_db_error)?;

        // Also delete any runtime row in case a worker had the lease (the
        // §6.7 spec leaves it to admin paths to reclaim — keep the table
        // consistent so a subsequent `CompleteTask` returns LEASE_EXPIRED).
        self.conn
            .execute(
                "DELETE FROM task_runtime WHERE task_id = $1",
                &[&task_id_uuid],
            )
            .await
            .map_err(map_db_error)?;

        Ok(CancelOutcome::TransitionedToCancelled)
    }

    async fn get_task_by_id(&mut self, task_id: TaskId) -> Result<Option<Task>> {
        let task_id_uuid = task_id.into_uuid();
        let row = self
            .conn
            .query_opt(
                "SELECT task_id, namespace, task_type, status::text, priority, payload, \
                        payload_hash, submitted_at, expires_at, attempt_number, max_retries, \
                        retry_initial_ms, retry_max_ms, retry_coefficient, retry_after, \
                        traceparent, tracestate, format_version, original_failure_count \
                   FROM tasks WHERE task_id = $1",
                &[&task_id_uuid],
            )
            .await
            .map_err(map_db_error)?;

        let Some(row) = row else {
            return Ok(None);
        };

        let task_id: Uuid = row.get(0);
        let namespace_str: String = row.get(1);
        let task_type_str: String = row.get(2);
        let status_str: String = row.get(3);
        let priority: i32 = row.get(4);
        let payload: Vec<u8> = row.get(5);
        let payload_hash_bytes: Vec<u8> = row.get(6);
        let submitted_at: DateTime<Utc> = row.get(7);
        let expires_at: DateTime<Utc> = row.get(8);
        let attempt_number: i32 = row.get(9);
        let max_retries: i32 = row.get(10);
        let retry_initial_ms: i64 = row.get(11);
        let retry_max_ms: i64 = row.get(12);
        let retry_coefficient: f32 = row.get(13);
        let retry_after: Option<DateTime<Utc>> = row.get(14);
        let traceparent: Vec<u8> = row.get(15);
        let tracestate: Vec<u8> = row.get(16);
        let format_version: i32 = row.get(17);
        let original_failure_count: i32 = row.get(18);

        let payload_hash: [u8; 32] = payload_hash_bytes.as_slice().try_into().map_err(|_| {
            StorageError::ConstraintViolation("tasks.payload_hash != 32 bytes".to_owned())
        })?;
        let status = TaskStatus::from_db_str(&status_str).ok_or_else(|| {
            StorageError::ConstraintViolation(format!("tasks.status unknown: {status_str}"))
        })?;

        Ok(Some(Task {
            task_id: TaskId::from_uuid(task_id),
            namespace: Namespace::new(namespace_str),
            task_type: TaskType::new(task_type_str),
            status,
            priority,
            payload: Bytes::from(payload),
            payload_hash,
            submitted_at: chrono_to_timestamp(submitted_at),
            expires_at: chrono_to_timestamp(expires_at),
            attempt_number: attempt_number as u32,
            max_retries: max_retries as u32,
            retry_initial_ms: retry_initial_ms as u64,
            retry_max_ms: retry_max_ms as u64,
            retry_coefficient,
            retry_after: retry_after.map(chrono_to_timestamp),
            traceparent: Bytes::from(traceparent),
            tracestate: Bytes::from(tracestate),
            format_version: format_version as u32,
            original_failure_count: original_failure_count as u32,
        }))
    }

    async fn list_dead_worker_runtimes(
        &mut self,
        stale_before: Timestamp,
        n: usize,
    ) -> Result<Vec<DeadWorkerRuntime>> {
        // §6.6 Reaper B: JOIN runtime to heartbeats; pick only alive (not
        // already-declared-dead) workers whose last heartbeat predates
        // `stale_before`. SKIP LOCKED so concurrent reaper passes don't
        // double-process.
        let cutoff = timestamp_to_chrono(stale_before);
        let limit = n as i64;
        let rows = self
            .conn
            .query(
                "SELECT r.task_id, r.attempt_number, r.worker_id, t.namespace, h.last_heartbeat_at \
                   FROM task_runtime r \
                   JOIN tasks t ON t.task_id = r.task_id \
                   JOIN worker_heartbeats h ON h.worker_id = r.worker_id \
                  WHERE h.last_heartbeat_at < $1 \
                    AND h.declared_dead_at IS NULL \
                  ORDER BY h.last_heartbeat_at ASC \
                  LIMIT $2 \
                    FOR UPDATE OF r SKIP LOCKED",
                &[&cutoff, &limit],
            )
            .await
            .map_err(map_db_error)?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id: Uuid = row.get(0);
            let attempt_number: i32 = row.get(1);
            let worker_id: Uuid = row.get(2);
            let namespace: String = row.get(3);
            let last_heartbeat_at: DateTime<Utc> = row.get(4);
            out.push(DeadWorkerRuntime {
                task_id: TaskId::from_uuid(task_id),
                attempt_number: attempt_number as u32,
                worker_id: WorkerId::from_uuid(worker_id),
                namespace: Namespace::new(namespace),
                last_heartbeat_at: chrono_to_timestamp(last_heartbeat_at),
            });
        }
        Ok(out)
    }

    async fn count_tasks_by_status(
        &mut self,
        namespace: &Namespace,
    ) -> Result<HashMap<TaskStatus, u64>> {
        let rows = self
            .conn
            .query(
                "SELECT status::text, COUNT(*)::bigint \
                   FROM tasks \
                  WHERE namespace = $1 \
                  GROUP BY status",
                &[&namespace.as_str()],
            )
            .await
            .map_err(map_db_error)?;
        let mut out = HashMap::new();
        for row in rows {
            let status_str: String = row.get(0);
            let count: i64 = row.get(1);
            if let Some(status) = TaskStatus::from_db_str(&status_str) {
                out.insert(status, count as u64);
            }
        }
        Ok(out)
    }

    async fn list_workers(
        &mut self,
        namespace: &Namespace,
        include_dead: bool,
    ) -> Result<Vec<WorkerInfo>> {
        // Aggregate inflight count per worker via LEFT JOIN to task_runtime.
        let sql = if include_dead {
            "SELECT h.worker_id, h.namespace, h.last_heartbeat_at, h.declared_dead_at, \
                    COALESCE(rt.cnt, 0)::bigint \
               FROM worker_heartbeats h \
          LEFT JOIN ( \
                SELECT worker_id, COUNT(*) AS cnt FROM task_runtime GROUP BY worker_id \
              ) rt ON rt.worker_id = h.worker_id \
              WHERE h.namespace = $1 \
              ORDER BY h.last_heartbeat_at DESC"
        } else {
            "SELECT h.worker_id, h.namespace, h.last_heartbeat_at, h.declared_dead_at, \
                    COALESCE(rt.cnt, 0)::bigint \
               FROM worker_heartbeats h \
          LEFT JOIN ( \
                SELECT worker_id, COUNT(*) AS cnt FROM task_runtime GROUP BY worker_id \
              ) rt ON rt.worker_id = h.worker_id \
              WHERE h.namespace = $1 AND h.declared_dead_at IS NULL \
              ORDER BY h.last_heartbeat_at DESC"
        };
        let rows = self
            .conn
            .query(sql, &[&namespace.as_str()])
            .await
            .map_err(map_db_error)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let worker_id: Uuid = row.get(0);
            let namespace_str: String = row.get(1);
            let last_heartbeat_at: DateTime<Utc> = row.get(2);
            let declared_dead_at: Option<DateTime<Utc>> = row.get(3);
            let inflight_count: i64 = row.get(4);
            out.push(WorkerInfo {
                worker_id: WorkerId::from_uuid(worker_id),
                namespace: Namespace::new(namespace_str),
                last_heartbeat_at: chrono_to_timestamp(last_heartbeat_at),
                declared_dead_at: declared_dead_at.map(chrono_to_timestamp),
                inflight_count: inflight_count as u32,
            });
        }
        Ok(out)
    }

    async fn enable_namespace(&mut self, namespace: &Namespace) -> Result<()> {
        self.conn
            .execute(
                "UPDATE namespace_quota SET disabled = FALSE WHERE namespace = $1",
                &[&namespace.as_str()],
            )
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn disable_namespace(&mut self, namespace: &Namespace) -> Result<()> {
        self.conn
            .execute(
                "UPDATE namespace_quota SET disabled = TRUE WHERE namespace = $1",
                &[&namespace.as_str()],
            )
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    // ========================================================================
    // Phase 5e admin writes
    // ========================================================================

    async fn upsert_namespace_quota(
        &mut self,
        namespace: &Namespace,
        quota: NamespaceQuotaUpsert,
    ) -> Result<()> {
        // §6.7 SetNamespaceQuota: rewrite the writable subset of the row;
        // strategy choice + `disabled` are owned by other paths. The INSERT
        // shape supplies stub values for `admitter_kind` / `dispatcher_kind`
        // when the namespace has no prior row (matching the
        // `system_default` defaults from migrations/0001).
        let max_pending = quota.max_pending.map(|v| v as i64);
        let max_inflight = quota.max_inflight.map(|v| v as i64);
        let max_workers = quota.max_workers.map(|v| v as i32);
        let max_waiters_per_replica = quota.max_waiters_per_replica.map(|v| v as i32);
        let max_submit_rpm = quota.max_submit_rpm.map(|v| v as i64);
        let max_dispatch_rpm = quota.max_dispatch_rpm.map(|v| v as i64);
        let max_replay_per_second = quota.max_replay_per_second.map(|v| v as i32);
        let max_retries_ceiling = quota.max_retries_ceiling as i32;
        let max_idempotency_ttl_seconds = quota.max_idempotency_ttl_seconds as i64;
        let max_payload_bytes = quota.max_payload_bytes as i32;
        let max_details_bytes = quota.max_details_bytes as i32;
        let min_heartbeat_interval_seconds = quota.min_heartbeat_interval_seconds as i32;
        let lazy_extension_threshold_seconds = quota.lazy_extension_threshold_seconds as i32;
        let max_error_classes = quota.max_error_classes as i32;
        let max_task_types = quota.max_task_types as i32;
        let trace_sampling_ratio = quota.trace_sampling_ratio;
        let log_level_override = quota.log_level_override.clone();
        let audit_log_retention_days = quota.audit_log_retention_days as i32;
        let metrics_export_enabled = quota.metrics_export_enabled;
        let empty_params: Vec<u8> = Vec::new();

        self.conn
            .execute(
                "INSERT INTO namespace_quota (\
                    namespace, admitter_kind, admitter_params, dispatcher_kind, dispatcher_params, \
                    max_pending, max_inflight, max_workers, max_waiters_per_replica, \
                    max_submit_rpm, max_dispatch_rpm, max_replay_per_second, \
                    max_retries_ceiling, max_idempotency_ttl_seconds, max_payload_bytes, \
                    max_details_bytes, min_heartbeat_interval_seconds, \
                    lazy_extension_threshold_seconds, max_error_classes, max_task_types, \
                    trace_sampling_ratio, log_level_override, audit_log_retention_days, \
                    metrics_export_enabled) \
                 VALUES ($1, 'Always', $21, 'PriorityFifo', $21, \
                    $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, \
                    $17, $18, $19, $20) \
                 ON CONFLICT (namespace) DO UPDATE SET \
                    max_pending = EXCLUDED.max_pending, \
                    max_inflight = EXCLUDED.max_inflight, \
                    max_workers = EXCLUDED.max_workers, \
                    max_waiters_per_replica = EXCLUDED.max_waiters_per_replica, \
                    max_submit_rpm = EXCLUDED.max_submit_rpm, \
                    max_dispatch_rpm = EXCLUDED.max_dispatch_rpm, \
                    max_replay_per_second = EXCLUDED.max_replay_per_second, \
                    max_retries_ceiling = EXCLUDED.max_retries_ceiling, \
                    max_idempotency_ttl_seconds = EXCLUDED.max_idempotency_ttl_seconds, \
                    max_payload_bytes = EXCLUDED.max_payload_bytes, \
                    max_details_bytes = EXCLUDED.max_details_bytes, \
                    min_heartbeat_interval_seconds = EXCLUDED.min_heartbeat_interval_seconds, \
                    lazy_extension_threshold_seconds = EXCLUDED.lazy_extension_threshold_seconds, \
                    max_error_classes = EXCLUDED.max_error_classes, \
                    max_task_types = EXCLUDED.max_task_types, \
                    trace_sampling_ratio = EXCLUDED.trace_sampling_ratio, \
                    log_level_override = EXCLUDED.log_level_override, \
                    audit_log_retention_days = EXCLUDED.audit_log_retention_days, \
                    metrics_export_enabled = EXCLUDED.metrics_export_enabled",
                &[
                    &namespace.as_str(),
                    &max_pending,
                    &max_inflight,
                    &max_workers,
                    &max_waiters_per_replica,
                    &max_submit_rpm,
                    &max_dispatch_rpm,
                    &max_replay_per_second,
                    &max_retries_ceiling,
                    &max_idempotency_ttl_seconds,
                    &max_payload_bytes,
                    &max_details_bytes,
                    &min_heartbeat_interval_seconds,
                    &lazy_extension_threshold_seconds,
                    &max_error_classes,
                    &max_task_types,
                    &trace_sampling_ratio,
                    &log_level_override,
                    &audit_log_retention_days,
                    &metrics_export_enabled,
                    &empty_params,
                ],
            )
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn list_tasks_by_filter(
        &mut self,
        namespace: &Namespace,
        filter: TaskFilter,
        limit: usize,
    ) -> Result<Vec<Task>> {
        // §6.7 PurgeTasks: dynamic WHERE built from `filter`. Predicates are
        // bound parameters; the `ANY($N::text[])` form keeps the SQL static
        // while still supporting variable-length IN-lists.
        let mut sql = String::from(
            "SELECT task_id, namespace, task_type, status::text, priority, payload, \
                    payload_hash, submitted_at, expires_at, attempt_number, max_retries, \
                    retry_initial_ms, retry_max_ms, retry_coefficient, retry_after, \
                    traceparent, tracestate, format_version, original_failure_count \
               FROM tasks \
              WHERE namespace = $1",
        );
        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
        params.push(Box::new(namespace.as_str().to_owned()));

        let task_types_owned: Option<Vec<String>> = filter
            .task_types
            .as_ref()
            .map(|v| v.iter().map(|t| t.as_str().to_owned()).collect());
        if let Some(ref types) = task_types_owned {
            sql.push_str(&format!(
                " AND task_type = ANY(${}::text[])",
                params.len() + 1
            ));
            params.push(Box::new(types.clone()));
        }

        let statuses_owned: Option<Vec<String>> = filter
            .statuses
            .as_ref()
            .map(|v| v.iter().map(|s| s.as_db_str().to_owned()).collect());
        if let Some(ref statuses) = statuses_owned {
            sql.push_str(&format!(
                " AND status::text = ANY(${}::text[])",
                params.len() + 1
            ));
            params.push(Box::new(statuses.clone()));
        }

        if let Some(before) = filter.submitted_before {
            sql.push_str(&format!(" AND submitted_at < ${}", params.len() + 1));
            params.push(Box::new(timestamp_to_chrono(before)));
        }
        if let Some(after) = filter.submitted_after {
            sql.push_str(&format!(" AND submitted_at > ${}", params.len() + 1));
            params.push(Box::new(timestamp_to_chrono(after)));
        }

        sql.push_str(&format!(
            " ORDER BY submitted_at ASC LIMIT ${}",
            params.len() + 1
        ));
        params.push(Box::new(limit as i64));

        let param_refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();

        let rows = self
            .conn
            .query(sql.as_str(), &param_refs)
            .await
            .map_err(map_db_error)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(task_from_row(&row)?);
        }
        Ok(out)
    }

    async fn list_tasks_by_terminal_status(
        &mut self,
        namespace: &Namespace,
        statuses: Vec<TerminalState>,
        limit: usize,
    ) -> Result<Vec<Task>> {
        // §6.7 ReplayDeadLetters: identical shape to `list_tasks_by_filter`
        // but the status set is constrained to terminal failure variants.
        let status_strs: Vec<String> = statuses
            .iter()
            .map(|s| s.as_task_status().as_db_str().to_owned())
            .collect();
        let limit = limit as i64;
        let rows = self
            .conn
            .query(
                "SELECT task_id, namespace, task_type, status::text, priority, payload, \
                        payload_hash, submitted_at, expires_at, attempt_number, max_retries, \
                        retry_initial_ms, retry_max_ms, retry_coefficient, retry_after, \
                        traceparent, tracestate, format_version, original_failure_count \
                   FROM tasks \
                  WHERE namespace = $1 \
                    AND status::text = ANY($2::text[]) \
                  ORDER BY submitted_at ASC \
                  LIMIT $3",
                &[&namespace.as_str(), &status_strs, &limit],
            )
            .await
            .map_err(map_db_error)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(task_from_row(&row)?);
        }
        Ok(out)
    }

    async fn replay_task(&mut self, task_id: TaskId) -> Result<ReplayOutcome> {
        // §6.7 ReplayDeadLetters: validate state, validate idempotency-key
        // ownership, then reset row to PENDING. Per the design we preserve
        // `original_failure_count` and reset `attempt_number` to 0 so the
        // replayed task starts fresh while still tracking its prior failure
        // count for forensics.
        let task_id_uuid = task_id.into_uuid();

        let row = self
            .conn
            .query_opt(
                "SELECT status::text, namespace, attempt_number, original_failure_count \
                   FROM tasks WHERE task_id = $1 FOR UPDATE",
                &[&task_id_uuid],
            )
            .await
            .map_err(map_db_error)?;
        let Some(row) = row else {
            return Ok(ReplayOutcome::NotFound);
        };
        let status_str: String = row.get(0);
        let namespace_str: String = row.get(1);
        let attempt_number: i32 = row.get(2);
        let original_failure_count: i32 = row.get(3);
        let Some(status) = TaskStatus::from_db_str(&status_str) else {
            return Err(StorageError::ConstraintViolation(format!(
                "tasks.status has unknown value: {status_str}"
            )));
        };

        let in_terminal_failure = matches!(
            status,
            TaskStatus::FailedNonretryable | TaskStatus::FailedExhausted | TaskStatus::Expired
        );
        if !in_terminal_failure {
            return Ok(ReplayOutcome::NotInTerminalFailureState);
        }

        // Idempotency-key ownership: any dedup row for this namespace must
        // either point at this task or not exist at all. If a different
        // task owns the key, the replay would create a phantom duplicate;
        // refuse.
        let claimed_elsewhere: Option<i64> = self
            .conn
            .query_opt(
                "SELECT 1 FROM idempotency_keys \
                  WHERE namespace = $1 AND task_id != $2 \
                    AND EXISTS (SELECT 1 FROM idempotency_keys k2 \
                                 WHERE k2.namespace = $1 AND k2.task_id = $2)",
                &[&namespace_str, &task_id_uuid],
            )
            .await
            .map_err(map_db_error)?
            .map(|r| r.get::<_, i32>(0) as i64);
        if claimed_elsewhere.is_some() {
            return Ok(ReplayOutcome::KeyClaimedElsewhere);
        }

        // Compute the new `original_failure_count`: preserve the existing
        // value if it's already non-zero (a re-replay), otherwise capture
        // the current `attempt_number` so forensics retain the prior
        // attempt count even after the row resets.
        let new_original_failure_count = if original_failure_count > 0 {
            original_failure_count
        } else {
            attempt_number
        };

        self.conn
            .execute(
                "UPDATE tasks SET status = 'PENDING'::taskq_task_status, \
                                  attempt_number = 0, \
                                  retry_after = NULL, \
                                  original_failure_count = $2 \
                  WHERE task_id = $1",
                &[&task_id_uuid, &new_original_failure_count],
            )
            .await
            .map_err(map_db_error)?;

        Ok(ReplayOutcome::Replayed)
    }

    async fn add_error_classes(&mut self, namespace: &Namespace, classes: &[String]) -> Result<()> {
        if classes.is_empty() {
            return Ok(());
        }
        for class in classes {
            self.conn
                .execute(
                    "INSERT INTO error_class_registry (namespace, error_class) \
                     VALUES ($1, $2) \
                     ON CONFLICT (namespace, error_class) DO NOTHING",
                    &[&namespace.as_str(), &class.as_str()],
                )
                .await
                .map_err(map_db_error)?;
        }
        Ok(())
    }

    async fn deprecate_error_class(&mut self, namespace: &Namespace, class: &str) -> Result<()> {
        self.conn
            .execute(
                "UPDATE error_class_registry SET deprecated = TRUE \
                  WHERE namespace = $1 AND error_class = $2",
                &[&namespace.as_str(), &class],
            )
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn add_task_types(&mut self, namespace: &Namespace, types: &[TaskType]) -> Result<()> {
        if types.is_empty() {
            return Ok(());
        }
        for ty in types {
            self.conn
                .execute(
                    "INSERT INTO task_type_registry (namespace, task_type) \
                     VALUES ($1, $2) \
                     ON CONFLICT (namespace, task_type) DO NOTHING",
                    &[&namespace.as_str(), &ty.as_str()],
                )
                .await
                .map_err(map_db_error)?;
        }
        Ok(())
    }

    async fn deprecate_task_type(
        &mut self,
        namespace: &Namespace,
        task_type: &TaskType,
    ) -> Result<()> {
        self.conn
            .execute(
                "UPDATE task_type_registry SET deprecated = TRUE \
                  WHERE namespace = $1 AND task_type = $2",
                &[&namespace.as_str(), &task_type.as_str()],
            )
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    // ========================================================================
    // Lifecycle
    // ========================================================================

    async fn commit(self) -> Result<()> {
        self.conn
            .batch_execute("COMMIT")
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn rollback(self) -> Result<()> {
        self.conn
            .batch_execute("ROLLBACK")
            .await
            .map_err(map_db_error)?;
        Ok(())
    }
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

/// Decode a full `tasks` row into a [`Task`]. The column order MUST match
/// every callsite — see `get_task_by_id`, `list_tasks_by_filter`, and
/// `list_tasks_by_terminal_status`.
fn task_from_row(row: &Row) -> Result<Task> {
    let task_id: Uuid = row.get(0);
    let namespace_str: String = row.get(1);
    let task_type_str: String = row.get(2);
    let status_str: String = row.get(3);
    let priority: i32 = row.get(4);
    let payload: Vec<u8> = row.get(5);
    let payload_hash_bytes: Vec<u8> = row.get(6);
    let submitted_at: DateTime<Utc> = row.get(7);
    let expires_at: DateTime<Utc> = row.get(8);
    let attempt_number: i32 = row.get(9);
    let max_retries: i32 = row.get(10);
    let retry_initial_ms: i64 = row.get(11);
    let retry_max_ms: i64 = row.get(12);
    let retry_coefficient: f32 = row.get(13);
    let retry_after: Option<DateTime<Utc>> = row.get(14);
    let traceparent: Vec<u8> = row.get(15);
    let tracestate: Vec<u8> = row.get(16);
    let format_version: i32 = row.get(17);
    let original_failure_count: i32 = row.get(18);

    let payload_hash: [u8; 32] = payload_hash_bytes.as_slice().try_into().map_err(|_| {
        StorageError::ConstraintViolation("tasks.payload_hash != 32 bytes".to_owned())
    })?;
    let status = TaskStatus::from_db_str(&status_str).ok_or_else(|| {
        StorageError::ConstraintViolation(format!("tasks.status unknown: {status_str}"))
    })?;

    Ok(Task {
        task_id: TaskId::from_uuid(task_id),
        namespace: Namespace::new(namespace_str),
        task_type: TaskType::new(task_type_str),
        status,
        priority,
        payload: Bytes::from(payload),
        payload_hash,
        submitted_at: chrono_to_timestamp(submitted_at),
        expires_at: chrono_to_timestamp(expires_at),
        attempt_number: attempt_number as u32,
        max_retries: max_retries as u32,
        retry_initial_ms: retry_initial_ms as u64,
        retry_max_ms: retry_max_ms as u64,
        retry_coefficient,
        retry_after: retry_after.map(chrono_to_timestamp),
        traceparent: Bytes::from(traceparent),
        tracestate: Bytes::from(tracestate),
        format_version: format_version as u32,
        original_failure_count: original_failure_count as u32,
    })
}

fn locked_task_from_row(row: &Row) -> Result<LockedTask> {
    let task_id: Uuid = row.get(0);
    let namespace: String = row.get(1);
    let task_type: String = row.get(2);
    let attempt_number: i32 = row.get(3);
    let priority: i32 = row.get(4);
    let payload: Vec<u8> = row.get(5);
    let submitted_at: DateTime<Utc> = row.get(6);
    let expires_at: DateTime<Utc> = row.get(7);
    let max_retries: i32 = row.get(8);
    let retry_initial_ms: i64 = row.get(9);
    let retry_max_ms: i64 = row.get(10);
    let retry_coefficient: f32 = row.get(11);
    let traceparent: Vec<u8> = row.get(12);
    let tracestate: Vec<u8> = row.get(13);

    Ok(LockedTask {
        task_id: TaskId::from_uuid(task_id),
        namespace: Namespace::new(namespace),
        task_type: TaskType::new(task_type),
        attempt_number: attempt_number as u32,
        priority,
        payload: Bytes::from(payload),
        submitted_at: chrono_to_timestamp(submitted_at),
        expires_at: chrono_to_timestamp(expires_at),
        max_retries: max_retries as u32,
        retry_initial_ms: retry_initial_ms as u64,
        retry_max_ms: retry_max_ms as u64,
        retry_coefficient,
        traceparent: Bytes::from(traceparent),
        tracestate: Bytes::from(tracestate),
    })
}

/// Read one column from `namespace_quota` for `namespace`, falling back to
/// `system_default` if the per-namespace row is missing or the column is
/// NULL there.
async fn read_capacity_limit(
    conn: &PgConn,
    namespace: &Namespace,
    column: &str,
) -> Result<Option<u64>> {
    // Two queries: the cost is rounding error per design.md §9.1 and the
    // alternative (a UNION ALL with COALESCE on dynamic columns) requires
    // server-side query construction that's harder to read.
    let sql = format!("SELECT {column} FROM namespace_quota WHERE namespace = $1");

    let specific = conn
        .query_opt(&sql, &[&namespace.as_str()])
        .await
        .map_err(map_db_error)?;
    if let Some(row) = specific {
        if let Some(val) = column_to_u64(&row, 0) {
            return Ok(Some(val));
        }
    }

    let default = conn
        .query_opt(&sql, &[&"system_default"])
        .await
        .map_err(map_db_error)?;
    if let Some(row) = default {
        if let Some(val) = column_to_u64(&row, 0) {
            return Ok(Some(val));
        }
    }
    Ok(None)
}

fn column_to_u64(row: &Row, idx: usize) -> Option<u64> {
    // Capacity columns are mostly bigint, but max_workers and similar
    // are integer; try both.
    if let Some(val) = row.try_get::<_, Option<i64>>(idx).ok().flatten() {
        return Some(val as u64);
    }
    if let Some(val) = row.try_get::<_, Option<i32>>(idx).ok().flatten() {
        return Some(val as u64);
    }
    None
}

async fn read_quota_row(conn: &PgConn, namespace_str: &str) -> Result<Option<NamespaceQuota>> {
    let row = conn
        .query_opt(
            "SELECT namespace, admitter_kind, admitter_params, dispatcher_kind, dispatcher_params, \
                    max_pending, max_inflight, max_workers, max_waiters_per_replica, \
                    max_submit_rpm, max_dispatch_rpm, max_replay_per_second, \
                    max_retries_ceiling, max_idempotency_ttl_seconds, max_payload_bytes, \
                    max_details_bytes, min_heartbeat_interval_seconds, \
                    lazy_extension_threshold_seconds, max_error_classes, max_task_types, \
                    trace_sampling_ratio, log_level_override, audit_log_retention_days, \
                    metrics_export_enabled \
               FROM namespace_quota WHERE namespace = $1",
            &[&namespace_str],
        )
        .await
        .map_err(map_db_error)?;
    let Some(row) = row else { return Ok(None) };

    let namespace_str_out: String = row.get(0);
    let admitter_kind: String = row.get(1);
    let admitter_params: Vec<u8> = row.get(2);
    let dispatcher_kind: String = row.get(3);
    let dispatcher_params: Vec<u8> = row.get(4);

    Ok(Some(NamespaceQuota {
        namespace: Namespace::new(namespace_str_out),
        admitter_kind,
        admitter_params: Bytes::from(admitter_params),
        dispatcher_kind,
        dispatcher_params: Bytes::from(dispatcher_params),
        max_pending: row.get::<_, Option<i64>>(5).map(|v| v as u64),
        max_inflight: row.get::<_, Option<i64>>(6).map(|v| v as u64),
        max_workers: row.get::<_, Option<i32>>(7).map(|v| v as u32),
        max_waiters_per_replica: row.get::<_, Option<i32>>(8).map(|v| v as u32),
        max_submit_rpm: row.get::<_, Option<i64>>(9).map(|v| v as u64),
        max_dispatch_rpm: row.get::<_, Option<i64>>(10).map(|v| v as u64),
        max_replay_per_second: row.get::<_, Option<i32>>(11).map(|v| v as u32),
        max_retries_ceiling: row.get::<_, i32>(12) as u32,
        max_idempotency_ttl_seconds: row.get::<_, i64>(13) as u64,
        max_payload_bytes: row.get::<_, i32>(14) as u32,
        max_details_bytes: row.get::<_, i32>(15) as u32,
        min_heartbeat_interval_seconds: row.get::<_, i32>(16) as u32,
        lazy_extension_threshold_seconds: row.get::<_, i32>(17) as u32,
        max_error_classes: row.get::<_, i32>(18) as u32,
        max_task_types: row.get::<_, i32>(19) as u32,
        trace_sampling_ratio: row.get::<_, f32>(20),
        log_level_override: row.get::<_, Option<String>>(21),
        audit_log_retention_days: row.get::<_, i32>(22) as u32,
        metrics_export_enabled: row.get::<_, bool>(23),
    }))
}

/// Merge a specific row over a default row: `specific.<field>` wins when set,
/// otherwise fall back to `default.<field>`.
fn merge_quotas(specific: NamespaceQuota, default: NamespaceQuota) -> NamespaceQuota {
    NamespaceQuota {
        namespace: specific.namespace,
        admitter_kind: specific.admitter_kind,
        admitter_params: specific.admitter_params,
        dispatcher_kind: specific.dispatcher_kind,
        dispatcher_params: specific.dispatcher_params,
        max_pending: specific.max_pending.or(default.max_pending),
        max_inflight: specific.max_inflight.or(default.max_inflight),
        max_workers: specific.max_workers.or(default.max_workers),
        max_waiters_per_replica: specific
            .max_waiters_per_replica
            .or(default.max_waiters_per_replica),
        max_submit_rpm: specific.max_submit_rpm.or(default.max_submit_rpm),
        max_dispatch_rpm: specific.max_dispatch_rpm.or(default.max_dispatch_rpm),
        max_replay_per_second: specific
            .max_replay_per_second
            .or(default.max_replay_per_second),
        max_retries_ceiling: specific.max_retries_ceiling,
        max_idempotency_ttl_seconds: specific.max_idempotency_ttl_seconds,
        max_payload_bytes: specific.max_payload_bytes,
        max_details_bytes: specific.max_details_bytes,
        min_heartbeat_interval_seconds: specific.min_heartbeat_interval_seconds,
        lazy_extension_threshold_seconds: specific.lazy_extension_threshold_seconds,
        max_error_classes: specific.max_error_classes,
        max_task_types: specific.max_task_types,
        trace_sampling_ratio: specific.trace_sampling_ratio,
        log_level_override: specific.log_level_override.or(default.log_level_override),
        audit_log_retention_days: specific.audit_log_retention_days,
        metrics_export_enabled: specific.metrics_export_enabled,
    }
}

/// Decompose a `TaskOutcome` into the column shape `task_results` expects
/// plus the next status the `tasks` row should hold and any retry_after.
type OutcomeRow = (
    &'static str,      // outcome enum string
    Option<Vec<u8>>,   // result_payload
    Option<String>,    // error_class
    Option<String>,    // error_message
    Option<Vec<u8>>,   // error_details
    Timestamp,         // recorded_at
    &'static str,      // next status enum string
    Option<Timestamp>, // retry_after
);

fn outcome_to_row(outcome: TaskOutcome) -> OutcomeRow {
    match outcome {
        TaskOutcome::Success {
            result_payload,
            recorded_at,
        } => (
            OUTCOME_SUCCESS,
            Some(result_payload.to_vec()),
            None,
            None,
            None,
            recorded_at,
            STATUS_COMPLETED,
            None,
        ),
        TaskOutcome::FailedNonretryable {
            error_class,
            error_message,
            error_details,
            recorded_at,
        } => (
            OUTCOME_NONRETRYABLE_FAIL,
            None,
            Some(error_class),
            Some(error_message),
            Some(error_details.to_vec()),
            recorded_at,
            STATUS_FAILED_NONRETRYABLE,
            None,
        ),
        TaskOutcome::FailedExhausted {
            error_class,
            error_message,
            error_details,
            recorded_at,
        } => (
            OUTCOME_RETRYABLE_FAIL,
            None,
            Some(error_class),
            Some(error_message),
            Some(error_details.to_vec()),
            recorded_at,
            STATUS_FAILED_EXHAUSTED,
            None,
        ),
        TaskOutcome::Expired {
            error_class,
            error_message,
            error_details,
            recorded_at,
        } => (
            OUTCOME_EXPIRED,
            None,
            Some(error_class),
            Some(error_message),
            Some(error_details.to_vec()),
            recorded_at,
            STATUS_EXPIRED,
            None,
        ),
        TaskOutcome::WaitingRetry {
            error_class,
            error_message,
            error_details,
            retry_after,
            recorded_at,
        } => (
            OUTCOME_RETRYABLE_FAIL,
            None,
            Some(error_class),
            Some(error_message),
            Some(error_details.to_vec()),
            recorded_at,
            STATUS_WAITING_RETRY,
            Some(retry_after),
        ),
    }
}

fn is_safe_partition_name(name: &str) -> bool {
    name.starts_with("idempotency_keys_")
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

// ----------------------------------------------------------------------------
// Stream wrapper around the dispatcher's mpsc receiver.
// ----------------------------------------------------------------------------

struct ReceiverStream {
    rx: mpsc::Receiver<WakeSignal>,
}

impl Stream for ReceiverStream {
    type Item = WakeSignal;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}
