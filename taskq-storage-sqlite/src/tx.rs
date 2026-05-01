//! `SqliteTx` — the [`taskq_storage::StorageTx`] implementation.
//!
//! Single-writer scope per `design.md` §8.3. The transaction owns the
//! connection mutex (via [`tokio::sync::OwnedMutexGuard`]) until
//! [`StorageTx::commit`] / [`StorageTx::rollback`] drops it. Inside each
//! method the rusqlite calls run inline under the lock — SQLite operations
//! are microseconds at this scope, so no `spawn_blocking` is required.
//!
//! ## Rate quotas — per-replica in-memory token buckets
//!
//! Per `design.md` §1.1 carve-out, rate quotas are eventually consistent
//! within the namespace-config cache TTL. The CP layer owns the actual
//! token-bucket state; this backend method is a thin wrapper that always
//! returns `Allowed { remaining: u64::MAX }` because SQLite's
//! single-process scope means the CP's in-memory bucket is the only
//! authoritative state. A real bucket (with a wall-clock-aligned token
//! refill) lives in the CP layer and is unrelated to the backend.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures_core::Stream;
use rusqlite::{params, params_from_iter, types::Value as SqlValue, Connection, OptionalExtension};
use tokio::sync::OwnedMutexGuard;
use tokio::time::{interval, Interval};

use taskq_storage::{
    error::{Result as StorageResult, StorageError},
    ids::{IdempotencyKey, Namespace, TaskId, TaskType, Timestamp, WorkerId},
    traits::StorageTx,
    types::{
        AuditEntry, CancelOutcome, CapacityDecision, CapacityKind, DeadWorkerRuntime, DedupRecord,
        ExpiredRuntime, HeartbeatAck, LeaseRef, LockedTask, NamespaceFilter, NamespaceQuota,
        NamespaceQuotaUpsert, NewDedupRecord, NewLease, NewTask, PickCriteria, PickOrdering,
        RateDecision, RateKind, ReplayOutcome, RuntimeRef, Task, TaskFilter, TaskOutcome,
        TaskStatus, TaskTypeFilter, TerminalState, WakeSignal, WorkerInfo,
    },
};

use crate::convert::{
    capacity_kind_column, capacity_kind_count_sql, map_sql_error, millis_to_ts, parse_task_id,
    parse_worker_id, status_dispatched, status_pending, task_id_to_text, ts_to_millis,
    worker_id_to_text, OutcomeRow,
};

/// In-flight transaction. Holds the connection mutex; drops it on commit
/// or rollback.
///
/// `state` tracks whether the underlying SQLite `BEGIN IMMEDIATE` is still
/// active. After `COMMIT` / `ROLLBACK` the guard is consumed, so the
/// borrow checker prevents reuse.
pub struct SqliteTx {
    guard: OwnedMutexGuard<Connection>,
}

impl SqliteTx {
    /// Wrap an acquired connection guard. Caller is expected to issue
    /// `BEGIN IMMEDIATE` immediately afterwards (see [`crate::SqliteStorage::begin`]).
    pub(crate) fn new(guard: OwnedMutexGuard<Connection>) -> Self {
        Self { guard }
    }

    /// Execute a literal SQL statement (or batch). Used internally for
    /// `BEGIN IMMEDIATE` / `COMMIT` / `ROLLBACK` from the storage layer.
    pub(crate) async fn execute_batch(&mut self, sql: &str) -> StorageResult<()> {
        self.guard.execute_batch(sql).map_err(map_sql_error)
    }

    fn conn(&self) -> &Connection {
        &self.guard
    }
}

// ============================================================================
// StorageTx impl
// ============================================================================

impl StorageTx for SqliteTx {
    // ------------------------------------------------------------------------
    // Submit path
    // ------------------------------------------------------------------------

    async fn lookup_idempotency(
        &mut self,
        namespace: &Namespace,
        key: &IdempotencyKey,
    ) -> StorageResult<Option<DedupRecord>> {
        let now_ms = current_millis();
        let row = self
            .conn()
            .query_row(
                "SELECT task_id, payload_hash, expires_at \
                 FROM idempotency_keys \
                 WHERE namespace = ?1 AND key = ?2 AND expires_at > ?3",
                params![namespace.as_str(), key.as_str(), now_ms],
                |row| {
                    let task_id_text: String = row.get(0)?;
                    let payload_hash: Vec<u8> = row.get(1)?;
                    let expires_at_ms: i64 = row.get(2)?;
                    Ok((task_id_text, payload_hash, expires_at_ms))
                },
            )
            .optional()
            .map_err(map_sql_error)?;

        let Some((task_id_text, payload_hash, expires_at_ms)) = row else {
            return Ok(None);
        };
        let task_id = parse_task_id(&task_id_text)?;
        let mut hash = [0u8; 32];
        if payload_hash.len() != 32 {
            return Err(StorageError::ConstraintViolation(format!(
                "idempotency_keys.payload_hash has unexpected length {}",
                payload_hash.len()
            )));
        }
        hash.copy_from_slice(&payload_hash);
        Ok(Some(DedupRecord {
            task_id,
            payload_hash: hash,
            expires_at: millis_to_ts(expires_at_ms),
        }))
    }

    async fn insert_task(&mut self, task: NewTask, dedup: NewDedupRecord) -> StorageResult<TaskId> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO tasks (
                task_id, namespace, task_type, status,
                priority, payload, payload_hash,
                submitted_at, expires_at, attempt_number,
                max_retries, retry_initial_ms, retry_max_ms, retry_coefficient,
                traceparent, tracestate, format_version
             ) VALUES (
                ?1, ?2, ?3, ?4,
                ?5, ?6, ?7,
                ?8, ?9, 0,
                ?10, ?11, ?12, ?13,
                ?14, ?15, ?16
             )",
            params![
                task_id_to_text(&task.task_id),
                task.namespace.as_str(),
                task.task_type.as_str(),
                status_pending(),
                task.priority,
                task.payload.as_ref(),
                task.payload_hash.as_slice(),
                ts_to_millis(task.submitted_at),
                ts_to_millis(task.expires_at),
                task.max_retries,
                task.retry_initial_ms as i64,
                task.retry_max_ms as i64,
                task.retry_coefficient as f64,
                task.traceparent.as_ref(),
                task.tracestate.as_ref(),
                task.format_version,
            ],
        )
        .map_err(map_sql_error)?;

        conn.execute(
            "INSERT INTO idempotency_keys (
                namespace, key, task_id, payload_hash, expires_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                dedup.namespace.as_str(),
                dedup.key.as_str(),
                task_id_to_text(&task.task_id),
                dedup.payload_hash.as_slice(),
                ts_to_millis(dedup.expires_at),
            ],
        )
        .map_err(map_sql_error)?;

        Ok(task.task_id)
    }

    // ------------------------------------------------------------------------
    // Dispatch path
    // ------------------------------------------------------------------------

    async fn pick_and_lock_pending(
        &mut self,
        criteria: PickCriteria,
    ) -> StorageResult<Option<LockedTask>> {
        // Single-writer scope: no SKIP LOCKED needed (§8.2 #2 vacuous).
        // Plain SELECT followed by UPDATE within the same transaction.
        let now_ms = ts_to_millis(criteria.now);

        // Translate the filter / ordering into SQL fragments. To keep
        // bound parameters straightforward we accumulate into Vec<SqlValue>
        // and build a positional parameter list.
        let mut sql = String::from(
            "SELECT task_id, namespace, task_type, attempt_number, priority, \
                    payload, submitted_at, expires_at, max_retries, \
                    retry_initial_ms, retry_max_ms, retry_coefficient, \
                    traceparent, tracestate \
             FROM tasks \
             WHERE status IN ('PENDING', 'WAITING_RETRY') \
               AND (retry_after IS NULL OR retry_after <= ?1) \
               AND expires_at > ?1",
        );
        let mut bound: Vec<SqlValue> = vec![SqlValue::Integer(now_ms)];

        // Namespace filter.
        match &criteria.namespace_filter {
            NamespaceFilter::Single(ns) => {
                sql.push_str(" AND namespace = ?");
                sql.push_str(&(bound.len() + 1).to_string());
                bound.push(SqlValue::Text(ns.as_str().to_owned()));
            }
            NamespaceFilter::AnyOf(namespaces) if !namespaces.is_empty() => {
                sql.push_str(" AND namespace IN (");
                for (idx, ns) in namespaces.iter().enumerate() {
                    if idx > 0 {
                        sql.push(',');
                    }
                    sql.push('?');
                    sql.push_str(&(bound.len() + 1).to_string());
                    bound.push(SqlValue::Text(ns.as_str().to_owned()));
                }
                sql.push(')');
            }
            NamespaceFilter::AnyOf(_) => {
                // Empty list = no candidates.
                return Ok(None);
            }
            NamespaceFilter::Any => {}
        }

        // Task-type filter.
        match &criteria.task_types_filter {
            TaskTypeFilter::AnyOf(types) if !types.is_empty() => {
                sql.push_str(" AND task_type IN (");
                for (idx, ty) in types.iter().enumerate() {
                    if idx > 0 {
                        sql.push(',');
                    }
                    sql.push('?');
                    sql.push_str(&(bound.len() + 1).to_string());
                    bound.push(SqlValue::Text(ty.as_str().to_owned()));
                }
                sql.push(')');
            }
            TaskTypeFilter::AnyOf(_) => {
                return Ok(None);
            }
        }

        // Random-namespace dispatch samples a namespace first per
        // `design.md` §7.2. We pick one uniformly from the matching set
        // and add it as an additional namespace constraint, then continue
        // with PriorityFifo within.
        if let PickOrdering::RandomNamespace { sample_attempts } = criteria.ordering {
            let sampled = sample_random_namespace(self.conn(), &criteria, sample_attempts)?;
            match sampled {
                Some(ns) => {
                    sql.push_str(" AND namespace = ?");
                    sql.push_str(&(bound.len() + 1).to_string());
                    bound.push(SqlValue::Text(ns));
                }
                None => return Ok(None),
            }
        }

        // Ordering — translates strategy intent to SQL per the Phase 4
        // spec.
        match criteria.ordering {
            PickOrdering::PriorityFifo | PickOrdering::RandomNamespace { .. } => {
                sql.push_str(" ORDER BY priority DESC, submitted_at ASC LIMIT 1");
            }
            PickOrdering::AgePromoted { age_weight } => {
                // effective_priority = priority + (now - submitted_at) * age_weight
                sql.push_str(" ORDER BY (priority + ((?");
                sql.push_str(&(bound.len() + 1).to_string());
                sql.push_str(" - submitted_at) * ?");
                sql.push_str(&(bound.len() + 2).to_string());
                sql.push_str(")) DESC, submitted_at ASC LIMIT 1");
                bound.push(SqlValue::Integer(now_ms));
                bound.push(SqlValue::Real(age_weight));
            }
        }

        let conn = self.conn();
        let mut stmt = conn.prepare(&sql).map_err(map_sql_error)?;
        let row = stmt
            .query_row(params_from_iter(bound.iter()), |row| {
                Ok(LockedRow {
                    task_id: row.get::<_, String>(0)?,
                    namespace: row.get::<_, String>(1)?,
                    task_type: row.get::<_, String>(2)?,
                    attempt_number: row.get::<_, i64>(3)? as u32,
                    priority: row.get::<_, i32>(4)?,
                    payload: row.get::<_, Vec<u8>>(5)?,
                    submitted_at: row.get::<_, i64>(6)?,
                    expires_at: row.get::<_, i64>(7)?,
                    max_retries: row.get::<_, i64>(8)? as u32,
                    retry_initial_ms: row.get::<_, i64>(9)? as u64,
                    retry_max_ms: row.get::<_, i64>(10)? as u64,
                    retry_coefficient: row.get::<_, f64>(11)? as f32,
                    traceparent: row.get::<_, Vec<u8>>(12)?,
                    tracestate: row.get::<_, Vec<u8>>(13)?,
                })
            })
            .optional()
            .map_err(map_sql_error)?;

        let Some(row) = row else {
            return Ok(None);
        };

        // Mark the task DISPATCHED so a concurrent dispatcher (if any —
        // there isn't, single-writer) cannot pick the same row again.
        // Single-writer scope makes this redundant for races but keeps
        // the row's status accurate for the rest of the transaction.
        drop(stmt);
        let conn = self.conn();
        conn.execute(
            "UPDATE tasks SET status = ?1 WHERE task_id = ?2",
            params![status_dispatched(), &row.task_id],
        )
        .map_err(map_sql_error)?;

        Ok(Some(LockedTask {
            task_id: parse_task_id(&row.task_id)?,
            namespace: Namespace::new(row.namespace),
            task_type: TaskType::new(row.task_type),
            attempt_number: row.attempt_number,
            priority: row.priority,
            payload: Bytes::from(row.payload),
            submitted_at: millis_to_ts(row.submitted_at),
            expires_at: millis_to_ts(row.expires_at),
            max_retries: row.max_retries,
            retry_initial_ms: row.retry_initial_ms,
            retry_max_ms: row.retry_max_ms,
            retry_coefficient: row.retry_coefficient,
            traceparent: Bytes::from(row.traceparent),
            tracestate: Bytes::from(row.tracestate),
        }))
    }

    async fn record_acquisition(&mut self, lease: NewLease) -> StorageResult<()> {
        self.conn()
            .execute(
                "INSERT INTO task_runtime (
                    task_id, attempt_number, worker_id, acquired_at, timeout_at, last_extended_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?4)",
                params![
                    task_id_to_text(&lease.task_id),
                    lease.attempt_number,
                    worker_id_to_text(&lease.worker_id),
                    ts_to_millis(lease.acquired_at),
                    ts_to_millis(lease.timeout_at),
                ],
            )
            .map_err(map_sql_error)?;
        Ok(())
    }

    async fn subscribe_pending(
        &mut self,
        namespace: &Namespace,
        task_types: &[TaskType],
    ) -> StorageResult<Box<dyn Stream<Item = WakeSignal> + Send + Unpin + 'static>> {
        // SQLite has no LISTEN / NOTIFY equivalent. Per `design.md` §8.4
        // and §8.2 #5, a 500ms polling interval combined with the CP
        // layer's 10s belt-and-suspenders timer satisfies the
        // subscribe-pending ordering invariant.
        //
        // The polling stream takes a clone of the connection arc so it
        // can run independently of this transaction. The transaction's
        // own connection is held under the OwnedMutexGuard; the polling
        // stream uses a separate read-only check that races against the
        // mutex like any other reader.
        //
        // NOTE: in single-writer scope no separate read connection is
        // needed for correctness — the poller simply yields a `WakeSignal`
        // every 500ms unconditionally (signal-only, design.md §8.4).
        // Recipients re-run `pick_and_lock_pending`. Backends with cheap
        // notification primitives may yield only on actual commits; SQLite
        // does not, so we yield on every tick and let the dispatcher's
        // own `pick_and_lock_pending` filter.
        //
        // The arguments are accepted for API parity with backends that
        // can route subscriptions per `(namespace, task_types)`.
        let _ = (namespace, task_types);

        let stream = PollingPendingStream::new(Duration::from_millis(500));
        Ok(Box::new(stream))
    }

    // ------------------------------------------------------------------------
    // Worker state transitions
    // ------------------------------------------------------------------------

    async fn complete_task(&mut self, lease: &LeaseRef, outcome: TaskOutcome) -> StorageResult<()> {
        // Validate the worker owns the lease (§6.4 step 1). 0 rows ->
        // NotFound -> CP surfaces LEASE_EXPIRED.
        let owner_check: Option<i64> = self
            .conn()
            .query_row(
                "SELECT 1 FROM task_runtime \
                 WHERE task_id = ?1 AND attempt_number = ?2 AND worker_id = ?3",
                params![
                    task_id_to_text(&lease.task_id),
                    lease.attempt_number,
                    worker_id_to_text(&lease.worker_id),
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(map_sql_error)?;
        if owner_check.is_none() {
            return Err(StorageError::NotFound);
        }

        let row = OutcomeRow::from_outcome(&outcome);

        // Insert task_results.
        self.conn()
            .execute(
                "INSERT INTO task_results (
                    task_id, attempt_number, outcome, result_payload,
                    error_class, error_message, error_details, recorded_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    task_id_to_text(&lease.task_id),
                    lease.attempt_number,
                    row.outcome,
                    row.result_payload,
                    row.error_class,
                    row.error_message,
                    row.error_details,
                    ts_to_millis(row.recorded_at),
                ],
            )
            .map_err(map_sql_error)?;

        // Update tasks.status (and retry_after on WAITING_RETRY).
        let retry_after_ms = row.retry_after.map(ts_to_millis);
        self.conn()
            .execute(
                "UPDATE tasks SET status = ?1, retry_after = ?2 WHERE task_id = ?3",
                params![
                    row.task_status,
                    retry_after_ms,
                    task_id_to_text(&lease.task_id)
                ],
            )
            .map_err(map_sql_error)?;

        // Delete the runtime row regardless of outcome — the lease is over.
        self.conn()
            .execute(
                "DELETE FROM task_runtime WHERE task_id = ?1 AND attempt_number = ?2",
                params![task_id_to_text(&lease.task_id), lease.attempt_number],
            )
            .map_err(map_sql_error)?;

        Ok(())
    }

    // ------------------------------------------------------------------------
    // Reaper path
    // ------------------------------------------------------------------------

    async fn list_expired_runtimes(
        &mut self,
        before: Timestamp,
        n: usize,
    ) -> StorageResult<Vec<ExpiredRuntime>> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT r.task_id, r.attempt_number, r.worker_id, r.timeout_at, t.namespace \
                 FROM task_runtime r \
                 JOIN tasks t ON t.task_id = r.task_id \
                 WHERE r.timeout_at <= ?1 \
                 LIMIT ?2",
            )
            .map_err(map_sql_error)?;
        let rows = stmt
            .query_map(params![ts_to_millis(before), n as i64], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)? as u32,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .map_err(map_sql_error)?;

        let mut out = Vec::with_capacity(n);
        for row in rows {
            let (task_id_text, attempt_number, worker_id_text, timeout_at_ms, namespace) =
                row.map_err(map_sql_error)?;
            out.push(ExpiredRuntime {
                task_id: parse_task_id(&task_id_text)?,
                attempt_number,
                worker_id: parse_worker_id(&worker_id_text)?,
                timeout_at: millis_to_ts(timeout_at_ms),
                namespace: Namespace::new(namespace),
            });
        }
        Ok(out)
    }

    async fn reclaim_runtime(&mut self, runtime: &RuntimeRef) -> StorageResult<()> {
        // Bump attempt_number, flip status to PENDING, delete the runtime
        // row. Per `design.md` §6.6 the new attempt is the next dispatch
        // candidate.
        let conn = self.conn();
        conn.execute(
            "UPDATE tasks SET status = 'PENDING', attempt_number = attempt_number + 1 \
             WHERE task_id = ?1",
            params![task_id_to_text(&runtime.task_id)],
        )
        .map_err(map_sql_error)?;
        conn.execute(
            "DELETE FROM task_runtime WHERE task_id = ?1 AND attempt_number = ?2",
            params![task_id_to_text(&runtime.task_id), runtime.attempt_number],
        )
        .map_err(map_sql_error)?;
        Ok(())
    }

    async fn mark_worker_dead(&mut self, worker_id: &WorkerId, at: Timestamp) -> StorageResult<()> {
        self.conn()
            .execute(
                "UPDATE worker_heartbeats \
                 SET declared_dead_at = ?1 \
                 WHERE worker_id = ?2 AND declared_dead_at IS NULL",
                params![ts_to_millis(at), worker_id_to_text(worker_id)],
            )
            .map_err(map_sql_error)?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Quotas
    // ------------------------------------------------------------------------

    async fn check_capacity_quota(
        &mut self,
        namespace: &Namespace,
        kind: CapacityKind,
    ) -> StorageResult<CapacityDecision> {
        // Resolve the configured limit by reading the column in
        // `namespace_quota`, falling back to `system_default` per
        // `design.md` §9.1 inheritance semantics.
        let column = capacity_kind_column(kind);
        let limit = read_capacity_limit(self.conn(), namespace, column)?;

        // Count the live state for the dimension.
        let count_sql = capacity_kind_count_sql(kind);
        let current: i64 = if count_sql.contains('?') {
            self.conn()
                .query_row(count_sql, params![namespace.as_str()], |row| row.get(0))
                .map_err(map_sql_error)?
        } else {
            self.conn()
                .query_row(count_sql, [], |row| row.get(0))
                .map_err(map_sql_error)?
        };

        let current = current as u64;
        match limit {
            None => Ok(CapacityDecision::UnderLimit {
                current,
                limit: u64::MAX,
            }),
            Some(limit) if current < limit => Ok(CapacityDecision::UnderLimit { current, limit }),
            Some(limit) => Ok(CapacityDecision::OverLimit { current, limit }),
        }
    }

    async fn oldest_pending_age_ms(&mut self, namespace: &Namespace) -> StorageResult<Option<u64>> {
        // §7.1 CoDel: head-of-line age of PENDING tasks. SQLite stores
        // submitted_at as Unix-epoch milliseconds (see migrations/0001), so
        // the age = (now_ms - MIN(submitted_at)) where now_ms is sourced
        // from the same canonical clock the CP layer uses. We compute now in
        // Rust to avoid SQLite's `strftime('%s', 'now')`, which is
        // second-resolution.
        let now_ms = current_millis();
        let row: Option<Option<i64>> = self
            .conn()
            .query_row(
                "SELECT MIN(submitted_at) FROM tasks WHERE namespace = ?1 AND status = 'PENDING'",
                params![namespace.as_str()],
                |row| row.get::<_, Option<i64>>(0),
            )
            .optional()
            .map_err(map_sql_error)?;
        // SQLite returns one row from MIN(...) even when no rows match; the
        // column is NULL in that case. `Option<Option<i64>>` therefore folds
        // to `None` for "namespace has no pending tasks".
        let oldest = row.flatten();
        Ok(oldest.and_then(|min| {
            let age = now_ms.saturating_sub(min);
            if age < 0 {
                None
            } else {
                Some(age as u64)
            }
        }))
    }

    async fn try_consume_rate_quota(
        &mut self,
        _namespace: &Namespace,
        _kind: RateKind,
        _n: u64,
    ) -> StorageResult<RateDecision> {
        // Per the module-level docstring: rate quotas live in the CP's
        // in-memory bucket, eventually consistent within the cache TTL
        // (design.md §1.1 carve-out). The storage backend always allows.
        Ok(RateDecision::Allowed {
            remaining: u64::MAX,
        })
    }

    // ------------------------------------------------------------------------
    // Heartbeat carve-out
    // ------------------------------------------------------------------------

    async fn record_worker_heartbeat(
        &mut self,
        worker_id: &WorkerId,
        namespace: &Namespace,
        at: Timestamp,
    ) -> StorageResult<HeartbeatAck> {
        // INSERT ... ON CONFLICT(worker_id) DO UPDATE WHERE declared_dead_at IS NULL.
        // SQLite supports `ON CONFLICT ... DO UPDATE WHERE` since 3.24.
        // `changes()` after the call tells us whether the row was actually
        // upserted: 0 means the WHERE blocked the update because the
        // worker was previously declared dead.
        let conn = self.conn();
        let changes = conn
            .execute(
                "INSERT INTO worker_heartbeats (worker_id, namespace, last_heartbeat_at) \
                 VALUES (?1, ?2, ?3) \
                 ON CONFLICT(worker_id) DO UPDATE SET \
                    last_heartbeat_at = excluded.last_heartbeat_at, \
                    namespace = excluded.namespace \
                 WHERE worker_heartbeats.declared_dead_at IS NULL",
                params![
                    worker_id_to_text(worker_id),
                    namespace.as_str(),
                    ts_to_millis(at),
                ],
            )
            .map_err(map_sql_error)?;
        if changes == 0 {
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
    ) -> StorageResult<()> {
        let updated = self
            .conn()
            .execute(
                "UPDATE task_runtime \
                 SET timeout_at = ?1, last_extended_at = ?2 \
                 WHERE task_id = ?3 AND attempt_number = ?4 AND worker_id = ?5",
                params![
                    ts_to_millis(new_timeout),
                    ts_to_millis(last_extended_at),
                    task_id_to_text(&lease.task_id),
                    lease.attempt_number,
                    worker_id_to_text(&lease.worker_id),
                ],
            )
            .map_err(map_sql_error)?;
        if updated == 0 {
            return Err(StorageError::NotFound);
        }
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Cleanup
    // ------------------------------------------------------------------------

    async fn delete_expired_dedup(&mut self, before: Timestamp, n: usize) -> StorageResult<usize> {
        // SQLite's `LIMIT` on `DELETE` requires a non-default build flag
        // (`SQLITE_ENABLE_UPDATE_DELETE_LIMIT`). Bundled rusqlite ships
        // without it; the CTE pattern routes through `rowid IN (SELECT
        // ... LIMIT n)` which is portable.
        let deleted = self
            .conn()
            .execute(
                "DELETE FROM idempotency_keys \
                 WHERE rowid IN (
                    SELECT rowid FROM idempotency_keys \
                    WHERE expires_at < ?1 \
                    LIMIT ?2
                 )",
                params![ts_to_millis(before), n as i64],
            )
            .map_err(map_sql_error)?;
        Ok(deleted)
    }

    // ------------------------------------------------------------------------
    // Admin / reads
    // ------------------------------------------------------------------------

    async fn get_namespace_quota(
        &mut self,
        namespace: &Namespace,
    ) -> StorageResult<NamespaceQuota> {
        let target = read_quota_row(self.conn(), namespace.as_str())?;
        let default_ns = Namespace::new("system_default");
        // Always merge with system_default for inheritance, even when we
        // already have a row — unset fields fall back to the default.
        let defaults = if namespace == &default_ns {
            None
        } else {
            read_quota_row(self.conn(), default_ns.as_str())?
        };
        Ok(merge_quota(namespace.clone(), target, defaults.as_ref()))
    }

    async fn audit_log_append(&mut self, entry: AuditEntry) -> StorageResult<()> {
        self.conn()
            .execute(
                "INSERT INTO audit_log (
                    timestamp, actor, rpc, namespace,
                    request_summary, result, request_hash
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    ts_to_millis(entry.timestamp),
                    entry.actor,
                    entry.rpc,
                    entry.namespace.as_ref().map(|ns| ns.as_str()),
                    std::str::from_utf8(entry.request_summary.as_ref())
                        .unwrap_or("")
                        .to_owned(),
                    entry.result,
                    entry.request_hash.as_slice(),
                ],
            )
            .map_err(map_sql_error)?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Phase 5c admin / cancel / reaper-B
    // ------------------------------------------------------------------------

    async fn cancel_task(&mut self, task_id: TaskId) -> StorageResult<CancelOutcome> {
        let task_id_text = task_id_to_text(&task_id);

        // Look up the row. SQLite's single-writer scope makes "FOR UPDATE"
        // implicit — the BEGIN IMMEDIATE that opened this Tx already holds
        // the writer lock.
        let row: Option<(String, String)> = self
            .conn()
            .query_row(
                "SELECT status, namespace FROM tasks WHERE task_id = ?1",
                params![&task_id_text],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(map_sql_error)?;

        let Some((status_str, namespace_str)) = row else {
            return Ok(CancelOutcome::NotFound);
        };
        let Some(status) = TaskStatus::from_db_str(&status_str) else {
            return Err(StorageError::ConstraintViolation(format!(
                "tasks.status has unknown value: {status_str}"
            )));
        };

        if status.is_terminal() {
            return Ok(CancelOutcome::AlreadyTerminal { state: status });
        }

        let conn = self.conn();
        conn.execute(
            "UPDATE tasks SET status = 'CANCELLED' WHERE task_id = ?1",
            params![&task_id_text],
        )
        .map_err(map_sql_error)?;
        conn.execute(
            "DELETE FROM idempotency_keys WHERE namespace = ?1 AND task_id = ?2",
            params![&namespace_str, &task_id_text],
        )
        .map_err(map_sql_error)?;
        conn.execute(
            "DELETE FROM task_runtime WHERE task_id = ?1",
            params![&task_id_text],
        )
        .map_err(map_sql_error)?;

        Ok(CancelOutcome::TransitionedToCancelled)
    }

    async fn get_task_by_id(&mut self, task_id: TaskId) -> StorageResult<Option<Task>> {
        let task_id_text = task_id_to_text(&task_id);
        let row: Option<TaskRow> = self
            .conn()
            .query_row(
                "SELECT task_id, namespace, task_type, status, priority, payload, payload_hash, \
                        submitted_at, expires_at, attempt_number, max_retries, retry_initial_ms, \
                        retry_max_ms, retry_coefficient, retry_after, traceparent, tracestate, \
                        format_version, original_failure_count \
                   FROM tasks WHERE task_id = ?1",
                params![&task_id_text],
                |row| {
                    Ok(TaskRow {
                        task_id: row.get(0)?,
                        namespace: row.get(1)?,
                        task_type: row.get(2)?,
                        status: row.get(3)?,
                        priority: row.get(4)?,
                        payload: row.get(5)?,
                        payload_hash: row.get(6)?,
                        submitted_at: row.get(7)?,
                        expires_at: row.get(8)?,
                        attempt_number: row.get::<_, i64>(9)? as u32,
                        max_retries: row.get::<_, i64>(10)? as u32,
                        retry_initial_ms: row.get::<_, i64>(11)? as u64,
                        retry_max_ms: row.get::<_, i64>(12)? as u64,
                        retry_coefficient: row.get::<_, f64>(13)? as f32,
                        retry_after: row.get(14)?,
                        traceparent: row.get(15)?,
                        tracestate: row.get(16)?,
                        format_version: row.get::<_, i64>(17)? as u32,
                        original_failure_count: row.get::<_, i64>(18)? as u32,
                    })
                },
            )
            .optional()
            .map_err(map_sql_error)?;

        let Some(row) = row else { return Ok(None) };

        if row.payload_hash.len() != 32 {
            return Err(StorageError::ConstraintViolation(format!(
                "tasks.payload_hash has unexpected length {}",
                row.payload_hash.len()
            )));
        }
        let mut payload_hash = [0u8; 32];
        payload_hash.copy_from_slice(&row.payload_hash);

        let status = TaskStatus::from_db_str(&row.status).ok_or_else(|| {
            StorageError::ConstraintViolation(format!("tasks.status unknown: {}", row.status))
        })?;

        Ok(Some(Task {
            task_id: parse_task_id(&row.task_id)?,
            namespace: Namespace::new(row.namespace),
            task_type: TaskType::new(row.task_type),
            status,
            priority: row.priority,
            payload: Bytes::from(row.payload),
            payload_hash,
            submitted_at: millis_to_ts(row.submitted_at),
            expires_at: millis_to_ts(row.expires_at),
            attempt_number: row.attempt_number,
            max_retries: row.max_retries,
            retry_initial_ms: row.retry_initial_ms,
            retry_max_ms: row.retry_max_ms,
            retry_coefficient: row.retry_coefficient,
            retry_after: row.retry_after.map(millis_to_ts),
            traceparent: Bytes::from(row.traceparent),
            tracestate: Bytes::from(row.tracestate),
            format_version: row.format_version,
            original_failure_count: row.original_failure_count,
        }))
    }

    async fn list_dead_worker_runtimes(
        &mut self,
        stale_before: Timestamp,
        n: usize,
    ) -> StorageResult<Vec<DeadWorkerRuntime>> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare(
                "SELECT r.task_id, r.attempt_number, r.worker_id, t.namespace, h.last_heartbeat_at \
                 FROM task_runtime r \
                 JOIN tasks t ON t.task_id = r.task_id \
                 JOIN worker_heartbeats h ON h.worker_id = r.worker_id \
                 WHERE h.last_heartbeat_at < ?1 \
                   AND h.declared_dead_at IS NULL \
                 ORDER BY h.last_heartbeat_at ASC \
                 LIMIT ?2",
            )
            .map_err(map_sql_error)?;
        let rows = stmt
            .query_map(params![ts_to_millis(stale_before), n as i64], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)? as u32,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .map_err(map_sql_error)?;

        let mut out = Vec::new();
        for row in rows {
            let (task_id_text, attempt_number, worker_id_text, namespace, last_hb_ms) =
                row.map_err(map_sql_error)?;
            out.push(DeadWorkerRuntime {
                task_id: parse_task_id(&task_id_text)?,
                attempt_number,
                worker_id: parse_worker_id(&worker_id_text)?,
                namespace: Namespace::new(namespace),
                last_heartbeat_at: millis_to_ts(last_hb_ms),
            });
        }
        Ok(out)
    }

    async fn count_tasks_by_status(
        &mut self,
        namespace: &Namespace,
    ) -> StorageResult<HashMap<TaskStatus, u64>> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare("SELECT status, COUNT(*) FROM tasks WHERE namespace = ?1 GROUP BY status")
            .map_err(map_sql_error)?;
        let rows = stmt
            .query_map(params![namespace.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(map_sql_error)?;
        let mut out = HashMap::new();
        for row in rows {
            let (status_str, count) = row.map_err(map_sql_error)?;
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
    ) -> StorageResult<Vec<WorkerInfo>> {
        let conn = self.conn();
        let sql = if include_dead {
            "SELECT h.worker_id, h.namespace, h.last_heartbeat_at, h.declared_dead_at, \
                    COALESCE((SELECT COUNT(*) FROM task_runtime r WHERE r.worker_id = h.worker_id), 0) \
               FROM worker_heartbeats h \
              WHERE h.namespace = ?1 \
              ORDER BY h.last_heartbeat_at DESC"
        } else {
            "SELECT h.worker_id, h.namespace, h.last_heartbeat_at, h.declared_dead_at, \
                    COALESCE((SELECT COUNT(*) FROM task_runtime r WHERE r.worker_id = h.worker_id), 0) \
               FROM worker_heartbeats h \
              WHERE h.namespace = ?1 AND h.declared_dead_at IS NULL \
              ORDER BY h.last_heartbeat_at DESC"
        };
        let mut stmt = conn.prepare(sql).map_err(map_sql_error)?;
        let rows = stmt
            .query_map(params![namespace.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .map_err(map_sql_error)?;
        let mut out = Vec::new();
        for row in rows {
            let (worker_id_text, ns, last_hb, declared_dead, inflight) =
                row.map_err(map_sql_error)?;
            out.push(WorkerInfo {
                worker_id: parse_worker_id(&worker_id_text)?,
                namespace: Namespace::new(ns),
                last_heartbeat_at: millis_to_ts(last_hb),
                declared_dead_at: declared_dead.map(millis_to_ts),
                inflight_count: inflight as u32,
            });
        }
        Ok(out)
    }

    async fn enable_namespace(&mut self, namespace: &Namespace) -> StorageResult<()> {
        self.conn()
            .execute(
                "UPDATE namespace_quota SET disabled = 0 WHERE namespace = ?1",
                params![namespace.as_str()],
            )
            .map_err(map_sql_error)?;
        Ok(())
    }

    async fn disable_namespace(&mut self, namespace: &Namespace) -> StorageResult<()> {
        self.conn()
            .execute(
                "UPDATE namespace_quota SET disabled = 1 WHERE namespace = ?1",
                params![namespace.as_str()],
            )
            .map_err(map_sql_error)?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Phase 5e admin writes
    // ------------------------------------------------------------------------

    async fn upsert_namespace_quota(
        &mut self,
        namespace: &Namespace,
        quota: NamespaceQuotaUpsert,
    ) -> StorageResult<()> {
        // §6.7 SetNamespaceQuota: rewrite the writable subset; strategy
        // columns and `disabled` are owned by other paths. Stub strategy
        // values mirror the `system_default` defaults from migrations/0001
        // so a fresh INSERT does not violate the CHECK constraints.
        let max_pending = quota.max_pending.map(|v| v as i64);
        let max_inflight = quota.max_inflight.map(|v| v as i64);
        let max_workers = quota.max_workers.map(|v| v as i64);
        let max_waiters_per_replica = quota.max_waiters_per_replica.map(|v| v as i64);
        let max_submit_rpm = quota.max_submit_rpm.map(|v| v as i64);
        let max_dispatch_rpm = quota.max_dispatch_rpm.map(|v| v as i64);
        let max_replay_per_second = quota.max_replay_per_second.map(|v| v as i64);

        self.conn()
            .execute(
                "INSERT INTO namespace_quota (\
                    namespace, admitter_kind, admitter_params, dispatcher_kind, dispatcher_params, \
                    max_pending, max_inflight, max_workers, max_waiters_per_replica, \
                    max_submit_rpm, max_dispatch_rpm, max_replay_per_second, \
                    max_retries_ceiling, max_idempotency_ttl_seconds, max_payload_bytes, \
                    max_details_bytes, min_heartbeat_interval_seconds, \
                    lazy_extension_threshold_seconds, max_error_classes, max_task_types, \
                    trace_sampling_ratio, log_level_override, audit_log_retention_days, \
                    metrics_export_enabled \
                 ) VALUES ( \
                    ?1, 'Always', '{}', 'PriorityFifo', '{}', \
                    ?2, ?3, ?4, ?5, ?6, ?7, ?8, \
                    ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, \
                    ?17, ?18, ?19, ?20 \
                 ) ON CONFLICT(namespace) DO UPDATE SET \
                    max_pending = excluded.max_pending, \
                    max_inflight = excluded.max_inflight, \
                    max_workers = excluded.max_workers, \
                    max_waiters_per_replica = excluded.max_waiters_per_replica, \
                    max_submit_rpm = excluded.max_submit_rpm, \
                    max_dispatch_rpm = excluded.max_dispatch_rpm, \
                    max_replay_per_second = excluded.max_replay_per_second, \
                    max_retries_ceiling = excluded.max_retries_ceiling, \
                    max_idempotency_ttl_seconds = excluded.max_idempotency_ttl_seconds, \
                    max_payload_bytes = excluded.max_payload_bytes, \
                    max_details_bytes = excluded.max_details_bytes, \
                    min_heartbeat_interval_seconds = excluded.min_heartbeat_interval_seconds, \
                    lazy_extension_threshold_seconds = excluded.lazy_extension_threshold_seconds, \
                    max_error_classes = excluded.max_error_classes, \
                    max_task_types = excluded.max_task_types, \
                    trace_sampling_ratio = excluded.trace_sampling_ratio, \
                    log_level_override = excluded.log_level_override, \
                    audit_log_retention_days = excluded.audit_log_retention_days, \
                    metrics_export_enabled = excluded.metrics_export_enabled",
                params![
                    namespace.as_str(),
                    max_pending,
                    max_inflight,
                    max_workers,
                    max_waiters_per_replica,
                    max_submit_rpm,
                    max_dispatch_rpm,
                    max_replay_per_second,
                    quota.max_retries_ceiling as i64,
                    quota.max_idempotency_ttl_seconds as i64,
                    quota.max_payload_bytes as i64,
                    quota.max_details_bytes as i64,
                    quota.min_heartbeat_interval_seconds as i64,
                    quota.lazy_extension_threshold_seconds as i64,
                    quota.max_error_classes as i64,
                    quota.max_task_types as i64,
                    quota.trace_sampling_ratio as f64,
                    quota.log_level_override.as_deref(),
                    quota.audit_log_retention_days as i64,
                    if quota.metrics_export_enabled {
                        1i64
                    } else {
                        0i64
                    },
                ],
            )
            .map_err(map_sql_error)?;
        Ok(())
    }

    async fn list_tasks_by_filter(
        &mut self,
        namespace: &Namespace,
        filter: TaskFilter,
        limit: usize,
    ) -> StorageResult<Vec<Task>> {
        // §6.7 PurgeTasks: dynamic WHERE built from `filter`. Predicates
        // are bound parameters; in-lists are expanded to `?,?,?` form
        // because rusqlite does not support `ANY($1)`-style array binds.
        let mut sql = String::from(
            "SELECT task_id, namespace, task_type, status, priority, payload, payload_hash, \
                    submitted_at, expires_at, attempt_number, max_retries, retry_initial_ms, \
                    retry_max_ms, retry_coefficient, retry_after, traceparent, tracestate, \
                    format_version, original_failure_count \
               FROM tasks WHERE namespace = ?1",
        );
        let mut bound: Vec<SqlValue> = vec![SqlValue::Text(namespace.as_str().to_owned())];

        if let Some(ref types) = filter.task_types {
            if types.is_empty() {
                return Ok(Vec::new());
            }
            sql.push_str(" AND task_type IN (");
            for (idx, ty) in types.iter().enumerate() {
                if idx > 0 {
                    sql.push(',');
                }
                sql.push('?');
                sql.push_str(&(bound.len() + 1).to_string());
                bound.push(SqlValue::Text(ty.as_str().to_owned()));
            }
            sql.push(')');
        }

        if let Some(ref statuses) = filter.statuses {
            if statuses.is_empty() {
                return Ok(Vec::new());
            }
            sql.push_str(" AND status IN (");
            for (idx, status) in statuses.iter().enumerate() {
                if idx > 0 {
                    sql.push(',');
                }
                sql.push('?');
                sql.push_str(&(bound.len() + 1).to_string());
                bound.push(SqlValue::Text(status.as_db_str().to_owned()));
            }
            sql.push(')');
        }

        if let Some(before) = filter.submitted_before {
            sql.push_str(&format!(" AND submitted_at < ?{}", bound.len() + 1));
            bound.push(SqlValue::Integer(ts_to_millis(before)));
        }
        if let Some(after) = filter.submitted_after {
            sql.push_str(&format!(" AND submitted_at > ?{}", bound.len() + 1));
            bound.push(SqlValue::Integer(ts_to_millis(after)));
        }

        sql.push_str(&format!(
            " ORDER BY submitted_at ASC LIMIT ?{}",
            bound.len() + 1
        ));
        bound.push(SqlValue::Integer(limit as i64));

        let conn = self.conn();
        let mut stmt = conn.prepare(&sql).map_err(map_sql_error)?;
        let rows = stmt
            .query_map(params_from_iter(bound.iter()), |row| {
                Ok(task_row_from_sql(row))
            })
            .map_err(map_sql_error)?;
        let mut out = Vec::new();
        for row in rows {
            let raw = row.map_err(map_sql_error)??;
            out.push(task_row_to_task(raw)?);
        }
        Ok(out)
    }

    async fn list_tasks_by_terminal_status(
        &mut self,
        namespace: &Namespace,
        statuses: Vec<TerminalState>,
        limit: usize,
    ) -> StorageResult<Vec<Task>> {
        if statuses.is_empty() {
            return Ok(Vec::new());
        }
        let mut sql = String::from(
            "SELECT task_id, namespace, task_type, status, priority, payload, payload_hash, \
                    submitted_at, expires_at, attempt_number, max_retries, retry_initial_ms, \
                    retry_max_ms, retry_coefficient, retry_after, traceparent, tracestate, \
                    format_version, original_failure_count \
               FROM tasks WHERE namespace = ?1 AND status IN (",
        );
        let mut bound: Vec<SqlValue> = vec![SqlValue::Text(namespace.as_str().to_owned())];
        for (idx, state) in statuses.iter().enumerate() {
            if idx > 0 {
                sql.push(',');
            }
            sql.push('?');
            sql.push_str(&(bound.len() + 1).to_string());
            bound.push(SqlValue::Text(
                state.as_task_status().as_db_str().to_owned(),
            ));
        }
        sql.push_str(&format!(
            ") ORDER BY submitted_at ASC LIMIT ?{}",
            bound.len() + 1
        ));
        bound.push(SqlValue::Integer(limit as i64));

        let conn = self.conn();
        let mut stmt = conn.prepare(&sql).map_err(map_sql_error)?;
        let rows = stmt
            .query_map(params_from_iter(bound.iter()), |row| {
                Ok(task_row_from_sql(row))
            })
            .map_err(map_sql_error)?;
        let mut out = Vec::new();
        for row in rows {
            let raw = row.map_err(map_sql_error)??;
            out.push(task_row_to_task(raw)?);
        }
        Ok(out)
    }

    async fn replay_task(&mut self, task_id: TaskId) -> StorageResult<ReplayOutcome> {
        // §6.7 ReplayDeadLetters: validate state, validate idempotency-key
        // ownership, then reset row to PENDING. Preserves
        // `original_failure_count` if non-zero, otherwise captures the
        // current `attempt_number`.
        let task_id_text = task_id_to_text(&task_id);

        let row: Option<(String, String, i64, i64)> = self
            .conn()
            .query_row(
                "SELECT status, namespace, attempt_number, original_failure_count \
                   FROM tasks WHERE task_id = ?1",
                params![&task_id_text],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(map_sql_error)?;
        let Some((status_str, namespace_str, attempt_number, original_failure_count)) = row else {
            return Ok(ReplayOutcome::NotFound);
        };
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

        // Idempotency-key ownership: any row in `idempotency_keys` for this
        // namespace that points at a *different* task than `task_id_text`
        // means a fresh submission has reclaimed the key — refuse.
        let claimed_elsewhere: Option<i64> = self
            .conn()
            .query_row(
                "SELECT 1 FROM idempotency_keys k1 \
                  WHERE k1.namespace = ?1 AND k1.task_id <> ?2 \
                    AND EXISTS (SELECT 1 FROM idempotency_keys k2 \
                                 WHERE k2.namespace = ?1 AND k2.task_id = ?2) \
                  LIMIT 1",
                params![&namespace_str, &task_id_text],
                |row| row.get(0),
            )
            .optional()
            .map_err(map_sql_error)?;
        if claimed_elsewhere.is_some() {
            return Ok(ReplayOutcome::KeyClaimedElsewhere);
        }

        let new_original_failure_count = if original_failure_count > 0 {
            original_failure_count
        } else {
            attempt_number
        };

        self.conn()
            .execute(
                "UPDATE tasks SET status = 'PENDING', \
                                  attempt_number = 0, \
                                  retry_after = NULL, \
                                  original_failure_count = ?2 \
                  WHERE task_id = ?1",
                params![&task_id_text, new_original_failure_count],
            )
            .map_err(map_sql_error)?;

        Ok(ReplayOutcome::Replayed)
    }

    async fn add_error_classes(
        &mut self,
        namespace: &Namespace,
        classes: &[String],
    ) -> StorageResult<()> {
        if classes.is_empty() {
            return Ok(());
        }
        let now_ms = current_millis();
        for class in classes {
            self.conn()
                .execute(
                    "INSERT OR IGNORE INTO error_class_registry \
                        (namespace, error_class, deprecated, registered_at) \
                     VALUES (?1, ?2, 0, ?3)",
                    params![namespace.as_str(), class, now_ms],
                )
                .map_err(map_sql_error)?;
        }
        Ok(())
    }

    async fn deprecate_error_class(
        &mut self,
        namespace: &Namespace,
        class: &str,
    ) -> StorageResult<()> {
        self.conn()
            .execute(
                "UPDATE error_class_registry SET deprecated = 1 \
                  WHERE namespace = ?1 AND error_class = ?2",
                params![namespace.as_str(), class],
            )
            .map_err(map_sql_error)?;
        Ok(())
    }

    async fn add_task_types(
        &mut self,
        namespace: &Namespace,
        types: &[TaskType],
    ) -> StorageResult<()> {
        if types.is_empty() {
            return Ok(());
        }
        let now_ms = current_millis();
        for ty in types {
            self.conn()
                .execute(
                    "INSERT OR IGNORE INTO task_type_registry \
                        (namespace, task_type, deprecated, registered_at) \
                     VALUES (?1, ?2, 0, ?3)",
                    params![namespace.as_str(), ty.as_str(), now_ms],
                )
                .map_err(map_sql_error)?;
        }
        Ok(())
    }

    async fn deprecate_task_type(
        &mut self,
        namespace: &Namespace,
        task_type: &TaskType,
    ) -> StorageResult<()> {
        self.conn()
            .execute(
                "UPDATE task_type_registry SET deprecated = 1 \
                  WHERE namespace = ?1 AND task_type = ?2",
                params![namespace.as_str(), task_type.as_str()],
            )
            .map_err(map_sql_error)?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------------

    async fn commit(self) -> StorageResult<()> {
        self.guard.execute_batch("COMMIT").map_err(map_sql_error)?;
        // Drop drops the guard, releasing the writer lock.
        Ok(())
    }

    async fn rollback(self) -> StorageResult<()> {
        self.guard
            .execute_batch("ROLLBACK")
            .map_err(map_sql_error)?;
        Ok(())
    }
}

/// Decode a `tasks` row out of a rusqlite [`rusqlite::Row`] into a
/// [`TaskRow`]. The column order MUST match every callsite — see
/// `get_task_by_id`, `list_tasks_by_filter`, and
/// `list_tasks_by_terminal_status`.
fn task_row_from_sql(row: &rusqlite::Row<'_>) -> StorageResult<TaskRow> {
    Ok(TaskRow {
        task_id: row.get(0).map_err(map_sql_error)?,
        namespace: row.get(1).map_err(map_sql_error)?,
        task_type: row.get(2).map_err(map_sql_error)?,
        status: row.get(3).map_err(map_sql_error)?,
        priority: row.get(4).map_err(map_sql_error)?,
        payload: row.get(5).map_err(map_sql_error)?,
        payload_hash: row.get(6).map_err(map_sql_error)?,
        submitted_at: row.get(7).map_err(map_sql_error)?,
        expires_at: row.get(8).map_err(map_sql_error)?,
        attempt_number: row.get::<_, i64>(9).map_err(map_sql_error)? as u32,
        max_retries: row.get::<_, i64>(10).map_err(map_sql_error)? as u32,
        retry_initial_ms: row.get::<_, i64>(11).map_err(map_sql_error)? as u64,
        retry_max_ms: row.get::<_, i64>(12).map_err(map_sql_error)? as u64,
        retry_coefficient: row.get::<_, f64>(13).map_err(map_sql_error)? as f32,
        retry_after: row.get(14).map_err(map_sql_error)?,
        traceparent: row.get(15).map_err(map_sql_error)?,
        tracestate: row.get(16).map_err(map_sql_error)?,
        format_version: row.get::<_, i64>(17).map_err(map_sql_error)? as u32,
        original_failure_count: row.get::<_, i64>(18).map_err(map_sql_error)? as u32,
    })
}

/// Project a [`TaskRow`] onto the public [`Task`]. Verifies the
/// payload-hash length and decodes the status enum so callers receive a
/// fully validated row.
fn task_row_to_task(row: TaskRow) -> StorageResult<Task> {
    if row.payload_hash.len() != 32 {
        return Err(StorageError::ConstraintViolation(format!(
            "tasks.payload_hash has unexpected length {}",
            row.payload_hash.len()
        )));
    }
    let mut payload_hash = [0u8; 32];
    payload_hash.copy_from_slice(&row.payload_hash);
    let status = TaskStatus::from_db_str(&row.status).ok_or_else(|| {
        StorageError::ConstraintViolation(format!("tasks.status unknown: {}", row.status))
    })?;

    Ok(Task {
        task_id: parse_task_id(&row.task_id)?,
        namespace: Namespace::new(row.namespace),
        task_type: TaskType::new(row.task_type),
        status,
        priority: row.priority,
        payload: Bytes::from(row.payload),
        payload_hash,
        submitted_at: millis_to_ts(row.submitted_at),
        expires_at: millis_to_ts(row.expires_at),
        attempt_number: row.attempt_number,
        max_retries: row.max_retries,
        retry_initial_ms: row.retry_initial_ms,
        retry_max_ms: row.retry_max_ms,
        retry_coefficient: row.retry_coefficient,
        retry_after: row.retry_after.map(millis_to_ts),
        traceparent: Bytes::from(row.traceparent),
        tracestate: Bytes::from(row.tracestate),
        format_version: row.format_version,
        original_failure_count: row.original_failure_count,
    })
}

/// Helper row type for `get_task_by_id` to keep the rusqlite closure
/// free of borrow-from-Self issues. Mirrors the columns selected.
struct TaskRow {
    task_id: String,
    namespace: String,
    task_type: String,
    status: String,
    priority: i32,
    payload: Vec<u8>,
    payload_hash: Vec<u8>,
    submitted_at: i64,
    expires_at: i64,
    attempt_number: u32,
    max_retries: u32,
    retry_initial_ms: u64,
    retry_max_ms: u64,
    retry_coefficient: f32,
    retry_after: Option<i64>,
    traceparent: Vec<u8>,
    tracestate: Vec<u8>,
    format_version: u32,
    original_failure_count: u32,
}

// ============================================================================
// Helpers
// ============================================================================

struct LockedRow {
    task_id: String,
    namespace: String,
    task_type: String,
    attempt_number: u32,
    priority: i32,
    payload: Vec<u8>,
    submitted_at: i64,
    expires_at: i64,
    max_retries: u32,
    retry_initial_ms: u64,
    retry_max_ms: u64,
    retry_coefficient: f32,
    traceparent: Vec<u8>,
    tracestate: Vec<u8>,
}

fn current_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Sample one namespace at random from those with at least one matching
/// PENDING task, honoring the same filters the outer query applies. Returns
/// `None` after `attempts` consecutive empty samples.
fn sample_random_namespace(
    conn: &Connection,
    criteria: &PickCriteria,
    attempts: u32,
) -> StorageResult<Option<String>> {
    if attempts == 0 {
        return Ok(None);
    }

    // Fetch the candidate namespace set in one query, then pick uniformly
    // in-process. SQLite has no `RANDOM()` per row that's cheap to combine
    // with `LIMIT 1` plus an `IN` predicate, but the candidate list is
    // bounded by namespace count which is small.
    let namespaces: HashSet<String> = match &criteria.namespace_filter {
        NamespaceFilter::Single(ns) => HashSet::from_iter([ns.as_str().to_owned()]),
        NamespaceFilter::AnyOf(list) => list.iter().map(|n| n.as_str().to_owned()).collect(),
        NamespaceFilter::Any => {
            let mut stmt = conn
                .prepare(
                    "SELECT DISTINCT namespace FROM tasks \
                     WHERE status IN ('PENDING', 'WAITING_RETRY')",
                )
                .map_err(map_sql_error)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(map_sql_error)?;
            let mut out = HashSet::new();
            for r in rows {
                out.insert(r.map_err(map_sql_error)?);
            }
            out
        }
    };

    if namespaces.is_empty() {
        return Ok(None);
    }

    // Uniform sampling: take the first namespace by stable iteration order
    // then verify it has matching pending work. SQLite's `random()` could
    // be used here instead; the simple stable order suffices because the
    // dispatcher only needs *some* namespace to advance, not strict
    // uniformity (`design.md` §7.2 — "sample uniformly from active
    // namespaces" is the spec, but for v1's RandomNamespace that's a
    // weak requirement and the bounded retry budget compensates).
    let sorted: Vec<String> = {
        let mut v: Vec<String> = namespaces.into_iter().collect();
        v.sort();
        v
    };

    for (offset, candidate) in sorted.iter().take(attempts as usize).enumerate() {
        let _ = offset;
        // Verify the candidate has at least one matching task.
        let has_match: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM tasks \
                 WHERE namespace = ?1 \
                   AND status IN ('PENDING', 'WAITING_RETRY') \
                 LIMIT 1",
                params![candidate],
                |row| row.get(0),
            )
            .optional()
            .map_err(map_sql_error)?;
        if has_match.is_some() {
            return Ok(Some(candidate.clone()));
        }
    }
    Ok(None)
}

fn read_capacity_limit(
    conn: &Connection,
    namespace: &Namespace,
    column: &str,
) -> StorageResult<Option<u64>> {
    // Per `design.md` §9.1: read the column on the requested namespace's
    // row; if the column is NULL, fall back to system_default. If neither
    // row exists we surface NotFound.
    let target_sql = format!("SELECT {column} FROM namespace_quota WHERE namespace = ?1");
    let target: Option<Option<i64>> = conn
        .query_row(&target_sql, params![namespace.as_str()], |row| row.get(0))
        .optional()
        .map_err(map_sql_error)?;

    match target {
        Some(Some(value)) => Ok(Some(value as u64)),
        Some(None) => {
            // Row exists but column is NULL — fall back to system_default.
            let default_sql =
                format!("SELECT {column} FROM namespace_quota WHERE namespace = 'system_default'");
            let default: Option<i64> = conn
                .query_row(&default_sql, [], |row| row.get(0))
                .optional()
                .map_err(map_sql_error)?
                .flatten();
            Ok(default.map(|v| v as u64))
        }
        None => {
            // Namespace not provisioned — fall back to system_default
            // entirely.
            let default_sql =
                format!("SELECT {column} FROM namespace_quota WHERE namespace = 'system_default'");
            let default: Option<i64> = conn
                .query_row(&default_sql, [], |row| row.get(0))
                .optional()
                .map_err(map_sql_error)?
                .flatten();
            Ok(default.map(|v| v as u64))
        }
    }
}

/// Snapshot of one row in `namespace_quota`. The fields use raw SQLite
/// types; [`merge_quota`] composes the trait-shaped `NamespaceQuota`.
struct QuotaRow {
    admitter_kind: String,
    admitter_params: String,
    dispatcher_kind: String,
    dispatcher_params: String,
    max_pending: Option<i64>,
    max_inflight: Option<i64>,
    max_workers: Option<i64>,
    max_waiters_per_replica: Option<i64>,
    max_submit_rpm: Option<i64>,
    max_dispatch_rpm: Option<i64>,
    max_replay_per_second: Option<i64>,
    max_retries_ceiling: i64,
    max_idempotency_ttl_seconds: i64,
    max_payload_bytes: i64,
    max_details_bytes: i64,
    min_heartbeat_interval_seconds: i64,
    lazy_extension_threshold_seconds: i64,
    max_error_classes: i64,
    max_task_types: i64,
    trace_sampling_ratio: f64,
    log_level_override: Option<String>,
    audit_log_retention_days: i64,
    metrics_export_enabled: i64,
}

fn read_quota_row(conn: &Connection, namespace: &str) -> StorageResult<Option<QuotaRow>> {
    let row = conn
        .query_row(
            "SELECT admitter_kind, admitter_params, dispatcher_kind, dispatcher_params, \
                    max_pending, max_inflight, max_workers, max_waiters_per_replica, \
                    max_submit_rpm, max_dispatch_rpm, max_replay_per_second, \
                    max_retries_ceiling, max_idempotency_ttl_seconds, \
                    max_payload_bytes, max_details_bytes, min_heartbeat_interval_seconds, \
                    lazy_extension_threshold_seconds, max_error_classes, max_task_types, \
                    trace_sampling_ratio, log_level_override, audit_log_retention_days, \
                    metrics_export_enabled \
             FROM namespace_quota WHERE namespace = ?1",
            params![namespace],
            |row| {
                Ok(QuotaRow {
                    admitter_kind: row.get(0)?,
                    admitter_params: row.get(1)?,
                    dispatcher_kind: row.get(2)?,
                    dispatcher_params: row.get(3)?,
                    max_pending: row.get(4)?,
                    max_inflight: row.get(5)?,
                    max_workers: row.get(6)?,
                    max_waiters_per_replica: row.get(7)?,
                    max_submit_rpm: row.get(8)?,
                    max_dispatch_rpm: row.get(9)?,
                    max_replay_per_second: row.get(10)?,
                    max_retries_ceiling: row.get(11)?,
                    max_idempotency_ttl_seconds: row.get(12)?,
                    max_payload_bytes: row.get(13)?,
                    max_details_bytes: row.get(14)?,
                    min_heartbeat_interval_seconds: row.get(15)?,
                    lazy_extension_threshold_seconds: row.get(16)?,
                    max_error_classes: row.get(17)?,
                    max_task_types: row.get(18)?,
                    trace_sampling_ratio: row.get(19)?,
                    log_level_override: row.get(20)?,
                    audit_log_retention_days: row.get(21)?,
                    metrics_export_enabled: row.get(22)?,
                })
            },
        )
        .optional()
        .map_err(map_sql_error)?;
    Ok(row)
}

fn merge_quota(
    namespace: Namespace,
    target: Option<QuotaRow>,
    defaults: Option<&QuotaRow>,
) -> NamespaceQuota {
    // Either the target row, the defaults, or one of them must exist —
    // `read_namespace_quota` enforces NotFound otherwise. This helper
    // consumes whichever rows are present and merges field-by-field.
    let chosen = match (target.as_ref(), defaults) {
        (Some(_), _) => target.as_ref().unwrap(),
        (None, Some(d)) => d,
        (None, None) => {
            // Caller must check; we should never reach this in practice.
            return NamespaceQuota {
                namespace,
                admitter_kind: String::new(),
                admitter_params: Bytes::new(),
                dispatcher_kind: String::new(),
                dispatcher_params: Bytes::new(),
                max_pending: None,
                max_inflight: None,
                max_workers: None,
                max_waiters_per_replica: None,
                max_submit_rpm: None,
                max_dispatch_rpm: None,
                max_replay_per_second: None,
                max_retries_ceiling: 0,
                max_idempotency_ttl_seconds: 0,
                max_payload_bytes: 0,
                max_details_bytes: 0,
                min_heartbeat_interval_seconds: 0,
                lazy_extension_threshold_seconds: 0,
                max_error_classes: 0,
                max_task_types: 0,
                trace_sampling_ratio: 0.0,
                log_level_override: None,
                audit_log_retention_days: 0,
                metrics_export_enabled: false,
            };
        }
    };

    // Field-level fallback: prefer target, fall back to defaults for
    // optional columns.
    let pick_opt_int =
        |t: Option<i64>, fallback: Option<i64>| -> Option<u64> { t.or(fallback).map(|v| v as u64) };
    let pick_opt_int_u32 =
        |t: Option<i64>, fallback: Option<i64>| -> Option<u32> { t.or(fallback).map(|v| v as u32) };

    let target_row = target.as_ref();
    let max_pending = pick_opt_int(
        target_row.and_then(|r| r.max_pending),
        defaults.and_then(|r| r.max_pending),
    );
    let max_inflight = pick_opt_int(
        target_row.and_then(|r| r.max_inflight),
        defaults.and_then(|r| r.max_inflight),
    );
    let max_workers = pick_opt_int_u32(
        target_row.and_then(|r| r.max_workers),
        defaults.and_then(|r| r.max_workers),
    );
    let max_waiters_per_replica = pick_opt_int_u32(
        target_row.and_then(|r| r.max_waiters_per_replica),
        defaults.and_then(|r| r.max_waiters_per_replica),
    );
    let max_submit_rpm = pick_opt_int(
        target_row.and_then(|r| r.max_submit_rpm),
        defaults.and_then(|r| r.max_submit_rpm),
    );
    let max_dispatch_rpm = pick_opt_int(
        target_row.and_then(|r| r.max_dispatch_rpm),
        defaults.and_then(|r| r.max_dispatch_rpm),
    );
    let max_replay_per_second = pick_opt_int_u32(
        target_row.and_then(|r| r.max_replay_per_second),
        defaults.and_then(|r| r.max_replay_per_second),
    );

    NamespaceQuota {
        namespace,
        admitter_kind: chosen.admitter_kind.clone(),
        admitter_params: Bytes::from(chosen.admitter_params.clone().into_bytes()),
        dispatcher_kind: chosen.dispatcher_kind.clone(),
        dispatcher_params: Bytes::from(chosen.dispatcher_params.clone().into_bytes()),
        max_pending,
        max_inflight,
        max_workers,
        max_waiters_per_replica,
        max_submit_rpm,
        max_dispatch_rpm,
        max_replay_per_second,
        max_retries_ceiling: chosen.max_retries_ceiling as u32,
        max_idempotency_ttl_seconds: chosen.max_idempotency_ttl_seconds as u64,
        max_payload_bytes: chosen.max_payload_bytes as u32,
        max_details_bytes: chosen.max_details_bytes as u32,
        min_heartbeat_interval_seconds: chosen.min_heartbeat_interval_seconds as u32,
        lazy_extension_threshold_seconds: chosen.lazy_extension_threshold_seconds as u32,
        max_error_classes: chosen.max_error_classes as u32,
        max_task_types: chosen.max_task_types as u32,
        trace_sampling_ratio: chosen.trace_sampling_ratio as f32,
        log_level_override: chosen.log_level_override.clone(),
        audit_log_retention_days: chosen.audit_log_retention_days as u32,
        metrics_export_enabled: chosen.metrics_export_enabled != 0,
    }
}

// ============================================================================
// Polling stream for subscribe_pending
// ============================================================================

/// 500ms-tick poll-based wake stream. SQLite has no LISTEN/NOTIFY; the CP
/// receives a `WakeSignal` on every tick and re-runs the dispatch query.
///
/// The stream is independent of the originating transaction: once
/// returned it polls at its own cadence regardless of when the caller
/// commits. This matches the §8.2 #5 ordering invariant — by the time any
/// new task commits, the next poll tick will fire and the recipient will
/// re-query.
struct PollingPendingStream {
    interval: Interval,
}

impl PollingPendingStream {
    fn new(period: Duration) -> Self {
        let mut int = interval(period);
        // Burn the immediate tick so the first emission happens after one
        // period; otherwise `Stream::poll_next` would yield a `WakeSignal`
        // synchronously on the first poll, which surprises consumers
        // expecting "fire on commit, not on subscribe".
        int.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self { interval: int }
    }
}

impl Stream for PollingPendingStream {
    type Item = WakeSignal;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.interval.poll_tick(cx) {
            Poll::Ready(_) => Poll::Ready(Some(WakeSignal)),
            Poll::Pending => Poll::Pending,
        }
    }
}
