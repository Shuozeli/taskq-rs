-- Initial schema for the taskq-rs SQLite backend.
--
-- Same logical model as Postgres (see design.md §4) translated into SQLite
-- types. Choices documented inline:
--
--   uuid         -> TEXT  (human-readable hyphenated UUID; debuggable in
--                          sqlite3 CLI, low storage premium at this scale)
--   bigint       -> INTEGER (SQLite type-affinity hint)
--   timestamptz  -> INTEGER (Unix epoch milliseconds — matches the
--                            `Timestamp` representation in taskq-storage)
--   jsonb        -> TEXT (free-form JSON; SQLite's JSON1 functions can
--                         query it if needed, but storage is plain text)
--   enum         -> TEXT with CHECK constraint listing the variants
--
-- Per design.md §8.3 SQLite is single-process embedded/dev only. No
-- partitioning of `idempotency_keys` (DELETE WHERE expires_at < ? is
-- acceptable at this scope). No BRIN equivalent on heartbeats — SQLite has
-- only B-tree indexes; the index churn is acceptable at single-process
-- scale.

-- ============================================================================
-- tasks
-- ============================================================================

CREATE TABLE tasks (
    task_id            TEXT    PRIMARY KEY NOT NULL,
    namespace          TEXT    NOT NULL,
    task_type          TEXT    NOT NULL,
    status             TEXT    NOT NULL CHECK (status IN (
                          'PENDING',
                          'DISPATCHED',
                          'COMPLETED',
                          'FAILED_NONRETRYABLE',
                          'FAILED_EXHAUSTED',
                          'EXPIRED',
                          'WAITING_RETRY',
                          'CANCELLED'
                       )),
    priority           INTEGER NOT NULL,
    payload            BLOB    NOT NULL,
    payload_hash       BLOB    NOT NULL,
    submitted_at       INTEGER NOT NULL,
    expires_at         INTEGER NOT NULL,
    attempt_number     INTEGER NOT NULL DEFAULT 0,
    max_retries        INTEGER NOT NULL,
    retry_initial_ms   INTEGER NOT NULL,
    retry_max_ms       INTEGER NOT NULL,
    retry_coefficient  REAL    NOT NULL,
    retry_after        INTEGER,
    traceparent        BLOB    NOT NULL,
    tracestate         BLOB    NOT NULL,
    format_version     INTEGER NOT NULL,
    original_failure_count INTEGER NOT NULL DEFAULT 0
);

-- Composite index supporting the dispatch query
-- (`design.md` §8.3 Postgres mirror).
CREATE INDEX idx_tasks_pending_priority
    ON tasks (namespace, status, priority DESC, submitted_at ASC);

CREATE INDEX idx_tasks_status
    ON tasks (status);

-- ============================================================================
-- task_runtime (lease)
-- ============================================================================

CREATE TABLE task_runtime (
    task_id           TEXT    NOT NULL,
    attempt_number    INTEGER NOT NULL,
    worker_id         TEXT    NOT NULL,
    acquired_at       INTEGER NOT NULL,
    timeout_at        INTEGER NOT NULL,
    last_extended_at  INTEGER NOT NULL,
    PRIMARY KEY (task_id, attempt_number),
    FOREIGN KEY (task_id) REFERENCES tasks (task_id) ON DELETE CASCADE
);

CREATE INDEX idx_runtime_timeout_at
    ON task_runtime (timeout_at);
CREATE INDEX idx_runtime_worker
    ON task_runtime (worker_id);

-- ============================================================================
-- worker_heartbeats
-- ============================================================================
--
-- Carve-out per design.md §1.1: written at READ COMMITTED. SQLite does not
-- have ANSI isolation levels, but its single-writer model + WAL journaling
-- gives a similar effect — heartbeat writes serialize cheaply.
--
-- B-tree index on `last_heartbeat_at` (no BRIN in SQLite). Trade-off
-- documented in the README: more index churn on heartbeat updates, which
-- is acceptable at single-process / dev scale.

CREATE TABLE worker_heartbeats (
    worker_id          TEXT    PRIMARY KEY NOT NULL,
    namespace          TEXT    NOT NULL,
    last_heartbeat_at  INTEGER NOT NULL,
    declared_dead_at   INTEGER
);

CREATE INDEX idx_heartbeats_last_heartbeat_at
    ON worker_heartbeats (last_heartbeat_at)
    WHERE declared_dead_at IS NULL;

-- ============================================================================
-- task_results
-- ============================================================================

CREATE TABLE task_results (
    task_id          TEXT    NOT NULL,
    attempt_number   INTEGER NOT NULL,
    outcome          TEXT    NOT NULL CHECK (outcome IN (
                       'SUCCESS',
                       'RETRYABLE_FAIL',
                       'NONRETRYABLE_FAIL',
                       'CANCELLED',
                       'EXPIRED'
                     )),
    result_payload   BLOB,
    error_class      TEXT,
    error_message    TEXT,
    error_details    BLOB,
    recorded_at      INTEGER NOT NULL,
    PRIMARY KEY (task_id, attempt_number),
    FOREIGN KEY (task_id) REFERENCES tasks (task_id) ON DELETE CASCADE
);

-- ============================================================================
-- idempotency_keys
-- ============================================================================
--
-- No partitioning per design.md §8.3 (SQLite has no declarative
-- partitioning, single-process scope makes a regular `DELETE` acceptable).

CREATE TABLE idempotency_keys (
    namespace      TEXT    NOT NULL,
    key            TEXT    NOT NULL,
    task_id        TEXT    NOT NULL,
    payload_hash   BLOB    NOT NULL,
    expires_at     INTEGER NOT NULL,
    PRIMARY KEY (namespace, key)
);

CREATE INDEX idx_idempotency_expires_at
    ON idempotency_keys (expires_at);

-- ============================================================================
-- namespace_quota
-- ============================================================================

CREATE TABLE namespace_quota (
    namespace                          TEXT    PRIMARY KEY NOT NULL,
    admitter_kind                      TEXT    NOT NULL CHECK (admitter_kind IN (
                                          'Always', 'MaxPending', 'CoDel'
                                       )),
    admitter_params                    TEXT    NOT NULL,
    dispatcher_kind                    TEXT    NOT NULL CHECK (dispatcher_kind IN (
                                          'PriorityFifo', 'AgePromoted', 'RandomNamespace'
                                       )),
    dispatcher_params                  TEXT    NOT NULL,

    -- Capacity (read inline transactionally per design.md §9.1)
    max_pending                        INTEGER,
    max_inflight                       INTEGER,
    max_workers                        INTEGER,
    max_waiters_per_replica            INTEGER,

    -- Rate (eventually consistent within cache TTL per design.md §1.1)
    max_submit_rpm                     INTEGER,
    max_dispatch_rpm                   INTEGER,
    max_replay_per_second              INTEGER,

    -- Per-task ceilings
    max_retries_ceiling                INTEGER NOT NULL,
    max_idempotency_ttl_seconds        INTEGER NOT NULL,
    max_payload_bytes                  INTEGER NOT NULL,
    max_details_bytes                  INTEGER NOT NULL,
    min_heartbeat_interval_seconds     INTEGER NOT NULL,

    -- Lease/heartbeat invariant (design.md §9.1)
    lazy_extension_threshold_seconds   INTEGER NOT NULL,

    -- Cardinality budget
    max_error_classes                  INTEGER NOT NULL,
    max_task_types                     INTEGER NOT NULL,

    -- Observability
    trace_sampling_ratio               REAL    NOT NULL,
    log_level_override                 TEXT,
    audit_log_retention_days           INTEGER NOT NULL,
    metrics_export_enabled             INTEGER NOT NULL CHECK (metrics_export_enabled IN (0, 1))
);

-- Seed the system_default row per design.md §9.1: the 'system_default'
-- namespace is reserved as the inheritance fallback. Defaults match the
-- design's documented values.
INSERT INTO namespace_quota (
    namespace,
    admitter_kind, admitter_params,
    dispatcher_kind, dispatcher_params,
    max_pending, max_inflight, max_workers, max_waiters_per_replica,
    max_submit_rpm, max_dispatch_rpm, max_replay_per_second,
    max_retries_ceiling,
    max_idempotency_ttl_seconds,
    max_payload_bytes,
    max_details_bytes,
    min_heartbeat_interval_seconds,
    lazy_extension_threshold_seconds,
    max_error_classes,
    max_task_types,
    trace_sampling_ratio,
    log_level_override,
    audit_log_retention_days,
    metrics_export_enabled
) VALUES (
    'system_default',
    'Always', '{}',
    'PriorityFifo', '{}',
    NULL, NULL, NULL, 5000,
    NULL, NULL, NULL,
    10,
    86400,           -- 24h dedup TTL ceiling per design.md §13
    10485760,        -- 10 MB
    65536,           -- 64 KB
    10,              -- 10s minimum heartbeat interval
    60,              -- 60s lazy-extension threshold (>= 2 * min_heartbeat_interval)
    64,
    32,
    1.0,
    NULL,
    90,
    1
);

-- ============================================================================
-- error_class_registry
-- ============================================================================

CREATE TABLE error_class_registry (
    namespace      TEXT    NOT NULL,
    error_class    TEXT    NOT NULL,
    deprecated     INTEGER NOT NULL DEFAULT 0 CHECK (deprecated IN (0, 1)),
    registered_at  INTEGER NOT NULL,
    PRIMARY KEY (namespace, error_class)
);

-- ============================================================================
-- audit_log
-- ============================================================================

CREATE TABLE audit_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp         INTEGER NOT NULL,
    actor             TEXT    NOT NULL,
    rpc               TEXT    NOT NULL,
    namespace         TEXT,
    request_summary   TEXT    NOT NULL,
    result            TEXT    NOT NULL,
    request_hash      BLOB    NOT NULL
);

CREATE INDEX idx_audit_timestamp ON audit_log (timestamp);
CREATE INDEX idx_audit_namespace_rpc ON audit_log (namespace, rpc);

-- ============================================================================
-- task_type_retry_config
-- ============================================================================

CREATE TABLE task_type_retry_config (
    namespace          TEXT    NOT NULL,
    task_type          TEXT    NOT NULL,
    initial_ms         INTEGER NOT NULL,
    max_ms             INTEGER NOT NULL,
    coefficient        REAL    NOT NULL,
    max_retries        INTEGER NOT NULL,
    PRIMARY KEY (namespace, task_type)
);

-- ============================================================================
-- taskq_meta
-- ============================================================================

CREATE TABLE taskq_meta (
    -- Single-row table; primary key is a fixed sentinel.
    id                 INTEGER PRIMARY KEY CHECK (id = 1),
    schema_version     INTEGER NOT NULL,
    format_version     INTEGER NOT NULL,
    last_migrated_at   INTEGER NOT NULL
);

INSERT INTO taskq_meta (id, schema_version, format_version, last_migrated_at)
VALUES (1, 1, 1, 0);
