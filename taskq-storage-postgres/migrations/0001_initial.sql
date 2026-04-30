-- taskq-rs initial Postgres schema (Phase 3).
--
-- Implements the logical data model from design.md §4 plus the Postgres-
-- specific notes from design.md §8.3:
--   * `idempotency_keys` is range-partitioned by `expires_at` (daily
--     granularity) so cleanup is `DROP PARTITION` and partition count stays
--     bounded (hard 90-day TTL ceiling enforced upstream at SetNamespaceQuota).
--   * `worker_heartbeats` carries a BRIN index on `last_heartbeat_at` for
--     Reaper B's "scan everything older than T" pattern; cheap to maintain
--     on append-mostly time-correlated data.
--   * `tasks` carries a composite index on
--     `(namespace, status, priority DESC, submitted_at ASC)` for dispatch.
--
-- Per code-standards: all DB ops in transactions; no emoji; typed columns
-- only (no free-form JSON outside `audit_log.request_summary`).

-- ----------------------------------------------------------------------------
-- 1. taskq_meta single-row metadata table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taskq_meta (
    only_row        boolean PRIMARY KEY DEFAULT TRUE,
    schema_version  integer NOT NULL,
    format_version  integer NOT NULL,
    last_migrated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT taskq_meta_singleton CHECK (only_row = TRUE)
);

INSERT INTO taskq_meta (only_row, schema_version, format_version)
VALUES (TRUE, 1, 1)
ON CONFLICT (only_row) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 2. Task status enum and tasks table
-- ----------------------------------------------------------------------------
-- Both `CREATE TYPE` blocks below are schema-local: the `pg_type` lookup
-- restricts to `pg_namespace = current_schema()` so we don't trip on a
-- name collision in a sibling schema (matters for the smoke-test
-- per-schema isolation pattern).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type t
          JOIN pg_namespace n ON n.oid = t.typnamespace
         WHERE t.typname = 'taskq_task_status'
           AND n.nspname = ANY (current_schemas(false))
    ) THEN
        CREATE TYPE taskq_task_status AS ENUM (
            'PENDING',
            'DISPATCHED',
            'COMPLETED',
            'FAILED_NONRETRYABLE',
            'FAILED_EXHAUSTED',
            'EXPIRED',
            'WAITING_RETRY',
            'CANCELLED'
        );
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS tasks (
    task_id                 uuid PRIMARY KEY,
    namespace               text NOT NULL,
    task_type               text NOT NULL,
    status                  taskq_task_status NOT NULL,
    priority                integer NOT NULL,
    payload                 bytea NOT NULL,
    payload_hash            bytea NOT NULL,
    submitted_at            timestamptz NOT NULL,
    expires_at              timestamptz NOT NULL,
    attempt_number          integer NOT NULL DEFAULT 0,
    max_retries             integer NOT NULL,
    retry_initial_ms        bigint NOT NULL,
    retry_max_ms            bigint NOT NULL,
    retry_coefficient       real NOT NULL,
    retry_after             timestamptz,
    traceparent             bytea NOT NULL,
    tracestate              bytea NOT NULL,
    format_version          integer NOT NULL,
    original_failure_count  integer NOT NULL DEFAULT 0,
    CONSTRAINT tasks_payload_hash_len CHECK (octet_length(payload_hash) = 32),
    CONSTRAINT tasks_attempt_nonneg CHECK (attempt_number >= 0)
);

CREATE INDEX IF NOT EXISTS tasks_dispatch_idx
    ON tasks (namespace, status, priority DESC, submitted_at ASC);

CREATE INDEX IF NOT EXISTS tasks_namespace_idx
    ON tasks (namespace);

-- ----------------------------------------------------------------------------
-- 3. task_runtime (lease) table
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS task_runtime (
    task_id            uuid NOT NULL,
    attempt_number     integer NOT NULL,
    worker_id          uuid NOT NULL,
    acquired_at        timestamptz NOT NULL,
    timeout_at         timestamptz NOT NULL,
    last_extended_at   timestamptz NOT NULL,
    PRIMARY KEY (task_id, attempt_number),
    FOREIGN KEY (task_id) REFERENCES tasks (task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS task_runtime_timeout_idx
    ON task_runtime (timeout_at);

CREATE INDEX IF NOT EXISTS task_runtime_worker_idx
    ON task_runtime (worker_id);

-- ----------------------------------------------------------------------------
-- 4. worker_heartbeats (READ COMMITTED carve-out)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_id          uuid PRIMARY KEY,
    namespace          text NOT NULL,
    last_heartbeat_at  timestamptz NOT NULL,
    declared_dead_at   timestamptz
);

-- BRIN index on last_heartbeat_at: cheap to maintain on append-mostly
-- time-correlated data, sufficient for Reaper B's range scans, and does not
-- defeat HOT updates the way a B-tree on the same column would.
CREATE INDEX IF NOT EXISTS worker_heartbeats_last_heartbeat_brin_idx
    ON worker_heartbeats USING BRIN (last_heartbeat_at);

-- ----------------------------------------------------------------------------
-- 5. task_results
-- ----------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type t
          JOIN pg_namespace n ON n.oid = t.typnamespace
         WHERE t.typname = 'taskq_outcome'
           AND n.nspname = ANY (current_schemas(false))
    ) THEN
        CREATE TYPE taskq_outcome AS ENUM (
            'success',
            'retryable_fail',
            'nonretryable_fail',
            'cancelled',
            'expired'
        );
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS task_results (
    task_id          uuid NOT NULL,
    attempt_number   integer NOT NULL,
    outcome          taskq_outcome NOT NULL,
    result_payload   bytea,
    error_class      text,
    error_message    text,
    error_details    bytea,
    recorded_at      timestamptz NOT NULL,
    PRIMARY KEY (task_id, attempt_number),
    FOREIGN KEY (task_id) REFERENCES tasks (task_id) ON DELETE CASCADE
);

-- ----------------------------------------------------------------------------
-- 6. idempotency_keys (range-partitioned by expires_at, daily granularity)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS idempotency_keys (
    namespace      text NOT NULL,
    key            text NOT NULL,
    task_id        uuid NOT NULL,
    payload_hash   bytea NOT NULL,
    expires_at     timestamptz NOT NULL,
    PRIMARY KEY (namespace, key, expires_at),
    CONSTRAINT idempotency_payload_hash_len CHECK (octet_length(payload_hash) = 32)
) PARTITION BY RANGE (expires_at);

CREATE INDEX IF NOT EXISTS idempotency_keys_task_idx
    ON idempotency_keys (task_id);

-- Default catch-all partition keeps INSERTs whose expires_at lands outside
-- the pre-created daily range from failing. Maintenance code (the partition
-- helper below) splits real ranges out of it.
CREATE TABLE IF NOT EXISTS idempotency_keys_default
    PARTITION OF idempotency_keys DEFAULT;

-- Partition helper: idempotently creates daily partitions up to target_date
-- (inclusive). Each partition covers [day, day+1) in UTC.
CREATE OR REPLACE FUNCTION taskq_create_idempotency_partitions_through(target_date date)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    cur_date date;
    end_date date;
    partition_name text;
    next_date date;
    created_count integer := 0;
BEGIN
    cur_date := (CURRENT_DATE - INTERVAL '1 day')::date;
    end_date := target_date;

    WHILE cur_date <= end_date LOOP
        partition_name := format('idempotency_keys_%s', to_char(cur_date, 'YYYYMMDD'));
        next_date := cur_date + INTERVAL '1 day';

        IF NOT EXISTS (
            SELECT 1 FROM pg_class
            WHERE relname = partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF idempotency_keys FOR VALUES FROM (%L) TO (%L)',
                partition_name,
                cur_date::timestamptz,
                next_date::timestamptz
            );
            created_count := created_count + 1;
        END IF;

        cur_date := next_date;
    END LOOP;

    RETURN created_count;
END;
$$;

-- Pre-create partitions through ~95 days from migration time (today + 95).
-- The hard 90-day TTL ceiling at SetNamespaceQuota gives us a 5-day buffer.
SELECT taskq_create_idempotency_partitions_through((CURRENT_DATE + INTERVAL '95 days')::date);

-- ----------------------------------------------------------------------------
-- 7. namespace_quota
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS namespace_quota (
    namespace                          text PRIMARY KEY,
    admitter_kind                      text NOT NULL,
    admitter_params                    bytea NOT NULL,
    dispatcher_kind                    text NOT NULL,
    dispatcher_params                  bytea NOT NULL,

    max_pending                        bigint,
    max_inflight                       bigint,
    max_workers                        integer,
    max_waiters_per_replica            integer,

    max_submit_rpm                     bigint,
    max_dispatch_rpm                   bigint,
    max_replay_per_second              integer,

    max_retries_ceiling                integer NOT NULL,
    max_idempotency_ttl_seconds        bigint NOT NULL,
    max_payload_bytes                  integer NOT NULL,
    max_details_bytes                  integer NOT NULL,
    min_heartbeat_interval_seconds     integer NOT NULL,

    lazy_extension_threshold_seconds   integer NOT NULL,

    max_error_classes                  integer NOT NULL,
    max_task_types                     integer NOT NULL,

    trace_sampling_ratio               real NOT NULL,
    log_level_override                 text,
    audit_log_retention_days           integer NOT NULL,
    metrics_export_enabled             boolean NOT NULL
);

-- Insert system_default per design.md §9.1: single source of fallback
-- inheritance. Per-namespace rows override specific fields; unset fields
-- inherit from this row.
INSERT INTO namespace_quota (
    namespace,
    admitter_kind,
    admitter_params,
    dispatcher_kind,
    dispatcher_params,
    max_pending,
    max_inflight,
    max_workers,
    max_waiters_per_replica,
    max_submit_rpm,
    max_dispatch_rpm,
    max_replay_per_second,
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
    'Always',
    '\x'::bytea,
    'PriorityFifo',
    '\x'::bytea,
    NULL,
    NULL,
    NULL,
    5000,
    NULL,
    NULL,
    100,
    25,
    7776000,           -- 90 days hard ceiling per design.md §8.3
    10485760,          -- 10 MB
    65536,             -- 64 KB
    5,                 -- min_heartbeat_interval_seconds
    30,                -- lazy_extension_threshold_seconds (>= 2 * min_heartbeat)
    64,                -- max_error_classes default
    32,                -- max_task_types default
    1.0,
    NULL,
    90,
    TRUE
)
ON CONFLICT (namespace) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 8. error_class_registry
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS error_class_registry (
    namespace      text NOT NULL,
    error_class    text NOT NULL,
    deprecated     boolean NOT NULL DEFAULT FALSE,
    registered_at  timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (namespace, error_class)
);

-- ----------------------------------------------------------------------------
-- 9. audit_log
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_log (
    id                bigserial PRIMARY KEY,
    timestamp         timestamptz NOT NULL,
    actor             text NOT NULL,
    rpc               text NOT NULL,
    namespace         text,
    request_summary   jsonb NOT NULL,
    result            text NOT NULL,
    request_hash      bytea NOT NULL,
    CONSTRAINT audit_log_request_hash_len CHECK (octet_length(request_hash) = 32)
);

CREATE INDEX IF NOT EXISTS audit_log_namespace_timestamp_idx
    ON audit_log (namespace, timestamp);

-- ----------------------------------------------------------------------------
-- 10. task_type_retry_config
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS task_type_retry_config (
    namespace          text NOT NULL,
    task_type          text NOT NULL,
    initial_ms         bigint NOT NULL,
    max_ms             bigint NOT NULL,
    coefficient        real NOT NULL,
    max_retries        integer NOT NULL,
    PRIMARY KEY (namespace, task_type)
);

-- ----------------------------------------------------------------------------
-- 11. NOTIFY trigger for subscribe_pending
-- ----------------------------------------------------------------------------
-- Per design.md §8.4: any commit that writes a PENDING row in `tasks` raises
-- a per-namespace pg_notify so subscribers wake. Channel name is
-- `taskq_pending_<namespace>`.
CREATE OR REPLACE FUNCTION taskq_notify_pending() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = 'PENDING' THEN
        PERFORM pg_notify('taskq_pending_' || NEW.namespace, NEW.task_type);
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS taskq_tasks_notify_insert ON tasks;
CREATE TRIGGER taskq_tasks_notify_insert
    AFTER INSERT ON tasks
    FOR EACH ROW EXECUTE FUNCTION taskq_notify_pending();

DROP TRIGGER IF EXISTS taskq_tasks_notify_update ON tasks;
CREATE TRIGGER taskq_tasks_notify_update
    AFTER UPDATE OF status ON tasks
    FOR EACH ROW
    WHEN (NEW.status = 'PENDING' AND OLD.status IS DISTINCT FROM NEW.status)
    EXECUTE FUNCTION taskq_notify_pending();
