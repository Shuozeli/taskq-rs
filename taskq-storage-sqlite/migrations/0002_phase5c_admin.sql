-- Phase 5c additions to the SQLite backend.
--
-- Logical schema mirrors taskq-storage-postgres/migrations/0002_phase5c_admin.sql
-- (see that file for rationale). SQLite type mapping per the Phase 4
-- conventions:
--   bool      -> INTEGER NOT NULL DEFAULT 0
--   timestamptz -> INTEGER (Unix epoch milliseconds)
--
-- SQLite's ALTER TABLE ADD COLUMN does not support `IF NOT EXISTS` until
-- 3.35; the migration runner is idempotent on a successful run via
-- `_taskq_migrations`, so we don't need IF NOT EXISTS here.

ALTER TABLE namespace_quota
    ADD COLUMN disabled INTEGER NOT NULL DEFAULT 0
    CHECK (disabled IN (0, 1));

CREATE TABLE IF NOT EXISTS task_type_registry (
    namespace      TEXT    NOT NULL,
    task_type      TEXT    NOT NULL,
    deprecated     INTEGER NOT NULL DEFAULT 0 CHECK (deprecated IN (0, 1)),
    registered_at  INTEGER NOT NULL,
    PRIMARY KEY (namespace, task_type)
);
