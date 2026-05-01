-- taskq-rs Phase 5c additions (`design.md` §6.7, §9.4).
--
-- 1. `namespace_quota.disabled` — flipped by EnableNamespace / DisableNamespace
--    admin RPCs. SubmitTask rejects with NAMESPACE_DISABLED when set.
--
-- 2. `task_type_registry` — append-only registry of allowed `task_type`
--    strings per namespace, parallel to `error_class_registry`. Phase 5b's
--    SetNamespaceConfig adds rows here; ReportFailure rejects unknown classes.
--    `max_task_types` (in `namespace_quota`) caps the cardinality at
--    SetNamespaceConfig time per `design.md` §11.3.
--
-- 3. Bump `taskq_meta.schema_version` to 2 — `taskq-cp`'s startup gate looks
--    at this column and refuses to start when it doesn't match the binary
--    constant.

ALTER TABLE namespace_quota
    ADD COLUMN IF NOT EXISTS disabled boolean NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS task_type_registry (
    namespace      text NOT NULL,
    task_type      text NOT NULL,
    deprecated     boolean NOT NULL DEFAULT FALSE,
    registered_at  timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (namespace, task_type)
);

UPDATE taskq_meta SET schema_version = 2, last_migrated_at = NOW() WHERE only_row = TRUE;
