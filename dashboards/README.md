<!-- agent-updated: 2026-05-02T21:35:28Z -->

# Reference dashboards

Reference Grafana dashboard JSON for the `taskq-rs` standard metric set.
Pair with a Prometheus scraper pulling `taskq-cp`'s `/metrics` endpoint
(see [`OPERATIONS.md`](../OPERATIONS.md) §4 for setup).

## Files

| File | Contents |
|------|----------|
| [`grafana-overview.json`](./grafana-overview.json) | Operator overview: throughput, queue depths, dispatch latency, lease-expiration rate, worker fleet size, SSI conflicts. 8 panels in a 12-wide grid. |

## Prerequisites

1. **`taskq-cp`** running with the Prometheus exporter selected:

   ```toml
   # /etc/taskq/cp.toml
   [otel_exporter]
   kind = "prometheus"
   ```

   The `/metrics` endpoint is served on `health_addr` (default
   `0.0.0.0:9090`). See
   [`taskq-cp/src/observability.rs`](../taskq-cp/src/observability.rs)
   `init_prometheus`.

2. **Prometheus** scraping the endpoint. Reference scrape config:

   ```yaml
   scrape_configs:
     - job_name: 'taskq-cp'
       scrape_interval: 30s
       static_configs:
         - targets: ['taskq-cp.internal:9090']
   ```

3. **Grafana 10+** with the Prometheus datasource plugin enabled.

## Importing

1. Grafana UI -> Dashboards -> New -> Import.
2. Upload `grafana-overview.json` or paste its contents.
3. When prompted, select the Prometheus datasource that scrapes
   `taskq-cp`. The dashboard uses the `${DS_PROMETHEUS}` template
   variable; selecting the datasource at import time binds it.
4. Save. Default refresh is `30s`; default time range is the last 1h.

To import via API:

```sh
curl -X POST -H "Authorization: Bearer ${GRAFANA_API_KEY}" \
     -H "Content-Type: application/json" \
     -d @grafana-overview.json \
     https://grafana.internal/api/dashboards/db
```

## Metric set reference

The full standard metric set is documented in
[`design.md`](../design.md) §11.3 and emitted by
[`taskq-cp/src/observability.rs`](../taskq-cp/src/observability.rs)
`MetricsHandle::register`. Quick reference for the panels in this
dashboard:

| Panel | Metric | Type | Labels |
|-------|--------|------|--------|
| Throughput: submit vs complete | `taskq_submit_total`, `taskq_complete_total` | counter | `namespace`, `task_type` |
| Pending depth | `taskq_pending_count` | gauge | `namespace` |
| Inflight (DISPATCHED) | `taskq_inflight_count` | gauge | `namespace` |
| Dispatch latency p99 | `taskq_dispatch_latency_seconds` | histogram | `namespace` (no `task_type`) |
| Long-poll wait p50 | `taskq_long_poll_wait_seconds` | histogram | `namespace` (no `task_type`) |
| Lease expirations | `taskq_lease_expired_total` | counter | `namespace`, `task_type` |
| Workers registered | `taskq_workers_registered` | gauge | `namespace` |
| Storage SSI conflicts | `taskq_storage_serialization_conflict_total` | counter | `op` |

Histograms drop `task_type` from labels per
[`design.md`](../design.md) §11.3 cardinality budget — bucket count
otherwise multiplies by ~20x per task_type. Per-task-type latency
analysis goes through traces.

## Customization

The dashboard intentionally targets Grafana 10+ baseline (timeseries
panels, classic palette, table legend). Operators wanting:

- **Per-task-type breakouts** — add overrides to the throughput / lease
  expirations panels using the `task_type` label (counters retain it).
- **Alerts** — add Grafana unified alerting rules on `taskq_pending_count`
  approaching the namespace's `max_pending` (cross-reference
  [`protocol/QUOTAS.md`](../protocol/QUOTAS.md) §2.1) or on
  `taskq_lease_expired_total` rate exceeding a baseline.
- **More panels** — the full metric set in
  [`design.md`](../design.md) §11.3 includes
  `taskq_idempotency_hit_total`, `taskq_rejection_total`,
  `taskq_retry_total`, `taskq_terminal_total`, `taskq_error_class_total`,
  `taskq_heartbeat_total`, `taskq_quota_usage_ratio`,
  `taskq_rate_limit_hit_total`, `taskq_waiters_active`,
  `taskq_storage_transaction_seconds`, `taskq_storage_retry_attempts`,
  `taskq_replay_total`, `taskq_deprecated_field_used_total`,
  `taskq_schema_version`, `taskq_audit_log_pruned_total`. Add panels as
  needed.

## Cross-references

- [`design.md`](../design.md) §11 — observability framework
- [`OPERATIONS.md`](../OPERATIONS.md) §4 — observability setup
- [`protocol/QUOTAS.md`](../protocol/QUOTAS.md) §6.3 — cardinality budget
  math
- [`problems/11-observability.md`](../problems/11-observability.md) —
  full problem discussion
