# Worker Fabric Runbook

This runbook covers production background workers from `services/workers/*` and
their script launchers in `scripts/`.

## Launch Policy

The source of truth is `services.platform.runtime.worker_launch_policy`. CI
asserts that every `services/workers/*` package is classified and that
flag-gated jobs are disabled by default.

| Package | Production decision | Runtime surface |
|---------|---------------------|-----------------|
| `anomaly_processor` | Selected | `anomaly_processor_worker` compose service |
| `entity_resolver` | Selected | `entity_resolver_worker` compose service |
| `housekeeper` | Selected | `housekeeper_worker` compose service |
| `relationship_ontology_proposals` | Selected | `relationship_ontology_proposals_worker` compose service |
| `sage_structural_features` | Selected | `sage_structural_features_worker` compose service |
| `sage_topology_optimizer` | Selected | `sage_topology_optimizer_worker` compose service |
| `calibration_updater` | Selected via housekeeper | `calibration_updater` housekeeper job |
| `deadline_resolver` | Selected via housekeeper | `deadline_resolver` housekeeper job |
| `edge_drift` | Selected via housekeeper | `edge_drift` housekeeper job |
| `maintenance` | Selected via housekeeper | default lifecycle/metrics housekeeper jobs |
| `precipitation` | Not selected by default | `HOUSEKEEPER_ENABLE_PRECIPITATION=1` or `HOUSEKEEPER_ENABLE_EXPENSIVE_JOBS=1` |
| `topology_sweeper` | Dogfood/flag-gated by default | `HOUSEKEEPER_ENABLE_TOPOLOGY_SWEEPER=1` or dogfood launcher |

## Health And Metrics

All production workers expose `/healthz` and `/metrics` on
`INGESTION_HEALTH_PORT` (default `9300` in compose). Prometheus scrapes every
production process listed in `services.platform.runtime.process_manifest`.

Primary checks:

```bash
docker compose ps
curl -fsS http://localhost:9300/healthz
.venv/bin/pytest services/platform/runtime/tests/test_process_manifest.py -q
.venv/bin/pytest services/platform/runtime/tests/test_worker_launch_policy.py -q
```

Alerts:

- `WorkerHeartbeatStale`: worker event loop stopped advancing.
- `WorkerScrapeDown`: process is down or health port is unreachable.
- `SchemaRLSDriftDetected`: `schema_drift_monitor` found live schema/RLS drift
  or could not complete its check.
- Worker-specific counters:
  - `anomaly_processor_*`
  - `entity_resolver_*`
  - `schema_drift_*`
  - `housekeeper_*` via default worker metrics and scheduler logs.

## Safe Operations

Restart one worker:

```bash
docker compose up -d --no-deps --force-recreate entity_resolver_worker
```

Scale a horizontally safe worker:

```bash
docker compose up -d --scale anomaly_processor_worker=2 anomaly_processor_worker
```

Do not scale singleton or lease-backed workers unless their manifest marks them
non-singleton and their runbook allows it.

Enable an expensive/gated job only after a staging soak:

```bash
HOUSEKEEPER_ENABLE_PRECIPITATION=1 docker compose up -d housekeeper_worker
```

Rollback a gated job:

```bash
HOUSEKEEPER_ENABLE_PRECIPITATION=0 docker compose up -d --force-recreate housekeeper_worker
```

## Incident Triage

1. Check `WorkerScrapeDown` first. If scrape is down, inspect process logs and
   compose health before looking at domain metrics.
2. Check `WorkerHeartbeatStale`. A stale heartbeat usually means an event loop
   is blocked or the worker is deadlocked.
3. Check queue/domain counters:
   - anomaly path: `think_trigger_queue` rows for `T3`, `signal_memory_fabric`
   - entity path: `entity_aliases`, `entity_review_queue`, `clarification_requests`
   - housekeeper path: scheduler job stats and the target table the job owns.
4. Restart the smallest affected worker. Avoid full-stack restarts unless
   shared infrastructure is unhealthy.
5. If a worker repeatedly restarts, disable only its gated job or stop only that
   compose service, then preserve logs for root-cause analysis.

## Queue Depth Inspection

Use `scripts/inspect_queue_depth.py` when an alert or customer report points to
stalled background work. It returns bounded counts only and writes a
`queue_depth.inspect` row to `operator_action_log`. The operator actor must
have tenant-wide `admin` or `leadership`.

```bash
python scripts/inspect_queue_depth.py \
  --tenant "$TENANT_ID" \
  --operator-actor "$OPERATOR_ACTOR_ID"
```

The report covers:

- `think_trigger_queue`: pending, ready, locked
- `model_reeval_queue`: pending
- `pending_post_commit_actions`: pending, dead-lettered
- `ingestion_failures`: unresolved, quarantined
- `source_onboarding_runs`: pending, in-progress, failed

## Verification Before Release

Run:

```bash
.venv/bin/pytest \
  services/platform/runtime/tests/test_process_manifest.py \
  services/platform/runtime/tests/test_worker_launch_policy.py \
  services/platform/runtime/tests/test_observability_provisioning.py \
  services/workers/housekeeper/tests/test_worker.py \
  -q
```

For resolver or anomaly changes, also run their worker-specific integration
tests before promotion.
