# Workers — Background Jobs

> Source: `services/workers/` + launchers in `scripts/`.
> Part of the [architecture overview](index.md).

**One-line:** out-of-request-path worker packages (anomaly, entity, calibration,
deadline, precipitation, edge-drift, topology-sweep, maintenance) that poll
Postgres tables/queues to enqueue Think triggers and maintain the substrate.

!!! info "Deployment status — verified on this branch"
    `docker-compose.yml` now wires first-class `services/workers/*` processes for
    `anomaly_processor_worker`, `entity_resolver_worker`, and
    `housekeeper_worker`, all represented in the runtime process manifest.
    Housekeeper runs the low-frequency lifecycle jobs by default and keeps
    expensive jobs opt-in behind environment flags.

## The worker packages

| Package | Job (from module docstring) | Deployed? |
|---------|------------------------------|-----------|
| `topology_sweeper` | Re-runs the latent topology field over high-activation Models → `relationship_candidates` + `T4` (`LatentTopologyService.sweep_tenant`). | Dogfood launcher; production can run via Housekeeper opt-in. |
| `anomaly_processor` | Wave 4-B. Detects six anomaly kinds, scores significance, debounces, writes sub-threshold signals to the Memory Fabric (`signal_memory_fabric`), and enqueues `T3` triggers. | Compose service `anomaly_processor_worker`. |
| `entity_resolver` | Deferred LLM resolution of `content._unresolved_phrases` → inserts aliases, appends entities, re-enqueues `T1` (medium-confidence → `entity_review_queue`). | Compose service `entity_resolver_worker`. |
| `calibration_updater` | Wave 4-C weekly. Turns the append-only `calibration_stats` log into the mutable `calibration_offsets` table. | Scheduled by `housekeeper_worker`. |
| `deadline_resolver` | Wave 4-A. Polls prediction Models whose `evaluate_at` passed → enqueues `T2 prediction_overdue` (never writes `models` directly; Think's deterministic T2 handler owns the deltas). | Scheduled by `housekeeper_worker`. |
| `precipitation` | Wave 4-C nightly. Clusters related `hypothesis`/`concern` Models into one `pattern_candidates` row per dense embedding cluster → promoted by Think `T4 pattern_review`. | Housekeeper opt-in. |
| `edge_drift` | Samples `model_edges` vs. legacy array columns to detect typed-edge drift parity (`EdgesRepo.get_drift_sample`). | Scheduled by `housekeeper_worker`. |
| `maintenance` | Wave 4-D. `daily.py` (decay + archival + alias cleanup + orphan/think_runs/region-lock cleanup), `weekly.py` (relationship maintenance + calibration + partition extension + memory-fabric decay), `monthly.py` (vacuum analyze, cold-partition notes, reports), `scheduler.py` (in-process asyncio scheduler). | Partially scheduled by `housekeeper_worker`; full daily/monthly bundles remain separate scheduling decisions. |
| `neighborhood_detector` | **No source on this branch** — only stale `__pycache__/` + `tests/`. Relates to the retired accepted-memory "neighborhood" topology. | Not present. |
| `topology_updater` | **No source on this branch** — only stale `__pycache__/` + `tests/`. Relates to the retired accepted-memory topology. | Not present. |

## How it's wired

```mermaid
graph TD
    subgraph workers["services/workers (mostly undeployed)"]
      SWEEP["topology_sweeper"]
      ANOM["anomaly_processor"]
      ENT["entity_resolver"]
      CAL["calibration_updater"]
      DL["deadline_resolver"]
      PREC["precipitation"]
      DRIFT["edge_drift"]
      MAINT["maintenance (daily/weekly/monthly)"]
    end

    PG[("PostgreSQL: models · observations · queues · calibration_*")]
    TTQ[("think_trigger_queue")]
    LLM["lib.llm provider"]

    SWEEP -->|"sweep → candidates"| PG
    SWEEP -->|"T4"| TTQ
    ANOM -->|"detect → T3"| TTQ
    ANOM --> PG
    ENT -->|"resolve phrases"| LLM
    ENT -->|"alias + re-enqueue T1"| TTQ
    DL -->|"T2 prediction_overdue"| TTQ
    PREC -->|"pattern_candidates"| PG
    PREC -->|"T4 pattern_review"| TTQ
    CAL -->|"calibration_offsets"| PG
    DRIFT -->|"drift sample"| PG
    MAINT -->|"decay · cleanup · partition extend"| PG
```

## Entry points

- `topology_sweeper` — `scripts/run_topology_sweeper.py` (also via `dogfood_up.sh`).
- `anomaly_processor` — `scripts/run_anomaly_processor_worker.py` (compose
  `anomaly_processor_worker`).
- `entity_resolver` — `scripts/run_entity_resolver_worker.py` (compose
  `entity_resolver_worker`).
- `maintenance` / low-frequency lifecycle jobs — `scripts/run_housekeeper_worker.py`
  (compose `housekeeper_worker`).
- Remaining expensive jobs — scheduled through Housekeeper when their opt-in flags
  are enabled, or through their dedicated launcher where one exists.

## Dependencies

**Inbound** *(verified)*: compose launches `anomaly_processor_worker`,
`entity_resolver_worker`, and `housekeeper_worker`; dogfood can still launch
`topology_sweeper` directly.

**Outbound** *(verified)*: `services.domain` (models decay/repo, `EdgesRepo` drift
sample, entity aliases, falsifiers), `services.reasoning` (topology field,
retrieval maintenance), `lib.llm.provider` (entity resolver), and the
`think_trigger_queue` / calibration tables.

## Design rationale

> **TODO(human):** This is the most decision-heavy layer to document. Capture:
>
> - Which expensive Housekeeper jobs should be enabled by default per environment
>   (`topology_sweeper`, `precipitation`, relationship ontology proposals, SAGE
>   structural features).
> - The fate of the source-less `neighborhood_detector` and `topology_updater`
>   packages (delete, or placeholders for re-introduction?).
> - The intended schedule/cadence and ownership of each opt-in worker once deployed.
