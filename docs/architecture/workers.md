# Workers — Background Jobs

> Source: `services/workers/` + launchers in `scripts/`.
> Part of the [architecture overview](index.md).

**One-line:** out-of-request-path worker packages (anomaly, entity, calibration,
deadline, precipitation, edge-drift, topology-sweep, maintenance) that poll
Postgres tables/queues to enqueue Think triggers and maintain the substrate.

!!! danger "Deployment status — verified on this branch"
    **No `services/workers/*` package is wired into `docker-compose.yml`.** The
    only one with a launcher is `topology_sweeper` (`scripts/run_topology_sweeper.py`,
    also started by `scripts/dogfood_up.sh`). The rest are **implemented but not
    deployed** as first-class processes here. (The compose stack *does* run the
    `think_worker`, `post_commit_worker`, and the ingestion
    consumer workers — but those live in [`services/reasoning`](reasoning.md) and
    [`services/ingest`](ingest.md), **not** in `services/workers/`.) This matches
    `CODEBASE-ARCHITECTURE.md` §12 ("implemented but not first-class compose
    services yet").

## The worker packages

| Package | Job (from module docstring) | Deployed? |
|---------|------------------------------|-----------|
| `topology_sweeper` | Re-runs the latent topology field over high-activation Models → `relationship_candidates` + `T4` (`LatentTopologyService.sweep_tenant`). | Launcher only (`scripts/run_topology_sweeper.py`, dogfood). |
| `anomaly_processor` | Wave 4-B. Detects six anomaly kinds, scores significance, debounces, writes sub-threshold signals to the Memory Fabric (`signal_memory_fabric`), and enqueues `T3` triggers. | Not in compose. |
| `entity_resolver` | Deferred LLM resolution of `content._unresolved_phrases` → inserts aliases, appends entities, re-enqueues `T1` (medium-confidence → `entity_review_queue`). | Not in compose. |
| `calibration_updater` | Wave 4-C weekly. Turns the append-only `calibration_stats` log into the mutable `calibration_offsets` table. | Not in compose. |
| `deadline_resolver` | Wave 4-A. Polls prediction Models whose `evaluate_at` passed → enqueues `T2 prediction_overdue` (never writes `models` directly; Think's deterministic T2 handler owns the deltas). | Not in compose. |
| `precipitation` | Wave 4-C nightly. Clusters related `hypothesis`/`concern` Models into one `pattern_candidates` row per dense embedding cluster → promoted by Think `T4 pattern_review`. | Not in compose. |
| `edge_drift` | Samples `model_edges` vs. legacy array columns to detect typed-edge drift parity (`EdgesRepo.get_drift_sample`). | Not in compose. |
| `maintenance` | Wave 4-D. `daily.py` (decay + archival + alias cleanup + orphan/think_runs/region-lock cleanup), `weekly.py` (relationship maintenance + calibration + partition extension + memory-fabric decay), `monthly.py` (vacuum analyze, cold-partition notes, reports), `scheduler.py` (in-process asyncio scheduler). | Not in compose (in-process scheduler). |
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
- All other packages — importable worker classes (`worker.py`) or, for
  `maintenance`, the in-process `scheduler.py`. **No compose service or
  `scripts/run_*` launcher exists for them on this branch.**

## Dependencies

**Inbound** *(verified)*: only the `topology_sweeper` launcher and the in-process
maintenance scheduler invoke these today; the rest await wiring.

**Outbound** *(verified)*: `services.domain` (models decay/repo, `EdgesRepo` drift
sample, entity aliases, falsifiers), `services.reasoning` (topology field,
retrieval maintenance), `lib.llm.provider` (entity resolver), and the
`think_trigger_queue` / calibration tables.

## Design rationale

> **TODO(human):** This is the most decision-heavy layer to document. Capture:
>
> - **Why most workers are not deployed** — are anomaly/precipitation/calibration/
>   edge-drift/deadline intended for production and pending a wiring PR, or
>   deliberately dormant? (The memory of the project notes these are "coded +
>   migrated but NOT deployed.")
> - The fate of the source-less `neighborhood_detector` and `topology_updater`
>   packages (delete, or placeholders for re-introduction?).
> - The intended schedule/cadence and ownership of each worker once deployed.
