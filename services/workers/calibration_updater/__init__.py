"""
services/workers/calibration_updater — Wave 4-C calibration worker.

Weekly job that turns the immutable append-only `calibration_stats`
log into a mutable `calibration_offsets` table (per
ARCHITECTURE-FINAL.md §9 and BUILD-PLAN §5 Prompt 4.C).

Pipeline
--------
1. Harvest `calibration_stats` rows from resolved predictions (any
   Model whose `resolved_at IS NOT NULL`). The worker upserts one
   `calibration_stats` row per resolved Model at the start of each
   run — idempotent via `ON CONFLICT DO NOTHING` on
   (source_model_id, resolved_at).
2. For every distinct (tenant_id, actor_id, proposition_kind) tuple,
   compute bucketed empirical rates over a 180-day window. If any
   bucket has < 5 samples or the tuple has < 20 total samples,
   fall back to cold-start defaults.
3. Upsert rows into `calibration_offsets`.
4. Bulk-apply the freshest offsets to all currently-active Models via
   `ModelsRepo.bulk_confidence_update`. Clipped per the [0.05, 0.95]
   CHECK on `models.confidence`.

Entry point: `services.workers.calibration_updater.worker.run_once`.
"""
from services.workers.calibration_updater.worker import run_once

__all__ = ["run_once"]
