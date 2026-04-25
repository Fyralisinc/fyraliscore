"""services/workers/anomaly_processor/ — Wave 4-B.

Anomaly processor per ARCHITECTURE-FINAL.md §18 and BUILD-PLAN.md
§5 Prompt 4.B. Detects six anomaly kinds, scores significance, applies
debounce, writes sub-threshold signals to the Memory Fabric, and
enqueues T3 triggers into `think_trigger_queue` for Wave 3-B Think to
pick up.

Scope carve-out (per agent brief):
- This package does NOT build `pattern_candidates` / Precipitation
  logic. Agent 4-C owns those.
- `signal_memory_fabric` (migration 0009) is owned here.
- T3 enqueue payload includes `region_spec={'entity_ids': [...]}` AND
  `seed_entity_ids=[...]` so the Wave 3 worker's `_populate_seed_fields`
  (bug #1 patch, 2026-04-21T07:07:04Z) rehydrates the TriggerContext
  correctly.
"""
from .detectors import (
    AnomalyCandidate,
    detect_activation_decay_anomaly,
    detect_commitment_drift,
    detect_contestation_cluster,
    detect_external_signal_anomaly,
    detect_resource_overcommit,
    detect_silent_disagreement,
)
from .memory_fabric import promote_if_accumulated, record_subthreshold_signal
from .significance import SIGNIFICANCE_THRESHOLD, compute_significance
from .worker import AnomalyProcessor, AnomalyProcessorConfig

__all__ = [
    "AnomalyCandidate",
    "AnomalyProcessor",
    "AnomalyProcessorConfig",
    "SIGNIFICANCE_THRESHOLD",
    "compute_significance",
    "detect_activation_decay_anomaly",
    "detect_commitment_drift",
    "detect_contestation_cluster",
    "detect_external_signal_anomaly",
    "detect_resource_overcommit",
    "detect_silent_disagreement",
    "promote_if_accumulated",
    "record_subthreshold_signal",
]
