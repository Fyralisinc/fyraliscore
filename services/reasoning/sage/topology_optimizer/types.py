"""services/reasoning/sage/topology_optimizer/types.py — Phase 13 row + report types.

The Topology Optimizer (doc §5.6 + §16) consumes outcome events appended
by the Outcome Evaluator and writes ONLY to the Discovery Utility Layer
(affordance profiles, discovery shortcuts, negative memory, region
sufficient state — doc §22.1). Canonical merge/split/promote/demote
operations are produced as candidate payloads that must travel through
the existing validation pipeline; they are NEVER applied here.

`OptimizationRunReport` is the return shape of `TopologyOptimizer.optimize`:
a structured tally of every utility-layer write performed during the run,
plus four candidate buckets and a free-form metrics dict. Keeping the
candidate lists separate from the applied counters makes the
"discovery-utility vs. canonical-truth" boundary explicit at the API
surface, mirroring the way the spec separates the two columns at §22.1.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(frozen=True, slots=True)
class OptimizationRunReport:
    """Structured outcome of one Topology Optimizer run.

    Fields are bucketed by destination:

      * `affordance_*`, `shortcut_*`, `negative_memory_*`,
        `region_refreshes` — counts of writes APPLIED to the Discovery
        Utility Layer.
      * `canonical_*_candidates` — payload tuples for proposed canonical
        topology ops (merge/split/promote/demote). The optimizer does
        NOT apply them; they are forwarded to the validation pipeline.
      * `metrics` — free-form float-valued counters (e.g. useful node
        count, noisy-path count) for observability + later learned
        ranking.
    """

    inquiry_session_id: UUID
    affordance_reinforces: int
    affordance_decays: int
    shortcut_creates_or_bumps: int
    shortcut_decays: int
    negative_memory_inserts: int
    region_refreshes: int
    question_policy_updates: int
    canonical_merge_candidates: tuple[dict, ...]
    canonical_split_candidates: tuple[dict, ...]
    canonical_promote_candidates: tuple[dict, ...]
    canonical_demote_candidates: tuple[dict, ...]
    metrics: dict[str, float]
    experience_loop: dict[str, Any] | None = None


__all__ = ["OptimizationRunReport"]
