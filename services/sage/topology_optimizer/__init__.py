"""services.sage.topology_optimizer — Phase 13 Topology Optimizer.

Re-exports the rule-based topology update surface (doc §5.6 + §16):

  * `TopologyOptimizer` — main class; `.optimize` consumes
    `inquiry_outcome_events` for a session and writes ONLY to the
    Discovery Utility Layer (affordances, shortcuts, negative memory,
    region summaries). Doc §22.1 forbids canonical-truth writes here.
  * `OptimizationRunReport` — structured result of one optimization
    pass: counts of utility-layer writes performed plus four buckets
    of canonical merge/split/promote/demote *candidate* dicts that
    must travel through validation before changing canonical truth.
  * `optimize_topology` — thin functional wrapper that constructs a
    default-wired optimizer and runs one pass.
  * `enqueue_for_validation` — current no-op stub for the canonical-
    op gate; will be wired to the validation queue in a later phase.

Module-level tunables (REINFORCE_DELTA, DECAY_FACTOR,
SHORTCUT_POSITIVE_DELTA, NEGATIVE_MEMORY_TTL) are exported so tests
and ops can read / monkey-patch them when reasoning about behavior.
"""

from services.sage.topology_optimizer.api import optimize_topology
from services.sage.topology_optimizer.optimizer import (
    DECAY_FACTOR,
    NEGATIVE_MEMORY_TTL,
    REINFORCE_DELTA,
    SHORTCUT_POSITIVE_DELTA,
    TopologyOptimizer,
    enqueue_for_validation,
)
from services.sage.topology_optimizer.types import OptimizationRunReport


__all__ = [
    "DECAY_FACTOR",
    "NEGATIVE_MEMORY_TTL",
    "OptimizationRunReport",
    "REINFORCE_DELTA",
    "SHORTCUT_POSITIVE_DELTA",
    "TopologyOptimizer",
    "enqueue_for_validation",
    "optimize_topology",
]
