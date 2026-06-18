"""services.reasoning.sage.topology_optimizer — Phase 13 Topology Optimizer.

Re-exports the rule-based topology update surface (doc §5.6 + §16):

  * `TopologyOptimizer` — main class; `.optimize` consumes
    `inquiry_outcome_events` for a session and writes ONLY to the
    Discovery Utility Layer (affordances, shortcuts, negative memory,
    region summaries). Doc §22.1 forbids canonical-truth writes here.
  * `OptimizationRunReport` — structured result of one optimization
    pass: counts of utility-layer writes performed plus four buckets
    of canonical merge/split/promote/demote *candidate* dicts that
    must travel through validation before changing canonical truth.
  * `OptimizationCadenceRequest` / `run_optimization_pass` — adapter
    layer for routes, scheduled jobs, and tests that need one
    default-wired optimization pass.
  * `optimize_topology` — backwards-compatible functional wrapper.
  * `enqueue_for_validation` — pure compatibility inspector. The product
    path persists validation candidates from `TopologyOptimizer`.

Module-level tunables (REINFORCE_DELTA, DECAY_FACTOR,
SHORTCUT_POSITIVE_DELTA, NEGATIVE_MEMORY_TTL) are exported so tests
and ops can read / monkey-patch them when reasoning about behavior.
"""

from services.reasoning.sage.topology_optimizer.api import optimize_topology
from services.reasoning.sage.topology_optimizer.cadence import (
    OptimizationCadenceRequest,
    SCHEDULED_TRIGGER,
    normalize_trigger_event,
    run_optimization_pass,
)
from services.reasoning.sage.topology_optimizer.optimizer import (
    DECAY_FACTOR,
    NEGATIVE_MEMORY_TTL,
    REINFORCE_DELTA,
    SHORTCUT_POSITIVE_DELTA,
    TopologyOptimizer,
    enqueue_for_validation,
)
from services.reasoning.sage.topology_optimizer.types import OptimizationRunReport


__all__ = [
    "DECAY_FACTOR",
    "NEGATIVE_MEMORY_TTL",
    "OptimizationCadenceRequest",
    "OptimizationRunReport",
    "REINFORCE_DELTA",
    "SCHEDULED_TRIGGER",
    "SHORTCUT_POSITIVE_DELTA",
    "TopologyOptimizer",
    "enqueue_for_validation",
    "normalize_trigger_event",
    "optimize_topology",
    "run_optimization_pass",
]
