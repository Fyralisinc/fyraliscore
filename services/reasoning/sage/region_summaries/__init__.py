"""services.reasoning.sage.region_summaries — Phase 11 region sufficient-state surface.

Compact, evidence-backed summaries of important regions in the
Synthesis graph. A region is a logical cluster of related Models;
each summary captures what is currently known, what is unresolved,
what counter-evidence exists, and where the next best inquiry
frontiers lie — so retrieval can start from a digest instead of
re-traversing raw nodes.

Schema reference: db/migrations/0088_sage_region_sufficient_state.sql.
Spec reference:   fyralis-sage-synthesis-self-evolution.md §12 / Phase 11.

Re-exports:
  * `RegionSufficientState` and nested JSON shapes — row type.
  * `RegionSummariesRepo`                          — asyncpg repo.
  * `should_refresh`, `refresh_region`             — refresh skeleton.
"""

from services.reasoning.sage.region_summaries.refresh import (
    refresh_region,
    should_refresh,
)
from services.reasoning.sage.region_summaries.repo import RegionSummariesRepo
from services.reasoning.sage.region_summaries.types import (
    Constraint,
    Counterevidence,
    FalsificationWatch,
    Frontier,
    Hypothesis,
    RegionSufficientState,
    Unknown,
)

__all__ = [
    "Constraint",
    "Counterevidence",
    "FalsificationWatch",
    "Frontier",
    "Hypothesis",
    "RegionSufficientState",
    "RegionSummariesRepo",
    "Unknown",
    "refresh_region",
    "should_refresh",
]
