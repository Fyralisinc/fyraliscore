"""Per-source reconciliation algorithms. Per ingestion LLD §3 + the
M6.2b prompt's gap-detection contract.

============================================================
ROLE
============================================================
A reconciler is a per-source function that examines the completed
shard state for a (run, source) pair and decides whether the
coverage is acceptable ("clean" — no gaps) or whether additional
re-shared shards are needed to fill gaps. The M6.2b Reconciler
service calls these algorithms; per-source reconcilers in
M6.3-M6.6 will implement the real algorithms.

The contract — codified by M6.2b Phase 1:

    RECONCILER_DISPATCH[source](
        shards: list[asyncpg.Record],          # all shards for this (run, source)
        run: asyncpg.Record,                   # the source_onboarding_runs row
    ) -> ReconciliationDecision

============================================================
DISPATCH TABLE — STUB ON M6.2b, REAL ON M6.3-M6.6
============================================================
Same shape as M6.2a's `PLANNER_DISPATCH` + `FETCHER_DISPATCH`:

    | source   | M6.x sub-block |
    |----------|----------------|
    | gmail    | M6.3           |
    | github   | M6.4           |
    | slack    | M6.5           |
    | discord  | M6.6           |

**The pre-M6.3 default is `has_gaps=False`, NOT NotImplementedError.**
Unlike the planner/fetcher stubs (which raise to surface "no real
implementation yet"), the reconciler stub MUST return a default
"clean" decision because the system needs to function pre-M6.3-M6.6
— if reconcilers raised, no tenant onboarding would ever complete.
The clean default is the right pre-implementation behaviour: assume
no gaps until a real algorithm proves otherwise.

The stub message names the responsible M6.x sub-block so operators
querying `source_onboarding_runs` (or service logs) immediately
know where the implementation work lives. Same fail-loud-with-
context pattern as M6.2a, just defaulting to clean rather than
raising.

============================================================
RESHARED-SHARD CONTRACT (LOAD-BEARING for M6.3-M6.6)
============================================================
`ReconciliationDecision.new_shards` is a list of `ResharedShard`.
Each entry describes ONE new shard to INSERT into onboarding_shards
with `parent_shard_id` set. The per-source algorithm picks which
original shard "owns" each gap (typically the original whose
window/identifier overlaps the gap region).

The new shard's `recency_score` defaults to a boosted value above
1.0 so reshared shards run ahead of any remaining low-recency
backfill (per LLD §3 + HLD §6 specifications). M6.3-M6.6
per-source reconcilers may override per source-specific concerns.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from services.ingestion.planners import Shard


# ---------------------------------------------------------------------
# Public types.
# ---------------------------------------------------------------------
class ResharedShard(BaseModel):
    """A new shard to INSERT for re-share, linking to an original.

    `parent_shard_id` references the original `onboarding_shards.id`
    whose gap this new shard fills. The original shard transitions
    to `state='reconciliation_resharded'` (terminal) in the same
    transaction that INSERTs this row.

    The Pydantic shape is used for type-checking at the dispatch
    boundary; the Reconciler service translates this into the actual
    INSERT.
    """

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    shard: Shard
    parent_shard_id: UUID


class ReconciliationDecision(BaseModel):
    """Output of a per-source reconciliation algorithm.

    `has_gaps=False` → CLEAN path: Reconciler stamps `reconciled_at`
    on `source_onboarding_runs` and emits `source_onboarding_completed`
    to TenantOnboarding.

    `has_gaps=True` → RE-SHARE path: Reconciler increments
    `reconciliation_pass_count`, transitions `source_onboarding_runs.status`
    back to 'in_progress', marks the affected original shards
    `state='reconciliation_resharded'`, INSERTs `new_shards` rows, and
    emits `shard_fetch_requested` per new shard.

    `message` is operator-visible. It's stored only if it indicates a
    pre-M6.x stub default (so ops queries can grep for the M6.x
    references); real reconcilers may pass empty strings.
    """

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    has_gaps: bool
    message: str = ""
    new_shards: list[ResharedShard] = Field(default_factory=list)


Reconciler = Callable[
    [list[asyncpg.Record], asyncpg.Record],
    Awaitable[ReconciliationDecision],
]


# ---------------------------------------------------------------------
# Default-clean stub factory.
# ---------------------------------------------------------------------
def _not_implemented_reconciler(source: str, milestone: str) -> Reconciler:
    """Build a reconciler stub that returns a default-clean decision.

    Unlike planner/fetcher stubs which raise `NotImplementedError`,
    the reconciler stub MUST return a valid decision because the
    system needs to function pre-M6.3-M6.6 (otherwise no tenant
    onboarding ever completes). The clean default means: "assume
    no gaps until a real algorithm proves otherwise."

    The `message` includes the M6.x reference so operators querying
    `source_onboarding_runs` failures (or scanning service logs)
    know where the implementation lives. Same fail-loud-with-context
    pattern as M6.2a planner/fetcher stubs, but defaulting to clean
    rather than raising.
    """
    async def stub(
        shards: list[asyncpg.Record], run: asyncpg.Record,
    ) -> ReconciliationDecision:
        return ReconciliationDecision(
            has_gaps=False,
            message=(
                f"Reconciler for source={source!r} not yet implemented; "
                f"defaulting to clean. Pending in {milestone} per "
                f"docs/ingestion/04-implementation-plan.md §{milestone}. "
                f"Until then, all (tenant, {source}) onboardings are "
                f"treated as gap-free; this is the expected pre-{milestone} "
                f"steady state, not a regression."
            ),
        )
    stub.__name__ = f"_not_implemented_reconciler_{source}"
    return stub


# ---------------------------------------------------------------------
# Dispatch table — M6.2b Phase 1.
# ---------------------------------------------------------------------
# Module-level mutable dict; ALL_CAPS (constant-style, outside the
# pattern-alignment analyzer's services/ingestion/workflows/*.py
# scope per the Rule 5 calibration). Production replacements
# (M6.3-M6.6) overwrite entries at module-import time; tests rebind
# via monkeypatch.setitem.
RECONCILER_DISPATCH: dict[str, Reconciler] = {
    "gmail":   _not_implemented_reconciler("gmail",   "M6.3"),
    "github":  _not_implemented_reconciler("github",  "M6.4"),
    "slack":   _not_implemented_reconciler("slack",   "M6.5"),
    "discord": _not_implemented_reconciler("discord", "M6.6"),
}


__all__ = [
    "RECONCILER_DISPATCH",
    "Reconciler",
    "ReconciliationDecision",
    "ResharedShard",
]


# Per-source modules import below — each assigns into RECONCILER_DISPATCH
# at module-load time (per A18).
from services.ingestion.reconcilers import gmail as _gmail  # noqa: E402,F401
from services.ingestion.reconcilers import github as _github  # noqa: E402,F401
