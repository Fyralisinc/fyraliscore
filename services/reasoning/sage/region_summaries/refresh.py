"""services.reasoning.sage.region_summaries.refresh — Phase 11 refresh skeleton.

Decides *when* a region summary should be re-derived and performs the
mechanical part of the refresh (load current members, recompute the
two numeric scores and a heuristic frontier list). The LLM-driven
narrative synthesis (summary text, hypothesis/constraint extraction,
counterevidence call-outs, falsification watch generation) lives in
a separate path and is intentionally left as a TODO here.

Trigger taxonomy (Phase 11):
    validated_model_update | high_impact_signal | prediction_error |
    user_contestation     | scheduled          | region_anomaly

Refresh policy (v1):
    * Always refresh on `user_contestation` and `prediction_error` —
      these are explicit "the current summary is wrong" signals.
    * Always refresh on `region_anomaly` — a structural shift means
      the old summary's frontiers are likely stale.
    * Refresh on `validated_model_update` only when the region has
      not been refreshed within the past hour (debounce).
    * Refresh on `high_impact_signal` when the signal nudges the
      region's priority above a threshold OR when the region is
      already in the active band.
    * `scheduled` is the slow background sweep — caller decides
      cadence; `should_refresh` always returns True for it.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg

from services.reasoning.sage.region_summaries.types import (
    Frontier,
    RefreshReason,
    RegionSufficientState,
)


# Tunable knobs — kept module-level so tests / ops can monkeypatch.
DEBOUNCE_VALIDATED_UPDATE = timedelta(hours=1)
HIGH_IMPACT_PRIORITY_FLOOR = 0.6


def should_refresh(
    region: RegionSufficientState,
    trigger_reason: str,
    *,
    now: datetime | None = None,
) -> bool:
    """Decide whether `trigger_reason` warrants a refresh of `region`.

    Pure function — no DB. The caller looks up the current region row,
    passes it here with the inbound trigger, and acts on the boolean.
    """
    now_ts = now or datetime.now(timezone.utc)

    if trigger_reason in (
        "user_contestation",
        "prediction_error",
        "region_anomaly",
        "scheduled",
    ):
        return True

    if trigger_reason == "validated_model_update":
        last = region.updated_at
        if last is None:
            return True
        # Treat naive timestamps as UTC — matches the column default
        # (now() returns timestamptz in Postgres but the driver may
        # surface naive when callers strip tzinfo).
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        return (now_ts - last) >= DEBOUNCE_VALIDATED_UPDATE

    if trigger_reason == "high_impact_signal":
        return region.priority_score >= HIGH_IMPACT_PRIORITY_FLOOR

    # Unknown reason — be conservative and refresh, but log via the
    # caller's metric. We don't import structlog here to keep the
    # module pure.
    return True


async def refresh_region(
    region_id: UUID,
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    reason: RefreshReason = "scheduled",
) -> RegionSufficientState:
    """Rebuild the numeric/heuristic parts of a region summary in place.

    Loads the current row (or constructs a stub if absent), pulls the
    live member Models, recomputes `priority_score`,
    `prediction_error_score`, and a placeholder `next_best_frontiers`
    list from cheap structural signals, then UPSERTs.

    NOT in scope for this skeleton:
        * LLM synthesis of `summary`, `active_hypotheses`,
          `active_constraints`, `known_counterevidence`,
          `unresolved_unknowns`, `falsification_watch`. The caller
          chains an LLM step *after* this function and re-upserts the
          fully synthesized row through the repo.
    """
    # Import locally to keep the public re-export surface in
    # `__init__.py` from cycling through the repo at module load.
    from services.reasoning.sage.region_summaries.repo import RegionSummariesRepo

    repo = RegionSummariesRepo(pool=None, tenant_id=tenant_id)
    current = await repo.get(region_id, conn=conn)

    # Member-model snapshot. The region itself is not a first-class
    # entity yet, so the source of truth for membership is
    # `model_structural_features.region_ids` (migration 0085). We
    # query it directly to avoid importing the structural-features
    # service from this skeleton.
    rows = await conn.fetch(
        """
        SELECT model_id
        FROM model_structural_features
        WHERE tenant_id = $1
          AND $2 = ANY(region_ids)
        """,
        tenant_id,
        region_id,
    )
    member_ids: list[UUID] = [r["model_id"] for r in rows]

    # Heuristic priority: log-scale on the active member count. A
    # region with no members scores 0; the score saturates near 1.0
    # at ~50 members. This is intentionally crude — Phase 11 just
    # needs *some* signal to bootstrap the leaderboard.
    member_count = len(member_ids)
    if member_count == 0:
        priority = 0.0
    else:
        # Avoid an extra dependency on math: use a simple ratio.
        priority = min(1.0, member_count / 50.0)

    # Prediction-error placeholder: until Phase 12 lands a real
    # `model_prediction_errors` join, leave whatever the previous
    # refresh wrote. Conservative — never zeroes a known error.
    pred_err = current.prediction_error_score if current is not None else 0.0

    # Frontier placeholder: one entry per member, capped so a region
    # with thousands of members doesn't blow up the JSONB column.
    frontiers: list[Frontier] = [
        Frontier(
            target=str(mid),
            rationale="member-model follow-up (placeholder)",
            expected_information_gain=0.0,
        )
        for mid in member_ids[:5]
    ]

    # Preserve narrative-text fields from the existing row when
    # present — the LLM synthesis step is the one allowed to rewrite
    # them. New regions get an empty stub summary so the NOT NULL
    # constraint on `summary` is satisfied.
    summary_text = current.summary if current is not None else ""
    label = current.region_label if current is not None else None
    hypotheses = current.active_hypotheses if current is not None else []
    constraints = current.active_constraints if current is not None else []
    counterevidence = (
        current.known_counterevidence if current is not None else []
    )
    unknowns = current.unresolved_unknowns if current is not None else []
    falsification = current.falsification_watch if current is not None else []
    goals = current.affected_goals if current is not None else []
    commitments = current.affected_commitments if current is not None else []

    updated = RegionSufficientState(
        region_id=region_id,
        tenant_id=tenant_id,
        region_label=label,
        summary=summary_text,
        active_hypotheses=hypotheses,
        active_constraints=constraints,
        known_counterevidence=counterevidence,
        unresolved_unknowns=unknowns,
        affected_goals=goals,
        affected_commitments=commitments,
        member_model_ids=member_ids,
        priority_score=priority,
        prediction_error_score=pred_err,
        next_best_frontiers=frontiers,
        falsification_watch=falsification,
        last_refreshed_reason=reason,
    )

    # TODO: LLM summary synthesis — call the region-summary prompt
    # with (member Models, edges, recent observations, current
    # hypotheses) and overwrite `summary`, `active_hypotheses`,
    # `active_constraints`, `known_counterevidence`,
    # `unresolved_unknowns`, `falsification_watch` on `updated`
    # before the upsert below.

    return await repo.upsert(updated, conn=conn)


__all__ = ["refresh_region", "should_refresh"]
