"""
services/retrieval/second_pass.py — adaptive second-pass expansion.

Spec reference: ARCHITECTURE-FINAL.md §8 "Second-pass expansion".
BUILD-PLAN reference: §4 Prompt 3.A item 3.
RA-2 reference: RETRIEVAL-DESIGN-AUDIT §1 and §9 +
VARIANCE-INVESTIGATION-FINDINGS (second_pass_expand "imported but
never called"). This module now also owns a
`should_run_second_pass(...)` decision function so Think has a single
place to consult for "is an expansion warranted on this result?".

Supported dimensions (Wave 3-A):
  - `dependency_context`   — follow depends_on transitively from
    first-pass Commitments (both forward and backward), 2 hops max.
    Surface Commitments + their Models (via scope_entities match).
  - `supporting_evidence`  — for each first-pass Model, fetch
    supporting_event_ids → Observations + supporting_model_ids → Models
    (1 hop beyond the first-pass).
  - `adjacent_commitments` — Commitments that share a Goal (via
    contributes_to) with any first-pass Commitment, excluding the
    first-pass Commitments themselves.

Unknown dimensions are logged-and-skipped — Think may emit speculative
tokens; we don't want to raise and break the reasoning loop.

Invariants:
  - 2-hop global cap (visited set + depth counter).
  - Read-only except for ModelsRepo.retrieve reconsolidation of new
    Models.
  - Merges into the caller's RetrievalResult additively (never
    re-ranks or drops first-pass items; Wave 3-B Think may re-rank
    post-second-pass on its own).
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Sequence
from uuid import UUID

import asyncpg
import structlog

from lib.shared.types import CommitmentRow, ModelRow, ObservationRow

from services.models.repo import ModelsRepo

from .pathways import (
    _MODEL_SELECT_SQL,
    _OBS_SELECT_SQL,
    _hydrate_commitment,
    _hydrate_model,
    _hydrate_obs,
)
from .primary import RetrievalResult


_SECOND_PASS_MAX_HOPS = 2

_SUPPORTED_DIMENSIONS = {
    "dependency_context",
    "supporting_evidence",
    "adjacent_commitments",
}

# RA-2 activation thresholds. Exposed as module-level constants so
# services/retrieval/config.py (RA-5) can override them via
# RetrievalConfig without touching this module's signatures.
SECOND_PASS_SPARSE_THRESHOLD = 5
SECOND_PASS_BRIDGE_CONFIDENCE_THRESHOLD = 0.7

_log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------
# RA-2 — Should-run decision
# ---------------------------------------------------------------------


@dataclass
class SecondPassDecision:
    """
    Verdict from `should_run_second_pass`. `run` is the headline bool;
    `trigger_condition` names the specific rule that fired (for logs
    and metrics); `suggested_dimensions` is the list of dimensions the
    caller should expand on. `reason_detail` holds observability
    counters (item counts that crossed thresholds, confidences, etc.).
    """

    run: bool
    trigger_condition: str
    suggested_dimensions: list[str] = field(default_factory=list)
    reason_detail: dict[str, Any] = field(default_factory=dict)


def _context_is_saturated(
    primary_result: RetrievalResult,
    *,
    token_budget_saturation_ratio: float = 1.0,
) -> bool:
    """Heuristic: if the trigger's retrieval already saturated its
    assembly budget, a second pass has no room to add value. We probe
    two signals:
      1. `primary_result.notes['token_budget']` — if the assembler has
         already annotated the result with a post-assembly token count
         above its budget * saturation_ratio, bail.
      2. Fallback count-based: > 120 Models is our practical ceiling
         (assembler caps at 40; 120 is 3x the cap) — above that,
         adding more is almost certainly dead weight.
    """
    token_info = primary_result.notes.get("token_budget") if primary_result.notes else None
    if isinstance(token_info, dict):
        used = token_info.get("used")
        budget = token_info.get("budget")
        if isinstance(used, (int, float)) and isinstance(budget, (int, float)) and budget > 0:
            if used >= budget * token_budget_saturation_ratio:
                return True
    # Fallback: count-based saturation.
    return len(primary_result.models) > 120


def should_run_second_pass(
    primary_result: RetrievalResult,
    trigger: Any | None = None,
    *,
    sparse_threshold: int = SECOND_PASS_SPARSE_THRESHOLD,
    bridge_confidence_threshold: float = SECOND_PASS_BRIDGE_CONFIDENCE_THRESHOLD,
    t2_has_authoritative_handler: bool | None = None,
) -> SecondPassDecision:
    """
    Decide whether second-pass expansion is warranted on this primary
    result. Rules (in priority order):

    NON-ACTIVATION gates (checked first; short-circuit):
      - T2 with authoritative handler → NO second pass (deterministic
        resolution needs no expansion). Caller passes
        `t2_has_authoritative_handler=True` to force this branch; if
        the trigger carries a `model_id` and the trigger's own context
        signals an authoritative handler path, the caller handles that.
      - Token-budget saturation on the primary → NO second pass.

    ACTIVATION paths (any one fires):
      - Sparse result: len(primary.models) < sparse_threshold. Suggests
        `dependency_context` + `adjacent_commitments` to broaden.
      - High-confidence commitments with counterparty ref: at least one
        commitment in primary.acts.commitments has
        `external_counterparty_ref` set AND some bound Model's
        confidence >= bridge_confidence_threshold. Suggests
        `dependency_context` (Bridge spine expansion is valuable).
      - Anomaly-flagged items: any observation in primary has
        `kind == "anomaly_flagged"`. Suggests `supporting_evidence`.

    Returns a SecondPassDecision. The caller is responsible for logging
    it (the decision carries enough detail to do so); this function
    does NOT emit logs itself so tests can drive it hermetically.
    """
    notes: dict[str, Any] = {}

    # --- NON-ACTIVATION gates ---
    # T2 authoritative handler explicit override.
    if t2_has_authoritative_handler is True:
        return SecondPassDecision(
            run=False,
            trigger_condition="t2_authoritative_handler",
            reason_detail={"trigger_kind": getattr(trigger, "kind", None)},
        )
    # Token-budget saturation.
    if _context_is_saturated(primary_result):
        return SecondPassDecision(
            run=False,
            trigger_condition="token_budget_saturated",
            reason_detail={"model_count": len(primary_result.models)},
        )

    # --- ACTIVATION paths ---
    model_count = len(primary_result.models)
    notes["model_count"] = model_count
    notes["sparse_threshold"] = sparse_threshold

    # Sparse
    if model_count < sparse_threshold:
        return SecondPassDecision(
            run=True,
            trigger_condition="sparse_primary",
            suggested_dimensions=[
                "dependency_context",
                "adjacent_commitments",
            ],
            reason_detail=notes,
        )

    # Bridge-worthy (commitment with counterparty + high-confidence Model).
    commits = primary_result.acts.get("commitments", []) if primary_result.acts else []
    commits_with_ref: list[UUID] = []
    for c in commits:
        if getattr(c, "external_counterparty_ref", None) is not None:
            commits_with_ref.append(c.id)
    notes["commits_with_counterparty_ref"] = len(commits_with_ref)

    if commits_with_ref:
        # Look for a scoped Model with high confidence.
        high_conf_on_bridge = 0
        for m in primary_result.models:
            if m.confidence is None or float(m.confidence) < bridge_confidence_threshold:
                continue
            # Model scope overlaps at least one commitment with ref?
            scope = m.scope_entities or []
            for e in scope:
                if not isinstance(e, dict):
                    continue
                if e.get("type") in ("commitment",):
                    try:
                        if UUID(str(e.get("id"))) in commits_with_ref:
                            high_conf_on_bridge += 1
                            break
                    except (ValueError, TypeError):
                        continue
        notes["high_confidence_bridge_models"] = high_conf_on_bridge
        notes["bridge_confidence_threshold"] = bridge_confidence_threshold
        if high_conf_on_bridge >= 1:
            return SecondPassDecision(
                run=True,
                trigger_condition="high_confidence_commitment_with_counterparty",
                suggested_dimensions=["dependency_context"],
                reason_detail=notes,
            )

    # Anomaly-flagged observations.
    anomaly_obs = [
        o for o in primary_result.observations
        if getattr(o, "kind", None) == "anomaly_flagged"
    ]
    notes["anomaly_flagged_observations"] = len(anomaly_obs)
    if anomaly_obs:
        return SecondPassDecision(
            run=True,
            trigger_condition="anomaly_flagged",
            suggested_dimensions=["supporting_evidence", "dependency_context"],
            reason_detail=notes,
        )

    return SecondPassDecision(
        run=False,
        trigger_condition="no_activation_rule_matched",
        reason_detail=notes,
    )


def log_second_pass_decision(
    decision: SecondPassDecision,
    *,
    trigger: Any | None = None,
    tenant_id: UUID | None = None,
) -> None:
    """Emit a structured log event for a second-pass decision. Split
    out from `should_run_second_pass` so unit tests can drive the
    decision function without the logging side effect."""
    _log.info(
        "retrieval.second_pass_decision",
        run=decision.run,
        trigger_condition=decision.trigger_condition,
        suggested_dimensions=decision.suggested_dimensions,
        reason_detail=decision.reason_detail,
        trigger_kind=getattr(trigger, "kind", None),
        tenant_id=str(tenant_id) if tenant_id is not None else None,
    )


async def second_pass_expand(
    first_result: RetrievalResult,
    missing_dimensions: Sequence[str],
    conn: asyncpg.Connection,
    *,
    models_repo: ModelsRepo | None = None,
    max_hops: int = _SECOND_PASS_MAX_HOPS,
) -> RetrievalResult:
    """
    Expand first_result along the named dimensions and return a new
    RetrievalResult (the original is not mutated — this is a
    transformation, not a mutation).
    """
    if max_hops < 0 or max_hops > _SECOND_PASS_MAX_HOPS:
        raise ValueError(
            f"max_hops must be in [0, {_SECOND_PASS_MAX_HOPS}], got {max_hops}"
        )

    tenant_id = first_result.trigger.tenant_id

    # Seed sets from the first pass — never re-processed as new.
    original_model_ids: set[UUID] = {m.id for m in first_result.models}
    original_commit_ids: set[UUID] = {
        c.id for c in first_result.acts.get("commitments", [])
    }
    original_obs_ids: set[UUID] = {o.id for o in first_result.observations}

    # Accumulators for newly-discovered items.
    new_models: dict[UUID, ModelRow] = {}
    new_observations: dict[UUID, ObservationRow] = {}
    new_commitments: dict[UUID, CommitmentRow] = {}
    notes: dict[str, Any] = {
        "dimensions_requested": list(missing_dimensions),
        "dimensions_unknown": [],
        "dimensions_processed": [],
        "hops_used": {},
    }

    for dim in missing_dimensions:
        if dim not in _SUPPORTED_DIMENSIONS:
            notes["dimensions_unknown"].append(dim)
            _log.warning("second_pass.unknown_dimension", dimension=dim)
            continue

        if dim == "dependency_context":
            hops = await _expand_dependency_context(
                conn,
                tenant_id,
                original_commit_ids,
                original_model_ids,
                new_commitments,
                new_models,
                max_hops=max_hops,
            )
            notes["hops_used"][dim] = hops

        elif dim == "supporting_evidence":
            await _expand_supporting_evidence(
                conn,
                tenant_id,
                first_result.models,
                original_obs_ids,
                original_model_ids,
                new_observations,
                new_models,
            )
            notes["hops_used"][dim] = 1

        elif dim == "adjacent_commitments":
            await _expand_adjacent_commitments(
                conn,
                tenant_id,
                original_commit_ids,
                new_commitments,
                new_models,
                original_model_ids,
            )
            notes["hops_used"][dim] = 1

        notes["dimensions_processed"].append(dim)

    # Reconsolidate newly-surfaced Models. First-pass Models were
    # reconsolidated by primary_retrieve; do not re-bump them here
    # (would double-count activation on a second_pass call).
    reconsolidated_ids: list[UUID] = []
    if new_models:
        if models_repo is None:
            models_repo = ModelsRepo(pool=None)  # type: ignore[arg-type]
        updated = await models_repo.retrieve(list(new_models.keys()), conn=conn)
        for m in updated:
            new_models[m.id] = m
        reconsolidated_ids = [m.id for m in updated]

    # Compose the expanded result. We append (never rewrite): first
    # the first-pass items, then the new items in discovery order.
    merged_models = list(first_result.models) + [
        m for mid, m in new_models.items() if mid not in original_model_ids
    ]
    merged_observations = list(first_result.observations) + [
        o for oid, o in new_observations.items() if oid not in original_obs_ids
    ]
    merged_commits = list(first_result.acts.get("commitments", [])) + [
        c for cid, c in new_commitments.items() if cid not in original_commit_ids
    ]
    merged_acts = dict(first_result.acts)
    merged_acts["commitments"] = merged_commits

    expansion_notes = {
        **first_result.notes,
        "second_pass": notes,
        "second_pass_reconsolidated": len(reconsolidated_ids),
        "second_pass_new_models": len(new_models),
        "second_pass_new_observations": len(new_observations),
        "second_pass_new_commitments": len(new_commitments),
    }

    return RetrievalResult(
        trigger=first_result.trigger,
        observations=merged_observations,
        models=merged_models,
        acts=merged_acts,
        resources=list(first_result.resources),
        pathway_results=list(first_result.pathway_results),
        notes=expansion_notes,
        model_scores=dict(first_result.model_scores),
    )


# ---------------------------------------------------------------------
# Dimension-specific expansions
# ---------------------------------------------------------------------


async def _expand_dependency_context(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    original_commit_ids: set[UUID],
    original_model_ids: set[UUID],
    new_commitments: dict[UUID, CommitmentRow],
    new_models: dict[UUID, ModelRow],
    *,
    max_hops: int,
) -> int:
    """
    Follow depends_on transitively from the first-pass commitments,
    bi-directional. Stop at max_hops. Surface discovered Commitments +
    any Models scoped to them.
    """
    if not original_commit_ids:
        return 0

    visited: set[UUID] = set(original_commit_ids)
    frontier: set[UUID] = set(original_commit_ids)
    hops = 0

    for _ in range(max_hops):
        if not frontier:
            break
        rows = await conn.fetch(
            """
            SELECT dependency_commitment_id AS d,
                   dependent_commitment_id AS t
            FROM depends_on
            WHERE dependent_commitment_id = ANY($1::uuid[])
               OR dependency_commitment_id = ANY($1::uuid[])
            """,
            list(frontier),
        )
        next_frontier: set[UUID] = set()
        for r in rows:
            for cid in (r["d"], r["t"]):
                if cid is not None and cid not in visited:
                    visited.add(cid)
                    next_frontier.add(cid)
        hops += 1
        frontier = next_frontier

    discovered = visited - original_commit_ids
    if discovered:
        # Fetch the commitment rows.
        crs = await conn.fetch(
            """
            SELECT * FROM commitments
            WHERE id = ANY($1::uuid[]) AND tenant_id = $2
            """,
            list(discovered),
            tenant_id,
        )
        for cr in crs:
            cr_row = _hydrate_commitment(cr)
            new_commitments.setdefault(cr_row.id, cr_row)

        # Fetch Models scoped to them.
        scope_filters: list[str] = []
        params: list[Any] = [tenant_id]
        for cid in discovered:
            import json
            params.append(json.dumps([{"type": "commitment", "id": str(cid)}]))
            scope_filters.append(f"scope_entities @> ${len(params)}::jsonb")
        if scope_filters:
            sql = f"""
                SELECT {_MODEL_SELECT_SQL} FROM models
                WHERE tenant_id = $1
                  AND status = 'active'
                  AND ({' OR '.join(scope_filters)})
                ORDER BY activation DESC, created_at DESC
                LIMIT 200
            """
            model_rows = await conn.fetch(sql, *params)
            for r in model_rows:
                mid = r["id"]
                if mid in original_model_ids or mid in new_models:
                    continue
                new_models[mid] = _hydrate_model(r)

    return hops


async def _expand_supporting_evidence(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    first_pass_models: list[ModelRow],
    original_obs_ids: set[UUID],
    original_model_ids: set[UUID],
    new_observations: dict[UUID, ObservationRow],
    new_models: dict[UUID, ModelRow],
) -> None:
    """
    For each first-pass Model, fetch its supporting_event_ids
    (Observations) and supporting_model_ids (Models) — 1 hop beyond
    the first pass.
    """
    if not first_pass_models:
        return

    all_event_ids: set[UUID] = set()
    all_supporting_model_ids: set[UUID] = set()
    for m in first_pass_models:
        for eid in m.supporting_event_ids:
            if eid not in original_obs_ids:
                all_event_ids.add(eid)
        for smid in m.supporting_model_ids:
            if smid not in original_model_ids:
                all_supporting_model_ids.add(smid)

    if all_event_ids:
        obs_rows = await conn.fetch(
            f"""
            SELECT {_OBS_SELECT_SQL}
            FROM observations
            WHERE id = ANY($1::uuid[]) AND tenant_id = $2
            """,
            list(all_event_ids),
            tenant_id,
        )
        for r in obs_rows:
            oid = r["id"]
            new_observations.setdefault(oid, _hydrate_obs(r))

    if all_supporting_model_ids:
        rows = await conn.fetch(
            f"""
            SELECT {_MODEL_SELECT_SQL}
            FROM models
            WHERE id = ANY($1::uuid[])
              AND tenant_id = $2
              AND status = 'active'
            """,
            list(all_supporting_model_ids),
            tenant_id,
        )
        for r in rows:
            mid = r["id"]
            if mid in original_model_ids or mid in new_models:
                continue
            new_models[mid] = _hydrate_model(r)


async def _expand_adjacent_commitments(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    original_commit_ids: set[UUID],
    new_commitments: dict[UUID, CommitmentRow],
    new_models: dict[UUID, ModelRow],
    original_model_ids: set[UUID],
) -> None:
    """
    Find Commitments that contribute_to any of the same Goals as any
    first-pass Commitment. Exclude the first-pass Commitments
    themselves. Surface + their Models.
    """
    if not original_commit_ids:
        return

    # 1. Goals the first-pass commits contribute to.
    goal_rows = await conn.fetch(
        """
        SELECT DISTINCT goal_id FROM contributes_to
        WHERE commitment_id = ANY($1::uuid[])
        """,
        list(original_commit_ids),
    )
    goal_ids = {r["goal_id"] for r in goal_rows}
    if not goal_ids:
        return

    # 2. Sibling commits on those goals.
    sibling_rows = await conn.fetch(
        """
        SELECT DISTINCT commitment_id FROM contributes_to
        WHERE goal_id = ANY($1::uuid[])
        """,
        list(goal_ids),
    )
    siblings = {r["commitment_id"] for r in sibling_rows} - original_commit_ids
    if not siblings:
        return

    crs = await conn.fetch(
        """
        SELECT * FROM commitments
        WHERE id = ANY($1::uuid[]) AND tenant_id = $2
        """,
        list(siblings),
        tenant_id,
    )
    for cr in crs:
        cr_row = CommitmentRow.model_validate(dict(cr))
        new_commitments.setdefault(cr_row.id, cr_row)

    # 3. Models scoped to any sibling.
    scope_filters: list[str] = []
    params: list[Any] = [tenant_id]
    for cid in siblings:
        import json
        params.append(json.dumps([{"type": "commitment", "id": str(cid)}]))
        scope_filters.append(f"scope_entities @> ${len(params)}::jsonb")
    if scope_filters:
        sql = f"""
            SELECT {_MODEL_SELECT_SQL} FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND ({' OR '.join(scope_filters)})
            ORDER BY activation DESC, created_at DESC
            LIMIT 200
        """
        model_rows = await conn.fetch(sql, *params)
        for r in model_rows:
            mid = r["id"]
            if mid in original_model_ids or mid in new_models:
                continue
            new_models[mid] = _hydrate_model(r)


__all__ = [
    "second_pass_expand",
    "should_run_second_pass",
    "log_second_pass_decision",
    "SecondPassDecision",
    "SECOND_PASS_SPARSE_THRESHOLD",
    "SECOND_PASS_BRIDGE_CONFIDENCE_THRESHOLD",
]
