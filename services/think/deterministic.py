"""services/think/deterministic.py — deterministic trigger handlers.

Spec §7 "Authoritative vs inferential triggers":
  * T1 state_change → cascade handler
  * T2 prediction_overdue → resolution handler
  * T4 background_maintenance / entity_resolution_proposal → per-subkind
    deterministic handlers

All produce RawDiff. These paths do NOT call the LLM; they close the
loop cheaply for signals whose response is mechanically determinable.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from services.retrieval.assembler import ContextBundle
from services.retrieval.primary import TriggerContext

from .diff_schema import ClaimOp, RawDiff


# ---------------------------------------------------------------------
# Dispatch predicate
# ---------------------------------------------------------------------


def is_authoritative(trigger: TriggerContext) -> bool:
    """
    Spec §7 `is_authoritative`:

      T1 state_change       → True
      T2 prediction_overdue → True
      T4 background_maintenance / entity_resolution_proposal → True
      everything else       → False
    """
    if trigger.kind == "T1" and trigger.subkind == "state_change":
        return True
    if trigger.kind == "T2" and trigger.subkind in (
        "prediction_overdue", "prediction_deadline"
    ):
        return True
    if trigger.kind == "T4" and trigger.subkind in (
        "background_maintenance",
        "entity_resolution_proposal",
        "pattern_review",
    ):
        return True
    return False


# ---------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------


async def deterministic_handler(
    trigger: TriggerContext,
    bundle: ContextBundle,
    conn: asyncpg.Connection,
) -> RawDiff:
    """
    Dispatch to the per-subkind handler. Returns a RawDiff.
    """
    if trigger.kind == "T2":
        return await _handle_t2_prediction(trigger, bundle, conn)
    if trigger.kind == "T1" and trigger.subkind == "state_change":
        return await _handle_t1_state_change(trigger, bundle, conn)
    if trigger.kind == "T4":
        return await _handle_t4_background(trigger, bundle, conn)
    # Fallback: empty diff. Caller treats empty as no-op but still
    # records the trigger as applied (idempotency).
    return RawDiff(
        trigger_ref=_trigger_ref(trigger),
        tenant_id=trigger.tenant_id,
    )


# -----------------------------------------------------------------
# T2 — prediction resolution
# -----------------------------------------------------------------


async def _handle_t2_prediction(
    trigger: TriggerContext,
    bundle: ContextBundle,
    conn: asyncpg.Connection,
) -> RawDiff:
    """
    Resolve a prediction Model whose evaluate_at has passed.

    Simplified version of spec §7 `deterministic_handler_t2_prediction`:
      - Load the prediction.
      - If its falsifier is `prediction_deadline` with a `check`
        expression, evaluate the check against subsequent observations
        in the retrieval bundle. If the check matches any observation
        content, the falsifier did NOT trigger (prediction survives).
      - Otherwise produce a small confidence boost / drop.

    Wave 3-B scope: we do NOT try to parse arbitrary check expressions.
    We expect the falsifier to carry a `contradicting_state` or a
    machine-interpretable 'check' dict. Anything else → outcome=None
    (caller leaves Model untouched but records trigger applied).
    """
    model_id = trigger.model_id
    if model_id is None:
        return RawDiff(
            trigger_ref=_trigger_ref(trigger), tenant_id=trigger.tenant_id
        )

    row = await conn.fetchrow(
        """
        SELECT id, confidence, proposition_kind, falsifier,
               contributing_models, confirmed_count, contested_count,
               last_confirmed_at, resolution_outcome,
               confidence_at_assertion
        FROM models WHERE id = $1
        """,
        model_id,
    )
    if row is None:
        return RawDiff(
            trigger_ref=_trigger_ref(trigger), tenant_id=trigger.tenant_id
        )

    falsifier = row["falsifier"] or {}
    if isinstance(falsifier, (bytes, bytearray)):
        import json as _json
        falsifier = _json.loads(falsifier.decode())
    elif isinstance(falsifier, str):
        import json as _json
        try:
            falsifier = _json.loads(falsifier)
        except Exception:
            falsifier = {}

    outcome: bool | None = None
    if isinstance(falsifier, dict):
        fkind = falsifier.get("kind")
        if fkind == "commitment_outcome":
            # Does the referenced commitment sit in a contradicting_state?
            commitment_ref = falsifier.get("commitment_ref")
            contradicting = falsifier.get("contradicting_state")
            if commitment_ref and contradicting is not None:
                state = await conn.fetchval(
                    "SELECT state FROM commitments WHERE id = $1::uuid",
                    commitment_ref,
                )
                if state is not None:
                    # True == prediction survived (outcome confirmed).
                    if isinstance(contradicting, list):
                        outcome = state not in contradicting
                    else:
                        outcome = state != contradicting
        elif fkind == "prediction_deadline":
            # Simplistic: look for a supporting observation mentioning
            # the prediction's id in subsequent Obs content. This is
            # Wave-3-B; full parse-expression is Wave-5-B.
            ids_mentioned = [
                str(o.id) for o in bundle.observations
                if str(model_id) in (o.content_text or "")
            ]
            outcome = bool(ids_mentioned)

    new_confidence = float(row["confidence"])
    if outcome is True:
        delta = min(0.1, 0.95 - new_confidence)
    elif outcome is False:
        delta = -0.7 * new_confidence
    else:
        delta = 0.0

    new_confidence = _clip(new_confidence + delta)

    claim_ops: list[ClaimOp] = []
    changes: dict[str, Any] = {}
    if outcome is not None:
        changes["confidence"] = new_confidence
        changes["resolved_at"] = datetime.now(timezone.utc).isoformat()
        changes["resolution_outcome"] = bool(outcome)
        if outcome:
            changes["last_confirmed_at"] = datetime.now(timezone.utc).isoformat()
            changes["confirmed_count"] = int(row["confirmed_count"] or 0) + 1
        else:
            changes["contested_count"] = int(row["contested_count"] or 0) + 1
        claim_ops.append(
            ClaimOp(op="update", model_id=model_id, changes=changes)
        )

        # Contributing models — nudge per outcome.
        contributors = row["contributing_models"] or []
        for cid in contributors:
            c_row = await conn.fetchrow(
                "SELECT confidence FROM models WHERE id = $1", cid
            )
            if c_row is None:
                continue
            nudge = 0.03 if outcome else -0.05
            claim_ops.append(
                ClaimOp(
                    op="update",
                    model_id=cid,
                    changes={
                        "confidence": _clip(float(c_row["confidence"]) + nudge)
                    },
                )
            )

    return RawDiff(
        trigger_ref=_trigger_ref(trigger),
        tenant_id=trigger.tenant_id,
        claim_ops=claim_ops,
        act_ops=[],
        resource_ops=[],
        reasoning_trace=(
            f"T2 deterministic prediction resolution; outcome={outcome}"
        ),
    )


# -----------------------------------------------------------------
# T1 state_change — the cascade handler's own path
# -----------------------------------------------------------------


async def _handle_t1_state_change(
    trigger: TriggerContext,
    bundle: ContextBundle,
    conn: asyncpg.Connection,
) -> RawDiff:
    """
    state_change observations caused by apply itself route through here
    rather than through the LLM to prevent reasoning loops. In Wave
    3-B the primary cascade work happens INSIDE apply (via
    `services.think.cascade.cascade`), not through a re-issued T1 —
    so this handler intentionally returns an empty diff. The trigger
    is recorded as applied for idempotency.
    """
    return RawDiff(
        trigger_ref=_trigger_ref(trigger),
        tenant_id=trigger.tenant_id,
        reasoning_trace="T1 state_change handled by cascade engine; no diff",
    )


# -----------------------------------------------------------------
# T4 background maintenance
# -----------------------------------------------------------------


async def _handle_t4_background(
    trigger: TriggerContext,
    bundle: ContextBundle,
    conn: asyncpg.Connection,
) -> RawDiff:
    """
    T4 handler. Supports two subkinds end-to-end in Wave 3-B:

      * background_maintenance  — receive a proposal from Wave 3-A's
        maintenance worker (carried in trigger.seed_signature) and
        emit the corresponding archive / update op.
      * model_reeval            — receive a cause_model_id +
        cause_kind from the model_reeval_queue consumer and nudge the
        dependent Model's confidence.

    Other subkinds (`entity_resolution_proposal`, `pattern_review`)
    return empty RawDiff — those paths arrive with Wave 4-C.
    """
    claim_ops: list[ClaimOp] = []

    if trigger.subkind == "model_reeval":
        dependent_model_id = trigger.model_id
        cause_model_id: UUID | None = None
        cause_kind = "supporting_archived"
        if trigger.seed_signature:
            cmid = trigger.seed_signature.get("cause_model_id")
            if cmid is not None:
                try:
                    cause_model_id = UUID(str(cmid))
                except (ValueError, TypeError):
                    cause_model_id = None
            ck = trigger.seed_signature.get("cause_kind")
            if isinstance(ck, str):
                cause_kind = ck
        if dependent_model_id is not None:
            # Nudge confidence downward per cause_kind.
            nudge_map = {
                "supporting_archived": -0.05,
                "supporting_deprecated": -0.05,
                "supporting_superseded": -0.03,
                "contested_cluster": -0.10,
                "falsifier_triggered_upstream": -0.15,
            }
            nudge = nudge_map.get(cause_kind, -0.05)
            row = await conn.fetchrow(
                "SELECT confidence FROM models WHERE id = $1",
                dependent_model_id,
            )
            if row is not None:
                new_conf = _clip(float(row["confidence"]) + nudge)
                claim_ops.append(
                    ClaimOp(
                        op="update",
                        model_id=dependent_model_id,
                        changes={"confidence": new_conf},
                    )
                )

    if trigger.subkind == "background_maintenance":
        sig = trigger.seed_signature or {}
        action = sig.get("action")
        target = sig.get("model_id")
        if action == "suggest_archival" and target is not None:
            try:
                tmid = UUID(str(target))
            except (ValueError, TypeError):
                tmid = None
            if tmid is not None:
                claim_ops.append(
                    ClaimOp(op="archive", model_id=tmid, reason="decay")
                )

    if trigger.subkind == "pattern_review":
        # Wave 4-C: precipitation worker enqueued a candidate. We
        # promote it inline (insert the Pattern Model + flip
        # promoted_at). This branch does NOT go through the
        # claim_ops/apply path — the Pattern insertion is a
        # self-contained side effect tied to the candidate row, and
        # does not need the applier's validation envelope.
        sig = trigger.seed_signature or {}
        candidate_id_raw = sig.get("pattern_candidate_id")
        if candidate_id_raw is not None:
            try:
                candidate_id = UUID(str(candidate_id_raw))
            except (ValueError, TypeError):
                candidate_id = None
            if candidate_id is not None:
                from services.models.repo import ModelsRepo
                from services.workers.precipitation.proposer import (
                    promote_pattern_candidate,
                    reject_pattern_candidate,
                )
                # A Pattern Model requires a born_from_event_id. We
                # reuse the trigger's observation_id if present;
                # otherwise emit a lightweight state_change now.
                born_event = trigger.observation_id
                if born_event is None:
                    from services.observations.state_change import (
                        emit_state_change,
                    )
                    born_event = await emit_state_change(
                        conn,
                        kind="pattern_review_triggered",
                        entity_id=candidate_id,
                        entity_kind="pattern_candidate",
                        tenant_id=trigger.tenant_id,
                    )
                # ModelsRepo without a pool — every path it takes
                # uses the caller-supplied conn.
                repo = ModelsRepo(pool=None)
                try:
                    await promote_pattern_candidate(
                        conn,
                        candidate_id,
                        models_repo=repo,
                        born_from_event_id=born_event,
                    )
                except Exception as e:
                    # If promotion fails (e.g., constituent Models
                    # vanished between enqueue and promote), mark the
                    # candidate rejected so Think doesn't retry
                    # forever.
                    await reject_pattern_candidate(
                        conn,
                        candidate_id,
                        reason=f"promotion failed: {type(e).__name__}: {e}",
                    )

    return RawDiff(
        trigger_ref=_trigger_ref(trigger),
        tenant_id=trigger.tenant_id,
        claim_ops=claim_ops,
        act_ops=[],
        resource_ops=[],
        reasoning_trace=f"T4 deterministic handler; subkind={trigger.subkind}",
    )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _trigger_ref(trigger: TriggerContext) -> UUID:
    """
    Every Think run needs a stable trigger_ref for idempotency. For
    tests the TriggerContext may carry a `trigger_id` in
    seed_signature; otherwise we fall back to observation_id /
    model_id. Callers that need guaranteed stability pass
    seed_signature={'trigger_id': uuid}.
    """
    if trigger.seed_signature and "trigger_id" in trigger.seed_signature:
        try:
            return UUID(str(trigger.seed_signature["trigger_id"]))
        except (ValueError, TypeError):
            pass
    if trigger.observation_id is not None:
        return trigger.observation_id
    if trigger.model_id is not None:
        return trigger.model_id
    # Last resort: generate one. This makes tests that don't set
    # trigger_id behave sanely but loses idempotency — document it.
    from lib.shared.ids import uuid7
    return uuid7()


def _clip(v: float, lo: float = 0.05, hi: float = 0.95) -> float:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


__all__ = [
    "is_authoritative",
    "deterministic_handler",
]
