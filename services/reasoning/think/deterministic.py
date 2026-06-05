"""services/reasoning/think/deterministic.py — deterministic trigger handlers.

Spec §7 "Authoritative vs inferential triggers":
  * T1 state_change → cascade handler
  * T2 prediction_overdue / prediction_deadline → resolution handler
  * T2 belief_updated → deterministic no-op unless it has prediction shape
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

from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext

from .diff_schema import ClaimOp, RawDiff


# ---------------------------------------------------------------------
# Dispatch predicate
# ---------------------------------------------------------------------


_T2_HYPOTHESIS_RATIFICATION_SUBKINDS: frozenset[str] = frozenset({
    "hypothesis_approved",
    "hypothesis_corrected",
    "hypothesis_other",
})


def is_authoritative(trigger: TriggerContext) -> bool:
    """
    Spec §7 `is_authoritative`:

      T1 state_change       → True
      T2 prediction/belief_updated → True
      T2 hypothesis_{approved,corrected,other} → True
                            (imaginary-node ratification path)
      T3 missing_transition → True (imaginary-node detection path)
      T4 background_maintenance / entity_resolution_proposal → True
      everything else       → False
    """
    if trigger.kind == "T1" and trigger.subkind == "state_change":
        return True
    if trigger.kind == "T2" and trigger.subkind in (
        "belief_updated", "prediction_overdue", "prediction_deadline"
    ):
        return True
    if (
        trigger.kind == "T2"
        and trigger.subkind in _T2_HYPOTHESIS_RATIFICATION_SUBKINDS
    ):
        return True
    if trigger.kind == "T3" and trigger.subkind == "missing_transition":
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
    if (
        trigger.kind == "T2"
        and trigger.subkind in _T2_HYPOTHESIS_RATIFICATION_SUBKINDS
    ):
        return await _handle_t2_hypothesis_ratification(trigger, bundle, conn)
    if trigger.kind == "T2":
        return await _handle_t2_prediction(trigger, bundle, conn)
    if trigger.kind == "T1" and trigger.subkind == "state_change":
        return await _handle_t1_state_change(trigger, bundle, conn)
    if trigger.kind == "T3" and trigger.subkind == "missing_transition":
        return await _handle_t3_missing_transition(trigger, bundle, conn)
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
    `services.reasoning.think.cascade.cascade`), not through a re-issued T1 —
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
        cause_kind = "supporting_archived"
        if trigger.seed_signature:
            ck = trigger.seed_signature.get("cause_kind")
            if isinstance(ck, str):
                cause_kind = ck
        if dependent_model_id is not None:
            # Nudge confidence downward per cause_kind.
            #
            # Pre-S1 five-value taxonomy (preserved exactly):
            #   supporting_archived/deprecated/superseded — direct
            #     supporter went away; mild-to-moderate nudge.
            #   contested_cluster — a contesting cluster fired; stronger.
            #   falsifier_triggered_upstream — upstream falsifier hit;
            #     strongest standard nudge.
            #
            # S1 (migration 0031) widens the map for cause_kinds
            # produced by registry-driven edge cascades:
            #   contributor_archived — a contributing_to_resolution
            #     supporter (T2 prediction resolver) was archived.
            #     Treated similarly to supporting_archived.
            #   pattern_archived — the pattern this Model is an
            #     instance of was archived. Loses categorization;
            #     moderate nudge.
            #   instance_archived — one instance among many of a
            #     pattern was archived. Pattern's evidence base
            #     shrinks slightly; mild nudge.
            #
            # The pre-S1 CHECK on model_reeval_queue.cause_kind was
            # dropped in migration 0031 because cause_kinds are now
            # declarative (registry-owned). The default fallback of
            # -0.05 means an unknown cause_kind still produces a
            # safe small nudge; never silently drops the re-eval.
            nudge_map = {
                "supporting_archived": -0.05,
                "supporting_deprecated": -0.05,
                "supporting_superseded": -0.03,
                "contested_cluster": -0.10,
                "falsifier_triggered_upstream": -0.15,
                # S1 additions (registry-driven cascades):
                "contributor_archived": -0.05,
                "pattern_archived": -0.07,
                "instance_archived": -0.02,
                "counterevidence_archived": 0.05,
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
                from services.domain.models.repo import ModelsRepo
                from services.workers.precipitation.proposer import (
                    promote_pattern_candidate,
                    reject_pattern_candidate,
                )
                # A Pattern Model requires a born_from_event_id. We
                # reuse the trigger's observation_id if present;
                # otherwise emit a lightweight state_change now.
                born_event = trigger.observation_id
                if born_event is None:
                    from services.domain.observations.state_change import (
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
                    async with conn.transaction():
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
                    try:
                        async with conn.transaction():
                            await reject_pattern_candidate(
                                conn,
                                candidate_id,
                                reason=(
                                    "promotion failed: "
                                    f"{type(e).__name__}: {e}"
                                ),
                            )
                    except Exception:
                        pass

    return RawDiff(
        trigger_ref=_trigger_ref(trigger),
        tenant_id=trigger.tenant_id,
        claim_ops=claim_ops,
        act_ops=[],
        resource_ops=[],
        reasoning_trace=f"T4 deterministic handler; subkind={trigger.subkind}",
    )


# -----------------------------------------------------------------
# T2 hypothesis ratification — imaginary-node Approve / Correct / Other
# -----------------------------------------------------------------

# User-ratified hypotheses get a confidence floor and ceiling well below
# the directly-observed band ([0.85, 0.95]). The user is reconstructing,
# not observing — so even a confident "yes" caps lower than a fresh
# observation. The ceiling is the load-bearing invariant on this lineage.
USER_RATIFIED_HYPOTHESIS_CONFIDENCE: float = 0.65
USER_RATIFIED_HYPOTHESIS_CONFIDENCE_CEILING: float = 0.70

# User-corrected fact-Models — the user wrote the new claim, so it's
# slightly more authoritative than a system-imputed-then-approved one.
USER_CORRECTED_FACT_CONFIDENCE: float = 0.65
USER_CORRECTED_FACT_CONFIDENCE_CEILING: float = 0.75


async def _handle_t2_hypothesis_ratification(
    trigger: TriggerContext,
    bundle: ContextBundle,
    conn: asyncpg.Connection,
) -> RawDiff:
    """Dispatch the three Think-routed ratification actions to their
    per-subkind handlers."""
    if trigger.subkind == "hypothesis_approved":
        return await _handle_t2_hypothesis_approved(trigger, bundle, conn)
    if trigger.subkind == "hypothesis_corrected":
        return await _handle_t2_hypothesis_corrected(trigger, bundle, conn)
    if trigger.subkind == "hypothesis_other":
        return await _handle_t2_hypothesis_other(trigger, bundle, conn)
    return RawDiff(
        trigger_ref=_trigger_ref(trigger),
        tenant_id=trigger.tenant_id,
        reasoning_trace=(
            f"unrecognized T2 hypothesis subkind {trigger.subkind!r}; "
            "no-op"
        ),
    )


async def _handle_t2_hypothesis_approved(
    trigger: TriggerContext,
    bundle: ContextBundle,
    conn: asyncpg.Connection,
) -> RawDiff:
    """User confirmed the system's hypothesis. Bump confidence into the
    user-ratified band, add a ratification signal_readings entry, and
    increment confirmed_count. The Model stays a hypothesis (the user's
    "yes" is testimony, not a fresh observation) — we don't try to flip
    claim_role because `proposition` isn't in the applier's allowed-
    update columns."""
    model_id = trigger.model_id
    if model_id is None:
        return _empty_diff(trigger, "no model_id")

    row = await conn.fetchrow(
        """
        SELECT id, confidence, confirmed_count, signal_readings,
               claim_role, status
        FROM models
        WHERE id = $1 AND tenant_id = $2
        """,
        model_id, trigger.tenant_id,
    )
    if row is None:
        return _empty_diff(trigger, f"hypothesis {model_id} not found")
    if row["status"] != "active":
        return _empty_diff(
            trigger, f"hypothesis {model_id} already {row['status']}"
        )
    if row["claim_role"] != "hypothesis":
        return _empty_diff(
            trigger,
            f"model {model_id} is claim_role={row['claim_role']!r}, "
            "not a hypothesis",
        )

    current_conf = float(row["confidence"])
    # Idempotent boost: bring to band, but never decrease. If a previous
    # approval already pushed us above the target, leave it alone.
    new_conf = _clip(
        max(current_conf, USER_RATIFIED_HYPOTHESIS_CONFIDENCE),
        lo=0.05,
        hi=USER_RATIFIED_HYPOTHESIS_CONFIDENCE_CEILING,
    )

    sig = trigger.seed_signature or {}
    actor_id = _safe_uuid(sig.get("actor_id"))

    # Build a new signal_readings entry. Existing readings are preserved.
    import json as _json
    existing_raw = row["signal_readings"]
    if isinstance(existing_raw, (bytes, bytearray)):
        try:
            existing = _json.loads(existing_raw.decode())
        except Exception:
            existing = []
    elif isinstance(existing_raw, str):
        try:
            existing = _json.loads(existing_raw)
        except Exception:
            existing = []
    elif isinstance(existing_raw, list):
        existing = existing_raw
    else:
        existing = []
    if not isinstance(existing, list):
        existing = []
    existing = [r for r in existing if isinstance(r, dict)]

    existing.append({
        "kind": "ratification",
        "ratification_kind": "hypothesis_approved",
        "actor_id": str(actor_id) if actor_id else None,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "claim": "user approved",
    })

    changes: dict[str, Any] = {
        "confidence": new_conf,
        "signal_readings": existing,
        "confirmed_count": int(row["confirmed_count"] or 0) + 1,
        "last_confirmed_at": datetime.now(timezone.utc).isoformat(),
    }
    claim_op = ClaimOp(op="update", model_id=model_id, changes=changes)
    return RawDiff(
        trigger_ref=_trigger_ref(trigger),
        tenant_id=trigger.tenant_id,
        claim_ops=[claim_op],
        reasoning_trace=(
            f"T2 hypothesis_approved: model {model_id} ratified by "
            f"actor {actor_id}; confidence {current_conf:.3f} → {new_conf:.3f}"
        ),
    )


async def _handle_t2_hypothesis_corrected(
    trigger: TriggerContext,
    bundle: ContextBundle,
    conn: asyncpg.Connection,
) -> RawDiff:
    """User rejected the hypothesis and supplied a correction. Archive
    the hypothesis Model and insert a new fact-Model carrying the
    user's claim, with `was_system_hypothesis=True` provenance so the
    lineage is preserved."""
    model_id = trigger.model_id
    if model_id is None:
        return _empty_diff(trigger, "no model_id")

    sig = trigger.seed_signature or {}
    correction = sig.get("correction")
    if not isinstance(correction, dict) or not correction.get("natural"):
        return _empty_diff(
            trigger,
            "T2 hypothesis_corrected: missing correction.natural in payload",
        )

    row = await conn.fetchrow(
        """
        SELECT id, claim_role, status, scope_actors, scope_entities,
               scope_temporal, supporting_model_ids, born_from_event_id,
               proposition
        FROM models
        WHERE id = $1 AND tenant_id = $2
        """,
        model_id, trigger.tenant_id,
    )
    if row is None:
        return _empty_diff(trigger, f"hypothesis {model_id} not found")
    if row["status"] != "active":
        return _empty_diff(
            trigger, f"hypothesis {model_id} already {row['status']}"
        )
    if row["claim_role"] != "hypothesis":
        return _empty_diff(
            trigger,
            f"model {model_id} is claim_role={row['claim_role']!r}, "
            "not a hypothesis",
        )

    actor_id = _safe_uuid(sig.get("actor_id"))
    captured_obs_id = _safe_uuid(sig.get("captured_observation_id"))
    # Born_from_event_id for the new fact-Model: the user's correction
    # observation if present, else the ratification observation. Either
    # is a legitimate cause for the substrate's audit chain.
    born_event = captured_obs_id or trigger.observation_id
    if born_event is None:
        return _empty_diff(
            trigger,
            "T2 hypothesis_corrected: no usable born_from_event_id",
        )

    overrides = correction.get("proposition_overrides") or {}
    natural_text = str(correction["natural"]).strip()[:2000]
    proposition: dict[str, Any] = {
        "kind": "belief",
        "assertion": natural_text,
        "was_system_hypothesis": True,
        "lineage": {
            "source_hypothesis_id": str(model_id),
            "correction_actor_id": str(actor_id) if actor_id else None,
            "correction_observation_id": (
                str(captured_obs_id) if captured_obs_id else None
            ),
        },
    }
    if isinstance(overrides, dict):
        for k, v in overrides.items():
            if k in ("kind", "lineage"):
                # Don't let user payload subvert the kind or lineage
                # provenance fields.
                continue
            proposition[k] = v

    falsifier = {
        "kind": "observation_pattern",
        "pattern": (
            "Explicit ingestion evidence contradicting the user-supplied "
            f"correction for hypothesis {model_id}."
        ),
        "within_window": "30d",
    }

    fact_entry: dict[str, Any] = {
        "proposition": proposition,
        "natural": natural_text,
        "confidence": USER_CORRECTED_FACT_CONFIDENCE,
        "confidence_at_assertion": USER_CORRECTED_FACT_CONFIDENCE,
        "falsifier": falsifier,
        "scope_actors": list(row["scope_actors"] or []),
        "scope_entities": _coerce_jsonb_list(row["scope_entities"]),
        "scope_temporal": _coerce_jsonb_dict(row["scope_temporal"]),
        "supporting_event_ids": [born_event],
        "supporting_model_ids": [model_id] + list(
            row["supporting_model_ids"] or []
        ),
        "born_from_event_id": born_event,
    }

    return RawDiff(
        trigger_ref=_trigger_ref(trigger),
        tenant_id=trigger.tenant_id,
        claim_ops=[
            ClaimOp(
                op="archive",
                model_id=model_id,
                reason="hypothesis_user_corrected",
            ),
            ClaimOp(op="insert", entry=fact_entry),
        ],
        reasoning_trace=(
            f"T2 hypothesis_corrected: archive hypothesis {model_id}; "
            f"insert user-corrected fact-Model with "
            f"confidence={USER_CORRECTED_FACT_CONFIDENCE:.3f}"
        ),
    )


async def _handle_t2_hypothesis_other(
    trigger: TriggerContext,
    bundle: ContextBundle,
    conn: asyncpg.Connection,
) -> RawDiff:
    """User said "something else happened" and provided free-form
    context. The ratify handler already captured the explanation as an
    observation; here we just archive the hypothesis. Future Think runs
    can ingest the explanation observation and synthesize structured
    claims from it independently."""
    model_id = trigger.model_id
    if model_id is None:
        return _empty_diff(trigger, "no model_id")

    row = await conn.fetchrow(
        "SELECT claim_role, status FROM models "
        "WHERE id = $1 AND tenant_id = $2",
        model_id, trigger.tenant_id,
    )
    if row is None:
        return _empty_diff(trigger, f"hypothesis {model_id} not found")
    if row["status"] != "active":
        return _empty_diff(
            trigger, f"hypothesis {model_id} already {row['status']}"
        )
    if row["claim_role"] != "hypothesis":
        return _empty_diff(
            trigger,
            f"model {model_id} is claim_role={row['claim_role']!r}, "
            "not a hypothesis",
        )

    return RawDiff(
        trigger_ref=_trigger_ref(trigger),
        tenant_id=trigger.tenant_id,
        claim_ops=[
            ClaimOp(
                op="archive",
                model_id=model_id,
                reason="hypothesis_user_other",
            ),
        ],
        reasoning_trace=(
            f"T2 hypothesis_other: archive hypothesis {model_id}; "
            "explanation captured as observation by ratify handler"
        ),
    )


def _empty_diff(trigger: TriggerContext, why: str) -> RawDiff:
    return RawDiff(
        trigger_ref=_trigger_ref(trigger),
        tenant_id=trigger.tenant_id,
        reasoning_trace=why,
    )


def _safe_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (ValueError, TypeError):
        return None


# -----------------------------------------------------------------
# T3 missing_transition — imaginary-node hypothesis imputation
# -----------------------------------------------------------------


async def _handle_t3_missing_transition(
    trigger: TriggerContext,
    bundle: ContextBundle,
    conn: asyncpg.Connection,
) -> RawDiff:
    """Imaginary-node pattern: synthesize a low-confidence hypothesis
    Model that explains a detected substrate state-jump.

    Steps:
      1. Load source Model snapshot.
      2. Re-derive the discontinuity from the audit chain (the trigger
         payload carries `prev_event_id` but state may have shifted
         between enqueue and processing — fetch fresh).
      3. Run the deterministic imputer.
      4. Emit a `missing_transition_detected` state_change observation
         to provide `born_from_event_id` for the hypothesis Model.
      5. Return a RawDiff with one ClaimOp(op='insert').

    Idempotency: if the discontinuity has resolved between trigger
    enqueue and processing (e.g., a corrective audit event landed),
    `fetch_missing_transition_discontinuity` returns None and we
    return an empty RawDiff. The applier records the trigger as
    applied either way.

    Reconciliation: if a similar hypothesis Model already exists
    (cosine ≥ 0.85), the reconciler converts the insert into a
    confidence-update / new-confirmation reading — the same path that
    keeps the substrate clean for any other Model insert.
    """
    from datetime import datetime, timedelta, timezone

    from services.reasoning.dynamics import (
        fetch_missing_transition_discontinuity,
    )
    from services.reasoning.dynamics.hypothesis_imputer import (
        SourceModelSnapshot,
        impute_hypothesis,
    )
    from services.domain.observations.state_change import emit_state_change

    model_id = trigger.model_id
    if model_id is None:
        return RawDiff(
            trigger_ref=_trigger_ref(trigger),
            tenant_id=trigger.tenant_id,
            reasoning_trace=(
                "T3 missing_transition: trigger missing model_id; no-op"
            ),
        )

    src_row = await conn.fetchrow(
        """
        SELECT id, "natural" AS natural,
               scope_actors, scope_entities, scope_temporal,
               status
        FROM models
        WHERE id = $1 AND tenant_id = $2
        """,
        model_id,
        trigger.tenant_id,
    )
    if src_row is None:
        return RawDiff(
            trigger_ref=_trigger_ref(trigger),
            tenant_id=trigger.tenant_id,
            reasoning_trace=(
                f"T3 missing_transition: source model {model_id} not found"
            ),
        )
    if src_row["status"] != "active":
        # Discontinuities on archived Models are settled by definition;
        # don't fork off a hypothesis that nobody will ratify.
        return RawDiff(
            trigger_ref=_trigger_ref(trigger),
            tenant_id=trigger.tenant_id,
            reasoning_trace=(
                f"T3 missing_transition: source model {model_id} is "
                f"{src_row['status']}; no-op"
            ),
        )

    # The trigger payload's `prev_event_occurred_at` (set by the emitter)
    # gives us a tight lookback. If absent, fall back to 30 days.
    lookback_days = 30
    if isinstance(trigger.region_spec, dict):
        prev_iso = trigger.region_spec.get("prev_event_occurred_at")
        if isinstance(prev_iso, str):
            try:
                prev_dt = datetime.fromisoformat(prev_iso.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                lookback_days = max(
                    7,
                    int((now - prev_dt).total_seconds() / 86400.0) + 2,
                )
            except ValueError:
                pass

    discontinuity = await fetch_missing_transition_discontinuity(
        conn,
        tenant_id=trigger.tenant_id,
        model_id=model_id,
        since=datetime.now(timezone.utc) - timedelta(days=lookback_days),
    )
    if discontinuity is None:
        # Resolved between enqueue and processing — idempotent no-op.
        return RawDiff(
            trigger_ref=_trigger_ref(trigger),
            tenant_id=trigger.tenant_id,
            reasoning_trace=(
                f"T3 missing_transition: discontinuity for model {model_id} "
                "resolved before processing; no-op"
            ),
        )

    source = SourceModelSnapshot(
        model_id=src_row["id"],
        natural=src_row["natural"] or "",
        scope_actors=list(src_row["scope_actors"] or []),
        scope_entities=_coerce_jsonb_list(src_row["scope_entities"]),
        scope_temporal=_coerce_jsonb_dict(src_row["scope_temporal"]),
    )
    imputed = impute_hypothesis(discontinuity, source)

    born_event_id = await emit_state_change(
        conn,
        kind="missing_transition_detected",
        entity_id=model_id,
        tenant_id=trigger.tenant_id,
        entity_kind="model",
        metadata={
            "prev_event_id": discontinuity.prev_event_id,
            "next_event_id": discontinuity.next_event_id,
            "differing_fields": list(discontinuity.differing_fields),
            "gap_seconds": discontinuity.gap_seconds,
            "imputer_source": imputed.proposition.get(
                "imputation_source", "missing_transition_detector_v1"
            ),
        },
    )

    entry = imputed.to_claim_op_entry(born_from_event_id=born_event_id)
    claim_op = ClaimOp(op="insert", entry=entry)
    return RawDiff(
        trigger_ref=_trigger_ref(trigger),
        tenant_id=trigger.tenant_id,
        claim_ops=[claim_op],
        reasoning_trace=(
            f"T3 missing_transition: hypothesized intermediate state for "
            f"model {model_id} across "
            f"{discontinuity.prev_event_occurred_at.isoformat()}..."
            f"{discontinuity.next_event_occurred_at.isoformat()} "
            f"(fields={list(discontinuity.differing_fields)}); "
            f"confidence={imputed.confidence:.3f}"
        ),
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


def _coerce_jsonb_dict(value: Any) -> dict[str, Any]:
    """asyncpg may return JSONB columns as dict, bytes, or str depending
    on codec installation. Normalize to dict."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode()
        except UnicodeDecodeError:
            return {}
    if isinstance(value, str):
        import json
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _coerce_jsonb_list(value: Any) -> list[dict[str, Any]]:
    """Counterpart of `_coerce_jsonb_dict` for JSONB columns that hold
    arrays of objects (e.g. `models.scope_entities`)."""
    if value is None:
        return []
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode()
        except UnicodeDecodeError:
            return []
    if isinstance(value, str):
        import json
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [v for v in parsed if isinstance(v, dict)]
    return []


__all__ = [
    "is_authoritative",
    "deterministic_handler",
]
