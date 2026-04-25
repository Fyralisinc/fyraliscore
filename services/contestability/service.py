"""
services/contestability/service.py — contest_model entry point.

Spec §11 "Direct contestation" flow, distilled into a single async
function that the Gateway's `POST /contest/{model_id}` route calls.

Transitions
-----------
1. Standing check (services.contestability.standing). No standing → NoStandingError.
2. Insert a `contestation` Observation with `trust_tier='authoritative'`
   (first-person override per spec §11) whose content carries
   `contested_model_id`, `reason`, optional `proposed_alternative`,
   and `contestation_kind` ('belief' | 'reading').
3. Increment `models.contested_count`.
4. Apply first-person override if applicable (primary 0.3x, secondary
   0.5x, floor 0.15). Writes a `model_status_notes` row with kind
   `first_person_override`.
5. Enqueue a T3 trigger for Think (trigger_subkind =
   'belief_contestation' or 'reading_contestation'; payload includes
   observation_id, model_id, contestor_actor_id).
6. For 'reading' contestation, also update the Model's
   `signal_readings` array to mark the contesting actor's entry
   (inserting one if absent) with `contested: true`.

Returns `ContestationResult`.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.ids import uuid7

from services.contestability.standing import (
    StandingBasis,
    actor_has_standing_on_model,
)
from services.models.status_notes import add_note


ContestationKind = Literal["belief", "reading"]


# ---------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------


class ContestationError(CompanyOSError):
    default_code = "contestation_error"


class NoStandingError(ContestationError):
    default_code = "no_standing"


# ---------------------------------------------------------------------
# Inputs / outputs
# ---------------------------------------------------------------------


@dataclass
class ContestationInput:
    model_id: UUID
    contestor_actor_id: UUID
    tenant_id: UUID
    contestation_kind: ContestationKind
    rationale: str
    proposed_alternative: dict[str, Any] | None = None


@dataclass
class ContestationResult:
    observation_id: UUID
    trigger_id: UUID | None
    new_confidence: float
    previous_confidence: float
    standing_basis: StandingBasis | None
    override_applied: bool


# ---------------------------------------------------------------------
# Weights — spec §11 "First-person override rule" verbatim.
# ---------------------------------------------------------------------

PRIMARY_SUBJECT_MULTIPLIER = 0.3
SECONDARY_SUBJECT_MULTIPLIER = 0.5
OVERRIDE_FLOOR = 0.15


# ---------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------


async def contest_model(
    conn: asyncpg.Connection,
    inp: ContestationInput,
) -> ContestationResult:
    """
    Execute the contestation flow against `conn` (callers wrap in
    a transaction).
    """
    if inp.contestation_kind not in ("belief", "reading"):
        raise ValidationError(
            f"contestation_kind must be 'belief' or 'reading'; "
            f"got {inp.contestation_kind!r}",
            field="contestation_kind",
            value=inp.contestation_kind,
        )
    if not isinstance(inp.rationale, str) or not inp.rationale.strip():
        raise ValidationError(
            "rationale is required and must be non-empty",
            field="rationale",
        )

    # -- 1. Standing check ------------------------------------------
    standing = await actor_has_standing_on_model(
        conn,
        actor_id=inp.contestor_actor_id,
        model_id=inp.model_id,
    )
    if not standing.granted:
        raise NoStandingError(
            f"actor {inp.contestor_actor_id} has no standing on model {inp.model_id}",
            actor_id=str(inp.contestor_actor_id),
            model_id=str(inp.model_id),
        )

    # -- Pull Model snapshot (we need scope_actors[] for override logic) --
    model = await conn.fetchrow(
        """
        SELECT id, tenant_id, scope_actors, confidence, signal_readings,
               reading_contestable
        FROM models
        WHERE id = $1
        """,
        inp.model_id,
    )
    if model is None:
        raise ValidationError(
            f"model {inp.model_id} does not exist",
            model_id=str(inp.model_id),
        )
    if model["tenant_id"] != inp.tenant_id:
        raise ValidationError(
            "tenant mismatch: model belongs to a different tenant",
            model_tenant_id=str(model["tenant_id"]),
            request_tenant_id=str(inp.tenant_id),
        )

    previous_confidence = float(model["confidence"])
    scope_actors: list[UUID] = list(model["scope_actors"] or [])

    # -- 2. Insert contestation Observation -------------------------
    obs_id = uuid7()
    now = datetime.now(timezone.utc)
    content: dict[str, Any] = {
        "contested_model_id": str(inp.model_id),
        "contestation_kind": inp.contestation_kind,
        "reason": inp.rationale,
    }
    if inp.proposed_alternative is not None:
        content["proposed_alternative"] = inp.proposed_alternative

    content_text = (
        f"contestation ({inp.contestation_kind}) of model "
        f"{inp.model_id} by actor {inp.contestor_actor_id}: "
        f"{inp.rationale[:200]}"
    )
    entities_mentioned = [{"type": "model", "id": str(inp.model_id)}]

    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, ingested_at, kind,
            source_channel, source_actor_ref, actor_id,
            content, content_text,
            embedding, embedding_pending,
            trust_tier, external_id, cause_id, entities_mentioned
        ) VALUES (
            $1, $2, $3, $3, 'contestation',
            'ui:contestation', NULL, $4,
            $5::jsonb, $6,
            NULL, FALSE,
            'authoritative', NULL, NULL, $7::jsonb
        )
        """,
        obs_id,
        inp.tenant_id,
        now,
        inp.contestor_actor_id,
        json.dumps(content, sort_keys=True),
        content_text,
        json.dumps(entities_mentioned),
    )

    # -- 3. Increment contested_count -------------------------------
    await conn.execute(
        "UPDATE models SET contested_count = contested_count + 1 WHERE id = $1",
        inp.model_id,
    )

    # -- 4. First-person override (belief kind only) ----------------
    new_confidence = previous_confidence
    override_applied = False
    if inp.contestation_kind == "belief" and inp.contestor_actor_id in scope_actors:
        if scope_actors and inp.contestor_actor_id == scope_actors[0]:
            multiplier = PRIMARY_SUBJECT_MULTIPLIER
            role = "primary"
        else:
            multiplier = SECONDARY_SUBJECT_MULTIPLIER
            role = "secondary"
        new_confidence = max(OVERRIDE_FLOOR, previous_confidence * multiplier)
        # Apply the confidence change. Clip to [0.05, 0.95] to satisfy
        # the CHECK constraint (max() already gives us >= floor >=
        # 0.05; clip against the upper bound explicitly).
        new_confidence = min(new_confidence, 0.95)
        await conn.execute(
            "UPDATE models SET confidence = $1 WHERE id = $2",
            new_confidence, inp.model_id,
        )
        await add_note(
            model_id=inp.model_id,
            note=(
                f"first-person override ({role}) by actor "
                f"{inp.contestor_actor_id}: {inp.rationale[:200]}"
            ),
            kind="first_person_override",
            authored_by=inp.contestor_actor_id,
            conn=conn,
        )
        override_applied = True

    # -- Reading contestation: mark signal_readings entry -----------
    if inp.contestation_kind == "reading":
        existing = model["signal_readings"]
        if isinstance(existing, (bytes, bytearray)):
            existing = json.loads(existing.decode())
        elif isinstance(existing, str):
            existing = json.loads(existing)
        if not isinstance(existing, list):
            existing = []
        # Find the entry whose actor_id matches the contestor; insert
        # one if absent.
        updated = False
        for entry in existing:
            if not isinstance(entry, dict):
                continue
            if entry.get("actor_id") == str(inp.contestor_actor_id):
                entry["contested"] = True
                entry["contested_at"] = now.isoformat()
                entry["rationale"] = inp.rationale
                updated = True
                break
        if not updated:
            existing.append({
                "actor_id": str(inp.contestor_actor_id),
                "contested": True,
                "contested_at": now.isoformat(),
                "rationale": inp.rationale,
            })
        await conn.execute(
            "UPDATE models SET signal_readings = $1::jsonb WHERE id = $2",
            json.dumps(existing),
            inp.model_id,
        )
        await add_note(
            model_id=inp.model_id,
            note=(
                f"reading contestation by actor {inp.contestor_actor_id}: "
                f"{inp.rationale[:200]}"
            ),
            kind="first_person_override",
            authored_by=inp.contestor_actor_id,
            conn=conn,
        )

    # -- 5. Enqueue T3 trigger --------------------------------------
    trig_subkind = (
        "belief_contestation"
        if inp.contestation_kind == "belief"
        else "reading_contestation"
    )
    trig_id = uuid7()
    await conn.execute(
        """
        INSERT INTO think_trigger_queue (
            id, tenant_id, trigger_kind, trigger_subkind,
            observation_id, model_id, payload
        ) VALUES ($1, $2, 'T3', $3, $4, $5, $6::jsonb)
        """,
        trig_id,
        inp.tenant_id,
        trig_subkind,
        obs_id,
        inp.model_id,
        json.dumps({
            "contestor_actor_id": str(inp.contestor_actor_id),
            "contestation_kind": inp.contestation_kind,
        }),
    )

    return ContestationResult(
        observation_id=obs_id,
        trigger_id=trig_id,
        new_confidence=new_confidence,
        previous_confidence=previous_confidence,
        standing_basis=standing.basis,
        override_applied=override_applied,
    )


__all__ = [
    "ContestationInput",
    "ContestationResult",
    "ContestationError",
    "NoStandingError",
    "ContestationKind",
    "contest_model",
    "PRIMARY_SUBJECT_MULTIPLIER",
    "SECONDARY_SUBJECT_MULTIPLIER",
    "OVERRIDE_FLOOR",
]
