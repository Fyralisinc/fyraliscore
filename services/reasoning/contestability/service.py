"""
services/reasoning/contestability/service.py — contest_model entry point.

Spec §11 "Direct contestation" flow, distilled into a single async
function that the Gateway's `POST /contest/{model_id}` route calls.

Transitions
-----------
1. Standing check (services.reasoning.contestability.standing). No standing → NoStandingError.
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

from services.domain.models.status_notes import add_note
from services.domain.triggers import enqueue_trigger
from services.reasoning.contestability.standing import (
    StandingBasis,
    actor_has_standing_on_model,
)


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


@dataclass(frozen=True)
class _ModelSnapshot:
    tenant_id: UUID
    previous_confidence: float
    scope_actors: list[UUID]
    signal_readings: Any


@dataclass(frozen=True)
class _OverrideResult:
    new_confidence: float
    applied: bool


# ---------------------------------------------------------------------
# Weights — spec §11 "First-person override rule" verbatim.
# ---------------------------------------------------------------------

PRIMARY_SUBJECT_MULTIPLIER = 0.3
SECONDARY_SUBJECT_MULTIPLIER = 0.5
OVERRIDE_FLOOR = 0.15


# ---------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------


def _validate_contestation_input(inp: ContestationInput) -> None:
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


async def _require_standing(
    conn: asyncpg.Connection,
    inp: ContestationInput,
) -> Any:
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
    return standing


async def _load_model_snapshot(
    conn: asyncpg.Connection,
    inp: ContestationInput,
) -> _ModelSnapshot:
    model = await conn.fetchrow(
        """
        SELECT id, tenant_id, scope_actors, confidence, signal_readings,
               reading_contestable
        FROM accepted_current_models
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
    return _ModelSnapshot(
        tenant_id=model["tenant_id"],
        previous_confidence=float(model["confidence"]),
        scope_actors=list(model["scope_actors"] or []),
        signal_readings=model["signal_readings"],
    )


async def _insert_contestation_observation(
    conn: asyncpg.Connection,
    inp: ContestationInput,
) -> tuple[UUID, datetime]:
    obs_id = uuid7()
    now = datetime.now(timezone.utc)
    content: dict[str, Any] = {
        "contested_model_id": str(inp.model_id),
        "contestation_kind": inp.contestation_kind,
        "reason": inp.rationale,
    }
    if inp.proposed_alternative is not None:
        content["proposed_alternative"] = inp.proposed_alternative

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
        (
            f"contestation ({inp.contestation_kind}) of model "
            f"{inp.model_id} by actor {inp.contestor_actor_id}: "
            f"{inp.rationale[:200]}"
        ),
        json.dumps([{"type": "model", "id": str(inp.model_id)}]),
    )
    return obs_id, now


async def _apply_first_person_override(
    conn: asyncpg.Connection,
    inp: ContestationInput,
    snapshot: _ModelSnapshot,
) -> _OverrideResult:
    if inp.contestation_kind != "belief" or inp.contestor_actor_id not in snapshot.scope_actors:
        return _OverrideResult(
            new_confidence=snapshot.previous_confidence,
            applied=False,
        )

    if snapshot.scope_actors and inp.contestor_actor_id == snapshot.scope_actors[0]:
        multiplier = PRIMARY_SUBJECT_MULTIPLIER
        role = "primary"
    else:
        multiplier = SECONDARY_SUBJECT_MULTIPLIER
        role = "secondary"

    new_confidence = max(OVERRIDE_FLOOR, snapshot.previous_confidence * multiplier)
    new_confidence = min(new_confidence, 0.95)
    await conn.execute(
        "UPDATE models SET confidence = $1 WHERE id = $2",
        new_confidence,
        inp.model_id,
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
    return _OverrideResult(new_confidence=new_confidence, applied=True)


def _coerce_signal_readings(existing: Any) -> list[Any]:
    if isinstance(existing, (bytes, bytearray)):
        existing = json.loads(existing.decode())
    elif isinstance(existing, str):
        existing = json.loads(existing)
    if isinstance(existing, list):
        return existing
    return []


async def _mark_reading_contested(
    conn: asyncpg.Connection,
    inp: ContestationInput,
    *,
    signal_readings: Any,
    now: datetime,
) -> None:
    existing = _coerce_signal_readings(signal_readings)
    contested_at = now.isoformat()
    for entry in existing:
        if not isinstance(entry, dict):
            continue
        if entry.get("actor_id") != str(inp.contestor_actor_id):
            continue
        entry["contested"] = True
        entry["contested_at"] = contested_at
        entry["rationale"] = inp.rationale
        break
    else:
        existing.append(
            {
                "actor_id": str(inp.contestor_actor_id),
                "contested": True,
                "contested_at": contested_at,
                "rationale": inp.rationale,
            }
        )

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


async def _enqueue_contestation_trigger(
    conn: asyncpg.Connection,
    inp: ContestationInput,
    *,
    observation_id: UUID,
) -> UUID | None:
    trig_subkind = (
        "belief_contestation"
        if inp.contestation_kind == "belief"
        else "reading_contestation"
    )
    return await enqueue_trigger(
        conn,
        tenant_id=inp.tenant_id,
        trigger_kind="T3",
        trigger_subkind=trig_subkind,
        observation_id=observation_id,
        model_id=inp.model_id,
        payload={
            "contestor_actor_id": str(inp.contestor_actor_id),
            "contestation_kind": inp.contestation_kind,
        },
    )


async def contest_model(
    conn: asyncpg.Connection,
    inp: ContestationInput,
) -> ContestationResult:
    """
    Execute the contestation flow against `conn` (callers wrap in
    a transaction).
    """
    _validate_contestation_input(inp)
    standing = await _require_standing(conn, inp)
    snapshot = await _load_model_snapshot(conn, inp)
    obs_id, now = await _insert_contestation_observation(conn, inp)

    # -- 3. Increment contested_count -------------------------------
    await conn.execute(
        "UPDATE models SET contested_count = contested_count + 1 WHERE id = $1",
        inp.model_id,
    )

    override = await _apply_first_person_override(conn, inp, snapshot)

    # -- Reading contestation: mark signal_readings entry -----------
    if inp.contestation_kind == "reading":
        await _mark_reading_contested(
            conn,
            inp,
            signal_readings=snapshot.signal_readings,
            now=now,
        )

    trig_id = await _enqueue_contestation_trigger(
        conn,
        inp,
        observation_id=obs_id,
    )

    return ContestationResult(
        observation_id=obs_id,
        trigger_id=trig_id,
        new_confidence=override.new_confidence,
        previous_confidence=snapshot.previous_confidence,
        standing_basis=standing.basis,
        override_applied=override.applied,
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
