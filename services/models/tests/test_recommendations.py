"""
services/models/tests/test_recommendations.py — recommendation
proposition validation + DB-backed insert pipeline + archive lifecycle.

Covers Session 1 of RECOMMENDATION-BUILD-PLAN:
  - Pydantic shape: every required field is enforced.
  - DB-backed validation: target_act_ref existence and state-machine
    reachability for transitions.
  - Archive lifecycle: new archive_reason values land cleanly.
"""
from __future__ import annotations

import uuid
from typing import Any

import asyncpg
import pytest

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.models.propositions import (
    LEGAL_KINDS,
    RecommendationProposition,
    validate_proposition,
)
from services.models.repo import ModelsRepo
from services.observations.events import notify_scope

from .conftest import make_embedding


pytestmark = pytest.mark.integration


# =====================================================================
# Pydantic-shape tests (no DB)
# =====================================================================


def _good_recommendation_proposition(
    *, target_actor_id: str, commitment_id: str
) -> dict[str, Any]:
    return {
        "kind": "recommendation",
        "target_act_ref": {"type": "commitment", "id": commitment_id},
        "proposed_change": {
            "operation": "transition",
            "payload": {"new_state": "paused"},
        },
        "expected_impact": 340000.0,
        "qualitative_impact": None,
        "target_actor_id": target_actor_id,
    }


def test_recommendation_kind_in_legal_kinds() -> None:
    assert "recommendation" in LEGAL_KINDS


def test_recommendation_proposition_round_trips() -> None:
    raw = _good_recommendation_proposition(
        target_actor_id=str(uuid7()),
        commitment_id=str(uuid7()),
    )
    parsed = validate_proposition(raw)
    assert isinstance(parsed, RecommendationProposition)
    dumped = parsed.model_dump()
    assert dumped["kind"] == "recommendation"
    assert dumped["target_act_ref"]["type"] == "commitment"


def test_recommendation_rejects_unknown_target_act_ref_type() -> None:
    raw = _good_recommendation_proposition(
        target_actor_id=str(uuid7()), commitment_id=str(uuid7()),
    )
    raw["target_act_ref"]["type"] = "alien"
    with pytest.raises(ValidationError):
        validate_proposition(raw)


def test_recommendation_rejects_unknown_proposed_change_operation() -> None:
    raw = _good_recommendation_proposition(
        target_actor_id=str(uuid7()), commitment_id=str(uuid7()),
    )
    raw["proposed_change"]["operation"] = "yeet"
    with pytest.raises(ValidationError):
        validate_proposition(raw)


def test_recommendation_requires_one_impact_field() -> None:
    raw = _good_recommendation_proposition(
        target_actor_id=str(uuid7()), commitment_id=str(uuid7()),
    )
    raw["expected_impact"] = None
    raw["qualitative_impact"] = None
    with pytest.raises(ValidationError):
        validate_proposition(raw)


def test_recommendation_accepts_qualitative_only_impact() -> None:
    raw = _good_recommendation_proposition(
        target_actor_id=str(uuid7()), commitment_id=str(uuid7()),
    )
    raw["expected_impact"] = None
    raw["qualitative_impact"] = "key engineer attrition risk"
    parsed = validate_proposition(raw)
    assert parsed.kind == "recommendation"


def test_recommendation_rejects_empty_target_actor_id() -> None:
    # `target_actor_id` is Optional (recommendations can be unaddressed),
    # but if provided it must be a non-empty UUID string. Empty string is
    # the common bug — coerced from a missing DB column / null in JSON.
    raw = _good_recommendation_proposition(
        target_actor_id=str(uuid7()), commitment_id=str(uuid7()),
    )
    raw["target_actor_id"] = ""
    with pytest.raises(ValidationError):
        validate_proposition(raw)


def test_recommendation_accepts_missing_target_actor_id() -> None:
    raw = _good_recommendation_proposition(
        target_actor_id=str(uuid7()), commitment_id=str(uuid7()),
    )
    del raw["target_actor_id"]
    parsed = validate_proposition(raw)
    assert parsed.kind == "recommendation"
    assert parsed.target_actor_id is None


# =====================================================================
# DB-backed insert + cross-field validators
# =====================================================================


async def _seed_commitment(
    conn: asyncpg.Connection,
    *,
    tenant: uuid.UUID,
    owner_id: uuid.UUID,
    born_from_event: uuid.UUID,
    state: str = "active",
) -> uuid.UUID:
    cid = uuid7()
    await conn.execute(
        """
        INSERT INTO commitments (
            id, tenant_id, title, description, state, owner_id,
            created_by_event_id
        ) VALUES (
            $1, $2, 'Build rate limiter', NULL, $3, $4, $5
        )
        """,
        cid, tenant, state, owner_id, born_from_event,
    )
    return cid


def _model_create_for_recommendation(
    *,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    actor_id: uuid.UUID,
    proposition: dict[str, Any],
    confidence: float = 0.5,
) -> ModelCreate:
    return ModelCreate(
        tenant_id=tenant,
        born_from_event_id=born_from_event,
        proposition=proposition,
        natural="Pause the rate limiter commitment until capacity opens up.",
        embedding=make_embedding("recommendation:pause-rate-limiter"),
        scope_actors=[actor_id],
        scope_entities=[],
        scope_temporal={"valid_from": "2026-04-26T00:00:00Z", "valid_until": None},
        confidence=confidence,
        confidence_at_assertion=confidence,
    )


async def test_recommendation_insert_succeeds_with_valid_target(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    cid = await _seed_commitment(
        tx_conn, tenant=tenant, owner_id=actor_id,
        born_from_event=born_from_event, state="active",
    )
    prop = _good_recommendation_proposition(
        target_actor_id=str(actor_id), commitment_id=str(cid),
    )
    with notify_scope():
        row = await repo.insert(
            _model_create_for_recommendation(
                tenant=tenant, born_from_event=born_from_event,
                actor_id=actor_id, proposition=prop,
            ),
            conn=tx_conn,
        )
    assert row.proposition_kind == "recommendation"
    assert row.target_actor_id == actor_id
    assert row.caused_act_change_id is None


async def test_recommendation_insert_rejects_nonexistent_target(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    fake_cid = str(uuid7())
    prop = _good_recommendation_proposition(
        target_actor_id=str(actor_id), commitment_id=fake_cid,
    )
    with pytest.raises(ValidationError) as exc:
        with notify_scope():
            await repo.insert(
                _model_create_for_recommendation(
                    tenant=tenant, born_from_event=born_from_event,
                    actor_id=actor_id, proposition=prop,
                ),
                conn=tx_conn,
            )
    assert "non-existent" in exc.value.message


async def test_recommendation_insert_rejects_unreachable_transition(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    # Seed a commitment in 'closed' (terminal). No transitions allowed.
    cid = await _seed_commitment(
        tx_conn, tenant=tenant, owner_id=actor_id,
        born_from_event=born_from_event, state="closed",
    )
    prop = _good_recommendation_proposition(
        target_actor_id=str(actor_id), commitment_id=str(cid),
    )
    prop["proposed_change"]["payload"]["new_state"] = "active"
    with pytest.raises(ValidationError) as exc:
        with notify_scope():
            await repo.insert(
                _model_create_for_recommendation(
                    tenant=tenant, born_from_event=born_from_event,
                    actor_id=actor_id, proposition=prop,
                ),
                conn=tx_conn,
            )
    assert "unreachable" in exc.value.message.lower() or \
        "terminal" in exc.value.message.lower()


async def test_recommendation_insert_rejects_resource_transition(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    # Resources don't have a state machine — transition op is illegal.
    rid = uuid7()
    await tx_conn.execute(
        """
        INSERT INTO resources (
            id, tenant_id, kind, identity, current_value
        ) VALUES (
            $1, $2, 'financial', 'cash', '{"usd": 100}'::jsonb
        )
        """,
        rid, tenant,
    )
    prop = _good_recommendation_proposition(
        target_actor_id=str(actor_id), commitment_id=str(rid),
    )
    prop["target_act_ref"]["type"] = "resource"
    prop["proposed_change"] = {
        "operation": "transition",
        "payload": {"new_state": "depleted"},
    }
    with pytest.raises(ValidationError):
        with notify_scope():
            await repo.insert(
                _model_create_for_recommendation(
                    tenant=tenant, born_from_event=born_from_event,
                    actor_id=actor_id, proposition=prop,
                ),
                conn=tx_conn,
            )


async def test_recommendation_archive_with_acted_upon_reason(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    cid = await _seed_commitment(
        tx_conn, tenant=tenant, owner_id=actor_id,
        born_from_event=born_from_event,
    )
    prop = _good_recommendation_proposition(
        target_actor_id=str(actor_id), commitment_id=str(cid),
    )
    with notify_scope():
        row = await repo.insert(
            _model_create_for_recommendation(
                tenant=tenant, born_from_event=born_from_event,
                actor_id=actor_id, proposition=prop,
            ),
            conn=tx_conn,
        )
        archived = await repo.archive(
            row.id, "acted_upon", conn=tx_conn,
        )
    assert archived.status == "archived"
    assert archived.archive_reason == "acted_upon"


async def test_recommendation_archive_with_dismissed_by_user_reason(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    cid = await _seed_commitment(
        tx_conn, tenant=tenant, owner_id=actor_id,
        born_from_event=born_from_event,
    )
    prop = _good_recommendation_proposition(
        target_actor_id=str(actor_id), commitment_id=str(cid),
    )
    with notify_scope():
        row = await repo.insert(
            _model_create_for_recommendation(
                tenant=tenant, born_from_event=born_from_event,
                actor_id=actor_id, proposition=prop,
            ),
            conn=tx_conn,
        )
        archived = await repo.archive(
            row.id, "dismissed_by_user", conn=tx_conn,
        )
    assert archived.archive_reason == "dismissed_by_user"


async def test_target_actor_id_generated_only_for_recommendation(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    """A non-recommendation Model has target_actor_id NULL — the
    GENERATED column extracts the field only for recommendation kind."""
    state_prop = {
        "kind": "state",
        "subject": "alice",
        "assertion": "ships consistently",
    }
    with notify_scope():
        row = await repo.insert(
            _model_create_for_recommendation(
                tenant=tenant, born_from_event=born_from_event,
                actor_id=actor_id, proposition=state_prop,
            ),
            conn=tx_conn,
        )
    assert row.target_actor_id is None
