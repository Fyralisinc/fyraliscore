"""Tests for lib/shared/types.py — Pydantic schema mirrors."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from hypothesis import given, settings, strategies as st
from pydantic import ValidationError as PydanticValidationError

from lib.shared.ids import uuid7
from lib.shared.types import (
    ActorRow,
    CommitmentRow,
    DecisionRow,
    DependsOnEdge,
    EntityAliasRow,
    GoalRow,
    ModelCreate,
    ModelRow,
    ObservationCreate,
    ObservationRow,
    ResourceRow,
    ResourceTransactionRow,
)


NOW = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)
T = uuid7()     # tenant
E = uuid7()     # event id
A = uuid7()     # actor id


def _valid_embedding() -> list[float]:
    return [0.0] * 768


# ---------------------------------------------------------------------
# Observations
# ---------------------------------------------------------------------

def test_observation_create_minimal_accepts():
    payload = ObservationCreate(
        tenant_id=T,
        occurred_at=NOW,
        source_channel="slack:C01ENG",
        content={"text": "hi"},
        content_text="hi",
        trust_tier="authoritative",
    )
    assert payload.kind == "signal"     # default
    assert payload.entities_mentioned == []


def test_observation_create_rejects_unknown_kind():
    with pytest.raises(PydanticValidationError):
        ObservationCreate(
            tenant_id=T,
            occurred_at=NOW,
            source_channel="slack",
            content={},
            content_text="",
            trust_tier="authoritative",
            kind="not_a_kind",
        )


def test_observation_create_rejects_unknown_trust_tier():
    with pytest.raises(PydanticValidationError):
        ObservationCreate(
            tenant_id=T,
            occurred_at=NOW,
            source_channel="slack",
            content={},
            content_text="",
            trust_tier="bogus",
        )


def test_observation_row_required_fields():
    row = ObservationRow(
        id=uuid7(),
        tenant_id=T,
        occurred_at=NOW,
        ingested_at=NOW,
        kind="signal",
        source_channel="x",
        content={},
        content_text="",
        trust_tier="reputable",
        sequence_num=42,
    )
    assert row.embedding is None
    assert row.embedding_pending is False
    assert row.sequence_num == 42


def test_observation_row_extra_field_rejected():
    # extra="forbid" is the safety net that catches schema drift
    # when asyncpg returns columns we didn't declare.
    with pytest.raises(PydanticValidationError):
        ObservationRow(
            id=uuid7(), tenant_id=T, occurred_at=NOW, ingested_at=NOW,
            kind="signal", source_channel="x", content={},
            content_text="", trust_tier="reputable", sequence_num=1,
            invented_column="oh no",
        )


# ---------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------

def test_model_create_confidence_bounds():
    emb = _valid_embedding()
    # Below 0.05 rejected.
    with pytest.raises(PydanticValidationError):
        ModelCreate(
            tenant_id=T, born_from_event_id=E,
            proposition={"kind": "state"}, natural="x", embedding=emb,
            scope_temporal={"type": "now"}, confidence=0.04,
            confidence_at_assertion=0.04,
        )
    # Above 0.95 rejected.
    with pytest.raises(PydanticValidationError):
        ModelCreate(
            tenant_id=T, born_from_event_id=E,
            proposition={"kind": "state"}, natural="x", embedding=emb,
            scope_temporal={"type": "now"}, confidence=0.96,
            confidence_at_assertion=0.96,
        )


def test_model_create_boundary_values_accepted():
    emb = _valid_embedding()
    for conf in (0.05, 0.95):
        m = ModelCreate(
            tenant_id=T, born_from_event_id=E,
            proposition={"kind": "state"}, natural="x", embedding=emb,
            scope_temporal={"type": "now"}, confidence=conf,
            confidence_at_assertion=conf,
        )
        assert m.confidence == conf
        assert m.confidence_at_assertion == conf


def test_model_row_reserved_word_field():
    """
    The `natural` field collides with the SQL reserved keyword. The
    Pydantic model must expose it under that exact attribute name.
    """
    r = ModelRow(
        id=uuid7(), tenant_id=T, born_from_event_id=E,
        proposition={"kind": "state"}, natural="Alice ships features fast",
        embedding=_valid_embedding(),
        scope_temporal={"type": "now"},
        confidence=0.6, activation=1.0, created_at=NOW,
        confidence_at_assertion=0.6,
    )
    assert r.natural == "Alice ships features fast"


def test_model_row_defaults():
    r = ModelRow(
        id=uuid7(), tenant_id=T, born_from_event_id=E,
        proposition={"kind": "state"}, natural="x",
        embedding=_valid_embedding(), scope_temporal={"type": "now"},
        confidence=0.6, activation=1.0, created_at=NOW,
        confidence_at_assertion=0.6,
    )
    assert r.status == "active"
    assert r.retrieval_count == 0
    assert r.reading_contestable is True
    assert r.visible_to_subjects is True
    # Post-Wave-0 A1 defaults
    assert r.confirmed_count == 0
    assert r.contested_count == 0
    assert r.activation_coefficient == 1.0
    assert r.resolved_at is None
    assert r.resolution_outcome is None


def test_model_row_rejects_invalid_archive_reason():
    with pytest.raises(PydanticValidationError):
        ModelRow(
            id=uuid7(), tenant_id=T, born_from_event_id=E,
            proposition={}, natural="x", embedding=_valid_embedding(),
            scope_temporal={"type": "now"},
            confidence=0.5, activation=1.0, created_at=NOW,
            confidence_at_assertion=0.5,
            archive_reason="because_i_said_so",
        )


def test_model_row_accepts_deprecated_archive_reason():
    """Post-Wave-0 A3: 'deprecated' replaces pseudo-code's deprecated_at."""
    r = ModelRow(
        id=uuid7(), tenant_id=T, born_from_event_id=E,
        proposition={"kind": "state"}, natural="x",
        embedding=_valid_embedding(), scope_temporal={"type": "now"},
        confidence=0.5, activation=1.0, created_at=NOW,
        confidence_at_assertion=0.5,
        status="archived", archived_at=NOW, archive_reason="deprecated",
    )
    assert r.archive_reason == "deprecated"


# ---------------------------------------------------------------------
# Acts
# ---------------------------------------------------------------------

def test_goal_row_defaults():
    g = GoalRow(
        id=uuid7(), tenant_id=T, title="Ship Q3", created_at=NOW,
        last_state_change_at=NOW, created_by_event_id=E,
    )
    assert g.state == "active"
    assert g.altitude == "operational"
    assert g.cached_health == "healthy"


def test_commitment_row_states():
    for state in ("proposed", "active", "blocked", "paused",
                  "doneunverified", "doneverified", "closed"):
        c = CommitmentRow(
            id=uuid7(), tenant_id=T, title="t",
            state=state, created_at=NOW, last_state_change_at=NOW,
            created_by_event_id=E,
        )
        assert c.state == state


def test_commitment_row_rejects_bad_state():
    with pytest.raises(PydanticValidationError):
        CommitmentRow(
            id=uuid7(), tenant_id=T, title="t",
            state="not_a_state", created_at=NOW,
            last_state_change_at=NOW, created_by_event_id=E,
        )


def test_decision_row_rejects_bad_state():
    with pytest.raises(PydanticValidationError):
        DecisionRow(
            id=uuid7(), tenant_id=T, title="t", decision_text="d",
            state="proposed",   # decisions use drafted, not proposed
            created_at=NOW, last_state_change_at=NOW,
            created_by_event_id=E,
        )


def test_depends_on_requires_two_different_ids():
    # The SQL CHECK constraint is enforced by the DB. At the Pydantic
    # level we simply ensure the row is constructible with distinct
    # ids. (Same-id enforcement is a DB-layer test in Wave 1-D.)
    a, b = uuid7(), uuid7()
    edge = DependsOnEdge(dependent_commitment_id=a, dependency_commitment_id=b)
    assert edge.dependent_commitment_id != edge.dependency_commitment_id


# ---------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------

def test_resource_row_six_kinds_accepted():
    for k in ("financial", "ip", "relational", "capacity",
              "infrastructure", "regulatory"):
        r = ResourceRow(
            id=uuid7(), tenant_id=T, kind=k, identity="x",
            current_value={"raw": 1}, created_at=NOW, last_updated_at=NOW,
        )
        assert r.kind == k


def test_resource_row_rejects_unknown_kind():
    with pytest.raises(PydanticValidationError):
        ResourceRow(
            id=uuid7(), tenant_id=T, kind="mystical",
            identity="x", current_value={}, created_at=NOW,
            last_updated_at=NOW,
        )


def test_resource_transaction_types():
    for tt in ("acquire", "deploy", "release", "spend",
               "strengthen", "weaken", "expire"):
        r = ResourceTransactionRow(
            id=uuid7(), resource_id=uuid7(), tenant_id=T,
            transaction_type=tt, delta={"n": 1}, occurred_at=NOW,
            source_event_id=E, created_at=NOW,
        )
        assert r.transaction_type == tt


# ---------------------------------------------------------------------
# Actors
# ---------------------------------------------------------------------

def test_actor_row_type_enum():
    for t in ("human_internal", "human_external", "ai_agent"):
        a = ActorRow(
            id=uuid7(), tenant_id=T, type=t, display_name="n",
            created_at=NOW,
        )
        assert a.type == t


def test_actor_row_rejects_paraphrased_enum():
    """BUILD-PLAN 1-B says 'human'|'agent'; spec S5.3 is authoritative."""
    with pytest.raises(PydanticValidationError):
        ActorRow(
            id=uuid7(), tenant_id=T, type="human", display_name="n",
            created_at=NOW,
        )


# ---------------------------------------------------------------------
# Entity aliases
# ---------------------------------------------------------------------

def test_entity_alias_row_optional_actor():
    alias = EntityAliasRow(
        id=uuid7(), tenant_id=T, alias_text="the auth thing",
        resolved_entity_ref={"type": "commitment", "id": "c-187"},
        first_seen_at=NOW, last_used_at=NOW,
    )
    assert alias.actor_id is None
    assert alias.is_canonical is False
    assert alias.confidence == 0.8


# ---------------------------------------------------------------------
# Property tests
# ---------------------------------------------------------------------

@given(
    kind=st.sampled_from([
        "signal", "state_change", "anomaly_flagged", "contestation",
        "prediction_resolution", "transaction",
    ]),
    trust=st.sampled_from([
        "authoritative", "attested_agent", "authoritative_external",
        "reputable", "inferential", "inferential_external", "unvetted",
    ]),
)
@settings(max_examples=50)
def test_observation_create_property(kind: str, trust: str):
    payload = ObservationCreate(
        tenant_id=T,
        occurred_at=NOW,
        kind=kind,
        source_channel="slack",
        content={"k": 1},
        content_text="text",
        trust_tier=trust,
    )
    # Every combo round-trips to dict and back.
    d = payload.model_dump()
    clone = ObservationCreate(**d)
    assert clone.kind == kind
    assert clone.trust_tier == trust


@given(conf=st.floats(min_value=0.05, max_value=0.95))
@settings(max_examples=30)
def test_model_create_confidence_property(conf: float):
    emb = _valid_embedding()
    m = ModelCreate(
        tenant_id=T, born_from_event_id=E,
        proposition={"kind": "state"}, natural="x",
        embedding=emb, scope_temporal={"type": "now"},
        confidence=conf, confidence_at_assertion=conf,
    )
    assert 0.05 <= m.confidence <= 0.95
