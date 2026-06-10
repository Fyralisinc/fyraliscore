from __future__ import annotations

import uuid

import pytest

from lib.shared.errors import ValidationError
from lib.shared.types import ModelCreate
from services.domain.models.constructor import MODEL_CONTRACT_VERSION, construct_model
from services.domain.models.tests.conftest import make_embedding


def _mc(
    *,
    proposition: dict,
    natural: str = "Alice owns the onboarding renewal risk.",
) -> ModelCreate:
    tenant_id = uuid.uuid4()
    event_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    return ModelCreate(
        tenant_id=tenant_id,
        born_from_event_id=event_id,
        proposition=proposition,
        natural=natural,
        embedding=make_embedding(natural),
        scope_actors=[actor_id],
        scope_entities=[{"type": "customer", "id": "Beacon"}],
        scope_temporal={"type": "now"},
        confidence=0.58,
        confidence_at_assertion=0.58,
        supporting_event_ids=[event_id],
        signal_readings=[{"kind": "source_text", "value": natural}],
    )


def test_construct_model_adds_addressable_core_without_new_memory_layer() -> None:
    constructed = construct_model(
        _mc(
            proposition={
                "kind": "state",
                "subject": "Alice",
                "assertion": "owns the onboarding renewal risk",
            },
        )
    )

    prop = constructed.proposed.proposition

    assert prop["kind"] == "belief"
    assert prop["legacy_kind"] == "state"
    assert prop["model_contract_version"] == MODEL_CONTRACT_VERSION
    assert prop["semantic_address"]["claim_role"] == "fact"
    assert prop["semantic_address"]["subject"] == "Alice"
    assert prop["semantic_address"]["predicate"] == "asserts"
    assert prop["semantic_address"]["object"] == "owns the onboarding renewal risk"
    assert prop["semantic_address"]["fingerprint"]
    assert prop["belief_address"]["version"] == "belief_address_v1"
    assert prop["belief_address"]["fingerprint"] == prop["semantic_address"]["fingerprint"]
    assert (
        "spo:alice|asserts|owns the onboarding renewal risk"
        in prop["belief_address"]["obligation_keys"]
    )
    assert "OWNERSHIP" in prop["belief_address"]["answerable_primitives"]
    assert constructed.core.proposition is prop
    assert constructed.evidence.supporting_event_ids
    assert "customers" in constructed.projection.domain_tags
    assert "risk" in constructed.projection.domain_tags
    assert constructed.runtime.activation_coefficient == 1.0


def test_construct_model_rejects_runtime_fields_inside_proposition() -> None:
    with pytest.raises(ValidationError) as exc:
        construct_model(
            _mc(
                proposition={
                    "kind": "belief",
                    "claim_role": "fact",
                    "subject": "Alice",
                    "assertion": "owns the onboarding renewal risk",
                    "activation": 0.91,
                },
            )
        )

    assert "runtime fields" in exc.value.message


def test_construct_model_rejects_atomic_model_with_situation_fields() -> None:
    with pytest.raises(ValidationError) as exc:
        construct_model(
            _mc(
                proposition={
                    "kind": "belief",
                    "claim_role": "fact",
                    "subject": "Beacon renewal",
                    "assertion": "is blocked by onboarding uncertainty",
                    "member_model_ids": [str(uuid.uuid4()), str(uuid.uuid4())],
                    "relationship_summary": "These are linked.",
                },
            )
        )

    assert "situation composition fields" in exc.value.message


def test_construct_model_rejects_situation_with_atomic_operational_facets() -> None:
    with pytest.raises(ValidationError) as exc:
        construct_model(
            _mc(
                proposition={
                    "kind": "belief",
                    "claim_role": "situation",
                    "abstraction_level": "composite",
                    "situation": "Renewal pressure is cross-functional.",
                    "summary": "Renewal pressure is cross-functional.",
                    "member_model_ids": [str(uuid.uuid4()), str(uuid.uuid4())],
                    "relationship_summary": "Members share one renewal loop.",
                    "pressure_type": "revenue",
                    "shared_mechanism": "All members point at the same renewal loop.",
                    "operational_facets": [
                        {
                            "role": "state",
                            "subrole": "choice_state",
                            "value": "Ubuntu",
                            "state": "checked",
                        }
                    ],
                    "operational_roles": ["state"],
                },
            )
        )

    assert "atomic operational facet indexes" in exc.value.message


def test_construct_model_keeps_operational_facets_as_projection_data() -> None:
    natural = (
        "Operational memory record: catalog item Development Laptop. "
        "Form controls visible: radio 500 GB [add $300.00] checked=false; "
        "radio Ubuntu checked=true"
    )
    constructed = construct_model(
        _mc(
            proposition={
                "kind": "state",
                "subject": "Development Laptop catalog form",
                "assertion": "captures explicit UI configuration",
            },
            natural=natural,
        )
    )

    prop = constructed.proposed.proposition

    assert prop["kind"] == "belief"
    assert prop["operational_facet_schema"] == "operational_facets_v1"
    assert "delta" in prop["operational_roles"]
    assert "state" in constructed.projection.operational_roles
    assert prop["semantic_address"]["subject"] == "Development Laptop catalog form"


def test_construct_model_replaces_off_enum_grammar_axes_with_kind_defaults() -> None:
    """LLM-invented axis values must not reach the persisted proposition.

    The models table materializes grammar axes as GENERATED columns
    reading proposition->>'<axis>' under CHECK constraints (migration
    0048); an off-enum explicit value would fail the INSERT wholesale.
    The off-enum key is dropped and refilled with the stance default so
    the persisted payload stays explicit (target_actor_id's generated
    column reads proposition->>'claim_role' directly).
    """
    constructed = construct_model(
        _mc(
            proposition={
                "kind": "belief",
                "subject": "brex_account:acct-1",
                "assertion": "posted a $1,000.00 outflow",
                "time_mode": "point_in_time",
                "modality": "actual",
            },
        )
    )

    prop = constructed.proposed.proposition
    assert prop["time_mode"] == "current"
    assert prop["modality"] == "inferred"
    assert constructed.core.grammar.time_mode == "current"
    assert constructed.core.grammar.modality == "inferred"


def test_construct_model_off_enum_claim_role_on_norm_keeps_recommendation() -> None:
    """A norm with a bad claim_role must persist claim_role='recommendation'
    in the payload — target_actor_id's generated column requires it."""
    constructed = construct_model(
        _mc(
            proposition={
                "kind": "norm",
                "subject": "Alice",
                "assertion": "should review the renewal",
                "claim_role": "advice",
                "target_actor_id": str(uuid.uuid4()),
                "proposed_change": {
                    "operation": "create",
                    "payload": {"title": "Review the renewal"},
                },
                "qualitative_impact": "Reduces renewal risk.",
            },
        )
    )

    prop = constructed.proposed.proposition
    assert prop["claim_role"] == "recommendation"
    assert constructed.core.grammar.claim_role == "recommendation"


def test_construct_model_keeps_valid_explicit_grammar_axes() -> None:
    constructed = construct_model(
        _mc(
            proposition={
                "kind": "belief",
                "subject": "Alice",
                "assertion": "owns the renewal",
                "time_mode": "recurring",
                "modality": "expected",
            },
        )
    )

    prop = constructed.proposed.proposition
    assert prop["time_mode"] == "recurring"
    assert prop["modality"] == "expected"
    assert constructed.core.grammar.time_mode == "recurring"
    assert constructed.core.grammar.modality == "expected"
