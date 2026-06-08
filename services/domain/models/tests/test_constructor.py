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
