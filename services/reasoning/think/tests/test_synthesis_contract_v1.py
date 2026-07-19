from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from services.reasoning.think.compiled_reasoning import compile_frozen_synthesis_decision
from services.reasoning.think.synthesis_contract import (
    HandleBinding,
    SynthesisCompileContext,
    SynthesisContractError,
    SynthesisDecisionEnvelope,
)


DIGEST = "a" * 64


def _fixture():
    tenant, trigger = uuid4(), uuid4()
    observation, model, version = uuid4(), uuid4(), uuid4()
    bindings = (
        HandleBinding("M1", "accepted_model_head", model, version, tenant,
                      "workstream:atlas", "authoritative",
                      frozenset({"cause", "effect", "novelty_reference"})),
        HandleBinding("M2", "accepted_model_head", uuid4(), uuid4(), tenant,
                      "workstream:atlas", "authoritative",
                      frozenset({"effect"})),
        HandleBinding("O1", "observation", observation, None, tenant,
                      "workstream:atlas", "authoritative",
                      frozenset({"support"})),
        HandleBinding("O2", "observation", uuid4(), None, tenant,
                      "workstream:atlas", "authoritative",
                      frozenset({"effect"})),
        HandleBinding("O3", "observation", uuid4(), None, tenant,
                      "workstream:atlas", "independent",
                      frozenset({"counterevidence"})),
    )
    context = SynthesisCompileContext(
        "atlas-v1", DIGEST, tenant, "workstream:atlas", trigger,
        frozenset(row.canonical_id for row in bindings if row.object_kind == "observation"),
        bindings,
    )
    raw = {
        "schema_version": "think-synthesis-decision-v1", "dossier_id": "atlas-v1",
        "dossier_digest": DIGEST, "decision": {
            "kind": "synthesis", "thesis": "Ownership delay affected rollout timing.",
            "mechanism": "An unowned certificate delayed the rollout gate.",
            "cause_condition_handles": ["M1"], "effect_handles": ["M2"],
            "supporting_evidence_handles": ["O1"],
            "counterevidence": [{"handle": "O3", "bearing": "weakens",
                                  "explanation": "One status report claimed readiness."}],
            "strongest_alternative": {"thesis": "Capacity caused the delay.",
                "mechanism": "A capacity constraint could defer rollout.",
                "supporting_handles": [], "why_weaker": "No capacity evidence exists."},
            "novelty": {"classification": "novel", "relative_to_model_handles": [],
                        "explanation": "No accepted head states this mechanism."},
            "confidence": .82, "falsifying_evidence": ["An owned valid certificate before delay."],
            "relation": {"relation_kind": "causes", "source_handles": ["M1"],
                         "target": "synthesis_output", "direction": "source_to_target",
                         "explanation": "The prerequisite state caused the delayed outcome."},
        },
    }
    return raw, context


def test_provider_schema_contains_handles_and_no_uuid_fields() -> None:
    schema = str(SynthesisDecisionEnvelope.model_json_schema())
    assert "UUID" not in schema
    assert "canonical_id" not in schema
    raw, _ = _fixture()
    with pytest.raises(ValidationError):
        SynthesisDecisionEnvelope.model_validate({**raw, "provider_uuid": str(uuid4())})


def test_synthesis_compiles_exactly_one_composite_and_relation_only() -> None:
    raw, context = _fixture()
    diff = compile_frozen_synthesis_decision(
        SynthesisDecisionEnvelope.model_validate(raw), context=context,
    )
    assert len(diff.claim_ops) == len(diff.relation_claim_ops) == 1
    assert not diff.edge_ops and not diff.relation_frame_ops
    proposition = diff.claim_ops[0].entry["proposition"]
    relation = diff.relation_claim_ops[0]
    assert proposition["abstraction_level"] == "composite"
    assert str(relation.target_model_id) == diff.claim_ops[0].entry["born_from_event_id"]
    assert relation.source_model_version_id == context.bindings[0].exact_version_id
    assert relation.metadata["relation_claim_origin"] == "ti2_synthesis_contract"
    assert relation.metadata["atomic_with_synthesis"] is True


def test_abstention_compiles_to_zero_mutation() -> None:
    raw, context = _fixture()
    raw["decision"] = {"kind": "abstain", "reason_code": "insufficient_evidence",
                       "explanation": "The evidence cannot distinguish mechanisms.",
                       "missing_evidence": ["Certificate ownership timeline"],
                       "relevant_handles": ["O1"], "confidence": .8}
    # O1 is support-authorized; abstention relevant handles are intentionally
    # existence checked without granting a mutation role.
    diff = compile_frozen_synthesis_decision(
        SynthesisDecisionEnvelope.model_validate(raw), context=context,
    )
    assert not diff.claim_ops and not diff.relation_claim_ops and not diff.edge_ops


@pytest.mark.parametrize("mutation,match", [
    ("unknown", "unknown handle"), ("stale", "stale or malformed"),
    ("scope", "tenant or scope mismatch"), ("closure", "outside trigger closure"),
    ("unauthorized", "unauthorized support"), ("digest", "dossier identity"),
    ("duplicate_binding", "duplicate handle binding"),
])
def test_invalid_binding_fails_before_diff(mutation: str, match: str) -> None:
    raw, context = _fixture()
    if mutation == "unknown":
        raw["decision"]["supporting_evidence_handles"] = ["O9"]
    elif mutation == "stale":
        context = replace(context, bindings=(replace(context.bindings[0], current_accepted=False),
                                             *context.bindings[1:]))
    elif mutation == "scope":
        context = replace(context, bindings=(replace(context.bindings[0], canonical_scope_ref="other"),
                                             *context.bindings[1:]))
    elif mutation == "closure":
        context = replace(context, trigger_observation_ids=frozenset())
    elif mutation == "unauthorized":
        context = replace(context, bindings=(*context.bindings[:2],
            replace(context.bindings[2], allowed_roles=frozenset()), *context.bindings[3:]))
    elif mutation == "digest":
        raw["dossier_digest"] = "b" * 64
    elif mutation == "duplicate_binding":
        context = replace(context, bindings=(*context.bindings, context.bindings[0]))
    with pytest.raises(SynthesisContractError, match=match):
        compile_frozen_synthesis_decision(
            SynthesisDecisionEnvelope.model_validate(raw), context=context,
        )


def test_relation_semantics_are_not_inferred_from_thesis_text() -> None:
    raw, context = _fixture()
    raw["decision"]["thesis"] = "This text says blocks repeatedly but is not authority."
    raw["decision"]["relation"]["relation_kind"] = "fixture_only_relation"
    with pytest.raises(SynthesisContractError, match="unsupported governed"):
        compile_frozen_synthesis_decision(
            SynthesisDecisionEnvelope.model_validate(raw), context=context,
        )
