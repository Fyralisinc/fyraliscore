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
    SynthesisProviderDecision,
    bind_synthesis_provider_decision,
)


DIGEST = "a" * 64


def _fixture():
    tenant, trigger = uuid4(), uuid4()
    observation, model, version = uuid4(), uuid4(), uuid4()
    bindings = (
        HandleBinding("M1", "accepted_model_head", model, version, tenant,
                      "workstream:atlas", "authoritative",
                      frozenset({"cause", "support", "novelty_reference"})),
        HandleBinding("M2", "accepted_model_head", uuid4(), uuid4(), tenant,
                      "workstream:atlas", "authoritative",
                      frozenset({"cause", "support"})),
        HandleBinding("O1", "observation", observation, None, tenant,
                      "workstream:atlas", "authoritative",
                      frozenset({"cause", "support"})),
        HandleBinding("O2", "observation", uuid4(), None, tenant,
                      "workstream:atlas", "authoritative",
                      frozenset({"counterevidence"})),
        HandleBinding("O3", "observation", uuid4(), None, tenant,
                      "workstream:atlas", "independent",
                      frozenset({"effect", "support"})),
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
            "cause_condition_handles": ["M1", "M2", "O1"], "effect_handles": ["O3"],
            "supporting_evidence_handles": ["M1", "M2", "O1", "O3"],
            "counterevidence": [{"handle": "O2", "bearing": "weakens",
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
    schema = str(SynthesisProviderDecision.model_json_schema())
    assert "UUID" not in schema
    assert "canonical_id" not in schema
    assert "dossier_id" not in schema
    assert "dossier_digest" not in schema
    raw, _ = _fixture()
    provider_raw = {
        "schema_version": "think-synthesis-provider-decision-v2",
        "decision": raw["decision"],
    }
    with pytest.raises(ValidationError):
        SynthesisProviderDecision.model_validate(
            {**provider_raw, "dossier_id": "provider-authored"}
        )
    with pytest.raises(ValidationError):
        SynthesisProviderDecision.model_validate(
            {**provider_raw, "dossier_digest": "0" * 64}
        )


@pytest.mark.parametrize("invented_digest", ["0" * 64, "1" * 64, "f" * 64, "a" * 64])
def test_provider_cannot_author_any_dossier_digest(invented_digest: str) -> None:
    raw, _ = _fixture()
    with pytest.raises(ValidationError, match="extra_forbidden"):
        SynthesisProviderDecision.model_validate({
            "schema_version": "think-synthesis-provider-decision-v2",
            "dossier_digest": invented_digest,
            "decision": raw["decision"],
        })


def test_trusted_adapter_binds_identity_and_unknown_handles_still_fail() -> None:
    raw, context = _fixture()
    provider_raw = {
        "schema_version": "think-synthesis-provider-decision-v2",
        "decision": {**raw["decision"], "supporting_evidence_handles": ["O9"]},
    }
    provider_decision = SynthesisProviderDecision.model_validate(provider_raw)
    envelope = bind_synthesis_provider_decision(
        provider_decision,
        dossier_id=context.dossier_id,
        dossier_digest=context.dossier_digest,
    )
    assert envelope.dossier_id == context.dossier_id
    assert envelope.dossier_digest == context.dossier_digest
    with pytest.raises(SynthesisContractError, match="unknown handle"):
        compile_frozen_synthesis_decision(envelope, context=context)


@pytest.mark.parametrize(
    "relation_kind",
    ["supports", "causal_chain", "supports_causal_gate", "fixture_relation"],
)
def test_provider_relation_kind_is_closed_before_trusted_binding(
    relation_kind: str,
) -> None:
    raw, _ = _fixture()
    raw["decision"]["relation"]["relation_kind"] = relation_kind
    with pytest.raises(ValidationError, match="relation_kind"):
        SynthesisProviderDecision.model_validate({
            "schema_version": "think-synthesis-provider-decision-v2",
            "decision": raw["decision"],
        })


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("outside_cause", "subset of cause_condition"),
        ("effect_source", "subset of cause_condition"),
        ("no_model_source", "accepted Model source"),
        ("no_direct_support", "direct observation"),
    ],
)
def test_provider_cross_field_relation_contract_fails_before_binding(
    mutation: str, message: str,
) -> None:
    raw, _ = _fixture()
    decision = raw["decision"]
    if mutation == "outside_cause":
        decision["relation"]["source_handles"] = ["M1", "O2"]
    elif mutation == "effect_source":
        decision["relation"]["source_handles"] = ["M1", "O3"]
    elif mutation == "no_model_source":
        decision["relation"]["source_handles"] = ["O1"]
    elif mutation == "no_direct_support":
        decision["supporting_evidence_handles"] = ["M1", "M2"]
    with pytest.raises(ValidationError, match=message):
        SynthesisProviderDecision.model_validate({
            "schema_version": "think-synthesis-provider-decision-v2",
            "decision": decision,
        })


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
    ("unauthorized", "unauthorized cause"), ("digest", "dossier identity"),
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
    raw, _ = _fixture()
    raw["decision"]["thesis"] = "This text says blocks repeatedly but is not authority."
    raw["decision"]["relation"]["relation_kind"] = "fixture_only_relation"
    with pytest.raises(ValidationError, match="relation_kind"):
        SynthesisDecisionEnvelope.model_validate(raw)


def test_observational_cause_requires_a_causal_model_relation_source() -> None:
    raw, _ = _fixture()
    raw["decision"]["relation"]["source_handles"] = ["O1"]
    with pytest.raises(ValidationError, match="accepted Model source"):
        SynthesisDecisionEnvelope.model_validate(raw)
