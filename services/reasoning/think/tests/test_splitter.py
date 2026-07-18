"""services/reasoning/think/tests/test_splitter.py — atomic-model splitter unit tests.

Pure text heuristics; no DB, no LLM. These tests verify the
splitter behaves correctly across the cases the model-layer probe
flagged (compound LLM model entries) and across atomic / edge cases
where the splitter MUST leave the original op alone.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.reasoning.think.diff_schema import ClaimOp
from services.reasoning.think.splitter import (
    _atomic_evidence_matches,
    is_compound,
    split_compound_claim_op,
)


# No DB / network — pure unit tests.


# ---------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------


def _make_op(
    *,
    natural: str,
    prop_kind: str = "state",
    scope_actors: list[str] | None = None,
    scope_entities: list[dict] | None = None,
    extra_prop: dict | None = None,
) -> ClaimOp:
    """Build a minimal `ClaimOp(op='insert')` for testing."""
    prop: dict = {"kind": prop_kind}
    if prop_kind == "state":
        prop.update({"subject": "x", "assertion": natural})
    elif prop_kind == "concern":
        prop.update({"about": "x", "nature": natural, "raised_by": "user"})
    elif prop_kind == "prediction":
        prop.update({"expected": natural, "resolution": "tbd"})
    if extra_prop:
        prop.update(extra_prop)
    entry: dict = {
        "born_from_event_id": str(uuid4()),
        "proposition": prop,
        "natural": natural,
        "confidence": 0.8,
        "scope_actors": scope_actors or [],
        "scope_entities": scope_entities or [],
        "scope_temporal": {"valid_from": "2025-01-01T00:00:00Z", "valid_until": None},
    }
    return ClaimOp(op="insert", entry=entry)


# ---------------------------------------------------------------------
# 1. Atomic claim passes through unchanged
# ---------------------------------------------------------------------


def test_atomic_state_claim_passes_through_unchanged():
    op = _make_op(natural="The PR was merged into main.", prop_kind="state")
    out = split_compound_claim_op(op)
    assert len(out) == 1
    assert out[0] is op
    flag, reasons = is_compound(op.entry or {})
    assert flag is False
    assert reasons == []


def test_atomic_concern_claim_passes_through_unchanged():
    op = _make_op(
        natural="The release is at risk because of the failing test.",
        prop_kind="concern",
    )
    out = split_compound_claim_op(op)
    # "is" + "at risk" + "failing" may register state+concern but it's
    # one clause — should NOT split into 2+ verb-bearing conjuncts.
    assert len(out) == 1
    assert out[0] is op


def test_non_insert_op_passes_through_unchanged():
    op = ClaimOp(op="archive", model_id=uuid4(), reason="superseded")
    out = split_compound_claim_op(op)
    assert out == [op]


def test_empty_entry_passes_through_unchanged():
    op = ClaimOp(op="insert", entry={})
    out = split_compound_claim_op(op)
    assert out == [op]


def test_explicit_situation_passes_through_unchanged():
    members = [str(uuid4()), str(uuid4())]
    op = _make_op(
        natural="Atlas operating pressure is forming.",
        prop_kind="situation",
        extra_prop={
            "situation": "Atlas operating pressure",
            "summary": "Atlas has linked operating pressure.",
            "member_model_ids": members,
            "relationship_summary": "Operating signals are linked.",
            "status": "forming",
        },
    )
    out = split_compound_claim_op(op)
    assert out == [op]


def test_explicit_situation_with_empty_members_can_split():
    natural = (
        "Atlas renewal risk is rising, DeltaFleet has the same freshness "
        "issue, and support capacity is saturated."
    )
    op = _make_op(
        natural=natural,
        prop_kind="situation",
        extra_prop={
            "kind": "belief",
            "claim_role": "situation",
            "abstraction_level": "composite",
            "situation": "Reliability issue is becoming cross-customer pressure",
            "summary": natural,
            "member_model_ids": [],
            "relationship_summary": "Customer and support signals share one reliability mechanism.",
            "status": "forming",
        },
    )
    out = split_compound_claim_op(op)
    assert len(out) >= 3
    assert out[-1].entry["proposition"]["claim_role"] == "situation"
    assert out[-1].entry.get("member_model_pending") is True


def test_explicit_norm_passes_through_unchanged():
    op = _make_op(
        natural="Create a recovery plan and assign a sponsor.",
        prop_kind="norm",
        extra_prop={
            "claim_role": "recommendation",
            "target_actor_id": str(uuid4()),
            "proposed_change": {
                "operation": "create",
                "payload": {"title": "Recovery plan"},
            },
            "qualitative_impact": "Reduces execution risk.",
        },
    )
    out = split_compound_claim_op(op)
    assert out == [op]


def test_source_digest_pattern_passes_through_unsplit():
    natural = (
        "The aws:event source is showing a source cadence: 10 observations "
        "form a major source window. This should be represented as a compact "
        "source-pattern baseline, not left as independent low-level events."
    )
    op = _make_op(
        natural=natural,
        prop_kind="state",
        extra_prop={
            "kind": "belief",
            "claim_role": "pattern",
            "abstraction_level": "pattern",
            "time_mode": "recurring",
            "signature": "aws:event recurring source pattern",
            "observed_tendency": "10 observations form a major source window.",
            "domain_tags": ["source_digest", "major_source_window"],
        },
    )

    out = split_compound_claim_op(op)
    flag, reasons = is_compound(op.entry or {})

    assert out == [op]
    assert flag is False
    assert reasons == []


def test_curiosity_hypothesis_passes_through_unsplit():
    natural = (
        "Open operating questions remain for Atlas launch: who owns the next "
        "action, whether the blocker is on the critical path, and what would "
        "disconfirm the risk pattern. These questions should stay durable."
    )
    op = _make_op(
        natural=natural,
        prop_kind="state",
        extra_prop={
            "kind": "belief",
            "claim_role": "hypothesis",
            "abstraction_level": "atomic",
            "time_mode": "current",
            "modality": "inferred",
            "polarity": "neutral",
            "hypothesis_text": natural,
            "test_conditions": (
                "Resolve by finding owner, critical path status, and "
                "disconfirming evidence."
            ),
            "important_unknowns": [
                "responsible owner",
                "whether the blocker is on the critical path",
            ],
            "coverage_roles": ["curiosity", "epistemic", "intervention"],
            "retrieval_tags": [
                "open_question",
                "unresolved_unknown",
                "coverage_curiosity",
                "success_driver",
            ],
            "domain_tags": [
                "open_question",
                "coverage_curiosity",
                "success_driver",
            ],
        },
    )

    out = split_compound_claim_op(op)
    flag, reasons = is_compound(op.entry or {})

    assert out == [op]
    assert flag is False
    assert reasons == []


# ---------------------------------------------------------------------
# 2. 4-clause compound -> 4 atomic claims; conjunction alone is not a situation
# ---------------------------------------------------------------------


def test_four_clause_compound_splits_into_four_atomics_without_situation():
    natural = (
        "HarborRail procurement evidence is delayed, sponsor confidence is "
        "dropping, ARR is at risk, and security review needs SOC2 evidence"
    )
    op = _make_op(natural=natural, prop_kind="state")

    flag, reasons = is_compound(op.entry or {})
    assert flag is True
    # Must have flagged at least conjunction reason.
    assert any(r.startswith("multi_conjunction:") for r in reasons)

    out = split_compound_claim_op(op)
    assert len(out) == 4
    # Atomic ops are inserts and carry their own proposition.
    for atomic in out:
        assert atomic.op == "insert"
        assert atomic.entry is not None
        assert atomic.entry["proposition"]["kind"] in {"belief", "prediction"}
        # Each must preserve provenance / scope_temporal.
        assert atomic.entry.get("born_from_event_id") == op.entry["born_from_event_id"]
        assert atomic.entry.get("scope_temporal") == op.entry["scope_temporal"]
        # Embedding is dropped from atomic entries.
        assert "embedding" not in atomic.entry
        # Confidence is copied verbatim.
        assert atomic.entry.get("confidence") == op.entry["confidence"]


# ---------------------------------------------------------------------
# 3. Single-actor compound (X happened and X is concerned) splits
# ---------------------------------------------------------------------


def test_single_actor_compound_splits_correctly():
    natural = (
        "The PR was merged into main and the deploy is at risk because of "
        "the flaky integration test"
    )
    op = _make_op(natural=natural, prop_kind="state")
    out = split_compound_claim_op(op)
    assert len(out) >= 2
    # At least one of the atomics should be classified as a concern
    # (because of "at risk").
    roles = [o.entry["proposition"].get("claim_role") for o in out]
    assert "concern" in roles


# ---------------------------------------------------------------------
# 4. Compound with 3 distinct entities -> entity scope preserved per
#    atomic split (the splitter copies scope verbatim; this test
#    documents that contract).
# ---------------------------------------------------------------------


def test_compound_three_entities_preserves_scope_on_each_split():
    e1, e2, e3 = str(uuid4()), str(uuid4()), str(uuid4())
    natural = (
        "Globex is churning, Acme has stalled, and Initech is delayed on "
        "their migration"
    )
    op = _make_op(
        natural=natural,
        prop_kind="state",
        scope_entities=[
            {"type": "customer", "id": e1},
            {"type": "customer", "id": e2},
            {"type": "customer", "id": e3},
        ],
    )
    flag, reasons = is_compound(op.entry or {})
    assert flag is True
    # Either multi_conjunction OR multi_entity (or both) must fire.
    assert any(
        r.startswith("multi_conjunction:") or r.startswith("multi_entity:")
        for r in reasons
    )

    out = split_compound_claim_op(op)
    assert len(out) >= 2
    for atomic in out:
        # Scope is copied verbatim onto each atomic split.
        assert atomic.entry["scope_entities"] == op.entry["scope_entities"]


# ---------------------------------------------------------------------
# 5. Pressure-type inference
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "natural,expected_pressure",
    [
        (
            "ARR is at risk and the deal is slipping and renewals are dropping",
            "revenue",
        ),
        (
            "The team is overloaded and bandwidth is gone and capacity is "
            "stretched",
            "capacity",
        ),
        (
            "SOC2 evidence is missing and the audit needs attestation and "
            "data residency review is required",
            "compliance",
        ),
    ],
)
def test_pressure_type_inference(natural: str, expected_pressure: str):
    op = _make_op(
        natural=natural,
        prop_kind="state",
        extra_prop={
            "claim_role": "situation",
            "relationship_summary": "The clauses jointly express one pressure.",
        },
    )
    out = split_compound_claim_op(op)
    assert len(out) >= 2  # at least 1 atomic + 1 situation
    sit = out[-1]
    assert sit.entry["proposition"]["kind"] == "belief"
    assert sit.entry["proposition"]["claim_role"] == "situation"
    assert sit.entry["proposition"].get("pressure_type") == expected_pressure


def test_synthesized_situation_defaults_pressure_type_to_execution():
    op = _make_op(
        natural="The rollout has drifted and the owner has changed",
        prop_kind="state",
        extra_prop={
            "claim_role": "situation",
            "relationship_summary": "The clauses jointly express one pressure.",
        },
    )
    out = split_compound_claim_op(op)
    sit = out[-1]
    assert sit.entry["proposition"]["kind"] == "belief"
    assert sit.entry["proposition"]["claim_role"] == "situation"
    assert sit.entry["proposition"]["pressure_type"] == "execution"


# ---------------------------------------------------------------------
# 6. Structured operational bundles split into addressable Models
# ---------------------------------------------------------------------


def test_operational_facet_bundle_splits_into_atomic_models_without_situation():
    natural = (
        "Form controls visible: radio 500 GB [add $300.00] checked=false; "
        "radio Windows 8 [add $100.00] checked=false; radio Ubuntu checked=true; "
        "'Quantity' value='2'; 'Catalog item' value='Development Laptop (PC)'"
    )
    op = _make_op(natural=natural, prop_kind="state")

    flag, reasons = is_compound(op.entry or {})
    assert flag is True
    assert any(r == "multi_operational_facts:5" for r in reasons)

    out = split_compound_claim_op(op)

    assert len(out) == 5
    atomics = out
    assert [atomic.entry["natural"] for atomic in atomics] == [
        "500 GB adds 300 USD and is unchecked. "
        "Evidence: radio 500 GB [add $300.00] checked=false.",
        "Windows 8 adds 100 USD and is unchecked. "
        "Evidence: radio Windows 8 [add $100.00] checked=false.",
        "radio Ubuntu is checked. Evidence: radio Ubuntu checked=true.",
        "Quantity value is '2'. Evidence: 'Quantity' value='2'.",
        "Catalog item value is 'Development Laptop (PC)'. "
        "Evidence: 'Catalog item' value='Development Laptop (PC)'.",
    ]
    for atomic in atomics:
        prop = atomic.entry["proposition"]
        assert prop["kind"] == "belief"
        assert prop["claim_role"] == "fact"
        assert prop["abstraction_level"] == "atomic"
        assert prop["operational_split_source"] == "universal_facets"
        assert atomic.entry.get("born_from_event_id") == op.entry["born_from_event_id"]
        assert "embedding" not in atomic.entry
        atomic_flag, atomic_reasons = is_compound(atomic.entry)
        assert atomic_flag is False
        assert atomic_reasons == []


def test_single_operational_fact_passes_through_unchanged():
    op = _make_op(
        natural="Form controls visible: radio Ubuntu checked=true",
        prop_kind="state",
    )

    out = split_compound_claim_op(op)

    assert out == [op]
    assert is_compound(op.entry or {}) == (False, [])


# ---------------------------------------------------------------------
# 7. is_compound returns (False, []) for atomic, (True, [reasons]) for
#    compound
# ---------------------------------------------------------------------


def test_is_compound_returns_false_empty_for_atomic():
    op = _make_op(natural="The PR was merged.", prop_kind="state")
    flag, reasons = is_compound(op.entry or {})
    assert flag is False
    assert reasons == []


def test_is_compound_returns_true_with_reasons_for_compound():
    natural = (
        "The PR is merged and the deploy is at risk and ARR is dropping"
    )
    op = _make_op(natural=natural, prop_kind="state")
    flag, reasons = is_compound(op.entry or {})
    assert flag is True
    assert len(reasons) >= 1
    # The first reason should be the conjunction one for this text.
    assert any(r.startswith("multi_conjunction:") for r in reasons)


def test_compound_atomics_receive_only_predicate_compatible_observations():
    observation_bodies = (
        "Atlas release, update 1: The release certificate still has no clearly recorded owner.",
        "Atlas release, update 1: A late reply asks whether the certificate ownership handoff happened.",
        "Atlas release, update 1: The release dashboard remains optimistic while the underlying record is incomplete.",
        "Atlas release, update 1: Someone says 'they have it now' without naming the infrastructure owner.",
        "Atlas release, update 1: The rollout window moved again after the ownership question resurfaced.",
    )
    observation_ids = [str(uuid4()) for _ in observation_bodies]
    natural = (
        "Atlas release certificate ownership remains unresolved and the rollout "
        "window moved again."
    )
    op = _make_op(natural=natural)
    op.entry["supporting_event_ids"] = observation_ids
    op.entry["proposition"]["evidence_event_ids"] = observation_ids
    op.entry["evidence_observation_manifest"] = [
        {"observation_id": oid, "body": body, "source_channel": "slack:message"}
        for oid, body in zip(observation_ids, observation_bodies, strict=True)
    ]

    out = split_compound_claim_op(op)

    assert len(out) == 2
    atomics = out
    owner = next(item for item in atomics if "ownership" in item.entry["natural"])
    moved = next(item for item in atomics if "moved" in item.entry["natural"])
    assert owner.entry["supporting_event_ids"] == [observation_ids[0]]
    assert owner.entry["proposition"]["evidence_event_ids"] == [observation_ids[0]]
    assert moved.entry["supporting_event_ids"] == [observation_ids[4]]
    assert moved.entry["proposition"]["evidence_event_ids"] == [observation_ids[4]]


def test_atomic_evidence_matching_rejects_p6_distractors_and_wrong_predicates():
    assert not _atomic_evidence_matches(
        "Atlas release certificate ownership remains unresolved",
        "Week 1: The Atlas certificate training example uses a handoff checklist.",
    )
    assert not _atomic_evidence_matches(
        "The rollout window moved again",
        "Week 1: The book club moved its informal discussion.",
    )
    assert not _atomic_evidence_matches(
        "Cobalt renewal approval remains unresolved",
        "Week 1: Cobalt paint approval is listed in the Beacon office ticket.",
    )


def test_compound_split_quarantines_atomic_without_local_support():
    owner_id = str(uuid4())
    op = _make_op(
        natural=(
            "Atlas release certificate ownership remains unresolved and executive "
            "sentiment is worsening."
        )
    )
    op.entry["supporting_event_ids"] = [owner_id]
    op.entry["proposition"]["evidence_event_ids"] = [owner_id]
    op.entry["evidence_observation_manifest"] = [
        {
            "observation_id": owner_id,
            "body": "Atlas release certificate still has no clearly recorded owner.",
            "source_channel": "slack:message",
        }
    ]

    out = split_compound_claim_op(op)

    atomics = [item for item in out if not item.entry.get("member_model_pending")]
    assert len(atomics) == 1
    assert "ownership" in atomics[0].entry["natural"]
    assert atomics[0].entry["supporting_event_ids"] == [owner_id]


def test_compound_split_quarantines_causal_atomic_without_typed_derivation():
    owner_id, moved_id = str(uuid4()), str(uuid4())
    op = _make_op(
        natural=(
            "Atlas release ownership remains unresolved and unresolved ownership "
            "is delaying the rollout."
        )
    )
    op.entry["supporting_event_ids"] = [owner_id, moved_id]
    op.entry["proposition"]["evidence_event_ids"] = [owner_id, moved_id]
    op.entry["evidence_observation_manifest"] = [
        {
            "observation_id": owner_id,
            "body": "Atlas release certificate has no clearly recorded owner.",
            "source_channel": "slack:message",
        },
        {
            "observation_id": moved_id,
            "body": "Atlas release rollout window moved again.",
            "source_channel": "slack:message",
        },
    ]

    out = split_compound_claim_op(op)

    assert len(out) == 1
    assert "ownership remains unresolved" in out[0].entry["natural"]
    assert out[0].entry["supporting_event_ids"] == [owner_id]


def test_explicit_emergent_composite_preserves_parent_evidence_union():
    first_id, second_id = str(uuid4()), str(uuid4())
    op = _make_op(
        natural="Capacity is dropping and handoff risk is rising.",
        extra_prop={
            "claim_role": "situation",
            "shared_mechanism": "Capacity loss and handoff risk reinforce each other.",
            "composite_derivation": "The two directly observed pressures jointly constrain delivery.",
        },
    )
    op.entry["supporting_event_ids"] = [first_id, second_id]
    op.entry["proposition"]["evidence_event_ids"] = [first_id, second_id]

    out = split_compound_claim_op(op)

    composite = next(item for item in out if item.entry.get("member_model_pending"))
    assert composite.entry["supporting_event_ids"] == [first_id, second_id]
    assert composite.entry["proposition"]["evidence_event_ids"] == [
        first_id,
        second_id,
    ]
