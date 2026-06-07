from __future__ import annotations

from services.models.address import BELIEF_ADDRESS_VERSION, build_belief_address


def test_belief_address_is_stable_for_same_belief_core() -> None:
    first = build_belief_address(
        {
            "kind": "belief",
            "claim_role": "fact",
            "subject": "Beacon renewal",
            "assertion": "owned by Alice",
        },
        natural="Alice owns the Beacon renewal.",
    )
    second = build_belief_address(
        {
            "kind": "belief",
            "claim_role": "fact",
            "subject": "Beacon renewal",
            "assertion": "owned by Alice",
        },
        natural="The prose can change without changing the belief core.",
    )

    assert first["version"] == BELIEF_ADDRESS_VERSION
    assert first["fingerprint"] == second["fingerprint"]
    assert "subject_predicate:beacon renewal|asserts" in first["obligation_keys"]
    assert "OWNERSHIP" in first["answerable_primitives"]


def test_belief_address_distinguishes_different_belief_objects() -> None:
    left = build_belief_address({
        "kind": "belief",
        "claim_role": "fact",
        "subject": "Beacon renewal",
        "assertion": "owned by Alice",
    })
    right = build_belief_address({
        "kind": "belief",
        "claim_role": "fact",
        "subject": "Beacon renewal",
        "assertion": "blocked by SOC2 readiness",
    })

    assert left["fingerprint"] != right["fingerprint"]
    assert left["obligation_keys"] != right["obligation_keys"]


def test_belief_address_exposes_general_answerable_primitives() -> None:
    address = build_belief_address({
        "kind": "belief",
        "claim_role": "concern",
        "subject": "Beacon launch",
        "assertion": "blocked by quota risk",
        "open_falsifier": "quota increase approved",
    })

    assert "COUNTEREVIDENCE" in address["answerable_primitives"]
    assert "CONSTRAINT" in address["answerable_primitives"]
    assert any(key.startswith("qualifier:") for key in address["obligation_keys"])


def test_belief_address_contract_spans_diverse_model_shapes() -> None:
    cases = [
        (
            {
                "kind": "belief",
                "claim_role": "fact",
                "subject": "HelioWorks handoff",
                "assertion": "owner is platform enablement",
            },
            {"OWNERSHIP"},
        ),
        (
            {
                "kind": "belief",
                "claim_role": "concern",
                "subject": "Orion PatientSync",
                "assertion": "sandbox quota exhaustion blocks release",
            },
            {"CONSTRAINT", "COUNTEREVIDENCE"},
        ),
        (
            {
                "kind": "belief",
                "claim_role": "relation",
                "subject": "SOC2 evidence",
                "relation": "blocks",
                "object": "enterprise launch",
            },
            {"DEPENDENCY"},
        ),
        (
            {
                "kind": "belief",
                "claim_role": "pattern",
                "subject": "Vela ImportFlow",
                "observed_tendency": "month-end stalls recur when imports spike",
            },
            {"RECURRENCE"},
        ),
        (
            {
                "kind": "belief",
                "claim_role": "prediction",
                "expected": "renewal slips if legal approval remains missing",
            },
            {"COUNTEREVIDENCE", "GOAL_IMPACT"},
        ),
        (
            {
                "kind": "belief",
                "claim_role": "recommendation",
                "subject": "Northstar renewal",
                "proposed_change": {"operation": "assign_owner"},
            },
            {"COMMITMENT", "OWNERSHIP"},
        ),
        (
            {
                "kind": "belief",
                "claim_role": "situation",
                "abstraction_level": "composite",
                "situation": "owner gap and security gate reinforce renewal risk",
                "relationship_summary": "two blockers share one renewal mechanism",
            },
            {"DEPENDENCY", "COUNTEREVIDENCE", "CONSTRAINT", "OWNERSHIP"},
        ),
        (
            {
                "kind": "belief",
                "claim_role": "capability",
                "capability_id": "ask-fyralis-retrieval",
                "summary": "maps questions onto model evidence",
            },
            {"DEPENDENCY"},
        ),
    ]

    fingerprints: set[str] = set()
    for proposition, expected_primitives in cases:
        address = build_belief_address(
            proposition,
            natural="Surface wording one.",
        )
        same_belief = build_belief_address(
            proposition,
            natural="Different prose should not change the belief address.",
        )

        assert address["version"] == BELIEF_ADDRESS_VERSION
        assert address["fingerprint"] == same_belief["fingerprint"]
        assert address["obligation_keys"]
        assert expected_primitives <= set(address["answerable_primitives"])
        fingerprints.add(address["fingerprint"])

    assert len(fingerprints) == len(cases)
