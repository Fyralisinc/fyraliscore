from lib.evaluation.epistemic_repair.p2_oracles import (
    P2CaseObservation,
    P2_GATE_IDS,
    evaluate_gate,
    P2RaceObservation,
    race_conforms,
    stable_digest,
)


def test_gate_contract_covers_hg04_through_hg10() -> None:
    assert P2_GATE_IDS == ("HG-04", "HG-05", "HG-06", "HG-07", "HG-08", "HG-09", "HG-10")


def test_missing_observations_cannot_pass() -> None:
    result = evaluate_gate("HG-04", eligible_case_ids=("a", "b"), observations={})

    assert result.status == "missing"
    assert result.coverage == 0
    assert result.conformance is None


def test_unrun_observations_remain_explicitly_unrun() -> None:
    result = evaluate_gate(
        "HG-04",
        eligible_case_ids=("a",),
        observations={"a": P2CaseObservation("a", "unrun")},
    )

    assert result.status == "unrun"
    assert result.observed_count == 0


def test_complete_conforming_evidence_passes_continuously() -> None:
    observations = {
        "a": P2CaseObservation("a", "observed", "remain_noncanonical", (("HG-04", True),)),
        "b": P2CaseObservation("b", "observed", "reject", (("HG-04", True),)),
    }
    result = evaluate_gate("HG-04", eligible_case_ids=("a", "b"), observations=observations)

    assert result.status == "pass"
    assert result.coverage == 1.0
    assert result.conformance == 1.0
    assert result.violation_count == 0


def test_violation_fails_and_preserves_continuous_score() -> None:
    observations = {
        "a": P2CaseObservation("a", "observed", "accept", (("HG-09", True),)),
        "b": P2CaseObservation("b", "observed", "accept", (("HG-09", False),), violation_codes=("self_negating_relation_admitted",)),
    }
    result = evaluate_gate("HG-09", eligible_case_ids=("a", "b"), observations=observations)

    assert result.status == "fail"
    assert result.conformance == 0.5
    assert result.violation_codes == ("self_negating_relation_admitted",)


def test_disposition_is_compared_with_external_expectation() -> None:
    observation = P2CaseObservation(
        "a", "observed", "accept", (("HG-04", True),)
    )
    result = evaluate_gate(
        "HG-04",
        eligible_case_ids=("a",),
        observations={"a": observation},
        expected_dispositions={"a": "remain_noncanonical"},
    )

    assert result.status == "fail"
    assert result.conformance == 0.0


def test_race_oracle_requires_exact_outcome_and_snapshot_evidence() -> None:
    complete = P2RaceObservation(
        "race-a", "observed", "wholly_old_state", "before", "after"
    )
    missing_snapshot = P2RaceObservation(
        "race-a", "observed", "wholly_old_state", "before", None
    )

    assert race_conforms(complete, "wholly_old_state")
    assert not race_conforms(complete, "wholly_new_state")
    assert not race_conforms(missing_snapshot, "wholly_old_state")


def test_digest_is_key_order_independent_and_content_sensitive() -> None:
    assert stable_digest({"a": 1, "b": 2}) == stable_digest({"b": 2, "a": 1})
    assert stable_digest({"a": 1}) != stable_digest({"a": 2})
