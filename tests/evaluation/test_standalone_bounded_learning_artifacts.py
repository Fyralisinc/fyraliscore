from lib.contracts.kernel import canonical_sha256
from services.retrieval_evolution_postfix_vertical import run_bounded_retrieval_evolution_postfix
from services.source_equivalence_vertical import run_bounded_source_equivalence


def _digest_is_valid(payload):
    body = dict(payload)
    expected = body.pop("objective_sha256")
    return expected == canonical_sha256(body)


def test_retrieval_postfix_artifact_is_reproducible_and_policy_green():
    result = run_bounded_retrieval_evolution_postfix()
    assert _digest_is_valid(result)
    assert result["evaluation"]["verdict"] == "meets_preregistered_policy"
    assert result["population"] == {"batches": 9, "signals_per_batch": 12, "signals": 108}


def test_source_equivalence_artifact_is_reproducible_and_policy_green():
    result = run_bounded_source_equivalence()
    assert _digest_is_valid(result)
    assert result["evaluation"]["verdict"] == "meets_policy"
    assert result["population"]["source_batches"] == 8
