from dataclasses import replace

from lib.evaluation.epistemic_repair.p8_oracles import evaluate_p8
from lib.evaluation.epistemic_repair.p8_population import build_characterization_manifests, build_fault_schedule
from lib.evaluation.epistemic_repair.p8_runner import _distributions, _fault_results, _scale_results


def _evaluate(faults):
    return evaluate_p8(
        faults=faults,
        scale=_scale_results(),
        distributions=_distributions(),
        schedule_digest=build_fault_schedule().digest,
        manifest_digests=tuple(x.sealed_digest for x in build_characterization_manifests()),
    )


def test_every_fault_and_duplicate_replay_converges_with_receipts() -> None:
    faults = _fault_results()
    assert len(faults) == 24
    assert all(len(row.attempts) == 2 for row in faults)
    artifact = _evaluate(faults)
    assert artifact["evaluator_contract_ready"] is True
    assert artifact["deterministic_qualification_ready"] is False
    assert artifact["phase_exit_ready"] is False
    assert artifact["real_provider_canaries"]["status"] == "not_run"


def test_oracle_rejects_digest_divergence() -> None:
    faults = list(_fault_results())
    faults[7] = replace(faults[7], recovered_derived_digest="0" * 64)
    artifact = _evaluate(tuple(faults))
    assert artifact["evaluator_contract_ready"] is False
    assert artifact["phase_exit_ready"] is False
    assert artifact["contract_gates"]["P8-CONTRACT-02_fault_oracle_vectors"] is False


def test_oracle_rejects_hidden_partial_state_and_missing_receipts() -> None:
    faults = list(_fault_results())
    faults[0] = replace(faults[0], partial_truth_rows=1, attempts=())
    artifact = _evaluate(tuple(faults))
    assert artifact["evaluator_contract_ready"] is False
    assert artifact["phase_exit_ready"] is False
    assert artifact["contract_gates"]["P8-CONTRACT-02_fault_oracle_vectors"] is False
