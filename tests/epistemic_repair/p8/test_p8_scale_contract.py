from dataclasses import replace
import json

from lib.evaluation.epistemic_repair.p8_oracles import evaluate_p8
from lib.evaluation.epistemic_repair.p8_population import build_characterization_manifests, build_fault_schedule
from lib.evaluation.epistemic_repair.p8_runner import (
    _distributions,
    _fault_results,
    _scale_results,
    reopen_p8_artifact,
    run_p8_deterministic,
    write_p8_artifact,
)


def _evaluate(scale):
    return evaluate_p8(
        faults=_fault_results(),
        scale=scale,
        distributions=_distributions(),
        schedule_digest=build_fault_schedule().digest,
        manifest_digests=tuple(x.sealed_digest for x in build_characterization_manifests()),
    )


def test_complete_matrix_meets_every_declared_scale_limit() -> None:
    artifact = run_p8_deterministic()
    assert len(artifact["scale_reference_vectors"]) == 27
    assert artifact["contract_gates"]["P8-CONTRACT-04_scale_oracle_vectors"] is True
    assert artifact["deterministic_qualification_ready"] is False
    assert artifact["shared_resource_contention"]["isolated_from_semantic_matrix"] is True
    assert len(artifact["component_reference_distributions"]) == 5
    assert all(row["worst_example_ids"] for row in artifact["component_reference_distributions"])


def test_scale_oracle_rejects_queue_growth() -> None:
    scale = list(_scale_results())
    scale[-1] = replace(scale[-1], queue_depth_slope_final_half=.001)
    artifact = _evaluate(tuple(scale))
    assert artifact["phase_exit_ready"] is False
    assert artifact["contract_gates"]["P8-CONTRACT-04_scale_oracle_vectors"] is False


def test_scale_oracle_rejects_cross_tenant_leakage() -> None:
    scale = list(_scale_results())
    scale[2] = replace(scale[2], cross_tenant_leakage=1)
    artifact = _evaluate(tuple(scale))
    assert artifact["phase_exit_ready"] is False


def test_written_artifact_is_reopened_by_hash_and_tampering_fails(tmp_path) -> None:
    output = tmp_path / "p8.json"
    write_p8_artifact(run_p8_deterministic(), output)
    assert reopen_p8_artifact(output)["evaluator_contract_ready"] is True
    changed = json.loads(output.read_text(encoding="utf-8"))
    changed["fault_reference_vectors"] = []
    output.write_text(json.dumps(changed), encoding="utf-8")
    try:
        reopen_p8_artifact(output)
    except ValueError as exc:
        assert "digest mismatch" in str(exc)
    else:
        raise AssertionError("modified P8 artifact was accepted")
