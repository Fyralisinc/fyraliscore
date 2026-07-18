from pathlib import Path

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p8_exit import _contention_complete, _read_reopened, compose_p8_exit


def test_hash_reopen_review_detects_embedded_digest_tampering(tmp_path) -> None:
    path = tmp_path / "artifact.json"
    body = {"schema_version": "test", "rows": [1, 2, 3]}
    path.write_text(__import__("json").dumps({**body, "artifact_digest": canonical_sha256(body)}))
    _, review = _read_reopened(path)
    assert review["canonical_digest_matches"] is True
    path.write_text(__import__("json").dumps({**body, "rows": [1, 2, 4], "artifact_digest": canonical_sha256(body)}))
    _, review = _read_reopened(path)
    assert review["canonical_digest_matches"] is False


def test_contention_requires_exact_concurrent_denominator() -> None:
    result = {
        "schema_version": "p8-shared-contention-v2",
        "result": {
            "selected_cell_ids": ["p8-bs10-h12-t1", "p8-bs25-h12-t5", "p8-bs50-h12-t20"],
            "concurrent_cells": 3, "wall_time_ms": 12.0,
            "individual_wall_time_sum_ms": 30.0, "contention_ratio": .4,
            "evidence_digest": "a" * 64,
        },
    }
    assert _contention_complete(result) is True
    result["result"]["concurrent_cells"] = 2
    assert _contention_complete(result) is False


def test_current_exit_composition_fails_closed_on_missing_fault_and_scale_gates() -> None:
    required = (
        Path("/tmp/p8-production-fault-evidence.json"),
        Path("/tmp/p8-isolated-27-matrix-head-v3.json"),
        Path("/tmp/p8-component-characterization-v3.json"),
        Path("/tmp/p8-shared-contention-v2.json"),
        Path("/tmp/p8-codex-canary.jsonl"),
        Path("/tmp/p8-repeated-warm-pairs.json"),
    )
    if not all(path.exists() for path in required):
        return
    artifact = compose_p8_exit(
        fault_path=required[0], scale_path=required[1], characterization_path=required[2],
        contention_path=required[3], provider_canary_path=required[4],
        repeated_warm_path=required[5],
    )
    assert artifact["exit_ready"] is False
    assert artifact["gates"]["scale_latency"] is False
    assert artifact["gates"]["authorized_provider_canaries"] is False
    assert artifact["gates"]["hash_reopen_review"] is True


def _write_artifact(path: Path, body: dict) -> None:
    import json
    path.write_text(json.dumps({**body, "artifact_digest": canonical_sha256(body)}))


def test_exit_requires_complete_25_signal_warm_provenance(tmp_path: Path) -> None:
    commit = "a" * 40
    paths = {name: tmp_path / f"{name}.json" for name in (
        "fault", "scale", "characterization", "contention", "warm"
    )}
    _write_artifact(paths["fault"], {"commit": commit})
    _write_artifact(paths["scale"], {"commit": commit, "evaluation": {"gates": {}}})
    _write_artifact(paths["characterization"], {"commit": commit})
    _write_artifact(paths["contention"], {"commit": commit})
    warm = {
        "schema_version": "p8-repeated-warm-pair-v1", "commit": commit,
        "preregistration": {
            "controls": [[25, 12], [25, 100]], "repetitions": 5,
            "concurrencies": [1, 20], "warmups_excluded": True,
        },
        "analysis": {"diagnostic_complete": True},
    }
    _write_artifact(paths["warm"], warm)
    artifact = compose_p8_exit(
        fault_path=paths["fault"], scale_path=paths["scale"],
        characterization_path=paths["characterization"],
        contention_path=paths["contention"], repeated_warm_path=paths["warm"],
    )
    assert artifact["gates"]["repeated_warm_25_signal_provenance"] is True

    warm["preregistration"]["controls"] = [[10, 12], [10, 100]]
    _write_artifact(paths["warm"], warm)
    artifact = compose_p8_exit(
        fault_path=paths["fault"], scale_path=paths["scale"],
        characterization_path=paths["characterization"],
        contention_path=paths["contention"], repeated_warm_path=paths["warm"],
    )
    assert artifact["gates"]["repeated_warm_25_signal_provenance"] is False
