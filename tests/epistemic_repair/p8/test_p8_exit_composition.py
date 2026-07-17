from pathlib import Path

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p8_exit import _read_reopened, compose_p8_exit


def test_hash_reopen_review_detects_embedded_digest_tampering(tmp_path) -> None:
    path = tmp_path / "artifact.json"
    body = {"schema_version": "test", "rows": [1, 2, 3]}
    path.write_text(__import__("json").dumps({**body, "artifact_digest": canonical_sha256(body)}))
    _, review = _read_reopened(path)
    assert review["canonical_digest_matches"] is True
    path.write_text(__import__("json").dumps({**body, "rows": [1, 2, 4], "artifact_digest": canonical_sha256(body)}))
    _, review = _read_reopened(path)
    assert review["canonical_digest_matches"] is False


def test_current_exit_composition_fails_closed_on_missing_fault_and_scale_gates() -> None:
    required = (
        Path("/tmp/p8-production-fault-evidence.json"),
        Path("/tmp/p8-isolated-27-matrix-head-v3.json"),
        Path("/tmp/p8-component-characterization-v3.json"),
        Path("/tmp/p8-shared-contention-v2.json"),
        Path("/tmp/p8-codex-canary.jsonl"),
    )
    if not all(path.exists() for path in required):
        return
    artifact = compose_p8_exit(
        fault_path=required[0], scale_path=required[1], characterization_path=required[2],
        contention_path=required[3], provider_canary_path=required[4],
    )
    assert artifact["exit_ready"] is False
    assert artifact["gates"]["scale_latency"] is False
    assert artifact["gates"]["authorized_provider_canaries"] is False
    assert artifact["gates"]["hash_reopen_review"] is True
