import json

import pytest

from lib.contracts.kernel import canonical_sha256
from scripts.check_epistemic_repair_p8_scale_ready import require_scale_ready


def _write(path, body):
    path.write_text(json.dumps({**body, "artifact_digest": canonical_sha256(body)}))


def test_scale_guard_fails_closed_until_all_deterministic_gates_pass(tmp_path) -> None:
    path = tmp_path / "scale.json"
    commit = "a" * 40
    _write(path, {"commit": commit, "evaluation": {"scale_execution_ready": False}})
    with pytest.raises(RuntimeError, match="not fully green"):
        require_scale_ready(path, expected_head=commit)

    _write(path, {"commit": commit, "evaluation": {"scale_execution_ready": True}})
    require_scale_ready(path, expected_head=commit)


def test_scale_guard_rejects_wrong_commit_and_tampering(tmp_path) -> None:
    path = tmp_path / "scale.json"
    commit = "a" * 40
    body = {"commit": commit, "evaluation": {"scale_execution_ready": True}}
    _write(path, body)
    with pytest.raises(RuntimeError, match="commit"):
        require_scale_ready(path, expected_head="b" * 40)
    path.write_text(json.dumps({**body, "evaluation": {"scale_execution_ready": False},
                                "artifact_digest": canonical_sha256(body)}))
    with pytest.raises(RuntimeError, match="digest"):
        require_scale_ready(path, expected_head=commit)
