from __future__ import annotations

import json
from pathlib import Path

from services.platform.runtime.byoc_agent_token_rotation import (
    ByocAgentTokenRotationInputs,
    run_byoc_agent_token_rotation_plan,
)
from services.platform.runtime.byoc_contract import load_byoc_manifest


ROOT = Path(__file__).resolve().parents[4]
MANIFEST_PATH = ROOT / "deploy/byoc/dataplane.example.yaml"
MANIFEST = load_byoc_manifest(MANIFEST_PATH)
NEXT_REF = "prod/fyralis/dep-example01/agent-bootstrap-token-v2"


def test_agent_token_rotation_plan_is_sanitized_and_plan_only() -> None:
    report = run_byoc_agent_token_rotation_plan(
        ByocAgentTokenRotationInputs(
            manifest_path=MANIFEST_PATH,
            next_install_token_secret_ref=NEXT_REF,
            overlap_seconds=3600,
            activation_epoch=2,
        )
    )

    serialized = json.dumps(report.as_json(), sort_keys=True)
    assert report.status == "pass"
    assert report.required_checks_passed is True
    assert report.execution_mode == "plan_only"
    assert report.rotation_plan_id is not None
    assert report.current_secret_ref_digest is not None
    assert report.next_secret_ref_digest is not None
    assert report.current_and_next_refs_differ is True
    assert report.cloud_secret_updates_executed is False
    assert report.control_plane_mutations_executed is False
    assert report.privacy.raw_token_material_included is False
    assert report.privacy.secret_refs_included is False
    assert MANIFEST.secrets.bootstrap_token_secret_ref not in serialized
    assert NEXT_REF not in serialized
    assert "arn:aws" not in serialized
    assert "123456789012" not in serialized


def test_agent_token_rotation_plan_requires_distinct_next_ref() -> None:
    report = run_byoc_agent_token_rotation_plan(
        ByocAgentTokenRotationInputs(
            manifest_path=MANIFEST_PATH,
            next_install_token_secret_ref=MANIFEST.secrets.bootstrap_token_secret_ref,
        )
    )

    assert report.status == "fail"
    assert report.required_checks_passed is False
    assert _check(report.as_json(), "secret_refs_differ")["status"] == "fail"


def test_agent_token_rotation_plan_rejects_raw_looking_ref() -> None:
    raw_ref = "https://secrets.example.invalid/token"

    report = run_byoc_agent_token_rotation_plan(
        ByocAgentTokenRotationInputs(
            manifest_path=MANIFEST_PATH,
            next_install_token_secret_ref=raw_ref,
        )
    )

    serialized = json.dumps(report.as_json(), sort_keys=True)
    assert report.status == "fail"
    assert report.next_secret_ref_digest is None
    assert report.rotation_plan_id is None
    assert _check(report.as_json(), "next_secret_ref_safe")["status"] == "fail"
    assert raw_ref not in serialized


def _check(payload: dict[str, object], name: str) -> dict[str, object]:
    checks = payload["checks"]
    assert isinstance(checks, list)
    for check in checks:
        assert isinstance(check, dict)
        if check["name"] == name:
            return check
    raise AssertionError(f"missing check {name}")
