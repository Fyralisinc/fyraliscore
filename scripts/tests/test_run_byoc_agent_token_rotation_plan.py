from __future__ import annotations

import json
from pathlib import Path

from scripts.run_byoc_agent_token_rotation_plan import main


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "deploy/byoc/dataplane.example.yaml"
CURRENT_REF = "prod/fyralis/dep-example01/agent-bootstrap-token"
NEXT_REF = "prod/fyralis/dep-example01/agent-bootstrap-token-v2"


def test_run_byoc_agent_token_rotation_plan_json_output(capsys) -> None:
    code = main([
        "--json",
        "--manifest",
        str(MANIFEST),
        "--next-install-token-secret-ref",
        NEXT_REF,
        "--activation-epoch",
        "2",
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.agent_token_rotation_plan.v1"
    assert payload["status"] == "pass"
    assert payload["execution_mode"] == "plan_only"
    assert payload["manual_customer_secret_write_required"] is True
    assert payload["cloud_secret_updates_executed"] is False
    assert payload["control_plane_mutations_executed"] is False
    assert payload["privacy"]["raw_token_material_included"] is False
    assert payload["privacy"]["secret_refs_included"] is False
    assert CURRENT_REF not in rendered
    assert NEXT_REF not in rendered
    assert "local-install-token" not in rendered
    assert "arn:aws" not in rendered


def test_run_byoc_agent_token_rotation_plan_writes_output(
    tmp_path: Path,
    capsys,
) -> None:
    output = tmp_path / "agent-token-rotation.json"

    code = main([
        "--json",
        "--manifest",
        str(MANIFEST),
        "--next-install-token-secret-ref",
        NEXT_REF,
        "--output",
        str(output),
    ])

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8")) == json.loads(captured.out)


def test_run_byoc_agent_token_rotation_plan_fails_for_same_ref(capsys) -> None:
    code = main([
        "--json",
        "--manifest",
        str(MANIFEST),
        "--next-install-token-secret-ref",
        CURRENT_REF,
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == "fail"
    assert _check(payload, "secret_refs_differ")["status"] == "fail"


def _check(payload: dict[str, object], name: str) -> dict[str, object]:
    checks = payload["checks"]
    assert isinstance(checks, list)
    for check in checks:
        assert isinstance(check, dict)
        if check["name"] == name:
            return check
    raise AssertionError(f"missing check {name}")
