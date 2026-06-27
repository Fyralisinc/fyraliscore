from __future__ import annotations

import json
from pathlib import Path

from scripts.run_byoc_customer_handoff import main


ROOT = Path(__file__).resolve().parents[2]
ENV_TEMPLATE = ROOT / ".env.production.example"


def test_run_byoc_customer_handoff_json_output(capsys) -> None:
    code = main(["--json", "--env-file", str(ENV_TEMPLATE)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.customer_handoff_readiness.v1"
    assert payload["execution_mode"] == "customer_side_local"
    assert payload["customer_handoff_ready"] is True
    assert payload["source_onboarding_allowed"] is True
    assert payload["cloud_credentials_required"] is False
    assert payload["live_aws_api_calls_executed"] is False
    assert payload["terraform_plan_executed"] is False
    assert payload["mutating_cloud_commands_executed"] is False
    assert payload["privacy"]["child_report_details_included"] is False
    assert payload["privacy"]["evidence_package_body_included"] is False
    assert {section["name"] for section in payload["sections"]} == {
        "preflight_bundle",
        "evidence_package",
        "source_onboarding_gate",
    }


def test_run_byoc_customer_handoff_can_add_aws_live_contract_smoke(
    capsys,
) -> None:
    code = main([
        "--json",
        "--env-file",
        str(ENV_TEMPLATE),
        "--run-aws-live-preflight",
        "--skip-aws-live-preflight-aws",
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    preflight = _section(payload, "preflight_bundle")
    assert code == 0
    assert payload["customer_handoff_ready"] is True
    assert payload["live_aws_api_calls_executed"] is False
    assert payload["cloud_credentials_required"] is False
    assert preflight["metrics"]["aws_live_preflight_requested"] is True
    assert preflight["metrics"]["live_aws_api_calls_executed"] is False


def test_run_byoc_customer_handoff_can_require_aws_live_evidence(
    capsys,
) -> None:
    code = main([
        "--json",
        "--env-file",
        str(ENV_TEMPLATE),
        "--require-aws-live-preflight",
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    source_gate = _section(payload, "source_onboarding_gate")
    assert code == 1
    assert payload["customer_handoff_ready"] is False
    assert payload["source_onboarding_allowed"] is False
    assert source_gate["status"] == "fail"
    assert "aws_live_preflight_evidence" in source_gate["failed_check_codes"]


def test_run_byoc_customer_handoff_writes_output(
    tmp_path: Path,
    capsys,
) -> None:
    output = tmp_path / "byoc-customer-handoff.json"

    code = main([
        "--json",
        "--env-file",
        str(ENV_TEMPLATE),
        "--output",
        str(output),
    ])

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8")) == json.loads(captured.out)


def test_run_byoc_customer_handoff_output_is_sanitized(capsys) -> None:
    code = main(["--json", "--env-file", str(ENV_TEMPLATE)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload)
    assert code == 0
    assert "123456789012" not in rendered
    assert "arn:aws" not in rendered
    assert "ghcr.io" not in rendered
    assert "postgresql://" not in rendered
    assert "prepared_commands" not in rendered
    assert "token=" not in rendered


def _section(payload: dict[str, object], name: str) -> dict[str, object]:
    sections = payload["sections"]
    assert isinstance(sections, list)
    for section in sections:
        assert isinstance(section, dict)
        if section["name"] == name:
            return section
    raise AssertionError(f"missing section {name}")
