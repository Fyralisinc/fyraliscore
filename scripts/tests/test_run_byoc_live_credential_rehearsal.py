from __future__ import annotations

import json
from pathlib import Path

from scripts.run_byoc_live_credential_rehearsal import main


ROOT = Path(__file__).resolve().parents[2]
ENV_TEMPLATE = ROOT / ".env.production.example"


def test_run_byoc_live_credential_rehearsal_contract_smoke(
    tmp_path: Path,
    capsys,
) -> None:
    output_dir = tmp_path / "rehearsal"

    code = main([
        "--json",
        "--output-dir",
        str(output_dir),
        "--env-file",
        str(ENV_TEMPLATE),
        "--skip-live-aws",
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.live_credential_rehearsal.v1"
    assert payload["status"] == "pass"
    assert payload["required_checks_passed"] is True
    assert payload["source_onboarding_allowed"] is True
    assert payload["customer_credential_ready"] is False
    assert payload["skip_live_aws"] is True
    assert payload["live_aws_api_calls_executed"] is False
    assert payload["cloud_credentials_required"] is False
    assert payload["mutating_cloud_commands_executed"] is False
    assert payload["terraform_plan_executed"] is False
    assert set(payload["artifacts_written"]) == {
        "aws-live-preflight.json",
        "evidence-ledger.yaml",
        "evidence-package.yaml",
    }
    assert (output_dir / "aws-live-preflight.json").exists()
    assert (output_dir / "evidence-ledger.yaml").exists()
    assert (output_dir / "evidence-package.yaml").exists()
    assert "123456789012" not in rendered
    assert "arn:aws" not in rendered
    assert "ghcr.io" not in rendered


def test_run_byoc_live_credential_rehearsal_requires_live_api_calls(
    tmp_path: Path,
    capsys,
) -> None:
    code = main([
        "--json",
        "--output-dir",
        str(tmp_path / "rehearsal"),
        "--env-file",
        str(ENV_TEMPLATE),
        "--skip-live-aws",
        "--require-live-aws-api-calls",
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == "fail"
    assert payload["required_checks_passed"] is False
    assert payload["customer_credential_ready"] is False
    assert _check(payload, "live_aws_api_calls")["status"] == "fail"


def test_run_byoc_live_credential_rehearsal_writes_summary(
    tmp_path: Path,
    capsys,
) -> None:
    output_dir = tmp_path / "rehearsal"
    summary = tmp_path / "summary.json"

    code = main([
        "--json",
        "--output-dir",
        str(output_dir),
        "--env-file",
        str(ENV_TEMPLATE),
        "--skip-live-aws",
        "--output",
        str(summary),
    ])

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(summary.read_text(encoding="utf-8")) == json.loads(captured.out)


def _check(payload: dict[str, object], name: str) -> dict[str, object]:
    checks = payload["checks"]
    assert isinstance(checks, list)
    for check in checks:
        assert isinstance(check, dict)
        if check["name"] == name:
            return check
    raise AssertionError(f"missing check {name}")
