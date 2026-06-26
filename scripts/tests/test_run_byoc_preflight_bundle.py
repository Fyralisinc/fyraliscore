from __future__ import annotations

import json
from pathlib import Path

from scripts.run_byoc_preflight_bundle import main


ROOT = Path(__file__).resolve().parents[2]
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
ENV_TEMPLATE = ROOT / ".env.production.example"


def test_run_byoc_preflight_bundle_json_output(capsys) -> None:
    code = main(["--json", "--env-file", str(ENV_TEMPLATE)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.preflight_bundle.v1"
    assert payload["execution_mode"] == "customer_side_local"
    assert payload["privacy"]["command_output_included"] is False
    assert payload["terraform_init_executed"] is False
    assert payload["terraform_plan_executed"] is False
    assert payload["aws_live_preflight_requested"] is False
    assert payload["aws_live_preflight_executed"] is False
    assert payload["cloud_credentials_required"] is False
    assert {section["name"] for section in payload["sections"]} >= {
        "permissions_manifest",
        "aws_iac_package",
        "terraform_validation",
        "bootstrap_bundle",
        "bootstrap_runner",
        "post_deploy_validation",
    }


def test_run_byoc_preflight_bundle_writes_output(
    tmp_path: Path,
    capsys,
) -> None:
    output = tmp_path / "byoc-preflight.json"

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


def test_run_byoc_preflight_bundle_can_add_aws_live_contract_smoke(
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
    rendered = json.dumps(payload)
    section = _section(payload, "aws_live_preflight")
    assert code == 0
    assert payload["aws_live_preflight_requested"] is True
    assert payload["aws_live_preflight_executed"] is False
    assert payload["cloud_credentials_required"] is False
    assert section["status"] == "pass"
    assert section["metrics"]["live_aws_api_calls_executed"] is False
    assert "123456789012" not in rendered
    assert "arn:aws" not in rendered


def test_run_byoc_preflight_bundle_can_run_terraform_init_sanitized(
    tmp_path: Path,
    capsys,
) -> None:
    fake_terraform = _fake_terraform(
        tmp_path,
        stdout="terraform raw init output should not appear",
        stderr="terraform init stderr should not appear",
        exit_code=0,
    )

    code = main([
        "--json",
        "--env-file",
        str(ENV_TEMPLATE),
        "--run-terraform-init",
        "--terraform-bin",
        str(fake_terraform),
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload)
    section = _section(payload, "terraform_validation")
    assert code == 0
    assert payload["terraform_init_executed"] is True
    assert payload["terraform_validate_executed"] is False
    assert section["metrics"]["terraform_init_executed"] is True
    assert "terraform raw init output should not appear" not in rendered
    assert "terraform init stderr should not appear" not in rendered


def test_run_byoc_preflight_bundle_reports_failure(
    tmp_path: Path,
    capsys,
) -> None:
    bad_permissions = tmp_path / "permissions.yaml"
    bad_permissions.write_text(
        PERMISSIONS.read_text(encoding="utf-8").replace(
            "cloud_provider: aws",
            "cloud_provider: gcp",
            1,
        ),
        encoding="utf-8",
    )

    code = main([
        "--json",
        "--env-file",
        str(ENV_TEMPLATE),
        "--permissions-manifest",
        str(bad_permissions),
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload)
    permissions = _section(payload, "permissions_manifest")
    assert code == 1
    assert payload["status"] == "fail"
    assert "unsupported_provider" in permissions["failed_check_codes"]
    assert "cloudformation:CreateStack" not in rendered
    assert "arn:aws:iam::123456789012" not in rendered


def _section(payload: dict[str, object], name: str) -> dict[str, object]:
    sections = payload["sections"]
    assert isinstance(sections, list)
    for section in sections:
        assert isinstance(section, dict)
        if section["name"] == name:
            return section
    raise AssertionError(f"missing section {name}")


def _fake_terraform(
    tmp_path: Path,
    *,
    stdout: str,
    stderr: str,
    exit_code: int,
) -> Path:
    path = tmp_path / "terraform"
    path.write_text(
        "#!/usr/bin/env bash\n"
        f"printf '%s\\n' {stdout!r}\n"
        f"printf '%s\\n' {stderr!r} >&2\n"
        f"exit {exit_code}\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path
