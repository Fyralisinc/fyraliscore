from __future__ import annotations

import json
from pathlib import Path

from scripts.run_byoc_aws_live_preflight import main


ROOT = Path(__file__).resolve().parents[2]
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
IAM_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"


def test_run_byoc_aws_live_preflight_skip_live_json(capsys) -> None:
    code = main(["--json", "--skip-live-aws"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.aws_live_preflight.v1"
    assert payload["execution_mode"] == "customer_side_live"
    assert payload["cloud_credentials_required"] is False
    assert payload["live_aws_api_calls_executed"] is False
    assert payload["mutating_aws_api_calls_executed"] is False
    assert "123456789012" not in rendered
    assert "arn:aws" not in rendered


def test_run_byoc_aws_live_preflight_writes_output(
    tmp_path: Path,
    capsys,
) -> None:
    output = tmp_path / "aws-live-preflight.json"

    code = main(["--json", "--skip-live-aws", "--output", str(output)])

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8")) == json.loads(captured.out)


def test_run_byoc_aws_live_preflight_reports_contract_failure(
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
        "--skip-live-aws",
        "--dataplane-manifest",
        str(DATAPLANE),
        "--permissions-manifest",
        str(bad_permissions),
        "--iam-template",
        str(IAM_TEMPLATE),
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == "fail"
    assert any(
        check["name"] == "aws_permission_contract_present"
        and check["status"] == "fail"
        for check in payload["checks"]
    )
