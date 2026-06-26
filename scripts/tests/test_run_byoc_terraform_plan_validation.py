from __future__ import annotations

import json
from pathlib import Path

from scripts.run_byoc_terraform_plan_validation import main


ROOT = Path(__file__).resolve().parents[2]
IAC_PACKAGE = ROOT / "deploy/byoc/aws/iac-package.example.yaml"
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
IAM_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"


def test_run_byoc_terraform_plan_validation_json_output(capsys) -> None:
    code = main(["--json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.terraform_plan_validation.v1"
    assert payload["execution_mode"] == "contract_only"
    assert payload["terraform_plan_json_included"] is False
    assert payload["terraform_command_output_included"] is False


def test_run_byoc_terraform_plan_validation_writes_output(
    tmp_path: Path,
    capsys,
) -> None:
    output = tmp_path / "terraform-validation-report.json"

    code = main(["--json", "--output", str(output)])

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8")) == json.loads(captured.out)


def test_run_byoc_terraform_plan_validation_reports_failure(
    tmp_path: Path,
    capsys,
) -> None:
    _copy_package_tree(tmp_path)
    module_file = tmp_path / "deploy/byoc/aws/terraform/modules/runtime/main.tf"
    module_file.write_text(
        module_file.read_text(encoding="utf-8")
        + '\nresource "aws_cloudwatch_log_group" "raw" { name = "unsafe" }\n',
        encoding="utf-8",
    )

    code = main([
        "--json",
        "--iac-package",
        str(tmp_path / "deploy/byoc/aws/iac-package.example.yaml"),
        "--dataplane-manifest",
        str(DATAPLANE),
        "--permissions-manifest",
        str(PERMISSIONS),
        "--iam-template",
        str(IAM_TEMPLATE),
        "--repo-root",
        str(tmp_path),
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == "fail"
    assert "iac_package_contract" in {
        check["name"] for check in payload["checks"] if check["status"] == "fail"
    }


def _copy_package_tree(tmp_path: Path) -> None:
    for source in (ROOT / "deploy/byoc/aws").rglob("*"):
        if not source.is_file():
            continue
        rel = source.relative_to(ROOT)
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
