from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from services.platform.runtime.byoc_terraform_plan_validation import (
    ByocTerraformPlanValidationInputs,
    load_byoc_terraform_plan_validation_report,
    render_terraform_plan_validation_json,
    run_byoc_terraform_plan_validation,
)


ROOT = Path(__file__).resolve().parents[4]
IAC_PACKAGE = ROOT / "deploy/byoc/aws/iac-package.example.yaml"
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
IAM_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"


def _inputs(repo_root: Path = ROOT) -> ByocTerraformPlanValidationInputs:
    return ByocTerraformPlanValidationInputs(
        iac_package_path=IAC_PACKAGE,
        dataplane_manifest_path=DATAPLANE,
        permissions_manifest_path=PERMISSIONS,
        iam_template_path=IAM_TEMPLATE,
        repo_root=repo_root,
    )


def _check_by_name(report, name: str):
    return next(check for check in report.checks if check.name == name)


def test_terraform_plan_validation_passes_checked_in_scaffold() -> None:
    report = run_byoc_terraform_plan_validation(_inputs())

    assert report.status == "pass"
    assert report.required_checks_passed is True
    assert report.execution_mode == "contract_only"
    assert report.terraform_validate_executed is False
    assert report.terraform_plan_executed is False
    assert report.terraform_plan_json_included is False
    assert report.terraform_command_output_included is False
    assert report.module_count == 5
    assert "terraform_plan_contract_only" in {check.name for check in report.checks}
    validate = _check_by_name(report, "terraform_validate_execution")
    assert validate.status == "skipped"
    assert validate.required is False
    assert validate.metrics == {"requested": False, "executed": False}


def test_terraform_plan_validation_report_is_sanitized() -> None:
    rendered = render_terraform_plan_validation_json(
        run_byoc_terraform_plan_validation(_inputs())
    )

    assert "ghcr.io" not in rendered
    assert "terraform plan" not in rendered.lower()
    assert "terraform apply" not in rendered.lower()
    assert "raw_payload" not in rendered
    assert "token_value" not in rendered


def test_terraform_validate_execution_discards_command_output(
    tmp_path: Path,
) -> None:
    fake_terraform = _fake_terraform(
        tmp_path,
        stdout="raw secret output should not be captured",
        stderr="private stderr should not be captured",
        exit_code=0,
    )

    report = run_byoc_terraform_plan_validation(
        ByocTerraformPlanValidationInputs(
            iac_package_path=IAC_PACKAGE,
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            repo_root=ROOT,
            run_terraform_validate=True,
            terraform_bin=str(fake_terraform),
        )
    )
    rendered = render_terraform_plan_validation_json(report)
    validate = _check_by_name(report, "terraform_validate_execution")

    assert report.status == "pass"
    assert report.terraform_validate_executed is True
    assert report.terraform_plan_executed is False
    assert validate.status == "pass"
    assert validate.required is True
    assert validate.metrics["exit_code"] == 0
    assert "raw secret output should not be captured" not in rendered
    assert "private stderr should not be captured" not in rendered


def test_terraform_validate_failure_is_sanitized(tmp_path: Path) -> None:
    fake_terraform = _fake_terraform(
        tmp_path,
        stdout="customer prompt should not be captured",
        stderr="credential should not be captured",
        exit_code=3,
    )

    report = run_byoc_terraform_plan_validation(
        ByocTerraformPlanValidationInputs(
            iac_package_path=IAC_PACKAGE,
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            repo_root=ROOT,
            run_terraform_validate=True,
            terraform_bin=str(fake_terraform),
        )
    )
    rendered = render_terraform_plan_validation_json(report)
    validate = _check_by_name(report, "terraform_validate_execution")

    assert report.status == "fail"
    assert report.required_checks_passed is False
    assert report.terraform_validate_executed is True
    assert validate.status == "fail"
    assert validate.required is True
    assert validate.metrics["exit_code"] == 3
    assert "customer prompt should not be captured" not in rendered
    assert "credential should not be captured" not in rendered


def test_terraform_plan_validation_reports_scaffold_drift(tmp_path: Path) -> None:
    _copy_package_tree(tmp_path)
    module_file = tmp_path / "deploy/byoc/aws/terraform/modules/runtime/main.tf"
    module_file.write_text(
        module_file.read_text(encoding="utf-8")
        + '\nresource "aws_cloudwatch_log_group" "raw" { name = "unsafe" }\n',
        encoding="utf-8",
    )
    report = run_byoc_terraform_plan_validation(
        ByocTerraformPlanValidationInputs(
            iac_package_path=tmp_path / "deploy/byoc/aws/iac-package.example.yaml",
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            repo_root=tmp_path,
        )
    )

    assert report.status == "fail"
    assert "iac_package_contract" in {
        check.name for check in report.checks if check.status == "fail"
    }


def test_terraform_plan_validation_loads_report(tmp_path: Path) -> None:
    report = run_byoc_terraform_plan_validation(_inputs())
    path = tmp_path / "terraform-validation-report.json"
    path.write_text(render_terraform_plan_validation_json(report), encoding="utf-8")

    loaded = load_byoc_terraform_plan_validation_report(path)

    assert loaded == report


def test_terraform_plan_validation_rejects_extra_report_fields(tmp_path: Path) -> None:
    report = run_byoc_terraform_plan_validation(_inputs()).as_json()
    report["raw_output"] = "terraform validate output"
    path = tmp_path / "terraform-validation-report.json"
    path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(ValidationError):
        load_byoc_terraform_plan_validation_report(path)


def _copy_package_tree(tmp_path: Path) -> None:
    for source in (ROOT / "deploy/byoc/aws").rglob("*"):
        if not source.is_file():
            continue
        rel = source.relative_to(ROOT)
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")


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
