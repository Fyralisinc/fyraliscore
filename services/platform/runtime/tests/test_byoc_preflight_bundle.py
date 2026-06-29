from __future__ import annotations

import json
from pathlib import Path

from services.platform.runtime.byoc_preflight_bundle import (
    ByocPreflightBundleInputs,
    render_preflight_report_json,
    run_byoc_preflight_bundle,
)


ROOT = Path(__file__).resolve().parents[4]
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
IAM_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"
IAC_PACKAGE = ROOT / "deploy/byoc/aws/iac-package.example.yaml"
BUNDLE = ROOT / "deploy/byoc/bootstrap-bundle.example.yaml"
PLAN = ROOT / "deploy/byoc/bootstrap-plan.example.yaml"
ENV_TEMPLATE = ROOT / ".env.production.example"


def _inputs(
    *,
    permissions_manifest_path: Path = PERMISSIONS,
    run_terraform_validate: bool = False,
    terraform_bin: str = "terraform",
) -> ByocPreflightBundleInputs:
    return ByocPreflightBundleInputs(
        dataplane_manifest_path=DATAPLANE,
        permissions_manifest_path=permissions_manifest_path,
        iam_template_path=IAM_TEMPLATE,
        iac_package_path=IAC_PACKAGE,
        bootstrap_bundle_path=BUNDLE,
        bootstrap_plan_path=PLAN,
        env_path=ENV_TEMPLATE,
        repo_root=ROOT,
        run_terraform_validate=run_terraform_validate,
        terraform_bin=terraform_bin,
    )


def test_preflight_bundle_passes_checked_in_contracts() -> None:
    report = run_byoc_preflight_bundle(_inputs())
    sections = {section.name: section for section in report.sections}

    assert report.status == "pass"
    assert report.required_sections_passed is True
    assert report.execution_mode == "customer_side_local"
    assert report.deployment_id == "dep_example01"
    assert report.terraform_validate_executed is False
    assert report.terraform_plan_executed is False
    assert report.cloud_credentials_required is False
    assert report.mutating_cloud_commands_executed is False
    assert set(sections) == {
        "dataplane_manifest",
        "permissions_manifest",
        "aws_iac_package",
        "terraform_validation",
        "bootstrap_bundle",
        "bootstrap_runner",
        "post_deploy_validation",
    }
    assert all(section.status == "pass" for section in report.sections)
    assert sections["terraform_validation"].metrics["terraform_plan_executed"] is False
    assert sections["bootstrap_runner"].metrics["execution_mode"] == "dry-run"


def test_preflight_bundle_report_excludes_raw_child_details() -> None:
    rendered = render_preflight_report_json(run_byoc_preflight_bundle(_inputs()))

    assert "ghcr.io" not in rendered
    assert "cosign verify" not in rendered
    assert "prepared_commands" not in rendered
    assert "terraform plan" not in rendered.lower()
    assert "terraform apply" not in rendered.lower()
    assert "control.fyralis.com" not in rendered
    assert "postgresql://user:password" not in rendered
    assert "source_payload" not in rendered


def test_preflight_bundle_reports_bounded_permission_failure(tmp_path: Path) -> None:
    bad_permissions = tmp_path / "permissions.yaml"
    bad_permissions.write_text(
        PERMISSIONS.read_text(encoding="utf-8").replace(
            "cloud_provider: aws",
            "cloud_provider: gcp",
            1,
        ),
        encoding="utf-8",
    )

    report = run_byoc_preflight_bundle(
        _inputs(permissions_manifest_path=bad_permissions)
    )
    rendered = render_preflight_report_json(report)
    permissions = _section(report, "permissions_manifest")

    assert report.status == "fail"
    assert report.required_sections_passed is False
    assert permissions.status == "fail"
    assert "unsupported_provider" in permissions.failed_check_codes
    assert "cloudformation:CreateStack" not in rendered
    assert "arn:aws:iam::123456789012" not in rendered


def test_preflight_bundle_can_run_sanitized_terraform_validate(
    tmp_path: Path,
) -> None:
    fake_terraform = _fake_terraform(
        tmp_path,
        stdout="raw customer payload must not appear",
        stderr="token must not appear",
        exit_code=0,
    )

    report = run_byoc_preflight_bundle(
        _inputs(
            run_terraform_validate=True,
            terraform_bin=str(fake_terraform),
        )
    )
    rendered = render_preflight_report_json(report)
    terraform = _section(report, "terraform_validation")

    assert report.status == "pass"
    assert report.terraform_validate_executed is True
    assert terraform.metrics["terraform_validate_executed"] is True
    assert "raw customer payload must not appear" not in rendered
    assert "token must not appear" not in rendered


def test_preflight_bundle_json_output_is_machine_readable() -> None:
    payload = json.loads(
        render_preflight_report_json(run_byoc_preflight_bundle(_inputs()))
    )

    assert payload["schema_version"] == "fyralis.byoc.preflight_bundle.v1"
    assert payload["privacy"]["command_output_included"] is False
    assert payload["sections"][0]["name"] == "dataplane_manifest"


def _section(report, name: str):
    return next(section for section in report.sections if section.name == name)


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
