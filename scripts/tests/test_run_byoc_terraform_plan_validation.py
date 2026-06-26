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
    assert payload["terraform_init_executed"] is False
    assert payload["terraform_validate_executed"] is False
    assert payload["terraform_plan_executed"] is False
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


def test_run_byoc_terraform_plan_validation_can_run_validate(
    tmp_path: Path,
    capsys,
) -> None:
    fake_terraform = _fake_terraform(
        tmp_path,
        stdout="raw customer data should not appear",
        stderr="stderr customer data should not appear",
        exit_code=0,
    )

    code = main([
        "--json",
        "--run-terraform-validate",
        "--terraform-bin",
        str(fake_terraform),
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload)
    assert code == 0
    assert payload["terraform_init_executed"] is False
    assert payload["terraform_validate_executed"] is True
    assert payload["terraform_plan_executed"] is False
    validate = _check(payload, "terraform_validate_execution")
    assert validate["status"] == "pass"
    assert validate["metrics"]["exit_code"] == 0
    assert "raw customer data should not appear" not in rendered
    assert "stderr customer data should not appear" not in rendered


def test_run_byoc_terraform_plan_validation_can_run_init(
    tmp_path: Path,
    capsys,
) -> None:
    fake_terraform = _fake_terraform(
        tmp_path,
        stdout="raw init output should not appear",
        stderr="init stderr should not appear",
        exit_code=0,
    )

    code = main([
        "--json",
        "--run-terraform-init",
        "--terraform-bin",
        str(fake_terraform),
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload)
    init = _check(payload, "terraform_init_execution")
    assert code == 0
    assert payload["terraform_init_executed"] is True
    assert payload["terraform_validate_executed"] is False
    assert payload["terraform_plan_executed"] is False
    assert init["status"] == "pass"
    assert init["metrics"]["exit_code"] == 0
    assert "raw init output should not appear" not in rendered
    assert "init stderr should not appear" not in rendered


def test_run_byoc_terraform_plan_validation_init_failure_blocks_validate(
    tmp_path: Path,
    capsys,
) -> None:
    fake_terraform = _fake_terraform(
        tmp_path,
        stdout="raw init failure should not appear",
        stderr="secret init failure should not appear",
        exit_code=7,
    )

    code = main([
        "--json",
        "--run-terraform-init",
        "--run-terraform-validate",
        "--terraform-bin",
        str(fake_terraform),
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload)
    init = _check(payload, "terraform_init_execution")
    validate = _check(payload, "terraform_validate_execution")
    assert code == 1
    assert payload["terraform_init_executed"] is True
    assert payload["terraform_validate_executed"] is False
    assert init["status"] == "fail"
    assert init["metrics"]["exit_code"] == 7
    assert validate["status"] == "fail"
    assert validate["metrics"]["executed"] is False
    assert validate["metrics"]["terraform_init_passed"] is False
    assert "raw init failure should not appear" not in rendered
    assert "secret init failure should not appear" not in rendered


def test_run_byoc_terraform_plan_validation_sanitizes_validate_failure(
    tmp_path: Path,
    capsys,
) -> None:
    fake_terraform = _fake_terraform(
        tmp_path,
        stdout="raw payload should not appear",
        stderr="token should not appear",
        exit_code=7,
    )

    code = main([
        "--json",
        "--run-terraform-validate",
        "--terraform-bin",
        str(fake_terraform),
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload)
    assert code == 1
    assert payload["terraform_validate_executed"] is True
    validate = _check(payload, "terraform_validate_execution")
    assert validate["status"] == "fail"
    assert validate["metrics"]["exit_code"] == 7
    assert "raw payload should not appear" not in rendered
    assert "token should not appear" not in rendered


def _copy_package_tree(tmp_path: Path) -> None:
    for source in (ROOT / "deploy/byoc/aws").rglob("*"):
        if not source.is_file():
            continue
        rel = source.relative_to(ROOT)
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")


def _check(payload: dict[str, object], name: str) -> dict[str, object]:
    checks = payload["checks"]
    assert isinstance(checks, list)
    for check in checks:
        assert isinstance(check, dict)
        if check["name"] == name:
            return check
    raise AssertionError(f"missing check {name}")


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
