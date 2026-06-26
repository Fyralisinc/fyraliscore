from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts.generate_byoc_aws_iac_package import main


ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "deploy/byoc/aws/iac-package.example.yaml"


def test_generate_byoc_aws_iac_package_yaml_output(capsys) -> None:
    code = main([])

    captured = capsys.readouterr()
    payload = yaml.safe_load(captured.out)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.aws.iac_package.v1"
    assert payload["package_status"] == "scaffold_only"
    assert payload["execution"]["terraform_apply_allowed"] is False


def test_generate_byoc_aws_iac_package_json_output(capsys) -> None:
    code = main(["--json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["terraform"]["root_module_path"] == "deploy/byoc/aws/terraform"
    assert payload["safety"]["required_variables"] == [
        "deployment_id",
        "customer_id",
        "environment",
        "region",
        "aws_account_id",
        "cloudformation_stack_prefix",
        "permissions_boundary_policy_arn",
    ]


def test_generate_byoc_aws_iac_package_check_passes(capsys) -> None:
    code = main(["--check-package", str(PACKAGE)])

    captured = capsys.readouterr()
    assert code == 0
    assert captured.out == "BYOC AWS IaC package passed.\n"


def test_generate_byoc_aws_iac_package_reports_manifest_drift(
    tmp_path: Path,
    capsys,
) -> None:
    _copy_package_tree(tmp_path)
    package_path = tmp_path / "deploy/byoc/aws/iac-package.example.yaml"
    package = yaml.safe_load(package_path.read_text(encoding="utf-8"))
    package["safety"]["required_variables"].append("extra_value")
    package_path.write_text(
        yaml.safe_dump(package, sort_keys=False, width=1_000_000),
        encoding="utf-8",
    )

    code = main(
        [
            "--check-package",
            str(package_path),
            "--repo-root",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert code == 1
    assert "generated_iac_package_drift" in captured.err


def test_generate_byoc_aws_iac_package_reports_terraform_drift(
    tmp_path: Path,
    capsys,
) -> None:
    _copy_package_tree(tmp_path)
    variables = tmp_path / "deploy/byoc/aws/terraform/variables.tf"
    variables.write_text(
        variables.read_text(encoding="utf-8").replace(
            "Customer AWS account identifier",
            "Customer account identifier",
        ),
        encoding="utf-8",
    )

    code = main(
        [
            "--check-package",
            str(tmp_path / "deploy/byoc/aws/iac-package.example.yaml"),
            "--repo-root",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert code == 1
    assert "generated_terraform_file_drift" in captured.err


def test_generate_byoc_aws_iac_package_write_mode(
    tmp_path: Path,
    capsys,
) -> None:
    package_output = tmp_path / "deploy/byoc/aws/iac-package.example.yaml"

    code = main(
        [
            "--write",
            "--package-output",
            str(package_output),
            "--terraform-root",
            Path("deploy/byoc/aws/terraform").as_posix(),
            "--repo-root",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert captured.out == "BYOC AWS IaC package written.\n"
    assert package_output.exists()
    assert (tmp_path / "deploy/byoc/aws/terraform/versions.tf").exists()
    assert (tmp_path / "deploy/byoc/aws/terraform/variables.tf").exists()


def test_generate_byoc_aws_iac_package_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.aws.iac_package.v1"
    )


def _copy_package_tree(tmp_path: Path) -> None:
    for rel in (
        "deploy/byoc/aws/iac-package.example.yaml",
        "deploy/byoc/aws/terraform/versions.tf",
        "deploy/byoc/aws/terraform/variables.tf",
        "deploy/byoc/aws/terraform/locals.tf",
        "deploy/byoc/aws/terraform/outputs.tf",
    ):
        source = ROOT / rel
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
