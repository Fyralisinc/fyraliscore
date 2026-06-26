from __future__ import annotations

import json
from pathlib import Path

from scripts.validate_byoc_aws_iac_package import main


ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "deploy/byoc/aws/iac-package.example.yaml"
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
IAM_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"


def test_validate_byoc_aws_iac_package_passes_checked_in_sample(capsys) -> None:
    code = main(
        [
            str(PACKAGE),
            "--dataplane-manifest",
            str(DATAPLANE),
            "--permissions-manifest",
            str(PERMISSIONS),
            "--iam-template",
            str(IAM_TEMPLATE),
            "--repo-root",
            str(ROOT),
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert captured.out == "BYOC AWS IaC package passed.\n"


def test_validate_byoc_aws_iac_package_json_output(capsys) -> None:
    code = main(
        [
            "--json",
            str(PACKAGE),
            "--dataplane-manifest",
            str(DATAPLANE),
            "--permissions-manifest",
            str(PERMISSIONS),
            "--iam-template",
            str(IAM_TEMPLATE),
            "--repo-root",
            str(ROOT),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["valid"] is True
    assert payload["terraform_root_module"] == "deploy/byoc/aws/terraform"
    assert "deploy/byoc/aws/terraform/variables.tf" in payload["terraform_files"]


def test_validate_byoc_aws_iac_package_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.aws.iac_package.v1"
    )


def test_validate_byoc_aws_iac_package_reports_contract_errors(
    tmp_path: Path,
    capsys,
) -> None:
    _copy_package_tree(tmp_path)
    variables = tmp_path / "deploy/byoc/aws/terraform/variables.tf"
    variables.write_text(
        variables.read_text(encoding="utf-8")
        + '\nresource "aws_s3_bucket" "raw" { bucket = "unsafe" }\n',
        encoding="utf-8",
    )

    code = main(
        [
            str(tmp_path / "deploy/byoc/aws/iac-package.example.yaml"),
            "--repo-root",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert code == 1
    assert "terraform_resource_block_forbidden" in captured.err


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
