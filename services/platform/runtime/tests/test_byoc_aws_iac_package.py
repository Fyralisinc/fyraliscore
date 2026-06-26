from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from services.platform.runtime.byoc_aws_iac_package import (
    ByocAwsIacPackage,
    byoc_aws_iac_package_json_schema,
    load_byoc_aws_iac_package,
    validate_aws_iac_package_contract,
)
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_permissions import (
    load_byoc_aws_iam_template,
    load_byoc_permissions_manifest,
)


ROOT = Path(__file__).resolve().parents[4]
PACKAGE = ROOT / "deploy/byoc/aws/iac-package.example.yaml"
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
IAM_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"


def _package_data() -> dict:
    return yaml.safe_load(PACKAGE.read_text(encoding="utf-8"))


def test_checked_in_aws_iac_package_matches_contracts() -> None:
    package = load_byoc_aws_iac_package(PACKAGE)
    dataplane = load_byoc_manifest(DATAPLANE)
    permissions = load_byoc_permissions_manifest(PERMISSIONS)
    iam_template = load_byoc_aws_iam_template(IAM_TEMPLATE)

    assert validate_aws_iac_package_contract(
        package,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        iam_template=iam_template,
        repo_root=ROOT,
    ) == []
    assert package.package_status == "scaffold_only"
    assert package.execution.terraform_apply_allowed is False
    assert package.execution.no_inbound_control_plane_ports is True


def test_aws_iac_package_schema_is_exportable() -> None:
    schema = byoc_aws_iac_package_json_schema()

    assert schema["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.aws.iac_package.v1"
    )


def test_aws_iac_package_rejects_identity_mismatch() -> None:
    data = _package_data()
    data["region"] = "eu-west-1"
    package = ByocAwsIacPackage.model_validate(data)
    dataplane = load_byoc_manifest(DATAPLANE)

    violations = validate_aws_iac_package_contract(
        package,
        dataplane_manifest=dataplane,
        repo_root=ROOT,
    )

    assert "dataplane_manifest_mismatch" in {
        violation.code for violation in violations
    }


def test_aws_iac_package_rejects_mutating_terraform_resource(
    tmp_path: Path,
) -> None:
    _copy_package_tree(tmp_path)
    package_path = tmp_path / "deploy/byoc/aws/iac-package.example.yaml"
    variables = tmp_path / "deploy/byoc/aws/terraform/variables.tf"
    variables.write_text(
        variables.read_text(encoding="utf-8")
        + '\nresource "aws_s3_bucket" "raw" { bucket = "unsafe" }\n',
        encoding="utf-8",
    )
    package = load_byoc_aws_iac_package(package_path)

    violations = validate_aws_iac_package_contract(package, repo_root=tmp_path)

    assert "terraform_resource_block_forbidden" in {
        violation.code for violation in violations
    }


def test_aws_iac_package_rejects_sensitive_terraform_fragments(
    tmp_path: Path,
) -> None:
    _copy_package_tree(tmp_path)
    variables = tmp_path / "deploy/byoc/aws/terraform/variables.tf"
    variables.write_text(
        variables.read_text(encoding="utf-8")
        + '\nvariable "install_token_value" { type = string }\n',
        encoding="utf-8",
    )
    package = load_byoc_aws_iac_package(
        tmp_path / "deploy/byoc/aws/iac-package.example.yaml"
    )

    violations = validate_aws_iac_package_contract(package, repo_root=tmp_path)

    assert "terraform_sensitive_or_exec_fragment_forbidden" in {
        violation.code for violation in violations
    }


def test_aws_iac_package_rejects_missing_required_tag(
    tmp_path: Path,
) -> None:
    _copy_package_tree(tmp_path)
    locals_tf = tmp_path / "deploy/byoc/aws/terraform/locals.tf"
    locals_tf.write_text(
        locals_tf.read_text(encoding="utf-8").replace(
            '"fyralis:customer-id"   = var.customer_id\n',
            "",
        ),
        encoding="utf-8",
    )
    package = load_byoc_aws_iac_package(
        tmp_path / "deploy/byoc/aws/iac-package.example.yaml"
    )

    violations = validate_aws_iac_package_contract(package, repo_root=tmp_path)

    assert "required_tag_not_declared" in {violation.code for violation in violations}


def test_aws_iac_package_schema_rejects_apply_enabled() -> None:
    data = deepcopy(_package_data())
    data["execution"]["terraform_apply_allowed"] = True

    with pytest.raises(ValueError):
        ByocAwsIacPackage.model_validate(data)


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
