from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import yaml
import pytest
from pydantic import ValidationError

from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_permissions import (
    ByocAwsIamTemplateSkeleton,
    ByocPermissionsManifest,
    byoc_aws_iam_template_json_schema,
    byoc_permissions_json_schema,
    load_byoc_aws_iam_template,
    load_byoc_permissions_manifest,
    validate_aws_iam_template_contract,
    validate_permissions_manifest_contract,
)


ROOT = Path(__file__).resolve().parents[4]
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
AWS_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"


def _permissions_data() -> dict:
    return yaml.safe_load(PERMISSIONS.read_text(encoding="utf-8"))


def _template_data() -> dict:
    return yaml.safe_load(AWS_TEMPLATE.read_text(encoding="utf-8"))


def test_checked_in_permissions_manifest_matches_dataplane_contract() -> None:
    manifest = load_byoc_permissions_manifest(PERMISSIONS)
    dataplane = load_byoc_manifest(DATAPLANE)

    assert validate_permissions_manifest_contract(
        manifest,
        dataplane_manifest=dataplane,
    ) == []
    assert {role.name for role in manifest.roles} >= {
        "bootstrap_provisioner",
        "data_plane_agent",
        "gateway_runtime",
        "worker_runtime",
        "migration_runner",
    }


def test_checked_in_aws_template_matches_permissions_manifest() -> None:
    manifest = load_byoc_permissions_manifest(PERMISSIONS)
    template = load_byoc_aws_iam_template(AWS_TEMPLATE)

    assert validate_aws_iam_template_contract(
        template,
        permissions_manifest=manifest,
    ) == []


def test_permissions_schema_bundle_is_exportable() -> None:
    permissions_schema = byoc_permissions_json_schema()
    template_schema = byoc_aws_iam_template_json_schema()

    assert permissions_schema["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.permissions.v1"
    )
    assert template_schema["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.aws.iam_skeleton.v1"
    )


def test_permissions_reject_admin_policy_and_wildcard_action() -> None:
    data = _permissions_data()
    role = data["roles"][0]
    role["managed_policy_arns"] = [
        "arn:aws:iam::aws:policy/AdministratorAccess",
    ]
    role["grants"][0]["actions"] = ["iam:*"]
    manifest = ByocPermissionsManifest.model_validate(data)

    violations = validate_permissions_manifest_contract(manifest)

    assert {
        violation.code for violation in violations
    } >= {"forbidden_managed_policy", "wildcard_action_forbidden"}


def test_permissions_reject_control_plane_customer_data_access() -> None:
    data = _permissions_data()
    data["roles"].append(
        {
            "name": "control_plane_preflight",
            "actor": "fyralis_control_plane",
            "trust_boundary": "fyralis_control_plane_readonly",
            "trusted_principal_ref": "arn:aws:iam::999999999999:role/fyralis",
            "permissions_boundary_required": True,
            "permissions_boundary_ref": (
                "arn:aws:iam::123456789012:policy/fyralis-dep-example01-boundary"
            ),
            "managed_policy_arns": [],
            "grants": [
                {
                    "sid": "MutateAndReadSecrets",
                    "effect": "Allow",
                    "actions": ["cloudformation:UpdateStack"],
                    "resource_refs": [
                        (
                            "arn:aws:cloudformation:us-east-1:123456789012:"
                            "stack/fyralis-dep-example01-*/*"
                        )
                    ],
                    "resource_scope": "deployment_stack",
                    "customer_data_access": "secret_material",
                }
            ],
        }
    )
    manifest = ByocPermissionsManifest.model_validate(data)

    violations = validate_permissions_manifest_contract(manifest)

    assert {
        violation.code for violation in violations
    } >= {
        "control_plane_mutation_forbidden",
        "control_plane_customer_data_access_forbidden",
    }


def test_permissions_reject_unscoped_pass_role() -> None:
    data = _permissions_data()
    grant = data["roles"][0]["grants"][2]
    assert grant["sid"] == "PassCloudFormationServiceRoleOnly"
    grant["conditions"] = []
    manifest = ByocPermissionsManifest.model_validate(data)

    violations = validate_permissions_manifest_contract(manifest)

    assert "pass_role_service_condition_required" in {
        violation.code for violation in violations
    }


def test_permissions_reject_regional_wildcard_without_region_condition() -> None:
    data = _permissions_data()
    grant = data["roles"][0]["grants"][0]
    assert grant["sid"] == "ReadAccountAndRegionPreflight"
    grant["conditions"] = []
    manifest = ByocPermissionsManifest.model_validate(data)

    violations = validate_permissions_manifest_contract(manifest)

    assert "wildcard_resource_region_condition_required" in {
        violation.code for violation in violations
    }


def test_permissions_reject_dataplane_identity_mismatch() -> None:
    data = _permissions_data()
    data["region"] = "eu-west-1"
    manifest = ByocPermissionsManifest.model_validate(data)
    dataplane = load_byoc_manifest(DATAPLANE)

    violations = validate_permissions_manifest_contract(
        manifest,
        dataplane_manifest=dataplane,
    )

    assert "dataplane_manifest_mismatch" in {
        violation.code for violation in violations
    }


def test_permissions_schema_rejects_unsafe_global_principles() -> None:
    data = _permissions_data()
    data["principles"]["long_lived_static_keys_allowed"] = True

    with pytest.raises(ValidationError):
        ByocPermissionsManifest.model_validate(data)


def test_aws_template_rejects_unknown_policy_grant_sid() -> None:
    manifest = load_byoc_permissions_manifest(PERMISSIONS)
    data = deepcopy(_template_data())
    data["roles"][0]["policy_grant_sids"].append("UnknownGrant")
    template = ByocAwsIamTemplateSkeleton.model_validate(data)

    violations = validate_aws_iam_template_contract(
        template,
        permissions_manifest=manifest,
    )

    assert "unknown_policy_grant_sid" in {violation.code for violation in violations}
