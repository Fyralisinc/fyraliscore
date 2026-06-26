from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import yaml

from services.platform.runtime.byoc_bootstrap_bundle import load_byoc_bootstrap_bundle
from services.platform.runtime.byoc_bootstrap_plan import (
    ByocBootstrapPlanManifest,
    byoc_bootstrap_plan_json_schema,
    generate_bootstrap_plan,
    load_byoc_bootstrap_plan,
    validate_bootstrap_plan_contract,
)
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_permissions import load_byoc_permissions_manifest


ROOT = Path(__file__).resolve().parents[4]
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
BUNDLE = ROOT / "deploy/byoc/bootstrap-bundle.example.yaml"
PLAN = ROOT / "deploy/byoc/bootstrap-plan.example.yaml"
GENERATED_AT = datetime(2026, 6, 26, 12, 0, tzinfo=UTC)


def _inputs():
    return (
        load_byoc_manifest(DATAPLANE),
        load_byoc_permissions_manifest(PERMISSIONS),
        load_byoc_bootstrap_bundle(BUNDLE),
    )


def _source_paths() -> dict:
    return {
        "dataplane": Path("deploy/byoc/dataplane.example.yaml"),
        "permissions": Path("deploy/byoc/permissions.example.yaml"),
        "bootstrap_bundle": Path("deploy/byoc/bootstrap-bundle.example.yaml"),
    }


def _plan_data() -> dict:
    return yaml.safe_load(PLAN.read_text(encoding="utf-8"))


def test_checked_in_bootstrap_plan_matches_generated_contracts() -> None:
    dataplane, permissions, bundle = _inputs()
    checked_in = load_byoc_bootstrap_plan(PLAN)
    generated = generate_bootstrap_plan(
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        source_paths=_source_paths(),
        generated_at=GENERATED_AT,
        repo_root=ROOT,
    )

    assert checked_in == generated
    assert validate_bootstrap_plan_contract(
        checked_in,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        source_paths=_source_paths(),
        repo_root=ROOT,
    ) == []


def test_generated_bootstrap_plan_is_dry_run_only() -> None:
    dataplane, permissions, bundle = _inputs()
    plan = generate_bootstrap_plan(
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        source_paths=_source_paths(),
        generated_at=GENERATED_AT,
        repo_root=ROOT,
    )

    assert [step.order for step in plan.steps] == list(range(1, 11))
    assert all(step.mutates_cloud is False for step in plan.steps)
    assert all(step.requires_cloud_credentials is False for step in plan.steps)
    assert all(step.requires_inbound_connectivity is False for step in plan.steps)


def test_bootstrap_plan_schema_is_exportable() -> None:
    schema = byoc_bootstrap_plan_json_schema()

    assert schema["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.bootstrap_plan.v1"
    )


def test_bootstrap_plan_rejects_mutating_commands() -> None:
    data = _plan_data()
    data["steps"][0]["dry_run_commands"] = ["terraform apply -auto-approve"]
    plan = ByocBootstrapPlanManifest.model_validate(data)

    violations = validate_bootstrap_plan_contract(plan)

    assert "mutating_command_forbidden" in {violation.code for violation in violations}


def test_bootstrap_plan_rejects_order_and_phase_drift() -> None:
    data = _plan_data()
    data["steps"][1]["order"] = 5
    data["steps"][3]["phase"] = "preflight"
    plan = ByocBootstrapPlanManifest.model_validate(data)

    violations = validate_bootstrap_plan_contract(plan)

    assert {
        violation.code for violation in violations
    } >= {"step_order_not_contiguous", "phase_order_regression"}


def test_bootstrap_plan_rejects_source_digest_drift() -> None:
    data = deepcopy(_plan_data())
    data["source_manifests"][0]["digest"] = (
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )
    plan = ByocBootstrapPlanManifest.model_validate(data)

    violations = validate_bootstrap_plan_contract(
        plan,
        source_paths=_source_paths(),
        repo_root=ROOT,
    )

    assert "source_manifest_digest_mismatch" in {
        violation.code for violation in violations
    }


def test_bootstrap_plan_rejects_unknown_references() -> None:
    data = _plan_data()
    data["steps"][6]["artifact_roles"].append("terraform_module")
    data["steps"][6]["role_names"].append("missing_runtime")
    data["steps"][5]["component_names"].append("public_database")
    plan = ByocBootstrapPlanManifest.model_validate(data)
    dataplane, permissions, bundle = _inputs()

    violations = validate_bootstrap_plan_contract(
        plan,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
    )

    assert {
        violation.code for violation in violations
    } >= {
        "unknown_artifact_role",
        "unknown_permission_role",
        "unknown_dataplane_component",
    }
