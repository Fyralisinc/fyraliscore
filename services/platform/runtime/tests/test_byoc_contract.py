from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    byoc_manifest_json_schema,
    effective_runtime_processes,
    load_byoc_manifest,
    validate_byoc_manifest_contract,
)
from services.platform.runtime.process_manifest import production_processes


ROOT = Path(__file__).resolve().parents[4]


def _valid_manifest() -> dict:
    return {
        "schema_version": "fyralis.byoc.dataplane.v1",
        "deployment_id": "dep_test001",
        "customer_id": "cus_test001",
        "environment": "prod",
        "cloud_provider": "aws",
        "region": "us-east-1",
        "artifact_revision": "2026.06.26-1",
        "connectivity": {
            "direction": "egress_only",
            "protocol": "https",
            "port": 443,
            "auth": "mtls",
            "control_plane_url": "https://control.fyralis.com",
            "agent_poll_interval_seconds": 30,
            "heartbeat_interval_seconds": 15,
            "fail_closed_for_new_config_after": "24h",
            "continue_serving_local_traffic_when_disconnected": True,
        },
        "network": {
            "customer_ingress_components": ["gateway"],
            "endpoint_exposure": [
                {"component": "gateway", "exposure": "customer_ingress"},
                {"component": "postgres", "exposure": "private"},
                {"component": "broker", "exposure": "private"},
                {"component": "object_storage", "exposure": "private"},
                {"component": "redis", "exposure": "private"},
                {"component": "embedding", "exposure": "private"},
                {"component": "observability", "exposure": "private"},
                {"component": "metrics", "exposure": "private"},
                {"component": "data_plane_agent", "exposure": "private"},
            ],
            "private_service_endpoints": [
                "postgres",
                "broker",
                "object_storage",
                "redis",
                "embedding",
                "observability",
                "metrics",
                "data_plane_agent",
            ],
            "control_plane_inbound_allowed": False,
        },
        "identity": {
            "runtime_identity": "workload_identity",
            "provisioner_identity_ref": (
                "arn:aws:iam::123456789012:role/fyralis-test-provisioner"
            ),
            "agent_identity_ref": "arn:aws:iam::123456789012:role/fyralis-test-agent",
        },
        "secrets": {
            "provider": "aws-secrets-manager",
            "region": "us-east-1",
            "master_kek_secret_ref": "prod/fyralis/test/master-kek",
            "bootstrap_token_secret_ref": "prod/fyralis/test/bootstrap-token",
            "agent_client_certificate_secret_ref": "prod/fyralis/test/agent-cert",
            "raw_env_secrets_allowed": False,
        },
        "telemetry": {
            "mode": "aggregate-only",
            "contract": "aggregate-only-v1",
            "max_batch_bytes": 262144,
            "raw_logs_allowed": False,
            "raw_payloads_allowed": False,
            "raw_prompts_allowed": False,
            "pii_allowed": False,
            "allowlisted_label_keys": [
                "deployment_id",
                "region",
                "component",
                "version",
                "route_template",
                "source_family",
                "status_class",
                "error_code",
                "resource_class",
                "phase",
            ],
        },
        "data_residency": {
            "raw_payloads_leave_boundary": False,
            "prompts_leave_boundary": False,
            "embeddings_leave_boundary": False,
            "logs_leave_boundary": False,
            "pii_leaves_boundary": False,
            "provider_secrets_leave_boundary": False,
        },
        "runtime": {
            "process_manifest": "production",
            "per_source_isolation": True,
            "allowed_source_families": ["slack", "github", "gmail"],
            "disabled_processes": [
                {
                    "name": "telegram_gateway_worker",
                    "reason": "source family not enabled",
                }
            ],
        },
    }


def test_checked_in_byoc_manifest_passes_contract() -> None:
    manifest = load_byoc_manifest(ROOT / "deploy/byoc/dataplane.example.yaml")

    assert validate_byoc_manifest_contract(manifest) == []
    enabled = {process.name for process in effective_runtime_processes(manifest)}
    assert "gateway" in enabled
    assert "telegram_gateway_worker" not in enabled


def test_byoc_contract_rejects_control_plane_inbound_and_raw_data_egress() -> None:
    data = _valid_manifest()
    data["connectivity"]["direction"] = "inbound"

    with pytest.raises(ValidationError, match="egress_only"):
        ByocDataPlaneManifest.model_validate(data)

    data = _valid_manifest()
    data["data_residency"]["raw_payloads_leave_boundary"] = True

    with pytest.raises(ValidationError, match="False"):
        ByocDataPlaneManifest.model_validate(data)


def test_byoc_contract_flags_public_or_non_gateway_ingress() -> None:
    data = _valid_manifest()
    data["network"]["endpoint_exposure"].append(
        {"component": "postgres", "exposure": "public"}
    )
    data["network"]["endpoint_exposure"].append(
        {"component": "metrics", "exposure": "customer_ingress"}
    )
    manifest = ByocDataPlaneManifest.model_validate(data)

    violations = validate_byoc_manifest_contract(manifest)

    assert [(v.code, v.path) for v in violations] == [
        ("public_endpoint_forbidden", "network.endpoint_exposure.postgres"),
        ("customer_ingress_not_allowed", "network.endpoint_exposure.metrics"),
    ]


def test_byoc_contract_flags_missing_private_services_and_unsafe_labels() -> None:
    data = _valid_manifest()
    data["network"]["private_service_endpoints"] = ["postgres"]
    data["telemetry"]["allowlisted_label_keys"].extend(["tenant_id", "file_path"])
    manifest = ByocDataPlaneManifest.model_validate(data)

    violations = validate_byoc_manifest_contract(manifest)

    assert "missing_private_service_endpoint" in {v.code for v in violations}
    assert {
        v.path for v in violations if v.code == "unsafe_telemetry_label"
    } == {
        "telemetry.allowlisted_label_keys.tenant_id",
        "telemetry.allowlisted_label_keys.file_path",
    }


def test_byoc_contract_flags_unknown_or_duplicate_disabled_processes() -> None:
    data = _valid_manifest()
    data["runtime"]["disabled_processes"] = [
        {"name": "gateway", "reason": "bad"},
        {"name": "missing_worker", "reason": "bad"},
        {"name": "missing_worker", "reason": "bad again"},
    ]
    manifest = ByocDataPlaneManifest.model_validate(data)

    violations = validate_byoc_manifest_contract(manifest)

    assert [v.code for v in violations] == [
        "duplicate_disabled_process",
        "gateway_required",
        "unknown_runtime_process",
        "unknown_runtime_process",
    ]


def test_byoc_effective_processes_start_from_production_manifest() -> None:
    manifest = ByocDataPlaneManifest.model_validate(_valid_manifest())

    enabled = {process.name for process in effective_runtime_processes(manifest)}
    expected = {process.name for process in production_processes()}

    assert enabled == expected - {"telegram_gateway_worker"}


def test_byoc_json_schema_is_available_for_control_plane_contracts() -> None:
    schema = byoc_manifest_json_schema()

    assert schema["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.dataplane.v1"
    )
    assert "connectivity" in schema["properties"]
