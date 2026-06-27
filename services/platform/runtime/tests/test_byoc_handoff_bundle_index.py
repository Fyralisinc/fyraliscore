from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from services.platform.runtime.byoc_handoff_bundle_index import (
    ByocHandoffBundleArtifact,
    ByocHandoffBundleIndexInputs,
    build_byoc_handoff_bundle_index,
    render_handoff_bundle_index_json,
)


ROOT = Path(__file__).resolve().parents[4]
PACKAGE = ROOT / "deploy/byoc/evidence-package.example.yaml"
LEDGER = ROOT / "deploy/byoc/evidence-ledger.example.yaml"
AUTOMATION = ROOT / "deploy/byoc/product-health-automation.example.yaml"
INSTALL_REHEARSAL = ROOT / "deploy/byoc/product-health-install-rehearsal.example.yaml"
GENERATED_AT = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)


def test_handoff_bundle_index_lists_only_sanitized_artifact_metadata(
    tmp_path: Path,
) -> None:
    package = tmp_path / "evidence-package.yaml"
    ledger = tmp_path / "evidence-ledger.yaml"
    automation = tmp_path / "product-health-automation.yaml"
    install_rehearsal = tmp_path / "product-health-install-rehearsal.yaml"
    shutil.copyfile(PACKAGE, package)
    shutil.copyfile(LEDGER, ledger)
    shutil.copyfile(AUTOMATION, automation)
    shutil.copyfile(INSTALL_REHEARSAL, install_rehearsal)
    smoke_report = tmp_path / "control-plane-smoke.json"
    smoke_report.write_text(
        json.dumps(
            {
                "details": "https://gateway.customer.internal token=secret",
                "headers": {"authorization": "bearer raw"},
            }
        ),
        encoding="utf-8",
    )

    index = build_byoc_handoff_bundle_index(
        ByocHandoffBundleIndexInputs(
            evidence_package_path=package,
            evidence_ledger_path=ledger,
            product_health_automation_path=automation,
            product_health_install_rehearsal_path=install_rehearsal,
            repo_root=tmp_path,
            control_plane_read_smoke_report_path=smoke_report,
            generated_at=GENERATED_AT,
        )
    )
    rendered = render_handoff_bundle_index_json(index)

    assert index.schema_version == "fyralis.byoc.customer_handoff_bundle_index.v1"
    assert index.deployment_id == "dep_example01"
    assert index.customer_id == "cus_example01"
    assert index.artifact_count == 5
    assert index.signed_read_endpoint_count == 6
    assert index.privacy.artifact_bodies_included is False
    assert index.privacy.signed_headers_included is False
    assert {artifact.name for artifact in index.artifacts} == {
        "evidence_package",
        "evidence_ledger",
        "product_health_automation",
        "product_health_install_rehearsal",
        "control_plane_read_smoke_summary",
    }
    assert all(artifact.contents_included is False for artifact in index.artifacts)
    assert all(
        endpoint.response_body_included is False
        for endpoint in index.signed_read_endpoints
    )
    assert "https://gateway.customer.internal" not in rendered
    assert "token=secret" not in rendered
    assert "authorization" not in rendered.lower()
    assert "bearer raw" not in rendered.lower()
    assert "postgresql://" not in rendered
    assert "arn:aws" not in rendered


def test_handoff_bundle_index_declares_signed_read_endpoint_paths_only() -> None:
    index = build_byoc_handoff_bundle_index(
        ByocHandoffBundleIndexInputs(
            evidence_package_path=PACKAGE,
            evidence_ledger_path=LEDGER,
            repo_root=ROOT,
            generated_at=GENERATED_AT,
        )
    )

    endpoints = {endpoint.name: endpoint for endpoint in index.signed_read_endpoints}
    assert endpoints["deployment_overview"].path == (
        "/byoc/control-plane/deployment-overview"
    )
    assert endpoints["control_panel_state"].path == (
        "/byoc/control-plane/control-panel-state"
    )
    assert endpoints["control_panel_state"].optional_query_params == (
        "customer_id",
        "recent_limit",
    )
    assert endpoints["agent_fleet"].signed_read_required is True
    assert endpoints["runner_evidence_receipts"].response_schema_version == (
        "fyralis.byoc.runner_evidence_receipt_list.v1"
    )
    assert all("://" not in endpoint.path for endpoint in endpoints.values())


def test_handoff_bundle_artifact_rejects_unsafe_path() -> None:
    with pytest.raises(ValidationError):
        ByocHandoffBundleArtifact(
            name="unsafe",
            kind="evidence_package",
            required=True,
            present=True,
            path="../private/evidence-package.yaml",
            digest=f"sha256:{'a' * 64}",
            schema_version="fyralis.byoc.evidence_package.v1",
            export_scope="sanitized_customer_handoff_metadata_only",
        )


def test_handoff_bundle_index_rejects_paths_outside_repo() -> None:
    with pytest.raises(ValueError, match="repo_root"):
        build_byoc_handoff_bundle_index(
            ByocHandoffBundleIndexInputs(
                evidence_package_path=PACKAGE,
                evidence_ledger_path=LEDGER,
                repo_root=ROOT / "deploy/byoc/aws",
                generated_at=GENERATED_AT,
            )
        )
