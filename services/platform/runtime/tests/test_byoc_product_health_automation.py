from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_product_health_automation import (
    ByocProductHealthAutomation,
    generate_product_health_automation,
    load_product_health_automation,
    product_health_automation_json_schema,
    render_product_health_automation_artifacts,
    validate_product_health_automation_contract,
)


ROOT = Path(__file__).resolve().parents[4]
AUTOMATION = ROOT / "deploy/byoc/product-health-automation.example.yaml"
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"


def _automation_data() -> dict:
    return yaml.safe_load(AUTOMATION.read_text(encoding="utf-8"))


def test_checked_in_product_health_automation_matches_contract() -> None:
    automation = load_product_health_automation(AUTOMATION)
    dataplane = load_byoc_manifest(DATAPLANE)
    rendered = render_product_health_automation_artifacts(automation)

    assert validate_product_health_automation_contract(
        automation,
        dataplane_manifest=dataplane,
        repo_root=ROOT,
    ) == []
    assert automation.schema_version == "fyralis.byoc.product_health_automation.v1"
    assert automation.runtime.no_inbound_ports is True
    assert automation.runtime.egress_only_control_plane is True
    assert automation.privacy_boundary.raw_payloads_included is False
    assert automation.stored_scope == (
        "customer_side_product_health_automation_metadata_only"
    )
    assert set(rendered) == {
        "deploy/byoc/kubernetes/product-health-collector.cronjob.example.yaml",
        "deploy/byoc/systemd/product-health-collector.service.example",
        "deploy/byoc/systemd/product-health-collector.timer.example",
    }
    rendered_blob = "\n".join(rendered.values()).lower()
    assert "ports:" not in rendered_blob
    assert "postgresql://" not in rendered_blob
    assert "content_text" not in rendered_blob
    assert "error_summary" not in rendered_blob
    assert "/byoc/control-plane/product-health-snapshots" in rendered_blob


def test_product_health_automation_schema_is_exportable() -> None:
    schema = product_health_automation_json_schema()

    assert schema["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.product_health_automation.v1"
    )


def test_product_health_automation_rejects_dataplane_mismatch() -> None:
    data = _automation_data()
    data["artifact_revision"] = "2026.06.27-drift"
    automation = ByocProductHealthAutomation.model_validate(data)
    dataplane = load_byoc_manifest(DATAPLANE)

    violations = validate_product_health_automation_contract(
        automation,
        dataplane_manifest=dataplane,
        repo_root=ROOT,
    )

    assert "dataplane_manifest_mismatch" in {
        violation.code for violation in violations
    }


def test_product_health_automation_rejects_raw_secret_material() -> None:
    data = deepcopy(_automation_data())
    data["secret_refs"]["database_url_secret_ref"] = "postgresql://raw.example/db"

    with pytest.raises(ValueError):
        ByocProductHealthAutomation.model_validate(data)


def test_product_health_automation_rejects_artifact_drift(tmp_path: Path) -> None:
    _copy_automation_tree(tmp_path)
    artifact = tmp_path / "deploy/byoc/systemd/product-health-collector.timer.example"
    artifact.write_text(
        artifact.read_text(encoding="utf-8").replace("Persistent=true", "Persistent=false"),
        encoding="utf-8",
    )
    automation = load_product_health_automation(
        tmp_path / "deploy/byoc/product-health-automation.example.yaml"
    )
    dataplane = load_byoc_manifest(tmp_path / "deploy/byoc/dataplane.example.yaml")

    violations = validate_product_health_automation_contract(
        automation,
        dataplane_manifest=dataplane,
        repo_root=tmp_path,
    )

    assert "automation_artifact_drift" in {violation.code for violation in violations}


def test_generated_product_health_automation_matches_checked_in_manifest() -> None:
    dataplane = load_byoc_manifest(DATAPLANE)
    generated = generate_product_health_automation(
        dataplane_manifest=dataplane,
        dataplane_manifest_path=Path("deploy/byoc/dataplane.example.yaml"),
        collector_script_path=Path("scripts/run_byoc_product_health_collector.py"),
    )
    checked_in = load_product_health_automation(AUTOMATION)

    assert generated == checked_in


def _copy_automation_tree(tmp_path: Path) -> None:
    for source in (
        DATAPLANE,
        AUTOMATION,
        ROOT / "scripts/run_byoc_product_health_collector.py",
        ROOT / "deploy/byoc/kubernetes/product-health-collector.cronjob.example.yaml",
        ROOT / "deploy/byoc/systemd/product-health-collector.service.example",
        ROOT / "deploy/byoc/systemd/product-health-collector.timer.example",
    ):
        rel = source.relative_to(ROOT)
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
