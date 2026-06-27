from __future__ import annotations

import json
import shutil
from pathlib import Path

from scripts.generate_byoc_handoff_bundle_index import main


ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "deploy/byoc/evidence-package.example.yaml"
LEDGER = ROOT / "deploy/byoc/evidence-ledger.example.yaml"
AUTOMATION = ROOT / "deploy/byoc/product-health-automation.example.yaml"
INSTALL_REHEARSAL = ROOT / "deploy/byoc/product-health-install-rehearsal.example.yaml"


def test_generate_byoc_handoff_bundle_index_json_output(capsys) -> None:
    code = main(
        [
            "--json",
            "--evidence-package",
            str(PACKAGE),
            "--evidence-ledger",
            str(LEDGER),
            "--repo-root",
            str(ROOT),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["schema_version"] == (
        "fyralis.byoc.customer_handoff_bundle_index.v1"
    )
    assert payload["deployment_id"] == "dep_example01"
    assert payload["customer_id"] == "cus_example01"
    assert payload["artifact_count"] == 4
    assert payload["signed_read_endpoint_count"] == 6
    assert {artifact["name"] for artifact in payload["artifacts"]} == {
        "evidence_package",
        "evidence_ledger",
        "product_health_automation",
        "product_health_install_rehearsal",
    }
    assert "deployment_overview" in {
        endpoint["name"] for endpoint in payload["signed_read_endpoints"]
    }
    assert "control_panel_state" in {
        endpoint["name"] for endpoint in payload["signed_read_endpoints"]
    }
    assert payload["privacy"]["artifact_bodies_included"] is False
    assert payload["privacy"]["signed_headers_included"] is False
    assert "https://" not in rendered
    assert "postgresql://" not in rendered
    assert "arn:aws" not in rendered
    assert "token=" not in rendered
    assert "secret_ref" not in rendered.lower()


def test_generate_byoc_handoff_bundle_index_writes_output(
    tmp_path: Path,
    capsys,
) -> None:
    output = tmp_path / "handoff-index.json"

    code = main(
        [
            "--json",
            "--evidence-package",
            str(PACKAGE),
            "--evidence-ledger",
            str(LEDGER),
            "--repo-root",
            str(ROOT),
            "--output",
            str(output),
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8")) == json.loads(captured.out)


def test_generate_byoc_handoff_bundle_index_indexes_optional_artifact_by_digest(
    tmp_path: Path,
    capsys,
) -> None:
    package = tmp_path / "evidence-package.yaml"
    ledger = tmp_path / "evidence-ledger.yaml"
    automation = tmp_path / "product-health-automation.yaml"
    install_rehearsal = tmp_path / "product-health-install-rehearsal.yaml"
    shutil.copyfile(PACKAGE, package)
    shutil.copyfile(LEDGER, ledger)
    shutil.copyfile(AUTOMATION, automation)
    shutil.copyfile(INSTALL_REHEARSAL, install_rehearsal)
    handoff_report = tmp_path / "byoc-customer-handoff-report.json"
    handoff_report.write_text(
        json.dumps(
            {
                "schema_version": "fyralis.byoc.customer_handoff_readiness.v1",
                "details": "https://gateway.customer.internal token=secret",
            }
        ),
        encoding="utf-8",
    )

    code = main(
        [
            "--json",
            "--evidence-package",
            str(package),
            "--evidence-ledger",
            str(ledger),
            "--product-health-automation",
            str(automation),
            "--product-health-install-rehearsal",
            str(install_rehearsal),
            "--repo-root",
            str(tmp_path),
            "--customer-handoff-report",
            str(handoff_report),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    optional = [
        artifact
        for artifact in payload["artifacts"]
        if artifact["name"] == "customer_handoff_readiness_report"
    ][0]
    assert code == 0
    assert optional["present"] is True
    assert optional["digest"].startswith("sha256:")
    assert "https://gateway.customer.internal" not in rendered
    assert "token=secret" not in rendered


def test_generate_byoc_handoff_bundle_index_indexes_control_plane_smoke_summary(
    tmp_path: Path,
    capsys,
) -> None:
    package = tmp_path / "evidence-package.yaml"
    ledger = tmp_path / "evidence-ledger.yaml"
    automation = tmp_path / "product-health-automation.yaml"
    install_rehearsal = tmp_path / "product-health-install-rehearsal.yaml"
    shutil.copyfile(PACKAGE, package)
    shutil.copyfile(LEDGER, ledger)
    shutil.copyfile(AUTOMATION, automation)
    shutil.copyfile(INSTALL_REHEARSAL, install_rehearsal)
    smoke_summary = tmp_path / "control-plane-read-smoke-summary.json"
    smoke_summary.write_text(
        json.dumps(
            {
                "schema_version": (
                    "fyralis.byoc.control_plane_read_smoke_summary.v1"
                ),
                "status": "manual_required",
                "details": "https://gateway.customer.internal token=secret",
            }
        ),
        encoding="utf-8",
    )

    code = main(
        [
            "--json",
            "--evidence-package",
            str(package),
            "--evidence-ledger",
            str(ledger),
            "--product-health-automation",
            str(automation),
            "--product-health-install-rehearsal",
            str(install_rehearsal),
            "--repo-root",
            str(tmp_path),
            "--control-plane-read-smoke-summary",
            str(smoke_summary),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    rendered = json.dumps(payload, sort_keys=True)
    optional = [
        artifact
        for artifact in payload["artifacts"]
        if artifact["name"] == "control_plane_read_smoke_summary"
    ][0]
    assert code == 0
    assert optional["schema_version"] == (
        "fyralis.byoc.control_plane_read_smoke_summary.v1"
    )
    assert optional["export_scope"] == (
        "sanitized_control_plane_read_smoke_metadata_only"
    )
    assert optional["digest"].startswith("sha256:")
    assert "https://gateway.customer.internal" not in rendered
    assert "token=secret" not in rendered
