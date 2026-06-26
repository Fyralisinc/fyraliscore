from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml

from scripts.generate_byoc_evidence_package import main
from services.platform.runtime.byoc_bootstrap_bundle import load_byoc_bootstrap_bundle
from services.platform.runtime.byoc_bootstrap_plan import load_byoc_bootstrap_plan
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_evidence_ledger import (
    evidence_envelope_payload,
    generate_evidence_ledger,
    signed_evidence_envelope,
)
from services.platform.runtime.byoc_permissions import load_byoc_permissions_manifest


ROOT = Path(__file__).resolve().parents[2]
PLAN = ROOT / "deploy/byoc/bootstrap-plan.example.yaml"
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
BUNDLE = ROOT / "deploy/byoc/bootstrap-bundle.example.yaml"
LEDGER = ROOT / "deploy/byoc/evidence-ledger.example.yaml"
PACKAGE = ROOT / "deploy/byoc/evidence-package.example.yaml"
GENERATED_AT = datetime(2026, 6, 26, 12, 0, tzinfo=UTC)
SIGNING_SECRET = "local-evidence-signing-secret"


def _inputs():
    return (
        load_byoc_bootstrap_plan(PLAN),
        load_byoc_manifest(DATAPLANE),
        load_byoc_permissions_manifest(PERMISSIONS),
        load_byoc_bootstrap_bundle(BUNDLE),
    )


def _live_report(path: Path) -> Path:
    payload = {
        "status": "pass",
        "required_checks_passed": True,
        "checks": [
            {
                "name": "gateway_health",
                "status": "pass",
                "required": True,
                "details": "https://gateway.customer.internal token=secret",
            },
            {
                "name": "database_rls_safety",
                "status": "pass",
                "required": True,
                "details": "postgresql://user:password@db.internal/fyralis",
            },
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _envelope(path: Path, report_path: Path) -> Path:
    manifest = load_byoc_manifest(DATAPLANE)
    payload = evidence_envelope_payload(
        manifest=manifest,
        report_path=report_path,
        agent_id="agt_example01",
        nonce="nonce-for-evidence-envelope-001",
        issued_at=GENERATED_AT,
        expires_at=GENERATED_AT + timedelta(hours=1),
    )
    envelope = signed_evidence_envelope(payload, signing_secret=SIGNING_SECRET)
    path.write_text(
        json.dumps(envelope.model_dump(mode="json"), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path


def _signed_ledger(tmp_path: Path) -> tuple[Path, Path]:
    plan, dataplane, permissions, bundle = _inputs()
    report_path = _live_report(tmp_path / "post-deploy-report.json")
    envelope_path = _envelope(tmp_path / "post-deploy-envelope.json", report_path)
    ledger = generate_evidence_ledger(
        plan=plan,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        plan_path=PLAN,
        dataplane_manifest_path=DATAPLANE,
        permissions_manifest_path=PERMISSIONS,
        bootstrap_bundle_path=BUNDLE,
        post_deploy_report_path=report_path,
        post_deploy_envelope_path=envelope_path,
        evidence_signing_secret=SIGNING_SECRET,
        envelope_verified_at=GENERATED_AT + timedelta(minutes=5),
        generated_at=GENERATED_AT,
        repo_root=ROOT,
    )
    ledger_path = tmp_path / "evidence-ledger.yaml"
    ledger_path.write_text(
        yaml.safe_dump(
            ledger.model_dump(mode="json", exclude_none=True),
            sort_keys=False,
            width=1_000_000,
        ),
        encoding="utf-8",
    )
    return ledger_path, envelope_path


def test_generate_byoc_evidence_package_yaml_output(capsys) -> None:
    code = main(["--generated-at", "2026-06-26T12:00:00+00:00"])

    captured = capsys.readouterr()
    payload = yaml.safe_load(captured.out)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.evidence_package.v1"
    assert payload["export_scope"] == "sanitized_customer_handoff_metadata_only"
    assert "aws_iac_package" in {
        artifact["kind"] for artifact in payload["source_artifacts"]
    }


def test_generate_byoc_evidence_package_json_output(capsys) -> None:
    code = main([
        "--json",
        "--generated-at",
        "2026-06-26T12:00:00+00:00",
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert "aws_iac_package" in {
        artifact["kind"] for artifact in payload["source_artifacts"]
    }
    assert payload["source_artifacts"][-1]["kind"] == "evidence_ledger"
    assert payload["ledger"]["schema_version"] == "fyralis.byoc.evidence_ledger.v1"


def test_generate_byoc_evidence_package_check_passes(capsys) -> None:
    code = main(["--check-package", str(PACKAGE)])

    captured = capsys.readouterr()
    assert code == 0
    assert captured.out == "BYOC evidence package passed.\n"


def test_generate_byoc_evidence_package_check_reports_drift(
    tmp_path: Path,
    capsys,
) -> None:
    path = tmp_path / "evidence-package.yaml"
    package = yaml.safe_load(PACKAGE.read_text(encoding="utf-8"))
    package["source_artifacts"][0]["digest"] = (
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )
    path.write_text(
        yaml.safe_dump(package, sort_keys=False, width=1_000_000),
        encoding="utf-8",
    )

    code = main(["--check-package", str(path)])

    captured = capsys.readouterr()
    assert code == 1
    assert "generated_package_drift" in captured.err
    assert "source_artifact_digest_mismatch" in captured.err


def test_generate_byoc_evidence_package_summarizes_signed_envelope(
    tmp_path: Path,
    capsys,
) -> None:
    ledger_path, envelope_path = _signed_ledger(tmp_path)

    code = main([
        "--json",
        "--generated-at",
        "2026-06-26T12:00:00+00:00",
        "--ledger",
        str(ledger_path),
        "--post-deploy-envelope",
        str(envelope_path),
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload)
    assert code == 0
    assert payload["live_report_envelope"]["signature_verified"] is True
    assert payload["source_artifacts"][-1]["ref"] == "generated:external_evidence_ledger"
    assert "gateway.customer.internal" not in rendered
    assert "postgresql://user:password" not in rendered


def test_generate_byoc_evidence_package_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.evidence_package.v1"
    )
