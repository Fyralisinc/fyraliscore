from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from services.platform.runtime.byoc_bootstrap_bundle import load_byoc_bootstrap_bundle
from services.platform.runtime.byoc_bootstrap_plan import load_byoc_bootstrap_plan
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_evidence_ledger import (
    evidence_envelope_payload,
    generate_evidence_ledger,
    load_byoc_evidence_ledger,
    signed_evidence_envelope,
)
from services.platform.runtime.byoc_evidence_package import (
    ByocEvidencePackage,
    byoc_evidence_package_json_schema,
    generate_evidence_package,
    load_byoc_evidence_package,
    package_source_digests,
    validate_evidence_package_contract,
)
from services.platform.runtime.byoc_permissions import load_byoc_permissions_manifest


ROOT = Path(__file__).resolve().parents[4]
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


def _generate() -> ByocEvidencePackage:
    plan, dataplane, permissions, bundle = _inputs()
    return generate_evidence_package(
        ledger=load_byoc_evidence_ledger(LEDGER),
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        plan=plan,
        ledger_path=LEDGER,
        dataplane_manifest_path=DATAPLANE,
        permissions_manifest_path=PERMISSIONS,
        bootstrap_bundle_path=BUNDLE,
        plan_path=PLAN,
        generated_at=GENERATED_AT,
        repo_root=ROOT,
    )


def _live_report(path: Path) -> Path:
    payload = {
        "status": "pass",
        "required_checks_passed": True,
        "manifest_path": "/customer/private/deploy/byoc/dataplane.yaml",
        "checks": [
            {
                "name": "gateway_health",
                "status": "pass",
                "required": True,
                "details": "https://gateway.customer.internal token=secret",
                "metrics": {"endpoint_url": "https://gateway.customer.internal"},
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
    _, dataplane, _, _ = _inputs()
    payload = evidence_envelope_payload(
        manifest=dataplane,
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


def _package_data() -> dict:
    return yaml.safe_load(PACKAGE.read_text(encoding="utf-8"))


def test_checked_in_evidence_package_matches_generated_contract() -> None:
    plan, dataplane, permissions, bundle = _inputs()
    checked_in = load_byoc_evidence_package(PACKAGE)
    generated = _generate()

    assert checked_in == generated
    assert validate_evidence_package_contract(
        checked_in,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        plan=plan,
        source_digests=package_source_digests(
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            bootstrap_bundle_path=BUNDLE,
            plan_path=PLAN,
            ledger_path=LEDGER,
            repo_root=ROOT,
        ),
    ) == []


def test_evidence_package_is_sanitized_handoff_metadata_only() -> None:
    rendered = PACKAGE.read_text(encoding="utf-8")

    assert "control.fyralis.com" not in rendered
    assert "cosign verify" not in rendered
    assert "ghcr.io" not in rendered
    assert "gateway.customer.internal" not in rendered
    assert "postgresql://user:password" not in rendered
    assert "raw_payloads_included: false" in rendered
    assert "report_details_included: false" in rendered


def test_evidence_package_schema_is_exportable() -> None:
    schema = byoc_evidence_package_json_schema()

    assert schema["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.evidence_package.v1"
    )


def test_evidence_package_rejects_unsafe_artifact_ref() -> None:
    data = _package_data()
    data["source_artifacts"][0]["ref"] = "../private/report.json"

    with pytest.raises(ValidationError):
        ByocEvidencePackage.model_validate(data)


def test_evidence_package_reports_source_digest_drift() -> None:
    plan, dataplane, permissions, bundle = _inputs()
    data = _package_data()
    data["source_artifacts"][0]["digest"] = (
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    )
    package = ByocEvidencePackage.model_validate(data)

    violations = validate_evidence_package_contract(
        package,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        plan=plan,
        source_digests=package_source_digests(
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            bootstrap_bundle_path=BUNDLE,
            plan_path=PLAN,
            ledger_path=LEDGER,
            repo_root=ROOT,
        ),
    )

    assert "source_artifact_digest_mismatch" in {
        violation.code for violation in violations
    }


def test_evidence_package_summarizes_signed_live_report_envelope(
    tmp_path: Path,
) -> None:
    plan, dataplane, permissions, bundle = _inputs()
    ledger_path, envelope_path = _signed_ledger(tmp_path)
    package = generate_evidence_package(
        ledger=load_byoc_evidence_ledger(ledger_path),
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        plan=plan,
        ledger_path=ledger_path,
        dataplane_manifest_path=DATAPLANE,
        permissions_manifest_path=PERMISSIONS,
        bootstrap_bundle_path=BUNDLE,
        plan_path=PLAN,
        post_deploy_envelope_path=envelope_path,
        generated_at=GENERATED_AT,
        repo_root=ROOT,
    )
    rendered = package.model_dump_json()

    assert package.live_report_envelope is not None
    assert package.live_report_envelope.signature_verified is True
    assert package.live_report_envelope.envelope_digest.startswith("sha256:")
    assert package.source_artifacts[-1].ref == "generated:external_evidence_ledger"
    assert "gateway.customer.internal" not in rendered
    assert "postgresql://user:password" not in rendered


def test_signed_evidence_package_requires_envelope(tmp_path: Path) -> None:
    plan, dataplane, permissions, bundle = _inputs()
    ledger_path, _ = _signed_ledger(tmp_path)

    with pytest.raises(ValueError, match="requires --post-deploy-envelope"):
        generate_evidence_package(
            ledger=load_byoc_evidence_ledger(ledger_path),
            dataplane_manifest=dataplane,
            permissions_manifest=permissions,
            bootstrap_bundle=bundle,
            plan=plan,
            ledger_path=ledger_path,
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            bootstrap_bundle_path=BUNDLE,
            plan_path=PLAN,
            generated_at=GENERATED_AT,
            repo_root=ROOT,
        )
