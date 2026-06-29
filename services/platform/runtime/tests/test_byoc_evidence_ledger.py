from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from services.platform.runtime.byoc_bootstrap_bundle import load_byoc_bootstrap_bundle
from services.platform.runtime.byoc_bootstrap_plan import load_byoc_bootstrap_plan
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_evidence_ledger import (
    ByocDeploymentEvidenceLedger,
    evidence_envelope_payload,
    byoc_evidence_ledger_json_schema,
    generate_evidence_ledger,
    load_evidence_envelope,
    load_byoc_evidence_ledger,
    load_post_deploy_validation_report,
    signed_evidence_envelope,
    validate_evidence_ledger_contract,
    verify_evidence_envelope,
)
from services.platform.runtime.byoc_permissions import load_byoc_permissions_manifest
from services.platform.runtime.byoc_terraform_plan_validation import (
    ByocTerraformPlanValidationInputs,
    render_terraform_plan_validation_json,
    run_byoc_terraform_plan_validation,
)


ROOT = Path(__file__).resolve().parents[4]
PLAN = ROOT / "deploy/byoc/bootstrap-plan.example.yaml"
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
BUNDLE = ROOT / "deploy/byoc/bootstrap-bundle.example.yaml"
IAC_PACKAGE = ROOT / "deploy/byoc/aws/iac-package.example.yaml"
IAM_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"
LEDGER = ROOT / "deploy/byoc/evidence-ledger.example.yaml"
GENERATED_AT = datetime(2026, 6, 26, 12, 0, tzinfo=UTC)
SIGNING_SECRET = "local-evidence-signing-secret"


def _inputs():
    return (
        load_byoc_bootstrap_plan(PLAN),
        load_byoc_manifest(DATAPLANE),
        load_byoc_permissions_manifest(PERMISSIONS),
        load_byoc_bootstrap_bundle(BUNDLE),
    )


def _generate() -> ByocDeploymentEvidenceLedger:
    plan, dataplane, permissions, bundle = _inputs()
    return generate_evidence_ledger(
        plan=plan,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        plan_path=PLAN,
        dataplane_manifest_path=DATAPLANE,
        permissions_manifest_path=PERMISSIONS,
        bootstrap_bundle_path=BUNDLE,
        iac_package_path=IAC_PACKAGE,
        iam_template_path=IAM_TEMPLATE,
        env_path=ROOT / ".env.production.example",
        generated_at=GENERATED_AT,
        repo_root=ROOT,
    )


def _live_report(path: Path, *, failed: bool = False) -> Path:
    payload = {
        "status": "fail" if failed else "pass",
        "required_checks_passed": not failed,
        "manifest_path": "/customer/private/deploy/byoc/dataplane.yaml",
        "env_path": "/customer/private/.env.production",
        "elapsed_seconds": 1.234,
        "checks": [
            {
                "name": "gateway_health",
                "status": "fail" if failed else "pass",
                "required": True,
                "details": (
                    "https://gateway.customer.internal/healthz failed with "
                    "token=super-secret"
                    if failed
                    else "https://gateway.customer.internal/healthz returned 200"
                ),
                "metrics": {"endpoint_url": "https://gateway.customer.internal"},
            },
            {
                "name": "database_rls_safety",
                "status": "pass",
                "required": True,
                "details": "postgresql://user:password@db.internal/fyralis passed",
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


def _ledger_data() -> dict:
    return yaml.safe_load(LEDGER.read_text(encoding="utf-8"))


def test_checked_in_evidence_ledger_matches_generated_contract() -> None:
    plan, dataplane, _, _ = _inputs()
    checked_in = load_byoc_evidence_ledger(LEDGER)
    generated = _generate()

    assert checked_in == generated
    assert validate_evidence_ledger_contract(
        checked_in,
        dataplane_manifest=dataplane,
        plan=plan,
    ) == []
    terraform = next(
        evidence
        for evidence in checked_in.evidence
        if evidence.kind == "terraform_plan_validation"
    )
    assert terraform.source.type == "local_terraform_validator"
    assert terraform.operation_counts["contract_only_validation"] == 1
    assert terraform.operation_counts["terraform_modules"] == 5


def test_evidence_ledger_is_sanitized_metadata_only() -> None:
    rendered = LEDGER.read_text(encoding="utf-8")

    assert "cosign verify" not in rendered
    assert "helm template" not in rendered
    assert "ghcr.io" not in rendered
    assert "terraform plan" not in rendered.lower()
    assert "terraform apply" not in rendered.lower()
    assert "control.fyralis.example" not in rendered
    assert "raw_payloads_included: false" in rendered
    assert "command_output_included: false" in rendered


def test_evidence_ledger_schema_is_exportable() -> None:
    schema = byoc_evidence_ledger_json_schema()

    assert schema["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.evidence_ledger.v1"
    )


def test_evidence_ledger_rejects_missing_required_evidence() -> None:
    data = _ledger_data()
    data["evidence"] = data["evidence"][:2]
    ledger = ByocDeploymentEvidenceLedger.model_validate(data)

    violations = validate_evidence_ledger_contract(ledger)

    assert "missing_required_evidence" in {
        violation.code for violation in violations
    }


def test_evidence_ledger_rejects_unsafe_source_ref() -> None:
    data = _ledger_data()
    data["evidence"][0]["source"]["ref"] = "../secret-report.json"

    with pytest.raises(ValidationError):
        ByocDeploymentEvidenceLedger.model_validate(data)


def test_evidence_ledger_rejects_url_source_ref() -> None:
    data = _ledger_data()
    data["evidence"][0]["source"]["ref"] = "https://example.com/report.json"

    with pytest.raises(ValidationError):
        ByocDeploymentEvidenceLedger.model_validate(data)


def test_evidence_ledger_rejects_detail_field() -> None:
    data = deepcopy(_ledger_data())
    data["evidence"][0]["details"] = "raw report details are not allowed"

    with pytest.raises(ValidationError):
        ByocDeploymentEvidenceLedger.model_validate(data)


def test_evidence_ledger_summarizes_live_report_without_details(
    tmp_path: Path,
) -> None:
    plan, dataplane, permissions, bundle = _inputs()
    report_path = _live_report(tmp_path / "post-deploy-report.json")

    ledger = generate_evidence_ledger(
        plan=plan,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        plan_path=PLAN,
        dataplane_manifest_path=DATAPLANE,
        permissions_manifest_path=PERMISSIONS,
        bootstrap_bundle_path=BUNDLE,
        iac_package_path=IAC_PACKAGE,
        iam_template_path=IAM_TEMPLATE,
        post_deploy_report_path=report_path,
        generated_at=GENERATED_AT,
        repo_root=ROOT,
    )
    rendered = ledger.model_dump_json()
    validation = next(
        evidence
        for evidence in ledger.evidence
        if evidence.kind == "post_deploy_validation"
    )

    assert validation.source.type == "post_deploy_report_file"
    assert validation.check_summary.total == 2
    assert "gateway.customer.internal" not in rendered
    assert "super-secret" not in rendered
    assert "postgresql://user:password" not in rendered


def test_evidence_ledger_imports_terraform_validation_report_safely(
    tmp_path: Path,
) -> None:
    plan, dataplane, permissions, bundle = _inputs()
    report = run_byoc_terraform_plan_validation(
        ByocTerraformPlanValidationInputs(
            iac_package_path=IAC_PACKAGE,
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            repo_root=ROOT,
        )
    )
    report_path = tmp_path / "terraform-validation-report.json"
    report_path.write_text(
        render_terraform_plan_validation_json(report),
        encoding="utf-8",
    )

    ledger = generate_evidence_ledger(
        plan=plan,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        plan_path=PLAN,
        dataplane_manifest_path=DATAPLANE,
        permissions_manifest_path=PERMISSIONS,
        bootstrap_bundle_path=BUNDLE,
        iac_package_path=IAC_PACKAGE,
        iam_template_path=IAM_TEMPLATE,
        terraform_validation_report_path=report_path,
        generated_at=GENERATED_AT,
        repo_root=ROOT,
    )
    terraform = next(
        evidence
        for evidence in ledger.evidence
        if evidence.kind == "terraform_plan_validation"
    )

    assert terraform.source.type == "terraform_plan_report_file"
    assert terraform.source.ref == "generated:external_terraform_plan_report"
    assert terraform.check_summary.failed == 0
    assert "terraform plan" not in ledger.model_dump_json().lower()


def test_evidence_ledger_verifies_signed_live_report_envelope(
    tmp_path: Path,
) -> None:
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
        iac_package_path=IAC_PACKAGE,
        iam_template_path=IAM_TEMPLATE,
        post_deploy_report_path=report_path,
        post_deploy_envelope_path=envelope_path,
        evidence_signing_secret=SIGNING_SECRET,
        envelope_verified_at=GENERATED_AT + timedelta(minutes=5),
        generated_at=GENERATED_AT,
        repo_root=ROOT,
    )
    validation = next(
        evidence
        for evidence in ledger.evidence
        if evidence.kind == "post_deploy_validation"
    )

    assert validation.source.type == "signed_post_deploy_report_file"
    assert validation.signature_verified is True
    assert validation.envelope_digest is not None
    assert validation.source.ref == "generated:external_post_deploy_report"


def test_evidence_envelope_rejects_digest_mismatch(tmp_path: Path) -> None:
    _, dataplane, _, _ = _inputs()
    report_path = _live_report(tmp_path / "post-deploy-report.json")
    envelope = load_evidence_envelope(_envelope(tmp_path / "envelope.json", report_path))
    report_path.write_text('{"status":"pass","required_checks_passed":true,"checks":[]}')

    violations = verify_evidence_envelope(
        envelope,
        report_path=report_path,
        manifest=dataplane,
        signing_secret=SIGNING_SECRET,
        verified_at=GENERATED_AT + timedelta(minutes=5),
    )

    assert "report_digest_mismatch" in {violation.code for violation in violations}


def test_evidence_envelope_rejects_invalid_signature(tmp_path: Path) -> None:
    _, dataplane, _, _ = _inputs()
    report_path = _live_report(tmp_path / "post-deploy-report.json")
    envelope = load_evidence_envelope(_envelope(tmp_path / "envelope.json", report_path))

    violations = verify_evidence_envelope(
        envelope,
        report_path=report_path,
        manifest=dataplane,
        signing_secret="wrong-secret",
        verified_at=GENERATED_AT + timedelta(minutes=5),
    )

    assert "invalid_signature" in {violation.code for violation in violations}


def test_evidence_envelope_rejects_expired_envelope(tmp_path: Path) -> None:
    _, dataplane, _, _ = _inputs()
    report_path = _live_report(tmp_path / "post-deploy-report.json")
    envelope = load_evidence_envelope(_envelope(tmp_path / "envelope.json", report_path))

    violations = verify_evidence_envelope(
        envelope,
        report_path=report_path,
        manifest=dataplane,
        signing_secret=SIGNING_SECRET,
        verified_at=GENERATED_AT + timedelta(hours=2),
    )

    assert "envelope_expired" in {violation.code for violation in violations}


def test_signed_live_report_requires_signing_secret(tmp_path: Path) -> None:
    plan, dataplane, permissions, bundle = _inputs()
    report_path = _live_report(tmp_path / "post-deploy-report.json")
    envelope_path = _envelope(tmp_path / "post-deploy-envelope.json", report_path)

    with pytest.raises(ValueError, match="signing secret"):
        generate_evidence_ledger(
            plan=plan,
            dataplane_manifest=dataplane,
            permissions_manifest=permissions,
            bootstrap_bundle=bundle,
            plan_path=PLAN,
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            bootstrap_bundle_path=BUNDLE,
            iac_package_path=IAC_PACKAGE,
            iam_template_path=IAM_TEMPLATE,
            post_deploy_report_path=report_path,
            post_deploy_envelope_path=envelope_path,
            generated_at=GENERATED_AT,
            repo_root=ROOT,
        )


def test_evidence_ledger_fails_from_live_report_failure(
    tmp_path: Path,
) -> None:
    plan, dataplane, permissions, bundle = _inputs()
    report_path = _live_report(tmp_path / "post-deploy-report.json", failed=True)

    ledger = generate_evidence_ledger(
        plan=plan,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        plan_path=PLAN,
        dataplane_manifest_path=DATAPLANE,
        permissions_manifest_path=PERMISSIONS,
        bootstrap_bundle_path=BUNDLE,
        iac_package_path=IAC_PACKAGE,
        iam_template_path=IAM_TEMPLATE,
        post_deploy_report_path=report_path,
        generated_at=GENERATED_AT,
        repo_root=ROOT,
    )
    validation = next(
        evidence
        for evidence in ledger.evidence
        if evidence.kind == "post_deploy_validation"
    )

    assert ledger.overall_status == "fail"
    assert validation.status == "fail"
    assert validation.failed_check_codes == ("gateway_health",)


def test_live_report_import_rejects_sensitive_check_names(tmp_path: Path) -> None:
    path = _live_report(tmp_path / "post-deploy-report.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["checks"][0]["name"] = "credential_probe"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValidationError):
        load_post_deploy_validation_report(path)
