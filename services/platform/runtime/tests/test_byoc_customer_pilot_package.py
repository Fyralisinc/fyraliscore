from __future__ import annotations

import json
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

from services.platform.runtime.byoc_customer_pilot_package import (
    ByocCustomerPilotPackageInputs,
    ByocCustomerPilotPackageValidationInputs,
    build_byoc_customer_pilot_package,
    render_customer_pilot_package_manifest_json,
    render_customer_pilot_package_validation_json,
    validate_byoc_customer_pilot_package,
)


ROOT = Path(__file__).resolve().parents[4]
GENERATED_AT = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)


def test_customer_pilot_package_builds_sanitized_manual_package() -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-package-test-{uuid.uuid4().hex}"
    try:
        manifest = build_byoc_customer_pilot_package(
            ByocCustomerPilotPackageInputs(
                output_dir=output_dir,
                repo_root=ROOT,
                generated_at=GENERATED_AT,
            )
        )
        rendered = render_customer_pilot_package_manifest_json(manifest)

        assert manifest.schema_version == (
            "fyralis.byoc.customer_pilot_package_manifest.v1"
        )
        assert manifest.status == "manual_required"
        assert manifest.customer_pilot_ready is False
        assert manifest.manual_actions_required is True
        assert "complete_control_plane_read_smoke" in manifest.next_actions
        assert manifest.artifact_count == 8
        assert {artifact.name for artifact in manifest.artifacts} == {
            "evidence_package",
            "evidence_ledger",
            "product_health_automation",
            "live_test_readiness",
            "customer_handoff_readiness",
            "control_plane_read_smoke_summary",
            "handoff_bundle_index",
            "launch_readiness_summary",
        }
        assert all(artifact.contents_included is False for artifact in manifest.artifacts)
        assert all(artifact.digest.startswith("sha256:") for artifact in manifest.artifacts)
        assert (output_dir / "byoc-live-test-readiness.json").exists()
        assert (output_dir / "byoc-customer-handoff-report.json").exists()
        assert (output_dir / "byoc-control-plane-read-smoke-summary.json").exists()
        assert (output_dir / "byoc-customer-handoff-bundle-index.json").exists()
        assert (output_dir / "byoc-launch-readiness-summary.json").exists()
        assert (output_dir / "byoc-customer-pilot-package-manifest.json").exists()
        assert "https://" not in rendered
        assert "bearer " not in rendered.lower()
        assert "token=" not in rendered
        assert "arn:aws" not in rendered

        validation = validate_byoc_customer_pilot_package(
            ByocCustomerPilotPackageValidationInputs(
                manifest_path=(
                    output_dir / "byoc-customer-pilot-package-manifest.json"
                ),
                repo_root=ROOT,
                generated_at=GENERATED_AT,
            )
        )
        assert validation.status == "pass"
        assert validation.package_status == "manual_required"
        assert validation.verified_artifact_count == 8
        assert validation.failure_codes == ()
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def test_customer_pilot_package_manifest_matches_written_file() -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-package-test-{uuid.uuid4().hex}"
    try:
        manifest = build_byoc_customer_pilot_package(
            ByocCustomerPilotPackageInputs(
                output_dir=output_dir,
                repo_root=ROOT,
                generated_at=GENERATED_AT,
            )
        )

        written = json.loads(
            (output_dir / "byoc-customer-pilot-package-manifest.json").read_text(
                encoding="utf-8"
            )
        )
        assert written == json.loads(render_customer_pilot_package_manifest_json(manifest))
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def test_customer_pilot_package_can_use_prebuilt_live_readiness(
    tmp_path: Path,
) -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-package-test-{uuid.uuid4().hex}"
    live_report = tmp_path / "live-ready.json"
    live_report.write_text(json.dumps(_live_ready_report()), encoding="utf-8")
    try:
        manifest = build_byoc_customer_pilot_package(
            ByocCustomerPilotPackageInputs(
                output_dir=output_dir,
                repo_root=ROOT,
                live_test_readiness_path=live_report,
                generated_at=GENERATED_AT,
            )
        )

        copied_live = json.loads(
            (output_dir / "byoc-live-test-readiness.json").read_text(
                encoding="utf-8"
            )
        )
        assert copied_live["live_aws_ready"] is True
        assert copied_live["status"] == "pass"
        assert manifest.status == "manual_required"
        assert "complete_live_test_readiness" not in manifest.next_actions
        assert manifest.next_actions == ("complete_control_plane_read_smoke",)
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def test_customer_pilot_package_rejects_output_outside_repo(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="repo_root"):
        build_byoc_customer_pilot_package(
            ByocCustomerPilotPackageInputs(
                output_dir=tmp_path / "outside",
                repo_root=ROOT,
                generated_at=GENERATED_AT,
            )
        )


def test_customer_pilot_package_validation_fails_on_digest_drift() -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-package-test-{uuid.uuid4().hex}"
    try:
        build_byoc_customer_pilot_package(
            ByocCustomerPilotPackageInputs(
                output_dir=output_dir,
                repo_root=ROOT,
                generated_at=GENERATED_AT,
            )
        )
        (output_dir / "byoc-launch-readiness-summary.json").write_text(
            json.dumps(
                {
                    "schema_version": "fyralis.byoc.launch_readiness_summary.v1",
                    "details": "https://control-plane.example token=secret",
                }
            ),
            encoding="utf-8",
        )

        validation = validate_byoc_customer_pilot_package(
            ByocCustomerPilotPackageValidationInputs(
                manifest_path=(
                    output_dir / "byoc-customer-pilot-package-manifest.json"
                ),
                repo_root=ROOT,
                generated_at=GENERATED_AT,
            )
        )
        rendered = render_customer_pilot_package_validation_json(validation)

        assert validation.status == "fail"
        assert "launch_readiness_summary_digest_mismatch" in validation.failure_codes
        assert "https://control-plane.example" not in rendered
        assert "token=secret" not in rendered
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def _live_ready_report() -> dict[str, object]:
    return {
        "schema_version": "fyralis.byoc.live_test_readiness.v1",
        "status": "pass",
        "required_checks_passed": True,
        "live_aws_ready": True,
        "next_required_action": "run_live_credential_rehearsal",
        "execution_mode": "local_offline",
        "elapsed_seconds": 0.1,
        "deployment_id": "dep_example01",
        "customer_id": "cus_example01",
        "cloud_provider": "aws",
        "region": "us-east-1",
        "artifact_revision": "2026.06.26-1",
        "aws_profile_supplied": False,
        "aws_profile_configured": None,
        "aws_env_credentials_present": True,
        "aws_cli_available": True,
        "expected_aws_account_contract_present": True,
        "mutating_cloud_commands_executed": False,
        "privacy": {
            "aws_api_calls_executed": False,
            "credentials_included": False,
            "account_ids_included": False,
            "arns_included": False,
            "profile_names_included": False,
            "endpoint_urls_included": False,
            "command_output_included": False,
            "raw_customer_data_included": False,
            "raw_payloads_included": False,
            "prompts_included": False,
            "logs_included": False,
            "pii_included": False,
        },
        "checks": (),
    }
