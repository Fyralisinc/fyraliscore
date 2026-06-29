from __future__ import annotations

import json
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

from services.platform.runtime.byoc_customer_pilot_rehearsal import (
    ByocCustomerPilotRehearsalInputs,
    render_customer_pilot_rehearsal_json,
    run_byoc_customer_pilot_rehearsal,
)


ROOT = Path(__file__).resolve().parents[4]
GENERATED_AT = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)


def test_customer_pilot_rehearsal_builds_and_validates_clean_package() -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-rehearsal-test-{uuid.uuid4().hex}"
    try:
        output_dir.mkdir(parents=True)
        (output_dir / "stale.txt").write_text("remove me", encoding="utf-8")

        report = run_byoc_customer_pilot_rehearsal(
            ByocCustomerPilotRehearsalInputs(
                output_dir=output_dir,
                repo_root=ROOT,
                generated_at=GENERATED_AT,
            )
        )
        rendered = render_customer_pilot_rehearsal_json(report)

        assert report.schema_version == "fyralis.byoc.customer_pilot_rehearsal.v1"
        assert report.status == "manual_required"
        assert report.required_checks_passed is True
        assert report.customer_pilot_ready is False
        assert report.manual_actions_required is True
        assert report.package_status == "manual_required"
        assert report.package_validation_status == "pass"
        assert report.product_health_install_rehearsal_status == "pass"
        assert report.artifact_count == 9
        assert report.verified_artifact_count == 9
        assert "complete_control_plane_read_smoke" in report.next_actions
        assert "complete_live_test_readiness" in report.next_actions
        assert not (output_dir / "stale.txt").exists()
        assert (output_dir / "byoc-customer-pilot-package-manifest.json").exists()
        assert (output_dir / "byoc-customer-pilot-package-validation.json").exists()
        assert (
            output_dir / "byoc-product-health-install-rehearsal-report.json"
        ).exists()
        assert "https://" not in rendered
        assert "postgresql://" not in rendered
        assert "token=" not in rendered
        assert "arn:aws" not in rendered
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def test_customer_pilot_rehearsal_rejects_clean_output_outside_tmp() -> None:
    output_dir = ROOT / "docs" / f"pilot-rehearsal-test-{uuid.uuid4().hex}"

    with pytest.raises(ValueError, match="tmp"):
        run_byoc_customer_pilot_rehearsal(
            ByocCustomerPilotRehearsalInputs(
                output_dir=output_dir,
                repo_root=ROOT,
                generated_at=GENERATED_AT,
            )
        )

    assert not output_dir.exists()


def test_customer_pilot_rehearsal_rendered_summary_matches_output_files() -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-rehearsal-test-{uuid.uuid4().hex}"
    try:
        report = run_byoc_customer_pilot_rehearsal(
            ByocCustomerPilotRehearsalInputs(
                output_dir=output_dir,
                repo_root=ROOT,
                generated_at=GENERATED_AT,
            )
        )
        payload = json.loads(render_customer_pilot_rehearsal_json(report))

        assert payload["output_dir"] == output_dir.relative_to(ROOT).as_posix()
        assert set(payload["artifacts_written"]) == {
            "byoc-control-plane-read-smoke-summary.json",
            "byoc-customer-handoff-bundle-index.json",
            "byoc-customer-handoff-report.json",
            "byoc-customer-pilot-package-manifest.json",
            "byoc-customer-pilot-package-validation.json",
            "byoc-launch-readiness-summary.json",
            "byoc-live-test-readiness.json",
            "byoc-product-health-install-rehearsal-report.json",
        }
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)
