from __future__ import annotations

import json
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

from services.platform.runtime.byoc_customer_pilot_package import (
    ByocCustomerPilotPackageInputs,
    build_byoc_customer_pilot_package,
    render_customer_pilot_package_manifest_json,
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
        assert manifest.artifact_count == 7
        assert {artifact.name for artifact in manifest.artifacts} == {
            "evidence_package",
            "evidence_ledger",
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


def test_customer_pilot_package_rejects_output_outside_repo(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="repo_root"):
        build_byoc_customer_pilot_package(
            ByocCustomerPilotPackageInputs(
                output_dir=tmp_path / "outside",
                repo_root=ROOT,
                generated_at=GENERATED_AT,
            )
        )
