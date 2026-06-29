from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

from scripts.rehearse_byoc_customer_pilot_package import main


ROOT = Path(__file__).resolve().parents[2]


def test_rehearse_byoc_customer_pilot_package_json_output(capsys) -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-rehearsal-script-{uuid.uuid4().hex}"
    try:
        code = main(["--json", "--repo-root", str(ROOT), "--output-dir", str(output_dir)])

        payload = json.loads(capsys.readouterr().out)
        rendered = json.dumps(payload, sort_keys=True)
        assert code == 0
        assert payload["schema_version"] == "fyralis.byoc.customer_pilot_rehearsal.v1"
        assert payload["status"] == "manual_required"
        assert payload["required_checks_passed"] is True
        assert payload["package_validation_status"] == "pass"
        assert payload["product_health_install_rehearsal_status"] == "pass"
        assert payload["artifact_count"] == 9
        assert payload["verified_artifact_count"] == 9
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


def test_rehearse_byoc_customer_pilot_package_writes_summary(
    capsys,
) -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-rehearsal-script-{uuid.uuid4().hex}"
    summary = output_dir / "summary.json"
    try:
        code = main(
            [
                "--json",
                "--repo-root",
                str(ROOT),
                "--output-dir",
                str(output_dir),
                "--output",
                str(summary),
            ]
        )

        captured = capsys.readouterr()
        assert code == 0
        assert json.loads(summary.read_text(encoding="utf-8")) == json.loads(
            captured.out
        )
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def test_rehearse_byoc_customer_pilot_package_require_ready_fails_manual(
    capsys,
) -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-rehearsal-script-{uuid.uuid4().hex}"
    try:
        code = main(
            [
                "--json",
                "--require-ready",
                "--repo-root",
                str(ROOT),
                "--output-dir",
                str(output_dir),
            ]
        )

        payload = json.loads(capsys.readouterr().out)
        assert code == 1
        assert payload["status"] == "manual_required"
        assert payload["customer_pilot_ready"] is False
        assert "complete_customer_pilot_rehearsal_ready_evidence" in (
            payload["next_actions"]
        )
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)
