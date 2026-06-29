from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

from scripts.build_byoc_customer_pilot_package import main as build_main
from scripts.check_byoc_customer_pilot_package import main


ROOT = Path(__file__).resolve().parents[2]


def test_check_byoc_customer_pilot_package_json_output(capsys) -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-package-check-{uuid.uuid4().hex}"
    try:
        assert build_main(["--json", "--repo-root", str(ROOT), "--output-dir", str(output_dir)]) == 0
        capsys.readouterr()

        code = main(
            [
                "--json",
                "--repo-root",
                str(ROOT),
                "--manifest",
                str(output_dir / "byoc-customer-pilot-package-manifest.json"),
            ]
        )

        payload = json.loads(capsys.readouterr().out)
        rendered = json.dumps(payload, sort_keys=True)
        assert code == 0
        assert payload["schema_version"] == (
            "fyralis.byoc.customer_pilot_package_validation.v1"
        )
        assert payload["status"] == "pass"
        assert payload["package_status"] == "manual_required"
        assert payload["verified_artifact_count"] == 7
        assert payload["failure_codes"] == []
        assert "https://" not in rendered
        assert "bearer " not in rendered.lower()
        assert "token=" not in rendered
        assert "arn:aws" not in rendered
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def test_check_byoc_customer_pilot_package_require_ready_fails_manual(capsys) -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-package-check-{uuid.uuid4().hex}"
    try:
        assert build_main(["--json", "--repo-root", str(ROOT), "--output-dir", str(output_dir)]) == 0
        capsys.readouterr()

        code = main(
            [
                "--json",
                "--require-ready",
                "--repo-root",
                str(ROOT),
                "--manifest",
                str(output_dir / "byoc-customer-pilot-package-manifest.json"),
            ]
        )

        payload = json.loads(capsys.readouterr().out)
        assert code == 1
        assert payload["status"] == "pass"
        assert payload["customer_pilot_ready"] is False
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def test_check_byoc_customer_pilot_package_detects_tampered_artifact(capsys) -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-package-check-{uuid.uuid4().hex}"
    try:
        assert build_main(["--json", "--repo-root", str(ROOT), "--output-dir", str(output_dir)]) == 0
        capsys.readouterr()
        (output_dir / "byoc-control-plane-read-smoke-summary.json").write_text(
            json.dumps(
                {
                    "schema_version": (
                        "fyralis.byoc.control_plane_read_smoke_summary.v1"
                    ),
                    "details": "https://control-plane.example token=secret",
                }
            ),
            encoding="utf-8",
        )

        code = main(
            [
                "--json",
                "--repo-root",
                str(ROOT),
                "--manifest",
                str(output_dir / "byoc-customer-pilot-package-manifest.json"),
            ]
        )

        payload = json.loads(capsys.readouterr().out)
        rendered = json.dumps(payload, sort_keys=True)
        assert code == 1
        assert payload["status"] == "fail"
        assert "control_plane_read_smoke_summary_digest_mismatch" in (
            payload["failure_codes"]
        )
        assert "https://control-plane.example" not in rendered
        assert "token=secret" not in rendered
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)
