from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

from scripts.build_byoc_customer_pilot_package import main


ROOT = Path(__file__).resolve().parents[2]


def test_build_byoc_customer_pilot_package_json_output(capsys) -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-package-script-{uuid.uuid4().hex}"
    try:
        code = main(["--json", "--repo-root", str(ROOT), "--output-dir", str(output_dir)])

        payload = json.loads(capsys.readouterr().out)
        rendered = json.dumps(payload, sort_keys=True)
        assert code == 0
        assert payload["schema_version"] == (
            "fyralis.byoc.customer_pilot_package_manifest.v1"
        )
        assert payload["status"] == "manual_required"
        assert payload["manual_actions_required"] is True
        assert payload["artifact_count"] == 7
        assert (output_dir / "byoc-customer-pilot-package-manifest.json").exists()
        assert "https://" not in rendered
        assert "bearer " not in rendered.lower()
        assert "token=" not in rendered
        assert "arn:aws" not in rendered
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def test_build_byoc_customer_pilot_package_require_ready_fails_manual(capsys) -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-package-script-{uuid.uuid4().hex}"
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
    finally:
        shutil.rmtree(output_dir, ignore_errors=True)


def test_build_byoc_customer_pilot_package_accepts_live_readiness_input(
    tmp_path: Path,
    capsys,
) -> None:
    output_dir = ROOT / "tmp/byoc" / f"pilot-package-script-{uuid.uuid4().hex}"
    live_report = tmp_path / "live-ready.json"
    live_report.write_text(json.dumps(_live_ready_report()), encoding="utf-8")
    try:
        code = main(
            [
                "--json",
                "--repo-root",
                str(ROOT),
                "--output-dir",
                str(output_dir),
                "--live-test-readiness",
                str(live_report),
            ]
        )

        payload = json.loads(capsys.readouterr().out)
        copied_live = json.loads(
            (output_dir / "byoc-live-test-readiness.json").read_text(
                encoding="utf-8"
            )
        )
        assert code == 0
        assert payload["status"] == "manual_required"
        assert copied_live["status"] == "pass"
        assert copied_live["live_aws_ready"] is True
        assert payload["next_actions"] == ["complete_control_plane_read_smoke"]
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
        "checks": [],
    }
