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
