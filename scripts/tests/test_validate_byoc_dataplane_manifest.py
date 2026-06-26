from __future__ import annotations

import json
from pathlib import Path

from scripts.validate_byoc_dataplane_manifest import main


ROOT = Path(__file__).resolve().parents[2]


def test_validate_byoc_dataplane_manifest_passes_checked_in_sample(
    capsys,
) -> None:
    code = main([str(ROOT / "deploy/byoc/dataplane.example.yaml")])

    captured = capsys.readouterr()
    assert code == 0
    assert captured.out == "BYOC data-plane manifest passed.\n"


def test_validate_byoc_dataplane_manifest_json_output(capsys) -> None:
    code = main(["--json", str(ROOT / "deploy/byoc/dataplane.example.yaml")])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["valid"] is True
    assert payload["schema_errors"] == []
    assert "gateway" in payload["effective_runtime_processes"]


def test_validate_byoc_dataplane_manifest_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.dataplane.v1"
    )


def test_validate_byoc_dataplane_manifest_reports_contract_errors(
    tmp_path: Path,
    capsys,
) -> None:
    source = ROOT / "deploy/byoc/dataplane.example.yaml"
    manifest = source.read_text(encoding="utf-8")
    manifest = manifest.replace("exposure: private", "exposure: public", 1)
    path = tmp_path / "unsafe.yaml"
    path.write_text(manifest, encoding="utf-8")

    code = main([str(path)])

    captured = capsys.readouterr()
    assert code == 1
    assert "public_endpoint_forbidden" in captured.err
