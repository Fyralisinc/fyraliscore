from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts.generate_byoc_evidence_ledger import main


ROOT = Path(__file__).resolve().parents[2]
LEDGER = ROOT / "deploy/byoc/evidence-ledger.example.yaml"


def test_generate_byoc_evidence_ledger_yaml_output(capsys) -> None:
    code = main([
        "--generated-at",
        "2026-06-26T12:00:00+00:00",
        "--env-file",
        ".env.production.example",
    ])

    captured = capsys.readouterr()
    payload = yaml.safe_load(captured.out)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.evidence_ledger.v1"
    assert payload["overall_status"] == "pass"


def test_generate_byoc_evidence_ledger_json_output(capsys) -> None:
    code = main([
        "--json",
        "--generated-at",
        "2026-06-26T12:00:00+00:00",
        "--env-file",
        ".env.production.example",
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["export_scope"] == "sanitized_metadata_only"
    assert payload["evidence"][1]["kind"] == "bootstrap_runner"


def test_generate_byoc_evidence_ledger_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.evidence_ledger.v1"
    )


def test_generate_byoc_evidence_ledger_check_passes_checked_in_ledger(
    capsys,
) -> None:
    code = main([
        "--check-ledger",
        str(LEDGER),
        "--env-file",
        ".env.production.example",
    ])

    captured = capsys.readouterr()
    assert code == 0
    assert captured.out == "BYOC evidence ledger passed.\n"


def test_generate_byoc_evidence_ledger_check_reports_drift(
    tmp_path: Path,
    capsys,
) -> None:
    path = tmp_path / "evidence-ledger.yaml"
    path.write_text(
        LEDGER.read_text(encoding="utf-8").replace(
            "sha256:148ea4b590419817e3d1d90727708e2858c99127d3bebe11ad4bcfca080ca661",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        encoding="utf-8",
    )

    code = main([
        "--check-ledger",
        str(path),
        "--env-file",
        ".env.production.example",
    ])

    captured = capsys.readouterr()
    assert code == 1
    assert "generated_ledger_drift" in captured.err


def test_generate_byoc_evidence_ledger_check_rejects_extra_details(
    tmp_path: Path,
    capsys,
) -> None:
    path = tmp_path / "evidence-ledger.yaml"
    path.write_text(
        LEDGER.read_text(encoding="utf-8").replace(
            "  source:",
            "  details: raw report details are not allowed\n  source:",
            1,
        ),
        encoding="utf-8",
    )

    code = main(["--check-ledger", str(path)])

    captured = capsys.readouterr()
    assert code == 1
    assert "Extra inputs are not permitted" in captured.err
