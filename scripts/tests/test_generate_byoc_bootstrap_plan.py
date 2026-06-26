from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts.generate_byoc_bootstrap_plan import main


ROOT = Path(__file__).resolve().parents[2]
PLAN = ROOT / "deploy/byoc/bootstrap-plan.example.yaml"


def test_generate_byoc_bootstrap_plan_yaml_output(capsys) -> None:
    code = main(["--generated-at", "2026-06-26T12:00:00+00:00"])

    captured = capsys.readouterr()
    payload = yaml.safe_load(captured.out)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.bootstrap_plan.v1"
    assert payload["steps"][0]["operation"] == "validate_contracts"


def test_generate_byoc_bootstrap_plan_json_output(capsys) -> None:
    code = main([
        "--json",
        "--generated-at",
        "2026-06-26T12:00:00+00:00",
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["execution_mode"] == "dry-run"
    assert payload["steps"][1]["operation"] == "verify_artifact_signatures"


def test_generate_byoc_bootstrap_plan_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.bootstrap_plan.v1"
    )


def test_generate_byoc_bootstrap_plan_check_passes_checked_in_plan(capsys) -> None:
    code = main(["--check-plan", str(PLAN)])

    captured = capsys.readouterr()
    assert code == 0
    assert captured.out == "BYOC bootstrap plan passed.\n"


def test_generate_byoc_bootstrap_plan_check_reports_drift(
    tmp_path: Path,
    capsys,
) -> None:
    path = tmp_path / "bootstrap-plan.yaml"
    text = PLAN.read_text(encoding="utf-8")
    path.write_text(
        text.replace("Validate BYOC manifests locally", "Validate something else"),
        encoding="utf-8",
    )

    code = main(["--check-plan", str(path)])

    captured = capsys.readouterr()
    assert code == 1
    assert "generated_plan_drift" in captured.err


def test_generate_byoc_bootstrap_plan_check_reports_mutation(
    tmp_path: Path,
    capsys,
) -> None:
    path = tmp_path / "bootstrap-plan.yaml"
    text = PLAN.read_text(encoding="utf-8")
    path.write_text(
        text.replace(
            "python scripts/validate_byoc_dataplane_manifest.py "
            "deploy/byoc/dataplane.example.yaml",
            "terraform apply -auto-approve",
        ),
        encoding="utf-8",
    )

    code = main(["--check-plan", str(path)])

    captured = capsys.readouterr()
    assert code == 1
    assert "mutating_command_forbidden" in captured.err
