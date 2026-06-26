from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts.run_byoc_bootstrap_runner import main


ROOT = Path(__file__).resolve().parents[2]
PLAN = ROOT / "deploy/byoc/bootstrap-plan.example.yaml"


def test_run_byoc_bootstrap_runner_json_output(capsys) -> None:
    code = main(["--json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["status"] == "pass"
    assert payload["execution_mode"] == "dry-run"


def test_run_byoc_bootstrap_runner_yaml_output(capsys) -> None:
    code = main([])

    captured = capsys.readouterr()
    payload = yaml.safe_load(captured.out)
    assert code == 0
    assert payload["required_checks_passed"] is True
    assert payload["checks"][0]["name"] == "bootstrap_plan_schema"


def test_run_byoc_bootstrap_runner_writes_output_file(
    tmp_path: Path,
    capsys,
) -> None:
    output = tmp_path / "reports" / "byoc-bootstrap-runner.json"

    code = main(["--json", "--output", str(output)])

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8")) == json.loads(captured.out)


def test_run_byoc_bootstrap_runner_returns_nonzero_for_drift(
    tmp_path: Path,
    capsys,
) -> None:
    drifted = tmp_path / "bootstrap-plan.yaml"
    drifted.write_text(
        PLAN.read_text(encoding="utf-8").replace(
            "Validate BYOC manifests locally",
            "Validate a locally edited plan",
        ),
        encoding="utf-8",
    )

    code = main(["--json", "--plan", str(drifted)])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["status"] == "fail"
    assert "generated_plan_drift" in {
        check["name"] for check in payload["checks"] if check["status"] == "fail"
    }
