from __future__ import annotations

import json
from pathlib import Path

import yaml

from services.platform.runtime.byoc_bootstrap_runner import (
    ByocBootstrapRunnerInputs,
    render_runner_report_json,
    render_runner_report_yaml,
    run_byoc_bootstrap_runner,
)


ROOT = Path(__file__).resolve().parents[4]
PLAN = ROOT / "deploy/byoc/bootstrap-plan.example.yaml"
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
BUNDLE = ROOT / "deploy/byoc/bootstrap-bundle.example.yaml"


def _inputs(plan_path: Path = PLAN) -> ByocBootstrapRunnerInputs:
    return ByocBootstrapRunnerInputs(
        plan_path=plan_path,
        dataplane_manifest_path=DATAPLANE,
        permissions_manifest_path=PERMISSIONS,
        bootstrap_bundle_path=BUNDLE,
        repo_root=ROOT,
    )


def test_bootstrap_runner_passes_checked_in_plan() -> None:
    report = run_byoc_bootstrap_runner(_inputs())

    assert report.status == "pass"
    assert report.required_checks_passed is True
    assert report.execution_mode == "dry-run"
    assert sum(1 for check in report.checks if check.step_id) == 10
    assert all(check.status == "pass" for check in report.checks)


def test_bootstrap_runner_report_omits_raw_commands_and_artifact_refs() -> None:
    report = run_byoc_bootstrap_runner(_inputs())
    rendered = render_runner_report_json(report)

    assert "cosign verify" not in rendered
    assert "helm template" not in rendered
    assert "ghcr.io" not in rendered
    assert "prepared_commands" in rendered


def test_bootstrap_runner_yaml_output_is_parseable() -> None:
    report = run_byoc_bootstrap_runner(_inputs())

    payload = yaml.safe_load(render_runner_report_yaml(report))

    assert payload["status"] == "pass"
    assert payload["checks"][0]["name"] == "bootstrap_plan_schema"


def test_bootstrap_runner_fails_on_generated_plan_drift(tmp_path: Path) -> None:
    drifted = tmp_path / "bootstrap-plan.yaml"
    drifted.write_text(
        PLAN.read_text(encoding="utf-8").replace(
            "Validate BYOC manifests locally",
            "Validate a locally edited plan",
        ),
        encoding="utf-8",
    )

    report = run_byoc_bootstrap_runner(_inputs(drifted))

    assert report.status == "fail"
    drift_check = _check(report, "generated_plan_drift")
    assert drift_check.status == "fail"


def test_bootstrap_runner_fails_on_mutating_dry_run_command(tmp_path: Path) -> None:
    mutated = tmp_path / "bootstrap-plan.yaml"
    mutated.write_text(
        PLAN.read_text(encoding="utf-8").replace(
            "python scripts/validate_byoc_dataplane_manifest.py "
            "deploy/byoc/dataplane.example.yaml",
            "terraform apply -auto-approve",
        ),
        encoding="utf-8",
    )

    report = run_byoc_bootstrap_runner(_inputs(mutated))

    assert report.status == "fail"
    contract_check = _check(report, "bootstrap_plan_contract")
    assert contract_check.status == "fail"
    assert "mutating_command_forbidden" in contract_check.details


def test_bootstrap_runner_json_output_is_machine_readable() -> None:
    report = run_byoc_bootstrap_runner(_inputs())

    payload = json.loads(render_runner_report_json(report))

    assert payload["required_checks_passed"] is True
    assert payload["checks"][1]["name"] == "dataplane_manifest_schema"


def _check(report, name: str):
    return next(check for check in report.checks if check.name == name)
