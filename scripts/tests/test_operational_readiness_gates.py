from __future__ import annotations

import argparse
from pathlib import Path

from scripts import run_operational_readiness_gates as gates
from scripts.run_operational_readiness_gates import (
    GateResult,
    MANUAL_REQUIRED,
    PASS,
    _production_env_contract_gate,
    _schema_drift_gate,
)


def test_production_env_contract_gate_passes_for_checked_in_template() -> None:
    args = argparse.Namespace(command_timeout_s=30)

    result = _production_env_contract_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert result.command[-1] == "scripts/check_production_env_contract.py"


def test_schema_drift_gate_requires_staging_database_url(monkeypatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    args = argparse.Namespace(command_timeout_s=30)

    result = _schema_drift_gate(args)

    assert result.status == MANUAL_REQUIRED
    assert result.artifacts["tool"] == "scripts/check_schema_drift.py"


def test_schema_drift_gate_runs_against_configured_database(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://example.invalid/fyralis")
    calls = []

    def fake_run_command_gate(name, command, **kwargs):
        calls.append((name, command, kwargs))
        return GateResult(name=name, status=PASS, details=kwargs["details"])

    monkeypatch.setattr(gates, "_run_command_gate", fake_run_command_gate)
    args = argparse.Namespace(command_timeout_s=30)

    result = _schema_drift_gate(args)

    assert result.status == PASS
    assert calls[0][0] == "schema_drift_migration_rehearsal"
    assert calls[0][1][-1] == "scripts/check_schema_drift.py"


def test_ci_workflow_runs_migrations_before_schema_drift() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text()

    apply_idx = workflow.index("scripts/apply_db_migrations.py")
    drift_idx = workflow.index("scripts/check_schema_drift.py")

    assert apply_idx < drift_idx


def test_deploy_production_waits_for_green_ci() -> None:
    workflow = Path(".github/workflows/deploy-production.yml").read_text()

    assert "workflow_run:" in workflow
    assert 'workflows: ["CI"]' in workflow
    assert "branches: [production]" in workflow
    assert "github.event.workflow_run.conclusion == 'success'" in workflow
    assert "name: production" in workflow
    assert "\n  push:" not in workflow
