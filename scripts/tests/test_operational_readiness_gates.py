from __future__ import annotations

import argparse
from pathlib import Path

from scripts import run_operational_readiness_gates as gates
from scripts.run_operational_readiness_gates import (
    GateResult,
    MANUAL_REQUIRED,
    PASS,
    _byoc_agent_probe_gate,
    _byoc_agent_runner_gate,
    _byoc_bootstrap_bundle_gate,
    _byoc_bootstrap_plan_gate,
    _byoc_bootstrap_runner_gate,
    _byoc_control_plane_intake_gate,
    _byoc_dataplane_contract_gate,
    _byoc_evidence_package_gate,
    _byoc_evidence_ledger_gate,
    _byoc_permissions_contract_gate,
    _byoc_post_deploy_validation_gate,
    _github_required_checks_gate,
    _production_env_contract_gate,
    _schema_drift_gate,
)


def test_production_env_contract_gate_passes_for_checked_in_template() -> None:
    args = argparse.Namespace(command_timeout_s=30)

    result = _production_env_contract_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert result.command[-1] == "scripts/check_production_env_contract.py"


def test_byoc_dataplane_contract_gate_passes_for_checked_in_manifest() -> None:
    args = argparse.Namespace(command_timeout_s=30)

    result = _byoc_dataplane_contract_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert result.command[-1] == "deploy/byoc/dataplane.example.yaml"
    assert result.artifacts["manifest"] == "deploy/byoc/dataplane.example.yaml"


def test_byoc_permissions_contract_gate_passes_for_checked_in_manifest() -> None:
    args = argparse.Namespace(command_timeout_s=30)

    result = _byoc_permissions_contract_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert "scripts/validate_byoc_permissions_manifest.py" in result.command
    assert result.artifacts == {
        "manifest": "deploy/byoc/permissions.example.yaml",
        "dataplane_manifest": "deploy/byoc/dataplane.example.yaml",
        "aws_template": "deploy/byoc/aws/iam.bootstrap.template.yaml",
    }


def test_byoc_bootstrap_bundle_gate_passes_for_checked_in_bundle() -> None:
    args = argparse.Namespace(command_timeout_s=30)

    result = _byoc_bootstrap_bundle_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert "scripts/verify_byoc_bootstrap_bundle.py" in result.command
    assert "--verify-local-files" in result.command
    assert result.artifacts == {
        "bundle": "deploy/byoc/bootstrap-bundle.example.yaml",
        "dataplane_manifest": "deploy/byoc/dataplane.example.yaml",
        "permissions_manifest": "deploy/byoc/permissions.example.yaml",
    }


def test_byoc_bootstrap_plan_gate_passes_for_checked_in_plan() -> None:
    args = argparse.Namespace(command_timeout_s=30)

    result = _byoc_bootstrap_plan_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert "scripts/generate_byoc_bootstrap_plan.py" in result.command
    assert "--check-plan" in result.command
    assert result.artifacts == {
        "plan": "deploy/byoc/bootstrap-plan.example.yaml",
        "bundle": "deploy/byoc/bootstrap-bundle.example.yaml",
        "dataplane_manifest": "deploy/byoc/dataplane.example.yaml",
        "permissions_manifest": "deploy/byoc/permissions.example.yaml",
    }


def test_byoc_bootstrap_runner_gate_passes_for_checked_in_plan() -> None:
    args = argparse.Namespace(command_timeout_s=30)

    result = _byoc_bootstrap_runner_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert "scripts/run_byoc_bootstrap_runner.py" in result.command
    assert "--json" in result.command
    assert result.artifacts == {
        "plan": "deploy/byoc/bootstrap-plan.example.yaml",
        "bundle": "deploy/byoc/bootstrap-bundle.example.yaml",
        "dataplane_manifest": "deploy/byoc/dataplane.example.yaml",
        "permissions_manifest": "deploy/byoc/permissions.example.yaml",
        "env_template": ".env.production.example",
    }


def test_byoc_agent_probe_gate_passes_for_checked_in_manifest() -> None:
    args = argparse.Namespace(command_timeout_s=30)

    result = _byoc_agent_probe_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert "scripts/run_byoc_agent_probe.py" in result.command
    assert "--json" in result.command
    assert result.artifacts == {"manifest": "deploy/byoc/dataplane.example.yaml"}
    assert "local-byoc-agent-probe-token" not in result.stdout_tail


def test_byoc_agent_runner_gate_passes_for_checked_in_manifest() -> None:
    args = argparse.Namespace(command_timeout_s=30)

    result = _byoc_agent_runner_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert "scripts/run_byoc_agent_runner.py" in result.command
    assert "--json" in result.command
    assert "--iterations" in result.command
    assert "--mock-desired-revision" in result.command
    assert "--mock-config-epoch" in result.command
    assert result.artifacts == {"manifest": "deploy/byoc/dataplane.example.yaml"}
    assert "local-byoc-agent-runner-token" not in result.stdout_tail


def test_byoc_evidence_ledger_gate_passes_for_checked_in_ledger() -> None:
    args = argparse.Namespace(command_timeout_s=30)

    result = _byoc_evidence_ledger_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert "scripts/generate_byoc_evidence_ledger.py" in result.command
    assert "--check-ledger" in result.command
    assert result.artifacts == {
        "ledger": "deploy/byoc/evidence-ledger.example.yaml",
        "plan": "deploy/byoc/bootstrap-plan.example.yaml",
        "bundle": "deploy/byoc/bootstrap-bundle.example.yaml",
        "dataplane_manifest": "deploy/byoc/dataplane.example.yaml",
        "permissions_manifest": "deploy/byoc/permissions.example.yaml",
        "env_template": ".env.production.example",
    }


def test_byoc_evidence_package_gate_passes_for_checked_in_package() -> None:
    args = argparse.Namespace(command_timeout_s=30)

    result = _byoc_evidence_package_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert "scripts/generate_byoc_evidence_package.py" in result.command
    assert "--check-package" in result.command
    assert result.artifacts == {
        "package": "deploy/byoc/evidence-package.example.yaml",
        "ledger": "deploy/byoc/evidence-ledger.example.yaml",
        "plan": "deploy/byoc/bootstrap-plan.example.yaml",
        "bundle": "deploy/byoc/bootstrap-bundle.example.yaml",
        "dataplane_manifest": "deploy/byoc/dataplane.example.yaml",
        "permissions_manifest": "deploy/byoc/permissions.example.yaml",
    }


def test_byoc_control_plane_intake_gate_passes_for_api_contract() -> None:
    args = argparse.Namespace(command_timeout_s=30, skip_pytest=False)

    result = _byoc_control_plane_intake_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert "services/platform/runtime/tests/test_byoc_agent_control_plane.py" in (
        result.command
    )
    assert "services/app/gateway/tests/test_byoc_agent_router.py" in result.command
    assert "services/platform/runtime/tests/test_byoc_control_plane_intake.py" in (
        result.command
    )
    assert "services/app/gateway/tests/test_byoc_control_plane_router.py" in (
        result.command
    )


def test_byoc_post_deploy_validation_gate_passes_offline_contract() -> None:
    args = argparse.Namespace(command_timeout_s=30)

    result = _byoc_post_deploy_validation_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert "scripts/run_byoc_post_deploy_validation.py" in result.command
    assert result.artifacts == {
        "manifest": "deploy/byoc/dataplane.example.yaml",
        "env_template": ".env.production.example",
    }


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


def test_github_required_checks_gate_requires_live_context(monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)
    args = argparse.Namespace(command_timeout_s=30)

    result = _github_required_checks_gate(args)

    assert result.status == MANUAL_REQUIRED
    assert result.metrics == {
        "github_token_present": False,
        "github_repository_present": False,
    }
    assert result.artifacts["policy"] == ".github/main-required-checks.json"


def test_github_required_checks_gate_runs_when_token_and_repo_exist(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", "fake-token")
    monkeypatch.setenv("GITHUB_REPOSITORY", "fyralisinc/fyraliscore")
    calls = []

    def fake_run_command_gate(name, command, **kwargs):
        calls.append((name, command, kwargs))
        return GateResult(name=name, status=PASS, details=kwargs["details"])

    monkeypatch.setattr(gates, "_run_command_gate", fake_run_command_gate)
    args = argparse.Namespace(command_timeout_s=30)

    result = _github_required_checks_gate(args)

    assert result.status == PASS
    assert calls[0][0] == "github_main_required_checks"
    assert calls[0][1][-1] == "--live"


def test_ci_workflow_runs_migrations_before_schema_drift() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text()

    apply_idx = workflow.index("scripts/apply_db_migrations.py")
    drift_idx = workflow.index("scripts/check_schema_drift.py")

    assert apply_idx < drift_idx


def test_ci_workflow_checks_required_merge_policy() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text()

    assert "scripts/check_github_required_checks.py" in workflow


def test_deploy_production_waits_for_green_ci() -> None:
    workflow = Path(".github/workflows/deploy-production.yml").read_text()

    assert "workflow_run:" in workflow
    assert 'workflows: ["CI"]' in workflow
    assert "branches: [production]" in workflow
    assert "github.event.workflow_run.conclusion == 'success'" in workflow
    assert "name: production" in workflow
    assert "\n  push:" not in workflow


def test_deploy_workflows_use_compose_canary_helper() -> None:
    for path in (
        Path(".github/workflows/deploy-production.yml"),
        Path(".github/workflows/deploy-staging.yml"),
    ):
        workflow = path.read_text()
        assert "PREV_SHA=\"$(git rev-parse HEAD)\"" in workflow
        assert "git reset --hard origin/" in workflow
        assert (
            'bash scripts/deploy_compose_release.sh --previous-sha "${PREV_SHA}"'
            in workflow
        )


def test_compose_deploy_helper_has_canary_worker_rollout_and_rollback() -> None:
    helper = Path("scripts/deploy_compose_release.sh").read_text()

    assert "DEPLOY_GATEWAY_CANARY" in helper
    assert "GATEWAY_START_GRT_SCHEDULER=0" in helper
    assert "production_processes" in helper
    assert "wait_service_health" in helper
    assert "scripts/check_product_slo_gate.py" in helper
    assert 'git reset --hard "${PREVIOUS_SHA}"' in helper
