from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import yaml

from scripts.check_byoc_source_onboarding_gate import main
from services.platform.runtime.byoc_aws_live_preflight import (
    ByocAwsLivePreflightInputs,
    render_aws_live_preflight_json,
    run_byoc_aws_live_preflight,
)
from services.platform.runtime.byoc_bootstrap_bundle import load_byoc_bootstrap_bundle
from services.platform.runtime.byoc_bootstrap_plan import load_byoc_bootstrap_plan
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_evidence_ledger import generate_evidence_ledger
from services.platform.runtime.byoc_permissions import load_byoc_permissions_manifest


ROOT = Path(__file__).resolve().parents[2]
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
IAM_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"
BUNDLE = ROOT / "deploy/byoc/bootstrap-bundle.example.yaml"
PLAN = ROOT / "deploy/byoc/bootstrap-plan.example.yaml"
IAC_PACKAGE = ROOT / "deploy/byoc/aws/iac-package.example.yaml"
ENV_TEMPLATE = ROOT / ".env.production.example"
GENERATED_AT = datetime(2026, 6, 26, 12, 0, tzinfo=UTC)


def test_check_byoc_source_onboarding_gate_allows_checked_in_package(capsys) -> None:
    code = main(["--json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.source_onboarding_gate.v1"
    assert payload["gate_mode"] == "evidence_package"
    assert payload["source_onboarding_allowed"] is True
    assert _check(payload, "aws_live_preflight_evidence")["status"] == "skipped"


def test_check_byoc_source_onboarding_gate_can_require_aws_live_preflight(
    capsys,
) -> None:
    code = main(["--json", "--require-aws-live-preflight"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert payload["source_onboarding_allowed"] is False
    assert _check(payload, "aws_live_preflight_evidence")["status"] == "fail"


def test_check_byoc_source_onboarding_gate_accepts_aws_live_evidence(
    tmp_path: Path,
    capsys,
) -> None:
    ledger = _ledger_with_aws_live_preflight(tmp_path)

    code = main([
        "--json",
        "--evidence-ledger",
        str(ledger),
        "--require-aws-live-preflight",
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload)
    assert code == 0
    assert payload["gate_mode"] == "evidence_ledger"
    assert payload["source_onboarding_allowed"] is True
    assert _check(payload, "aws_live_preflight_evidence")["status"] == "pass"
    assert "123456789012" not in rendered
    assert "arn:aws" not in rendered


def test_check_byoc_source_onboarding_gate_can_require_live_post_deploy(
    capsys,
) -> None:
    code = main(["--json", "--require-live-post-deploy"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 1
    assert _check(payload, "post_deploy_live_evidence")["status"] == "fail"


def test_check_byoc_source_onboarding_gate_writes_output(
    tmp_path: Path,
    capsys,
) -> None:
    output = tmp_path / "source-onboarding-gate.json"

    code = main(["--json", "--output", str(output)])

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8")) == json.loads(captured.out)


def _ledger_with_aws_live_preflight(tmp_path: Path) -> Path:
    aws_report = run_byoc_aws_live_preflight(
        ByocAwsLivePreflightInputs(
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            skip_live_aws=True,
        )
    )
    aws_report_path = tmp_path / "aws-live-preflight.json"
    aws_report_path.write_text(
        render_aws_live_preflight_json(aws_report),
        encoding="utf-8",
    )
    ledger = generate_evidence_ledger(
        plan=load_byoc_bootstrap_plan(PLAN),
        dataplane_manifest=load_byoc_manifest(DATAPLANE),
        permissions_manifest=load_byoc_permissions_manifest(PERMISSIONS),
        bootstrap_bundle=load_byoc_bootstrap_bundle(BUNDLE),
        plan_path=PLAN,
        dataplane_manifest_path=DATAPLANE,
        permissions_manifest_path=PERMISSIONS,
        bootstrap_bundle_path=BUNDLE,
        iac_package_path=IAC_PACKAGE,
        iam_template_path=IAM_TEMPLATE,
        env_path=ENV_TEMPLATE,
        aws_live_preflight_report_path=aws_report_path,
        generated_at=GENERATED_AT,
        repo_root=ROOT,
    )
    ledger_path = tmp_path / "evidence-ledger.yaml"
    ledger_path.write_text(
        yaml.safe_dump(
            ledger.model_dump(mode="json", exclude_none=True),
            sort_keys=False,
            width=1_000_000,
        ),
        encoding="utf-8",
    )
    return ledger_path


def _check(payload: dict[str, object], name: str) -> dict[str, object]:
    checks = payload["checks"]
    assert isinstance(checks, list)
    for check in checks:
        assert isinstance(check, dict)
        if check["name"] == name:
            return check
    raise AssertionError(f"missing check {name}")
