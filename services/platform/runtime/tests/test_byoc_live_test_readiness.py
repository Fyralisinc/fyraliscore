from __future__ import annotations

import json
from pathlib import Path

from services.platform.runtime.byoc_live_test_readiness import (
    ByocLiveTestReadinessInputs,
    render_live_test_readiness_json,
    run_byoc_live_test_readiness,
)


ROOT = Path(__file__).resolve().parents[4]
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
IAM_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"


def test_live_test_readiness_reports_manual_when_aws_access_missing(
    monkeypatch,
) -> None:
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setattr(
        "services.platform.runtime.byoc_live_test_readiness.shutil.which",
        lambda _: None,
    )

    report = run_byoc_live_test_readiness(
        ByocLiveTestReadinessInputs(
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            repo_root=ROOT,
        )
    )

    rendered = render_live_test_readiness_json(report)
    assert report.status == "manual_required"
    assert report.required_checks_passed is True
    assert report.live_aws_ready is False
    assert report.next_required_action == "configure_aws_access"
    assert report.aws_cli_available is False
    assert report.aws_env_credentials_present is False
    assert "123456789012" not in rendered
    assert "arn:" not in rendered.lower()
    assert "aws_secret_access_key" not in rendered.lower()


def test_live_test_readiness_passes_when_profile_and_aws_cli_exist(
    tmp_path,
    monkeypatch,
) -> None:
    aws_dir = tmp_path / ".aws"
    aws_dir.mkdir()
    (aws_dir / "config").write_text(
        "[profile fyralis-byoc-staging]\nregion = us-east-1\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setattr(
        "services.platform.runtime.byoc_live_test_readiness.shutil.which",
        lambda _: "/usr/bin/aws",
    )

    report = run_byoc_live_test_readiness(
        ByocLiveTestReadinessInputs(
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            repo_root=ROOT,
            aws_profile="fyralis-byoc-staging",
            aws_config_dir=aws_dir,
            require_aws_access=True,
        )
    )

    rendered = render_live_test_readiness_json(report)
    assert report.status == "pass"
    assert report.live_aws_ready is True
    assert report.next_required_action == "run_live_credential_rehearsal"
    assert report.aws_profile_supplied is True
    assert report.aws_profile_configured is True
    assert "fyralis-byoc-staging" not in rendered
    assert "123456789012" not in rendered
    assert "arn:" not in rendered.lower()


def test_live_test_readiness_report_json_round_trips(monkeypatch) -> None:
    monkeypatch.setattr(
        "services.platform.runtime.byoc_live_test_readiness.shutil.which",
        lambda _: None,
    )

    report = run_byoc_live_test_readiness(
        ByocLiveTestReadinessInputs(
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            repo_root=ROOT,
        )
    )

    payload = json.loads(render_live_test_readiness_json(report))
    assert payload["schema_version"] == "fyralis.byoc.live_test_readiness.v1"
    assert payload["privacy"]["aws_api_calls_executed"] is False
    assert payload["mutating_cloud_commands_executed"] is False
