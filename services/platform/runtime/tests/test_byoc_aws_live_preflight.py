from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from services.platform.runtime.byoc_aws_live_preflight import (
    ByocAwsLivePreflightInputs,
    render_aws_live_preflight_json,
    run_byoc_aws_live_preflight,
)


ROOT = Path(__file__).resolve().parents[4]
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
IAM_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"
ACCOUNT_ID = "123456789012"
PRINCIPAL_ARN = "arn:aws:iam::123456789012:role/fyralis-dep-example01-bootstrap"


class _FakeSts:
    def __init__(self, account_id: str = ACCOUNT_ID) -> None:
        self.account_id = account_id

    def get_caller_identity(self) -> dict[str, str]:
        return {
            "Account": self.account_id,
            "Arn": f"arn:aws:sts::{self.account_id}:assumed-role/fake/session",
            "UserId": "fake-user",
        }


class _FakeEc2:
    def describe_availability_zones(self, **kwargs: Any) -> dict[str, object]:
        return {"AvailabilityZones": []}

    def describe_vpcs(self, **kwargs: Any) -> dict[str, object]:
        return {"Vpcs": []}


class _FakeTagging:
    def get_resources(self, **kwargs: Any) -> dict[str, object]:
        return {"ResourceTagMappingList": []}


class _FakeIam:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def simulate_principal_policy(self, **kwargs: Any) -> dict[str, object]:
        self.calls.append(kwargs)
        return {
            "EvaluationResults": [
                {"EvalDecision": "allowed"} for _ in kwargs["ActionNames"]
            ]
        }


class _FakeFailingIam:
    def simulate_principal_policy(self, **kwargs: Any) -> dict[str, object]:
        return {
            "EvaluationResults": [
                {"EvalDecision": "allowed"},
                {"EvalDecision": "implicitDeny"},
            ]
        }


def _factory(
    *,
    account_id: str = ACCOUNT_ID,
    iam: Any | None = None,
):
    clients = {
        "sts": _FakeSts(account_id),
        "ec2": _FakeEc2(),
        "resourcegroupstaggingapi": _FakeTagging(),
        "iam": iam or _FakeIam(),
    }

    def _client(service: str, profile: str | None, region: str | None) -> Any:
        assert profile is None
        assert region == "us-east-1"
        return clients[service]

    return _client


def test_aws_live_preflight_passes_with_sanitized_identity() -> None:
    report = run_byoc_aws_live_preflight(
        ByocAwsLivePreflightInputs(
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            aws_client_factory=_factory(),
        )
    )

    rendered = render_aws_live_preflight_json(report)
    payload = json.loads(rendered)
    assert report.required_checks_passed is True
    assert payload["schema_version"] == "fyralis.byoc.aws_live_preflight.v1"
    assert payload["live_aws_api_calls_executed"] is True
    assert payload["cloud_credentials_required"] is True
    assert payload["mutating_aws_api_calls_executed"] is False
    assert payload["privacy"]["account_id_included"] is False
    assert ACCOUNT_ID not in rendered
    assert "arn:aws" not in rendered
    assert "fake-user" not in rendered


def test_aws_live_preflight_account_mismatch_fails_without_leaking_account() -> None:
    report = run_byoc_aws_live_preflight(
        ByocAwsLivePreflightInputs(
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            aws_client_factory=_factory(account_id="999999999999"),
        )
    )

    rendered = render_aws_live_preflight_json(report)
    identity = _check(report.as_json(), "aws_sts_identity")
    assert report.required_checks_passed is False
    assert identity["status"] == "fail"
    assert identity["metrics"]["account_id_matches_expected"] is False
    assert ACCOUNT_ID not in rendered
    assert "999999999999" not in rendered
    assert "arn:aws" not in rendered


def test_aws_live_preflight_can_run_sanitized_iam_simulation() -> None:
    iam = _FakeIam()
    report = run_byoc_aws_live_preflight(
        ByocAwsLivePreflightInputs(
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            run_iam_policy_simulation=True,
            simulation_principal_arn=PRINCIPAL_ARN,
            aws_client_factory=_factory(iam=iam),
        )
    )

    rendered = render_aws_live_preflight_json(report)
    simulation = _check(report.as_json(), "iam_policy_simulation")
    assert report.required_checks_passed is True
    assert report.iam_policy_simulation_executed is True
    assert simulation["status"] == "pass"
    assert simulation["metrics"]["evaluation_count"] > 0
    assert simulation["metrics"]["denied_evaluation_count"] == 0
    assert iam.calls
    assert PRINCIPAL_ARN not in rendered
    assert "cloudformation:CreateStack" not in rendered
    assert "arn:aws" not in rendered


def test_aws_live_preflight_reports_simulation_denies_as_counts_only() -> None:
    report = run_byoc_aws_live_preflight(
        ByocAwsLivePreflightInputs(
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            run_iam_policy_simulation=True,
            simulation_principal_arn=PRINCIPAL_ARN,
            aws_client_factory=_factory(iam=_FakeFailingIam()),
        )
    )

    rendered = render_aws_live_preflight_json(report)
    simulation = _check(report.as_json(), "iam_policy_simulation")
    assert report.required_checks_passed is False
    assert simulation["status"] == "fail"
    assert simulation["metrics"]["denied_evaluation_count"] > 0
    assert "implicitDeny" not in rendered
    assert PRINCIPAL_ARN not in rendered


def test_aws_live_preflight_skip_live_runs_contract_only_shape() -> None:
    report = run_byoc_aws_live_preflight(
        ByocAwsLivePreflightInputs(
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            skip_live_aws=True,
        )
    )

    payload = report.as_json()
    assert report.required_checks_passed is True
    assert payload["cloud_credentials_required"] is False
    assert payload["live_aws_api_calls_executed"] is False
    assert _check(payload, "aws_sts_identity")["status"] == "skipped"


def _check(payload: dict[str, object], name: str) -> dict[str, object]:
    checks = payload["checks"]
    assert isinstance(checks, list)
    for check in checks:
        assert isinstance(check, dict)
        if check["name"] == name:
            return check
    raise AssertionError(f"missing check {name}")
