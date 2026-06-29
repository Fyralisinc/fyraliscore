from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from services.platform.runtime.byoc_launch_readiness_summary import (
    ByocLaunchReadinessSummaryInputs,
    build_byoc_launch_readiness_summary,
    render_launch_readiness_summary_json,
)


GENERATED_AT = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)


def test_launch_readiness_summary_passes_with_metadata_only_artifacts(
    tmp_path: Path,
) -> None:
    paths = _write_inputs(tmp_path)

    summary = build_byoc_launch_readiness_summary(
        ByocLaunchReadinessSummaryInputs(
            live_test_readiness_path=paths["live"],
            customer_handoff_report_path=paths["handoff"],
            handoff_bundle_index_path=paths["index"],
            control_plane_read_smoke_path=paths["smoke"],
            generated_at=GENERATED_AT,
        )
    )
    rendered = render_launch_readiness_summary_json(summary)

    assert summary.schema_version == "fyralis.byoc.launch_readiness_summary.v1"
    assert summary.status == "pass"
    assert summary.customer_pilot_ready is True
    assert summary.manual_actions_required is False
    assert summary.required_checks_passed is True
    assert summary.deployment_id == "dep_launch01"
    assert summary.customer_id == "cus_launch01"
    assert summary.next_actions == ("none",)
    assert {check.name for check in summary.checks} == {
        "live_test_readiness",
        "customer_handoff_readiness",
        "handoff_bundle_index",
        "control_plane_read_smoke",
        "identity_consistency",
    }
    assert summary.privacy.child_report_bodies_included is False
    assert summary.privacy.signed_headers_included is False
    assert "https://gateway.customer.internal" not in rendered
    assert "bearer raw" not in rendered.lower()
    assert "token=secret" not in rendered
    assert "arn:aws" not in rendered


def test_launch_readiness_summary_marks_manual_when_hosted_smoke_is_not_executed(
    tmp_path: Path,
) -> None:
    paths = _write_inputs(
        tmp_path,
        live_overrides={"status": "manual_required", "live_aws_ready": False},
        smoke_overrides={"mode": "signed_requests", "responses": None},
    )

    summary = build_byoc_launch_readiness_summary(
        ByocLaunchReadinessSummaryInputs(
            live_test_readiness_path=paths["live"],
            customer_handoff_report_path=paths["handoff"],
            handoff_bundle_index_path=paths["index"],
            control_plane_read_smoke_path=paths["smoke"],
            generated_at=GENERATED_AT,
        )
    )

    assert summary.status == "manual_required"
    assert summary.customer_pilot_ready is False
    assert summary.required_checks_passed is True
    assert summary.manual_actions_required is True
    assert set(summary.next_actions) == {
        "complete_live_test_readiness",
        "complete_control_plane_read_smoke",
    }


def test_launch_readiness_summary_accepts_sanitized_control_plane_smoke_summary(
    tmp_path: Path,
) -> None:
    paths = _write_inputs(tmp_path)
    paths["smoke"].write_text(
        json.dumps(
            {
                "schema_version": (
                    "fyralis.byoc.control_plane_read_smoke_summary.v1"
                ),
                "status": "pass",
                "mode": "executed",
                "hosted_read_executed": True,
                "required_surfaces_present": True,
                "surface_count": 6,
                "deployment_id": "dep_launch01",
                "customer_id": "cus_launch01",
            }
        ),
        encoding="utf-8",
    )

    summary = build_byoc_launch_readiness_summary(
        ByocLaunchReadinessSummaryInputs(
            live_test_readiness_path=paths["live"],
            customer_handoff_report_path=paths["handoff"],
            handoff_bundle_index_path=paths["index"],
            control_plane_read_smoke_path=paths["smoke"],
            generated_at=GENERATED_AT,
        )
    )

    assert summary.status == "pass"
    smoke_check = [
        check for check in summary.checks if check.name == "control_plane_read_smoke"
    ][0]
    assert smoke_check.status == "pass"
    assert smoke_check.source_schema_version == (
        "fyralis.byoc.control_plane_read_smoke_summary.v1"
    )


def test_launch_readiness_summary_fails_for_identity_mismatch(
    tmp_path: Path,
) -> None:
    paths = _write_inputs(
        tmp_path,
        handoff_overrides={"deployment_id": "dep_other01"},
    )

    summary = build_byoc_launch_readiness_summary(
        ByocLaunchReadinessSummaryInputs(
            live_test_readiness_path=paths["live"],
            customer_handoff_report_path=paths["handoff"],
            handoff_bundle_index_path=paths["index"],
            control_plane_read_smoke_path=paths["smoke"],
            generated_at=GENERATED_AT,
        )
    )

    assert summary.status == "fail"
    assert summary.customer_pilot_ready is False
    assert summary.required_checks_passed is False
    identity_check = [
        check for check in summary.checks if check.name == "identity_consistency"
    ][0]
    assert identity_check.status == "fail"
    assert identity_check.metrics == {"mismatch_count": 1}


def _write_inputs(
    tmp_path: Path,
    *,
    live_overrides: dict[str, object] | None = None,
    handoff_overrides: dict[str, object] | None = None,
    index_overrides: dict[str, object] | None = None,
    smoke_overrides: dict[str, object] | None = None,
) -> dict[str, Path]:
    identity = {
        "deployment_id": "dep_launch01",
        "customer_id": "cus_launch01",
        "cloud_provider": "aws",
        "region": "us-east-1",
        "artifact_revision": "rev_launch01",
    }
    live = {
        "schema_version": "fyralis.byoc.live_test_readiness.v1",
        "status": "pass",
        "required_checks_passed": True,
        "live_aws_ready": True,
        "next_required_action": "run_live_credential_rehearsal",
        **identity,
    }
    handoff = {
        "schema_version": "fyralis.byoc.customer_handoff_readiness.v1",
        "customer_handoff_ready": True,
        "source_onboarding_allowed": True,
        "required_sections_passed": True,
        **identity,
    }
    index = {
        "schema_version": "fyralis.byoc.customer_handoff_bundle_index.v1",
        "artifact_count": 2,
        "signed_read_endpoint_count": 6,
        "artifacts": [
            {
                "name": "evidence_package",
                "required": True,
                "present": True,
            },
            {
                "name": "evidence_ledger",
                "required": True,
                "present": True,
            },
        ],
        "signed_read_endpoints": [
            {"name": f"surface_{index}"} for index in range(6)
        ],
        "privacy": {
            "artifact_bodies_included": False,
            "signed_headers_included": False,
            "endpoint_urls_included": False,
            "credentials_included": False,
            "logs_included": False,
        },
        **identity,
    }
    smoke = {
        "schema_version": "fyralis.byoc.control_plane_read_smoke.v1",
        "mode": "executed",
        "responses": {
            "agent_fleet": {"response": {"items": []}},
            "deployment_overview": {"response": {"status": "active"}},
            "control_panel_state": {"response": {"status": "ready"}},
            "evidence_packages": {"response": {"items": []}},
            "preflight_reports": {"response": {"items": []}},
            "runner_evidence": {
                "response": {
                    "details": "https://gateway.customer.internal token=secret",
                    "headers": {"authorization": "bearer raw"},
                    "resource": "arn:aws:iam::123456789012:role/Unsafe",
                }
            },
        },
        **identity,
    }
    live.update(live_overrides or {})
    handoff.update(handoff_overrides or {})
    index.update(index_overrides or {})
    smoke.update(smoke_overrides or {})

    paths = {
        "live": tmp_path / "live.json",
        "handoff": tmp_path / "handoff.json",
        "index": tmp_path / "index.json",
        "smoke": tmp_path / "smoke.json",
    }
    for name, payload in (
        ("live", live),
        ("handoff", handoff),
        ("index", index),
        ("smoke", smoke),
    ):
        paths[name].write_text(json.dumps(payload), encoding="utf-8")
    return paths
