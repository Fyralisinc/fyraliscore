from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from services.platform.runtime.byoc_control_plane_read_smoke_summary import (
    ByocControlPlaneReadSmokeSummaryInputs,
    build_byoc_control_plane_read_smoke_summary,
    render_control_plane_read_smoke_summary_json,
)


GENERATED_AT = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)


def test_control_plane_read_smoke_summary_passes_for_executed_smoke(
    tmp_path: Path,
) -> None:
    smoke = tmp_path / "smoke.json"
    smoke.write_text(json.dumps(_executed_smoke()), encoding="utf-8")

    summary = build_byoc_control_plane_read_smoke_summary(
        ByocControlPlaneReadSmokeSummaryInputs(
            control_plane_read_smoke_path=smoke,
            generated_at=GENERATED_AT,
        )
    )
    rendered = render_control_plane_read_smoke_summary_json(summary)

    assert summary.schema_version == (
        "fyralis.byoc.control_plane_read_smoke_summary.v1"
    )
    assert summary.status == "pass"
    assert summary.mode == "executed"
    assert summary.hosted_read_executed is True
    assert summary.required_surfaces_present is True
    assert summary.surface_count == 5
    assert summary.next_actions == ("none",)
    assert summary.privacy.signed_headers_included is False
    assert summary.privacy.response_bodies_included is False
    assert "https://control-plane.example" not in rendered
    assert "/byoc/control-plane" not in rendered
    assert "deployment_id=dep_launch01" not in rendered
    assert "bearer raw" not in rendered.lower()
    assert "token=secret" not in rendered
    assert "arn:aws" not in rendered


def test_control_plane_read_smoke_summary_marks_signed_requests_manual(
    tmp_path: Path,
) -> None:
    smoke = tmp_path / "smoke.json"
    smoke.write_text(json.dumps(_signed_request_smoke()), encoding="utf-8")

    summary = build_byoc_control_plane_read_smoke_summary(
        ByocControlPlaneReadSmokeSummaryInputs(
            control_plane_read_smoke_path=smoke,
            generated_at=GENERATED_AT,
        )
    )
    rendered = render_control_plane_read_smoke_summary_json(summary)

    assert summary.status == "manual_required"
    assert summary.mode == "signed_requests"
    assert summary.hosted_read_executed is False
    assert summary.required_surfaces_present is True
    assert set(surface.status for surface in summary.surfaces) == {"manual_required"}
    assert summary.next_actions == ("run_hosted_control_plane_read_smoke",)
    assert "x-fyralis-signature" not in rendered.lower()
    assert "signed-secret" not in rendered
    assert "https://control-plane.example" not in rendered


def test_control_plane_read_smoke_summary_fails_when_surface_missing(
    tmp_path: Path,
) -> None:
    payload = _executed_smoke()
    payload["responses"].pop("runner_evidence")
    smoke = tmp_path / "smoke.json"
    smoke.write_text(json.dumps(payload), encoding="utf-8")

    summary = build_byoc_control_plane_read_smoke_summary(
        ByocControlPlaneReadSmokeSummaryInputs(
            control_plane_read_smoke_path=smoke,
            generated_at=GENERATED_AT,
        )
    )

    assert summary.status == "fail"
    assert summary.required_surfaces_present is False
    assert summary.surface_count == 4
    missing = [surface for surface in summary.surfaces if surface.status == "fail"]
    assert [surface.name for surface in missing] == ["runner_evidence"]


def _executed_smoke() -> dict[str, object]:
    return {
        "schema_version": "fyralis.byoc.control_plane_read_smoke.v1",
        "mode": "executed",
        "deployment_id": "dep_launch01",
        "customer_id": "cus_launch01",
        "responses": {
            "agent_fleet": {
                "path": "/byoc/control-plane/agents",
                "query": "deployment_id=dep_launch01",
                "response": {"items": []},
            },
            "deployment_overview": {
                "path": "/byoc/control-plane/deployment-overview",
                "query": "deployment_id=dep_launch01",
                "response": {"status": "active"},
            },
            "evidence_packages": {
                "path": "/byoc/control-plane/evidence-packages",
                "query": "deployment_id=dep_launch01",
                "response": {"items": []},
            },
            "preflight_reports": {
                "path": "/byoc/control-plane/preflight-reports",
                "query": "deployment_id=dep_launch01",
                "response": {"items": []},
            },
            "runner_evidence": {
                "path": "/byoc/control-plane/runner-evidence",
                "query": "deployment_id=dep_launch01",
                "response": {
                    "items": [
                        {
                            "details": "https://control-plane.example token=secret",
                            "headers": {"authorization": "bearer raw"},
                            "resource": "arn:aws:iam::123456789012:role/Unsafe",
                        }
                    ]
                },
            },
        },
    }


def _signed_request_smoke() -> dict[str, object]:
    return {
        "schema_version": "fyralis.byoc.control_plane_read_smoke.v1",
        "mode": "signed_requests",
        "deployment_id": "dep_launch01",
        "customer_id": "cus_launch01",
        "requests": {
            name: {
                "method": "GET",
                "path": f"/byoc/control-plane/{name}",
                "query": "deployment_id=dep_launch01",
                "headers": {
                    "x-fyralis-signature": "signed-secret",
                    "authorization": "bearer raw",
                },
            }
            for name in (
                "agent_fleet",
                "deployment_overview",
                "evidence_packages",
                "preflight_reports",
                "runner_evidence",
            )
        },
    }
