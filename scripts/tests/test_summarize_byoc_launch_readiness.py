from __future__ import annotations

import json
from pathlib import Path

from scripts.summarize_byoc_launch_readiness import main


def test_summarize_byoc_launch_readiness_json_output(
    tmp_path: Path,
    capsys,
) -> None:
    paths = _write_inputs(tmp_path)

    code = main(
        [
            "--json",
            "--live-test-readiness",
            str(paths["live"]),
            "--customer-handoff-report",
            str(paths["handoff"]),
            "--handoff-bundle-index",
            str(paths["index"]),
            "--control-plane-read-smoke",
            str(paths["smoke"]),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.launch_readiness_summary.v1"
    assert payload["status"] == "pass"
    assert payload["customer_pilot_ready"] is True
    assert payload["privacy"]["child_report_bodies_included"] is False
    assert payload["privacy"]["signed_headers_included"] is False
    assert "https://gateway.customer.internal" not in rendered
    assert "bearer raw" not in rendered.lower()
    assert "token=secret" not in rendered
    assert "arn:aws" not in rendered


def test_summarize_byoc_launch_readiness_writes_output(
    tmp_path: Path,
    capsys,
) -> None:
    paths = _write_inputs(tmp_path)
    output = tmp_path / "launch-readiness.json"

    code = main(
        [
            "--json",
            "--live-test-readiness",
            str(paths["live"]),
            "--customer-handoff-report",
            str(paths["handoff"]),
            "--handoff-bundle-index",
            str(paths["index"]),
            "--control-plane-read-smoke",
            str(paths["smoke"]),
            "--output",
            str(output),
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8")) == json.loads(captured.out)


def test_summarize_byoc_launch_readiness_require_ready_fails_manual_status(
    tmp_path: Path,
    capsys,
) -> None:
    paths = _write_inputs(
        tmp_path,
        live_overrides={"status": "manual_required", "live_aws_ready": False},
        smoke_overrides={"mode": "signed_requests", "responses": None},
    )

    code = main(
        [
            "--json",
            "--require-ready",
            "--live-test-readiness",
            str(paths["live"]),
            "--customer-handoff-report",
            str(paths["handoff"]),
            "--handoff-bundle-index",
            str(paths["index"]),
            "--control-plane-read-smoke",
            str(paths["smoke"]),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["status"] == "manual_required"
    assert payload["manual_actions_required"] is True
    assert payload["customer_pilot_ready"] is False


def test_summarize_byoc_launch_readiness_returns_zero_for_manual_by_default(
    tmp_path: Path,
    capsys,
) -> None:
    paths = _write_inputs(
        tmp_path,
        smoke_overrides={"mode": "signed_requests", "responses": None},
    )

    code = main(
        [
            "--json",
            "--live-test-readiness",
            str(paths["live"]),
            "--customer-handoff-report",
            str(paths["handoff"]),
            "--handoff-bundle-index",
            str(paths["index"]),
            "--control-plane-read-smoke",
            str(paths["smoke"]),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["status"] == "manual_required"
    assert payload["required_checks_passed"] is True


def _write_inputs(
    tmp_path: Path,
    *,
    live_overrides: dict[str, object] | None = None,
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
            {"name": "evidence_package", "required": True, "present": True},
            {"name": "evidence_ledger", "required": True, "present": True},
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
                    "url": "https://gateway.customer.internal",
                    "headers": {"authorization": "bearer raw"},
                    "token": "token=secret",
                    "resource": "arn:aws:iam::123456789012:role/Unsafe",
                }
            },
        },
        **identity,
    }
    live.update(live_overrides or {})
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
