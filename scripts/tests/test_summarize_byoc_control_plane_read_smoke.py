from __future__ import annotations

import json
from pathlib import Path

from scripts.summarize_byoc_control_plane_read_smoke import main


def test_summarize_control_plane_read_smoke_json_output(
    tmp_path: Path,
    capsys,
) -> None:
    smoke = tmp_path / "smoke.json"
    smoke.write_text(json.dumps(_executed_smoke()), encoding="utf-8")

    code = main(
        [
            "--json",
            "--control-plane-read-smoke",
            str(smoke),
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["schema_version"] == (
        "fyralis.byoc.control_plane_read_smoke_summary.v1"
    )
    assert payload["status"] == "pass"
    assert payload["hosted_read_executed"] is True
    assert payload["privacy"]["signed_headers_included"] is False
    assert payload["privacy"]["response_bodies_included"] is False
    assert "https://control-plane.example" not in rendered
    assert "x-fyralis-signature" not in rendered.lower()
    assert "bearer raw" not in rendered.lower()
    assert "arn:aws" not in rendered


def test_summarize_control_plane_read_smoke_writes_output(
    tmp_path: Path,
    capsys,
) -> None:
    smoke = tmp_path / "smoke.json"
    output = tmp_path / "summary.json"
    smoke.write_text(json.dumps(_executed_smoke()), encoding="utf-8")

    code = main(
        [
            "--json",
            "--control-plane-read-smoke",
            str(smoke),
            "--output",
            str(output),
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8")) == json.loads(captured.out)


def test_summarize_control_plane_read_smoke_require_executed_fails_manual(
    tmp_path: Path,
    capsys,
) -> None:
    smoke = tmp_path / "smoke.json"
    smoke.write_text(json.dumps(_signed_request_smoke()), encoding="utf-8")

    code = main(
        [
            "--json",
            "--require-executed",
            "--control-plane-read-smoke",
            str(smoke),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["status"] == "manual_required"
    assert payload["mode"] == "signed_requests"


def test_summarize_control_plane_read_smoke_returns_zero_for_manual_by_default(
    tmp_path: Path,
    capsys,
) -> None:
    smoke = tmp_path / "smoke.json"
    smoke.write_text(json.dumps(_signed_request_smoke()), encoding="utf-8")

    code = main(
        [
            "--json",
            "--control-plane-read-smoke",
            str(smoke),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["status"] == "manual_required"
    assert payload["hosted_read_executed"] is False


def _executed_smoke() -> dict[str, object]:
    return {
        "schema_version": "fyralis.byoc.control_plane_read_smoke.v1",
        "mode": "executed",
        "deployment_id": "dep_launch01",
        "customer_id": "cus_launch01",
        "responses": {
            "agent_fleet": {"response": {"items": []}},
            "deployment_overview": {"response": {"status": "active"}},
            "evidence_packages": {"response": {"items": []}},
            "preflight_reports": {"response": {"items": []}},
            "runner_evidence": {
                "path": "/byoc/control-plane/runner-evidence",
                "query": "deployment_id=dep_launch01",
                "response": {
                    "url": "https://control-plane.example",
                    "headers": {"authorization": "bearer raw"},
                    "signature": "x-fyralis-signature=signed-secret",
                    "resource": "arn:aws:iam::123456789012:role/Unsafe",
                },
            },
        },
    }


def _signed_request_smoke() -> dict[str, object]:
    surfaces = (
        "agent_fleet",
        "deployment_overview",
        "evidence_packages",
        "preflight_reports",
        "runner_evidence",
    )
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
            for name in surfaces
        },
    }
