from __future__ import annotations

import json
import urllib.parse

from scripts.smoke_byoc_control_plane_reads import (
    DEFAULT_SIGNING_SECRET_ENV,
    SCHEMA_VERSION,
    main,
)
from services.platform.runtime.byoc_control_plane_intake import (
    validate_evidence_receipt_read_auth_headers,
)


SIGNING_SECRET = "local-control-plane-read-smoke-secret"
SIGNING_KEY_REF = "control-plane/byoc/evidence-read-key"
DEPLOYMENT_ID = "dep_readsmoke01"
CUSTOMER_ID = "cus_readsmoke01"


def _base_args() -> list[str]:
    return [
        "--deployment-id",
        DEPLOYMENT_ID,
        "--customer-id",
        CUSTOMER_ID,
        "--limit",
        "5",
        "--key-ref",
        SIGNING_KEY_REF,
        "--nonce-prefix",
        "nonce-control-plane-read-smoke",
        "--timestamp",
        "2026-06-27T12:00:00+00:00",
    ]


def test_smoke_byoc_control_plane_reads_prints_signed_requests(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    code = main(_base_args())

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["mode"] == "signed_requests"
    assert payload["control_panel_recent_limit"] == 10
    assert set(payload["requests"]) == {
        "agent_fleet",
        "deployment_overview",
        "control_panel_state",
        "evidence_packages",
        "preflight_reports",
        "runner_evidence",
    }
    for request in payload["requests"].values():
        assert request["method"] == "GET"
        assert validate_evidence_receipt_read_auth_headers(
            request["headers"],
            method="GET",
            path=request["path"],
            query=request["query"],
            signing_secret=SIGNING_SECRET,
            expected_key_ref=SIGNING_KEY_REF,
            max_clock_skew_seconds=10**9,
        ) == []
    assert payload["requests"]["agent_fleet"]["query"] == (
        f"deployment_id={DEPLOYMENT_ID}&customer_id={CUSTOMER_ID}&limit=5"
    )
    assert payload["requests"]["deployment_overview"]["query"] == (
        f"deployment_id={DEPLOYMENT_ID}&customer_id={CUSTOMER_ID}"
    )
    assert payload["requests"]["control_panel_state"]["query"] == (
        f"deployment_id={DEPLOYMENT_ID}&customer_id={CUSTOMER_ID}&recent_limit=10"
    )
    assert SIGNING_SECRET not in rendered
    assert "install_token" not in rendered.lower()
    assert "secret_ref" not in rendered.lower()
    assert "payload" not in rendered.lower()
    assert captured.err == ""


def test_smoke_byoc_control_plane_reads_executes_all_signed_reads(
    monkeypatch,
    capsys,
) -> None:
    seen: list[dict[str, object]] = []
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    class _Response:
        def __init__(self, body: dict[str, object]) -> None:
            self._body = body

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def read(self) -> bytes:
            return json.dumps(self._body).encode("utf-8")

    def _body_for(path: str) -> dict[str, object]:
        if path.endswith("/agents"):
            return {
                "schema_version": "fyralis.byoc.agent_fleet_list.v1",
                "result_count": 1,
                "items": [],
                "stored_scope": "sanitized_agent_metadata_only",
            }
        if path.endswith("/deployment-overview"):
            return {
                "schema_version": "fyralis.byoc.deployment_overview.v1",
                "status": "ready",
                "next_action": "none",
                "stored_scope": "sanitized_deployment_metadata_only",
            }
        if path.endswith("/control-panel-state"):
            return {
                "schema_version": "fyralis.byoc.control_panel_state.v1",
                "overview": {"status": "ready"},
                "sections": [],
                "actions": [],
                "stored_scope": "sanitized_control_panel_metadata_only",
            }
        if path.endswith("/evidence-packages"):
            return {
                "schema_version": "fyralis.byoc.evidence_package_receipt_list.v1",
                "result_count": 1,
                "items": [],
                "stored_scope": "sanitized_metadata_only",
            }
        if path.endswith("/preflight-reports"):
            return {
                "schema_version": "fyralis.byoc.preflight_report_receipt_list.v1",
                "result_count": 1,
                "items": [],
                "stored_scope": "sanitized_metadata_only",
            }
        if path.endswith("/runner-evidence"):
            return {
                "schema_version": "fyralis.byoc.runner_evidence_receipt_list.v1",
                "result_count": 1,
                "items": [],
                "stored_scope": "sanitized_metadata_only",
            }
        raise AssertionError(path)

    def _fake_urlopen(request, *, timeout):
        parsed = urllib.parse.urlsplit(request.full_url)
        seen.append(
            {
                "url": request.full_url,
                "path": parsed.path,
                "query": parsed.query,
                "timeout": timeout,
                "headers": dict(request.headers),
            }
        )
        return _Response(_body_for(parsed.path))

    monkeypatch.setattr(
        "scripts.smoke_byoc_control_plane_reads.urllib.request.urlopen",
        _fake_urlopen,
    )

    code = main(
        [
            *_base_args(),
            "--base-url",
            "https://control.example.com/root",
            "--timeout-seconds",
            "4",
        ]
    )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["mode"] == "executed"
    assert payload["control_panel_recent_limit"] == 10
    assert len(seen) == 6
    assert {item["path"] for item in seen} == {
        "/root/byoc/control-plane/agents",
        "/root/byoc/control-plane/deployment-overview",
        "/root/byoc/control-plane/control-panel-state",
        "/root/byoc/control-plane/evidence-packages",
        "/root/byoc/control-plane/preflight-reports",
        "/root/byoc/control-plane/runner-evidence",
    }
    assert all(item["timeout"] == 4.0 for item in seen)
    assert payload["responses"]["deployment_overview"]["response"]["status"] == "ready"
    assert payload["responses"]["control_panel_state"]["response"]["stored_scope"] == (
        "sanitized_control_panel_metadata_only"
    )
    assert payload["responses"]["runner_evidence"]["response"]["result_count"] == 1
    assert "headers" not in rendered.lower()
    assert SIGNING_SECRET not in rendered
    assert "install_token" not in rendered.lower()
    assert "secret_ref" not in rendered.lower()


def test_smoke_byoc_control_plane_reads_requires_signing_secret(capsys) -> None:
    code = main(_base_args())

    captured = capsys.readouterr()
    assert code == 2
    assert DEFAULT_SIGNING_SECRET_ENV in captured.err
    assert captured.out == ""


def test_smoke_byoc_control_plane_reads_requires_key_ref(monkeypatch, capsys) -> None:
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)
    args = _base_args()
    key_ref_index = args.index("--key-ref")
    del args[key_ref_index : key_ref_index + 2]

    code = main(args)

    captured = capsys.readouterr()
    assert code == 2
    assert "--key-ref is required" in captured.err
    assert captured.out == ""


def test_smoke_byoc_control_plane_reads_requires_deployment_id(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)
    args = _base_args()
    deployment_id_index = args.index("--deployment-id")
    del args[deployment_id_index : deployment_id_index + 2]

    code = main(args)

    captured = capsys.readouterr()
    assert code == 1
    assert "deployment_id" in captured.err
    assert captured.out == ""


def test_smoke_byoc_control_plane_reads_rejects_base_url_query(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    code = main(
        [
            *_base_args(),
            "--base-url",
            "https://control.example.com?debug=true",
        ]
    )

    captured = capsys.readouterr()
    assert code == 1
    assert "--base-url must not include a query string" in captured.err
    assert captured.out == ""


def test_smoke_byoc_control_plane_reads_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["deployment_overview"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.deployment_overview.v1"
    )
    assert payload["control_panel_state"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.control_panel_state.v1"
    )
    assert payload["runner_evidence_receipt_list"]["properties"]["schema_version"][
        "const"
    ] == "fyralis.byoc.runner_evidence_receipt_list.v1"
