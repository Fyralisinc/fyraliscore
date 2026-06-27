from __future__ import annotations

import json

from scripts.get_byoc_deployment_overview import (
    DEFAULT_SIGNING_SECRET_ENV,
    DEPLOYMENT_OVERVIEW_PATH,
    main,
)
from services.platform.runtime.byoc_control_plane_intake import (
    validate_evidence_receipt_read_auth_headers,
)


SIGNING_SECRET = "local-control-plane-overview-read-secret"
SIGNING_KEY_REF = "control-plane/byoc/evidence-read-key"
DEPLOYMENT_ID = "dep_overviewcli01"
CUSTOMER_ID = "cus_overviewcli01"


def _base_args() -> list[str]:
    return [
        "--deployment-id",
        DEPLOYMENT_ID,
        "--customer-id",
        CUSTOMER_ID,
        "--key-ref",
        SIGNING_KEY_REF,
        "--nonce",
        "nonce-overview-read-test-001",
        "--timestamp",
        "2026-06-27T12:00:00+00:00",
    ]


def test_get_byoc_deployment_overview_prints_signed_get_request(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    code = main(_base_args())

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["method"] == "GET"
    assert payload["path"] == DEPLOYMENT_OVERVIEW_PATH
    assert payload["query"] == (
        f"deployment_id={DEPLOYMENT_ID}&customer_id={CUSTOMER_ID}"
    )
    assert validate_evidence_receipt_read_auth_headers(
        payload["headers"],
        method="GET",
        path=payload["path"],
        query=payload["query"],
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
        max_clock_skew_seconds=10**9,
    ) == []
    assert SIGNING_SECRET not in rendered
    assert "install_token" not in rendered.lower()
    assert "secret_ref" not in rendered.lower()
    assert "payload" not in rendered.lower()
    assert captured.err == ""


def test_get_byoc_deployment_overview_executes_signed_read(
    monkeypatch,
    capsys,
) -> None:
    posted: dict[str, object] = {}
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def read(self) -> bytes:
            return json.dumps(
                {
                    "schema_version": "fyralis.byoc.deployment_overview.v1",
                    "deployment_id": DEPLOYMENT_ID,
                    "customer_id": CUSTOMER_ID,
                    "generated_at": "2026-06-27T12:00:00Z",
                    "status": "ready",
                    "next_action": "none",
                    "metadata_sources": [
                        "agent_fleet",
                        "evidence_package_receipts",
                        "preflight_report_receipts",
                        "runner_evidence_receipts",
                    ],
                    "agent_summary": {
                        "enrolled_count": 1,
                        "passing_count": 1,
                        "degraded_count": 0,
                        "failing_count": 0,
                        "unknown_count": 0,
                        "connected_count": 1,
                        "disconnected_count": 0,
                        "heartbeat_observed_count": 1,
                        "evidence_package_required_count": 0,
                        "highest_desired_config_epoch": 1,
                        "current_desired_revision": "2026.06.27-1",
                        "mixed_desired_revisions": False,
                        "latest_heartbeat_accepted_at": "2026-06-27T11:59:00Z",
                        "latest_desired_state_updated_at": None,
                    },
                    "evidence_summary": {
                        "receipt_count": 1,
                        "passed_receipt_count": 1,
                        "failed_receipt_count": 0,
                        "skipped_receipt_count": 0,
                        "latest_receipt_id": "evpkg_0123456789abcdef0123456789abcdef",
                        "latest_ledger_status": "pass",
                        "latest_required_evidence_passed": True,
                        "latest_package_accepted_at": "2026-06-27T11:58:00Z",
                    },
                    "preflight_summary": {
                        "receipt_count": 1,
                        "passed_receipt_count": 1,
                        "failed_receipt_count": 0,
                        "skipped_receipt_count": 0,
                        "latest_receipt_id": "pfrep_0123456789abcdef0123456789abcdef",
                        "latest_preflight_status": "pass",
                        "latest_required_sections_passed": True,
                        "latest_failed_section_count": 0,
                        "latest_report_accepted_at": "2026-06-27T11:57:00Z",
                    },
                    "runner_summary": {
                        "receipt_count": 1,
                        "passed_receipt_count": 1,
                        "failed_receipt_count": 0,
                        "latest_receipt_id": "runev_0123456789abcdef0123456789abcdef",
                        "latest_runner_status": "pass",
                        "latest_required_checks_passed": True,
                        "latest_rollout_action": "none",
                        "latest_apply_plan_count": 0,
                        "latest_artifact_verification_count": 0,
                        "latest_digest_pinned_artifact_count": 0,
                        "latest_local_digest_checked_count": 0,
                        "latest_evidence_accepted_at": "2026-06-27T11:56:00Z",
                    },
                    "stored_scope": "sanitized_deployment_metadata_only",
                }
            ).encode("utf-8")

    def _fake_urlopen(request, *, timeout):
        posted["url"] = request.full_url
        posted["timeout"] = timeout
        posted["headers"] = dict(request.headers)
        return _Response()

    monkeypatch.setattr(
        "scripts.get_byoc_deployment_overview.urllib.request.urlopen",
        _fake_urlopen,
    )

    code = main(
        [
            *_base_args(),
            "--overview-url",
            "https://control.example.com/byoc/control-plane/deployment-overview",
            "--timeout-seconds",
            "3",
        ]
    )

    captured = capsys.readouterr()
    overview = json.loads(captured.out)
    rendered_headers = json.dumps(posted["headers"], sort_keys=True)
    assert code == 0
    assert posted["url"] == (
        "https://control.example.com/byoc/control-plane/deployment-overview"
        f"?deployment_id={DEPLOYMENT_ID}&customer_id={CUSTOMER_ID}"
    )
    assert posted["timeout"] == 3.0
    assert overview["schema_version"] == "fyralis.byoc.deployment_overview.v1"
    assert overview["status"] == "ready"
    assert overview["runner_summary"]["latest_runner_status"] == "pass"
    assert SIGNING_SECRET not in rendered_headers
    assert "install_token" not in captured.out.lower()
    assert "secret_ref" not in captured.out.lower()
    assert "payload" not in captured.out.lower()


def test_get_byoc_deployment_overview_requires_signing_secret(capsys) -> None:
    code = main(_base_args())

    captured = capsys.readouterr()
    assert code == 2
    assert DEFAULT_SIGNING_SECRET_ENV in captured.err
    assert captured.out == ""


def test_get_byoc_deployment_overview_requires_key_ref(monkeypatch, capsys) -> None:
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)
    args = _base_args()
    key_ref_index = args.index("--key-ref")
    del args[key_ref_index : key_ref_index + 2]

    code = main(args)

    captured = capsys.readouterr()
    assert code == 2
    assert "--key-ref is required" in captured.err
    assert captured.out == ""


def test_get_byoc_deployment_overview_requires_deployment_id(
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


def test_get_byoc_deployment_overview_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert "query" in payload
    assert payload["overview"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.deployment_overview.v1"
    )
