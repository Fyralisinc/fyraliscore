from __future__ import annotations

import json

from scripts.list_byoc_agents import (
    AGENT_FLEET_PATH,
    DEFAULT_SIGNING_SECRET_ENV,
    main,
)
from services.platform.runtime.byoc_control_plane_intake import (
    validate_evidence_receipt_read_auth_headers,
)


SIGNING_SECRET = "local-control-plane-fleet-read-secret"
SIGNING_KEY_REF = "control-plane/byoc/evidence-read-key"
DEPLOYMENT_ID = "dep_agentfleet01"
CUSTOMER_ID = "cus_agentfleet01"
AGENT_ID = "agt_agentfleet01"


def _base_args() -> list[str]:
    return [
        "--deployment-id",
        DEPLOYMENT_ID,
        "--customer-id",
        CUSTOMER_ID,
        "--agent-id",
        AGENT_ID,
        "--limit",
        "10",
        "--key-ref",
        SIGNING_KEY_REF,
        "--nonce",
        "nonce-agent-fleet-list-test-001",
        "--timestamp",
        "2026-06-27T12:00:00+00:00",
    ]


def test_list_byoc_agents_prints_signed_get_request(
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
    assert payload["path"] == AGENT_FLEET_PATH
    assert payload["query"] == (
        f"deployment_id={DEPLOYMENT_ID}&customer_id={CUSTOMER_ID}"
        f"&agent_id={AGENT_ID}&limit=10"
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


def test_list_byoc_agents_gets_signed_fleet_listing(
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
                    "schema_version": "fyralis.byoc.agent_fleet_list.v1",
                    "deployment_id": DEPLOYMENT_ID,
                    "customer_id": CUSTOMER_ID,
                    "agent_id": AGENT_ID,
                    "limit": 10,
                    "result_count": 1,
                    "stored_scope": "sanitized_agent_metadata_only",
                    "items": [
                        {
                            "schema_version": "fyralis.byoc.agent_fleet_item.v1",
                            "deployment_id": DEPLOYMENT_ID,
                            "customer_id": CUSTOMER_ID,
                            "agent_id": AGENT_ID,
                            "agent_version": "0.1.0",
                            "artifact_revision": "2026.06.26-1",
                            "cloud_provider": "aws",
                            "region": "us-east-1",
                            "desired_revision": "2026.06.27-1",
                            "desired_config_epoch": 7,
                            "evidence_package_required": True,
                            "heartbeat_interval_seconds": 15,
                            "telemetry_contract": "aggregate-only-v1",
                            "enrolled_at": "2026-06-27T11:59:00Z",
                            "latest_heartbeat_sequence": 1,
                            "latest_validation_status": "passing",
                            "stored_scope": "sanitized_agent_metadata_only",
                        }
                    ],
                }
            ).encode("utf-8")

    def _fake_urlopen(request, *, timeout):
        posted["url"] = request.full_url
        posted["timeout"] = timeout
        posted["headers"] = dict(request.headers)
        return _Response()

    monkeypatch.setattr(
        "scripts.list_byoc_agents.urllib.request.urlopen",
        _fake_urlopen,
    )

    code = main(
        [
            *_base_args(),
            "--list-url",
            "https://control.example.com/byoc/control-plane/agents",
            "--timeout-seconds",
            "3",
        ]
    )

    captured = capsys.readouterr()
    listing = json.loads(captured.out)
    rendered_headers = json.dumps(posted["headers"], sort_keys=True)
    assert code == 0
    assert posted["url"] == (
        "https://control.example.com/byoc/control-plane/agents"
        f"?deployment_id={DEPLOYMENT_ID}&customer_id={CUSTOMER_ID}"
        f"&agent_id={AGENT_ID}&limit=10"
    )
    assert posted["timeout"] == 3.0
    assert listing["schema_version"] == "fyralis.byoc.agent_fleet_list.v1"
    assert listing["items"][0]["desired_config_epoch"] == 7
    assert SIGNING_SECRET not in rendered_headers
    assert "install_token" not in captured.out.lower()
    assert "secret_ref" not in captured.out.lower()
    assert "payload" not in captured.out.lower()


def test_list_byoc_agents_requires_signing_secret(capsys) -> None:
    code = main(_base_args())

    captured = capsys.readouterr()
    assert code == 2
    assert DEFAULT_SIGNING_SECRET_ENV in captured.err
    assert captured.out == ""


def test_list_byoc_agents_requires_key_ref(monkeypatch, capsys) -> None:
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)
    args = _base_args()
    key_ref_index = args.index("--key-ref")
    del args[key_ref_index : key_ref_index + 2]

    code = main(args)

    captured = capsys.readouterr()
    assert code == 2
    assert "--key-ref is required" in captured.err
    assert captured.out == ""


def test_list_byoc_agents_rejects_unbounded_query(monkeypatch, capsys) -> None:
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    code = main(
        [
            "--key-ref",
            SIGNING_KEY_REF,
            "--nonce",
            "nonce-agent-fleet-list-test-002",
        ]
    )

    captured = capsys.readouterr()
    assert code == 1
    assert "deployment_id or customer_id" in captured.err
    assert captured.out == ""


def test_list_byoc_agents_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert "query" in payload
    assert payload["list"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.agent_fleet_list.v1"
    )
