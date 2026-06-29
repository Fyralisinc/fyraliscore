from __future__ import annotations

import json

from scripts.update_byoc_agent_desired_state import (
    DEFAULT_SIGNING_SECRET_ENV,
    main,
)
from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentDesiredStateUpdateRequest,
    validate_desired_state_update_request,
)


SIGNING_SECRET = "local-control-plane-desired-state-secret"
SIGNING_KEY_REF = "control-plane/byoc/evidence-intake-key"
DEPLOYMENT_ID = "dep_desiredupdate01"
CUSTOMER_ID = "cus_desiredupdate01"
AGENT_ID = "agt_desiredupdate01"


def _base_args() -> list[str]:
    return [
        "--deployment-id",
        DEPLOYMENT_ID,
        "--customer-id",
        CUSTOMER_ID,
        "--agent-id",
        AGENT_ID,
        "--desired-revision",
        "2026.06.27-1",
        "--config-epoch",
        "7",
        "--evidence-package-required",
        "--reason-code",
        "rollout_rehearsal",
        "--requested-by",
        "ops_backend",
        "--key-ref",
        SIGNING_KEY_REF,
        "--nonce",
        "nonce-desired-state-update-test-001",
        "--requested-at",
        "2026-06-27T12:00:00+00:00",
    ]


def test_update_byoc_agent_desired_state_prints_signed_update(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    code = main(_base_args())

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    request = ByocAgentDesiredStateUpdateRequest.model_validate(payload)
    assert code == 0
    assert request.schema_version == "fyralis.byoc.agent.desired_state_update.v1"
    assert request.deployment_id == DEPLOYMENT_ID
    assert request.customer_id == CUSTOMER_ID
    assert request.agent_id == AGENT_ID
    assert request.config_epoch == 7
    assert request.evidence_package_required is True
    assert validate_desired_state_update_request(
        request,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    ) == []
    assert SIGNING_SECRET not in rendered
    assert "artifact" not in rendered.lower()
    assert "config_body" not in rendered.lower()
    assert "payload" not in rendered.lower()
    assert captured.err == ""


def test_update_byoc_agent_desired_state_writes_output_file(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    output_path = tmp_path / "out" / "desired-state-update.json"
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    code = main([*_base_args(), "--output", str(output_path)])

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output_path.read_text(encoding="utf-8")) == json.loads(
        captured.out
    )


def test_update_byoc_agent_desired_state_posts_signed_update(
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
                    "schema_version": (
                        "fyralis.byoc.agent.desired_state_update_receipt.v1"
                    ),
                    "status": "accepted",
                    "deployment_id": DEPLOYMENT_ID,
                    "customer_id": CUSTOMER_ID,
                    "agent_id": AGENT_ID,
                    "previous_desired_revision": "2026.06.26-1",
                    "desired_revision": "2026.06.27-1",
                    "config_epoch": 7,
                    "evidence_package_required": True,
                    "accepted_at": "2026-06-27T12:01:00Z",
                    "stored_scope": "sanitized_agent_metadata_only",
                }
            ).encode("utf-8")

    def _fake_urlopen(request, *, timeout):
        posted["url"] = request.full_url
        posted["timeout"] = timeout
        posted["body"] = json.loads(request.data.decode("utf-8"))
        return _Response()

    monkeypatch.setattr(
        "scripts.update_byoc_agent_desired_state.urllib.request.urlopen",
        _fake_urlopen,
    )

    code = main(
        [
            *_base_args(),
            "--submit-url",
            "https://control.example.com/byoc/control-plane/agent-desired-state",
            "--timeout-seconds",
            "3",
        ]
    )

    captured = capsys.readouterr()
    receipt = json.loads(captured.out)
    posted_body = json.dumps(posted["body"], sort_keys=True)
    assert code == 0
    assert posted["url"] == (
        "https://control.example.com/byoc/control-plane/agent-desired-state"
    )
    assert posted["timeout"] == 3.0
    assert receipt["schema_version"] == (
        "fyralis.byoc.agent.desired_state_update_receipt.v1"
    )
    assert SIGNING_SECRET not in posted_body
    assert "artifact" not in posted_body.lower()
    assert "payload" not in posted_body.lower()


def test_update_byoc_agent_desired_state_requires_signing_secret(capsys) -> None:
    code = main(_base_args())

    captured = capsys.readouterr()
    assert code == 2
    assert DEFAULT_SIGNING_SECRET_ENV in captured.err
    assert captured.out == ""


def test_update_byoc_agent_desired_state_requires_key_ref(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)
    args = _base_args()
    key_ref_index = args.index("--key-ref")
    del args[key_ref_index : key_ref_index + 2]

    code = main(args)

    captured = capsys.readouterr()
    assert code == 2
    assert "--key-ref is required" in captured.err
    assert captured.out == ""


def test_update_byoc_agent_desired_state_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["request"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.agent.desired_state_update.v1"
    )
    assert payload["receipt"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.agent.desired_state_update_receipt.v1"
    )
