from __future__ import annotations

import json

import scripts.smoke_byoc_control_panel_proxy as smoke


def test_control_panel_proxy_smoke_prints_redacted_request_plan(capsys) -> None:
    result = smoke.main(
        [
            "--customer-id",
            "cus_proxy01",
            "--deployment-id",
            "dep_proxy01",
            "--recent-limit",
            "7",
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    rendered = json.dumps(payload).lower()
    assert payload["schema_version"] == (
        "fyralis.byoc.control_panel_proxy_smoke_plan.v1"
    )
    assert payload["headers"]["Authorization"] == "Bearer <redacted>"
    assert payload["requests"][0]["path"] == "/byoc/control-panel/deployments"
    assert payload["requests"][1]["path"] == "/byoc/control-panel/state"
    assert "secret" not in rendered
    assert "token-value" not in rendered


def test_control_panel_proxy_smoke_executes_sanitized_summary(
    monkeypatch,
    capsys,
) -> None:
    calls: list[tuple[str, str]] = []

    def _fake_get_json(url: str, *, bearer_token: str, timeout_seconds: float):
        calls.append((url, bearer_token))
        if url.endswith("/byoc/control-panel/deployments?customer_id=cus_proxy01"):
            return {
                "schema_version": "fyralis.byoc.control_panel_access_grant_list.v1",
                "result_count": 1,
                "items": [
                    {
                        "tenant_id": "11111111-1111-4111-8111-111111111111",
                        "customer_id": "cus_proxy01",
                        "deployment_ids": ["dep_proxy01"],
                        "role": "viewer",
                        "enabled": True,
                    }
                ],
            }
        assert "deployment_id=dep_proxy01" in url
        return {
            "schema_version": "fyralis.byoc.control_panel_state.v1",
            "stored_scope": "sanitized_control_panel_metadata_only",
            "sections": [{"code": "agent_fleet"}],
            "actions": [{"code": "review_evidence"}],
        }

    monkeypatch.setenv("FYRALIS_GATEWAY_BEARER_TOKEN", "gateway-token-value")
    monkeypatch.setattr(smoke, "_get_json", _fake_get_json)

    result = smoke.main(
        [
            "--base-url",
            "https://control.example.test",
            "--customer-id",
            "cus_proxy01",
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    rendered = json.dumps(payload).lower()
    assert payload["schema_version"] == "fyralis.byoc.control_panel_proxy_smoke.v1"
    assert payload["deployment_grant_count"] == 1
    assert payload["selected_deployment_id"] == "dep_proxy01"
    assert payload["section_count"] == 1
    assert payload["action_count"] == 1
    assert payload["bearer_token_included"] is False
    assert "gateway-token-value" not in rendered
    assert calls[0][1] == "gateway-token-value"


def test_control_panel_proxy_smoke_requires_token_for_execution(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.delenv("FYRALIS_GATEWAY_BEARER_TOKEN", raising=False)

    result = smoke.main(["--base-url", "https://control.example.test"])

    assert result == 2
    captured = capsys.readouterr()
    assert "FYRALIS_GATEWAY_BEARER_TOKEN" in captured.err
