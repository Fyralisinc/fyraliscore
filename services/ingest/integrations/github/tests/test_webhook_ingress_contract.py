from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import UUID

import pytest
from starlette.requests import Request

from services.app.webhooks import router
from services.ingest.integrations.github import metrics
from services.ingest.integrations.github.replay_cache import ReplayCache
from services.ingest.integrations.github.webhook_ingress import (
    handle_verified_pre_tenant,
    handle_verified_tenant,
)
from services.ingest.source_contract import webhook_ingress_definition


_TENANT_ID = UUID("11111111-1111-1111-1111-111111111111")
_INSTALLATION_ROW_ID = UUID("22222222-2222-2222-2222-222222222222")


def _request(*, event: str, delivery: str = "delivery-1") -> Request:
    headers = [
        (b"x-github-event", event.encode()),
        (b"x-github-delivery", delivery.encode()),
    ]
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/webhooks/github",
            "raw_path": b"/webhooks/github",
            "query_string": b"",
            "headers": headers,
            "client": ("127.0.0.1", 1),
            "server": ("test", 80),
        }
    )


def _runtime(**overrides: object) -> SimpleNamespace:
    values: dict[str, object | None] = {
        "pool": None,
        "secret_store": None,
        "tenant_resolver": None,
        "tenant_flags": None,
        "kafka_producer": None,
        "s3_raw_client": None,
        "github_client": None,
        "github_replay_cache": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    metrics.reset()


async def test_pre_tenant_ping_and_replay_policy() -> None:
    ping = await handle_verified_pre_tenant(
        request=_request(event="ping"),
        runtime=_runtime(),
        payload={"zen": "Keep it logically awesome."},
    )
    assert ping is not None
    assert ping.status_code == 200
    assert json.loads(ping.body) == {"handled": "ping"}
    assert metrics.get_counter("github_webhook_verified_total", result="ok") == 1

    runtime = _runtime(github_replay_cache=ReplayCache())
    payload = {"installation": {"id": 12345}}
    first = await handle_verified_pre_tenant(
        request=_request(event="issues", delivery="same-delivery"),
        runtime=runtime,
        payload=payload,
    )
    replay = await handle_verified_pre_tenant(
        request=_request(event="issues", delivery="same-delivery"),
        runtime=runtime,
        payload=payload,
    )
    assert first is None
    assert replay is not None
    assert json.loads(replay.body) == {"handled": "replay"}
    assert metrics.get_counter("github_webhook_replay_dropped_total") == 1


async def test_tenant_policy_filters_against_exact_installation_row() -> None:
    class Pool:
        async def fetchrow(self, query: str, installation_row_id: UUID):
            assert "WHERE id = $1" in query
            assert installation_row_id == _INSTALLATION_ROW_ID
            return {"selected_repositories": '["octo/allowed"]'}

    outcome = SimpleNamespace(
        tenant_id=_TENANT_ID,
        installation_row_id=_INSTALLATION_ROW_ID,
    )
    filtered = await handle_verified_tenant(
        request=_request(event="pull_request"),
        runtime=_runtime(pool=Pool()),
        outcome=outcome,
        tenant_id=_TENANT_ID,
        payload={
            "installation": {"id": 12345},
            "repository": {"full_name": "octo/other"},
        },
        verified=object(),
    )
    assert filtered is not None
    assert json.loads(filtered.body) == {"handled": "filtered_repo"}

    allowed = await handle_verified_tenant(
        request=_request(event="pull_request", delivery="delivery-2"),
        runtime=_runtime(pool=Pool()),
        outcome=outcome,
        tenant_id=_TENANT_ID,
        payload={
            "installation": {"id": 12345},
            "repository": {"full_name": "octo/allowed"},
        },
        verified=object(),
    )
    assert allowed is None


async def test_tenant_policy_dispatches_lifecycle_with_exact_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_cache: dict[str, object] = {}
    resolver = object()
    captured: dict[str, object] = {}

    async def dispatch(**kwargs: object) -> dict[str, str]:
        captured.update(kwargs)
        return {"handled": "installation_deleted"}

    monkeypatch.setattr(
        "services.ingest.integrations.github.webhook_ingress.lifecycle.dispatch",
        dispatch,
    )
    outcome = SimpleNamespace(
        tenant_id=_TENANT_ID,
        installation_row_id=_INSTALLATION_ROW_ID,
    )
    response = await handle_verified_tenant(
        request=_request(event="installation"),
        runtime=_runtime(
            pool=object(),
            github_client=SimpleNamespace(_installation_tokens=token_cache),
            tenant_resolver=resolver,
        ),
        outcome=outcome,
        tenant_id=_TENANT_ID,
        payload={"action": "deleted", "installation": {"id": 12345}},
        verified=object(),
    )

    assert response is not None
    assert json.loads(response.body) == {"handled": "installation_deleted"}
    assert captured["event_type"] == "installation"
    assert captured["tenant_id"] == _TENANT_ID
    assert captured["installation_row_id"] == _INSTALLATION_ROW_ID
    assert captured["installation_id"] == "12345"
    assert captured["installation_token_cache"] is token_cache
    assert captured["tenant_resolver"] is resolver


async def test_router_rejects_invalid_contract_hook_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def invalid_handler(**_: object) -> object:
        return object()

    monkeypatch.setattr(
        router,
        "resolve_webhook_verified_pre_tenant_handler",
        lambda _provider: invalid_handler,
    )
    response = await router._verified_pre_tenant_response(
        _request(event="issues"),
        provider="github",
        ingress=webhook_ingress_definition("github"),
        runtime=router.WebhookRuntime(**vars(_runtime())),
        payload={"installation": {"id": 12345}},
    )

    assert response is not None
    assert response.status_code == 503
    assert json.loads(response.body)["code"] == "webhook_processing_unavailable"


async def test_router_rejects_contract_hook_resolution_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(_provider: str) -> None:
        raise RuntimeError("missing binding")

    monkeypatch.setattr(
        router,
        "resolve_webhook_verified_pre_tenant_handler",
        fail,
    )
    response = await router._verified_pre_tenant_response(
        _request(event="issues"),
        provider="github",
        ingress=webhook_ingress_definition("github"),
        runtime=router.WebhookRuntime(**vars(_runtime())),
        payload={"installation": {"id": 12345}},
    )

    assert response is not None
    assert response.status_code == 503
