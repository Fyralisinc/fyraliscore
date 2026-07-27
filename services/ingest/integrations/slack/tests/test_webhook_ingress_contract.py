from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import UUID

import pytest
from starlette.requests import Request

from services.ingest.integrations.slack import webhook_ingress
from services.ingest.integrations.slack.webhook_ingress import (
    handle_verified_pre_tenant,
    handle_verified_tenant,
)


_TENANT_ID = UUID("11111111-1111-1111-1111-111111111111")
_INSTALLATION_ROW_ID = UUID("22222222-2222-2222-2222-222222222222")


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/webhooks/slack/events",
            "raw_path": b"/webhooks/slack/events",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 1),
            "server": ("test", 80),
        }
    )


def _runtime(**overrides: object) -> SimpleNamespace:
    values: dict[str, object | None] = {
        "pool": None,
        "secret_store": None,
        "tenant_resolver": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _outcome() -> SimpleNamespace:
    return SimpleNamespace(
        tenant_id=_TENANT_ID,
        installation_row_id=_INSTALLATION_ROW_ID,
    )


async def test_pre_tenant_policy_handles_only_url_verification() -> None:
    response = await handle_verified_pre_tenant(
        request=_request(),
        runtime=_runtime(),
        payload={
            "type": "url_verification",
            "token": "legacy-token-is-not-trusted",
            "challenge": "challenge-123",
        },
    )
    assert response is not None
    assert response.status_code == 200
    assert json.loads(response.body) == {"challenge": "challenge-123"}

    ordinary = await handle_verified_pre_tenant(
        request=_request(),
        runtime=_runtime(),
        payload={"team_id": "T_EXACT", "event": {"type": "message"}},
    )
    assert ordinary is None


@pytest.mark.parametrize(
    ("event_type", "handler_name"),
    (
        ("app_uninstalled", "handle_app_uninstalled"),
        ("tokens_revoked", "handle_tokens_revoked"),
    ),
)
async def test_tenant_policy_dispatches_lifecycle_with_exact_identity(
    monkeypatch: pytest.MonkeyPatch,
    event_type: str,
    handler_name: str,
) -> None:
    pool = object()
    secret_store = object()
    resolver = object()
    captured: list[object] = []

    async def handler(*args: object) -> None:
        captured.extend(args)

    monkeypatch.setattr(webhook_ingress.uninstall, handler_name, handler)
    response = await handle_verified_tenant(
        request=_request(),
        runtime=_runtime(
            pool=pool,
            secret_store=secret_store,
            tenant_resolver=resolver,
        ),
        outcome=_outcome(),
        tenant_id=_TENANT_ID,
        payload={
            "team_id": "T_EXACT",
            "type": "event_callback",
            "event": {"type": event_type},
        },
        verified=object(),
    )

    assert response is not None
    assert response.status_code == 200
    assert json.loads(response.body) == {"handled": event_type}
    assert captured == [
        pool,
        secret_store,
        resolver,
        _TENANT_ID,
        _INSTALLATION_ROW_ID,
        "T_EXACT",
    ]


async def test_tenant_policy_acks_lifecycle_when_runtime_dependencies_missing() -> None:
    response = await handle_verified_tenant(
        request=_request(),
        runtime=_runtime(),
        outcome=_outcome(),
        tenant_id=_TENANT_ID,
        payload={
            "team_id": "T_EXACT",
            "event": {"type": "app_uninstalled"},
        },
        verified=object(),
    )

    assert response is not None
    assert response.status_code == 200
    assert json.loads(response.body) == {"handled": "app_uninstalled"}


async def test_tenant_policy_leaves_non_lifecycle_events_for_ingestion() -> None:
    response = await handle_verified_tenant(
        request=_request(),
        runtime=_runtime(),
        outcome=_outcome(),
        tenant_id=_TENANT_ID,
        payload={
            "team_id": "T_EXACT",
            "event": {"type": "message"},
        },
        verified=object(),
    )
    assert response is None
