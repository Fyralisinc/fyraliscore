"""Router-level tests.

Spec: US1 / US2 / FR-001 / FR-002 / FR-014 / FR-017 / FR-018 / SC-006, SC-008.

These tests exercise the FastAPI router under the real gateway app
configuration — Bearer middleware must skip /webhooks/, the body-size
precheck must apply, the Slack url_verification handshake must work,
and unknown providers must 404.

The tests use a minimal FastAPI app with hand-stubbed `app.state` so
they do not require a live Postgres or Ollama for the path-routing
assertions. The E2E integration test (test_e2e_ingest.py) covers the
real-DB path. IN-08 introduced `app.state.tenant_resolver` as a hard
dependency of the router; these tests stub it with a coroutine that
returns a `Resolved` outcome bound to `_TENANT` by default.
"""
from __future__ import annotations

import json
import os
import re
import time as _t
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest

from lib.shared.errors import DependencyUnavailableError, ValidationError
from services.app.webhooks.tenant_resolver import (
    PayloadMissing,
    Resolved,
    UnknownInstallation,
)
from services.app.webhooks.tests.conftest import slack_sign
from services.ingest.ingestion.handlers import HandlerNotFound
from services.ingest.source_contract import WEBHOOK_INGRESS_CATALOG


_TENANT = UUID("11111111-1111-1111-1111-111111111111")
_INSTALLATION_ROW_ID = UUID("22222222-2222-2222-2222-222222222222")


class _StubResolver:
    """Minimal stub satisfying the TenantResolver surface used by the
    router: an async `resolve(provider, payload, headers)` returning
    one of the IN-07 outcome models.

    Default behavior: return `Resolved(_TENANT, ...)` for any payload
    that names a non-empty `team_id`; return `PayloadMissing` otherwise
    (so the URL-verification handshake path still works). Tests that
    need `UnknownInstallation` instantiate this with `force_outcome`.
    """

    def __init__(self, force_outcome=None) -> None:
        self._force = force_outcome

    async def resolve(self, provider, payload, headers, *, subpath=None):
        if self._force is not None:
            return self._force
        team_id = (payload or {}).get("team_id") if isinstance(payload, dict) else None
        if team_id:
            return Resolved(
                tenant_id=_TENANT,
                installation_row_id=_INSTALLATION_ROW_ID,
                secret_ref=None,
            )
        return PayloadMissing(provider=provider)


@pytest.fixture
def _patch_secrets_and_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wire env-based secrets for the test app. Tenant resolution is
    stubbed via `_router_app`'s `app.state.tenant_resolver`."""
    monkeypatch.setenv("WEBHOOK_SECRET_SLACK", "router-test-slack")


@pytest.fixture
def _router_app(_patch_secrets_and_tenant: None):
    """Build a FastAPI app with ONLY the webhook router mounted, plus
    stub `app.state` so the router's tenant resolver + ingestion deps
    have something to resolve. The path-routing tests don't reach the
    ingestion code; tests that do (e.g. successful 201) use the real
    `test_e2e_ingest.py` slice."""
    from fastapi import FastAPI

    from services.app.webhooks.router import build_webhooks_router

    app = FastAPI()
    app.include_router(build_webhooks_router())

    deps = MagicMock()
    deps.pool = MagicMock()
    deps.actor_repo = None
    deps.alias_repo = None
    deps.embedder = None
    app.state.deps = deps
    app.state.tenant_resolver = _StubResolver()
    # Tests fall back to env-var secrets (autouse fixture in conftest);
    # no secret_store is wired so load_installation_secrets bypasses the DB path
    # gracefully and reads `WEBHOOK_SECRET_SLACK`.
    return app


@pytest.mark.asyncio
async def test_unknown_provider_returns_404(_router_app) -> None:
    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/webhooks/twilio/inbound", content=b"{}")
    assert r.status_code == 404


def _materialize_declared_route(route_path: str) -> str:
    return re.sub(r"{[^{}]+}", "route-cert-installation", route_path)


def test_router_mounts_only_catalog_declared_webhook_routes(
    _router_app,
) -> None:
    from fastapi.routing import APIRoute

    mounted = [
        (route.path, method)
        for route in _router_app.routes
        if isinstance(route, APIRoute)
        for method in route.methods
    ]
    declared = [
        (ingress.route_path, "POST")
        for ingress in WEBHOOK_INGRESS_CATALOG.values()
    ]

    assert sorted(mounted) == sorted(declared)
    assert len(mounted) == len(set(mounted))
    assert "/webhooks/{provider}" not in {path for path, _method in mounted}
    assert "/webhooks/{provider}/{subpath:path}" not in {
        path for path, _method in mounted
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("route_id", "route_path"),
    [
        (route_id, ingress.route_path)
        for route_id, ingress in WEBHOOK_INGRESS_CATALOG.items()
    ],
)
async def test_every_declared_webhook_route_dispatches_to_its_owner(
    _router_app,
    route_id: str,
    route_path: str,
) -> None:
    from services.ingest.ingestion.core import MAX_PAYLOAD_BYTES

    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        response = await c.post(
            _materialize_declared_route(route_path),
            content=b"x" * (MAX_PAYLOAD_BYTES + 1),
        )

    assert response.status_code == 413
    assert response.json()["context"]["provider"] == route_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "route_path",
    [
        ingress.route_path
        for ingress in WEBHOOK_INGRESS_CATALOG.values()
    ],
)
async def test_undeclared_webhook_subpath_is_rejected(
    _router_app,
    route_path: str,
) -> None:
    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://t",
        follow_redirects=False,
    ) as c:
        response = await c.post(
            f"{_materialize_declared_route(route_path)}/undeclared",
            content=b"{}",
        )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_oversize_body_413(_router_app) -> None:
    from services.ingest.ingestion.core import MAX_PAYLOAD_BYTES

    oversize = b"x" * (MAX_PAYLOAD_BYTES + 1)
    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/webhooks/slack/events", content=oversize)
    assert r.status_code == 413
    assert r.json()["code"] == "payload_too_large"


@pytest.mark.asyncio
async def test_missing_signature_returns_401(_router_app) -> None:
    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/webhooks/slack/events", content=b'{"team_id":"T"}')
    assert r.status_code == 401
    body = r.json()
    assert body["context"]["reason"] == "missing_signature_header"
    assert body["context"]["provider"] == "slack"


@pytest.mark.asyncio
async def test_contract_secret_loader_failure_fails_closed(
    _router_app,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _failing_loader(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("secret backend carried sensitive diagnostics")

    monkeypatch.setattr(
        "services.app.webhooks.router.resolve_webhook_secret_loader",
        lambda route_id: _failing_loader,
    )

    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        response = await c.post(
            "/webhooks/slack/events",
            content=b'{"team_id":"T0001"}',
        )

    assert response.status_code == 503
    assert response.headers["Retry-After"] == "30"
    assert response.json() == {
        "code": "webhook_processing_unavailable",
        "message": "webhook processing temporarily unavailable",
        "context": {"provider": "slack"},
    }
    assert "sensitive" not in response.text


@pytest.mark.asyncio
async def test_contract_secret_loader_malformed_result_fails_closed(
    _router_app,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _malformed_loader(*args, **kwargs):
        del args, kwargs
        return {"value": "must-not-be-treated-as-a-secret"}

    monkeypatch.setattr(
        "services.app.webhooks.router.resolve_webhook_secret_loader",
        lambda route_id: _malformed_loader,
    )

    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        response = await c.post(
            "/webhooks/slack/events",
            content=b'{"team_id":"T0001"}',
        )

    assert response.status_code == 503
    assert response.json()["code"] == "webhook_processing_unavailable"
    assert "must-not" not in response.text


@pytest.mark.asyncio
async def test_integration_runtime_resolver_preferred_over_legacy_alias(
    _patch_secrets_and_tenant: None,
) -> None:
    from fastapi import FastAPI

    from services.app.webhooks.router import build_webhooks_router

    class _ExplodingLegacyResolver:
        async def resolve(self, provider, payload, headers, *, subpath=None):
            raise AssertionError("legacy tenant_resolver alias was used")

    app = FastAPI()
    app.include_router(build_webhooks_router())
    app.state.deps = MagicMock()
    app.state.tenant_resolver = _ExplodingLegacyResolver()
    app.state.integration_runtime = SimpleNamespace(
        pool=None,
        secret_store=None,
        tenant_resolver=_StubResolver(),
        tenant_flags=None,
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post("/webhooks/slack/events", content=b'{"team_id":"T"}')

    assert r.status_code == 401
    body = r.json()
    assert body["context"]["reason"] == "missing_signature_header"
    assert body["context"]["provider"] == "slack"


@pytest.mark.asyncio
async def test_spoofed_signature_returns_401(_router_app) -> None:
    import time as _t

    body = b'{"team_id":"T0001"}'
    ts = str(int(_t.time()))  # use real now — router uses real time.time()
    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": ts,
                "X-Slack-Signature": "v0=" + ("00" * 32),
            },
        )
    assert r.status_code == 401
    body_json = r.json()
    assert body_json["context"]["reason"] == "signature_mismatch"
    # Critical: response MUST NOT leak the body or the candidate sig.
    rendered = json.dumps(body_json)
    assert "team_id" not in rendered
    assert "00" * 32 not in rendered


@pytest.mark.asyncio
async def test_slack_url_verification_handshake(_router_app) -> None:
    """Slack sends a url_verification event on app install with a
    `challenge`. We verify the signature (still!) and echo the
    challenge — no Observation, no ingestion call.
    """
    import time as _t

    secret = os.environ["WEBHOOK_SECRET_SLACK"]
    body = json.dumps({
        "type": "url_verification",
        "token": "abc",
        "challenge": "chal-12345",
    }).encode("utf-8")
    ts = int(_t.time())
    sig = slack_sign(secret, body, ts)

    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
        )
    assert r.status_code == 200
    assert r.json() == {"challenge": "chal-12345"}


@pytest.mark.asyncio
async def test_slack_url_verification_requires_valid_signature(_router_app) -> None:
    """The contract hook must never turn the challenge into an auth bypass."""

    body = json.dumps(
        {
            "type": "url_verification",
            "token": "abc",
            "challenge": "must-not-be-echoed",
        }
    ).encode("utf-8")
    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        response = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(int(_t.time())),
                "X-Slack-Signature": "v0=" + ("00" * 32),
            },
        )

    assert response.status_code == 401
    assert response.json()["context"]["reason"] == "signature_mismatch"
    assert "must-not-be-echoed" not in response.text


@pytest.mark.asyncio
async def test_unknown_installation_returns_401(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """IN-08 SC-007: a verified payload whose installation_id cannot
    be mapped to an enabled `provider_installations` row returns 401
    `unknown_installation`. The team_id MUST NOT appear in the
    rendered response (defense against workspace-enumeration probes).
    """
    import time as _t
    from fastapi import FastAPI
    from services.app.webhooks.router import build_webhooks_router

    monkeypatch.setenv("WEBHOOK_SECRET_SLACK", "trsecret")

    app = FastAPI()
    app.include_router(build_webhooks_router())
    deps = MagicMock()
    app.state.deps = deps
    # Force the resolver to return UnknownInstallation regardless of payload.
    app.state.tenant_resolver = _StubResolver(
        force_outcome=UnknownInstallation(provider="slack"),
    )

    body = b'{"team_id":"T_UNKNOWN","event":{"type":"message","ts":"1","channel":"C","user":"U","text":"hi"}}'
    ts = int(_t.time())
    sig = slack_sign("trsecret", body, ts)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
        )
    assert r.status_code == 401
    body_json = r.json()
    assert body_json["context"]["reason"] == "unknown_installation"
    # SC-007: forged team_id must not leak in the response body.
    assert "T_UNKNOWN" not in json.dumps(body_json)


@pytest.mark.asyncio
async def test_failure_metric_increments(_router_app) -> None:
    """A 401 must bump the (provider, reason) counter."""
    from services.app.webhooks import metrics

    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        await c.post("/webhooks/slack/events", content=b"{}")
    assert metrics.get_count("slack", "missing_signature_header") == 1


@pytest.mark.asyncio
async def test_inline_validation_error_response_is_sanitized(
    _router_app,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_ingest = AsyncMock(
        side_effect=ValidationError(
            "secret parser detail: team_id=T_LEAK",
            channel="slack:message",
            raw_payload={"team_id": "T_LEAK"},
        )
    )
    monkeypatch.setattr("services.app.webhooks.router.ingest", mock_ingest)

    body = json.dumps({
        "team_id": "T0001",
        "event": {
            "type": "message",
            "ts": "1780184162.000102",
            "channel": "C01",
            "user": "U01",
            "text": "hi",
        },
    }).encode("utf-8")
    ts = int(_t.time())
    sig = slack_sign(os.environ["WEBHOOK_SECRET_SLACK"], body, ts)

    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
        )

    assert r.status_code == 400
    assert r.json() == {
        "code": "webhook_payload_rejected",
        "message": "webhook payload rejected",
        "context": {"provider": "slack"},
    }
    rendered = json.dumps(r.json())
    assert "T_LEAK" not in rendered
    assert "slack:message" not in rendered


@pytest.mark.asyncio
async def test_recoverable_inline_error_returns_retryable_sanitized_response(
    _router_app,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_ingest = AsyncMock(
        side_effect=DependencyUnavailableError(
            "postgres",
            "inline_ingest",
            message="dsn password=super-secret unavailable",
            dsn="postgresql://password=super-secret",
        )
    )
    monkeypatch.setattr("services.app.webhooks.router.ingest", mock_ingest)

    body = json.dumps({
        "team_id": "T0001",
        "event": {
            "type": "message",
            "ts": "1780184162.000103",
            "channel": "C01",
            "user": "U01",
            "text": "hi",
        },
    }).encode("utf-8")
    ts = int(_t.time())
    sig = slack_sign(os.environ["WEBHOOK_SECRET_SLACK"], body, ts)

    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
        )

    assert r.status_code == 503
    assert r.headers["Retry-After"] == "30"
    assert r.json() == {
        "code": "webhook_processing_unavailable",
        "message": "webhook processing temporarily unavailable",
        "context": {"provider": "slack"},
    }
    rendered = json.dumps(r.json())
    assert "super-secret" not in rendered
    assert "postgres" not in rendered


@pytest.mark.asyncio
async def test_missing_handler_returns_retryable_sanitized_response(
    _router_app,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_ingest = AsyncMock(
        side_effect=HandlerNotFound(
            "no handler for channel slack:message",
            channel="slack:message",
        )
    )
    monkeypatch.setattr("services.app.webhooks.router.ingest", mock_ingest)

    body = json.dumps({
        "team_id": "T0001",
        "event": {
            "type": "message",
            "ts": "1780184162.000104",
            "channel": "C01",
            "user": "U01",
            "text": "hi",
        },
    }).encode("utf-8")
    ts = int(_t.time())
    sig = slack_sign(os.environ["WEBHOOK_SECRET_SLACK"], body, ts)

    transport = httpx.ASGITransport(app=_router_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
        )

    assert r.status_code == 503
    assert r.headers["Retry-After"] == "30"
    assert r.json() == {
        "code": "webhook_processing_unavailable",
        "message": "webhook processing temporarily unavailable",
        "context": {"provider": "slack"},
    }
    assert "slack:message" not in json.dumps(r.json())
