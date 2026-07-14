"""Focused non-DB tests for the OAuth-first Figma connector."""
from __future__ import annotations

import hashlib
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import httpx
import pytest

from lib.shared.errors import StateTokenInvalidError
from services.ingest.integrations.figma.client import FigmaClient
from services.ingest.integrations.figma import oauth as figma_oauth


class _SecretStore:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.deleted: list[str] = []

    async def put(self, plaintext: str, *, label: str, tenant_id: Any) -> str:  # noqa: ARG002
        ref = f"ref-{len(self.values) + 1}"
        self.values[ref] = plaintext
        return ref

    async def get(self, ref: str, *, tenant_id: Any) -> bytes:  # noqa: ARG002
        return self.values[ref].encode("utf-8")

    async def delete(self, ref: str, *, tenant_id: Any) -> None:  # noqa: ARG002
        self.deleted.append(ref)
        self.values.pop(ref, None)


class _StatePool:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}

    async def execute(self, query: str, *args: Any) -> str:
        assert "INSERT INTO oauth_install_states" in query
        _, tenant_id, nonce, expires_at, context = args
        self.rows[nonce] = {
            "tenant_id": tenant_id,
            "expires_at": expires_at,
            "consumed_at": None,
            "provider": "figma",
            "context": json.loads(context),
        }
        return "INSERT 0 1"

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        nonce = args[0]
        row = self.rows.get(nonce)
        if "UPDATE oauth_install_states" in query:
            if (
                row is None
                or row["provider"] != "figma"
                or row["consumed_at"] is not None
            ):
                return None
            row["consumed_at"] = object()
            return {"tenant_id": row["tenant_id"], "context": row["context"]}
        if row is None:
            return None
        return {
            "provider": row["provider"],
            "consumed_at": row["consumed_at"],
            "expires_at": row["expires_at"],
        }


def test_selected_file_urls_and_callback_origin(monkeypatch: pytest.MonkeyPatch) -> None:
    assert figma_oauth._selected_file_keys([
        "https://www.figma.com/Design/AbC123456/Checkout",
        "https://www.figma.com/file/AbC123456/duplicate",
    ]) == ["AbC123456"]

    monkeypatch.setenv("FIGMA_OAUTH_UI_BASE_URL", "https://ui.fyralis.test/console")
    location = figma_oauth._callback_location(
        "/onboarding?step=sources",
        state="connected",
        installation_id=uuid4(),
    )
    parsed = urlsplit(location)
    assert parsed.scheme == "https"
    assert parsed.netloc == "ui.fyralis.test"
    assert parsed.path == "/console/onboarding"
    assert parse_qs(parsed.query)["figma"] == ["connected"]

    monkeypatch.setenv("COMPANY_OS_ENV", "prod")
    monkeypatch.setenv("FIGMA_OAUTH_UI_BASE_URL", "http://localhost:3003")
    monkeypatch.delenv("FIGMA_OAUTH_ALLOW_HTTP_LOOPBACK", raising=False)
    with pytest.raises(figma_oauth.FigmaOAuthError):
        figma_oauth._ui_base_url()
    monkeypatch.setenv("FIGMA_OAUTH_ALLOW_HTTP_LOOPBACK", "1")
    assert figma_oauth._ui_base_url() == "http://localhost:3003"


def test_byoc_deployment_readiness_is_admin_safe_and_least_privilege(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The admin contract exposes setup facts, never app credentials."""
    monkeypatch.setenv("COMPANY_OS_ENV", "development")
    monkeypatch.setenv("FYRALIS_ENV", "development")
    monkeypatch.setenv("FIGMA_OAUTH_ENABLED", "1")
    monkeypatch.setenv("FIGMA_CLIENT_ID", "customer-figma-client")
    monkeypatch.setenv("FIGMA_CLIENT_SECRET", "customer-figma-secret")
    monkeypatch.setenv(
        "FIGMA_REDIRECT_URI",
        "https://ingress.customer.test/integrations/figma/oauth/callback",
    )
    monkeypatch.setenv("FIGMA_OAUTH_UI_BASE_URL", "https://console.customer.test")
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "customer-state-hmac")
    monkeypatch.delenv("FIGMA_OAUTH_SCOPES", raising=False)

    readiness = figma_oauth._deployment_oauth_admin_readiness()
    serialized = json.dumps(readiness, sort_keys=True)

    assert readiness["runtime_ready"] is True
    assert readiness["deployment_model"] == "customer_owned_byoc_oauth_app"
    assert readiness["recommended_app_mode"] == "private"
    assert readiness["redirect_uri"] == (
        "https://ingress.customer.test/integrations/figma/oauth/callback"
    )
    assert readiness["required_scopes"] == [
        "current_user:read",
        "file_metadata:read",
        "file_content:read",
        "file_comments:read",
        "file_versions:read",
    ]
    assert "customer-figma-secret" not in serialized
    assert "customer-state-hmac" not in serialized
    assert "FIGMA_CLIENT_SECRET" not in serialized


def test_byoc_oauth_requires_exact_callback_and_supported_scopes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "FIGMA_REDIRECT_URI", "https://ingress.customer.test/not-figma"
    )
    with pytest.raises(figma_oauth.FigmaOAuthError) as redirect_error:
        figma_oauth._figma_redirect_uri()
    assert redirect_error.value.code == "figma_oauth_redirect_invalid"

    monkeypatch.setenv(
        "FIGMA_OAUTH_SCOPES", "current_user:read,file_content:read,webhooks:write"
    )
    with pytest.raises(figma_oauth.FigmaOAuthError) as scopes_error:
        figma_oauth._oauth_scopes()
    assert scopes_error.value.code == "figma_oauth_scopes_invalid"


def test_byoc_oauth_uses_one_validated_api_host_for_validation_and_fetching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COMPANY_OS_ENV", "development")
    monkeypatch.setenv("FYRALIS_ENV", "development")
    monkeypatch.setenv("FIGMA_API_BASE_URL", "http://figma-mock.test/v1/")
    assert figma_oauth._figma_api_base_url() == "http://figma-mock.test/v1"

    monkeypatch.setenv("COMPANY_OS_ENV", "prod")
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    with pytest.raises(figma_oauth.FigmaOAuthError) as api_base_error:
        figma_oauth._figma_api_base_url()
    assert api_base_error.value.code == "figma_api_base_url_invalid"


async def test_pkce_state_is_single_use_and_verifier_is_encrypted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "figma-state-test-key")
    tenant_id = uuid4()
    pool = _StatePool()
    store = _SecretStore()

    state, challenge = await figma_oauth._issue_figma_state(
        tenant_id=tenant_id,
        pool=pool,  # type: ignore[arg-type]
        secret_store=store,
        file_keys=["AbC123456"],
        return_path="/onboarding",
    )

    stored_context = next(iter(pool.rows.values()))["context"]
    verifier_ref = stored_context["pkce_verifier_ref"]
    verifier = store.values[verifier_ref]
    expected_challenge = figma_oauth._b64url(hashlib.sha256(verifier.encode("ascii")).digest())
    assert challenge == expected_challenge
    assert verifier not in state

    verified_tenant, context = await figma_oauth._verify_and_consume_figma_state(
        state, pool,  # type: ignore[arg-type]
    )
    assert verified_tenant == tenant_id
    assert context["file_keys"] == ["AbC123456"]
    with pytest.raises(StateTokenInvalidError) as exc:
        await figma_oauth._verify_and_consume_figma_state(state, pool)  # type: ignore[arg-type]
    assert exc.value.reason == "state_consumed"


@pytest.mark.parametrize(
    ("auth_kind", "expected_header", "unexpected_header"),
    [
        ("oauth", "authorization", "x-figma-token"),
        ("pat", "x-figma-token", "authorization"),
    ],
)
async def test_client_uses_the_correct_figma_auth_header(
    auth_kind: str,
    expected_header: str,
    unexpected_header: str,
) -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"id": "user-1"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FigmaClient(
        base_url="https://api.figma.test",
        api_token="test-token",
        auth_kind=auth_kind,
        http_client=http,
    )
    await client.get_current_user()
    await http.aclose()

    assert requests[0].headers[expected_header]
    assert unexpected_header not in requests[0].headers


@pytest.mark.parametrize("expired_status", [401, 403])
async def test_client_retries_an_expired_oauth_response_once_with_refreshed_token(
    monkeypatch: pytest.MonkeyPatch,
    expired_status: int,
) -> None:
    refresh_calls: list[bool] = []

    async def fake_refresh(**kwargs: Any) -> tuple[str, str, str, datetime] | None:
        refresh_calls.append(bool(kwargs["force"]))
        if not kwargs["force"]:
            return None
        return "refreshed-token", "new-access-ref", "refresh-ref", datetime.now(timezone.utc)

    monkeypatch.setattr(figma_oauth, "refresh_installation_access_token", fake_refresh)
    seen_tokens: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        token = request.headers.get("Authorization", "")
        seen_tokens.append(token)
        if token == "Bearer old-token":
            return httpx.Response(expired_status, json={"err": "expired"})
        return httpx.Response(200, json={"id": "user-1"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = FigmaClient(
        base_url="https://api.figma.test",
        api_token="old-token",
        auth_kind="oauth",
        pool=object(),
        secret_store=object(),
        tenant_id=uuid4(),
        install_row_id=uuid4(),
        http_client=http,
    )
    assert await client.get_current_user() == {"id": "user-1"}
    await http.aclose()

    assert refresh_calls == [False, True]
    assert seen_tokens == ["Bearer old-token", "Bearer refreshed-token"]


async def test_refresh_uses_current_figma_token_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIGMA_CLIENT_ID", "client-id")
    monkeypatch.setenv("FIGMA_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("FIGMA_REDIRECT_URI", "https://gateway.test/integrations/figma/oauth/callback")
    monkeypatch.setenv("FIGMA_OAUTH_REFRESH_URL", "https://api.figma.test/v1/oauth/token")
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"access_token": "fresh", "expires_in": 3600})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    response = await figma_oauth._exchange_oauth_refresh("refresh-value", http)
    await http.aclose()

    assert response["access_token"] == "fresh"
    assert requests[0].url.path == "/v1/oauth/token"
    form = parse_qs(requests[0].content.decode("utf-8"))
    assert form == {"grant_type": ["refresh_token"], "refresh_token": ["refresh-value"]}


async def test_refresh_persists_under_install_advisory_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIGMA_CLIENT_ID", "client-id")
    monkeypatch.setenv("FIGMA_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("FIGMA_REDIRECT_URI", "https://gateway.test/integrations/figma/oauth/callback")
    monkeypatch.setenv("FIGMA_OAUTH_REFRESH_URL", "https://api.figma.test/v1/oauth/token")
    tenant_id = uuid4()
    installation_id = uuid4()
    store = _SecretStore()
    store.values = {"old-access": "old", "refresh-ref": "refresh-value"}

    class _RefreshContext:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []
            self.row = {
                "secret_ref": "old-access",
                "refresh_secret_ref": "refresh-ref",
                "token_expires_at": datetime(2020, 1, 1, tzinfo=timezone.utc),
                "connection_state": "connected",
            }

        async def execute(self, query: str, *args: Any) -> str:
            self.calls.append((query, args))
            return "UPDATE 1"

        async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
            self.calls.append((query, args))
            return self.row

    context = _RefreshContext()

    @asynccontextmanager
    async def fake_tenant_transaction(*args: Any, **kwargs: Any):  # noqa: ARG001
        yield context

    monkeypatch.setattr(figma_oauth, "tenant_transaction", fake_tenant_transaction)

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/oauth/token"
        return httpx.Response(200, json={"access_token": "new-access", "expires_in": 3600})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await figma_oauth.refresh_installation_access_token(
            pool=object(),  # type: ignore[arg-type]
            secret_store=store,
            tenant_id=tenant_id,
            installation_id=installation_id,
            expected_access_ref="old-access",
            http_client=http,
        )

    assert result is not None
    token, access_ref, refresh_ref, _ = result
    assert token == "new-access"
    assert access_ref != "old-access"
    assert refresh_ref == "refresh-ref"
    assert "old-access" in store.deleted
    assert any("pg_advisory_xact_lock" in query for query, _ in context.calls)
    assert any("UPDATE figma_installations" in query for query, _ in context.calls)


async def test_invalid_refresh_grant_marks_reauthorization_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIGMA_CLIENT_ID", "client-id")
    monkeypatch.setenv("FIGMA_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("FIGMA_REDIRECT_URI", "https://gateway.test/integrations/figma/oauth/callback")
    monkeypatch.setenv("FIGMA_OAUTH_REFRESH_URL", "https://api.figma.test/v1/oauth/token")
    tenant_id = uuid4()
    installation_id = uuid4()
    store = _SecretStore()
    store.values = {"old-access": "old", "refresh-ref": "revoked"}

    class _Context:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        async def execute(self, query: str, *args: Any) -> str:
            self.calls.append((query, args))
            return "UPDATE 1"

        async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
            self.calls.append((query, args))
            return {
                "secret_ref": "old-access",
                "refresh_secret_ref": "refresh-ref",
                "token_expires_at": datetime(2020, 1, 1, tzinfo=timezone.utc),
                "connection_state": "connected",
            }

    context = _Context()

    @asynccontextmanager
    async def fake_tenant_transaction(*args: Any, **kwargs: Any):  # noqa: ARG001
        yield context

    monkeypatch.setattr(figma_oauth, "tenant_transaction", fake_tenant_transaction)

    async def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
        return httpx.Response(400, json={"error": "invalid_grant"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await figma_oauth.refresh_installation_access_token(
            pool=object(),  # type: ignore[arg-type]
            secret_store=store,
            tenant_id=tenant_id,
            installation_id=installation_id,
            expected_access_ref="old-access",
            http_client=http,
        )

    assert result is None
    state_update = next(
        args for query, args in context.calls
        if "SET connection_state = $1" in query
    )
    assert state_update[0] == "reauthorization_required"
