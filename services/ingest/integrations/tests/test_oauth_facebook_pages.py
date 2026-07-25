"""Facebook Pages OAuth install and callback tests."""
from __future__ import annotations

import base64
import json
from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI

from services.ingest.integrations.facebook_pages import oauth as facebook_oauth
from services.ingest.integrations.router import build_integrations_router


class _Auth:
    def __init__(self, tenant_id: UUID) -> None:
        self.tenant_id = tenant_id
        self.actor_id = uuid4()


class _AsyncContext:
    def __init__(self, value=None) -> None:
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _Pool:
    def __init__(self) -> None:
        self.state_inserts: list[tuple[object, ...]] = []
        self.conn = _Conn()

    async def execute(self, sql: str, *args):
        assert "oauth_install_states" in sql
        self.state_inserts.append(args)
        return "INSERT 0 1"

    def acquire(self):
        return _AsyncContext(self.conn)


class _Conn:
    def __init__(self) -> None:
        self.provider_install_args: tuple[object, ...] | None = None
        self.page_install_args: tuple[object, ...] | None = None
        self.trigger_args: tuple[object, ...] | None = None
        self.page_install_id = uuid4()

    def transaction(self):
        return _AsyncContext()

    async def fetchrow(self, sql: str, *args):
        if "provider_installations" in sql:
            self.provider_install_args = args
            return {"id": uuid4(), "was_inserted": True}
        if "facebook_page_installations" in sql:
            self.page_install_args = args
            return {"id": self.page_install_id, "was_inserted": True}
        raise AssertionError(f"unexpected fetchrow SQL: {sql}")

    async def execute(self, sql: str, *args):
        assert "onboarding_triggers" in sql
        self.trigger_args = args
        return "INSERT 0 1"


class _SecretStore:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    async def put(self, value: str, *, label: str, tenant_id: UUID) -> str:
        ref = f"secret://{label}"
        self.records.append(
            {
                "value": value,
                "label": label,
                "tenant_id": tenant_id,
                "ref": ref,
            }
        )
        return ref


class _FacebookClient:
    def __init__(self) -> None:
        self.closed = False
        self.subscriptions: list[tuple[str, str, tuple[str, ...]]] = []

    async def exchange_code(
        self,
        *,
        code: str,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
    ) -> dict[str, object]:
        assert code == "valid-code"
        assert client_id == "fb-client"
        assert client_secret == "fb-secret"
        assert (
            redirect_uri
            == "https://app.fyralis.test/integrations/facebook_pages/callback"
        )
        return {
            "access_token": "user-token",
            "scope": (
                "pages_show_list,pages_messaging "
                "pages_manage_metadata pages_read_engagement"
            ),
        }

    async def list_pages(self, user_access_token: str) -> list[dict[str, object]]:
        assert user_access_token == "user-token"
        return [
            {
                "id": "PAGE1",
                "name": "Wrong Page",
                "access_token": "page-token-1",
            },
            {
                "id": "PAGE2",
                "name": "Selected Page",
                "access_token": "page-token-2",
            },
        ]

    async def subscribe_page(
        self,
        *,
        page_id: str,
        page_access_token: str,
        fields: tuple[str, ...],
    ) -> dict[str, object]:
        self.subscriptions.append((page_id, page_access_token, fields))
        return {"success": True}

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _facebook_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FACEBOOK_APP_ID", "fb-client")
    monkeypatch.setenv("FACEBOOK_APP_SECRET", "fb-secret")
    monkeypatch.setenv(
        "FACEBOOK_REDIRECT_URI",
        "https://app.fyralis.test/integrations/facebook_pages/callback",
    )
    monkeypatch.setenv("FACEBOOK_WEBHOOK_VERIFY_TOKEN", "verify-token")
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "facebook-pages-oauth-test-key")


def _make_app(
    pool: _Pool,
    *,
    auth: _Auth | None = None,
    secret_store: _SecretStore | None = None,
) -> FastAPI:
    app = FastAPI()
    app.include_router(build_integrations_router())
    app.state.pool = pool
    if secret_store is not None:
        app.state.secret_store = secret_store

    @app.middleware("http")
    async def _inject_auth(request, call_next):
        if auth is not None:
            request.state.auth = auth
        return await call_next(request)

    return app


def _decode_state_payload(state: str) -> dict[str, object]:
    payload_b64, signature_b64 = state.split(".", 1)
    assert payload_b64
    assert signature_b64
    padded = payload_b64 + "=" * (-len(payload_b64) % 4)
    return json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))


async def test_install_redirect_requests_page_message_scopes_and_state() -> None:
    tenant_id = uuid4()
    pool = _Pool()
    app = _make_app(pool, auth=_Auth(tenant_id))

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        response = await client.get(
            "/integrations/facebook_pages/install",
            params={"page_id": "PAGE2"},
            follow_redirects=False,
        )

    assert response.status_code == 302
    parsed = urlparse(response.headers["location"])
    assert parsed.scheme == "https"
    assert parsed.netloc == "www.facebook.com"
    assert parsed.path == "/dialog/oauth"
    params = parse_qs(parsed.query)
    assert params["client_id"] == ["fb-client"]
    assert params["redirect_uri"] == [
        "https://app.fyralis.test/integrations/facebook_pages/callback"
    ]
    scopes = set(params["scope"][0].split(","))
    assert scopes == {
        "pages_show_list",
        "pages_messaging",
        "pages_manage_metadata",
        "pages_read_engagement",
    }

    assert len(pool.state_inserts) == 1
    state = params["state"][0]
    payload = _decode_state_payload(state)
    assert payload["tenant_id"] == str(tenant_id)
    assert payload["page_id"] == "PAGE2"


async def test_install_redirect_allows_scope_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "FACEBOOK_OAUTH_SCOPES",
        "pages_show_list,pages_messaging pages_manage_metadata",
    )
    tenant_id = uuid4()
    pool = _Pool()
    app = _make_app(pool, auth=_Auth(tenant_id))

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        response = await client.get(
            "/integrations/facebook_pages/install",
            follow_redirects=False,
        )

    assert response.status_code == 302
    params = parse_qs(urlparse(response.headers["location"]).query)
    assert set(params["scope"][0].split(",")) == {
        "pages_show_list",
        "pages_messaging",
        "pages_manage_metadata",
    }


async def test_callback_selects_page_stores_tokens_subscribes_and_triggers_backfill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    pool = _Pool()
    secret_store = _SecretStore()
    fake_client = _FacebookClient()

    async def _verify_state(state: str, state_pool: _Pool):
        assert state == "signed-state"
        assert state_pool is pool
        return tenant_id, {"page_id": "PAGE2"}

    monkeypatch.setattr(facebook_oauth, "verify_and_consume_state", _verify_state)
    monkeypatch.setattr(
        facebook_oauth,
        "FacebookPagesClient",
        lambda **_kwargs: fake_client,
    )
    app = _make_app(pool, secret_store=secret_store)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        response = await client.get(
            "/integrations/facebook_pages/callback",
            params={"code": "valid-code", "state": "signed-state"},
            follow_redirects=False,
        )

    assert response.status_code == 302
    assert response.headers["location"].startswith(
        "/integrations/facebook_pages/installed?page="
    )
    assert "PAGE2" not in response.headers["location"]
    assert fake_client.closed is True
    assert fake_client.subscriptions == [
        ("PAGE2", "page-token-2", ("messages", "message_echoes")),
    ]
    assert [record["label"] for record in secret_store.records] == [
        "facebook_pages_page_token:PAGE2",
        "facebook_pages_app_secret:PAGE2",
        "facebook_pages_verify_token:PAGE2",
    ]

    provider_args = pool.conn.provider_install_args
    assert provider_args is not None
    assert provider_args[1] == tenant_id
    assert provider_args[2] == "facebook_pages"
    assert provider_args[3] == "PAGE2"
    assert provider_args[4] == "secret://facebook_pages_app_secret:PAGE2"

    page_args = pool.conn.page_install_args
    assert page_args is not None
    assert page_args[0] == tenant_id
    assert page_args[1] == "PAGE2"
    assert page_args[2] == "Selected Page"
    assert page_args[3] == "secret://facebook_pages_page_token:PAGE2"
    assert page_args[4] == "secret://facebook_pages_app_secret:PAGE2"
    assert page_args[5] == "secret://facebook_pages_verify_token:PAGE2"
    assert set(page_args[6]) == {
        "pages_show_list",
        "pages_messaging",
        "pages_manage_metadata",
        "pages_read_engagement",
    }
    assert page_args[7] is True
    assert set(page_args[8]) == {"messages", "message_echoes"}

    trigger_args = pool.conn.trigger_args
    assert trigger_args is not None
    assert trigger_args[1] == tenant_id
    assert trigger_args[2] == "facebook_pages"
    assert trigger_args[3] == "install"
    assert trigger_args[4] == pool.conn.page_install_id
    trigger_payload = json.loads(trigger_args[5])
    assert trigger_payload["coverage"] == "All available history"
    assert trigger_payload["page_id"] == "PAGE2"
