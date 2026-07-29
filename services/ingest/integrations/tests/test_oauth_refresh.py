"""Unit tests for the OAuth refresh integration layer (proactive skew, persist,
reactive force, degraded-on-failure). Complements the doc-fixture contract tests
in tests/contract/test_oauth_refresh_contract.py with the persistence + trigger
behavior the contract layer can't express.
"""
from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from services.ingest.integrations.oauth_refresh import (
    OAuthRefreshError,
    REFRESH_CONFIGS,
    ensure_fresh_access_token,
    needs_refresh,
    refresh_and_persist,
)
from services.ingest.integrations import oauth_refresh

pytestmark = pytest.mark.asyncio

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
TENANT = "11111111-1111-1111-1111-111111111111"
INSTALL = "22222222-2222-2222-2222-222222222222"


class FakeStore:
    def __init__(self, initial: dict | None = None) -> None:
        self._data: dict[str, str] = dict(initial or {})
        self._n = 0
        self.puts: list[tuple[str, str, str]] = []

    async def get(self, ref, *, tenant_id):
        return self._data[ref].encode("utf-8")

    async def put(self, plaintext, *, label, tenant_id):
        self._n += 1
        ref = f"new-ref-{self._n}"
        val = plaintext.decode("utf-8") if isinstance(plaintext, bytes) else plaintext
        self._data[ref] = val
        self.puts.append((ref, label, val))
        return ref


class FakePool:
    def __init__(self) -> None:
        self.executed: list[tuple] = []

    async def execute(self, sql, *args):
        self.executed.append((sql, args))


def _http(body: dict, *, status: int = 200, captured: dict | None = None) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        if captured is not None:
            captured["form"] = dict(httpx.QueryParams(request.content.decode("utf-8")))
            captured["headers"] = dict(request.headers)
        return httpx.Response(status, json=body)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# --- proactive skew logic -------------------------------------------------

async def test_needs_refresh_skew():
    assert needs_refresh(None, now=NOW) is True            # unknown expiry
    assert needs_refresh(NOW - timedelta(seconds=1), now=NOW) is True   # expired
    assert needs_refresh(NOW + timedelta(seconds=30), now=NOW) is True  # within 120s skew
    assert needs_refresh(NOW + timedelta(hours=1), now=NOW) is False    # plenty of time


# --- refresh_and_persist: exchange + persistence + rotation ---------------

async def test_refresh_and_persist_quickbooks_rotates_and_updates_row(monkeypatch):
    monkeypatch.setenv("QUICKBOOKS_CLIENT_ID", "cid")
    monkeypatch.setenv("QUICKBOOKS_CLIENT_SECRET", "csec")
    store = FakeStore({"old-refresh-ref": "OLD-refresh"})
    pool = FakePool()
    captured: dict = {}
    body = {"access_token": "NEW-access", "refresh_token": "ROTATED-refresh", "expires_in": 3600}

    async with _http(body, captured=captured) as http:
        refreshed = await refresh_and_persist(
            provider="quickbooks", pool=pool, secret_store=store, http=http,
            tenant_id=TENANT, install_row_id=INSTALL,
            refresh_secret_ref="old-refresh-ref", now=NOW,
        )

    # exchange sent the resolved refresh token + Basic client auth
    assert captured["form"]["grant_type"] == "refresh_token"
    assert captured["form"]["refresh_token"] == "OLD-refresh"
    assert captured["headers"]["authorization"].startswith("Basic ")
    # persisted: new access ref + rotated refresh ref + row UPDATE
    assert refreshed.access_token == "NEW-access"
    assert refreshed.refresh_token == "ROTATED-refresh"
    labels = [lbl for _ref, lbl, _val in store.puts]
    assert any("quickbooks_access_token" in lbl for lbl in labels)
    assert any("quickbooks_refresh_token" in lbl for lbl in labels)
    assert len(pool.executed) == 1
    sql, args = pool.executed[0]
    assert "UPDATE quickbooks_installations" in sql
    # args: new_access_ref, new_refresh_ref, expires_at, install, tenant
    assert args[2] == NOW + timedelta(seconds=3600)
    assert args[3] == INSTALL and args[4] == TENANT


async def test_carta_remints_via_client_credentials_from_install(monkeypatch):
    monkeypatch.setenv("CARTA_CLIENT_ID", "carta-cid")
    monkeypatch.delenv("CARTA_CLIENT_SECRET", raising=False)
    # refresh_secret_ref holds the per-install client_credentials SECRET.
    store = FakeStore({"carta-cc-ref": "carta-install-client-secret"})
    pool = FakePool()
    captured: dict = {}
    body = {"access_token": "carta-new-access", "expires_in": 3600}

    async with _http(body, captured=captured) as http:
        refreshed = await refresh_and_persist(
            provider="carta", pool=pool, secret_store=store, http=http,
            tenant_id=TENANT, install_row_id=INSTALL,
            refresh_secret_ref="carta-cc-ref", now=NOW,
        )

    assert captured["form"]["grant_type"] == "client_credentials"
    assert "refresh_token" not in captured["form"]
    # scope is REQUIRED for Carta's client_credentials grant
    assert captured["form"]["scope"]
    # client_secret came from the install, not env, and rides in the HTTP
    # **Basic** header (base64(client_id:client_secret) — docs.carta.com
    # client-credentials flow), never in the form body.
    expected_basic = base64.b64encode(
        b"carta-cid:carta-install-client-secret"
    ).decode("ascii")
    assert captured["headers"]["authorization"] == f"Basic {expected_basic}"
    assert "client_secret" not in captured["form"]
    # no refresh token returned/persisted; the refresh_secret_ref is unchanged
    assert refreshed.refresh_token is None
    sql, args = pool.executed[0]
    assert "UPDATE carta_installations" in sql
    assert args[1] == "carta-cc-ref"  # refresh_secret_ref preserved


async def test_ramp_remints_via_client_credentials_from_install(monkeypatch):
    monkeypatch.delenv("RAMP_CLIENT_ID", raising=False)
    monkeypatch.delenv("RAMP_CLIENT_SECRET", raising=False)
    store = FakeStore({
        "ramp-cc-ref": json.dumps({
            "client_id": "ramp-install-cid",
            "client_secret": "ramp-install-secret",
        }),
    })
    pool = FakePool()
    captured: dict = {}
    body = {"access_token": "ramp-new-access", "expires_in": 3600}

    async with _http(body, captured=captured) as http:
        refreshed = await refresh_and_persist(
            provider="ramp", pool=pool, secret_store=store, http=http,
            tenant_id=TENANT, install_row_id=INSTALL,
            refresh_secret_ref="ramp-cc-ref", now=NOW,
        )

    assert captured["form"]["grant_type"] == "client_credentials"
    assert captured["form"]["scope"]
    assert "refresh_token" not in captured["form"]
    expected_basic = base64.b64encode(
        b"ramp-install-cid:ramp-install-secret"
    ).decode("ascii")
    assert captured["headers"]["authorization"] == f"Basic {expected_basic}"
    assert "client_secret" not in captured["form"]
    assert refreshed.access_token == "ramp-new-access"
    assert refreshed.refresh_token is None
    sql, args = pool.executed[0]
    assert "UPDATE ramp_installations" in sql
    assert args[1] == "ramp-cc-ref"


async def test_refresh_and_persist_linkedin_body_auth_and_updates_row(monkeypatch):
    monkeypatch.setenv("LINKEDIN_CLIENT_ID", "li-cid")
    monkeypatch.setenv("LINKEDIN_CLIENT_SECRET", "li-secret")
    store = FakeStore({"linkedin-refresh-ref": "OLD-linkedin-refresh"})
    pool = FakePool()
    captured: dict = {}
    body = {
        "access_token": "linkedin-new-access",
        "refresh_token": "linkedin-returned-refresh",
        "expires_in": 86400,
    }

    async with _http(body, captured=captured) as http:
        refreshed = await refresh_and_persist(
            provider="linkedin", pool=pool, secret_store=store, http=http,
            tenant_id=TENANT, install_row_id=INSTALL,
            refresh_secret_ref="linkedin-refresh-ref", now=NOW,
        )

    assert captured["form"]["grant_type"] == "refresh_token"
    assert captured["form"]["refresh_token"] == "OLD-linkedin-refresh"
    assert captured["form"]["client_id"] == "li-cid"
    assert captured["form"]["client_secret"] == "li-secret"
    assert "authorization" not in captured["headers"]
    assert refreshed.access_token == "linkedin-new-access"
    assert refreshed.refresh_token == "linkedin-returned-refresh"
    labels = [lbl for _ref, lbl, _val in store.puts]
    assert any("linkedin_access_token" in lbl for lbl in labels)
    assert any("linkedin_refresh_token" in lbl for lbl in labels)
    sql, args = pool.executed[0]
    assert "UPDATE linkedin_installations" in sql
    assert args[2] == NOW + timedelta(seconds=86400)
    assert args[3] == INSTALL and args[4] == TENANT


async def test_refresh_configs_cover_all_refresh_backed_install_tables():
    """Every dedicated install table that stores OAuth refresh-token material
    must be reachable by the shared refresh core."""
    refresh_token_tables = {
        "quickbooks_installations",
        "gusto_installations",
        "linkedin_installations",
    }
    configured = {
        cfg.install_table
        for cfg in REFRESH_CONFIGS.values()
        if cfg.grant_type == "refresh_token"
    }
    assert refresh_token_tables <= configured


# --- ensure_fresh_access_token: proactive vs reactive ---------------------

async def test_ensure_fresh_skips_when_token_valid(monkeypatch):
    monkeypatch.setenv("RAMP_CLIENT_ID", "r")
    monkeypatch.setenv("RAMP_CLIENT_SECRET", "s")
    store = FakeStore({"access-ref": "still-valid-token"})
    pool = FakePool()
    # No HTTP transport needed — a valid token must NOT call the endpoint.
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda r: (_ for _ in ()).throw(AssertionError("must not refresh")))) as http:
        token = await ensure_fresh_access_token(
            provider="ramp", pool=pool, secret_store=store, http=http,
            tenant_id=TENANT, install_row_id=INSTALL,
            current_access_ref="access-ref", refresh_secret_ref="rr",
            token_expires_at=NOW + timedelta(hours=1), now=NOW,
        )
    assert token == "still-valid-token"
    assert pool.executed == []


async def test_ensure_fresh_force_refreshes_even_if_valid(monkeypatch):
    monkeypatch.setenv("RAMP_CLIENT_ID", "r")
    monkeypatch.setenv("RAMP_CLIENT_SECRET", "s")
    store = FakeStore({"access-ref": "old", "rr": "ramp-refresh"})
    pool = FakePool()
    calls: list[dict[str, object]] = []

    async def durable_refresh(**kwargs: object) -> str:
        calls.append(kwargs)
        return "fresh-after-401"

    monkeypatch.setattr(
        oauth_refresh,
        "_refresh_through_renewal_job",
        durable_refresh,
    )
    async with httpx.AsyncClient() as http:
        token = await ensure_fresh_access_token(
            provider="ramp", pool=pool, secret_store=store, http=http,
            tenant_id=TENANT, install_row_id=INSTALL,
            current_access_ref="access-ref", refresh_secret_ref="rr",
            token_expires_at=NOW + timedelta(hours=1), force=True, now=NOW,
        )
    assert token == "fresh-after-401"           # reactive 401 path refreshed
    assert pool.executed == []
    assert calls == [
        {
            "provider": "ramp",
            "pool": pool,
            "secret_store": store,
            "http": http,
            "tenant_id": TENANT,
            "install_row_id": INSTALL,
            "now": NOW,
            "force": True,
            "request_binding": None,
        }
    ]


# --- degraded-on-failure (never crash, never silently drop) ---------------

async def test_failed_refresh_raises_degraded_signal(monkeypatch):
    monkeypatch.setenv("GUSTO_CLIENT_ID", "g")
    monkeypatch.setenv("GUSTO_CLIENT_SECRET", "x")
    store = FakeStore({"rr": "revoked-refresh"})
    pool = FakePool()
    async with _http({"error": "invalid_grant"}, status=400) as http:
        with pytest.raises(OAuthRefreshError) as exc:
            await refresh_and_persist(
                provider="gusto", pool=pool, secret_store=store, http=http,
                tenant_id=TENANT, install_row_id=INSTALL,
                refresh_secret_ref="rr", now=NOW,
            )
    assert exc.value.status == 400
    # nothing persisted on failure — the row keeps its prior (now-stale) token
    assert pool.executed == []
