"""Tests for the Google Calendar/Drive native push-channel engine.

Covers the security-critical bits without real Google: channel-token
verification (`resolve_push`), channel registration persistence
(`register_watch` via a fake client), and the webhook ingress (sync handshake,
unknown/unverified drop, verified push → drain + cursor advance).
"""
from __future__ import annotations

import dataclasses
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from fastapi import FastAPI

from lib.shared.ids import uuid7
from services.ingest.integrations import _google_watch
from services.ingest.integrations._google_watch import register_watch, resolve_push
from services.ingest.integrations.google_calendar.watch import SPEC as CAL_SPEC
from services.ingest.ingestion.fetchers import FetchResult


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
async def _clean(fresh_db: asyncpg.Pool):
    yield
    await fresh_db.execute("DELETE FROM onboarding_triggers WHERE source = 'google_calendar'")
    await fresh_db.execute("DELETE FROM google_calendar_calendars")
    await fresh_db.execute("DELETE FROM google_calendar_installations")


async def _seed_calendar(
    pool: asyncpg.Pool, *, channel_id: str | None = None, token: str | None = None,
    sync_token: str = "sync-0",
) -> tuple[UUID, UUID]:
    tenant = uuid4()
    await pool.execute("INSERT INTO tenants (id, name) VALUES ($1, 'gcal-push')", tenant)
    install = uuid7()
    await pool.execute(
        """
        INSERT INTO google_calendar_installations
            (id, tenant_id, workspace_domain, service_account_email, scope,
             inclusion_spec, resolved_calendar_count, resolved_at)
        VALUES ($1, $2, 'acme.com', 'svc@acme.iam', 'calendar.readonly',
                '{}'::jsonb, 1, now())
        """,
        install, tenant,
    )
    cal = uuid7()
    watch_state = "active" if channel_id else "inactive"
    await pool.execute(
        """
        INSERT INTO google_calendar_calendars
            (id, tenant_id, google_calendar_installation_id, calendar_id,
             owner_email, sync_token, state, watch_channel_id, watch_token,
             watch_state)
        VALUES ($1, $2, $3, 'alice@acme.com', 'alice@acme.com', $4, 'active',
                $5, $6, $7)
        """,
        cal, tenant, install, sync_token, channel_id, token, watch_state,
    )
    return tenant, cal


# ---------------------------------------------------------------------
# resolve_push — token verification
# ---------------------------------------------------------------------
async def test_resolve_push_unknown_channel(fresh_db: asyncpg.Pool) -> None:
    await _seed_calendar(fresh_db, channel_id="chan-A", token="tok-A")
    assert await resolve_push(fresh_db, CAL_SPEC, channel_id="nope", token="x") is None


async def test_resolve_push_bad_token(fresh_db: asyncpg.Pool) -> None:
    await _seed_calendar(fresh_db, channel_id="chan-A", token="tok-A")
    assert await resolve_push(fresh_db, CAL_SPEC, channel_id="chan-A", token="WRONG") is None
    assert await resolve_push(fresh_db, CAL_SPEC, channel_id="chan-A", token=None) is None


async def test_resolve_push_good_token(fresh_db: asyncpg.Pool) -> None:
    tenant, cal = await _seed_calendar(fresh_db, channel_id="chan-A", token="tok-A")
    row = await resolve_push(fresh_db, CAL_SPEC, channel_id="chan-A", token="tok-A")
    assert row is not None
    assert row["id"] == cal
    assert row["tenant_id"] == tenant
    assert row["calendar_id"] == "alice@acme.com"
    assert row["cursor_token"] == "sync-0"
    assert row["scope"] == "calendar.readonly"
    assert row["installation_id"] == await fresh_db.fetchval(
        "SELECT google_calendar_installation_id "
        "FROM google_calendar_calendars WHERE id = $1",
        cal,
    )


# ---------------------------------------------------------------------
# register_watch — channel persistence (fake client via dataclasses.replace)
# ---------------------------------------------------------------------
async def test_register_watch_persists_channel_state(fresh_db: asyncpg.Pool) -> None:
    tenant, cal = await _seed_calendar(fresh_db, sync_token="sync-0")

    watched: dict = {}

    class _FakeClient:
        async def watch_events(self, **kw):
            watched.update(kw)
            return {"resourceId": "res-123", "expiration": "4102444800000"}  # 2100-01-01

        async def stop_channel(self, **kw):
            return None

    bound: dict = {}

    async def _make_client(scope, *, tenant_id, installation_id):
        bound.update({
            "scope": scope,
            "tenant_id": tenant_id,
            "installation_id": installation_id,
        })

        async def _close():
            return None
        return _FakeClient(), _close

    test_spec = dataclasses.replace(CAL_SPEC, make_client=_make_client)

    # Lease the resource (drives the real leasing SQL) then register.
    async with fresh_db.acquire() as conn:
        leased = await _google_watch._lease_due_watches(conn, test_spec, limit=10)
    assert len(leased) == 1
    await register_watch(fresh_db, test_spec, leased[0], address="https://app.test/webhooks/google_calendar/push")

    # The watch call carried the address + a minted channel id + token.
    assert watched["calendar_id"] == "alice@acme.com"
    assert watched["address"] == "https://app.test/webhooks/google_calendar/push"
    assert watched["channel_id"] and watched["token"]
    install_id = await fresh_db.fetchval(
        "SELECT google_calendar_installation_id "
        "FROM google_calendar_calendars WHERE id = $1",
        cal,
    )
    assert bound == {
        "scope": "calendar.readonly",
        "tenant_id": tenant,
        "installation_id": install_id,
    }

    row = await fresh_db.fetchrow(
        "SELECT watch_channel_id, watch_resource_id, watch_token, "
        "watch_expiration, watch_state FROM google_calendar_calendars WHERE id = $1",
        cal,
    )
    assert row["watch_channel_id"] == watched["channel_id"]
    assert row["watch_resource_id"] == "res-123"
    assert row["watch_token"] == watched["token"]
    assert row["watch_expiration"] is not None
    assert row["watch_state"] == "active"


# ---------------------------------------------------------------------
# Webhook ingress
# ---------------------------------------------------------------------
def _push_app(pool: asyncpg.Pool) -> FastAPI:
    from services.app.webhooks.google_push import router

    app = FastAPI()
    app.state.pool = pool
    app.include_router(router)
    return app


async def test_ingress_sync_handshake_acks(fresh_db: asyncpg.Pool) -> None:
    app = _push_app(fresh_db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/webhooks/google_calendar/push",
            headers={"X-Goog-Resource-State": "sync", "X-Goog-Channel-ID": "chan-A"},
        )
    assert r.status_code == 200
    assert r.json()["status"] == "sync_ack"


async def test_ingress_unknown_channel_skipped(fresh_db: asyncpg.Pool) -> None:
    app = _push_app(fresh_db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/webhooks/google_calendar/push",
            headers={"X-Goog-Resource-State": "exists",
                     "X-Goog-Channel-ID": "ghost", "X-Goog-Channel-Token": "x"},
        )
    assert r.status_code == 200
    assert r.json()["reason"] == "unknown_or_unverified"


async def test_ingress_verified_push_drains_and_advances(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant, cal = await _seed_calendar(
        fresh_db, channel_id="chan-A", token="tok-A", sync_token="sync-0",
    )

    # Stub the real fetcher + ingest the drain reaches.
    async def _fake_fetch(install, shard_identifier, cursor):
        assert shard_identifier["sync_token"] == "sync-0"
        return FetchResult(
            records=[{"id": "ev-1"}, {"id": "ev-2"}],
            next_cursor={"next_sync_token": "sync-9"},
            end_of_data=True,
        )

    import services.ingest.ingestion.fetchers.google_calendar as gcal_fetcher
    monkeypatch.setattr(gcal_fetcher, "fetch_page_google_calendar", _fake_fetch)

    recorder: list = []

    async def _fake_ingest(channel, record, *, pool, tenant_id, **kw):
        recorder.append(record["id"])
        return SimpleNamespace(deduped=False, observation=SimpleNamespace(external_id=record["id"]))

    import services.ingest.ingestion.core as core
    monkeypatch.setattr(core, "ingest", _fake_ingest)

    app = _push_app(fresh_db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/webhooks/google_calendar/push",
            headers={"X-Goog-Resource-State": "exists",
                     "X-Goog-Channel-ID": "chan-A", "X-Goog-Channel-Token": "tok-A"},
        )
    assert r.status_code == 200, r.text
    assert r.json() == {"status": "ok", "ingested": 2}
    assert recorder == ["ev-1", "ev-2"]

    row = await fresh_db.fetchrow(
        "SELECT sync_token, last_push_at, last_synced_at "
        "FROM google_calendar_calendars WHERE id = $1", cal,
    )
    assert row["sync_token"] == "sync-9"        # cursor advanced
    assert row["last_push_at"] is not None       # push stamped
    assert row["last_synced_at"] is not None
