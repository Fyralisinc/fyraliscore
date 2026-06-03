"""Tests for the Google Calendar live delta poller (near-real-time ingestion).

Exercises the leasing + drain + persistence loop without real Google: the
fetcher is stubbed to return canned pages and `core.ingest` is stubbed to a
recorder, so the test asserts the poller's contract (lease only active +
cursor-seeded + due calendars, advance the sync_token, reset/raise failures)
against a real Postgres.
"""
from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from services.ingest.ingestion.fetchers import FetchResult
from services.ingest.integrations.google_calendar import live_poller


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
async def _clean_gcal_rows(fresh_db: asyncpg.Pool):
    yield
    await fresh_db.execute("DELETE FROM onboarding_triggers WHERE source = 'google_calendar'")
    await fresh_db.execute("DELETE FROM google_calendar_calendars")
    await fresh_db.execute("DELETE FROM google_calendar_installations")


async def _seed_calendar(
    pool: asyncpg.Pool, *, sync_token: str | None = "sync-0",
    state: str = "active", last_live_poll_at_sql: str = "NULL",
) -> tuple[UUID, UUID, UUID]:
    """Seed tenant + install + one calendar. Returns (tenant, install, calendar)."""
    from lib.shared.ids import uuid7

    tenant = uuid4()
    await pool.execute("INSERT INTO tenants (id, name) VALUES ($1, 'gcal-live')", tenant)
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
    await pool.execute(
        f"""
        INSERT INTO google_calendar_calendars
            (id, tenant_id, google_calendar_installation_id, calendar_id,
             owner_email, sync_token, state, last_live_poll_at)
        VALUES ($1, $2, $3, 'alice@acme.com', 'alice@acme.com', $4, $5,
                {last_live_poll_at_sql})
        """,
        cal, tenant, install, sync_token, state,
    )
    return tenant, install, cal


def _stub_ingest(monkeypatch, *, recorder: list) -> None:
    async def _ingest(channel, record, *, pool, tenant_id, **kw):
        recorder.append((channel, record.get("id"), tenant_id))
        return SimpleNamespace(
            deduped=False,
            observation=SimpleNamespace(external_id=str(record.get("id"))),
        )

    import services.ingest.ingestion.core as core
    monkeypatch.setattr(core, "ingest", _ingest)


def _stub_fetcher(monkeypatch, pages: list[FetchResult], *, captured: dict) -> None:
    calls = {"i": 0}

    async def _fetch(install, shard_identifier, cursor):
        captured["shard_identifier"] = shard_identifier
        captured["scope"] = install.get("scope")
        i = calls["i"]
        calls["i"] += 1
        return pages[min(i, len(pages) - 1)]

    monkeypatch.setattr(live_poller, "_fetcher", lambda: _fetch)


async def test_poll_drains_and_advances_sync_token(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant, _install, cal = await _seed_calendar(fresh_db, sync_token="sync-0")

    recorder: list = []
    _stub_ingest(monkeypatch, recorder=recorder)
    captured: dict = {}
    _stub_fetcher(
        monkeypatch,
        [FetchResult(
            records=[{"id": "ev-1"}, {"id": "ev-2"}],
            next_cursor={"next_sync_token": "sync-1"},
            end_of_data=True,
        )],
        captured=captured,
    )

    n = await live_poller.tick(fresh_db)
    assert n == 1

    # Incremental warm-start: the shard carried the stored sync_token + scope.
    assert captured["shard_identifier"]["sync_token"] == "sync-0"
    assert captured["shard_identifier"]["calendar_id"] == "alice@acme.com"
    assert captured["scope"] == "calendar.readonly"
    # Both events ingested on the live channel.
    assert [c[0] for c in recorder] == ["google_calendar:event", "google_calendar:event"]

    row = await fresh_db.fetchrow(
        "SELECT sync_token, last_synced_at, last_live_poll_at, "
        "consecutive_live_failures FROM google_calendar_calendars WHERE id = $1",
        cal,
    )
    assert row["sync_token"] == "sync-1"          # advanced
    assert row["last_synced_at"] is not None
    assert row["last_live_poll_at"] is not None    # leased
    assert row["consecutive_live_failures"] == 0


async def test_lease_skips_unseeded_paused_and_recent(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No sync_token yet (never backfilled) → not eligible for live poll.
    await _seed_calendar(fresh_db, sync_token=None)
    # Paused → not eligible.
    await _seed_calendar(fresh_db, sync_token="s", state="paused")
    # Recently polled (within the gap) → not eligible.
    await _seed_calendar(fresh_db, sync_token="s", last_live_poll_at_sql="now()")

    async with fresh_db.acquire() as conn:
        leased = await live_poller._lease_due_calendars(conn, limit=50)
    assert leased == []


async def test_failure_bumps_counter(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant, _install, cal = await _seed_calendar(fresh_db, sync_token="sync-0")

    from services.ingest.integrations.gmail.client import GoogleApiError

    async def _boom(install, shard_identifier, cursor):
        raise GoogleApiError("backend error")

    monkeypatch.setattr(live_poller, "_fetcher", lambda: _boom)

    await live_poller.tick(fresh_db)

    row = await fresh_db.fetchrow(
        "SELECT consecutive_live_failures, live_last_error, state "
        "FROM google_calendar_calendars WHERE id = $1", cal,
    )
    assert row["consecutive_live_failures"] == 1
    assert row["live_last_error"] is not None
    assert row["state"] == "active"  # one failure, not yet errored
