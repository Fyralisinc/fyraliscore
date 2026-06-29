"""Tests for the Google Drive live delta poller (near-real-time ingestion).

Parallel to test_live_poller_google_calendar: leasing + drain + persistence
exercised with a stubbed fetcher + stubbed `core.ingest` against real Postgres.
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from services.ingest.ingestion.fetchers import FetchResult
from services.ingest.integrations.google_drive import live_poller


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
async def _clean_gdrive_rows(fresh_db: asyncpg.Pool):
    yield
    await fresh_db.execute("DELETE FROM onboarding_triggers WHERE source = 'google_drive'")
    await fresh_db.execute("DELETE FROM google_drive_targets")
    await fresh_db.execute("DELETE FROM google_drive_installations")


async def _seed_target(
    pool: asyncpg.Pool, *, start_page_token: str | None = "tok-0",
    state: str = "active", last_live_poll_at_sql: str = "NULL",
) -> tuple[UUID, UUID, UUID]:
    from lib.shared.ids import uuid7

    tenant = uuid4()
    await pool.execute("INSERT INTO tenants (id, name) VALUES ($1, 'gdrive-live')", tenant)
    install = uuid7()
    await pool.execute(
        """
        INSERT INTO google_drive_installations
            (id, tenant_id, workspace_domain, service_account_email, scope,
             inclusion_spec, include_shared_drives, resolved_target_count, resolved_at)
        VALUES ($1, $2, 'acme.com', 'svc@acme.iam', 'drive.readonly',
                '{}'::jsonb, TRUE, 1, now())
        """,
        install, tenant,
    )
    tgt = uuid7()
    await pool.execute(
        f"""
        INSERT INTO google_drive_targets
            (id, tenant_id, google_drive_installation_id, drive_kind, drive_id,
             owner_email, start_page_token, state, last_live_poll_at)
        VALUES ($1, $2, $3, 'my_drive', 'my-drive', 'alice@acme.com', $4, $5,
                {last_live_poll_at_sql})
        """,
        tgt, tenant, install, start_page_token, state,
    )
    return tenant, install, tgt


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
        i = calls["i"]
        calls["i"] += 1
        return pages[min(i, len(pages) - 1)]

    monkeypatch.setattr(live_poller, "_fetcher", lambda: _fetch)


async def test_poll_drains_and_advances_start_page_token(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant, _install, tgt = await _seed_target(fresh_db, start_page_token="tok-0")

    recorder: list = []
    _stub_ingest(monkeypatch, recorder=recorder)
    captured: dict = {}
    _stub_fetcher(
        monkeypatch,
        [FetchResult(
            records=[{"id": "file-1"}],
            next_cursor={"next_start_page_token": "tok-1"},
            end_of_data=True,
        )],
        captured=captured,
    )

    n = await live_poller.tick(fresh_db)
    assert n == 1
    assert captured["shard_identifier"]["start_page_token"] == "tok-0"
    assert captured["shard_identifier"]["drive_kind"] == "my_drive"
    assert [c[0] for c in recorder] == ["google_drive:file"]

    row = await fresh_db.fetchrow(
        "SELECT start_page_token, last_synced_at, last_live_poll_at, "
        "consecutive_live_failures FROM google_drive_targets WHERE id = $1", tgt,
    )
    assert row["start_page_token"] == "tok-1"
    assert row["last_synced_at"] is not None
    assert row["last_live_poll_at"] is not None
    assert row["consecutive_live_failures"] == 0


async def test_lease_skips_unseeded_paused_and_recent(
    fresh_db: asyncpg.Pool,
) -> None:
    await _seed_target(fresh_db, start_page_token=None)
    await _seed_target(fresh_db, start_page_token="t", state="paused")
    await _seed_target(fresh_db, start_page_token="t", last_live_poll_at_sql="now()")

    async with fresh_db.acquire() as conn:
        leased = await live_poller._lease_due_targets(conn, limit=50)
    assert leased == []


async def test_concurrent_replicas_lease_distinct_targets(
    fresh_db: asyncpg.Pool,
) -> None:
    _tenant_a, _install_a, target_a = await _seed_target(
        fresh_db,
        start_page_token="tok-a",
    )
    _tenant_b, _install_b, target_b = await _seed_target(
        fresh_db,
        start_page_token="tok-b",
    )
    first_ready = asyncio.Event()
    release = asyncio.Event()

    async def _lease_one() -> UUID:
        async with fresh_db.acquire() as conn:
            async with conn.transaction():
                rows = await live_poller._lease_due_targets(conn, limit=1)
                assert len(rows) == 1
                first_ready.set()
                await release.wait()
                return rows[0]["id"]

    first = asyncio.create_task(_lease_one())
    try:
        await asyncio.wait_for(first_ready.wait(), timeout=5.0)
        async with fresh_db.acquire() as conn:
            async with conn.transaction():
                rows = await live_poller._lease_due_targets(conn, limit=1)
                assert len(rows) == 1
                second_id = rows[0]["id"]
        release.set()
        first_id = await asyncio.wait_for(first, timeout=5.0)
    finally:
        release.set()

    assert {first_id, second_id} == {target_a, target_b}


async def test_failure_bumps_counter(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant, _install, tgt = await _seed_target(fresh_db, start_page_token="tok-0")

    from services.ingest.integrations.gmail.client import GoogleRateLimited

    async def _boom(install, shard_identifier, cursor):
        raise GoogleRateLimited("429 throttled")

    monkeypatch.setattr(live_poller, "_fetcher", lambda: _boom)

    await live_poller.tick(fresh_db)

    row = await fresh_db.fetchrow(
        "SELECT consecutive_live_failures, live_last_error, state "
        "FROM google_drive_targets WHERE id = $1", tgt,
    )
    assert row["consecutive_live_failures"] == 1
    assert row["live_last_error"] is not None
    assert row["state"] == "active"
