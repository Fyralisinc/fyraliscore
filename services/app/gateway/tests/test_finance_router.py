"""Integration tests for services/app/gateway/finance_router.py.

Drives the finance testing control plane the way the UI does: install ->
backfill -> status, and a live/emit inline-fallback. Uses the real gateway app
(httpx ASGITransport) against the test DB; requires DATABASE_URL.
"""
from __future__ import annotations

import os
from uuid import uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio

from services.app.gateway.finance_router import build_finance_router


pytestmark = pytest.mark.asyncio


_DSN = os.environ.get("DATABASE_URL")
requires_db = pytest.mark.skipif(not _DSN, reason="DATABASE_URL not set")


@pytest_asyncio.fixture
async def app_client():
    """A minimal FastAPI app with just the finance router + the gateway deps
    the router needs (pool, actor_repo, alias_repo, embedder=None)."""
    from fastapi import FastAPI

    from services.domain.actors.repo import ActorRepo
    from services.domain.entity_aliases.repo import EntityAliasRepo
    from services.app.gateway.db_bootstrap import _register_codecs
    from services.domain.observations.partitions import ensure_partitions

    pool = await asyncpg.create_pool(dsn=_DSN, min_size=1, max_size=4, init=_register_codecs)
    # Apply migrations only if the finance tables are missing. Re-running the
    # full migration dir against an already-migrated DB trips the source-CHECK
    # re-run landmine: a prior test's onboarding_triggers row with
    # source='mercury' violates 0059's narrower CHECK when it DROP+re-ADDs.
    async with pool.acquire() as conn:
        exists = await conn.fetchval("SELECT to_regclass('public.quickbooks_installations')")
        if exists is None:
            from lib.shared.migrations import apply_migrations_dir
            import pathlib
            root = pathlib.Path(__file__).resolve().parents[4]
            await apply_migrations_dir(conn, root / "db" / "migrations")
    await ensure_partitions(pool, months_ahead=3)

    class _Deps:
        def __init__(self, pool):
            self.pool = pool
            self.actor_repo = ActorRepo(pool)
            self.alias_repo = EntityAliasRepo(pool)
            self.embedder = None

    app = FastAPI()
    app.state.deps = _Deps(pool)
    app.include_router(build_finance_router())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, pool
    await pool.close()


@requires_db
@pytest.mark.parametrize("source", ["mercury", "quickbooks"])
async def test_install_backfill_status_flow(app_client, source):
    client, pool = app_client
    tenant_id = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        tenant_id, f"fin-test-{source}",
    )
    headers = {"X-Tenant-Id": str(tenant_id)}
    # Per-run-unique seed: the `company_os` role is superuser and bypasses RLS,
    # and the observations unique key is (source_channel, external_id) IGNORING
    # tenant — so a fixed seed would dedup against a PRIOR test run's rows. A
    # tenant-derived seed keeps this run's synthetic external_ids unique.
    seed = int(tenant_id.int % 1_000_000)

    # 1. Install.
    r = await client.post(f"/finance/{source}/install", headers=headers)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["source"] == source
    assert body["sub_resources"] >= 1

    # 2. Status shows installed, no observations yet.
    r = await client.get(f"/finance/{source}/status", headers=headers)
    assert r.status_code == 200
    st = r.json()
    assert st["installed"] is True
    assert st["counts"]["total"] == 0

    # 3. Backfill ingests observations.
    r = await client.post(f"/finance/{source}/backfill",
                          headers=headers, json={"count": 3, "seed": seed})
    assert r.status_code == 201, r.text
    bf = r.json()
    assert bf["ingested"] > 0
    assert bf["records"] == bf["ingested"] + bf["deduped"]

    # 4. Status reflects the backfilled observations.
    r = await client.get(f"/finance/{source}/status", headers=headers)
    st = r.json()
    assert st["counts"]["total"] == bf["ingested"]
    assert len(st["recent"]) > 0

    # 5. Re-running the same backfill dedups (versioned external_id parity).
    r = await client.post(f"/finance/{source}/backfill",
                          headers=headers, json={"count": 3, "seed": seed})
    bf2 = r.json()
    assert bf2["deduped"] > 0

    # 6. Live emit (no webhook self-call available in test -> inline fallback).
    r = await client.post(f"/finance/{source}/live/emit",
                          headers=headers, json={"seq": 1})
    assert r.status_code == 201, r.text
    le = r.json()
    assert le["delivered_via"] in ("webhook", "inline_fallback")


@requires_db
async def test_unknown_source_404(app_client):
    client, _ = app_client
    r = await client.post("/finance/stripe/install",
                          headers={"X-Tenant-Id": str(uuid4())})
    assert r.status_code == 404


@requires_db
async def test_sources_listing(app_client):
    client, _ = app_client
    r = await client.get("/finance/sources")
    assert r.status_code == 200
    srcs = {s["source"] for s in r.json()["sources"]}
    assert srcs == {"mercury", "quickbooks", "brex", "ramp", "gusto", "deel"}
