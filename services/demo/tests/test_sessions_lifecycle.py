"""services/demo/tests/test_sessions_lifecycle.py — start / reset / end
flow exercised against the Pelago snapshot loader."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.demo.repo import (
    get_demo_config_by_company,
    get_demo_session,
    insert_demo_session,
    upsert_tenant,
)
from services.demo.sessions import (
    end_session,
    reset_session,
    start_session,
    sweep_inactive_sessions,
)


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_start_session_provisions_tenant_and_actor(
    fresh_db: asyncpg.Pool,
):
    result = await start_session(fresh_db, company_id="pelago")
    assert result.company_id == "pelago"
    assert result.auth_token

    tenant_row = await fresh_db.fetchrow(
        "SELECT is_demo, demo_config_id FROM tenants WHERE id = $1",
        result.tenant_id,
    )
    assert tenant_row is not None
    assert tenant_row["is_demo"] is True
    assert tenant_row["demo_config_id"] is not None

    actor_row = await fresh_db.fetchrow(
        "SELECT id FROM actors WHERE id = $1 AND tenant_id = $2",
        result.ceo_actor_id, result.tenant_id,
    )
    assert actor_row is not None

    rec_count = await fresh_db.fetchval(
        """
        SELECT COUNT(*) FROM models
        WHERE tenant_id = $1 AND proposition_kind = 'recommendation'
        """,
        result.tenant_id,
    )
    assert rec_count > 0


@pytest.mark.asyncio
async def test_start_session_rejects_unknown_company(
    fresh_db: asyncpg.Pool,
):
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await start_session(fresh_db, company_id="nope")


@pytest.mark.asyncio
async def test_reset_keeps_tenant_id_and_ceo_actor(
    fresh_db: asyncpg.Pool,
):
    started = await start_session(fresh_db, company_id="pelago")

    # Mutate state so reset has something to undo.
    await fresh_db.execute(
        """
        UPDATE demo_sessions
        SET signals_injected = 7, actions_taken = 3
        WHERE id = $1
        """,
        started.session_id,
    )

    await reset_session(fresh_db, session_id=started.session_id)

    refetched = await get_demo_session(fresh_db, started.session_id)
    assert refetched is not None
    assert refetched.tenant_id == started.tenant_id
    assert refetched.ceo_actor_id == started.ceo_actor_id
    assert refetched.signals_injected == 0
    assert refetched.actions_taken == 0

    # CEO actor still exists post-reset.
    actor_row = await fresh_db.fetchrow(
        "SELECT id FROM actors WHERE id = $1",
        started.ceo_actor_id,
    )
    assert actor_row is not None


@pytest.mark.asyncio
async def test_end_session_marks_ended_with_reason(fresh_db: asyncpg.Pool):
    started = await start_session(fresh_db, company_id="pelago")
    ok = await end_session(
        fresh_db, session_id=started.session_id, end_reason="user_ended",
    )
    assert ok is True
    refetched = await get_demo_session(fresh_db, started.session_id)
    assert refetched is not None
    assert refetched.ended_at is not None
    assert refetched.end_reason == "user_ended"


@pytest.mark.asyncio
async def test_inactivity_sweeper_ends_idle_sessions(fresh_db: asyncpg.Pool):
    cfg = await get_demo_config_by_company(fresh_db, "pelago")
    assert cfg is not None
    tid = uuid7()
    await upsert_tenant(
        fresh_db, tenant_id=tid, name="pelago-sweeper",
        is_demo=True, demo_config_id=cfg.id,
    )
    sess = await insert_demo_session(
        fresh_db, tenant_id=tid, demo_config_id=cfg.id, ceo_actor_id=None,
    )
    # Force the session into the past so the 4-hour cutoff trips.
    await fresh_db.execute(
        "UPDATE demo_sessions SET last_active_at = $2 WHERE id = $1",
        sess.id, datetime.now(timezone.utc) - timedelta(hours=5),
    )
    swept = await sweep_inactive_sessions(fresh_db)
    assert swept >= 1
    refetched = await get_demo_session(fresh_db, sess.id)
    assert refetched is not None
    assert refetched.end_reason == "inactivity"
