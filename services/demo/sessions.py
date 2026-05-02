"""services/demo/sessions.py — start / end / reset / inactivity sweep.

The lifecycle:

  1. VC opens /demo, picks a company.
  2. POST /v1/demo/sessions/start {company_id} →
     - clone-on-demand: create a fresh tenant_id, register it as a
       demo tenant pointing at the company's demo_config, load the
       SQL snapshot into it, find or mint the CEO actor, mint a
       short-lived auth token for the CEO actor.
     - return {session_id, tenant_id, auth_token, ceo_actor_id}.
  3. POST /v1/demo/sessions/{id}/reset → wipe tenant data and reload.
  4. POST /v1/demo/sessions/{id}/end → mark ended, schedule cleanup.
  5. Inactivity sweep ends sessions idle > 4 hours.

For the snapshot path: this module is provider-agnostic. The actual
snapshot loading is provided by `services.demo.snapshot.load_snapshot`,
which reads a SQL file (compressed `.zst` or plain `.sql`) and applies
it inside a transaction. Falls back to a deterministic synthetic
loader when no snapshot file exists, so the demo flow works even
before Session 2's generation pipeline has produced real snapshots.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.ids import uuid7
from services.demo.repo import (
    end_demo_session,
    get_demo_config_by_company,
    get_demo_session,
    insert_demo_session,
    list_active_sessions_older_than,
    upsert_tenant,
)

if TYPE_CHECKING:
    from services.gateway.auth import AuthContext


INACTIVITY_TTL = timedelta(hours=4)
SESSION_TOKEN_TTL = timedelta(hours=4)


class DemoStartError(CompanyOSError):
    default_code = "demo_start_error"


@dataclass
class DemoStartResult:
    session_id: UUID
    tenant_id: UUID
    auth_token: str
    auth_token_expires_at: datetime
    ceo_actor_id: UUID
    company_id: str


async def start_session(
    pool: asyncpg.Pool,
    *,
    company_id: str,
) -> DemoStartResult:
    """Provision a fresh demo tenant, load the snapshot, mint a CEO
    auth token. Atomic: any failure rolls back the new tenant rows.
    """
    from services.demo.snapshot import load_snapshot

    async with pool.acquire() as conn:
        cfg = await get_demo_config_by_company(conn, company_id)
        if cfg is None:
            raise ValidationError(
                f"unknown demo company_id={company_id!r}",
                field="company_id",
            )

    new_tenant_id = uuid7()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await upsert_tenant(
                conn,
                tenant_id=new_tenant_id,
                name=f"{cfg.name} (demo {new_tenant_id.hex[:8]})",
                is_demo=True,
                demo_config_id=cfg.id,
            )
            ceo_actor_id = await load_snapshot(
                conn,
                tenant_id=new_tenant_id,
                snapshot_uri=cfg.snapshot_uri,
                company_id=cfg.company_id,
            )
            session = await insert_demo_session(
                conn,
                tenant_id=new_tenant_id,
                demo_config_id=cfg.id,
                ceo_actor_id=ceo_actor_id,
            )

    # Mint auth token outside the transaction so a failure here doesn't
    # roll back the tenant provisioning (which is the expensive part).
    from services.gateway.auth import create_session

    token, ctx = await create_session(
        pool,
        actor_id=ceo_actor_id,
        tenant_id=new_tenant_id,
        ttl=SESSION_TOKEN_TTL,
    )
    return DemoStartResult(
        session_id=session.id,
        tenant_id=new_tenant_id,
        auth_token=token,
        auth_token_expires_at=ctx.expires_at,
        ceo_actor_id=ceo_actor_id,
        company_id=cfg.company_id,
    )


async def reset_session(
    pool: asyncpg.Pool,
    *,
    session_id: UUID,
) -> None:
    """Wipe the tenant's mutable data and reload the snapshot. Keeps
    the same tenant_id so the auth token remains valid."""
    from services.demo.snapshot import load_snapshot, wipe_tenant

    session = await get_demo_session(pool, session_id)
    if session is None or session.ended_at is not None:
        raise ValidationError("demo session not active", field="session_id")
    cfg = None
    async with pool.acquire() as conn:
        from services.demo.repo import get_demo_config_by_id

        cfg = await get_demo_config_by_id(conn, session.demo_config_id)
    if cfg is None:
        raise ValidationError(
            "demo config missing for session", field="demo_config_id"
        )

    async with pool.acquire() as conn:
        async with conn.transaction():
            preserve_ids = (
                [session.ceo_actor_id] if session.ceo_actor_id else []
            )
            await wipe_tenant(
                conn,
                tenant_id=session.tenant_id,
                preserve_actor_ids=preserve_ids,
            )
            await load_snapshot(
                conn,
                tenant_id=session.tenant_id,
                snapshot_uri=cfg.snapshot_uri,
                company_id=cfg.company_id,
                preserve_ceo_actor_id=session.ceo_actor_id,
            )
            await conn.execute(
                """
                UPDATE demo_sessions
                SET total_cost_usd = 0,
                    signals_injected = 0,
                    actions_taken = 0,
                    cost_cap_breached_at = NULL,
                    last_active_at = now()
                WHERE id = $1
                """,
                session_id,
            )
            await conn.execute(
                "DELETE FROM demo_session_costs WHERE demo_session_id = $1",
                session_id,
            )


async def end_session(
    pool: asyncpg.Pool,
    *,
    session_id: UUID,
    end_reason: str = "user_ended",
) -> bool:
    return await end_demo_session(pool, session_id, end_reason=end_reason)


async def sweep_inactive_sessions(pool: asyncpg.Pool) -> int:
    """End any sessions whose last_active_at is older than INACTIVITY_TTL.
    Designed to be called from a scheduled background task."""
    cutoff = datetime.now(timezone.utc) - INACTIVITY_TTL
    ids = await list_active_sessions_older_than(pool, cutoff=cutoff)
    for sid in ids:
        await end_session(pool, session_id=sid, end_reason="inactivity")
    return len(ids)


async def run_inactivity_sweeper(
    pool: asyncpg.Pool,
    *,
    interval_seconds: int | None = None,
) -> None:
    """Background coroutine: sweep every `interval_seconds` (default
    900s = 15 min). Cancellation-safe."""
    interval = interval_seconds or int(
        os.environ.get("DEMO_SWEEP_INTERVAL_SECONDS", "900")
    )
    while True:
        try:
            await sweep_inactive_sessions(pool)
        except Exception:
            pass
        await asyncio.sleep(interval)


__all__ = [
    "DemoStartError",
    "DemoStartResult",
    "INACTIVITY_TTL",
    "SESSION_TOKEN_TTL",
    "start_session",
    "reset_session",
    "end_session",
    "sweep_inactive_sessions",
    "run_inactivity_sweeper",
]
