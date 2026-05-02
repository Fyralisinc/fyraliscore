"""services/demo/repo.py — read/write helpers for demo infrastructure.

Thin asyncpg wrappers around `tenants`, `demo_configs`, `demo_sessions`,
and `demo_session_costs` (migration 0023). No business logic lives here
— it belongs in `sessions.py` / `budget.py`.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from lib.shared.types import (
    DemoConfigRow,
    DemoSessionRow,
    TenantRow,
)


# ---------------------------------------------------------------------
# Tenants
# ---------------------------------------------------------------------


async def get_tenant(
    conn: asyncpg.Connection | asyncpg.Pool,
    tenant_id: UUID,
) -> TenantRow | None:
    """Return the tenants row for `tenant_id`, or None when absent.

    Existing tenants in the system reference `tenant_id` as a free-
    floating UUID without a row in this table. Absence ⇒ non-demo.
    """
    row = await conn.fetchrow(
        "SELECT id, name, is_demo, demo_config_id, created_at, archived_at "
        "FROM tenants WHERE id = $1",
        tenant_id,
    )
    if row is None:
        return None
    return TenantRow(
        id=row["id"],
        name=row["name"],
        is_demo=row["is_demo"],
        demo_config_id=row["demo_config_id"],
        created_at=row["created_at"],
        archived_at=row["archived_at"],
    )


async def upsert_tenant(
    conn: asyncpg.Connection | asyncpg.Pool,
    *,
    tenant_id: UUID,
    name: str,
    is_demo: bool,
    demo_config_id: UUID | None,
) -> None:
    await conn.execute(
        """
        INSERT INTO tenants (id, name, is_demo, demo_config_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (id) DO UPDATE
          SET name = EXCLUDED.name,
              is_demo = EXCLUDED.is_demo,
              demo_config_id = EXCLUDED.demo_config_id
        """,
        tenant_id, name, is_demo, demo_config_id,
    )


# ---------------------------------------------------------------------
# Demo configs
# ---------------------------------------------------------------------


async def list_demo_configs(
    conn: asyncpg.Connection | asyncpg.Pool,
) -> list[DemoConfigRow]:
    rows = await conn.fetch(
        """
        SELECT id, company_id, name, description, tagline, snapshot_uri,
               model_routing, cost_cap_usd_per_session,
               notifications_suppressed, determinism_seed,
               reset_on_session_end, metadata, created_at
        FROM demo_configs
        ORDER BY company_id
        """
    )
    return [_hydrate_demo_config(r) for r in rows]


async def get_demo_config_by_company(
    conn: asyncpg.Connection | asyncpg.Pool,
    company_id: str,
) -> DemoConfigRow | None:
    row = await conn.fetchrow(
        """
        SELECT id, company_id, name, description, tagline, snapshot_uri,
               model_routing, cost_cap_usd_per_session,
               notifications_suppressed, determinism_seed,
               reset_on_session_end, metadata, created_at
        FROM demo_configs WHERE company_id = $1
        """,
        company_id,
    )
    return _hydrate_demo_config(row) if row else None


async def get_demo_config_by_id(
    conn: asyncpg.Connection | asyncpg.Pool,
    demo_config_id: UUID,
) -> DemoConfigRow | None:
    row = await conn.fetchrow(
        """
        SELECT id, company_id, name, description, tagline, snapshot_uri,
               model_routing, cost_cap_usd_per_session,
               notifications_suppressed, determinism_seed,
               reset_on_session_end, metadata, created_at
        FROM demo_configs WHERE id = $1
        """,
        demo_config_id,
    )
    return _hydrate_demo_config(row) if row else None


def _hydrate_demo_config(row: asyncpg.Record) -> DemoConfigRow:
    return DemoConfigRow(
        id=row["id"],
        company_id=row["company_id"],
        name=row["name"],
        description=row["description"],
        tagline=row["tagline"],
        snapshot_uri=row["snapshot_uri"],
        model_routing=_coerce_jsonb(row["model_routing"]),
        cost_cap_usd_per_session=row["cost_cap_usd_per_session"],
        notifications_suppressed=row["notifications_suppressed"],
        determinism_seed=row["determinism_seed"],
        reset_on_session_end=row["reset_on_session_end"],
        metadata=_coerce_jsonb(row["metadata"]),
        created_at=row["created_at"],
    )


# ---------------------------------------------------------------------
# Demo sessions
# ---------------------------------------------------------------------


async def insert_demo_session(
    conn: asyncpg.Connection | asyncpg.Pool,
    *,
    tenant_id: UUID,
    demo_config_id: UUID,
    ceo_actor_id: UUID | None,
) -> DemoSessionRow:
    session_id = uuid7()
    now = datetime.now(timezone.utc)
    await conn.execute(
        """
        INSERT INTO demo_sessions (
            id, tenant_id, demo_config_id, ceo_actor_id,
            started_at, last_active_at, total_cost_usd
        )
        VALUES ($1, $2, $3, $4, $5, $5, 0)
        """,
        session_id, tenant_id, demo_config_id, ceo_actor_id, now,
    )
    return DemoSessionRow(
        id=session_id,
        tenant_id=tenant_id,
        demo_config_id=demo_config_id,
        ceo_actor_id=ceo_actor_id,
        started_at=now,
        last_active_at=now,
        ended_at=None,
        end_reason=None,
        total_cost_usd=Decimal("0"),
        signals_injected=0,
        actions_taken=0,
        cost_cap_breached_at=None,
    )


async def get_demo_session(
    conn: asyncpg.Connection | asyncpg.Pool,
    session_id: UUID,
) -> DemoSessionRow | None:
    row = await conn.fetchrow(
        """
        SELECT id, tenant_id, demo_config_id, ceo_actor_id,
               started_at, last_active_at, ended_at, end_reason,
               total_cost_usd, signals_injected, actions_taken,
               cost_cap_breached_at
        FROM demo_sessions WHERE id = $1
        """,
        session_id,
    )
    if row is None:
        return None
    return DemoSessionRow(
        id=row["id"],
        tenant_id=row["tenant_id"],
        demo_config_id=row["demo_config_id"],
        ceo_actor_id=row["ceo_actor_id"],
        started_at=row["started_at"],
        last_active_at=row["last_active_at"],
        ended_at=row["ended_at"],
        end_reason=row["end_reason"],
        total_cost_usd=row["total_cost_usd"],
        signals_injected=row["signals_injected"],
        actions_taken=row["actions_taken"],
        cost_cap_breached_at=row["cost_cap_breached_at"],
    )


async def get_active_session_for_tenant(
    conn: asyncpg.Connection | asyncpg.Pool,
    tenant_id: UUID,
) -> DemoSessionRow | None:
    """Return the most recent unended demo_session for tenant, or None."""
    row = await conn.fetchrow(
        """
        SELECT id FROM demo_sessions
        WHERE tenant_id = $1 AND ended_at IS NULL
        ORDER BY started_at DESC LIMIT 1
        """,
        tenant_id,
    )
    if row is None:
        return None
    return await get_demo_session(conn, row["id"])


async def end_demo_session(
    conn: asyncpg.Connection | asyncpg.Pool,
    session_id: UUID,
    *,
    end_reason: str = "user_ended",
) -> bool:
    result = await conn.execute(
        """
        UPDATE demo_sessions
        SET ended_at = now(), end_reason = $2
        WHERE id = $1 AND ended_at IS NULL
        """,
        session_id, end_reason,
    )
    return result.strip().endswith("1")


async def touch_demo_session(
    conn: asyncpg.Connection | asyncpg.Pool,
    session_id: UUID,
) -> None:
    """Mark session as active right now (called on each meaningful interaction)."""
    await conn.execute(
        "UPDATE demo_sessions SET last_active_at = now() WHERE id = $1",
        session_id,
    )


async def increment_signal_count(
    conn: asyncpg.Connection | asyncpg.Pool,
    session_id: UUID,
) -> None:
    await conn.execute(
        "UPDATE demo_sessions SET signals_injected = signals_injected + 1, "
        "last_active_at = now() WHERE id = $1",
        session_id,
    )


async def increment_action_count(
    conn: asyncpg.Connection | asyncpg.Pool,
    session_id: UUID,
) -> None:
    await conn.execute(
        "UPDATE demo_sessions SET actions_taken = actions_taken + 1, "
        "last_active_at = now() WHERE id = $1",
        session_id,
    )


# ---------------------------------------------------------------------
# Cost ledger
# ---------------------------------------------------------------------


async def record_demo_session_cost(
    conn: asyncpg.Connection | asyncpg.Pool,
    *,
    demo_session_id: UUID,
    call_kind: str,
    model_name: str,
    input_tokens: int,
    output_tokens: int,
    cost_usd: float,
) -> None:
    """Append a cost row + bump the session's running total in one
    statement. Idempotent on an existing session id."""
    cost_id = uuid7()
    await conn.execute(
        """
        INSERT INTO demo_session_costs (
            id, demo_session_id, call_kind, model_name,
            input_tokens, output_tokens, cost_usd
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        cost_id, demo_session_id, call_kind, model_name,
        int(input_tokens), int(output_tokens), Decimal(str(cost_usd)),
    )
    await update_session_cost_total(conn, demo_session_id)


async def update_session_cost_total(
    conn: asyncpg.Connection | asyncpg.Pool,
    demo_session_id: UUID,
) -> Decimal:
    """Re-compute total_cost_usd from the costs ledger. Returns the
    new total. Cheap: one aggregate over a small per-session set."""
    row = await conn.fetchrow(
        """
        UPDATE demo_sessions
        SET total_cost_usd = COALESCE((
            SELECT SUM(cost_usd) FROM demo_session_costs
            WHERE demo_session_id = $1
        ), 0)
        WHERE id = $1
        RETURNING total_cost_usd
        """,
        demo_session_id,
    )
    return row["total_cost_usd"] if row else Decimal("0")


async def list_active_sessions_older_than(
    conn: asyncpg.Connection | asyncpg.Pool,
    *,
    cutoff: datetime,
) -> list[UUID]:
    """For the inactivity sweeper. Returns session ids whose
    last_active_at is older than `cutoff` and that haven't been ended."""
    rows = await conn.fetch(
        """
        SELECT id FROM demo_sessions
        WHERE ended_at IS NULL AND last_active_at < $1
        """,
        cutoff,
    )
    return [r["id"] for r in rows]


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _coerce_jsonb(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        import json
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


__all__ = [
    "get_tenant", "upsert_tenant",
    "list_demo_configs", "get_demo_config_by_company", "get_demo_config_by_id",
    "insert_demo_session", "get_demo_session", "get_active_session_for_tenant",
    "end_demo_session", "touch_demo_session",
    "increment_signal_count", "increment_action_count",
    "record_demo_session_cost", "update_session_cost_total",
    "list_active_sessions_older_than",
]
