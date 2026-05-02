"""services/demo/budget.py — per-session cost cap.

Wraps an `LLMUsageAggregator` so each demo session has a hard ceiling
on outgoing LLM spend. Once the cap trips, callers see
`DemoCostCapExceeded` and the action list shows a graceful "Demo limit
reached for this session — please reset" message.

Usage pattern:

    cap = await DemoBudget.for_session(conn, session_id)
    if cap.tripped:
        return _budget_exceeded_response()
    with using_usage_aggregator(cap.aggregator):
        out = await provider.structured(...)
    await cap.flush(conn, call_kind="think")     # writes ledger rows
    if cap.tripped_after_call:
        await mark_session_cost_capped(conn, session_id)

`for_session` returns None when the tenant is not in demo mode — non-
demo callers just bypass the budget entirely.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional
from uuid import UUID

import asyncpg

from lib.llm.provider import LLMUsageAggregator
from lib.shared.errors import CompanyOSError
from services.demo.repo import (
    get_demo_config_by_id,
    get_demo_session,
    get_tenant,
    record_demo_session_cost,
)


class DemoCostCapExceeded(CompanyOSError):
    default_code = "demo_cost_cap_exceeded"


@dataclass
class DemoBudget:
    """Per-session budget envelope. Use one per Think/Render run."""

    session_id: UUID
    cap_usd: Decimal
    spent_usd: Decimal
    aggregator: LLMUsageAggregator = field(default_factory=LLMUsageAggregator)

    @property
    def tripped(self) -> bool:
        """Already over budget *before* this call."""
        return self.spent_usd >= self.cap_usd

    @property
    def remaining_usd(self) -> Decimal:
        return max(Decimal("0"), self.cap_usd - self.spent_usd)

    @property
    def latest_call_cost_usd(self) -> Decimal:
        return Decimal(str(self.aggregator.total_cost_usd))

    @property
    def tripped_after_call(self) -> bool:
        """True once the latest aggregator-tracked usage pushed total
        spend past the cap."""
        return (self.spent_usd + self.latest_call_cost_usd) >= self.cap_usd

    async def flush(
        self,
        conn: asyncpg.Connection | asyncpg.Pool,
        *,
        call_kind: str,
    ) -> Decimal:
        """Persist aggregator usage rows to demo_session_costs and bump
        spent_usd. Resets the aggregator so the budget can be reused
        across multiple Think calls in the same session."""
        if not self.aggregator.calls:
            return self.spent_usd
        for call in self.aggregator.calls:
            await record_demo_session_cost(
                conn,
                demo_session_id=self.session_id,
                call_kind=call_kind,
                model_name=call.model_name or "unknown",
                input_tokens=call.input_tokens,
                output_tokens=call.output_tokens,
                cost_usd=call.cost_usd,
            )
        added = Decimal(str(self.aggregator.total_cost_usd))
        self.spent_usd += added
        self.aggregator.reset()
        return self.spent_usd

    @classmethod
    async def for_session(
        cls,
        conn: asyncpg.Connection | asyncpg.Pool,
        session_id: UUID,
    ) -> Optional["DemoBudget"]:
        """Build a budget envelope for an active session. Returns None
        when the session is unknown or already ended."""
        session = await get_demo_session(conn, session_id)
        if session is None or session.ended_at is not None:
            return None
        cfg = await get_demo_config_by_id(conn, session.demo_config_id)
        if cfg is None:
            return None
        return cls(
            session_id=session_id,
            cap_usd=Decimal(str(cfg.cost_cap_usd_per_session)),
            spent_usd=Decimal(str(session.total_cost_usd)),
        )

    @classmethod
    async def for_tenant(
        cls,
        conn: asyncpg.Connection | asyncpg.Pool,
        tenant_id: UUID,
    ) -> Optional["DemoBudget"]:
        """Look up the active demo session for a tenant, build a budget.

        Returns None when the tenant is non-demo or has no active session.
        """
        tenant = await get_tenant(conn, tenant_id)
        if tenant is None or not tenant.is_demo:
            return None
        from services.demo.repo import get_active_session_for_tenant

        sess = await get_active_session_for_tenant(conn, tenant_id)
        if sess is None:
            return None
        return await cls.for_session(conn, sess.id)


async def mark_session_cost_capped(
    conn: asyncpg.Connection | asyncpg.Pool,
    session_id: UUID,
) -> None:
    await conn.execute(
        """
        UPDATE demo_sessions
        SET cost_cap_breached_at = COALESCE(cost_cap_breached_at, now())
        WHERE id = $1
        """,
        session_id,
    )


__all__ = [
    "DemoBudget",
    "DemoCostCapExceeded",
    "mark_session_cost_capped",
]
