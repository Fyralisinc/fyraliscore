"""Durable workers for perception-to-episode construction and settlement."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import asyncpg

from .construction import EpisodeConstructionService
from .intake import EpisodeIntakeRepository, PerceptionOutboxRow
from .service import EpisodeRoutingService


class EpisodeConstructorWorker:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._intake = EpisodeIntakeRepository()
        self._router = EpisodeRoutingService()
        self._construction = EpisodeConstructionService()

    async def process_claimed(
        self, item: PerceptionOutboxRow, *, worker_id: str
    ) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT set_config('app.current_tenant',$1::text,true)",
                    str(item.tenant_id),
                )
                memberships = await self._router.route(item, conn=conn)
                included = [value for value in memberships if value.decision == "include"]
                for episode_id in sorted({value.episode_id for value in included}, key=str):
                    membership = next(value for value in included if value.episode_id == episode_id)
                    state = await conn.fetchval(
                        "SELECT lifecycle_state FROM episodes WHERE id=$1 AND tenant_id=$2",
                        episode_id,item.tenant_id,
                    )
                    if state == "settled":
                        await self._construction.reopen_for_late_evidence(
                            episode_id,tenant_id=item.tenant_id,
                            membership_id=membership.id,conn=conn,
                        )
                    elif state == "dormant":
                        await self._construction.reactivate_from_dormant(
                            episode_id, tenant_id=item.tenant_id,
                            membership_id=membership.id, conn=conn,
                        )
                    else:
                        await self._construction.ensure_opened(
                            episode_id,tenant_id=item.tenant_id,conn=conn,
                        )
                await self._intake.complete(
                    item.id,tenant_id=item.tenant_id,worker_id=worker_id,conn=conn
                )

    async def run_once(
        self,
        *,
        worker_id: str,
        batch_size: int = 50,
        lease_seconds: int = 60,
        retry_delay_seconds: int = 5,
        max_attempts: int = 5,
    ) -> int:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                claimed = await self._intake.claim(
                    worker_id=worker_id,batch_size=batch_size,
                    lease_seconds=lease_seconds,conn=conn,
                )
        for item in claimed:
            try:
                await self.process_claimed(item,worker_id=worker_id)
            except Exception as exc:  # noqa: BLE001 - durable retry owns failures
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await self._intake.retry(
                            item.id,tenant_id=item.tenant_id,worker_id=worker_id,
                            error=f"{type(exc).__name__}: {exc}",
                            delay_seconds=retry_delay_seconds,max_attempts=max_attempts,
                            conn=conn,
                        )
        return len(claimed)


class EpisodeSettlementWorker:
    def __init__(self,pool: asyncpg.Pool,*,quiet_period: timedelta) -> None:
        self._pool=pool
        self._quiet_period=quiet_period
        self._construction=EpisodeConstructionService()

    async def run_once(
        self,*,batch_size: int=50,evaluated_at: datetime | None=None
    ) -> int:
        now=evaluated_at or datetime.now(UTC)
        async with self._pool.acquire() as conn:
            rows=await conn.fetch(
                """
                SELECT id,tenant_id,lifecycle_state FROM episodes
                 WHERE lifecycle_state IN ('open','reopened','dormant')
                   AND last_ingested_at <= $1
                 ORDER BY last_ingested_at,id LIMIT $2
                """,
                now-self._quiet_period,batch_size,
            )
        settled=0
        for row in rows:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "SELECT set_config('app.current_tenant',$1::text,true)",
                        str(row["tenant_id"]),
                    )
                    if row["lifecycle_state"] in {"open","reopened"}:
                        dormant=await self._construction.mark_dormant(
                            row["id"],tenant_id=row["tenant_id"],
                            quiet_period=self._quiet_period,evaluated_at=now,conn=conn,
                        )
                        if not dormant:
                            continue
                    await self._construction.settle(
                        row["id"],tenant_id=row["tenant_id"],reason="quiet_period",
                        evaluated_at=now,conn=conn,
                    )
                    settled+=1
        return settled


__all__=["EpisodeConstructorWorker","EpisodeSettlementWorker"]
