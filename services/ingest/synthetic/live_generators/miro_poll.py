"""Synthetic Miro polling through the production poll-change dispatch."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg


_LIVE_BASE = datetime(2026, 6, 15, tzinfo=timezone.utc)


@dataclass
class MiroPollResult:
    http_status: int | None
    external_hint: str
    tenant_id: UUID | None = None


class MiroPollGenerator:
    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        kafka_producer: Any = None,
        s3_raw_client: Any = None,
        tenant_flags: Any = None,
    ) -> None:
        self._pool = pool
        self._producer = kafka_producer
        self._s3 = s3_raw_client
        self._flags = tenant_flags
        self._seq = 0
        self._actor_repo: Any = None
        self._alias_repo: Any = None
        self._install_cache: dict[tuple[UUID, str], str] = {}

    async def __aenter__(self) -> "MiroPollGenerator":
        from services.domain.actors.repo import ActorRepo
        from services.domain.entity_aliases.repo import EntityAliasRepo

        self._actor_repo = ActorRepo(self._pool)
        self._alias_repo = EntityAliasRepo(self._pool)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    async def _installation_id(self, tenant_id: UUID, org_id: str) -> str:
        key = (tenant_id, org_id)
        cached = self._install_cache.get(key)
        if cached is not None:
            return cached
        rows = await self._pool.fetch(
            "SELECT id FROM miro_installations "
            "WHERE tenant_id = $1 AND org_id = $2 AND disabled_at IS NULL",
            tenant_id,
            org_id,
        )
        if len(rows) != 1:
            raise ValueError(
                "miro target must resolve exactly one active installation: "
                f"tenant_id={tenant_id}, org_id={org_id!r}, matches={len(rows)}"
            )
        value = str(rows[0]["id"])
        self._install_cache[key] = value
        return value

    async def simulate_event(
        self, *, target: Any, content: str = "live-board-item",
    ) -> MiroPollResult:
        from services.ingest.integrations.miro.poll import (
            PollDeps,
            handle_polled_change,
        )

        org_id = target.miro_org
        board_id = target.miro_board
        if not org_id or not board_id:
            raise ValueError("miro target is missing org or board identity")
        installation_id = await self._installation_id(
            target.tenant_id,
            org_id,
        )
        self._seq += 1
        occurred = _LIVE_BASE + timedelta(minutes=self._seq)
        item_id = f"live-miro-item-{self._seq}"
        version = str(self._seq)
        timestamp = occurred.isoformat().replace("+00:00", "Z")
        item = {
            "id": item_id,
            "boardId": board_id,
            "type": "sticky_note",
            "data": {"content": content},
            "createdAt": timestamp,
            "modifiedAt": timestamp,
            "version": version,
        }
        await handle_polled_change(
            item,
            PollDeps(
                pool=self._pool,
                tenant_id=target.tenant_id,
                installation_id=installation_id,
                org_id=org_id,
                board_id=board_id,
                actor_repo=self._actor_repo,
                alias_repo=self._alias_repo,
                embedder=None,
                s3_raw_client=self._s3,
                kafka_producer=self._producer,
                tenant_flags=self._flags,
            ),
        )
        return MiroPollResult(
            http_status=None,
            external_hint=f"miro:{org_id}:item:{item_id}:{version}",
            tenant_id=target.tenant_id,
        )


__all__ = ["MiroPollGenerator", "MiroPollResult"]
