"""TelegramGatewayGenerator — synthetic live MTProto updates (IN-TELEGRAM).

Drives the PRODUCTION Telegram live path in-process, the way DiscordGatewayGenerator
does for Discord:

  Generator → mint one fresh `updateNewMessage` (unique id + a current
              partition-window timestamp)
            → build a per-tenant `gateway.dispatch.DispatchDeps` (bound to the
              tenant's telegram_installations id, resolved from the pool)
            → `gateway.dispatch.handle_update(update, deps)`
            → cutover `shadow_write_raw(source="telegram", ingress_kind="gateway")`
              → ingestion.raw.telegram → normalizer → observation_writer.

Telegram is NOT an HTTP-webhook provider; its live path is a persistent updates
connection. So — exactly like Discord's gateway dispatch — the live observation
flows through the SAME normalizer→observation_writer Kafka chain as backfill,
landing in `observations` while backfill is still in flight. There is no HTTP
status to assert (the generator returns None for it, like Discord).

The live `installation_id` is the REAL telegram_installations.id for the tenant
(resolved + cached from the pool), so the live external_id is derived through the
same `idempotency.telegram_message` constructor as backfill — giving genuine
cross-path parity AND per-tenant uniqueness. Each `simulate_message` uses a
globally-unique message id (≥ 1_000_000, disjoint from backfill ids 1..N) and a
2026-06 timestamp, so N live events ⇒ N distinct observations.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg


log = logging.getLogger(__name__)

# Current-window base (2026-06-xx, epoch SECONDS — the handler reads message
# `date` as epoch seconds) so live timestamps land inside the observations
# partition coverage and are distinct from the 2026-01 backfill window.
_LIVE_BASE_S = 1781000000
# Live message ids start far above any backfill id (fixtures use 1..N).
_LIVE_ID_BASE = 1_000_000


@dataclass
class TelegramGatewayResult:
    http_status: int | None  # always None — gateway dispatch has no HTTP status
    external_hint: str
    tenant_id: UUID | None = None


class TelegramGatewayGenerator:
    """Drives Telegram live updates for all telegram targets via direct dispatch."""

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
        self._install_cache: dict[tuple[UUID, int], str] = {}
        self._actor_repo: Any = None
        self._alias_repo: Any = None

    async def __aenter__(self) -> "TelegramGatewayGenerator":
        from services.domain.actors.repo import ActorRepo
        from services.domain.entity_aliases.repo import EntityAliasRepo
        self._actor_repo = ActorRepo(self._pool)
        self._alias_repo = EntityAliasRepo(self._pool)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return None

    async def _installation_id(self, tenant_id: UUID, dialog_id: int) -> str:
        """Resolve the one active install owning the target dialog."""
        cache_key = (tenant_id, dialog_id)
        cached = self._install_cache.get(cache_key)
        if cached is not None:
            return cached
        rows = await self._pool.fetch(
            """
            SELECT ti.id
              FROM telegram_installations ti
              JOIN telegram_dialogs td
                ON td.telegram_installation_id = ti.id
             WHERE ti.tenant_id = $1
               AND ti.disabled_at IS NULL
               AND td.tenant_id = $1
               AND td.dialog_id = $2
            """,
            tenant_id,
            dialog_id,
        )
        if len(rows) != 1:
            raise ValueError(
                "telegram target must resolve to exactly one active installation: "
                f"tenant_id={tenant_id}, dialog_id={dialog_id}, matches={len(rows)}",
            )
        iid = str(rows[0]["id"])
        self._install_cache[cache_key] = iid
        return iid

    def _mint_message(self, content: str) -> dict[str, Any]:
        self._seq += 1
        sender = _LIVE_ID_BASE + self._seq
        return {
            "id": _LIVE_ID_BASE + self._seq,
            "date": _LIVE_BASE_S + self._seq,
            "edit_date": None,
            "message": content,
            "out": False,
            "from_id": {"user_id": sender},
            "sender_username": f"live_user_{self._seq}",
        }

    async def simulate_message(
        self, *, target: "Any", content: str = "hello",
    ) -> TelegramGatewayResult:
        """Mint one fresh live message + dispatch it through the production path."""
        from services.ingest.integrations.telegram.gateway.dispatch import (
            DispatchDeps,
            handle_update,
        )

        dialog_id = target.telegram_dialog_id
        if dialog_id is None:
            raise ValueError("telegram target is missing telegram_dialog_id")
        installation_id = await self._installation_id(
            target.tenant_id,
            dialog_id,
        )
        message = self._mint_message(content)
        update = {
            "event": "new_message",
            "message": message,
            "dialog_id": dialog_id,
            "dialog_kind": target.telegram_dialog_kind or "chat",
            "dialog_title": target.telegram_dialog_title,
        }
        deps = DispatchDeps(
            pool=self._pool,
            tenant_id=target.tenant_id,
            installation_id=installation_id,
            actor_repo=self._actor_repo,
            alias_repo=self._alias_repo,
            embedder=None,
            s3_raw_client=self._s3,
            kafka_producer=self._producer,
            tenant_flags=self._flags,
        )
        await handle_update(update, deps)
        return TelegramGatewayResult(
            http_status=None,
            external_hint=(
                f"telegram:{installation_id}:{dialog_id}:{message['id']}:none"
            ),
            tenant_id=getattr(target, "tenant_id", None),
        )


__all__ = ["TelegramGatewayGenerator", "TelegramGatewayResult"]
