"""CartaPollGenerator — synthetic live Carta cap-table changes (IN-CARTA).

Drives the PRODUCTION Carta live POLL path in-process, the way
TelegramGatewayGenerator does for Telegram — but Carta's live edge is a POLLER,
not a persistent connection:

  Generator → mint one fresh cap-table change (unique entity id + a current
              partition-window timestamp)
            → build a per-tenant `poll.PollDeps` (bound to the tenant's
              carta_installations id + firm_id, resolved from the pool)
            → `integrations.carta.poll.handle_polled_change(change, deps)`
            → cutover `shadow_write_raw(source="carta", ingress_kind="poll")`
              → ingestion.raw.carta → normalizer → observation_writer.

Carta is NOT an HTTP-webhook provider; its live path is an interval re-list. So —
exactly like Telegram's gateway dispatch — the live observation flows through the
SAME normalizer→observation_writer Kafka chain as backfill, landing in
`observations` while backfill is still in flight. There is no HTTP status to
assert (the generator returns None for it, like Telegram).

The live `firm_id` is the REAL carta_installations.firm_id for the tenant
(resolved + cached from the pool), so the live external_id is derived through the
SAME `carta_entity(firm_id, entity_kind, entity_id, sync_token)` constructor as
backfill — giving genuine cross-path parity AND per-tenant uniqueness (the firm_id
namespaces the global observations UNIQUE). Each `simulate_event` uses a
globally-unique entity id (≥ 1_000_000, disjoint from backfill ids 1..N) and a
2026-06 timestamp, so N live events ⇒ N distinct observations.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg


log = logging.getLogger(__name__)

# Current-window base (2026-06-xx) so live timestamps land inside the
# observations partition coverage and are distinct from the 2026-01 backfill
# window.
_LIVE_BASE = datetime(2026, 6, 15, tzinfo=timezone.utc)
# Live entity ids start far above any backfill id (fixtures use 1000..N).
_LIVE_ID_BASE = 1_000_000

# The cap-table entity kind the synthetic live change defaults to (the live edge
# re-lists across all kinds; one kind suffices to prove the path).
_DEFAULT_ENTITY_TYPE = "OptionGrant"


@dataclass
class CartaPollResult:
    http_status: int | None  # always None — poll dispatch has no HTTP status
    external_hint: str
    tenant_id: UUID | None = None


class CartaPollGenerator:
    """Drives Carta live cap-table changes for all carta targets via direct
    poll dispatch."""

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
        # Cache: tenant_id -> (installation_id, firm_id).
        self._install_cache: dict[UUID, tuple[str, str]] = {}
        self._actor_repo: Any = None
        self._alias_repo: Any = None

    async def __aenter__(self) -> "CartaPollGenerator":
        from services.domain.actors.repo import ActorRepo
        from services.domain.entity_aliases.repo import EntityAliasRepo
        self._actor_repo = ActorRepo(self._pool)
        self._alias_repo = EntityAliasRepo(self._pool)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return None

    async def _resolve_install(self, tenant_id: UUID) -> tuple[str, str]:
        """Resolve (and cache) the tenant's (carta_installations.id, firm_id), so
        the live external_id is namespaced identically to backfill. Superuser
        test connection bypasses the table's RLS (the harness convention)."""
        cached = self._install_cache.get(tenant_id)
        if cached is not None:
            return cached
        row = await self._pool.fetchrow(
            "SELECT id, firm_id FROM carta_installations "
            "WHERE tenant_id = $1 AND disabled_at IS NULL "
            "ORDER BY created_at LIMIT 1",
            tenant_id,
        )
        if row is not None:
            resolved = (str(row["id"]), str(row["firm_id"]))
        else:
            resolved = (str(tenant_id), str(tenant_id))
        self._install_cache[tenant_id] = resolved
        return resolved

    def _mint_change(self, content: str, entity_type: str) -> dict[str, Any]:
        self._seq += 1
        entity_id = str(_LIVE_ID_BASE + self._seq)
        updated = (_LIVE_BASE + timedelta(minutes=self._seq)).isoformat()
        entity = {
            "Id": entity_id,
            # A fresh, globally-unique sync_token keeps each live change distinct.
            "SyncToken": str(self._seq),
            "DocNumber": f"{entity_type[:3].upper()}-{entity_id}",
            "Status": "exercised",  # a lifecycle state_change on the cap table
            "Quantity": 500,
            "StrikePrice": 1.25,
            "StakeholderRef": {"value": str(self._seq), "name": content},
            "MetaData": {"LastUpdatedTime": updated},
        }
        return {"entity_type": entity_type, "entity": entity}

    async def simulate_event(
        self, *, target: "Any", content: str = "live-grant",
    ) -> CartaPollResult:
        """Mint one fresh cap-table change + dispatch it through the production
        poll path."""
        from services.ingest.integrations.carta.poll import (
            PollDeps,
            handle_polled_change,
        )

        installation_id, firm_id = await self._resolve_install(target.tenant_id)
        entity_type = getattr(target, "carta_entity_type", None) or _DEFAULT_ENTITY_TYPE
        change = self._mint_change(content, entity_type)
        entity_id = change["entity"]["Id"]
        sync_token = change["entity"]["SyncToken"]

        deps = PollDeps(
            pool=self._pool,
            tenant_id=target.tenant_id,
            installation_id=installation_id,
            firm_id=firm_id,
            actor_repo=self._actor_repo,
            alias_repo=self._alias_repo,
            embedder=None,
            s3_raw_client=self._s3,
            kafka_producer=self._producer,
            tenant_flags=self._flags,
        )
        await handle_polled_change(change, deps)

        # entity_kind normalisation mirrors the handler's _ENTITY_NORMALISE.
        entity_kind = entity_type.lower()
        _NORMALISE = {
            "shareholder": "shareholder", "shareclass": "share_class",
            "safenote": "safe_note", "optiongrant": "option_grant",
        }
        kind = _NORMALISE.get(entity_kind, entity_kind)
        return CartaPollResult(
            http_status=None,
            external_hint=(
                f"carta:{firm_id}:{kind}:{entity_id}:{sync_token}"
            ),
            tenant_id=getattr(target, "tenant_id", None),
        )


__all__ = ["CartaPollGenerator", "CartaPollResult"]
