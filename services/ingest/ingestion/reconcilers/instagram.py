"""Instagram conversation gap detection."""
from __future__ import annotations

import os
from typing import Any

import asyncpg
import orjson

from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
    ResharedShard,
)
from services.ingest.ingestion.workflows.state import load_state
from services.ingest.integrations.instagram.onboarding import (
    upsert_discovered_conversations,
)
from services.ingest.integrations.instagram.records import business_endpoint_ids


SHARD_KIND_CONVERSATION_HISTORY = "instagram_conversation_history"
RESHARE_RECENCY_SCORE = 1.5

_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.instagram: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


async def _open_instagram_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers.instagram import _open_instagram_client as _open
    return await _open(install)


def _decode_identifier(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, (str, bytes)):
        return orjson.loads(raw)
    return dict(raw)


def _provider_conversation_id(identifier: dict[str, Any]) -> str:
    return str(
        identifier.get("provider_conversation_id")
        or identifier.get("conversation_id")
        or ""
    ).strip()


def _conversation_identifier(
    *, install: asyncpg.Record, conversation: dict[str, Any], parent_shard_id: Any,
) -> dict[str, Any] | None:
    provider_id = str(conversation.get("id") or "").strip()
    if not provider_id:
        return None
    business_id = str(install["ig_business_account_id"])
    business_ids = business_endpoint_ids(
        business_id,
        install["page_id"] if "page_id" in install else None,
        install["webhook_delivery_account_id"]
        if "webhook_delivery_account_id" in install
        else None,
    )
    participants = conversation.get("participants")
    participants = participants.get("data") if isinstance(participants, dict) else participants
    participant: dict[str, Any] | None = None
    if isinstance(participants, list):
        participant = next(
            (
                item for item in participants
                if (
                    isinstance(item, dict)
                    and str(item.get("id") or "") not in business_ids
                )
            ),
            None,
        )
    participant_id = str((participant or {}).get("id") or "").strip() or None
    return {
        "shard_kind": SHARD_KIND_CONVERSATION_HISTORY,
        "installation_id": str(install["id"]),
        "ig_business_account_id": business_id,
        "page_id": install["page_id"] if "page_id" in install else None,
        "webhook_delivery_account_id": (
            install["webhook_delivery_account_id"]
            if "webhook_delivery_account_id" in install
            else None
        ),
        "provider_conversation_id": provider_id,
        "thread_key": f"{business_id}:{participant_id or provider_id}",
        "participant_id": participant_id,
        "participant_username": (participant or {}).get("username"),
        "participant_display_name": (participant or {}).get("name"),
        "messages_cursor": None,
        "parent_shard_id": str(parent_shard_id),
        "ingress_kind": "poll",
    }


async def _discover_conversations(
    *, pool: Any, client: Any, install: asyncpg.Record,
) -> list[dict[str, Any]]:
    """Page the recent conversation list and persist Graph-authoritative ids."""
    if not hasattr(client, "list_conversations"):
        return []
    try:
        max_pages = max(1, min(100, int(os.environ.get("INSTAGRAM_DISCOVERY_MAX_PAGES", "10"))))
    except ValueError:
        max_pages = 10
    after: str | None = None
    conversations: list[dict[str, Any]] = []
    for _ in range(max_pages):
        page, after = await client.list_conversations(
            ig_business_account_id=str(install["ig_business_account_id"]),
            limit=50,
            after=after,
        )
        conversations.extend(item for item in page if isinstance(item, dict))
        if not after:
            break
    if conversations:
        await upsert_discovered_conversations(
            pool,
            tenant_id=install["tenant_id"],
            installation_id=install["id"],
            ig_business_account_id=str(install["ig_business_account_id"]),
            conversations=conversations,
            page_id=install["page_id"] if "page_id" in install else None,
            webhook_delivery_account_id=(
                install["webhook_delivery_account_id"]
                if "webhook_delivery_account_id" in install
                else None
            ),
        )
    return conversations


async def _load_high_water(pool: Any, shard_id: Any) -> str | None:
    state = await load_state(pool, "shard_fetch", str(shard_id))
    if state is None or not state.state_data:
        return None
    cursor = state.state_data.get("cursor")
    if isinstance(cursor, dict):
        high_water = cursor.get("high_water_message_id")
        return str(high_water) if high_water else None
    return None


async def _check_one_shard(
    *, pool: Any, client: Any, shard: asyncpg.Record,
) -> ResharedShard | None:
    identifier = _decode_identifier(shard["shard_identifier"])
    if identifier.get("shard_kind") != SHARD_KIND_CONVERSATION_HISTORY:
        return None
    conversation_id = _provider_conversation_id(identifier)
    if not conversation_id:
        return None
    high_water = await _load_high_water(pool, shard["id"])
    if not high_water:
        return None
    try:
        messages, _next = await client.list_conversation_messages(
            conversation_id=conversation_id,
            limit=1,
        )
    except Exception:  # noqa: BLE001
        return None
    newest = None
    if messages:
        newest = str(messages[0].get("id") or messages[0].get("mid") or "")
    if not newest or newest == high_water:
        return None
    gap_identifier = dict(identifier)
    gap_identifier["parent_shard_id"] = str(shard["id"])
    gap_identifier["gap_baseline_message_id"] = high_water
    gap_identifier["messages_cursor"] = None
    gap_identifier["ingress_kind"] = "poll"
    return ResharedShard(
        shard=Shard(
            shard_kind=SHARD_KIND_CONVERSATION_HISTORY,
            shard_identifier=gap_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )


_LOAD_INSTAGRAM_INSTALLS_SQL = """
SELECT ii.id, ii.tenant_id, ii.base_url, ii.ig_business_account_id, ii.page_id,
       ii.access_token_ref, ii.history_lookback_days, ii.connection_status,
       ii.disabled_at, iwr.webhook_delivery_account_id
  FROM instagram_installations ii
  LEFT JOIN LATERAL (
      SELECT webhook_delivery_account_id
        FROM instagram_webhook_routes
       WHERE instagram_installation_id = ii.id AND enabled = TRUE
       ORDER BY updated_at DESC
       LIMIT 1
  ) iwr ON TRUE
 WHERE ii.tenant_id = $1 AND ii.disabled_at IS NULL AND ii.connection_status = 'active'
"""


def _select_install_for_shard(
    identifier: dict[str, Any],
    *,
    installs_by_id: dict[str, asyncpg.Record],
    installs_by_account: dict[str, asyncpg.Record],
) -> asyncpg.Record | None:
    install_id = str(identifier.get("installation_id") or "")
    if install_id and install_id in installs_by_id:
        return installs_by_id[install_id]
    account_id = str(identifier.get("ig_business_account_id") or "")
    if account_id and account_id in installs_by_account:
        return installs_by_account[account_id]
    if len(installs_by_id) == 1:
        return next(iter(installs_by_id.values()))
    return None


async def reconcile_instagram(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    installs = await pool.fetch(_LOAD_INSTAGRAM_INSTALLS_SQL, run["tenant_id"])
    if not installs:
        return ReconciliationDecision(has_gaps=False)

    installs_by_id = {str(row["id"]): row for row in installs}
    installs_by_account = {
        str(row["ig_business_account_id"]): row
        for row in installs
        if row["ig_business_account_id"] is not None
    }
    grouped: dict[str, tuple[asyncpg.Record, list[asyncpg.Record]]] = {}
    for shard in active:
        identifier = _decode_identifier(shard["shard_identifier"])
        install = _select_install_for_shard(
            identifier,
            installs_by_id=installs_by_id,
            installs_by_account=installs_by_account,
        )
        if install is None:
            continue
        key = str(install["id"])
        if key not in grouped:
            grouped[key] = (install, [])
        grouped[key][1].append(shard)

    new_shards: list[ResharedShard] = []
    for install, install_shards in grouped.values():
        client, close = await _open_instagram_client(install)
        try:
            discovered = await _discover_conversations(
                pool=pool,
                client=client,
                install=install,
            )
            known_conversations = {
                _provider_conversation_id(_decode_identifier(shard["shard_identifier"]))
                for shard in shards
                if str(_decode_identifier(shard["shard_identifier"]).get("installation_id") or "")
                == str(install["id"])
            }
            # A newly discovered customer thread has no completed shard yet.
            # Re-share from a finished shard solely to use the workflow's
            # established parent-link and dispatch semantics.
            anchor = install_shards[0]
            for conversation in discovered:
                provider_id = str(conversation.get("id") or "").strip()
                if not provider_id or provider_id in known_conversations:
                    continue
                identifier = _conversation_identifier(
                    install=install,
                    conversation=conversation,
                    parent_shard_id=anchor["id"],
                )
                if identifier is not None:
                    new_shards.append(ResharedShard(
                        shard=Shard(
                            shard_kind=SHARD_KIND_CONVERSATION_HISTORY,
                            shard_identifier=identifier,
                            recency_score=RESHARE_RECENCY_SCORE,
                        ),
                        parent_shard_id=anchor["id"],
                    ))
            for shard in install_shards:
                reshared = await _check_one_shard(
                    pool=pool,
                    client=client,
                    shard=shard,
                )
                if reshared is not None:
                    new_shards.append(reshared)
        finally:
            await close()

    if new_shards:
        return ReconciliationDecision(
            has_gaps=True,
            new_shards=new_shards,
            message=f"instagram reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)


RECONCILER_DISPATCH["instagram"] = reconcile_instagram


__all__ = [
    "RESHARE_RECENCY_SCORE",
    "SHARD_KIND_CONVERSATION_HISTORY",
    "reconcile_instagram",
    "set_pool_provider",
]
