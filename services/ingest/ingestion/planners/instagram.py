"""Instagram conversation-history planner."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import orjson

from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.planners.context import PlannerContext
from services.ingest.integrations.instagram.records import business_endpoint_ids


SHARD_KIND_CONVERSATION_HISTORY = "instagram_conversation_history"


def _decode_conversations(install: Any) -> list[dict[str, Any]]:
    raw = install["conversations"] if "conversations" in install else None
    if raw is None:
        return []
    if isinstance(raw, (str, bytes)):
        try:
            decoded = orjson.loads(raw)
        except orjson.JSONDecodeError:
            return []
    elif isinstance(raw, list):
        decoded = raw
    else:
        return []
    return [item for item in decoded if isinstance(item, dict)]


def _parse_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _recency_score(conversation: dict[str, Any]) -> float:
    last = _parse_time(
        conversation.get("last_message_at") or conversation.get("updated_time")
    )
    if last is None:
        return 1.0
    age_days = max(0.0, (datetime.now(timezone.utc) - last).total_seconds() / 86400.0)
    return max(0.25, min(2.0, 1.5 / (1.0 + age_days / 14.0)))


async def _discover_conversations(ctx: PlannerContext) -> list[dict[str, Any]]:
    if ctx.source_client is None:
        return []
    try:
        max_pages = max(
            1,
            min(100, int(os.environ.get("INSTAGRAM_PLAN_DISCOVERY_MAX_PAGES", "20"))),
        )
    except ValueError:
        max_pages = 20
    account_id = str(ctx.install["ig_business_account_id"])
    after: str | None = None
    conversations: list[dict[str, Any]] = []
    for _ in range(max_pages):
        page, after = await ctx.source_client.list_conversations(
            ig_business_account_id=account_id,
            limit=50,
            after=after,
        )
        conversations.extend(item for item in page if isinstance(item, dict))
        if not after:
            break
    return conversations


def _participant_details(
    conversation: dict[str, Any], *, business_ids: frozenset[str],
) -> tuple[str | None, str | None, str | None]:
    participants = conversation.get("participants")
    data = participants.get("data") if isinstance(participants, dict) else participants
    if not isinstance(data, list):
        return None, None, None
    for participant in data:
        if (
            isinstance(participant, dict)
            and str(participant.get("id") or "") not in business_ids
        ):
            participant_id = str(participant.get("id") or "").strip() or None
            return participant_id, participant.get("username"), participant.get("name")
    return None, None, None


async def plan_shards_instagram(ctx: PlannerContext) -> list[Shard]:
    install_id = str(ctx.install["id"])
    ig_business_account_id = str(ctx.install["ig_business_account_id"])
    page_id = ctx.install["page_id"] if "page_id" in ctx.install else None
    webhook_delivery_account_id = (
        ctx.install["webhook_delivery_account_id"]
        if "webhook_delivery_account_id" in ctx.install
        else None
    )
    business_ids = business_endpoint_ids(
        ig_business_account_id,
        page_id,
        webhook_delivery_account_id,
    )
    lookback_raw = (
        ctx.install["history_lookback_days"]
        if "history_lookback_days" in ctx.install else 90
    )
    lookback_days = int(lookback_raw or 90)
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, lookback_days))
    known: dict[str, dict[str, Any]] = {}
    for conv in _decode_conversations(ctx.install):
        provider_id = str(conv.get("provider_conversation_id") or "").strip()
        if provider_id:
            known[provider_id] = conv
    # Source onboarding runs on install and on a coalesced new-thread replay.
    # Querying Graph here keeps the first customer after an empty install from
    # waiting for an already-existing shard before recovery can begin.
    for conv in await _discover_conversations(ctx):
        provider_id = str(conv.get("id") or "").strip()
        if provider_id:
            participant_id, username, display_name = _participant_details(
                conv,
                business_ids=business_ids,
            )
            known[provider_id] = {
                **conv,
                "provider_conversation_id": provider_id,
                "thread_key": f"{ig_business_account_id}:{participant_id or provider_id}",
                "participant_id": participant_id,
                "participant_username": username,
                "participant_display_name": display_name,
                "last_message_at": conv.get("updated_time"),
            }

    shards: list[Shard] = []
    for conv in known.values():
        # A webhook only has enough information to form a local thread key.
        # Fetching history requires the opaque Meta Conversations API id.
        provider_conversation_id = str(
            conv.get("provider_conversation_id") or ""
        ).strip()
        if not provider_conversation_id:
            continue
        last_message_at = _parse_time(
            conv.get("last_message_at") or conv.get("updated_time")
        )
        if last_message_at is not None and last_message_at < cutoff:
            continue
        shards.append(Shard(
            shard_kind=SHARD_KIND_CONVERSATION_HISTORY,
            shard_identifier={
                "shard_kind": SHARD_KIND_CONVERSATION_HISTORY,
                "installation_id": install_id,
                "ig_business_account_id": ig_business_account_id,
                "page_id": page_id,
                "webhook_delivery_account_id": webhook_delivery_account_id,
                "provider_conversation_id": provider_conversation_id,
                "thread_key": conv.get("thread_key") or conv.get("conversation_id"),
                "participant_id": conv.get("participant_id"),
                "participant_username": conv.get("participant_username"),
                "participant_display_name": conv.get("participant_display_name"),
                "messages_cursor": conv.get("messages_cursor"),
                "high_water_message_id": conv.get("high_water_message_id"),
            },
            recency_score=_recency_score(conv),
        ))
    return shards


PLANNER_DISPATCH["instagram"] = plan_shards_instagram


__all__ = ["SHARD_KIND_CONVERSATION_HISTORY", "plan_shards_instagram"]
