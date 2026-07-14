"""Instagram conversation-message backfill fetcher."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import InstagramApiError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingest.integrations.instagram.records import build_history_record


SHARD_KIND_CONVERSATION_HISTORY = "instagram_conversation_history"
_DEFAULT_PAGE_SIZE = 50


def _page_size() -> int:
    try:
        return max(1, min(100, int(os.environ.get("INSTAGRAM_BACKFILL_PAGE_SIZE", "50"))))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class InstagramCursor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    after: str | None = None
    high_water_message_id: str | None = None
    stop_at_message_id: str | None = None
    messages_seen: int = 0


def _decode_cursor(raw: dict[str, Any] | None) -> InstagramCursor:
    return InstagramCursor.model_validate(raw or {})


def _encode_cursor(cursor: InstagramCursor) -> dict[str, Any]:
    return cursor.model_dump(mode="json")


async def _open_instagram_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_instagram_client
    return await open_instagram_client(install)


async def fetch_page_instagram(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    if shard_identifier.get("shard_kind") != SHARD_KIND_CONVERSATION_HISTORY:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    provider_conversation_id = str(
        shard_identifier.get("provider_conversation_id")
        or shard_identifier.get("conversation_id")
        or ""
    ).strip()
    ig_business_account_id = str(
        shard_identifier.get("ig_business_account_id")
        or (install["ig_business_account_id"] if "ig_business_account_id" in install else "")
    )
    if not provider_conversation_id or not ig_business_account_id:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)
    webhook_delivery_account_id = str(
        shard_identifier.get("webhook_delivery_account_id")
        or (
            install["webhook_delivery_account_id"]
            if "webhook_delivery_account_id" in install
            else ""
        )
        or ""
    ).strip() or None

    cur = _decode_cursor(cursor)
    if cur.after is None and isinstance(shard_identifier.get("messages_cursor"), str):
        cur.after = shard_identifier.get("messages_cursor")
    if cur.stop_at_message_id is None:
        baseline = shard_identifier.get("gap_baseline_message_id")
        cur.stop_at_message_id = str(baseline) if baseline else None

    lookback_days = int(install["history_lookback_days"] or 90) if "history_lookback_days" in install else 90
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, lookback_days))

    client, close = await _open_instagram_client(install)
    try:
        try:
            messages, next_after = await client.list_conversation_messages(
                conversation_id=provider_conversation_id,
                limit=_page_size(),
                after=cur.after,
            )
        except InstagramApiError as exc:
            if getattr(exc, "code", "") == "instagram_api_rate_limited":
                return FetchResult(
                    records=[],
                    next_cursor=_encode_cursor(cur),
                    end_of_data=False,
                )
            raise

        records: list[dict[str, Any]] = []
        stop_reached = False
        for message in messages:
            if not isinstance(message, dict):
                continue
            message_id = message.get("id") or message.get("mid")
            # Meta returns newest first. A poll re-walk stops at the previous
            # high-water mark, while the initial backfill is bounded by the
            # configured retention window.
            if cur.stop_at_message_id and str(message_id or "") == cur.stop_at_message_id:
                stop_reached = True
                break
            created_raw = message.get("created_time")
            if isinstance(created_raw, str):
                try:
                    created_at = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=timezone.utc)
                    if created_at < cutoff:
                        stop_reached = True
                        break
                except ValueError:
                    pass
            if cur.high_water_message_id is None:
                cur.high_water_message_id = str(message_id) if message_id else None
            records.append(build_history_record(
                message,
                ig_business_account_id=ig_business_account_id,
                page_id=shard_identifier.get("page_id"),
                conversation_id=provider_conversation_id,
                webhook_delivery_account_id=webhook_delivery_account_id,
                participant_id=shard_identifier.get("participant_id"),
                participant_username=shard_identifier.get("participant_username"),
                participant_display_name=shard_identifier.get("participant_display_name"),
            ))
        cur.messages_seen += len(records)
        cur.after = next_after
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=stop_reached or next_after is None,
        )
    finally:
        await close()


FETCHER_DISPATCH["instagram"] = fetch_page_instagram


__all__ = [
    "InstagramCursor",
    "SHARD_KIND_CONVERSATION_HISTORY",
    "fetch_page_instagram",
]
