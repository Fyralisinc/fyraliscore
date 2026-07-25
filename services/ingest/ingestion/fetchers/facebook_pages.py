"""Facebook Pages Graph pagination fetcher."""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from services.ingest.ingestion.fetchers import FetchResult


log = logging.getLogger(__name__)

SHARD_KIND_PAGE_HISTORY = "facebook_page_history"
_DEFAULT_PAGE_SIZE = 100


def _page_size() -> int:
    try:
        return min(
            100,
            max(1, int(os.environ.get("FACEBOOK_PAGES_BACKFILL_PAGE_SIZE", "100"))),
        )
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class FacebookPagesCursor(BaseModel):
    """Nested cursor for one Page.

    `conversation_after` resumes Page conversation listing, while
    `current_conversation_id` + `message_after` resume the current conversation's
    message pagination.
    """

    model_config = ConfigDict(extra="forbid")

    seeded: bool = False
    conversation_after: str | None = None
    conversation_listing_exhausted: bool = False
    pending_conversations: list[dict[str, Any]] = Field(default_factory=list)
    current_conversation: dict[str, Any] | None = None
    message_after: str | None = None
    current_conversation_messages_seen: int = 0
    current_conversation_oldest_message_at: str | None = None
    current_conversation_newest_message_at: str | None = None
    conversation_count: int = 0
    message_count: int = 0
    oldest_message_at: str | None = None
    exhausted_reason: str | None = None


def _decode_cursor(raw: dict[str, Any] | None) -> FacebookPagesCursor:
    if raw is None:
        return FacebookPagesCursor()
    return FacebookPagesCursor.model_validate(raw)


def _encode_cursor(cursor: FacebookPagesCursor) -> dict[str, Any]:
    return cursor.model_dump(mode="json")


async def _open_facebook_pages_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_facebook_pages_client

    return await open_facebook_pages_client(install)


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
        except ValueError:
            return None


def _bump_message_coverage(cursor: FacebookPagesCursor, message: dict[str, Any]) -> None:
    ts = _parse_ts(message.get("created_time"))
    if ts is None:
        return
    ts_iso = ts.isoformat()
    oldest = _parse_ts(cursor.oldest_message_at)
    if oldest is None or ts < oldest:
        cursor.oldest_message_at = ts_iso
    conversation_oldest = _parse_ts(cursor.current_conversation_oldest_message_at)
    if conversation_oldest is None or ts < conversation_oldest:
        cursor.current_conversation_oldest_message_at = ts_iso
    conversation_newest = _parse_ts(cursor.current_conversation_newest_message_at)
    if conversation_newest is None or ts > conversation_newest:
        cursor.current_conversation_newest_message_at = ts_iso


def _record_for_message(
    *,
    page_id: str,
    page_name: str | None,
    conversation_id: str,
    message: dict[str, Any],
) -> dict[str, Any]:
    return {
        "source": "backfill",
        "page_id": page_id,
        "page_name": page_name,
        "conversation_id": conversation_id,
        "id": message.get("id"),
        "created_time": message.get("created_time"),
        "message": {
            "id": message.get("id"),
            "message": message.get("message"),
            "text": message.get("message"),
            "from": message.get("from"),
            "to": message.get("to"),
            "attachments": message.get("attachments"),
            "shares": message.get("shares"),
            "sticker": message.get("sticker"),
        },
    }


async def _call_optional(obj: Any, method: str, **kwargs: Any) -> None:
    fn = getattr(obj, method, None)
    if fn is None:
        return
    await fn(**kwargs)


async def _finish_current_conversation(
    client: Any,
    cursor: FacebookPagesCursor,
    *,
    installation_id: UUID,
    reason: str,
) -> None:
    conversation = cursor.current_conversation or {}
    conversation_id = conversation.get("id")
    if isinstance(conversation_id, str) and conversation_id:
        await _call_optional(
            client,
            "mark_conversation_exhausted",
            installation_id=installation_id,
            conversation_id=conversation_id,
            oldest_message_at=_parse_ts(cursor.current_conversation_oldest_message_at),
            newest_message_at=_parse_ts(cursor.current_conversation_newest_message_at),
            message_count=cursor.current_conversation_messages_seen,
            reason=reason,
        )
        cursor.conversation_count += 1
    cursor.current_conversation = None
    cursor.message_after = None
    cursor.current_conversation_messages_seen = 0
    cursor.current_conversation_oldest_message_at = None
    cursor.current_conversation_newest_message_at = None


async def fetch_page_facebook_pages(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    if shard_identifier.get("shard_kind") != SHARD_KIND_PAGE_HISTORY:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)
    page_id = shard_identifier.get("page_id")
    if not isinstance(page_id, str) or not page_id:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    installation_id = UUID(str(shard_identifier.get("installation_id") or install["id"]))
    tenant_id = install["tenant_id"]
    page_name = shard_identifier.get("page_name")
    if not isinstance(page_name, str):
        page_name = install["page_name"] if "page_name" in install else None

    cur = _decode_cursor(cursor)
    cur.seeded = True
    page_size = _page_size()
    client, close = await _open_facebook_pages_client(install)
    try:
        if cur.current_conversation is None:
            if not cur.pending_conversations:
                if cur.conversation_listing_exhausted:
                    cur.exhausted_reason = (
                        "all_available_history_graph_pagination_exhausted"
                    )
                    return FetchResult(
                        records=[],
                        next_cursor=_encode_cursor(cur),
                        end_of_data=True,
                    )
                conversations, next_after = await client.list_conversations(
                    page_id=page_id,
                    after=cur.conversation_after,
                    limit=page_size,
                )
                cur.conversation_after = next_after
                cur.conversation_listing_exhausted = next_after is None
                for conversation in conversations:
                    await _call_optional(
                        client,
                        "upsert_conversation_state",
                        installation_id=installation_id,
                        tenant_id=tenant_id,
                        page_id=page_id,
                        conversation=conversation,
                    )
                cur.pending_conversations = conversations
                if not cur.pending_conversations and cur.conversation_listing_exhausted:
                    cur.exhausted_reason = (
                        "all_available_history_graph_pagination_exhausted"
                    )
                    return FetchResult(
                        records=[],
                        next_cursor=_encode_cursor(cur),
                        end_of_data=True,
                    )
                return FetchResult(
                    records=[],
                    next_cursor=_encode_cursor(cur),
                    end_of_data=False,
                )
            cur.current_conversation = cur.pending_conversations.pop(0)
            cur.message_after = None
            cur.current_conversation_messages_seen = 0
            cur.current_conversation_oldest_message_at = None
            cur.current_conversation_newest_message_at = None

        conversation_id = (cur.current_conversation or {}).get("id")
        if not isinstance(conversation_id, str) or not conversation_id:
            await _finish_current_conversation(
                client,
                cur,
                installation_id=installation_id,
                reason="conversation_missing_id",
            )
            return FetchResult(
                records=[],
                next_cursor=_encode_cursor(cur),
                end_of_data=False,
            )

        messages, next_after = await client.list_messages(
            conversation_id=conversation_id,
            after=cur.message_after,
            limit=page_size,
        )
        records: list[dict[str, Any]] = []
        for message in messages:
            mid = message.get("id")
            if not isinstance(mid, str) or not mid:
                continue
            records.append(
                _record_for_message(
                    page_id=page_id,
                    page_name=page_name,
                    conversation_id=conversation_id,
                    message=message,
                )
            )
            cur.message_count += 1
            cur.current_conversation_messages_seen += 1
            _bump_message_coverage(cur, message)

        cur.message_after = next_after
        if next_after is None:
            await _finish_current_conversation(
                client,
                cur,
                installation_id=installation_id,
                reason="graph_message_pagination_exhausted",
            )

        log.info(
            "facebook_pages_backfill_page",
            extra={
                "page_id": page_id,
                "conversation_id": conversation_id,
                "records": len(records),
                "next_after": bool(next_after),
            },
        )
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=False,
        )
    finally:
        await close()




__all__ = [
    "FacebookPagesCursor",
    "SHARD_KIND_PAGE_HISTORY",
    "fetch_page_facebook_pages",
]
