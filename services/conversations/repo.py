"""services.conversations.repo — DB layer for per-card conversations.

Owns CRUD over `card_conversations` and `card_exchanges` (migration
0024). The handler in `.handler` is the only consumer; the router in
`.api` projects these dataclasses onto the wire shape.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7


@dataclass
class CardExchange:
    id: UUID
    conversation_id: UUID
    probe_kind: str  # "phrase" | "chip" | "ask"
    probe_id: Optional[str]
    probe_action: str
    probe_text: str
    response_html: str
    follow_ups: list[dict[str, str]]
    created_at: datetime
    latency_ms: Optional[int] = None

    def to_wire(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "conversation_id": str(self.conversation_id),
            "probe_kind": self.probe_kind,
            "probe_id": self.probe_id,
            "probe_action": self.probe_action,
            "probe_text": self.probe_text,
            "response_html": self.response_html,
            "follow_ups": self.follow_ups,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class CardConversation:
    id: UUID
    tenant_id: UUID
    actor_id: UUID
    card_id: UUID
    created_at: datetime
    last_probed_at: Optional[datetime]
    archived_at: Optional[datetime]
    archive_reason: Optional[str]
    probed_phrase_ids: list[str] = field(default_factory=list)
    used_chip_ids: list[str] = field(default_factory=list)

    def to_wire(self, exchanges: list[CardExchange]) -> dict[str, Any]:
        return {
            "conversation_id": str(self.id),
            "card_id": str(self.card_id),
            "exchanges": [ex.to_wire() for ex in exchanges],
            "probed_phrase_ids": list(self.probed_phrase_ids),
            "used_chip_ids": list(self.used_chip_ids),
            "last_probed_at": (
                self.last_probed_at.isoformat() if self.last_probed_at else None
            ),
            "archived": self.archived_at is not None,
        }


def _decode_json_array(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v]
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    return []


def _decode_json_obj_list(v: Any) -> list[dict[str, str]]:
    if v is None:
        return []
    if isinstance(v, list):
        return [dict(x) for x in v if isinstance(x, dict)]
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [dict(x) for x in parsed if isinstance(x, dict)]
    return []


class ConversationRepo:
    """Thin wrapper over the gateway pool. All methods take an
    explicit (tenant_id, actor_id) so cross-tenant access is impossible
    by construction."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_or_create(
        self, *, tenant_id: UUID, actor_id: UUID, card_id: UUID,
    ) -> CardConversation:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, tenant_id, actor_id, card_id, created_at,
                       last_probed_at, archived_at, archive_reason,
                       probed_phrase_ids, used_chip_ids
                FROM card_conversations
                WHERE tenant_id = $1 AND actor_id = $2 AND card_id = $3
                """,
                tenant_id, actor_id, card_id,
            )
            if row is not None:
                return _row_to_conv(row)
            new_id = uuid7()
            row = await conn.fetchrow(
                """
                INSERT INTO card_conversations
                  (id, tenant_id, actor_id, card_id)
                VALUES ($1, $2, $3, $4)
                RETURNING id, tenant_id, actor_id, card_id, created_at,
                          last_probed_at, archived_at, archive_reason,
                          probed_phrase_ids, used_chip_ids
                """,
                new_id, tenant_id, actor_id, card_id,
            )
            return _row_to_conv(row)

    async def fetch(
        self, *, tenant_id: UUID, actor_id: UUID, card_id: UUID,
    ) -> Optional[CardConversation]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, tenant_id, actor_id, card_id, created_at,
                       last_probed_at, archived_at, archive_reason,
                       probed_phrase_ids, used_chip_ids
                FROM card_conversations
                WHERE tenant_id = $1 AND actor_id = $2 AND card_id = $3
                """,
                tenant_id, actor_id, card_id,
            )
            return _row_to_conv(row) if row else None

    async def list_exchanges(
        self, *, conversation_id: UUID,
    ) -> list[CardExchange]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, conversation_id, probe_kind, probe_id,
                       probe_action, probe_text, response_html,
                       follow_ups, created_at, latency_ms
                FROM card_exchanges
                WHERE conversation_id = $1
                ORDER BY created_at ASC
                """,
                conversation_id,
            )
            return [_row_to_exchange(r) for r in rows]

    async def append_exchange(
        self,
        *,
        conversation: CardConversation,
        probe_kind: str,
        probe_id: Optional[str],
        probe_action: str,
        probe_text: str,
        response_html: str,
        follow_ups: list[dict[str, str]],
        latency_ms: Optional[int] = None,
    ) -> CardExchange:
        if conversation.archived_at is not None:
            raise ConversationArchivedError(conversation.id)
        ex_id = uuid7()
        now = datetime.now(timezone.utc)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO card_exchanges
                      (id, conversation_id, tenant_id, probe_kind, probe_id,
                       probe_action, probe_text, response_html, follow_ups,
                       created_at, latency_ms)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11)
                    RETURNING id, conversation_id, probe_kind, probe_id,
                              probe_action, probe_text, response_html,
                              follow_ups, created_at, latency_ms
                    """,
                    ex_id, conversation.id, conversation.tenant_id,
                    probe_kind, probe_id, probe_action, probe_text,
                    response_html, json.dumps(follow_ups), now, latency_ms,
                )
                # Update probed/used trackers + last_probed_at.
                probed = list(conversation.probed_phrase_ids)
                used = list(conversation.used_chip_ids)
                if probe_kind == "phrase" and probe_id and probe_id not in probed:
                    probed.append(probe_id)
                if probe_kind == "chip" and probe_id and probe_id not in used:
                    used.append(probe_id)
                await conn.execute(
                    """
                    UPDATE card_conversations
                    SET last_probed_at = $2,
                        probed_phrase_ids = $3::jsonb,
                        used_chip_ids = $4::jsonb
                    WHERE id = $1
                    """,
                    conversation.id, now, json.dumps(probed), json.dumps(used),
                )
                conversation.last_probed_at = now
                conversation.probed_phrase_ids = probed
                conversation.used_chip_ids = used
        return _row_to_exchange(row)

    async def clear(
        self, *, tenant_id: UUID, actor_id: UUID, card_id: UUID,
    ) -> bool:
        """Remove all exchanges and reset the trackers. The
        conversation row stays so a fresh start re-uses the same id.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT id FROM card_conversations
                    WHERE tenant_id = $1 AND actor_id = $2 AND card_id = $3
                    """,
                    tenant_id, actor_id, card_id,
                )
                if row is None:
                    return False
                await conn.execute(
                    "DELETE FROM card_exchanges WHERE conversation_id = $1",
                    row["id"],
                )
                await conn.execute(
                    """
                    UPDATE card_conversations
                    SET probed_phrase_ids = '[]'::jsonb,
                        used_chip_ids = '[]'::jsonb,
                        last_probed_at = NULL
                    WHERE id = $1
                    """,
                    row["id"],
                )
                return True

    async def archive(
        self, *, tenant_id: UUID, actor_id: UUID, card_id: UUID, reason: str,
    ) -> None:
        """Mark the conversation read-only. Called by the triage handler
        when a card is acted/held/dismissed. Idempotent: re-archiving a
        conversation does not overwrite the original reason."""
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE card_conversations
                SET archived_at = COALESCE(archived_at, now()),
                    archive_reason = COALESCE(archive_reason, $4)
                WHERE tenant_id = $1 AND actor_id = $2 AND card_id = $3
                """,
                tenant_id, actor_id, card_id, reason,
            )


class ConversationArchivedError(Exception):
    def __init__(self, conversation_id: UUID):
        super().__init__(f"conversation {conversation_id} is archived")
        self.conversation_id = conversation_id


def _row_to_conv(row: Any) -> CardConversation:
    return CardConversation(
        id=row["id"],
        tenant_id=row["tenant_id"],
        actor_id=row["actor_id"],
        card_id=row["card_id"],
        created_at=row["created_at"],
        last_probed_at=row["last_probed_at"],
        archived_at=row["archived_at"],
        archive_reason=row["archive_reason"],
        probed_phrase_ids=_decode_json_array(row["probed_phrase_ids"]),
        used_chip_ids=_decode_json_array(row["used_chip_ids"]),
    )


def _row_to_exchange(row: Any) -> CardExchange:
    return CardExchange(
        id=row["id"],
        conversation_id=row["conversation_id"],
        probe_kind=row["probe_kind"],
        probe_id=row["probe_id"],
        probe_action=row["probe_action"],
        probe_text=row["probe_text"],
        response_html=row["response_html"],
        follow_ups=_decode_json_obj_list(row["follow_ups"]),
        created_at=row["created_at"],
        latency_ms=row["latency_ms"],
    )
