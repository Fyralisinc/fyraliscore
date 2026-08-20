"""Serialized, append-only episode lifecycle transitions."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7


EpisodeState = Literal["open", "dormant", "settled", "reopened", "superseded"]
EventKind = Literal["opened", "dormant", "settled", "reopened", "superseded", "split", "merged"]

_ALLOWED: dict[str | None, set[str]] = {
    None: {"open"},
    "open": {"dormant", "settled", "superseded"},
    "dormant": {"open", "settled", "superseded"},
    "settled": {"reopened", "superseded"},
    "reopened": {"dormant", "settled", "superseded"},
    "superseded": set(),
}


class EpisodeLifecycleEventRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: UUID
    tenant_id: UUID
    episode_id: UUID
    event_kind: EventKind
    from_state: EpisodeState | None
    to_state: EpisodeState
    event_time_watermark: datetime
    ingestion_time_watermark: datetime
    rule_name: str
    rule_version: str
    cause_ref: dict[str, Any]
    transition_key: str
    created_at: datetime


_COLUMNS = (
    "id", "tenant_id", "episode_id", "event_kind", "from_state", "to_state",
    "event_time_watermark", "ingestion_time_watermark", "rule_name",
    "rule_version", "cause_ref", "transition_key", "created_at",
)


def _event(row: asyncpg.Record) -> EpisodeLifecycleEventRow:
    value = dict(row)
    if isinstance(value["cause_ref"], str):
        value["cause_ref"] = json.loads(value["cause_ref"])
    return EpisodeLifecycleEventRow.model_validate(value)


class EpisodeLifecycleRepository:
    async def transition(
        self,
        episode_id: UUID,
        *,
        tenant_id: UUID,
        to_state: EpisodeState,
        event_kind: EventKind,
        event_time_watermark: datetime,
        ingestion_time_watermark: datetime,
        rule_name: str,
        rule_version: str,
        cause_ref: dict[str, Any],
        conn: asyncpg.Connection,
    ) -> EpisodeLifecycleEventRow:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"episode-lifecycle:{tenant_id}:{episode_id}",
        )
        episode = await conn.fetchrow(
            "SELECT lifecycle_state FROM episodes WHERE id=$1 AND tenant_id=$2 FOR UPDATE",
            episode_id, tenant_id,
        )
        if episode is None:
            raise ValidationError("episode not found")
        from_state = str(episode["lifecycle_state"])
        has_events = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM episode_lifecycle_events "
            "WHERE tenant_id=$1 AND episode_id=$2)",
            tenant_id, episode_id,
        )
        if not has_events and from_state == "open" and to_state == "open":
            transition_from: str | None = None
        else:
            transition_from = from_state
            if to_state not in _ALLOWED.get(from_state, set()):
                raise ValidationError(
                    f"invalid episode transition {from_state!r} -> {to_state!r}"
                )
        semantic = {
            "tenant_id": str(tenant_id), "episode_id": str(episode_id),
            "event_kind": event_kind, "from_state": transition_from,
            "to_state": to_state,
            "event_time_watermark": event_time_watermark.isoformat(),
            "ingestion_time_watermark": ingestion_time_watermark.isoformat(),
            "rule_name": rule_name, "rule_version": rule_version,
            "cause_ref": cause_ref,
        }
        key = hashlib.sha256(
            json.dumps(semantic, sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()
        row = await conn.fetchrow(
            f"""
            INSERT INTO episode_lifecycle_events (
              id, tenant_id, episode_id, event_kind, from_state, to_state,
              event_time_watermark, ingestion_time_watermark, rule_name,
              rule_version, cause_ref, transition_key
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12)
            ON CONFLICT (tenant_id, transition_key) DO NOTHING
            RETURNING {', '.join(_COLUMNS)}
            """,
            uuid7(), tenant_id, episode_id, event_kind, transition_from, to_state,
            event_time_watermark, ingestion_time_watermark, rule_name, rule_version,
            json.dumps(cause_ref, sort_keys=True), key,
        )
        if row is None:
            row = await conn.fetchrow(
                f"SELECT {', '.join(_COLUMNS)} FROM episode_lifecycle_events "
                "WHERE tenant_id=$1 AND transition_key=$2",
                tenant_id, key,
            )
        assert row is not None
        await conn.execute(
            "UPDATE episodes SET lifecycle_state=$3, updated_at=now() "
            "WHERE id=$1 AND tenant_id=$2",
            episode_id, tenant_id, to_state,
        )
        return _event(row)

    async def history(
        self, episode_id: UUID, *, tenant_id: UUID, conn: asyncpg.Connection
    ) -> list[EpisodeLifecycleEventRow]:
        rows = await conn.fetch(
            f"SELECT {', '.join(_COLUMNS)} FROM episode_lifecycle_events "
            "WHERE tenant_id=$1 AND episode_id=$2 ORDER BY created_at,id",
            tenant_id, episode_id,
        )
        return [_event(row) for row in rows]


__all__ = ["EpisodeLifecycleEventRow", "EpisodeLifecycleRepository"]
