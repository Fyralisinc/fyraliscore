"""Miro poll-change dispatch through the canonical ingestion pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import orjson
import structlog

from services.ingest.ingestion.core import ingest
from services.ingest.ingestion.shadow_write import shadow_write_raw


log = structlog.get_logger("integrations.miro.poll")
CHANNEL = "miro:item"


@dataclass
class PollDeps:
    pool: asyncpg.Pool
    tenant_id: UUID
    installation_id: str
    org_id: str
    board_id: str
    actor_repo: Any = None
    alias_repo: Any = None
    embedder: Any = None
    s3_raw_client: Any = None
    kafka_producer: Any = None
    tenant_flags: Any = None


def build_change_record(
    item: dict[str, Any], *, org_id: str, board_id: str,
) -> dict[str, Any] | None:
    if not isinstance(item, dict) or not item.get("id"):
        return None
    return {
        "_fyralis_record_type": "item",
        "_fyralis_board_id": board_id,
        "_fyralis_org_id": org_id,
        "item": item,
    }


async def _attempt_cutover(deps: PollDeps, record: dict[str, Any]) -> bool:
    try:
        await shadow_write_raw(
            tenant_id=deps.tenant_id,
            source="miro",
            ingress_kind="poll",
            raw_body=orjson.dumps(record, option=orjson.OPT_SORT_KEYS),
            s3_client=deps.s3_raw_client,
            kafka_producer=deps.kafka_producer,
            ingress_metadata={
                "event_type": "poll_change",
                "board_id": deps.board_id,
                "item_id": (record.get("item") or {}).get("id"),
            },
        )
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("miro_poll.cutover_failed", error=str(exc)[:200])
        return False


async def handle_polled_change(item: dict[str, Any], deps: PollDeps) -> None:
    record = build_change_record(
        item,
        org_id=deps.org_id,
        board_id=deps.board_id,
    )
    if record is None:
        return
    kafka_enabled = False
    if (
        deps.tenant_flags is not None
        and deps.kafka_producer is not None
        and deps.s3_raw_client is not None
    ):
        kafka_enabled = await deps.tenant_flags.kafka_path_enabled(
            deps.tenant_id,
        )
    if kafka_enabled and await _attempt_cutover(deps, record):
        return
    if kafka_enabled:
        log.warning("miro_poll.kafka_path_fallback_to_inline")
    await ingest(
        CHANNEL,
        record,
        pool=deps.pool,
        tenant_id=deps.tenant_id,
        actor_repo=deps.actor_repo,
        alias_repo=deps.alias_repo,
        embedder=deps.embedder,
    )


__all__ = ["CHANNEL", "PollDeps", "build_change_record", "handle_polled_change"]
