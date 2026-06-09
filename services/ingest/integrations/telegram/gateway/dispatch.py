"""services/ingest/integrations/telegram/gateway/dispatch.py — live update dispatch.

The bridge between the persistent MTProto updates connection (worker.py) and the
ingestion pipeline. `handle_update` is called for every live message update the
connection delivers. It is the Discord-gateway `handle_message_create` analog,
with one simplification: a Telegram gateway worker holds ONE account's session =
ONE tenant's install, so the tenant is known by construction (carried on
`DispatchDeps`) — there is no per-update tenant resolution.

Flow (parallel to the webhook-router / discord-gateway cutover):
  1. Build the canonical message record (the SAME `build_message_record` the
     backfill fetcher uses → identical external_id → cross-path dedup).
  2. Cutover: if `ingestion.kafka_path_enabled` for the tenant (kafka-first
     default), shadow-write the record to `ingestion.raw.telegram`
     (`ingress_kind="gateway"`) and return — the normalizer + observation_writer
     produce the observation, concurrently with any in-flight backfill.
  3. Fallback / inline: otherwise `core.ingest("telegram:message", record, …)`,
     then a best-effort M2 shadow audit (when SHADOW_WRITE_ENABLED).

There is NO HMAC signature gate — the trust boundary is the authenticated MTProto
connection itself (as with Discord's gateway / Gmail Pub/Sub).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import orjson
import structlog

from services.ingest.ingestion.core import ingest
from services.ingest.ingestion.feature_flags import SHADOW_WRITE_ENABLED
from services.ingest.ingestion.shadow_write import shadow_write_raw
from services.ingest.integrations.telegram.records import (
    CHANNEL,
    build_message_record,
)


log = structlog.get_logger("integrations.telegram.gateway.dispatch")


@dataclass
class DispatchDeps:
    """Dependencies injected into every dispatch call. Built once at worker
    startup (bound to one install/tenant) and reused for the process lifetime.

    Shadow-path deps (s3_raw_client / kafka_producer / tenant_flags) are
    optional: when unwired the worker falls back to inline `ingest()`.
    """

    pool: asyncpg.Pool
    tenant_id: UUID
    installation_id: str
    actor_repo: Any = None
    alias_repo: Any = None
    embedder: Any = None
    s3_raw_client: Any = None    # raw_tier.s3.S3Client | None
    kafka_producer: Any = None   # kafka.IdempotentProducer | None
    tenant_flags: Any = None     # feature_flags.TenantFlags | None


def _update_to_record(update: dict[str, Any], deps: DispatchDeps) -> dict[str, Any] | None:
    """Extract the canonical record from a live update, or None to skip.

    `update` shape (produced by the worker's Telethon event handler and by the
    synthetic generator):
        {"event": "new_message",
         "message": {id, date, edit_date, message, out, from_id, …},
         "dialog_id": int, "dialog_kind": str, "dialog_title": str|None}
    """
    if update.get("event") != "new_message":
        return None
    message = update.get("message")
    dialog_id = update.get("dialog_id")
    if not isinstance(message, dict) or not isinstance(dialog_id, int):
        return None
    # Skip our own outgoing messages (the service account's sends).
    if message.get("out") is True:
        return None
    return build_message_record(
        message,
        installation_id=deps.installation_id,
        dialog_id=dialog_id,
        dialog_kind=update.get("dialog_kind") or "chat",
        dialog_title=update.get("dialog_title"),
    )


def _gateway_raw(record: dict[str, Any]) -> tuple[bytes, dict[str, Any]]:
    """Canonical raw body + ingress_metadata for a live update. The body IS the
    canonical record (byte-stable orjson) — the same shape the backfill path
    publishes — so the normalizer feeds `handle_telegram` identically and
    content_hash dedup / replay-from-raw hold."""
    raw_body = orjson.dumps(record, option=orjson.OPT_SORT_KEYS)
    ingress_metadata = {
        "event_type": "new_message",
        "dialog_id": record.get("_fyralis_dialog_id"),
        "message_id": record.get("id"),
    }
    return raw_body, ingress_metadata


async def _attempt_gateway_cutover(
    deps: DispatchDeps, *, record: dict[str, Any],
) -> bool:
    """Publish the live record to `ingestion.raw.telegram`. Returns True on
    success; False (best-effort) on any failure so the caller falls back."""
    raw_body, ingress_metadata = _gateway_raw(record)
    try:
        await shadow_write_raw(
            tenant_id=deps.tenant_id,
            source="telegram",
            ingress_kind="gateway",
            raw_body=raw_body,
            s3_client=deps.s3_raw_client,
            kafka_producer=deps.kafka_producer,
            ingress_metadata=ingress_metadata,
        )
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "telegram_gateway.cutover_failed", error=str(exc)[:200],
        )
        return False


async def _maybe_shadow_write_gateway(
    deps: DispatchDeps, *, record: dict[str, Any],
) -> None:
    """Best-effort post-inline M2 audit shadow write (never fails the path)."""
    if not SHADOW_WRITE_ENABLED:
        return
    if deps.s3_raw_client is None or deps.kafka_producer is None:
        return
    try:
        await _attempt_gateway_cutover(deps, record=record)
    except Exception:  # noqa: BLE001
        log.debug("telegram_gateway.shadow_audit_failed")


async def handle_update(update: dict[str, Any], deps: DispatchDeps) -> None:
    """Handle one live update from the persistent connection."""
    record = _update_to_record(update, deps)
    if record is None:
        return

    # ---- Cutover branch (kafka-first default; shared kill-switch flag) ----
    flag_enabled = False
    if (
        deps.tenant_flags is not None
        and deps.kafka_producer is not None
        and deps.s3_raw_client is not None
    ):
        flag_enabled = await deps.tenant_flags.kafka_path_enabled(deps.tenant_id)

    if flag_enabled:
        if await _attempt_gateway_cutover(deps, record=record):
            return
        # Graceful degradation — fall through to inline so we don't drop it.
        log.warning("telegram_gateway.kafka_path_fallback_to_inline")

    # ---- Inline path ----
    try:
        await ingest(
            CHANNEL,
            record,
            pool=deps.pool,
            tenant_id=deps.tenant_id,
            actor_repo=deps.actor_repo,
            alias_repo=deps.alias_repo,
            embedder=deps.embedder,
        )
    except Exception:  # noqa: BLE001
        log.exception(
            "telegram_gateway_ingest_failed",
            dialog_id=record.get("_fyralis_dialog_id"),
            message_id=record.get("id"),
        )
        return

    if not flag_enabled:
        await _maybe_shadow_write_gateway(deps, record=record)


__all__ = ["DispatchDeps", "handle_update"]
