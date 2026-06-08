"""services/ingest/integrations/linkedin/poll.py — live POLL dispatch.

LinkedIn is POLL-ONLY: there is NO webhook. The live edge re-lists changed
organization objects on an interval and dispatches each detected change directly
through the ingestion pipeline — the Telegram-gateway `handle_update` analog, but
driven by a poller instead of a persistent connection.

A LinkedIn poller holds ONE organization's install = ONE tenant's install, so the
tenant is known by construction (carried on `PollDeps`) — there is no per-change
tenant resolution.

Flow (parallel to the carta poll cutover):
  1. Build the canonical change record (the SAME `_fyralis_record_type` tagged
     shape the backfill fetcher emits → identical external_id → cross-path
     dedup). `build_change_record` is the shared constructor.
  2. Cutover: if `ingestion.kafka_path_enabled` for the tenant (kafka-first
     default), shadow-write the record to `ingestion.raw.linkedin`
     (`ingress_kind="poll"`) and return — the normalizer + observation_writer
     produce the observation, concurrently with any in-flight backfill.
  3. Fallback / inline: otherwise `core.ingest("linkedin:object", record, …)`,
     then a best-effort M2 shadow audit (when SHADOW_WRITE_ENABLED).

There is NO HMAC signature gate and NO HTTP status — the trust boundary is the
authenticated OAuth poll connection itself (as with Carta / Telegram's gateway).
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


log = structlog.get_logger("integrations.linkedin.poll")


CHANNEL = "linkedin:object"


@dataclass
class PollDeps:
    """Dependencies injected into every poll-dispatch call. Built once per
    poll cycle (bound to one install/tenant) and reused for the cycle.

    Shadow-path deps (s3_raw_client / kafka_producer / tenant_flags) are
    optional: when unwired the poller falls back to inline `ingest()`.
    """

    pool: asyncpg.Pool
    tenant_id: UUID
    installation_id: str
    organization_urn: str
    actor_repo: Any = None
    alias_repo: Any = None
    embedder: Any = None
    s3_raw_client: Any = None    # raw_tier.s3.S3Client | None
    kafka_producer: Any = None   # kafka.IdempotentProducer | None
    tenant_flags: Any = None     # feature_flags.TenantFlags | None


def build_change_record(
    change: dict[str, Any], *, organization_urn: str,
) -> dict[str, Any] | None:
    """Build the canonical fetcher-shaped record from a polled change, or None.

    A polled change is `{"entity_type": "share"|..., "entity": {<full LinkedIn
    object incl. Id, MetaData.LastUpdatedTime>}}`. The record is the SAME shape
    `fetch_page_linkedin` emits so `handle_linkedin_object` builds an identical
    external_id — giving cross-path dedup with backfill.
    """
    entity_type = change.get("entity_type")
    entity = change.get("entity")
    if not isinstance(entity_type, str) or not entity_type:
        return None
    if not isinstance(entity, dict) or not entity.get("Id"):
        return None
    return {
        "_fyralis_record_type": entity_type.lower(),
        "_fyralis_org_urn": organization_urn,
        "entity": entity,
    }


def _poll_raw(record: dict[str, Any]) -> tuple[bytes, dict[str, Any]]:
    """Canonical raw body + ingress_metadata for a polled change. The body IS
    the canonical record (byte-stable orjson) — the same shape the backfill path
    publishes — so the normalizer feeds `handle_linkedin_object` identically and
    content_hash dedup / replay-from-raw hold."""
    raw_body = orjson.dumps(record, option=orjson.OPT_SORT_KEYS)
    entity = record.get("entity") or {}
    ingress_metadata = {
        "event_type": "poll_change",
        "entity_type": record.get("_fyralis_record_type"),
        "entity_id": entity.get("Id") if isinstance(entity, dict) else None,
    }
    return raw_body, ingress_metadata


async def _attempt_poll_cutover(
    deps: PollDeps, *, record: dict[str, Any],
) -> bool:
    """Publish the polled record to `ingestion.raw.linkedin`. Returns True on
    success; False (best-effort) on any failure so the caller falls back."""
    raw_body, ingress_metadata = _poll_raw(record)
    try:
        await shadow_write_raw(
            tenant_id=deps.tenant_id,
            source="linkedin",  # type: ignore[arg-type]  # wiring widens SourceLiteral
            ingress_kind="poll",
            raw_body=raw_body,
            s3_client=deps.s3_raw_client,
            kafka_producer=deps.kafka_producer,
            ingress_metadata=ingress_metadata,
        )
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "linkedin_poll.cutover_failed", error=str(exc)[:200],
        )
        return False


async def _maybe_shadow_write_poll(
    deps: PollDeps, *, record: dict[str, Any],
) -> None:
    """Best-effort post-inline M2 audit shadow write (never fails the path)."""
    if not SHADOW_WRITE_ENABLED:
        return
    if deps.s3_raw_client is None or deps.kafka_producer is None:
        return
    try:
        await _attempt_poll_cutover(deps, record=record)
    except Exception:  # noqa: BLE001
        log.debug("linkedin_poll.shadow_audit_failed")


async def handle_polled_change(change: dict[str, Any], deps: PollDeps) -> None:
    """Handle one detected change from a poll cycle."""
    record = build_change_record(change, organization_urn=deps.organization_urn)
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
        if await _attempt_poll_cutover(deps, record=record):
            return
        # Graceful degradation — fall through to inline so we don't drop it.
        log.warning("linkedin_poll.kafka_path_fallback_to_inline")

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
            "linkedin_poll_ingest_failed",
            entity_type=record.get("_fyralis_record_type"),
            entity_id=(record.get("entity") or {}).get("Id"),
        )
        return

    if not flag_enabled:
        await _maybe_shadow_write_poll(deps, record=record)


__all__ = ["PollDeps", "build_change_record", "handle_polled_change", "CHANNEL"]
