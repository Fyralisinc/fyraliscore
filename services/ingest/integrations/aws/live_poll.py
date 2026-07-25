"""services/ingest/integrations/aws/live_poll.py — live POLL dispatch (IN-AWS).

The bridge between the AWS live-poll loop (an SQS / EventBridge consumer that
long-polls a queue of CloudTrail events) and the ingestion pipeline.
`handle_polled_event` is called for every CloudTrail-shaped event the poll loop
drains. It is the Telegram-gateway `handle_update` analog (direct-dispatch, no
HTTP webhook), with one difference: a single poll loop can drain events for
MANY installs, so the tenant/install is resolved PER EVENT from
`aws_installations` by (account_id, region) — there is no per-loop tenant binding.

Flow (parallel to the telegram gateway cutover):
  1. Resolve the (account_id, region) -> aws_installations row -> tenant_id +
     installation id. Unknown account/region -> drop (not our install).
  2. Build the canonical event record (the SAME tagging the backfill fetcher
     applies -> identical IMMUTABLE external_id -> cross-path dedup).
  3. Cutover: if `ingestion.kafka_path_enabled` for the tenant (kafka-first
     default), shadow-write the record to `ingestion.raw.aws`
     (`ingress_kind="poll"`) and return — the normalizer + observation_writer
     produce the observation, concurrently with any in-flight backfill.
  4. Fallback / inline: otherwise `core.ingest("aws:event", record, …)`, then a
     best-effort M2 shadow audit (when SHADOW_WRITE_ENABLED).

There is NO HMAC signature gate — the trust boundary is the IAM-authenticated
poll of the customer's own SQS/EventBridge queue (as with Telegram's MTProto
connection / Gmail's Pub/Sub), not a signed webhook header.
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


log = structlog.get_logger("integrations.aws.live_poll")


CHANNEL = "aws:event"


@dataclass
class PollDeps:
    """Dependencies injected into every poll dispatch call. Built once at the
    poll loop's startup and reused for the process lifetime.

    Shadow-path deps (s3_raw_client / kafka_producer / tenant_flags) are
    optional: when unwired the loop falls back to inline `ingest()`.
    """

    pool: asyncpg.Pool
    tenant_id: UUID | None = None
    installation_id: str | None = None
    actor_repo: Any = None
    alias_repo: Any = None
    embedder: Any = None
    s3_raw_client: Any = None    # raw_tier.s3.S3Client | None
    kafka_producer: Any = None   # kafka.IdempotentProducer | None
    tenant_flags: Any = None     # feature_flags.TenantFlags | None


@dataclass(frozen=True)
class _ResolvedInstall:
    tenant_id: UUID
    installation_id: str
    account_id: str
    region: str


def _event_namespace(event: dict[str, Any]) -> tuple[str | None, str | None]:
    """The (account_id, region) the polled CloudTrail event belongs to.

    A real CloudTrail event carries `recipientAccountId` + `awsRegion`; the
    synthetic generator pre-tags `_fyralis_account_id` / `_fyralis_region`.
    """
    account = event.get("_fyralis_account_id") or event.get("recipientAccountId")
    region = event.get("_fyralis_region") or event.get("awsRegion")
    account = account if isinstance(account, str) and account else None
    region = region if isinstance(region, str) and region else None
    return account, region


async def _resolve_install(
    deps: PollDeps, *, account_id: str, region: str,
) -> _ResolvedInstall | None:
    """Verify the poller's exact tenant/install owns this account and region.

    Queue consumers are provisioned per installation. Resolving by account and
    region alone is ambiguous across tenants and previously selected the first
    matching row with ``LIMIT 1``.
    """
    if deps.tenant_id is None or not deps.installation_id:
        log.error("aws_poll.missing_exact_installation_binding")
        return None
    row = await deps.pool.fetchrow(
        """
        SELECT id, tenant_id
          FROM aws_installations
         WHERE tenant_id = $1
           AND id = $2::uuid
           AND account_id = $3
           AND region = $4
           AND disabled_at IS NULL
        """,
        deps.tenant_id,
        deps.installation_id,
        account_id,
        region,
    )
    if row is None:
        return None
    return _ResolvedInstall(
        tenant_id=row["tenant_id"],
        installation_id=str(row["id"]),
        account_id=account_id,
        region=region,
    )


def _build_event_record(
    event: dict[str, Any], resolved: _ResolvedInstall,
) -> dict[str, Any]:
    """Tag the polled event with the SAME `_fyralis_*` namespace the backfill
    fetcher applies, so the handler derives the IDENTICAL immutable external_id
    on both edges (cross-path dedup)."""
    record = dict(event)
    record["_fyralis_record_type"] = "event"
    record["_fyralis_account_id"] = resolved.account_id
    record["_fyralis_region"] = resolved.region
    return record


def _poll_raw(record: dict[str, Any]) -> tuple[bytes, dict[str, Any]]:
    """Canonical raw body + ingress_metadata for one polled event. The body IS
    the canonical record (byte-stable orjson) — the same shape the backfill path
    publishes — so the normalizer feeds `handle_aws_event` identically and
    content_hash dedup / replay-from-raw hold."""
    raw_body = orjson.dumps(record, option=orjson.OPT_SORT_KEYS)
    ingress_metadata = {
        "event_type": "cloudtrail_event",
        "account_id": record.get("_fyralis_account_id"),
        "region": record.get("_fyralis_region"),
        "event_id": record.get("eventId"),
    }
    return raw_body, ingress_metadata


async def _attempt_poll_cutover(
    deps: PollDeps, *, tenant_id: UUID, record: dict[str, Any],
) -> bool:
    """Publish the polled record to `ingestion.raw.aws` (ingress_kind="poll").
    Returns True on success; False (best-effort) on any failure so the caller
    falls back to inline."""
    raw_body, ingress_metadata = _poll_raw(record)
    try:
        await shadow_write_raw(
            tenant_id=tenant_id,
            source="aws",
            ingress_kind="poll",
            raw_body=raw_body,
            s3_client=deps.s3_raw_client,
            kafka_producer=deps.kafka_producer,
            ingress_metadata=ingress_metadata,
        )
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("aws_poll.cutover_failed", error=str(exc)[:200])
        return False


async def _maybe_shadow_write_poll(
    deps: PollDeps, *, tenant_id: UUID, record: dict[str, Any],
) -> None:
    """Best-effort post-inline M2 audit shadow write (never fails the path)."""
    if not SHADOW_WRITE_ENABLED:
        return
    if deps.s3_raw_client is None or deps.kafka_producer is None:
        return
    try:
        await _attempt_poll_cutover(deps, tenant_id=tenant_id, record=record)
    except Exception:  # noqa: BLE001
        log.debug("aws_poll.shadow_audit_failed")


async def handle_polled_event(event: dict[str, Any], deps: PollDeps) -> None:
    """Handle one CloudTrail-shaped event drained from the live poll loop."""
    if not isinstance(event, dict):
        return
    if event.get("eventId") is None:
        return

    account_id, region = _event_namespace(event)
    if not account_id or not region:
        return
    resolved = await _resolve_install(deps, account_id=account_id, region=region)
    if resolved is None:
        # No enabled install owns this account/region — not ours.
        return

    record = _build_event_record(event, resolved)

    # ---- Cutover branch (kafka-first default; shared kill-switch flag) ----
    flag_enabled = False
    if (
        deps.tenant_flags is not None
        and deps.kafka_producer is not None
        and deps.s3_raw_client is not None
    ):
        flag_enabled = await deps.tenant_flags.kafka_path_enabled(resolved.tenant_id)

    if flag_enabled:
        if await _attempt_poll_cutover(deps, tenant_id=resolved.tenant_id, record=record):
            return
        # Graceful degradation — fall through to inline so we don't drop it.
        log.warning("aws_poll.kafka_path_fallback_to_inline")

    # ---- Inline path ----
    try:
        await ingest(
            CHANNEL,
            record,
            pool=deps.pool,
            tenant_id=resolved.tenant_id,
            actor_repo=deps.actor_repo,
            alias_repo=deps.alias_repo,
            embedder=deps.embedder,
        )
    except Exception:  # noqa: BLE001
        log.exception(
            "aws_poll_ingest_failed",
            account_id=resolved.account_id,
            region=resolved.region,
            event_id=record.get("eventId"),
        )
        return

    if not flag_enabled:
        await _maybe_shadow_write_poll(deps, tenant_id=resolved.tenant_id, record=record)


__all__ = ["PollDeps", "handle_polled_event", "CHANNEL"]
