"""services/integrations/discord/gateway/dispatch.py — dispatch router.

The bridge between the Discord Gateway WSS client (which speaks the opcode
protocol) and the ingestion handler (which writes substrate rows).

`handle_dispatch` is called by the client for every op-0 DISPATCH frame.
It routes on `t` (event name) and, for MESSAGE_CREATE, applies the filter
chain documented in research R7:

  1. `author.bot is True` → drop, metric `filtered_bot_total{source=…}`
  2. `webhook_id is not None` → drop, metric `filtered_bot_total{source="webhook"}`
  3. resolve tenant via existing TenantResolver
  4. on UnknownInstallation → drop, metric `dropped_unknown_installation_total`,
     log without raw guild_id
  5. otherwise → call `ingest("discord:message", payload, …)` which
     persists exactly one observation (idempotent on `external_id`)

GUILD_DELETE and GUILD_CREATE bump a dispatch counter and otherwise do
nothing — per Clarifications, IN-09's outbound-401 chokepoint remains the
canonical kick-detection path.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import orjson
import structlog

from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.ingestion.core import ingest
from services.ingestion.feature_flags import (
    KAFKA_PATH_ENABLED,
    SHADOW_WRITE_ENABLED,
)
from services.ingestion.shadow_write import shadow_write_raw
from services.integrations.discord.gateway import metrics
from services.integrations.discord.oauth import short_guild_hash
from services.webhooks.tenant_resolver import (
    PayloadMissing,
    Resolved,
    UnknownInstallation,
)


log = structlog.get_logger("integrations.discord.gateway.dispatch")


@dataclass
class DispatchDeps:
    """Dependencies injected into every dispatch call. Built once at
    worker startup and reused for the process lifetime.

    Shadow-path deps (s3_raw_client / kafka_producer / tenant_flags)
    are optional. When unwired (default), the shadow write is a
    silent no-op — the worker behaves exactly as before M2.2. The
    gateway worker bootstrap wires them in when the M2 dev stack is
    available; production wires them when the operator opts into
    the shadow path.
    """
    pool: asyncpg.Pool
    tenant_resolver: Any  # services.webhooks.tenant_resolver.TenantResolver
    actor_repo: ActorRepo | None
    alias_repo: EntityAliasRepo | None
    embedder: Any  # OllamaClient | None
    application_id: str | None
    # M2.2 — optional shadow-path deps. When None, _maybe_shadow_write_gateway
    # silently no-ops.
    s3_raw_client: Any = None    # services.ingestion.raw_tier.s3.S3Client | None
    kafka_producer: Any = None   # services.ingestion.kafka.IdempotentProducer | None
    tenant_flags: Any = None     # services.ingestion.feature_flags.TenantFlags | None


async def handle_dispatch(frame: dict[str, Any], deps: DispatchDeps) -> None:
    """Top-level dispatch — called for every op-0 DISPATCH frame.

    Routes on `frame["t"]` (event name). Unhandled events bump
    `dispatch_total{event=...}` and return without action.
    """
    event = frame.get("t") or ""
    payload = frame.get("d") or {}

    if event == "MESSAGE_CREATE":
        await handle_message_create(payload, deps)
        return

    if event in ("GUILD_CREATE", "GUILD_DELETE", "READY", "RESUMED"):
        # Metric increment happened in the client's dispatch loop.
        # Nothing else for the worker to do (Clarifications: GUILD_DELETE
        # is metric-only; IN-09's chokepoint is the kick-detection path).
        return

    # Other events (MESSAGE_UPDATE, MESSAGE_DELETE, TYPING_START, …)
    # are not in v1 scope. The dispatch metric was already incremented
    # in the client; nothing else to do.


async def handle_message_create(message: dict[str, Any], deps: DispatchDeps) -> None:
    """The hot path. See module docstring + contracts/module-gateway-dispatch.md."""
    # 1. Author filter (R7: fastest, before any DB hit).
    author = message.get("author") or {}
    if isinstance(author, dict) and author.get("bot") is True:
        source = (
            "self"
            if author.get("id") and author["id"] == deps.application_id
            else "other_bot"
        )
        metrics.inc("discord_gateway_filtered_bot_total", source=source)
        return
    if message.get("webhook_id") is not None:
        metrics.inc("discord_gateway_filtered_bot_total", source="webhook")
        return

    # 2. DM (no guild_id) — out of scope for v1.
    guild_id = message.get("guild_id")
    if not guild_id or not isinstance(guild_id, str):
        metrics.inc("discord_gateway_dispatch_total", event="MESSAGE_CREATE_DM")
        return

    # 3. Tenant resolution (reuse IN-07 substrate). `resolve()` returns
    # a discriminated outcome — never raises for "unknown" / "missing".
    outcome = await deps.tenant_resolver.resolve(
        "discord",
        payload={"guild_id": guild_id},
        headers={},
    )
    if isinstance(outcome, (UnknownInstallation, PayloadMissing)):
        metrics.inc("discord_gateway_dropped_unknown_installation_total")
        log.info(
            "discord_gateway_dropped_unknown_installation",
            short_guild_hash=short_guild_hash(guild_id),
            # NEVER log raw guild_id — SC-006.
        )
        return
    if not isinstance(outcome, Resolved):
        log.error(
            "discord_gateway_bad_tenant_resolver_result",
            short_guild_hash=short_guild_hash(guild_id),
        )
        return
    tenant_id: UUID = outcome.tenant_id

    # ---- Cutover branch (parallel to the M5.3 webhook-router cutover) ----
    # Read `ingestion.kafka_path_enabled` for the resolved tenant.
    # default=False — pre-cutover tenants stay on the inline path; this
    # default is the load-bearing N1 invariant (missing flag rows MUST
    # NOT activate cutover for tenants never explicitly enabled).
    #
    # The Gateway MESSAGE_CREATE frame — unlike a Discord webhook
    # interaction (M5.4 deferral) — has no synchronous response-shape
    # requirement, so the publish-and-return cutover shape applies here
    # exactly as it does for slack/github in services/webhooks/router.py.
    # When TRUE: publish the frame to `ingestion.raw` and return; the
    # writer pool produces the observation via M5.2's full-mode path.
    #
    # `flag_enabled` is also consulted below to skip the M2 shadow write
    # after a fallback-to-inline (retrying the same publish would almost
    # certainly fail again — parity with the router's suppression).
    flag_enabled = False
    if (
        deps.tenant_flags is not None
        and deps.kafka_producer is not None
        and deps.s3_raw_client is not None
    ):
        flag_enabled = await deps.tenant_flags.get_bool(
            tenant_id, KAFKA_PATH_ENABLED, default=False,
        )

    if flag_enabled:
        cutover_ok = await _attempt_gateway_cutover(
            deps, tenant_id=tenant_id, message=message, guild_id=guild_id,
        )
        if cutover_ok:
            metrics.inc("discord_gateway_kafka_path_total", outcome="success")
            return
        # Graceful degradation: the publish failed; fall through to
        # inline ingest() so the message is not dropped. NOT
        # gate-relaxation — the flag stays TRUE; the `fallback` metric
        # is the operator's signal that cutover connectivity is degraded.
        metrics.inc("discord_gateway_kafka_path_total", outcome="fallback")
        log.warning(
            "discord_gateway.kafka_path_fallback_to_inline",
            short_guild_hash=short_guild_hash(guild_id),
        )

    # 4. Hand to the unified ingest path. The handler registered for
    # `discord:message` shapes the payload; `ingest()` handles dedup
    # via the existing unique index.
    try:
        result = await ingest(
            "discord:message",
            message,
            pool=deps.pool,
            tenant_id=tenant_id,
            actor_repo=deps.actor_repo,
            alias_repo=deps.alias_repo,
            embedder=deps.embedder,
        )
    except Exception:  # noqa: BLE001
        log.exception(
            "discord_gateway_ingest_failed",
            short_guild_hash=short_guild_hash(guild_id),
            message_id=message.get("id"),
        )
        return

    if result.deduped:
        metrics.inc("discord_gateway_messages_dedup_total")
    else:
        metrics.inc("discord_gateway_messages_total")

    # ---- M2.2 Shadow path ----
    # AFTER successful inline ingest(), before the function returns.
    # Same ordering rationale as M2.1's webhook router (see
    # services/webhooks/router.py:741-771) — inline is the source of
    # truth during M2; observable divergence is "inline observation
    # exists, shadow record missing." Best-effort; failures caught
    # inside the helper.
    #
    # Suppressed when `flag_enabled` (cutover mode): a fallback-to-inline
    # already attempted the Kafka publish; the M2 shadow audit is moot
    # once the tenant is on the cutover path.
    if not flag_enabled:
        await _maybe_shadow_write_gateway(
            deps,
            tenant_id=tenant_id,
            message=message,
            guild_id=guild_id,
        )


def _gateway_raw(
    message: dict[str, Any], guild_id: str,
) -> tuple[bytes, dict[str, Any]]:
    """Build the canonical raw body + ingress_metadata for a Gateway
    MESSAGE_CREATE frame. Shared by the cutover publish and the M2
    shadow write so both produce byte-identical S3 bodies (content_hash
    dedup + N2 replay-from-raw depend on this byte-equality).

    Canonical-JSON via orjson OPT_SORT_KEYS — Discord retransmissions of
    the same message_id arrive byte-identical at the WSS layer, so the
    canonical form matches across retransmissions (the dedup property
    the work order requires). NEVER emit the raw guild_id — only its
    short hash (SC-006).
    """
    raw_body = orjson.dumps(message, option=orjson.OPT_SORT_KEYS)
    ingress_metadata: dict[str, Any] = {
        "event_type": "MESSAGE_CREATE",
        "message_id": message.get("id"),
        "channel_id": message.get("channel_id"),
        "short_guild_hash": short_guild_hash(guild_id),
    }
    return raw_body, ingress_metadata


async def _attempt_gateway_cutover(
    deps: DispatchDeps,
    *,
    tenant_id: UUID,
    message: dict[str, Any],
    guild_id: str,
) -> bool:
    """M5.3-style cutover for the Gateway MESSAGE_CREATE path: publish the
    frame to `ingestion.raw` (S3 PutIfAbsent → Kafka publish). Returns
    True on full success, False on any failure (caller MUST fall back to
    inline `ingest()`).

    Unlike `_maybe_shadow_write_gateway` (best-effort audit alongside the
    inline source-of-truth), this is the SOLE write when it succeeds — so
    its failure must be observable to the caller via the return value
    rather than swallowed-and-continued.
    """
    raw_body, ingress_metadata = _gateway_raw(message, guild_id)
    try:
        await shadow_write_raw(
            tenant_id=tenant_id,
            source="discord",
            ingress_kind="gateway",
            raw_body=raw_body,
            s3_client=deps.s3_raw_client,
            kafka_producer=deps.kafka_producer,
            ingress_metadata=ingress_metadata,
        )
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "discord_gateway.kafka_path_failed",
            short_guild_hash=short_guild_hash(guild_id),
            error_type=type(exc).__name__,
            error_message=str(exc)[:200],
        )
        return False


async def _maybe_shadow_write_gateway(
    deps: DispatchDeps,
    *,
    tenant_id: UUID,
    message: dict[str, Any],
    guild_id: str,
) -> None:
    """Shadow write for a Discord Gateway MESSAGE_CREATE frame.
    PRIME DIRECTIVE (M2 work order §M2.2): a failure here MUST NOT
    propagate. Caller's metric increment + return are unaffected.

    Raw-body strategy: the gateway client deserializes WSS frames
    into dicts before passing them to dispatch; we re-serialize the
    `message` dict to canonical JSON (orjson + sorted keys) for the
    shadow body. Hash determinism is preserved because the canonical
    form is byte-equal for byte-equal logical content. Discord
    retransmissions of the same message_id arrive byte-identical at
    the WSS layer, so the canonical form also matches across
    retransmissions, which is the dedup property the work order
    requires.

    No-ops cleanly when the shadow deps aren't wired (the default
    for the pre-M2 worker bootstrap and for any test that doesn't
    explicitly construct DispatchDeps with shadow deps).
    """
    if deps.s3_raw_client is None or deps.kafka_producer is None:
        return
    try:
        if deps.tenant_flags is not None:
            enabled = await deps.tenant_flags.get_bool(
                tenant_id, SHADOW_WRITE_ENABLED, default=True,
            )
            if not enabled:
                return

        raw_body, ingress_metadata = _gateway_raw(message, guild_id)
        await shadow_write_raw(
            tenant_id=tenant_id,
            source="discord",
            ingress_kind="gateway",
            raw_body=raw_body,
            s3_client=deps.s3_raw_client,
            kafka_producer=deps.kafka_producer,
            ingress_metadata=ingress_metadata,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "shadow_path.failure",
            source="discord",
            ingress_kind="gateway",
            short_guild_hash=short_guild_hash(guild_id),
            error_type=type(exc).__name__,
            error_message=str(exc)[:200],
        )


__all__ = ["DispatchDeps", "handle_dispatch", "handle_message_create"]
