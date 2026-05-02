"""services/demo/notifications.py — outbound notification suppression
for demo tenants.

The codebase doesn't actually send external notifications today
(grep for `send_email`/`post_message` returns nothing). When a future
notifier is added — Slack, email, SMS — wrap the send call with::

    if await should_suppress(pool, tenant_id):
        log.info("notification.suppressed", tenant_id=tenant_id)
        return
    await actually_send(...)

Demo tenants suppress by default (per `demo_configs.notifications_suppressed`).
Non-demo tenants always send.
"""
from __future__ import annotations

from uuid import UUID

import asyncpg
import structlog

from services.demo.repo import get_demo_config_by_id, get_tenant


log = structlog.get_logger("demo.notifications")


async def should_suppress(
    conn: asyncpg.Connection | asyncpg.Pool,
    tenant_id: UUID,
) -> bool:
    """Return True if outbound notifications for this tenant should be
    swallowed. Non-demo tenants always return False."""
    tenant = await get_tenant(conn, tenant_id)
    if tenant is None or not tenant.is_demo:
        return False
    if tenant.demo_config_id is None:
        return True             # demo tenant without a config → safe default
    cfg = await get_demo_config_by_id(conn, tenant.demo_config_id)
    if cfg is None:
        return True
    return bool(cfg.notifications_suppressed)


async def log_suppressed(
    *,
    tenant_id: UUID,
    channel: str,
    target: str,
    payload_preview: str,
) -> None:
    """Structured log so demo authors can verify the right call was made.
    Caller invokes this *instead of* the actual send when suppressed."""
    log.info(
        "demo_notification_suppressed",
        tenant_id=str(tenant_id),
        channel=channel,
        target=target,
        preview=payload_preview[:200],
    )


__all__ = ["should_suppress", "log_suppressed"]
