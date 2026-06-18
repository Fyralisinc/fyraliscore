"""services/platform/extensions/egress/delivery.py — opt-in webhook push.

Drains pending ``extension_webhook_delivery`` rows (enqueued by the projector for
extensions that registered a callback), POSTs the redacted item to the extension's
callback URL with an HMAC ``X-Fyralis-Signature``, and records the outcome:
delivered, or retried with exponential backoff, then dead-lettered after
``max_attempts``. ``http_post`` is injectable so tests need no network.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Awaitable, Callable

from services.platform.extensions.egress import webhook
from services.platform.extensions.identity import ExtensionOAuthClientsRepo

log = logging.getLogger("extensions.egress.delivery")

# (url, body, headers) -> HTTP status code
HttpPost = Callable[[str, bytes, dict[str, str]], Awaitable[int]]


async def _default_post(url: str, body: bytes, headers: dict[str, str]) -> int:
    import httpx
    async with httpx.AsyncClient(timeout=15.0) as c:
        r = await c.post(url, content=body, headers=headers)
        return r.status_code


async def run_webhook_pass(
    pool: Any, *, http_post: HttpPost | None = None, max_attempts: int = 6, batch: int = 100,
) -> dict[str, int]:
    """One delivery sweep. Returns {delivered, retried, failed, total}."""
    post = http_post or _default_post
    repo = ExtensionOAuthClientsRepo(pool)
    targets: dict[str, tuple[str, str] | None] = {}

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT d.id, d.egress_seq, d.extension_id, d.attempts, "
            "       e.tenant_id, e.payload "
            "FROM extension_webhook_delivery d "
            "JOIN extension_egress e ON e.seq = d.egress_seq "
            "WHERE d.status='pending' AND d.next_attempt_at <= now() "
            "ORDER BY d.next_attempt_at LIMIT $1",
            batch,
        )

    delivered = retried = failed = 0
    for r in rows:
        ext = r["extension_id"]
        if ext not in targets:
            targets[ext] = await repo.webhook_target(ext)
        target = targets[ext]
        if target is None:
            await _fail(pool, r["id"], "no_callback_registered")
            failed += 1
            continue
        url, secret = target
        payload = r["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        body = json.dumps({
            "type": "observation", "seq": r["egress_seq"],
            "tenant_id": str(r["tenant_id"]), "observation": payload,
        }).encode()
        headers = {"content-type": "application/json",
                   webhook.SIGNATURE_HEADER: webhook.sign(body, secret)}
        ok = False
        err = ""
        try:
            status = await post(url, body, headers)
            ok = 200 <= status < 300
            err = "" if ok else f"http_{status}"
        except Exception as exc:  # noqa: BLE001 — network failure → retry
            err = f"{type(exc).__name__}: {exc}"[:300]

        if ok:
            await _delivered(pool, r["id"])
            delivered += 1
        elif r["attempts"] + 1 >= max_attempts:
            await _fail(pool, r["id"], err)
            failed += 1
        else:
            await _retry(pool, r["id"], err)
            retried += 1

    if rows:
        log.info("egress.webhook_pass delivered=%d retried=%d failed=%d", delivered, retried, failed)
    return {"delivered": delivered, "retried": retried, "failed": failed, "total": len(rows)}


async def _delivered(pool: Any, delivery_id: Any) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE extension_webhook_delivery "
            "SET status='delivered', attempts=attempts+1, delivered_at=now() WHERE id=$1",
            delivery_id,
        )


async def _retry(pool: Any, delivery_id: Any, err: str) -> None:
    # exponential backoff: 2^attempts seconds (capped in SQL by attempts growth)
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE extension_webhook_delivery "
            "SET attempts=attempts+1, last_error=$2, "
            "    next_attempt_at = now() + (power(2, least(attempts, 10)) * interval '1 second') "
            "WHERE id=$1",
            delivery_id, err,
        )


async def _fail(pool: Any, delivery_id: Any, err: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE extension_webhook_delivery "
            "SET status='failed', attempts=attempts+1, last_error=$2 WHERE id=$1",
            delivery_id, err,
        )


__all__ = ["run_webhook_pass", "HttpPost"]
