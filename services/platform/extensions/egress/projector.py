"""services/platform/extensions/egress/projector.py — the egress projection worker.

Tails the ``observations`` system-of-record by an (occurred_at, id) cursor and, for
every active grant whose capabilities admit the observation's channel, emits the
**redacted** :class:`ObservationView`:

  * to the ``extension_egress`` outbox (the cursor PULL read-model), enqueuing a
    webhook delivery when the extension registered a callback; and
  * to the ``ext.egress.v1`` Kafka topic when a producer is configured (the
    faithful E3.1 stream / external transport).

``run_projection_pass`` is one bounded sweep (idempotent via the outbox's unique
(extension, observation) index), so re-runs are safe and a crash resumes from the
persisted cursor.
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from lib.extensions.host_api.v1 import Capabilities, ObservationView
from services.platform.extensions.egress.planner import GrantSpec, plan_egress
from services.platform.extensions.egress.store import EgressStore

log = logging.getLogger("extensions.egress.projector")

_OBS_COLS = (
    "id, tenant_id, occurred_at, kind, source_channel, content, content_text, "
    "trust_tier, external_id, entities_mentioned"
)


async def _active_grants(conn: Any, tenant_id: UUID) -> list[GrantSpec]:
    rows = await conn.fetch(
        "SELECT extension_id, capabilities FROM extension_grants "
        "WHERE tenant_id=$1 AND revoked_at IS NULL",
        tenant_id,
    )
    out: list[GrantSpec] = []
    for r in rows:
        caps = r["capabilities"]
        if isinstance(caps, str):
            import json
            caps = json.loads(caps)
        out.append(GrantSpec(extension_id=r["extension_id"],
                             capabilities=Capabilities.from_dict(caps)))
    return out


async def _has_callback(conn: Any, extension_id: str, cache: dict[str, bool]) -> bool:
    if extension_id not in cache:
        val = await conn.fetchval(
            "SELECT callback_url FROM extension_oauth_clients "
            "WHERE extension_id=$1 AND callback_url IS NOT NULL AND revoked_at IS NULL "
            "LIMIT 1",
            extension_id,
        )
        cache[extension_id] = bool(val)
    return cache[extension_id]


async def run_projection_pass(
    pool: Any, *, producer: Any = None, batch: int = 500,
) -> int:
    """One bounded projection sweep. Returns the number of observations processed.

    The cross-tenant scan assumes the worker's DB role bypasses RLS (the app/
    sweeper role), like the other core sweepers."""
    store = EgressStore(pool)
    callback_cache: dict[str, bool] = {}
    grants_cache: dict[UUID, list[GrantSpec]] = {}
    processed = 0

    async with pool.acquire() as conn:
        marker = await conn.fetchrow(
            "SELECT last_occurred_at, last_observation_id FROM extension_egress_progress WHERE id=1"
        )
        last_at = marker["last_occurred_at"] if marker else None
        last_id = marker["last_observation_id"] if marker else None
        rows = await conn.fetch(
            f"SELECT {_OBS_COLS} FROM observations "
            "WHERE ($1::timestamptz IS NULL OR (occurred_at, id) > ($1, $2)) "
            "ORDER BY occurred_at, id LIMIT $3",
            last_at, last_id, batch,
        )

    if not rows:
        return 0

    for row in rows:
        tenant_id = row["tenant_id"]
        if tenant_id not in grants_cache:
            async with pool.acquire() as conn:
                grants_cache[tenant_id] = await _active_grants(conn, tenant_id)
        grants = grants_cache[tenant_id]
        if grants:
            view = ObservationView.from_row(row)
            for item in plan_egress(view, grants):
                async with pool.acquire() as conn:
                    wants_webhook = await _has_callback(conn, item.extension_id, callback_cache)
                await store.append(
                    extension_id=item.extension_id, tenant_id=tenant_id,
                    observation_id=row["id"], source_channel=view.source_channel,
                    payload=_view_payload(item.view), enqueue_webhook=wants_webhook,
                )
                if producer is not None:
                    await _produce(producer, item)
        processed += 1
        last_at, last_id = row["occurred_at"], row["id"]

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE extension_egress_progress "
            "SET last_occurred_at=$1, last_observation_id=$2, updated_at=now() WHERE id=1",
            last_at, last_id,
        )
    log.info("egress.projection_pass processed=%d", processed)
    return processed


def _view_payload(view: ObservationView) -> dict[str, Any]:
    from datetime import datetime
    return {
        "id": str(view.id), "tenant_id": str(view.tenant_id),
        "occurred_at": view.occurred_at.isoformat() if isinstance(view.occurred_at, datetime) else view.occurred_at,
        "kind": view.kind, "source_channel": view.source_channel,
        "content": view.content, "content_text": view.content_text,
        "trust_tier": view.trust_tier, "external_id": view.external_id,
        "entities_mentioned": view.entities_mentioned,
    }


async def _produce(producer: Any, item: Any) -> None:
    import json
    from services.platform.extensions.egress.kafka import EGRESS_TOPIC
    await producer.produce(
        EGRESS_TOPIC,
        json.dumps({"extension_id": item.extension_id, "tenant_id": item.tenant_id,
                    "view": _view_payload(item.view)}).encode(),
        key=item.extension_id.encode(),
    )


__all__ = ["run_projection_pass"]
