"""services/ingest/ingestion/reconcilers/signal.py — gap detection (IN-SIGNAL).

Per A17 + A18 + A18.3 (reconciler pool-provider seam). Cloned from the Telegram
reconciler (its archetype).

============================================================
GAP DETECTION ALGORITHM
============================================================
After a thread shard completes, its cursor carries `high_water_max_id` — the MAX
message id the fetcher walked. The reconciler probes the LIVE thread with a 1-row
`has_history_since(min_id=high_water_max_id)`: if any newer message exists, a
reshare is emitted for that thread, warm-started at the high-water so the walk
only re-fetches the changed tail (incremental mode in the fetcher).

`external_id` parity (install-namespaced) means re-walked messages dedup against
what backfill already wrote — only genuinely new messages produce new
observations. The probe is one cheap call per thread; it can over-reshare but
never under-reshares, and dedup makes re-walks idempotent.

NOTE: the persistent live linked-device session (the gateway worker) is the
primary live path; its native sync replay is its own reconciler. This DB-side
reconciler is the backfill-completeness safety net (it catches anything that
arrived between the backfill sweep and the live session coming up).
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg
import orjson

from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
    ResharedShard,
)
from services.ingest.ingestion.workflows.state import load_state


log = logging.getLogger(__name__)


SHARD_KIND_THREAD_HISTORY = "signal_thread_history"
RESHARE_RECENCY_SCORE = 1.5


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.signal: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


# Test seam — re-exported so the harness can patch this module's symbol too
# (the fetcher's and the reconciler's `_open_signal_client` are patched
# together by `_install_factories`).
async def _open_signal_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers.signal import _open_signal_client as _open
    return await _open(install)


def _decode_identifier(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, (str, bytes)):
        return orjson.loads(raw)
    return dict(raw)


async def _load_shard_high_water(pool: Any, shard_id: Any) -> int | None:
    state = await load_state(pool, "shard_fetch", str(shard_id))
    if state is None or not state.state_data:
        return None
    cursor = state.state_data.get("cursor")
    if isinstance(cursor, dict):
        hw = cursor.get("high_water_max_id")
        return hw if isinstance(hw, int) and hw > 0 else None
    return None


async def _check_one_shard_for_gap(
    *, pool: Any, client: Any, shard: asyncpg.Record,
) -> ResharedShard | None:
    identifier = _decode_identifier(shard["shard_identifier"])
    if identifier.get("shard_kind") != SHARD_KIND_THREAD_HISTORY:
        return None
    thread_id = identifier.get("thread_id")
    if not isinstance(thread_id, int):
        return None

    high_water = await _load_shard_high_water(pool, shard["id"])
    if high_water is None:
        return None  # No reference point (empty thread / cursor).

    try:
        has_updates = await client.has_history_since(
            thread_id=thread_id,
            thread_kind=identifier.get("thread_kind") or "direct",
            min_id=high_water,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.signal.probe_failed",
            extra={"shard_id": str(shard["id"]), "error": str(exc)[:200]},
        )
        return None

    if not has_updates:
        return None

    gap_identifier = dict(identifier)
    gap_identifier["parent_shard_id"] = str(shard["id"])
    gap_identifier["gap_baseline_max_id"] = high_water
    # Warm-start the reshared walk at the high-water so it only re-fetches the
    # newer tail (incremental mode in the fetcher).
    gap_identifier["offset_id_cursor"] = high_water
    return ResharedShard(
        shard=Shard(
            shard_kind=SHARD_KIND_THREAD_HISTORY,
            shard_identifier=gap_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )


_LOAD_SIGNAL_INSTALL_SQL = """
SELECT id, tenant_id, account_label,
       session_secret_ref, backfill_session_secret_ref, disabled_at
  FROM signal_installations
 WHERE tenant_id = $1 AND disabled_at IS NULL
 LIMIT 1
"""


async def reconcile_signal(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    install = await pool.fetchrow(_LOAD_SIGNAL_INSTALL_SQL, run["tenant_id"])
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_signal_client(install)
    try:
        new_shards: list[ResharedShard] = []
        for shard in active:
            reshared = await _check_one_shard_for_gap(
                pool=pool, client=client, shard=shard,
            )
            if reshared is not None:
                new_shards.append(reshared)
    finally:
        await close()

    if new_shards:
        return ReconciliationDecision(
            has_gaps=True, new_shards=new_shards,
            message=f"signal reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)


RECONCILER_DISPATCH["signal"] = reconcile_signal


__all__ = [
    "RESHARE_RECENCY_SCORE",
    "SHARD_KIND_THREAD_HISTORY",
    "reconcile_signal",
    "set_pool_provider",
]
