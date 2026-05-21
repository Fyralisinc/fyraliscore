"""services/ingestion/reconcilers/github.py — GitHub gap detection (M6.4).

Per A17 + A18 + A18.3 (reconciler pool-provider seam).

============================================================
GAP DETECTION ALGORITHM
============================================================
Two-tier check per shard:
  (1) Etag fast-path: HEAD-like request against the same endpoint
      the fetcher used; if etag matches stored, NO change → clean.
  (2) Cursor-based: fetch the FIRST page of the endpoint with
      etag=None; if any record's `updated_at` > stored
      `last_seen_updated_at`, gap exists.

Gap-fill shard: `shard_kind="github_repo_events"` (same as backfill;
the gap is just "more pages of the same endpoint"), `recency_score=1.5`
per A17. Cursor starts at `page=1` again with the OLD
`last_seen_updated_at` baseline so the fetcher knows when to stop
(records older than baseline are pruned during normalization).

============================================================
WIRE-IN
============================================================
Module-level assignment into `RECONCILER_DISPATCH['github']`. Pool
provider seam (A18.3) for reading shard cursors.
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg
import orjson

from services.ingestion.planners import Shard
from services.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
    ResharedShard,
)
from services.ingestion.workflows.state import load_state


log = logging.getLogger(__name__)


SHARD_KIND_REPO_EVENTS = "github_repo_events"
RESHARE_RECENCY_SCORE = 1.5


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.github: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


# Test seam (production wires a GithubClient share).
async def _open_github_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingestion.fetchers._clients import open_github_client
    return await open_github_client(install)


def _decode_identifier(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, (str, bytes)):
        return orjson.loads(raw)
    return dict(raw)


async def _load_shard_cursor(
    pool: Any, shard_id: Any,
) -> dict[str, Any] | None:
    state = await load_state(pool, "shard_fetch", str(shard_id))
    if state is None:
        return None
    cursor = state.state_data.get("cursor") if state.state_data else None
    return cursor if isinstance(cursor, dict) else None


async def _check_one_shard_for_gap(
    *, pool: Any, client: Any, install: asyncpg.Record,
    shard: asyncpg.Record,
) -> ResharedShard | None:
    identifier = _decode_identifier(shard["shard_identifier"])
    owner = identifier.get("owner")
    repo = identifier.get("repo")
    event_type = identifier.get("event_type")
    if not (owner and repo and event_type):
        return None

    cursor = await _load_shard_cursor(pool, shard["id"])
    if cursor is None:
        return None
    stored_etag = cursor.get("etag")
    last_seen = cursor.get("last_seen_updated_at")

    # Etag fast-path: if the response 304s, no gap.
    try:
        has_changes, current_etag = await client.head_repo_events(
            owner=owner, repo=repo, event_type=event_type,
            etag=stored_etag,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort gap check
        log.warning(
            "reconcilers.github.head_failed",
            extra={"shard_id": str(shard["id"]),
                   "error": str(exc)[:200]},
        )
        return None
    if not has_changes:
        return None  # clean

    # Cursor-based: check the first page; if newest record's
    # updated_at > last_seen, gap exists.
    try:
        page, _new_etag, _next = await client.list_repo_events(
            owner=owner, repo=repo, event_type=event_type,
            page=1, per_page=10, etag=None,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "reconcilers.github.list_failed",
            extra={"shard_id": str(shard["id"]),
                   "error": str(exc)[:200]},
        )
        return None
    if not page:
        return None
    newest = max(
        (item.get("updated_at") or "" for item in page),
        default="",
    )
    if last_seen is not None and newest <= last_seen:
        return None  # no records newer than baseline

    gap_identifier = {
        "shard_kind": SHARD_KIND_REPO_EVENTS,
        "repo_full_name": identifier.get("repo_full_name"),
        "owner": owner, "repo": repo,
        "event_type": event_type,
        "installation_id": identifier.get("installation_id"),
        "parent_shard_id": str(shard["id"]),
        "gap_baseline_updated_at": last_seen,
    }
    return ResharedShard(
        shard=Shard(
            shard_kind=SHARD_KIND_REPO_EVENTS,
            shard_identifier=gap_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
        ),
        parent_shard_id=shard["id"],
    )


async def reconcile_github(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    active = [s for s in shards if s["state"] == "done"]
    if not active:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    tenant_id = run["tenant_id"]
    install = await pool.fetchrow(
        """
        SELECT id, tenant_id, provider, installation_id, enabled
          FROM provider_installations
         WHERE tenant_id = $1 AND provider = 'github' AND enabled = TRUE
         LIMIT 1
        """,
        tenant_id,
    )
    if install is None:
        return ReconciliationDecision(has_gaps=False)

    client, close = await _open_github_client(install)
    try:
        new_shards: list[ResharedShard] = []
        for shard in active:
            reshared = await _check_one_shard_for_gap(
                pool=pool, client=client, install=install, shard=shard,
            )
            if reshared is not None:
                new_shards.append(reshared)
    finally:
        await close()

    if new_shards:
        return ReconciliationDecision(
            has_gaps=True, new_shards=new_shards,
            message=f"github reconciler: {len(new_shards)} gap(s).",
        )
    return ReconciliationDecision(has_gaps=False)


RECONCILER_DISPATCH["github"] = reconcile_github


__all__ = [
    "RESHARE_RECENCY_SCORE",
    "SHARD_KIND_REPO_EVENTS",
    "reconcile_github",
    "set_pool_provider",
]
