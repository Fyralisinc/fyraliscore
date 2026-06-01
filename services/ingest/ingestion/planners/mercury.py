"""services/ingest/ingestion/planners/mercury.py — Mercury planner (finance).

Per the Gmail/Calendar/Jira loader precedent (A18.2): Mercury's install record
is account-scoped, and the planner needs the 1-to-N active-account list to emit
one shard per account. The enrichment lives in `mercury_accounts`; the
SourceOnboarding loader JSON-aggregates it into `ctx.install["accounts"]` so the
planner stays stateless (no DB I/O).

Each shard is one account's transaction stream. The fetcher walks
`GET /account/{id}/transactions` on first run, then incrementally via the
per-account transaction high-water cursor.

`ctx.source_client` is None — accounts are read from DB state (populated at
seed/install time by `MercuryClient.list_accounts`).
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_ACCOUNT_TXNS = "mercury_account_txns"


def _decode_accounts(install: Any) -> list[dict[str, Any]]:
    raw = install["accounts"] if "accounts" in install else None
    if raw is None:
        return []
    if isinstance(raw, (str, bytes)):
        try:
            decoded = orjson.loads(raw)
        except orjson.JSONDecodeError:
            return []
    elif isinstance(raw, list):
        decoded = raw
    else:
        return []
    return [a for a in decoded if isinstance(a, dict)]


async def plan_shards_mercury(ctx: PlannerContext) -> list[Shard]:
    """One `mercury_account_txns` shard per active account.

    Reads DB state only (accounts pre-aggregated by the loader), so
    `ctx.source_client` is None — same as Jira/Calendar/Gmail.
    """
    install_id = str(ctx.install["id"])
    accounts = _decode_accounts(ctx.install)

    shards: list[Shard] = []
    for acct in accounts:
        account_id = acct.get("account_id")
        if not isinstance(account_id, str) or not account_id:
            continue
        shards.append(Shard(
            shard_kind=SHARD_KIND_ACCOUNT_TXNS,
            shard_identifier={
                "shard_kind": SHARD_KIND_ACCOUNT_TXNS,
                "account_id": account_id,
                "account_name": acct.get("account_name"),
                "installation_id": install_id,
                # The high-water transaction-createdAt cursor — None on first sync.
                "txn_cursor": acct.get("txn_cursor"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.mercury.planned",
        extra={"account_shards": len(shards), "installation_id": install_id},
    )
    return shards


PLANNER_DISPATCH["mercury"] = plan_shards_mercury


__all__ = ["SHARD_KIND_ACCOUNT_TXNS", "plan_shards_mercury"]
