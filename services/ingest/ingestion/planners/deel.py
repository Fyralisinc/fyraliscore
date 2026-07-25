"""services/ingest/ingestion/planners/deel.py — Deel planner (finance).

Per the Gmail/Calendar/Jira loader precedent (A18.2): Deel's install record
is contract-scoped, and the planner needs the 1-to-N active-contract list to emit
one shard per contract. The enrichment lives in `deel_contracts`; the
SourceOnboarding loader JSON-aggregates it into `ctx.install["contracts"]` so the
planner stays stateless (no DB I/O).

TODO(human): confirm the Deel resource taxonomy to shard on. The archetype
shards one shard per contract and streams that contract's payments; if the
verified read surface is org-wide rather than per-contract, collapse to one
shard per install instead.

Each shard is one contract's payment stream. The fetcher walks
`GET /contract/{id}/payments` on first run, then incrementally via the
per-contract payment high-water cursor.

`ctx.source_client` is None — contracts are read from DB state (populated at
seed/install time by `DeelClient.list_contracts`).
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_CONTRACT_PAYMENTS = "deel_contract_payments"


def _decode_contracts(install: Any) -> list[dict[str, Any]]:
    raw = install["contracts"] if "contracts" in install else None
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
    return [c for c in decoded if isinstance(c, dict)]


async def plan_shards_deel(ctx: PlannerContext) -> list[Shard]:
    """One `deel_contract_payments` shard per active contract.

    Reads DB state only (contracts pre-aggregated by the loader), so
    `ctx.source_client` is None — same as Jira/Calendar/Gmail.
    """
    install_id = str(ctx.install["id"])
    contracts = _decode_contracts(ctx.install)

    shards: list[Shard] = []
    for con in contracts:
        contract_id = con.get("contract_id")
        if not isinstance(contract_id, str) or not contract_id:
            continue
        shards.append(Shard(
            shard_kind=SHARD_KIND_CONTRACT_PAYMENTS,
            shard_identifier={
                "shard_kind": SHARD_KIND_CONTRACT_PAYMENTS,
                "contract_id": contract_id,
                "contract_name": con.get("contract_name"),
                "installation_id": install_id,
                # The high-water payment-createdAt cursor — None on first sync.
                "payment_cursor": con.get("payment_cursor"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.deel.planned",
        extra={"contract_shards": len(shards), "installation_id": install_id},
    )
    return shards




__all__ = ["SHARD_KIND_CONTRACT_PAYMENTS", "plan_shards_deel"]
