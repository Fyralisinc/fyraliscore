"""services/ingest/ingestion/planners/aws.py — AWS planner (IN-AWS).

Per ingestion LLD §3 + the Grafana loader precedent (A18.2). Like Grafana
(annotations are org-wide), AWS CloudTrail management events are account/region-
wide — there is no per-resource sub-table, so the planner emits exactly ONE
`aws_account_events` shard per install. The shard streams the account/region's
management events via `CloudTrail:LookupEvents` over a time window.

`ctx.source_client` is None — the planner reads only the install row (loaded by
SourceOnboarding's `_LOAD_AWS_INSTALL_SQL`), same as Grafana/Calendar/Drive.
"""
from __future__ import annotations

import logging

from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_ACCOUNT_EVENTS = "aws_account_events"


async def plan_shards_aws(ctx: PlannerContext) -> list[Shard]:
    """One `aws_account_events` shard for the install.

    Reads DB state only (`ctx.source_client` is None), so it stays stateless —
    same as Grafana/Calendar/Drive. The warm-start cursor (`events_cursor_ms`,
    the high-water event `eventTime` in epoch ms) is carried into the shard so a
    re-onboarding runs incrementally; None on first sync -> full time-window walk.
    """
    install = ctx.install
    install_id = str(install["id"])
    account_id = install["account_id"] if "account_id" in install else None
    if not account_id:
        log.warning("planners.aws.no_account_id", extra={"installation_id": install_id})
        return []

    region = install["region"] if "region" in install else "us-east-1"
    updated_cursor = (
        install["events_cursor_ms"] if "events_cursor_ms" in install else None
    )

    shard = Shard(
        shard_kind=SHARD_KIND_ACCOUNT_EVENTS,
        shard_identifier={
            "shard_kind": SHARD_KIND_ACCOUNT_EVENTS,
            "installation_id": install_id,
            "account_id": account_id,
            "region": region,
            # High-water event `eventTime` (epoch ms) — None on first full sync.
            "updated_cursor": updated_cursor,
        },
        recency_score=1.0,
        window_start=None,
        window_end=None,
    )

    log.info(
        "planners.aws.planned",
        extra={"installation_id": install_id, "shards": 1},
    )
    return [shard]


PLANNER_DISPATCH["aws"] = plan_shards_aws


__all__ = ["SHARD_KIND_ACCOUNT_EVENTS", "plan_shards_aws"]
