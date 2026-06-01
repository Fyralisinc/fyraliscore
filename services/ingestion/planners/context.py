"""services/ingestion/planners/context.py — PlannerContext (M6.4 / A18.6).

Per [05-lld-amendments.md A18.6] (per-source planner data access via
context object — M6.4 substrate extension). Planners that need DB
access (Gmail-style: read pre-resolved per-source enrichment tables)
or API access (GitHub-style: enumerate repos via source client at
plan time) receive a context with whichever surfaces they need.

============================================================
ROLE
============================================================
The original M6.2a planner contract was
`Callable[[UUID, asyncpg.Record], Awaitable[list[Shard]]]`. M6.3
showed that Gmail's planner needs the install record enriched with
mailboxes via M6.2a's loader (the A18.2 pattern). M6.4 surfaces a
NEW class of need: GitHub's planner enumerates repos via the
source-side API at plan time, which the loader cannot help with.

PlannerContext supersedes the old signature with a single object
carrying all three surfaces:

    PlannerContext:
      - install: asyncpg.Record  — same as before; per-source loader
        may have enriched this with JSON-aggregated columns.
      - conn: asyncpg.Connection — the in-transaction connection from
        SourceOnboarding's call site. Per-source planners that need
        DB access (rare; Gmail uses the loader pattern instead) can
        query directly.
      - source_client: Any | None — a per-source API client (e.g.,
        GithubClient for GitHub). Built by source_onboarding's
        `_build_source_client(source)` based on env credentials.
        `None` for sources whose planner doesn't need API access
        (Gmail).
      - tenant_id: UUID — explicit tenant scope for the planner.

============================================================
BACKWARD COMPATIBILITY
============================================================
M6.3 Gmail's planner is refactored as part of M6.4 to accept the
new context shape. The Planner type alias is updated; per-source
planners take a single argument going forward. The change is NOT
backward-compatible at the type level (callers of PLANNER_DISPATCH
must build a PlannerContext); this is intentional per A18.6's
"single contract" framing.

============================================================
TEST USAGE
============================================================
Tests construct PlannerContext directly with fakes:

    ctx = PlannerContext(
        tenant_id=uuid4(),
        install=_FakeRecord(...),
        conn=_FakeConn(),
        source_client=_FakeGithubClient(),
    )
    shards = await plan_shards_github(ctx)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg


@dataclass(frozen=True)
class PlannerContext:
    """Bundle of plan-time surfaces a per-source planner may need.

    Stays a plain frozen dataclass (not Pydantic) because two of its
    fields (`conn`, `source_client`) carry types that aren't worth
    schema-validating at the dispatch boundary.
    """

    tenant_id: UUID
    install: asyncpg.Record
    conn: asyncpg.Connection
    source_client: Any | None = None


__all__ = ["PlannerContext"]
