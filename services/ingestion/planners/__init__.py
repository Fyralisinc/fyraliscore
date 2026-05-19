"""Per-source shard planners. Per ingestion LLD §3.

============================================================
ROLE
============================================================
A planner is a per-source function that decomposes ONE tenant install
into a list of `Shard` rows, each describing a unit of fetch work
(a Slack channel + time window, a GitHub repo, a Gmail mailbox + time
window, a Discord guild channel + time window). The M6.2a
SourceOnboarding service calls these planners; M6.2a's ShardFetch
service then consumes each `Shard`.

The contract — codified by M6.2a Phase 1:

    PLANNER_DISPATCH[source](
        tenant_id: UUID,
        install: asyncpg.Record,
    ) -> list[Shard]

The planner is invoked with the row from `provider_installations`
(for slack/github/discord) or `gmail_installations` (for gmail) and
returns a list of `Shard` dataclasses ready for INSERT into the
M1-shipped `onboarding_shards` table (LLD §1.2; migration 0045).

============================================================
DISPATCH TABLE — STUB ON M6.2a, REAL ON M6.3-M6.6
============================================================
M6.2a Phase 1 ships the dispatch table with `NotImplementedError`
stubs for every source. Each per-source M6.x sub-block replaces ONE
stub with a real implementation:

    | source   | shard_kind value           | M6.x sub-block |
    |----------|----------------------------|----------------|
    | gmail    | "gmail_mailbox_window"     | M6.3           |
    | github   | "github_repo_events"       | M6.4           |
    | slack    | "slack_channel_window"     | M6.5           |
    | discord  | "discord_channel_window"   | M6.6           |

Same fail-loud pattern as M5.1's deferred Kafka readers (which raise
`NotImplementedError` until M-Temporal wires the real ones). The
SourceOnboarding service catches `NotImplementedError`, marks the
parent `source_onboarding_runs` row 'failed' with an informative
reason, and emits `source_onboarding_completed` with failure status.
This is the pre-M6.3 expected steady state.

Tests inject test planners via `PLANNER_DISPATCH[source] =
<test_fn>` (or `monkeypatch.setitem(PLANNER_DISPATCH, ...)`). The
dispatch table is module-level on purpose: it's the public API both
production code and tests bind against.

============================================================
SHARD DATACLASS
============================================================
A `Shard` is the planner's output format — the minimum a planner
must produce for SourceOnboarding to INSERT a row into the existing
`onboarding_shards` table (LLD §1.2; M1-shipped 0045 schema):

  - `shard_kind` (TEXT NOT NULL) — per-source convention per A15.
  - `shard_identifier` (JSONB NOT NULL) — per-source identity (e.g.
    `{"channel_id": "C123"}` for Slack).
  - `recency_score` (DOUBLE PRECISION NOT NULL) — LLD §1.2 +
    `exp(-age_days/τ)`; higher = run earlier. Test planners use 1.0.
  - `window_start` / `window_end` (TIMESTAMPTZ NULLABLE) — fetch
    window. Both NULL = "all time"; LLD §3 per-source.

See [docs/ingestion/05-lld-amendments.md A15](../../../docs/ingestion/05-lld-amendments.md#a15--m62a-uses-m1-shipped-onboarding_shards-schema-no-new-migration)
for the column-naming map and the full M6.2a-prompt-words →
existing-schema-columns reconciliation.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable
from uuid import UUID

import asyncpg


# ---------------------------------------------------------------------
# Public types.
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class Shard:
    """Planner output: one row to INSERT into `onboarding_shards`."""

    shard_kind: str
    shard_identifier: dict[str, Any]
    recency_score: float = 1.0
    window_start: dt.datetime | None = None
    window_end: dt.datetime | None = None


# `Planner` type alias for readability and dispatch-table typing.
# Updated in M6.4 per [A18.6] — planners receive a `PlannerContext`
# bundle (install + conn + per-source API client). Gmail uses only
# `ctx.install`; GitHub uses `ctx.source_client` for API enumeration.
from services.ingestion.planners.context import PlannerContext  # noqa: E402

Planner = Callable[[PlannerContext], Awaitable[list[Shard]]]


# ---------------------------------------------------------------------
# NotImplementedError stub factory.
# ---------------------------------------------------------------------
def _not_implemented_planner(source: str, milestone: str) -> Planner:
    """Build a planner stub that raises `NotImplementedError` loudly."""
    async def stub(ctx: PlannerContext) -> list[Shard]:
        raise NotImplementedError(
            f"Planner for source={source!r} is not yet implemented. "
            f"Pending in {milestone} per "
            f"docs/ingestion/04-implementation-plan.md §{milestone}."
        )
    stub.__name__ = f"_not_implemented_planner_{source}"
    return stub


# ---------------------------------------------------------------------
# Dispatch table — M6.2a Phase 1.
# ---------------------------------------------------------------------
# Module-level mutable dict by design: the public API. Tests rebind
# entries via `PLANNER_DISPATCH[source] = test_fn`. Production
# replacements (M6.3-M6.6) overwrite the stub at module-import time
# (i.e., the per-source planner module assigns into the dict during its
# import, replacing the stub).
PLANNER_DISPATCH: dict[str, Planner] = {
    "gmail":   _not_implemented_planner("gmail",   "M6.3"),
    "github":  _not_implemented_planner("github",  "M6.4"),
    "slack":   _not_implemented_planner("slack",   "M6.5"),
    "discord": _not_implemented_planner("discord", "M6.6"),
}


__all__ = [
    "PLANNER_DISPATCH",
    "Planner",
    "PlannerContext",
    "Shard",
]


# Per-source modules import below — each assigns into PLANNER_DISPATCH
# at module-load time (per A18 — M6.3 establishes this wire-in pattern).
# Order is informational only; assignments are last-wins, but each
# source touches a distinct key so ordering doesn't matter.
from services.ingestion.planners import gmail as _gmail  # noqa: E402,F401
from services.ingestion.planners import github as _github  # noqa: E402,F401
from services.ingestion.planners import slack as _slack  # noqa: E402,F401
from services.ingestion.planners import discord as _discord  # noqa: E402,F401
