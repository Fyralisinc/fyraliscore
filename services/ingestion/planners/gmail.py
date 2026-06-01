"""services/ingestion/planners/gmail.py — Gmail backfill planner (M6.3).

Per ingestion LLD §3 (per-source planners) + [05-lld-amendments.md A15]
(M1-shipped onboarding_shards schema) + A17 (Reconciler state machine,
which depends on the planner's shard_kind values) + A18 (M6.3 ships
per-source backfill as net-new code; per-source loaders may aggregate
per-source enrichment data via JSON-aggregating LEFT JOIN).

============================================================
ROLE
============================================================
Decomposes one Gmail install row into a list of `Shard`, one per
ACTIVE mailbox. Single-mailbox installs produce one Shard; multi-
mailbox installs produce N. The shard descriptor identifies the
mailbox by email + Gmail user id; the mailbox's install-time
`history_id` is preserved as a watermark reference for the
reconciler.

============================================================
INSTALL-RECORD ENRICHMENT (S1 amendment / A18)
============================================================
The planner contract is stateless (no DB I/O, no API I/O at plan
time). Gmail's installs are workspace-scoped, so M6.2a's
`_LOAD_GMAIL_INSTALL_SQL` (`services/ingestion/workflows/source_onboarding.py`)
aggregates the active mailbox list as a JSON column via LEFT JOIN
against `gmail_mailbox_watches` filtered to `state='active'`. The
planner orjson-decodes `install["mailboxes"]` and emits one shard
per mailbox.

Mailboxes in non-active states (pending/paused/opted_out/errored)
are excluded by the loader's filter — matching the steady-state
poller's leasing predicate (history_poller.py:55) and the inline-
fetcher's no-watch-row early-exit (fetcher.py:77-80).

If no mailboxes are active (empty inclusion list, all opted out,
etc.) the planner returns an empty list. M6.2a's SourceOnboarding
treats the empty-shard case as a clean run that routes through
Reconciler with pass_count=0, per M6.2b Phase 1 Decision 1.

============================================================
SHARD KIND CONVENTION (per A15 + A17)
============================================================
M6.3 uses TWO shard_kind values for Gmail:

  - `"gmail_mailbox_window"` — initial backfill via users.messages.list.
    Created by THIS planner. The fetcher pages through the entire
    mailbox; the reconciler runs gap detection at end.
  - `"gmail_history_gap"` — re-share gap fill via users.history.list.
    Created by the M6.3 Gmail reconciler (NOT this planner) when a
    gap is detected between the fetcher's final_history_id and the
    mailbox's current historyId.

The fetcher (`services/ingestion/fetchers/gmail.py`) dispatches on
shard_kind to pick the right Gmail API. This is per-source dispatch
inside the per-source fetcher; the M6.2a FETCHER_DISPATCH keys on
`source`, not on shard_kind.

============================================================
TWO-PATH COEXISTENCE (per A18)
============================================================
M6.3 is the BACKFILL path. The existing
`services/integrations/gmail/{fetcher,history_poller,watch_scheduler}.py`
remains the steady-state push/poll path. Both paths write to
`observations` eventually — backfill via Kafka `ingestion.raw` →
normalizer → writer; steady-state via inline dispatch. Coexistence
is documented in A18 and resolved in the M7-territory ticket
"Inline-ingestion path retirement for Gmail steady-state" (filed
in M6.3 Phase 3).

============================================================
WIRE-IN
============================================================
This module assigns into `PLANNER_DISPATCH['gmail']` at import time.
The package `services/ingestion/planners/__init__.py` imports this
module to trigger the assignment. Tests rebind via
`monkeypatch.setitem(PLANNER_DISPATCH, "gmail", test_fn)`.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg
import orjson

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext


SHARD_KIND_MAILBOX_WINDOW = "gmail_mailbox_window"


def _decode_mailboxes(install: asyncpg.Record) -> list[dict[str, Any]]:
    """Decode the JSON-aggregated mailbox list from the install record.

    Per the S1 amendment, M6.2a's `_LOAD_GMAIL_INSTALL_SQL` returns
    a `mailboxes` column as a JSON string (asyncpg returns the json
    type as `str`). Each entry: `{email_address, google_user_id,
    history_id}`. The aggregate is `[]` when no active mailboxes
    exist; this function always returns a list (possibly empty).
    """
    raw = install["mailboxes"]
    if raw is None:
        return []
    if isinstance(raw, (str, bytes)):
        decoded = orjson.loads(raw)
    else:
        decoded = list(raw)
    return decoded if isinstance(decoded, list) else []


async def plan_shards_gmail(ctx: PlannerContext) -> list[Shard]:
    """Return one Shard per active mailbox.

    M6.4 contract: receives `PlannerContext`. Gmail uses only
    `ctx.install` (the S1-amended loader provides `mailboxes`).
    `ctx.conn` and `ctx.source_client` are unused for Gmail.
    """
    mailboxes = _decode_mailboxes(ctx.install)
    return [
        Shard(
            shard_kind=SHARD_KIND_MAILBOX_WINDOW,
            shard_identifier={
                # Mirror of the row's shard_kind column. The Gmail
                # fetcher dispatches on this to pick the right Gmail
                # API (messages.list vs history.list). The row column
                # serves indexes / operator queries; the identifier
                # field is the fetcher's wire to dispatch.
                "shard_kind": SHARD_KIND_MAILBOX_WINDOW,
                "mailbox_email": mb["email_address"],
                "user_id": mb.get("google_user_id"),
                # The install-time history_id is the reference point
                # for the M6.3 reconciler's gap detection. It may be
                # NULL if the watch is still 'pending' (race against
                # provision); the reconciler must handle NULL.
                "initial_history_id": mb.get("history_id"),
            },
            # No per-mailbox recency boost in initial backfill. The
            # Reconciler's gap-fill shards use 1.5 per A17.
            recency_score=1.0,
            window_start=None,
            window_end=None,
        )
        for mb in mailboxes
        if mb.get("email_address")
    ]


# Wire into the dispatch table at module-import time. Module-level
# assignment is intentional (same shape M6.4-M6.6 will follow).
PLANNER_DISPATCH["gmail"] = plan_shards_gmail


__all__ = ["SHARD_KIND_MAILBOX_WINDOW", "plan_shards_gmail"]
