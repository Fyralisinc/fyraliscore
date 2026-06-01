"""services/ingestion/reconcilers/gmail.py — Gmail gap-detection (M6.3).

Per ingestion LLD §3 (per-source reconcilers) + [05-lld-amendments.md A17]
(Reconciler state machine + re-share semantics + recency_score=1.5
boost) + A18 (M6.3 ships per-source backfill as net-new code; this
reconciler is its gap-detection half).

============================================================
ROLE
============================================================
After SourceOnboarding rolls up "all Gmail shards done", the M6.2b
Reconciler service hands off (run, shards) to THIS function. We
decide CLEAN (no gap) vs RE-SHARE (gap exists, emit new shards).

For each terminal shard in the (run, source) pair:
  1. Extract the shard's `final_history_id` from its cursor (stored
     in workflow_states.state_data["cursor"], not on the shard row).
     The fetcher's last page stamped this via users.getProfile.
  2. Call users.getProfile NOW to read the mailbox's CURRENT
     historyId.
  3. If current > final_history_id (numerically), gap exists.
     Build a `gmail_history_gap` shard for the range; mark it as a
     child of the original via `parent_shard_id`.
  4. If current <= final_history_id, this mailbox is clean.

Returns `ReconciliationDecision(has_gaps, new_shards)`. M6.2b's
Reconciler service handles the rest of the state machine.

============================================================
SHARD-KIND HANDLING
============================================================
Two shard kinds reach this reconciler:

  - `gmail_mailbox_window` (planner-created): the normal backfill
    shard. Has `final_history_id` in its cursor; gap-checked.
  - `gmail_history_gap` (a prior reconciler pass created it): a
    gap-fill that has now completed. Its `final_history_id` should
    match its `end_history_id` (the range upper bound the prior
    reshare aimed for). Re-check just to be safe — if new mail
    arrived during gap-fill, ANOTHER pass may be needed (the
    `pending → in_progress → completed → ...` cycle continues).

Shards in `reconciliation_resharded` state are EXCLUDED — they've
been superseded by a child shard whose own check supersedes them.

============================================================
RECENCY BOOST (per A17)
============================================================
Gap-fill shards use `recency_score = 1.5` so they run ahead of any
remaining low-recency backfill in concurrent multi-tenant fetcher
processing. This is per LLD §3's recency policy and is set at
reconciliation time, not at planner time.

============================================================
NULL final_history_id HANDLING
============================================================
If a shard's `final_history_id` is None (e.g., the fetcher
end-of-data'd on the very first page WITHOUT calling getProfile, or
the watch was 'pending' at plan time and `initial_history_id` was
NULL), we cannot detect a gap deterministically. The conservative
choice is `has_gaps=False` for that shard — there's no reference
point to compare against. Operator runbook §6.D documents this as a
known limitation; the F4 retrofit (OAuth → onboarding_triggers) is
the long-term resolution because it ensures the watch is `active`
(with an `initial_history_id`) before the planner runs.

============================================================
WIRE-IN
============================================================
This module assigns into `RECONCILER_DISPATCH['gmail']` at import
time; `services/ingestion/reconcilers/__init__.py` imports this
module to trigger the assignment. Tests rebind via
`monkeypatch.setitem(RECONCILER_DISPATCH, "gmail", test_fn)`.
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
from services.ingestion.workflows.retry import retry_with_backoff_on_429
from services.ingestion.workflows.state import load_state
from services.integrations.gmail.client import (
    GMAIL_METADATA_SCOPE,
    GMAIL_READONLY_SCOPE,
    GmailClient,
    GoogleApiError,
    GoogleHttpClient,
    GoogleRateLimited,
)
from services.integrations.gmail.dwd import get_minter


log = logging.getLogger(__name__)


SHARD_KIND_MAILBOX_WINDOW = "gmail_mailbox_window"
SHARD_KIND_HISTORY_GAP = "gmail_history_gap"

# Per A17: gap-fill shards get a recency boost to run ahead of any
# remaining low-recency backfill.
RESHARE_RECENCY_SCORE = 1.5


_SCOPE_ALIAS = {
    "gmail.metadata": GMAIL_METADATA_SCOPE,
    "gmail.readonly": GMAIL_READONLY_SCOPE,
}


# ---------------------------------------------------------------------
# Pool-acquisition seam — tests inject the test pool here.
# ---------------------------------------------------------------------
# The reconciler reads workflow_states (for shard cursors) outside the
# Reconciler service's own transaction. We need a pool reference; the
# RECONCILER_DISPATCH contract doesn't pass one, so we resolve via
# this module-level seam.
_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    """Register a callable / pool for the reconciler to use.

    Production wires this in via the service startup; tests rebind
    per-test. The provider may be an `asyncpg.Pool` directly or a
    callable returning one. If unset, `_get_pool` raises.
    """
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.gmail: pool provider not registered. Call "
            "set_pool_provider(pool) from service startup before "
            "the reconciler runs."
        )
    if callable(_pool_provider) and not hasattr(_pool_provider, "acquire"):
        return _pool_provider()
    return _pool_provider


# ---------------------------------------------------------------------
# Gmail-client factory hook (test seam, mirrors fetchers/gmail.py).
# ---------------------------------------------------------------------
async def _open_gmail_client(install_or_shard_install: asyncpg.Record):  # noqa: ANN202
    """Yield (gmail_client, http_close_callable). Same shape as
    fetchers/gmail.py:_open_gmail_client; tests patch this symbol."""
    minter = get_minter()
    http = GoogleHttpClient(minter)
    await http.__aenter__()

    async def close() -> None:
        await http.__aexit__(None, None, None)

    return GmailClient(http), close


# ---------------------------------------------------------------------
# Install-load helper.
# ---------------------------------------------------------------------
_LOAD_GMAIL_INSTALL_FOR_RECONCILE_SQL = """
SELECT id, tenant_id, workspace_domain, service_account_email,
       scope, disabled_at
  FROM gmail_installations
 WHERE id = $1 AND disabled_at IS NULL
"""


async def _load_install_for_run(
    pool: Any, *, tenant_id: Any, scope_hint: dict[str, Any] | None = None,
) -> asyncpg.Record | None:
    """Load the Gmail install for the run's tenant.

    Tenancy is the right scope (1 active install per tenant). We
    don't use the S1-amended loader from M6.2a here because it
    aggregates mailboxes — we just need the install row for scope +
    workspace_domain. Local helper keeps the reconciler decoupled
    from the planner's loader.
    """
    _ = scope_hint  # reserved for future per-source disambiguation
    return await pool.fetchrow(
        """
        SELECT id, tenant_id, workspace_domain, service_account_email,
               scope, disabled_at
          FROM gmail_installations
         WHERE tenant_id = $1 AND disabled_at IS NULL
         LIMIT 1
        """,
        tenant_id,
    )


# ---------------------------------------------------------------------
# Cursor load helper — read final_history_id from workflow_states.
# ---------------------------------------------------------------------
async def _load_shard_final_history_id(
    pool: Any, shard_id: Any,
) -> str | None:
    """Read the shard's `final_history_id` from the N1 home.

    The fetcher's last page stamped this; if it's missing the
    fetcher never completed a full pass and we can't gap-check. The
    reconciler treats missing as 'no reference point' (clean for
    that shard).
    """
    state = await load_state(pool, "shard_fetch", str(shard_id))
    if state is None:
        return None
    cursor = state.state_data.get("cursor") if state.state_data else None
    if not isinstance(cursor, dict):
        return None
    fhi = cursor.get("final_history_id")
    return str(fhi) if fhi is not None else None


# ---------------------------------------------------------------------
# Per-shard gap detection.
# ---------------------------------------------------------------------
def _decode_identifier(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, (str, bytes)):
        return orjson.loads(raw)
    return dict(raw)


async def _check_one_mailbox_for_gap(
    *,
    pool: Any,
    gmail: GmailClient,
    install: asyncpg.Record,
    shard: asyncpg.Record,
) -> ResharedShard | None:
    """Return a ResharedShard if this mailbox has a gap, else None."""
    identifier = _decode_identifier(shard["shard_identifier"])
    mailbox_email = identifier.get("mailbox_email")
    user_id = identifier.get("user_id")
    if not mailbox_email:
        log.warning(
            "reconcilers.gmail.missing_mailbox_email",
            extra={"shard_id": str(shard["id"])},
        )
        return None

    final_history_id = await _load_shard_final_history_id(pool, shard["id"])
    if final_history_id is None:
        # No reference point — see module docstring on NULL handling.
        return None

    scope_long = _SCOPE_ALIAS.get(install["scope"])
    if scope_long is None:
        log.warning(
            "reconcilers.gmail.unknown_scope",
            extra={"scope": install["scope"]},
        )
        return None

    try:
        profile = await retry_with_backoff_on_429(
            lambda: gmail.get_profile(
                user_email=mailbox_email, scope=scope_long,
            ),
            retry_on=GoogleRateLimited,
        )
    except GoogleApiError as exc:
        # Failure to call getProfile is conservatively treated as
        # "can't determine gap" — clean for this pass. The next
        # periodic reconciliation (M6.x Phase 5+) would re-check.
        log.warning(
            "reconcilers.gmail.get_profile_failed",
            extra={
                "mailbox_email": mailbox_email,
                "error": str(exc)[:200],
            },
        )
        return None

    current_history_id = profile.get("historyId")
    if current_history_id is None:
        return None

    try:
        current_i = int(current_history_id)
        final_i = int(final_history_id)
    except (TypeError, ValueError):
        log.warning(
            "reconcilers.gmail.non_numeric_history_id",
            extra={
                "mailbox_email": mailbox_email,
                "final_history_id": final_history_id,
                "current_history_id": str(current_history_id),
            },
        )
        return None

    if current_i <= final_i:
        return None  # mailbox is clean

    # Gap detected. Build the gap-fill shard.
    gap_identifier = {
        "shard_kind": SHARD_KIND_HISTORY_GAP,
        "mailbox_email": mailbox_email,
        "user_id": user_id,
        "start_history_id": final_history_id,
        "end_history_id": str(current_i),
        "parent_shard_id": str(shard["id"]),
    }
    return ResharedShard(
        shard=Shard(
            shard_kind=SHARD_KIND_HISTORY_GAP,
            shard_identifier=gap_identifier,
            recency_score=RESHARE_RECENCY_SCORE,
            window_start=None,
            window_end=None,
        ),
        parent_shard_id=shard["id"],
    )


# ---------------------------------------------------------------------
# Main entrypoint — RECONCILER_DISPATCH['gmail'].
# ---------------------------------------------------------------------
async def reconcile_gmail(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    """Per-mailbox gap detection across all done shards for this run.

    Excludes `reconciliation_resharded` shards (they've been
    superseded). Per A17, returns `has_gaps=True` with one new
    `gmail_history_gap` shard per mailbox that has accumulated new
    history since the fetcher's `final_history_id`.
    """
    # Filter shards: only ones that are 'done' contribute to the
    # gap check. `failed` and `reconciliation_resharded` are skipped.
    active_shards = [s for s in shards if s["state"] == "done"]
    if not active_shards:
        return ReconciliationDecision(has_gaps=False)

    pool = _get_pool()
    tenant_id = run["tenant_id"]
    install = await _load_install_for_run(pool, tenant_id=tenant_id)
    if install is None:
        # Install gone (disabled mid-flight). Nothing to reconcile;
        # treat as clean. M6.2b's Reconciler service stamps
        # reconciled_at and emits source_onboarding_completed; the
        # downstream consumer handles the "install missing" follow-
        # up.
        log.warning(
            "reconcilers.gmail.install_missing",
            extra={
                "tenant_id": str(tenant_id),
                "run_id": str(run["onboarding_run_id"]),
            },
        )
        return ReconciliationDecision(has_gaps=False)

    gmail, close = await _open_gmail_client(install)
    try:
        new_shards: list[ResharedShard] = []
        for shard in active_shards:
            reshared = await _check_one_mailbox_for_gap(
                pool=pool, gmail=gmail, install=install, shard=shard,
            )
            if reshared is not None:
                new_shards.append(reshared)
    finally:
        await close()

    if new_shards:
        return ReconciliationDecision(
            has_gaps=True,
            new_shards=new_shards,
            message=(
                f"Gmail reconciler detected {len(new_shards)} mailbox gap(s); "
                f"resharding with shard_kind={SHARD_KIND_HISTORY_GAP!r}."
            ),
        )
    return ReconciliationDecision(has_gaps=False)


# Wire into the dispatch table at module-import time.
RECONCILER_DISPATCH["gmail"] = reconcile_gmail


__all__ = [
    "RESHARE_RECENCY_SCORE",
    "SHARD_KIND_HISTORY_GAP",
    "SHARD_KIND_MAILBOX_WINDOW",
    "reconcile_gmail",
    "set_pool_provider",
]
