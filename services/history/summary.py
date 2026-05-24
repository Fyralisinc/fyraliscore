"""services/history/summary.py — Ledger summary counters.

Drives the `/v1/history/summary` endpoint (spec §6.1 summary strip).
Returns six counters with WoW deltas:

  * events             — total events in the window
  * model_updates      — substrate-level updates
  * predictions_made   — predictions filed in the window
  * predictions_accuracy — calibration over resolved predictions
  * actions_taken      — commitment / decision actions
  * contestations      — contested claims; split = "N unresolved"

Comparison window is the period immediately preceding `range_days`
(so range_days=30 compares the most-recent 30 days against the 30
days before that).

Predictions counters resolve from the `predictions` table when it
exists (Phase 4 agent migration 0041). When the table is absent,
prediction counters fall back to zero — the rest of the summary
remains valid.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg

import structlog


log = structlog.get_logger("history.summary")


# ---------------------------------------------------------------------
# Output shapes — JSON-serialisable dicts. See spec §6.1.
# ---------------------------------------------------------------------


def _counter(value: int, delta_pct: float, delta_label: str) -> dict[str, Any]:
    return {
        "value": value,
        "delta_pct": round(delta_pct, 4),
        "delta_label": delta_label,
    }


def _counter_split(value: int, split: str) -> dict[str, Any]:
    return {"value": value, "split": split}


def _counter_pp(value: float, delta_pp: float, delta_label: str) -> dict[str, Any]:
    return {
        "value": round(value, 4),
        "delta_pp": round(delta_pp, 4),
        "delta_label": delta_label,
    }


def _pct_change(current: int, previous: int) -> float:
    """Return (current - previous) / previous; 0 when previous == 0
    so we never divide by zero."""
    if previous <= 0:
        return 0.0
    return (current - previous) / previous


def _fmt_pct(delta_pct: float, suffix: str = "") -> str:
    sign = "+" if delta_pct >= 0 else ""
    return f"{sign}{int(round(delta_pct * 100))}%{suffix}"


# ---------------------------------------------------------------------
# Per-counter queries
# ---------------------------------------------------------------------


_EVENTS_COUNT_SQL = """
SELECT count(*) FROM observations
WHERE tenant_id = $1 AND occurred_at >= $2 AND occurred_at < $3
"""


_MODEL_UPDATES_COUNT_SQL = """
SELECT count(*) FROM (
  -- Models created in window.
  SELECT id FROM models
  WHERE tenant_id = $1 AND created_at >= $2 AND created_at < $3
  UNION ALL
  -- Models archived in window.
  SELECT id FROM models
  WHERE tenant_id = $1 AND archived_at IS NOT NULL
    AND archived_at >= $2 AND archived_at < $3
) AS s
"""


_ACTIONS_TAKEN_COUNT_SQL = """
SELECT count(*) FROM observations
WHERE tenant_id = $1
  AND kind = 'state_change'
  AND occurred_at >= $2 AND occurred_at < $3
"""


_CONTESTATIONS_COUNT_SQL = """
SELECT count(*) FROM models
WHERE tenant_id = $1
  AND contested_count > 0
  AND (
    last_confirmed_at IS NULL
    OR last_confirmed_at < $2
  )
  AND created_at < $3
"""


_CONTESTATIONS_UNRESOLVED_SQL = """
SELECT count(*) FROM models
WHERE tenant_id = $1
  AND status = 'active'
  AND contested_count > confirmed_count
"""


# Predictions counters. The `predictions` table is created by Phase 4
# (migration 0041). Until then, these queries will raise; we catch
# UndefinedTableError and fall back to zeros.

_PREDICTIONS_MADE_SQL = """
SELECT
  count(*) FILTER (WHERE made_at >= $2 AND made_at < $3) AS made,
  count(*) FILTER (
    WHERE made_at >= $2 AND made_at < $3 AND resolved_at IS NOT NULL
  ) AS resolved,
  count(*) FILTER (
    WHERE made_at >= $2 AND made_at < $3 AND resolved_at IS NULL
  ) AS active
FROM predictions
WHERE tenant_id = $1
"""


_PREDICTIONS_ACCURACY_SQL = """
WITH window_res AS (
  SELECT resolution_correct
  FROM predictions
  WHERE tenant_id = $1
    AND resolved_at IS NOT NULL
    AND resolved_at >= $2 AND resolved_at < $3
),
prior_res AS (
  SELECT resolution_correct
  FROM predictions
  WHERE tenant_id = $1
    AND resolved_at IS NOT NULL
    AND resolved_at >= $4 AND resolved_at < $2
)
SELECT
  (SELECT count(*) FROM window_res) AS w_total,
  (SELECT count(*) FILTER (WHERE resolution_correct) FROM window_res) AS w_correct,
  (SELECT count(*) FROM prior_res) AS p_total,
  (SELECT count(*) FILTER (WHERE resolution_correct) FROM prior_res) AS p_correct
"""


# ---------------------------------------------------------------------
# Resilient helpers
# ---------------------------------------------------------------------


# Postgres errors we treat as "expected absence" of the predictions
# table (or its expected columns). Anything outside this set must
# propagate so genuine schema bugs aren't silently swallowed.
_PREDICTIONS_ABSENT_ERRORS = (
    asyncpg.UndefinedTableError,
    asyncpg.UndefinedColumnError,
)


async def _safe_fetchrow(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
    savepoint: str,
    log_event: str,
) -> asyncpg.Record | None:
    """fetchrow wrapped in a SAVEPOINT.

    Why a SAVEPOINT: when a `prepare` fails (UndefinedTable /
    UndefinedColumn) inside an open transaction, Postgres aborts the
    whole transaction. The caller passed us their `conn` — we can't
    take the whole call site down because the predictions table
    hasn't shipped yet. SAVEPOINT + ROLLBACK TO restores the
    transaction to a clean state so subsequent queries succeed.
    """
    sp = f"hist_summary_{savepoint}"
    await conn.execute(f"SAVEPOINT {sp}")
    try:
        row = await conn.fetchrow(query, *args)
    except _PREDICTIONS_ABSENT_ERRORS as exc:
        await conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
        log.info(log_event, error=str(exc))
        return None
    await conn.execute(f"RELEASE SAVEPOINT {sp}")
    return row


# ---------------------------------------------------------------------
# Public builder
# ---------------------------------------------------------------------


async def build_summary(
    *,
    tenant_id: UUID,
    range_days: int = 30,
    conn: asyncpg.Connection,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return the Ledger summary payload — six counters with WoW deltas.

    `range_days` is the trailing window (default 30). The comparison
    window is the same length immediately preceding.
    """
    if range_days <= 0:
        range_days = 30
    now = now or datetime.now(timezone.utc)
    cur_end = now
    cur_start = now - timedelta(days=range_days)
    prev_end = cur_start
    prev_start = cur_start - timedelta(days=range_days)

    # events
    events_cur = int(
        await conn.fetchval(_EVENTS_COUNT_SQL, tenant_id, cur_start, cur_end)
        or 0
    )
    events_prev = int(
        await conn.fetchval(_EVENTS_COUNT_SQL, tenant_id, prev_start, prev_end)
        or 0
    )
    events_delta = _pct_change(events_cur, events_prev)

    # model_updates
    mu_cur = int(
        await conn.fetchval(
            _MODEL_UPDATES_COUNT_SQL, tenant_id, cur_start, cur_end,
        )
        or 0
    )
    mu_prev = int(
        await conn.fetchval(
            _MODEL_UPDATES_COUNT_SQL, tenant_id, prev_start, prev_end,
        )
        or 0
    )
    mu_delta = _pct_change(mu_cur, mu_prev)

    # actions_taken
    actions_cur = int(
        await conn.fetchval(
            _ACTIONS_TAKEN_COUNT_SQL, tenant_id, cur_start, cur_end,
        )
        or 0
    )
    actions_prev = int(
        await conn.fetchval(
            _ACTIONS_TAKEN_COUNT_SQL, tenant_id, prev_start, prev_end,
        )
        or 0
    )
    actions_delta = _pct_change(actions_cur, actions_prev)

    # contestations
    contestations_total = int(
        await conn.fetchval(
            _CONTESTATIONS_COUNT_SQL, tenant_id, cur_start, cur_end,
        )
        or 0
    )
    contestations_unresolved = int(
        await conn.fetchval(_CONTESTATIONS_UNRESOLVED_SQL, tenant_id)
        or 0
    )

    # predictions counters — resilient to the predictions table being
    # absent OR having a different (pre-0041) schema. Each query is
    # wrapped in a SAVEPOINT so a failed prepare doesn't abort the
    # caller's transaction.
    predictions_made_val = 0
    predictions_made_split = "0 resolved \u00b7 0 active"
    predictions_accuracy_value = 0.0
    predictions_accuracy_delta_pp = 0.0
    predictions_accuracy_label = "no resolved predictions"

    row = await _safe_fetchrow(
        conn,
        _PREDICTIONS_MADE_SQL,
        tenant_id, cur_start, cur_end,
        savepoint="predictions_made",
        log_event="history.summary.predictions_table_absent",
    )
    if row is not None:
        made = int(row["made"] or 0)
        resolved = int(row["resolved"] or 0)
        active = int(row["active"] or 0)
        predictions_made_val = made
        predictions_made_split = (
            f"{resolved} resolved \u00b7 {active} active"
        )

    row = await _safe_fetchrow(
        conn,
        _PREDICTIONS_ACCURACY_SQL,
        tenant_id, cur_start, cur_end, prev_start,
        savepoint="predictions_accuracy",
        log_event="history.summary.predictions_accuracy_table_absent",
    )
    if row is not None:
        w_total = int(row["w_total"] or 0)
        w_correct = int(row["w_correct"] or 0)
        p_total = int(row["p_total"] or 0)
        p_correct = int(row["p_correct"] or 0)
        if w_total > 0:
            predictions_accuracy_value = w_correct / w_total
            if p_total > 0:
                prev_acc = p_correct / p_total
                predictions_accuracy_delta_pp = (
                    predictions_accuracy_value - prev_acc
                )
            sign = "+" if predictions_accuracy_delta_pp >= 0 else ""
            pp_int = int(round(predictions_accuracy_delta_pp * 100))
            predictions_accuracy_label = (
                f"{sign}{pp_int}pp last {range_days} days"
            )

    return {
        "events": _counter(
            events_cur,
            events_delta,
            f"{_fmt_pct(events_delta)} vs prev period",
        ),
        "model_updates": _counter(
            mu_cur, mu_delta, _fmt_pct(mu_delta),
        ),
        "predictions_made": _counter_split(
            predictions_made_val, predictions_made_split,
        ),
        "predictions_accuracy": _counter_pp(
            predictions_accuracy_value,
            predictions_accuracy_delta_pp,
            predictions_accuracy_label,
        ),
        "actions_taken": _counter(
            actions_cur, actions_delta, _fmt_pct(actions_delta),
        ),
        "contestations": _counter_split(
            contestations_total,
            f"{contestations_unresolved} unresolved",
        ),
        "range_days": range_days,
    }


__all__ = ["build_summary"]
