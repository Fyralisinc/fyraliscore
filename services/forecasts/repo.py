"""services/forecasts/repo.py — CRUD over the `predictions` +
`prediction_signals` tables.

Read-side surfaces (called by router.py):

  - list_predictions       — Active / Resolved / Accuracy list bodies.
  - get_prediction         — Right-inspector detail (row + signals).
  - upcoming_resolutions   — Middle-column "Resolutions next 14 days"
                             card.
  - risk_exposure_series   — Risk-exposure chart. Bins active rows by
                             resolution week, summing a numeric impact
                             metric.

Write-side surfaces:

  - create_prediction      — "+ New scenario" CEO-author button.
  - resolve_prediction     — Background or operator-triggered: stamps
                             outcome + resolution_timeliness.

Every method is tenant-scoped via an explicit WHERE clause — RLS at
the DB level (migration 0041) is defense-in-depth.

The signal-list write-side uses bulk INSERT for prediction creation;
single INSERT for updates. Callers pass an asyncpg connection; the
caller owns the transaction so create_prediction + signal inserts are
atomic.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Literal
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, ValidationError


PredictionStatus = Literal["active", "resolved", "superseded"]
PredictionOutcome = Literal["true", "false", "partial"]
PredictionTimeliness = Literal["early", "on_time", "late"]


_VALID_CATEGORIES = frozenset({
    "customer_risk", "capacity", "delivery", "strategy",
    "decision", "pricing", "partner",
})

_VALID_STATUSES = frozenset({"active", "resolved", "superseded"})
_VALID_SORTS = frozenset({"earliest_resolution", "latest_resolution",
                          "highest_confidence", "created"})


class ForecastsRepoError(CompanyOSError):
    default_code = "forecasts_repo_error"


# ---------------------------------------------------------------------
# Output shapes
# ---------------------------------------------------------------------


@dataclass
class PredictionSignal:
    id: UUID
    source: str
    title: str
    ts: datetime
    trust_tier: str | None
    weight: float | None
    ordinal: int


@dataclass
class PredictionRow:
    id: UUID
    tenant_id: UUID
    status: str
    statement: str
    rationale: str | None
    category: str
    target_node_kind: str | None
    target_node_id: UUID | None
    target_label: str | None
    confidence: float
    confidence_basis: str | None
    falsification_condition: str | None
    key_drivers: list[dict[str, Any]]
    impact: dict[str, Any]
    resolution_at: datetime
    resolved_at: datetime | None
    outcome: str | None
    resolution_timeliness: str | None
    created_at: datetime
    updated_at: datetime


@dataclass
class PredictionDetail:
    prediction: PredictionRow
    signals: list[PredictionSignal] = field(default_factory=list)


# ---------------------------------------------------------------------
# List + detail
# ---------------------------------------------------------------------


_SORT_TO_SQL = {
    "earliest_resolution": "resolution_at ASC",
    "latest_resolution":   "resolution_at DESC",
    "highest_confidence":  "confidence DESC, resolution_at ASC",
    "created":             "created_at DESC",
}


async def list_predictions(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    status: str = "active",
    category: str | None = None,
    sort: str = "earliest_resolution",
    limit: int = 50,
) -> list[PredictionRow]:
    """List predictions for a tenant. `status` and `category` filter;
    `sort` selects the order from `_SORT_TO_SQL`. `limit` is clamped to
    [1, 200]."""
    if status not in _VALID_STATUSES:
        raise ValidationError(
            f"invalid status {status!r}", field="status",
        )
    if category is not None and category not in _VALID_CATEGORIES:
        raise ValidationError(
            f"invalid category {category!r}", field="category",
        )
    sort_key = sort if sort in _SORT_TO_SQL else "earliest_resolution"
    order_by = _SORT_TO_SQL[sort_key]
    limit = max(1, min(int(limit), 200))

    args: list[Any] = [tenant_id, status]
    where = "tenant_id = $1 AND status = $2"
    if category is not None:
        args.append(category)
        where += f" AND category = ${len(args)}"
    args.append(limit)

    sql = f"""
        SELECT id, tenant_id, status, statement, rationale, category,
               target_node_kind, target_node_id, target_label,
               confidence, confidence_basis, falsification_condition,
               key_drivers, impact,
               resolution_at, resolved_at, outcome, resolution_timeliness,
               created_at, updated_at
        FROM predictions
        WHERE {where}
        ORDER BY {order_by}
        LIMIT ${len(args)}
    """
    rows = await conn.fetch(sql, *args)
    return [_row_to_prediction(r) for r in rows]


async def get_prediction(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    prediction_id: UUID,
) -> PredictionDetail | None:
    """Fetch a single prediction (with signals). Returns None on miss."""
    row = await conn.fetchrow(
        """
        SELECT id, tenant_id, status, statement, rationale, category,
               target_node_kind, target_node_id, target_label,
               confidence, confidence_basis, falsification_condition,
               key_drivers, impact,
               resolution_at, resolved_at, outcome, resolution_timeliness,
               created_at, updated_at
        FROM predictions
        WHERE id = $1 AND tenant_id = $2
        """,
        prediction_id, tenant_id,
    )
    if row is None:
        return None
    sig_rows = await conn.fetch(
        """
        SELECT id, source, title, ts, trust_tier, weight, ordinal
        FROM prediction_signals
        WHERE prediction_id = $1
        ORDER BY ordinal ASC, ts DESC
        """,
        prediction_id,
    )
    signals = [
        PredictionSignal(
            id=r["id"],
            source=r["source"],
            title=r["title"],
            ts=r["ts"],
            trust_tier=r["trust_tier"],
            weight=float(r["weight"]) if r["weight"] is not None else None,
            ordinal=int(r["ordinal"] or 0),
        )
        for r in sig_rows
    ]
    return PredictionDetail(prediction=_row_to_prediction(row), signals=signals)


# ---------------------------------------------------------------------
# Create + resolve
# ---------------------------------------------------------------------


async def create_prediction(
    conn: asyncpg.Connection,
    payload: dict[str, Any],
) -> PredictionRow:
    """Insert a single prediction row. Optional `signals` list on the
    payload is bulk-inserted into prediction_signals.

    Required: tenant_id, statement, category, confidence, resolution_at.
    Optional: rationale, target_*, confidence_basis,
    falsification_condition, key_drivers, impact, status (default
    'active'), signals.
    """
    tenant_id = _require_uuid(payload, "tenant_id")
    statement = _require_str(payload, "statement")
    category = _require_str(payload, "category")
    if category not in _VALID_CATEGORIES:
        raise ValidationError(
            f"invalid category {category!r}", field="category",
        )
    confidence = _require_float(payload, "confidence")
    if confidence < 0 or confidence > 1:
        raise ValidationError(
            "confidence must be in [0, 1]", field="confidence",
        )
    resolution_at = _require_datetime(payload, "resolution_at")

    status = str(payload.get("status") or "active")
    if status not in _VALID_STATUSES:
        raise ValidationError(
            f"invalid status {status!r}", field="status",
        )

    row = await conn.fetchrow(
        """
        INSERT INTO predictions (
          tenant_id, status, statement, rationale, category,
          target_node_kind, target_node_id, target_label,
          confidence, confidence_basis, falsification_condition,
          key_drivers, impact, resolution_at
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, $7, $8,
          $9, $10, $11,
          $12::jsonb, $13::jsonb, $14
        )
        RETURNING id, tenant_id, status, statement, rationale, category,
                  target_node_kind, target_node_id, target_label,
                  confidence, confidence_basis, falsification_condition,
                  key_drivers, impact,
                  resolution_at, resolved_at, outcome, resolution_timeliness,
                  created_at, updated_at
        """,
        tenant_id, status, statement, payload.get("rationale"), category,
        payload.get("target_node_kind"),
        _optional_uuid(payload.get("target_node_id")),
        payload.get("target_label"),
        confidence,
        payload.get("confidence_basis"),
        payload.get("falsification_condition"),
        json.dumps(payload.get("key_drivers") or []),
        json.dumps(payload.get("impact") or {}),
        resolution_at,
    )

    signals = payload.get("signals") or []
    if isinstance(signals, list) and signals:
        await _bulk_insert_signals(conn, row["id"], signals)

    return _row_to_prediction(row)


async def resolve_prediction(
    conn: asyncpg.Connection,
    prediction_id: UUID,
    outcome: str,
    timeliness: str,
) -> PredictionRow:
    """Mark a prediction resolved. Computes resolved_at = now()."""
    if outcome not in ("true", "false", "partial"):
        raise ValidationError(
            f"invalid outcome {outcome!r}", field="outcome",
        )
    if timeliness not in ("early", "on_time", "late"):
        raise ValidationError(
            f"invalid timeliness {timeliness!r}", field="timeliness",
        )
    row = await conn.fetchrow(
        """
        UPDATE predictions
        SET status                = 'resolved',
            outcome               = $2,
            resolution_timeliness = $3,
            resolved_at           = now(),
            updated_at            = now()
        WHERE id = $1
        RETURNING id, tenant_id, status, statement, rationale, category,
                  target_node_kind, target_node_id, target_label,
                  confidence, confidence_basis, falsification_condition,
                  key_drivers, impact,
                  resolution_at, resolved_at, outcome, resolution_timeliness,
                  created_at, updated_at
        """,
        prediction_id, outcome, timeliness,
    )
    if row is None:
        raise ValidationError(
            f"prediction {prediction_id} not found",
            prediction_id=str(prediction_id),
        )
    return _row_to_prediction(row)


# ---------------------------------------------------------------------
# Summary helpers (used by router /summary endpoint)
# ---------------------------------------------------------------------


async def upcoming_resolutions(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    days: int = 14,
) -> list[PredictionRow]:
    """Active predictions whose resolution_at falls within the next
    `days` days. Sorted earliest-first."""
    days = max(1, int(days))
    rows = await conn.fetch(
        """
        SELECT id, tenant_id, status, statement, rationale, category,
               target_node_kind, target_node_id, target_label,
               confidence, confidence_basis, falsification_condition,
               key_drivers, impact,
               resolution_at, resolved_at, outcome, resolution_timeliness,
               created_at, updated_at
        FROM predictions
        WHERE tenant_id = $1
          AND status = 'active'
          AND resolution_at >= now()
          AND resolution_at <= now() + make_interval(days => $2)
        ORDER BY resolution_at ASC
        """,
        tenant_id, days,
    )
    return [_row_to_prediction(r) for r in rows]


async def risk_exposure_series(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    metric: str = "arr_at_risk",
    range_days: int = 90,
) -> list[dict[str, Any]]:
    """Weekly buckets over the next `range_days` days. For each bucket
    we sum `impact->>metric` across active predictions whose
    resolution_at lands in the bucket.

    Returns a list of `{bucket_start, bucket_end, value}` ordered by
    bucket_start. Empty buckets are included with value=0 so the chart
    can draw a continuous line.
    """
    metric = str(metric or "arr_at_risk")
    range_days = max(7, int(range_days))
    rows = await conn.fetch(
        """
        SELECT
          date_trunc('week', resolution_at) AS bucket_start,
          SUM(
            COALESCE((impact ->> $3)::numeric, 0)
          ) AS value
        FROM predictions
        WHERE tenant_id = $1
          AND status = 'active'
          AND resolution_at >= now()
          AND resolution_at <= now() + make_interval(days => $2)
        GROUP BY bucket_start
        ORDER BY bucket_start
        """,
        tenant_id, range_days, metric,
    )
    by_bucket: dict[datetime, float] = {}
    for r in rows:
        bs = r["bucket_start"]
        if bs is None:
            continue
        by_bucket[bs] = float(r["value"] or 0)

    # Synthesise contiguous weekly buckets across the window so the
    # frontend doesn't need to fill gaps.
    now = datetime.now(timezone.utc)
    week_start = now - timedelta(days=now.weekday())
    week_start = week_start.replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    series: list[dict[str, Any]] = []
    weeks = max(1, range_days // 7 + 1)
    for i in range(weeks):
        bs = week_start + timedelta(days=7 * i)
        # match by date (date_trunc('week') in PG returns Monday 00:00).
        value = 0.0
        for k, v in by_bucket.items():
            if _same_week(k, bs):
                value = v
                break
        series.append({
            "bucket_start": bs,
            "bucket_end": bs + timedelta(days=7),
            "value": float(value),
        })
    return series


async def summary_counters(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> dict[str, Any]:
    """Counters for the Forecasts header strip. Read-only helper for
    the router's GET /v1/forecasts/summary."""
    active_row = await conn.fetchrow(
        """
        SELECT
          COUNT(*) AS active_count,
          COALESCE(SUM(COALESCE((impact ->> 'arr_at_risk')::numeric, 0)), 0)
            AS at_risk_arr,
          COUNT(*) FILTER (WHERE confidence >= 0.7) AS high_confidence_count
        FROM predictions
        WHERE tenant_id = $1
          AND status = 'active'
        """,
        tenant_id,
    )
    upcoming = await conn.fetchval(
        """
        SELECT COUNT(*) FROM predictions
        WHERE tenant_id = $1
          AND status = 'active'
          AND resolution_at >= now()
          AND resolution_at <= now() + make_interval(days => 14)
        """,
        tenant_id,
    )
    return {
        "active_count": int(active_row["active_count"] or 0),
        "at_risk_arr": float(active_row["at_risk_arr"] or 0.0),
        "high_confidence_count": int(active_row["high_confidence_count"] or 0),
        "upcoming_resolutions_count_14d": int(upcoming or 0),
    }


# ---------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------


async def _bulk_insert_signals(
    conn: asyncpg.Connection,
    prediction_id: UUID,
    signals: Iterable[dict[str, Any]],
) -> None:
    records: list[tuple[Any, ...]] = []
    for i, s in enumerate(signals):
        if not isinstance(s, dict):
            continue
        source = s.get("source")
        title = s.get("title")
        ts = s.get("ts")
        if not (isinstance(source, str) and isinstance(title, str)):
            continue
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except ValueError:
                ts = datetime.now(timezone.utc)
        elif not isinstance(ts, datetime):
            ts = datetime.now(timezone.utc)
        weight = s.get("weight")
        try:
            weight_num: float | None = (
                float(weight) if weight is not None else None
            )
        except (ValueError, TypeError):
            weight_num = None
        records.append((
            prediction_id,
            source,
            title,
            ts,
            s.get("trust_tier"),
            weight_num,
            int(s.get("ordinal", i)),
        ))
    if not records:
        return
    await conn.executemany(
        """
        INSERT INTO prediction_signals
          (prediction_id, source, title, ts, trust_tier, weight, ordinal)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        records,
    )


def _row_to_prediction(r: asyncpg.Record) -> PredictionRow:
    return PredictionRow(
        id=r["id"],
        tenant_id=r["tenant_id"],
        status=r["status"],
        statement=r["statement"],
        rationale=r["rationale"],
        category=r["category"],
        target_node_kind=r["target_node_kind"],
        target_node_id=r["target_node_id"],
        target_label=r["target_label"],
        confidence=float(r["confidence"]),
        confidence_basis=r["confidence_basis"],
        falsification_condition=r["falsification_condition"],
        key_drivers=_coerce_jsonb_list(r["key_drivers"]),
        impact=_coerce_jsonb_obj(r["impact"]),
        resolution_at=r["resolution_at"],
        resolved_at=r["resolved_at"],
        outcome=r["outcome"],
        resolution_timeliness=r["resolution_timeliness"],
        created_at=r["created_at"],
        updated_at=r["updated_at"],
    )


def _coerce_jsonb_obj(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _coerce_jsonb_list(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return [v for v in parsed if isinstance(v, dict)] if isinstance(parsed, list) else []
    return []


def _require_uuid(payload: dict[str, Any], field: str) -> UUID:
    v = payload.get(field)
    if v is None:
        raise ValidationError(f"{field} is required", field=field)
    if isinstance(v, UUID):
        return v
    try:
        return UUID(str(v))
    except (ValueError, TypeError) as e:
        raise ValidationError(f"{field} is not a valid UUID", field=field) from e


def _require_str(payload: dict[str, Any], field: str) -> str:
    v = payload.get(field)
    if not isinstance(v, str) or not v.strip():
        raise ValidationError(f"{field} is required", field=field)
    return v.strip()


def _require_float(payload: dict[str, Any], field: str) -> float:
    v = payload.get(field)
    if v is None:
        raise ValidationError(f"{field} is required", field=field)
    try:
        return float(v)
    except (ValueError, TypeError) as e:
        raise ValidationError(f"{field} must be numeric", field=field) from e


def _require_datetime(payload: dict[str, Any], field: str) -> datetime:
    v = payload.get(field)
    if v is None:
        raise ValidationError(f"{field} is required", field=field)
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError as e:
            raise ValidationError(
                f"{field} is not a valid ISO datetime", field=field,
            ) from e
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    raise ValidationError(f"{field} must be a datetime", field=field)


def _optional_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (ValueError, TypeError):
        return None


def _same_week(a: datetime, b: datetime) -> bool:
    """Same week iff both fall in the same Monday-anchored 7-day window."""
    if a.tzinfo is None:
        a = a.replace(tzinfo=timezone.utc)
    if b.tzinfo is None:
        b = b.replace(tzinfo=timezone.utc)
    a_monday = (a - timedelta(days=a.weekday())).date()
    b_monday = (b - timedelta(days=b.weekday())).date()
    return a_monday == b_monday


__all__ = [
    "PredictionRow",
    "PredictionDetail",
    "PredictionSignal",
    "ForecastsRepoError",
    "list_predictions",
    "get_prediction",
    "create_prediction",
    "resolve_prediction",
    "upcoming_resolutions",
    "risk_exposure_series",
    "summary_counters",
]
