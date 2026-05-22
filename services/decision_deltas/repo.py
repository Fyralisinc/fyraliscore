"""
services/decision_deltas/repo.py — CRUD for decision_deltas + evidence.

Surface
-------
  list_deltas(conn, tenant_id, status=None, target=None, limit=50)
  get_delta(conn, tenant_id, delta_id)
  create_delta(conn, tenant_id, ...)
  update_status(conn, delta_id, status, user_id, ...)
  accept_and_apply(conn, delta_id, user_id)

Caller owns the transaction. Every query is tenant-scoped via the
explicit `tenant_id` parameter; RLS is the defense-in-depth layer
(see migration 0036 + 0040).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, ValidationError


# ---------------------------------------------------------------------
# Enumerations — closed sets, mirror the SQL CHECK constraints.
# ---------------------------------------------------------------------

VALID_STATUSES: frozenset[str] = frozenset({
    "proposed", "accepted", "delegated",
    "contested", "superseded", "dismissed",
})

VALID_LABELS: frozenset[str] = frozenset({
    "proposed_change", "needs_review",
    "authority_required", "recommended_update",
})

# Open enums — kept as guidance, not enforced at the application
# layer. The DB column has no CHECK; new domains land without a
# migration.
COMMON_TARGET_KINDS: tuple[str, ...] = (
    "customer", "commitment", "goal", "decision",
    "risk", "resource", "actor",
)
COMMON_CATEGORIES: tuple[str, ...] = (
    "customer_risk", "capacity", "delivery", "strategy",
    "decision", "pricing", "revenue",
)


class DecisionDeltaRepoError(CompanyOSError):
    default_code = "decision_delta_repo_error"


class DeltaNotFoundError(DecisionDeltaRepoError):
    default_code = "decision_delta_not_found"


class InvalidStatusTransitionError(DecisionDeltaRepoError):
    default_code = "decision_delta_invalid_status"


# ---------------------------------------------------------------------
# Dataclasses — output shapes used by router + tests.
# ---------------------------------------------------------------------


@dataclass
class EvidenceItem:
    id: UUID
    delta_id: UUID
    source: str
    title: str
    ts: datetime
    trust_tier: str | None
    excerpt: str | None
    weight: float | None
    ordinal: int


@dataclass
class DecisionDeltaView:
    id: UUID
    tenant_id: UUID
    status: str
    label: str
    main_assertion: str
    current_state: dict[str, Any] | None
    suggested_update: dict[str, Any] | None
    target_node_kind: str | None
    target_node_id: UUID | None
    confidence: float | None
    confidence_basis: str | None
    falsification_condition: str | None
    consequence_preview: dict[str, Any] | None
    impact: dict[str, Any] | None
    category: str | None
    source_recommendation_id: UUID | None
    created_at: datetime
    updated_at: datetime
    accepted_at: datetime | None
    accepted_by: UUID | None
    resolution_target_at: datetime | None
    evidence: list[EvidenceItem] = field(default_factory=list)


# ---------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------


_LIST_SQL_BASE = """
SELECT
  id, tenant_id, status, label, main_assertion,
  current_state, suggested_update,
  target_node_kind, target_node_id,
  confidence, confidence_basis,
  falsification_condition, consequence_preview, impact,
  category, source_recommendation_id,
  created_at, updated_at, accepted_at, accepted_by,
  resolution_target_at
FROM decision_deltas
WHERE tenant_id = $1
"""


async def list_deltas(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    status: str | Iterable[str] | None = None,
    target_kind: str | None = None,
    target_id: UUID | None = None,
    category: str | None = None,
    limit: int = 50,
) -> list[DecisionDeltaView]:
    """Return decision deltas for a tenant, optionally filtered.

    `status` may be a single value or an iterable; passing an iterable
    yields rows whose status is in the set. Evidence is NOT loaded
    here — call `get_delta` for the per-delta detail with evidence.
    """
    clauses: list[str] = []
    args: list[Any] = [tenant_id]

    if status is not None:
        if isinstance(status, str):
            statuses = [status]
        else:
            statuses = [s for s in status]
        for s in statuses:
            if s not in VALID_STATUSES:
                raise ValidationError(
                    f"invalid status filter {s!r}", field="status",
                )
        args.append(statuses)
        clauses.append(f"status = ANY(${len(args)}::text[])")

    if target_kind is not None:
        args.append(target_kind)
        clauses.append(f"target_node_kind = ${len(args)}")

    if target_id is not None:
        args.append(target_id)
        clauses.append(f"target_node_id = ${len(args)}")

    if category is not None:
        args.append(category)
        clauses.append(f"category = ${len(args)}")

    extra = ""
    if clauses:
        extra = " AND " + " AND ".join(clauses)

    args.append(max(1, int(limit)))
    sql = (
        _LIST_SQL_BASE
        + extra
        + f" ORDER BY created_at DESC LIMIT ${len(args)}"
    )
    rows = await conn.fetch(sql, *args)
    return [_row_to_view(r) for r in rows]


async def get_delta(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    delta_id: UUID,
) -> DecisionDeltaView | None:
    """Load a single delta with its evidence list. Returns None if
    not found / wrong tenant."""
    row = await conn.fetchrow(
        _LIST_SQL_BASE + " AND id = $2",
        tenant_id, delta_id,
    )
    if row is None:
        return None
    view = _row_to_view(row)
    evidence_rows = await conn.fetch(
        """
        SELECT id, delta_id, source, title, ts, trust_tier,
               excerpt, weight, ordinal
        FROM decision_delta_evidence
        WHERE delta_id = $1
        ORDER BY ordinal ASC, ts ASC
        """,
        delta_id,
    )
    view.evidence = [
        EvidenceItem(
            id=e["id"],
            delta_id=e["delta_id"],
            source=e["source"],
            title=e["title"],
            ts=e["ts"],
            trust_tier=e["trust_tier"],
            excerpt=e["excerpt"],
            weight=(
                float(e["weight"])
                if e["weight"] is not None
                else None
            ),
            ordinal=int(e["ordinal"]),
        )
        for e in evidence_rows
    ]
    return view


# ---------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------


async def create_delta(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    main_assertion: str,
    label: str = "proposed_change",
    status: str = "proposed",
    current_state: dict[str, Any] | None = None,
    suggested_update: dict[str, Any] | None = None,
    target_node_kind: str | None = None,
    target_node_id: UUID | None = None,
    confidence: float | None = None,
    confidence_basis: str | None = None,
    falsification_condition: str | None = None,
    consequence_preview: dict[str, Any] | None = None,
    impact: dict[str, Any] | None = None,
    category: str | None = None,
    source_recommendation_id: UUID | None = None,
    resolution_target_at: datetime | None = None,
    evidence: list[dict[str, Any]] | None = None,
) -> UUID:
    """Insert a delta + (optional) evidence list. Returns the new id.

    Invariants enforced here:
      - main_assertion non-empty
      - label / status in their CHECK sets
      - confidence in [0, 1]
      - confidence > 0.7 requires a falsification_condition
        (matches spec §2.1: "Required for high-confidence inferred
        claims").
    """
    if not main_assertion or not main_assertion.strip():
        raise ValidationError(
            "main_assertion is required", field="main_assertion",
        )
    if label not in VALID_LABELS:
        raise ValidationError(
            f"invalid label {label!r}", field="label",
        )
    if status not in VALID_STATUSES:
        raise ValidationError(
            f"invalid status {status!r}", field="status",
        )
    if confidence is not None:
        if not (0.0 <= float(confidence) <= 1.0):
            raise ValidationError(
                "confidence must be in [0, 1]", field="confidence",
            )
        if (
            float(confidence) > 0.7
            and not (
                falsification_condition
                and falsification_condition.strip()
            )
        ):
            raise ValidationError(
                "falsification_condition required for confidence > 0.7",
                field="falsification_condition",
            )

    row = await conn.fetchrow(
        """
        INSERT INTO decision_deltas (
          tenant_id, status, label, main_assertion,
          current_state, suggested_update,
          target_node_kind, target_node_id,
          confidence, confidence_basis,
          falsification_condition, consequence_preview, impact,
          category, source_recommendation_id, resolution_target_at
        ) VALUES (
          $1, $2, $3, $4,
          $5::jsonb, $6::jsonb,
          $7, $8,
          $9, $10,
          $11, $12::jsonb, $13::jsonb,
          $14, $15, $16
        )
        RETURNING id
        """,
        tenant_id, status, label, main_assertion.strip(),
        _dump_jsonb(current_state), _dump_jsonb(suggested_update),
        target_node_kind, target_node_id,
        confidence, confidence_basis,
        falsification_condition,
        _dump_jsonb(consequence_preview), _dump_jsonb(impact),
        category, source_recommendation_id, resolution_target_at,
    )
    delta_id: UUID = row["id"]

    if evidence:
        await _insert_evidence(conn, delta_id=delta_id, items=evidence)

    return delta_id


async def _insert_evidence(
    conn: asyncpg.Connection,
    *,
    delta_id: UUID,
    items: list[dict[str, Any]],
) -> None:
    """Insert evidence rows in bulk. Each item must carry source,
    title, ts; the rest is optional. ordinal defaults to position."""
    rows: list[tuple[Any, ...]] = []
    for i, raw in enumerate(items):
        if not isinstance(raw, dict):
            continue
        source = raw.get("source")
        title = raw.get("title")
        ts = raw.get("ts")
        if not isinstance(source, str) or not source.strip():
            raise ValidationError(
                "evidence.source is required", field="evidence.source",
            )
        if not isinstance(title, str) or not title.strip():
            raise ValidationError(
                "evidence.title is required", field="evidence.title",
            )
        if isinstance(ts, str):
            try:
                ts_v: datetime = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
            except ValueError as e:
                raise ValidationError(
                    f"evidence.ts is not parseable: {ts!r}",
                    field="evidence.ts",
                ) from e
        elif isinstance(ts, datetime):
            ts_v = ts
        else:
            raise ValidationError(
                "evidence.ts is required", field="evidence.ts",
            )
        weight = raw.get("weight")
        if weight is not None:
            try:
                weight = float(weight)
            except (TypeError, ValueError) as e:
                raise ValidationError(
                    "evidence.weight is not a float",
                    field="evidence.weight",
                ) from e
        ordinal = raw.get("ordinal", i)
        rows.append(
            (
                source.strip(), title.strip(), ts_v,
                raw.get("trust_tier"), raw.get("excerpt"),
                weight, int(ordinal), delta_id,
            )
        )

    if not rows:
        return

    await conn.executemany(
        """
        INSERT INTO decision_delta_evidence (
          source, title, ts, trust_tier, excerpt, weight,
          ordinal, delta_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """,
        rows,
    )


async def update_status(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    delta_id: UUID,
    status: str,
    user_id: UUID | None = None,
    set_accepted: bool = False,
) -> DecisionDeltaView:
    """Transition a delta to a new status.

    Allowed transitions are gated here at the application layer so
    we don't have to ALTER TABLE every time the lifecycle changes.
    The rules:

      proposed   -> accepted | delegated | contested | dismissed
      delegated  -> accepted | contested | dismissed
      contested  -> proposed | dismissed
      accepted   -> (terminal)
      superseded -> (terminal)
      dismissed  -> (terminal)
    """
    if status not in VALID_STATUSES:
        raise ValidationError(
            f"invalid status {status!r}", field="status",
        )
    current = await get_delta(
        conn, tenant_id=tenant_id, delta_id=delta_id,
    )
    if current is None:
        raise DeltaNotFoundError(
            f"decision delta {delta_id} not found",
            delta_id=str(delta_id),
        )
    if not _is_allowed_transition(current.status, status):
        raise InvalidStatusTransitionError(
            f"cannot transition from {current.status} to {status}",
            from_status=current.status, to_status=status,
        )

    if set_accepted or status == "accepted":
        await conn.execute(
            """
            UPDATE decision_deltas
            SET status = $2,
                accepted_at = CASE WHEN $2 = 'accepted' THEN now()
                                   ELSE accepted_at END,
                accepted_by = CASE WHEN $2 = 'accepted' THEN $3
                                   ELSE accepted_by END
            WHERE id = $1 AND tenant_id = $4
            """,
            delta_id, status, user_id, tenant_id,
        )
    else:
        await conn.execute(
            """
            UPDATE decision_deltas
            SET status = $2
            WHERE id = $1 AND tenant_id = $3
            """,
            delta_id, status, tenant_id,
        )

    updated = await get_delta(
        conn, tenant_id=tenant_id, delta_id=delta_id,
    )
    if updated is None:  # pragma: no cover — concurrent delete
        raise DeltaNotFoundError(
            f"decision delta {delta_id} disappeared during update",
            delta_id=str(delta_id),
        )
    return updated


def _is_allowed_transition(from_status: str, to_status: str) -> bool:
    if from_status == to_status:
        return False
    allowed: dict[str, set[str]] = {
        "proposed":   {"accepted", "delegated", "contested", "dismissed"},
        "delegated":  {"accepted", "contested", "dismissed"},
        "contested":  {"proposed", "dismissed"},
        "accepted":   set(),
        "superseded": set(),
        "dismissed":  set(),
    }
    return to_status in allowed.get(from_status, set())


async def accept_and_apply(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    delta_id: UUID,
    user_id: UUID,
) -> tuple[DecisionDeltaView, dict[str, Any]]:
    """Accept a delta and execute the side effects described by its
    `consequence_preview`. Delegates to services.decision_deltas.apply
    for the actual side-effect dispatch.

    Returns the updated view plus a dict describing the triggered
    events (passed back to the API caller).
    """
    # Late import — avoid cycle (apply imports back into repo for
    # status transitions).
    from services.decision_deltas import apply as apply_mod

    return await apply_mod.apply_acceptance(
        conn=conn,
        tenant_id=tenant_id,
        delta_id=delta_id,
        user_id=user_id,
    )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _row_to_view(r: asyncpg.Record) -> DecisionDeltaView:
    return DecisionDeltaView(
        id=r["id"],
        tenant_id=r["tenant_id"],
        status=r["status"],
        label=r["label"],
        main_assertion=r["main_assertion"],
        current_state=_load_jsonb(r["current_state"]),
        suggested_update=_load_jsonb(r["suggested_update"]),
        target_node_kind=r["target_node_kind"],
        target_node_id=r["target_node_id"],
        confidence=(
            float(r["confidence"])
            if r["confidence"] is not None
            else None
        ),
        confidence_basis=r["confidence_basis"],
        falsification_condition=r["falsification_condition"],
        consequence_preview=_load_jsonb(r["consequence_preview"]),
        impact=_load_jsonb(r["impact"]),
        category=r["category"],
        source_recommendation_id=r["source_recommendation_id"],
        created_at=r["created_at"],
        updated_at=r["updated_at"],
        accepted_at=r["accepted_at"],
        accepted_by=r["accepted_by"],
        resolution_target_at=r["resolution_target_at"],
    )


def _dump_jsonb(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, default=str)


def _load_jsonb(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(decoded, dict):
            return decoded
    return None


__all__ = [
    "VALID_STATUSES",
    "VALID_LABELS",
    "COMMON_TARGET_KINDS",
    "COMMON_CATEGORIES",
    "DecisionDeltaRepoError",
    "DeltaNotFoundError",
    "InvalidStatusTransitionError",
    "EvidenceItem",
    "DecisionDeltaView",
    "list_deltas",
    "get_delta",
    "create_delta",
    "update_status",
    "accept_and_apply",
]
