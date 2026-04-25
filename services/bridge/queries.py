"""services/bridge/queries.py — full Bridge queries (Wave 5-B).

Spec refs: ARCHITECTURE-FINAL.md §27. The §27 shape maps 1:1 to the
Q2-resolved superset `customer_commitments` (SCHEMA-LOCK.md W5.Q2).

Five public queries:

  revenue_at_risk(tenant_id, *, horizon_days=90, conn=None)
  capability_at_risk(tenant_id, *, conn=None)
  commitment_feasibility(proposed, tenant_id, *, conn=None)
  critical_path(goal_id, *, conn=None)
  customer_health_timeline(customer_id, *, window_days=30, conn=None)

Hard constraints (Prompt 5.B):
  - Every query starts with `WHERE tenant_id = $1`.
  - Return types are Pydantic models for easy FastAPI response
    serialization.
  - `Decimal` for money amounts — never float.
  - NULL `revenue_at_risk_usd` on a `customer_commitments` row falls
    back to `customer.current_value->>'arr_cents' / 100` (legacy
    Wave 2-C semantics). Callers that want only explicitly-priced rows
    inspect `CustomerRevenueRow.fallback_used`.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from lib.shared.db import get_pool
from lib.shared.types import CommitmentRow


# =====================================================================
# Return types
# =====================================================================


class _BridgeModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class CustomerRevenueRow(_BridgeModel):
    customer_resource_id: UUID
    customer_name: str
    total_at_risk_usd: Decimal
    blocked_usd: Decimal
    paused_usd: Decimal
    doneunverified_usd: Decimal
    prediction_driven_usd: Decimal = Decimal("0")
    at_risk_commitment_ids: list[UUID] = Field(default_factory=list)
    fallback_used: bool = False


class RevenueAtRiskReport(_BridgeModel):
    tenant_id: UUID
    horizon_days: int
    generated_at: datetime
    customers: list[CustomerRevenueRow]
    grand_total_usd: Decimal
    fallback_count: int = 0


class CapabilityRisk(_BridgeModel):
    resource_id: UUID
    resource_name: str
    available: float
    deployed: float
    capacity: float
    utilization: float
    deploying_commitment_ids: list[UUID] = Field(default_factory=list)


class FeasibilityReport(_BridgeModel):
    feasible: bool
    reasons: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    confidence: float = 0.85


class CriticalPathEntry(_BridgeModel):
    commitment: CommitmentRow
    goal_id: UUID
    is_critical_path: bool
    cached_health: str | None = None


class HealthPoint(_BridgeModel):
    # Daily bucket
    day: date
    total_at_risk_usd: Decimal
    blocked_commitment_count: int


@dataclass(frozen=True)
class ProposedCommitment:
    """Input to `commitment_feasibility`. Mirrors the shape Think hands
    us before it creates a Commitment in active state (see spec §27).

    `estimated_capacity.deploys` is a list of `{resource_id, units}`
    dicts. `constrained_by_decision_ids` is optional. `owner_id` used
    for the owner-capacity warning.
    """

    tenant_id: UUID
    owner_id: UUID | None = None
    estimated_capacity: dict[str, Any] | None = None
    constrained_by_decision_ids: list[UUID] | None = None


# =====================================================================
# Constants
# =====================================================================

AT_RISK_STATES: tuple[str, ...] = ("blocked", "paused", "doneunverified")

OWNER_ACTIVE_STATES: tuple[str, ...] = (
    "proposed",
    "active",
    "blocked",
    "paused",
    "doneunverified",
)
OWNER_COMMITMENT_WARN_THRESHOLD = 5


# =====================================================================
# revenue_at_risk
# =====================================================================


async def revenue_at_risk(
    tenant_id: UUID,
    *,
    horizon_days: int = 90,
    conn: asyncpg.Connection | None = None,
) -> RevenueAtRiskReport:
    """Per-customer revenue-at-risk report per spec §27.

    An at-risk commitment is one whose state is in AT_RISK_STATES AND
    (a) due_date < now() + horizon_days OR (b) a prediction Model
    scoped to the commitment asserts `direction='will_slip'` with
    confidence > 0.6.

    For each at-risk (customer, commitment) linkage we pull
    `cc.revenue_at_risk_usd` if set; otherwise we fall back to the
    customer Resource's ARR, split evenly across the at-risk
    commitments served by that customer. `fallback_used=True` is set on
    customers that ever hit the fallback path — the dashboard layer
    surfaces this in the UI.
    """
    interval = timedelta(days=int(horizon_days))
    sql = """
        WITH at_risk_cmt AS (
          SELECT c.id AS commitment_id, c.state
          FROM commitments c
          WHERE c.tenant_id = $1
            AND c.state = ANY($2::text[])
            AND (
              (c.due_date IS NOT NULL AND c.due_date < now() + $3)
              OR EXISTS (
                SELECT 1 FROM models m
                WHERE m.tenant_id = $1
                  AND m.proposition_kind = 'prediction'
                  AND m.status = 'active'
                  AND m.scope_entities @> jsonb_build_array(
                    jsonb_build_object('type', 'commitment', 'id', c.id::text)
                  )
                  AND m.proposition->>'direction' = 'will_slip'
                  AND m.confidence > 0.6
              )
            )
        ),
        -- Commitments flagged at-risk ONLY by a prediction (due date
        -- hasn't hit the horizon yet) — used to surface prediction-
        -- driven risk in a separate bucket.
        pred_only AS (
          SELECT c.id AS commitment_id
          FROM commitments c
          WHERE c.tenant_id = $1
            AND c.state = ANY($2::text[])
            AND (c.due_date IS NULL OR c.due_date >= now() + $3)
            AND EXISTS (
              SELECT 1 FROM models m
              WHERE m.tenant_id = $1
                AND m.proposition_kind = 'prediction'
                AND m.status = 'active'
                AND m.scope_entities @> jsonb_build_array(
                  jsonb_build_object('type', 'commitment', 'id', c.id::text)
                )
                AND m.proposition->>'direction' = 'will_slip'
                AND m.confidence > 0.6
            )
        ),
        linked AS (
          SELECT
            cc.customer_resource_id,
            cc.commitment_id,
            cc.revenue_at_risk_usd,
            c.state AS cmt_state,
            (cc.commitment_id IN (SELECT commitment_id FROM pred_only))
              AS prediction_driven
          FROM customer_commitments cc
          JOIN at_risk_cmt ar ON ar.commitment_id = cc.commitment_id
          JOIN commitments c ON c.id = cc.commitment_id
          WHERE cc.tenant_id = $1
        ),
        -- Per customer, count how many at-risk commitments are linked
        -- so the ARR fallback can be split evenly across them.
        customer_counts AS (
          SELECT customer_resource_id,
                 COUNT(*) AS at_risk_cnt,
                 COUNT(*) FILTER (WHERE revenue_at_risk_usd IS NULL)
                   AS null_cnt
          FROM linked
          GROUP BY customer_resource_id
        )
        SELECT
          l.customer_resource_id,
          COALESCE(r.identity, '') AS customer_name,
          r.current_value AS customer_current_value,
          COALESCE(cc2.at_risk_cnt, 0) AS at_risk_cnt,
          COALESCE(cc2.null_cnt, 0) AS null_cnt,
          array_agg(l.commitment_id) AS at_risk_commitment_ids,
          -- Bucket per state, using revenue_at_risk_usd directly; NULLs
          -- are summed as zero here — the fallback is applied in Python
          -- so it's easy to report fallback_used.
          COALESCE(SUM(
            CASE WHEN l.cmt_state = 'blocked'
                 THEN l.revenue_at_risk_usd ELSE 0 END
          ), 0) AS blocked_usd_explicit,
          COALESCE(SUM(
            CASE WHEN l.cmt_state = 'paused'
                 THEN l.revenue_at_risk_usd ELSE 0 END
          ), 0) AS paused_usd_explicit,
          COALESCE(SUM(
            CASE WHEN l.cmt_state = 'doneunverified'
                 THEN l.revenue_at_risk_usd ELSE 0 END
          ), 0) AS doneunverified_usd_explicit,
          COALESCE(SUM(
            CASE WHEN l.prediction_driven
                 THEN l.revenue_at_risk_usd ELSE 0 END
          ), 0) AS prediction_driven_usd_explicit,
          -- Per-state NULL counts for fallback split.
          COUNT(*) FILTER (
            WHERE l.cmt_state = 'blocked' AND l.revenue_at_risk_usd IS NULL
          ) AS blocked_null_cnt,
          COUNT(*) FILTER (
            WHERE l.cmt_state = 'paused' AND l.revenue_at_risk_usd IS NULL
          ) AS paused_null_cnt,
          COUNT(*) FILTER (
            WHERE l.cmt_state = 'doneunverified' AND l.revenue_at_risk_usd IS NULL
          ) AS doneunverified_null_cnt,
          COUNT(*) FILTER (
            WHERE l.prediction_driven AND l.revenue_at_risk_usd IS NULL
          ) AS prediction_driven_null_cnt
        FROM linked l
        LEFT JOIN resources r
          ON r.id = l.customer_resource_id AND r.tenant_id = $1
        LEFT JOIN customer_counts cc2
          ON cc2.customer_resource_id = l.customer_resource_id
        GROUP BY l.customer_resource_id, r.identity, r.current_value,
                 cc2.at_risk_cnt, cc2.null_cnt
    """

    async def _run(c: asyncpg.Connection) -> list[asyncpg.Record]:
        return await c.fetch(sql, tenant_id, list(AT_RISK_STATES), interval)

    if conn is not None:
        rows = await _run(conn)
    else:
        pool = get_pool()
        async with pool.acquire() as c2:
            rows = await _run(c2)

    customers: list[CustomerRevenueRow] = []
    fallback_count = 0
    for r in rows:
        at_risk_cnt = int(r["at_risk_cnt"] or 0)
        null_cnt = int(r["null_cnt"] or 0)
        blocked_explicit = Decimal(r["blocked_usd_explicit"] or 0)
        paused_explicit = Decimal(r["paused_usd_explicit"] or 0)
        doneu_explicit = Decimal(r["doneunverified_usd_explicit"] or 0)
        pred_explicit = Decimal(r["prediction_driven_usd_explicit"] or 0)
        blocked_null = int(r["blocked_null_cnt"] or 0)
        paused_null = int(r["paused_null_cnt"] or 0)
        doneu_null = int(r["doneunverified_null_cnt"] or 0)
        pred_null = int(r["prediction_driven_null_cnt"] or 0)

        # Fallback per null row: ARR / at_risk_cnt (split evenly).
        fallback_used = null_cnt > 0
        cv = dict(r["customer_current_value"] or {}) if r["customer_current_value"] else {}
        arr_cents = int(cv.get("arr_cents", 0) or 0)
        arr_usd = (Decimal(arr_cents) / Decimal(100)).quantize(Decimal("0.01"))

        if at_risk_cnt > 0 and null_cnt > 0 and arr_usd > 0:
            per_row = (arr_usd / Decimal(at_risk_cnt)).quantize(Decimal("0.01"))
        else:
            per_row = Decimal("0")

        blocked_usd = (blocked_explicit + per_row * Decimal(blocked_null)).quantize(Decimal("0.01"))
        paused_usd = (paused_explicit + per_row * Decimal(paused_null)).quantize(Decimal("0.01"))
        doneu_usd = (doneu_explicit + per_row * Decimal(doneu_null)).quantize(Decimal("0.01"))
        pred_usd = (pred_explicit + per_row * Decimal(pred_null)).quantize(Decimal("0.01"))
        total = (blocked_usd + paused_usd + doneu_usd).quantize(Decimal("0.01"))

        ids_raw = r["at_risk_commitment_ids"] or []
        ids = [UUID(str(x)) if not isinstance(x, UUID) else x for x in ids_raw]

        customers.append(
            CustomerRevenueRow(
                customer_resource_id=r["customer_resource_id"],
                customer_name=r["customer_name"] or "",
                total_at_risk_usd=total,
                blocked_usd=blocked_usd,
                paused_usd=paused_usd,
                doneunverified_usd=doneu_usd,
                prediction_driven_usd=pred_usd,
                at_risk_commitment_ids=ids,
                fallback_used=fallback_used,
            )
        )
        if fallback_used:
            fallback_count += 1

    # Also surface every customer that has linked commitments but zero
    # at-risk ones, so a dashboard can show them explicitly (Test 14).
    # Pull them in a second pass.
    seen_ids = {c.customer_resource_id for c in customers}
    zero_sql = """
        SELECT DISTINCT r.id AS customer_resource_id,
                        r.identity AS customer_name
        FROM resources r
        WHERE r.tenant_id = $1
          AND r.kind = 'relational'
          AND r.archived_at IS NULL
          AND EXISTS (
            SELECT 1 FROM customer_commitments cc
            WHERE cc.customer_resource_id = r.id
              AND cc.tenant_id = $1
          )
    """

    async def _run_zero(c: asyncpg.Connection) -> list[asyncpg.Record]:
        return await c.fetch(zero_sql, tenant_id)

    if conn is not None:
        zero_rows = await _run_zero(conn)
    else:
        pool = get_pool()
        async with pool.acquire() as c2:
            zero_rows = await _run_zero(c2)

    for r in zero_rows:
        if r["customer_resource_id"] in seen_ids:
            continue
        customers.append(
            CustomerRevenueRow(
                customer_resource_id=r["customer_resource_id"],
                customer_name=r["customer_name"] or "",
                total_at_risk_usd=Decimal("0"),
                blocked_usd=Decimal("0"),
                paused_usd=Decimal("0"),
                doneunverified_usd=Decimal("0"),
                prediction_driven_usd=Decimal("0"),
                at_risk_commitment_ids=[],
                fallback_used=False,
            )
        )

    customers.sort(key=lambda c: c.total_at_risk_usd, reverse=True)
    grand_total = sum((c.total_at_risk_usd for c in customers), Decimal("0")).quantize(Decimal("0.01"))

    return RevenueAtRiskReport(
        tenant_id=tenant_id,
        horizon_days=int(horizon_days),
        generated_at=datetime.now(timezone.utc),
        customers=customers,
        grand_total_usd=grand_total,
        fallback_count=fallback_count,
    )


# =====================================================================
# capability_at_risk
# =====================================================================


async def capability_at_risk(
    tenant_id: UUID,
    *,
    conn: asyncpg.Connection | None = None,
) -> list[CapabilityRisk]:
    """Capacity Resources whose utilization > 0.85, with deploying commitments.

    Matches spec §27's SQL skeleton. Uses current_value JSONB path:
    `total_units`, `deployed_units`, `available_units`.
    """
    sql = """
        SELECT r.id, r.identity AS resource_name,
               COALESCE((r.current_value->>'available_units')::float, 0) AS available,
               COALESCE((r.current_value->>'deployed_units')::float, 0) AS deployed,
               COALESCE((r.current_value->>'total_units')::float, 0) AS capacity,
               COALESCE(
                 (r.current_value->>'deployed_units')::float
                   / NULLIF((r.current_value->>'total_units')::float, 0),
                 0
               ) AS utilization,
               COALESCE(
                 array_remove(
                   array_agg(rd.commitment_id) FILTER (WHERE rd.released_at IS NULL),
                   NULL
                 ),
                 '{}'::uuid[]
               ) AS deploying_commitment_ids
        FROM resources r
        LEFT JOIN resource_deployments rd ON rd.resource_id = r.id
        WHERE r.tenant_id = $1
          AND r.kind = 'capacity'
          AND r.archived_at IS NULL
        GROUP BY r.id, r.identity, r.current_value
        HAVING COALESCE(
          (r.current_value->>'deployed_units')::float
            / NULLIF((r.current_value->>'total_units')::float, 0),
          0
        ) > 0.85
        ORDER BY utilization DESC
    """

    async def _run(c: asyncpg.Connection) -> list[asyncpg.Record]:
        return await c.fetch(sql, tenant_id)

    if conn is not None:
        rows = await _run(conn)
    else:
        pool = get_pool()
        async with pool.acquire() as c2:
            rows = await _run(c2)
    out: list[CapabilityRisk] = []
    for r in rows:
        ids_raw = r["deploying_commitment_ids"] or []
        ids = [UUID(str(x)) if not isinstance(x, UUID) else x for x in ids_raw]
        out.append(
            CapabilityRisk(
                resource_id=r["id"],
                resource_name=r["resource_name"] or "",
                available=float(r["available"] or 0),
                deployed=float(r["deployed"] or 0),
                capacity=float(r["capacity"] or 0),
                utilization=float(r["utilization"] or 0),
                deploying_commitment_ids=ids,
            )
        )
    return out


# =====================================================================
# commitment_feasibility
# =====================================================================


async def commitment_feasibility(
    proposed_commitment: ProposedCommitment,
    tenant_id: UUID,
    *,
    conn: asyncpg.Connection | None = None,
) -> FeasibilityReport:
    """Full feasibility check. Mirrors spec §27 pseudocode.

    Checks:
      (a) required-capacity: each `deploys` row → `available_units >= quantity`
      (b) owner capacity: actor already owning > 5 active Commitments → warning
      (c) constrained_by Decisions in 'revisited' → warning
      (d) resource exists, is in-tenant, not archived, is capacity

    `confidence` = 0.85 when feasible with no warnings; 0.7 with
    warnings; 0.4 when infeasible (matches spec).
    """

    reasons: list[str] = []
    warnings: list[str] = []

    async def _do(c: asyncpg.Connection) -> FeasibilityReport:
        deploys_block = proposed_commitment.estimated_capacity or {}
        deploys = deploys_block.get("deploys") or []
        for d in deploys:
            rid_raw = d.get("resource_id") if isinstance(d, dict) else None
            if rid_raw is None:
                reasons.append("deploy entry missing resource_id")
                continue
            try:
                rid = rid_raw if isinstance(rid_raw, UUID) else UUID(str(rid_raw))
            except (ValueError, TypeError):
                reasons.append("deploy entry has invalid resource_id")
                continue
            try:
                units = int(d.get("units", 0) or 0)
            except (ValueError, TypeError):
                reasons.append(f"resource {rid} has non-integer units")
                continue
            row = await c.fetchrow(
                """
                SELECT kind, current_value, archived_at, tenant_id
                FROM resources WHERE id = $1
                """,
                rid,
            )
            if row is None:
                reasons.append(f"resource {rid} does not exist")
                continue
            if row["tenant_id"] != tenant_id:
                reasons.append(f"resource {rid} belongs to a different tenant")
                continue
            if row["archived_at"] is not None:
                reasons.append(f"resource {rid} is archived")
                continue
            if row["kind"] != "capacity":
                # Non-capacity resources don't have availability math.
                continue
            available = int((row["current_value"] or {}).get("available_units", 0) or 0)
            if available < units:
                reasons.append(
                    f"resource {rid} has {available} available units, need {units}"
                )

        owner = proposed_commitment.owner_id
        if owner is not None:
            n_owned = await c.fetchval(
                """
                SELECT COUNT(*) FROM commitments
                WHERE tenant_id = $1
                  AND owner_id = $2
                  AND state = ANY($3::text[])
                """,
                tenant_id, owner, list(OWNER_ACTIVE_STATES),
            )
            n_owned = int(n_owned or 0)
            if n_owned > OWNER_COMMITMENT_WARN_THRESHOLD:
                warnings.append(
                    f"owner already owns {n_owned} active commitments "
                    f"(> {OWNER_COMMITMENT_WARN_THRESHOLD})"
                )

        for dec_id in proposed_commitment.constrained_by_decision_ids or []:
            state = await c.fetchval(
                "SELECT state FROM decisions WHERE id = $1 AND tenant_id = $2",
                dec_id, tenant_id,
            )
            if state is None:
                warnings.append(f"constrained_by decision {dec_id} not found")
                continue
            if state == "revisited":
                warnings.append(
                    f"decision {dec_id} is in state 'revisited' — review before proceeding"
                )

        feasible = len(reasons) == 0
        if not feasible:
            confidence = 0.4
        elif warnings:
            confidence = 0.7
        else:
            confidence = 0.85
        return FeasibilityReport(
            feasible=feasible,
            reasons=reasons,
            warnings=warnings,
            confidence=confidence,
        )

    if conn is not None:
        return await _do(conn)
    pool = get_pool()
    async with pool.acquire() as c2:
        return await _do(c2)


# =====================================================================
# critical_path
# =====================================================================


async def critical_path(
    goal_id: UUID,
    *,
    tenant_id: UUID,
    conn: asyncpg.Connection | None = None,
) -> list[CriticalPathEntry]:
    """All Commitments that contribute_to this Goal with is_critical_path=TRUE.

    Returned in the order they would be walked (created_at ASC). Each
    entry carries the commitment row + the Goal's cached_health for
    convenience (dashboard shows the rollup next to the CP entries).
    """
    sql = """
        SELECT c.*, g.cached_health AS goal_cached_health
        FROM contributes_to ct
        JOIN commitments c ON c.id = ct.commitment_id
        JOIN goals g ON g.id = ct.goal_id
        WHERE ct.goal_id = $1
          AND ct.is_critical_path = TRUE
          AND c.tenant_id = $2
          AND g.tenant_id = $2
        ORDER BY c.created_at ASC
    """

    async def _run(c: asyncpg.Connection) -> list[asyncpg.Record]:
        return await c.fetch(sql, goal_id, tenant_id)

    if conn is not None:
        rows = await _run(conn)
    else:
        pool = get_pool()
        async with pool.acquire() as c2:
            rows = await _run(c2)
    out: list[CriticalPathEntry] = []
    for r in rows:
        d = dict(r)
        cached = d.pop("goal_cached_health", None)
        cmt = CommitmentRow.model_validate(d)
        out.append(
            CriticalPathEntry(
                commitment=cmt,
                goal_id=goal_id,
                is_critical_path=True,
                cached_health=cached,
            )
        )
    return out


# =====================================================================
# customer_health_timeline
# =====================================================================


async def customer_health_timeline(
    customer_id: UUID,
    *,
    tenant_id: UUID,
    window_days: int = 30,
    conn: asyncpg.Connection | None = None,
) -> list[HealthPoint]:
    """Daily snapshot of (date, total_at_risk_usd, blocked_commitment_count)
    for a customer over the last `window_days`.

    Wave 5-B simplification per Prompt 5-B: we reconstruct state from
    `state_change` observations whose content references the commitment
    and whose metadata carries `new_state`. We compute the state of each
    served commitment at the END of each day in the window, then sum
    revenue_at_risk_usd (with customer-ARR fallback) for all commitments
    whose state on that day was in AT_RISK_STATES.

    The cost model is O(commitments_served × window_days). For the
    typical customer with a handful of served commitments this is fast
    enough for a dashboard render.
    """
    today = datetime.now(timezone.utc).date()
    days = [today - timedelta(days=window_days - 1 - i) for i in range(window_days)]

    sql_served = """
        SELECT cc.commitment_id, cc.revenue_at_risk_usd,
               c.created_at, c.state AS current_state
        FROM customer_commitments cc
        JOIN commitments c ON c.id = cc.commitment_id
        WHERE cc.customer_resource_id = $1
          AND cc.tenant_id = $2
    """
    sql_customer = """
        SELECT current_value FROM resources
        WHERE id = $1 AND tenant_id = $2
    """
    # Pull all state_change observations for these commitments. One
    # query is cheaper than per-day.
    sql_states = """
        SELECT o.occurred_at,
               (o.content->>'entity_id')::uuid AS commitment_id,
               o.content->'metadata'->>'new_state' AS new_state
        FROM observations o
        WHERE o.tenant_id = $2
          AND o.kind = 'state_change'
          AND (o.content->>'entity_kind') = 'commitment'
          AND (o.content->>'entity_id')::uuid = ANY($1::uuid[])
          AND o.occurred_at >= (now() - $3::interval)
        ORDER BY o.occurred_at ASC
    """

    async def _do(c: asyncpg.Connection) -> list[HealthPoint]:
        served_rows = await c.fetch(sql_served, customer_id, tenant_id)
        if not served_rows:
            # No served commitments → flatline empty report.
            return [
                HealthPoint(day=d, total_at_risk_usd=Decimal("0"), blocked_commitment_count=0)
                for d in days
            ]
        cust_row = await c.fetchrow(sql_customer, customer_id, tenant_id)
        cv = dict(cust_row["current_value"] or {}) if cust_row else {}
        arr_cents = int(cv.get("arr_cents", 0) or 0)
        arr_usd = (Decimal(arr_cents) / Decimal(100)).quantize(Decimal("0.01"))

        cmt_ids = [r["commitment_id"] for r in served_rows]
        state_rows = await c.fetch(
            sql_states, cmt_ids, tenant_id, timedelta(days=int(window_days))
        )

        # Build per-commitment timeline: list[(occurred_at date, new_state)]
        transitions: dict[UUID, list[tuple[date, str]]] = {
            r["commitment_id"]: [] for r in served_rows
        }
        for s in state_rows:
            cid = s["commitment_id"]
            st = s["new_state"]
            if cid in transitions and st:
                transitions[cid].append((s["occurred_at"].date(), st))

        # Initial state at window-start = current_state if we saw no
        # earlier transitions, else the first transition's state.
        rar_by_cmt: dict[UUID, Decimal | None] = {
            r["commitment_id"]: r["revenue_at_risk_usd"] for r in served_rows
        }

        n_served = len(served_rows)
        per_fallback = (
            (arr_usd / Decimal(n_served)).quantize(Decimal("0.01"))
            if n_served > 0 and arr_usd > 0
            else Decimal("0")
        )

        # For each day, compute state of each commitment at end-of-day.
        current_state: dict[UUID, str] = {}
        for r in served_rows:
            current_state[r["commitment_id"]] = r["current_state"] or ""

        # For each day we replay transitions up to EOD.
        # Sort transitions per cmt by date ASC (already sorted).
        pointer: dict[UUID, int] = {cid: 0 for cid in transitions}

        # Seed current_state to the state BEFORE the first transition
        # within window (the "incoming" state). We approximate: assume
        # the state at the start of the window equals the state before
        # the first transition we have; if there are no transitions, we
        # leave it at current_state (post-window final). This is
        # acceptable under Wave 5-B's "daily snapshot" simplification.
        timeline_per_day: dict[date, dict[UUID, str]] = {}
        for day in days:
            # Advance pointer: apply any transitions whose date <= day.
            for cid, tx_list in transitions.items():
                while pointer[cid] < len(tx_list) and tx_list[pointer[cid]][0] <= day:
                    current_state[cid] = tx_list[pointer[cid]][1]
                    pointer[cid] += 1
            timeline_per_day[day] = dict(current_state)

        out: list[HealthPoint] = []
        for day in days:
            day_state = timeline_per_day[day]
            blocked_cnt = sum(1 for s in day_state.values() if s in AT_RISK_STATES)
            total = Decimal("0")
            for cid, st in day_state.items():
                if st not in AT_RISK_STATES:
                    continue
                rar = rar_by_cmt.get(cid)
                if rar is not None:
                    total += Decimal(rar)
                else:
                    total += per_fallback
            out.append(
                HealthPoint(
                    day=day,
                    total_at_risk_usd=total.quantize(Decimal("0.01")),
                    blocked_commitment_count=blocked_cnt,
                )
            )
        return out

    if conn is not None:
        return await _do(conn)
    pool = get_pool()
    async with pool.acquire() as c2:
        return await _do(c2)


__all__ = [
    "AT_RISK_STATES",
    "OWNER_COMMITMENT_WARN_THRESHOLD",
    "CapabilityRisk",
    "CriticalPathEntry",
    "CustomerRevenueRow",
    "FeasibilityReport",
    "HealthPoint",
    "ProposedCommitment",
    "RevenueAtRiskReport",
    "capability_at_risk",
    "commitment_feasibility",
    "critical_path",
    "customer_health_timeline",
    "revenue_at_risk",
]
