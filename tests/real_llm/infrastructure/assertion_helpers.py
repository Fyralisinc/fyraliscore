"""Range, set, existential, and structural assertion helpers for real-LLM tests."""

from __future__ import annotations

from collections import Counter
from typing import Iterable
from uuid import UUID

import asyncpg

from lib.shared.types import ModelRow


def _prefix(context: str, msg: str) -> str:
    return f"{context}: {msg}" if context else msg


def _entity_in_scope(
    model: ModelRow,
    scope_entity_type: str | None,
    scope_entity_id: UUID | None,
) -> bool:
    """Return True iff at least one scope_entities entry matches type and/or id."""
    target_id = str(scope_entity_id) if scope_entity_id is not None else None
    for entry in model.scope_entities:
        if scope_entity_type is not None and entry.get("type") != scope_entity_type:
            continue
        if target_id is not None and str(entry.get("id")) != target_id:
            continue
        return True
    return False


def assert_model_count_in_range(
    models: Iterable[ModelRow],
    low: int,
    high: int,
    context: str = "",
) -> None:
    """Assert the number of models is within the inclusive [low, high] band."""
    actual = len(list(models))
    assert low <= actual <= high, _prefix(
        context,
        f"Model count {actual} outside expected range [{low}, {high}]",
    )


def assert_at_least_one_model_matching(
    models: Iterable[ModelRow],
    *,
    scope_actor_id: UUID | None = None,
    scope_entity_type: str | None = None,
    scope_entity_id: UUID | None = None,
    proposition_kind: str | set[str] | None = None,
    proposition_text_contains: list[str] | None = None,
    confidence_range: tuple[float, float] | None = None,
    context: str = "",
) -> list[ModelRow]:
    """Assert at least one Model matches all supplied criteria; return matches."""
    models_list = list(models)
    matches: list[ModelRow] = []
    for m in models_list:
        if scope_actor_id is not None and scope_actor_id not in m.scope_actors:
            continue
        if scope_entity_type is not None or scope_entity_id is not None:
            if not _entity_in_scope(m, scope_entity_type, scope_entity_id):
                continue
        if proposition_kind is not None:
            kinds = (
                {proposition_kind}
                if isinstance(proposition_kind, str)
                else set(proposition_kind)
            )
            if m.proposition.get("kind") not in kinds:
                continue
        if proposition_text_contains:
            natural_lc = m.natural.lower()
            if not any(s.lower() in natural_lc for s in proposition_text_contains):
                continue
        if confidence_range is not None:
            lo, hi = confidence_range
            if not (lo <= m.confidence <= hi):
                continue
        matches.append(m)

    assert matches, _prefix(
        context,
        f"No Model matching criteria found among {len(models_list)} Models",
    )
    return matches


def assert_proposition_kind_distribution(
    models: Iterable[ModelRow],
    expected: dict[str, tuple[float, float]],
    context: str = "",
) -> None:
    """Assert the per-kind fraction of models falls within expected bands."""
    models_list = list(models)
    assert models_list, _prefix(context, "No Models to assess distribution")

    counts: Counter[str] = Counter(
        m.proposition.get("kind") for m in models_list
    )
    total = sum(counts.values())

    violations: list[str] = []
    for kind, (min_frac, max_frac) in expected.items():
        actual_frac = counts.get(kind, 0) / total
        if not (min_frac <= actual_frac <= max_frac):
            violations.append(
                f"  {kind}: {actual_frac:.2f} (expected {min_frac:.2f}-{max_frac:.2f})"
            )

    assert not violations, _prefix(
        context,
        "Proposition kind distribution violations:\n" + "\n".join(violations),
    )


async def assert_commitment_transitioned(
    commitment_id: UUID,
    from_state: str,
    to_state: str,
    *,
    pool: asyncpg.Pool,
    within_observations: int | None = None,
    context: str = "",
) -> None:
    """Assert a state_change observation records the from->to transition for commitment_id."""
    target_id = str(commitment_id)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT content, occurred_at
            FROM observations
            WHERE kind = 'state_change'
              AND content->>'entity_id' = $1
              AND content->>'entity_kind' = 'commitment'
            ORDER BY occurred_at ASC, sequence_num ASC
            """,
            target_id,
        )

    if within_observations is not None:
        rows = rows[:within_observations]

    found = False
    seen: list[tuple[str | None, str | None]] = []
    for r in rows:
        content = r["content"]
        if isinstance(content, str):
            import json as _json

            content = _json.loads(content)
        prev = content.get("from_state")
        nxt = content.get("to_state")
        seen.append((prev, nxt))
        if prev == from_state and nxt == to_state:
            found = True
            break

    assert found, _prefix(
        context,
        f"Commitment {commitment_id} did not transition {from_state!r} -> {to_state!r}; "
        f"observed transitions: {seen}",
    )


async def assert_cascade_chain_intact(
    tenant_id: UUID,
    starting_event_id: UUID,
    *,
    pool: asyncpg.Pool,
    min_depth: int = 1,
    context: str = "",
) -> None:
    """Walk observations.cause_id forward from starting_event_id; assert chain depth >= min_depth."""
    async with pool.acquire() as conn:
        # Confirm the starting observation exists for this tenant.
        starter = await conn.fetchval(
            """
            SELECT id FROM observations
            WHERE id = $1 AND tenant_id = $2
            LIMIT 1
            """,
            starting_event_id,
            tenant_id,
        )
        assert starter is not None, _prefix(
            context,
            f"Starting observation {starting_event_id} not found for tenant {tenant_id}",
        )

        # BFS over cause_id edges. depth counts edges traversed from the
        # starter; min_depth=1 means at least one downstream observation
        # references the starter (directly or transitively).
        frontier: set[UUID] = {starting_event_id}
        visited: set[UUID] = {starting_event_id}
        depth = 0
        while frontier and depth < min_depth:
            rows = await conn.fetch(
                """
                SELECT id FROM observations
                WHERE tenant_id = $1
                  AND cause_id = ANY($2::uuid[])
                """,
                tenant_id,
                list(frontier),
            )
            next_frontier: set[UUID] = set()
            for r in rows:
                rid = r["id"]
                if rid not in visited:
                    visited.add(rid)
                    next_frontier.add(rid)
            if not next_frontier:
                break
            depth += 1
            frontier = next_frontier

    assert depth >= min_depth, _prefix(
        context,
        f"Cascade chain from {starting_event_id} reached depth {depth}, "
        f"expected >= {min_depth}",
    )


async def assert_bridge_revenue_at_risk(
    tenant_id: UUID,
    customer_resource_id: UUID,
    range_usd: tuple[float, float],
    *,
    pool: asyncpg.Pool,
    context: str = "",
) -> None:
    """Sum revenue_at_risk_usd for non-terminal commitments served by the customer; assert in range."""
    low, high = range_usd
    async with pool.acquire() as conn:
        total = await conn.fetchval(
            """
            SELECT COALESCE(SUM(cc.revenue_at_risk_usd), 0)::float8
            FROM customer_commitments cc
            JOIN commitments c ON c.id = cc.commitment_id
            WHERE cc.tenant_id = $1
              AND cc.customer_resource_id = $2
              AND cc.revenue_at_risk_usd IS NOT NULL
              AND c.state NOT IN ('doneverified', 'closed')
            """,
            tenant_id,
            customer_resource_id,
        )
    actual = float(total or 0.0)
    assert low <= actual <= high, _prefix(
        context,
        f"Revenue-at-risk for customer {customer_resource_id} = ${actual:,.2f} "
        f"outside expected range [${low:,.2f}, ${high:,.2f}]",
    )


__all__ = [
    "assert_model_count_in_range",
    "assert_at_least_one_model_matching",
    "assert_proposition_kind_distribution",
    "assert_commitment_transitioned",
    "assert_cascade_chain_intact",
    "assert_bridge_revenue_at_risk",
]
