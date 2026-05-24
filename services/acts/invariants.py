"""
services/acts/invariants.py — C1-C10 and G1-G4 validation.

Per ARCHITECTURE-FINAL.md §3.1 (Goals) and §3.2 (Commitments). These
checks run at INSERT and transition time; they are also callable
standalone for audit (pass an existing connection/transaction).

Each check returns (ok: bool, violations: list[InvariantViolation]).
The top-level `validate_commitment_invariants` / `validate_goal_invariants`
wrappers aggregate all of them and surface every violation at once —
callers that want fail-fast semantics can loop with `raise_on_violation`.

Cascade engine (Wave 3-B) reuses these same functions, so they must
be pure SQL queries against the passed-in connection — no pool lookups.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import InvariantViolation
from lib.shared.types import CommitmentState


# ---------------------------------------------------------------------
# Terminal state sets (duplicated from state_machines.py deliberately;
# these are authoritative per spec §3 and used in bare SQL contexts).
# ---------------------------------------------------------------------
_COMMITMENT_TERMINAL_SQL = ("doneverified", "closed")


# =====================================================================
# Commitment invariants (C1-C10)
# =====================================================================

async def _check_c1_owner_required(
    conn: asyncpg.Connection, commitment_id: UUID
) -> list[InvariantViolation]:
    """C1: active/blocked/paused/doneunverified require owner_id."""
    row = await conn.fetchrow(
        "SELECT state, owner_id FROM commitments WHERE id = $1",
        commitment_id,
    )
    if row is None:
        return []
    state = row["state"]
    owner_id = row["owner_id"]
    if state in ("active", "blocked", "paused", "doneunverified") and owner_id is None:
        return [
            InvariantViolation(
                "C1",
                f"commitment in state {state!r} requires owner_id",
                commitment_id=str(commitment_id),
                state=state,
            )
        ]
    return []


async def _check_c3_doneverified_resolved(
    conn: asyncpg.Connection, commitment_id: UUID
) -> list[InvariantViolation]:
    """C3: doneverified requires >= 1 resolved_by_event_id."""
    row = await conn.fetchrow(
        "SELECT state, resolved_by_event_ids FROM commitments WHERE id = $1",
        commitment_id,
    )
    if row is None:
        return []
    resolved = row["resolved_by_event_ids"] or []
    if row["state"] == "doneverified" and len(resolved) == 0:
        return [
            InvariantViolation(
                "C3",
                "doneverified commitment requires >=1 resolved_by_event_id",
                commitment_id=str(commitment_id),
            )
        ]
    return []


async def _check_c5_active_owner_contributors(
    conn: asyncpg.Connection, commitment_id: UUID
) -> list[InvariantViolation]:
    """C5: owner and contributors reference active Actors."""
    violations: list[InvariantViolation] = []
    row = await conn.fetchrow(
        "SELECT owner_id FROM commitments WHERE id = $1",
        commitment_id,
    )
    if row is None:
        return []
    owner_id = row["owner_id"]
    if owner_id is not None:
        owner_status = await conn.fetchval(
            "SELECT status FROM actors WHERE id = $1", owner_id
        )
        if owner_status is None:
            violations.append(
                InvariantViolation(
                    "C5",
                    "owner_id does not reference an actor",
                    commitment_id=str(commitment_id),
                    owner_id=str(owner_id),
                )
            )
        elif owner_status != "active":
            violations.append(
                InvariantViolation(
                    "C5",
                    f"owner actor status is {owner_status!r}, must be 'active'",
                    commitment_id=str(commitment_id),
                    owner_id=str(owner_id),
                    actor_status=owner_status,
                )
            )
    bad_contribs = await conn.fetch(
        """
        SELECT cc.actor_id, COALESCE(a.status, '<missing>') AS status
        FROM commitment_contributors cc
        LEFT JOIN actors a ON a.id = cc.actor_id
        WHERE cc.commitment_id = $1
          AND (a.status IS DISTINCT FROM 'active')
        """,
        commitment_id,
    )
    for r in bad_contribs:
        violations.append(
            InvariantViolation(
                "C5",
                f"contributor actor status is {r['status']!r}, must be 'active'",
                commitment_id=str(commitment_id),
                actor_id=str(r["actor_id"]),
                actor_status=r["status"],
            )
        )
    return violations


async def check_c6_depends_on_acyclic(
    conn: asyncpg.Connection,
    dependent_id: UUID,
    dependency_id: UUID,
) -> list[InvariantViolation]:
    """
    C6: inserting (dependent → dependency) would create a cycle iff
    dependent is already reachable from dependency via depends_on.

    This is the PRE-INSERT guard used by add_edge().
    """
    if dependent_id == dependency_id:
        return [
            InvariantViolation(
                "C6",
                "self-dependency forbidden",
                dependent=str(dependent_id),
                dependency=str(dependency_id),
            )
        ]
    # Would this edge create a cycle? Walk from `dependency_id` forward
    # along depends_on; if we reach `dependent_id` we'd have a cycle.
    cycle_row = await conn.fetchrow(
        """
        WITH RECURSIVE reach(commitment_id) AS (
          SELECT dependency_commitment_id FROM depends_on
          WHERE dependent_commitment_id = $1
          UNION
          SELECT d.dependency_commitment_id
          FROM depends_on d
          JOIN reach r ON r.commitment_id = d.dependent_commitment_id
        )
        SELECT 1 FROM reach WHERE commitment_id = $2 LIMIT 1
        """,
        dependency_id,  # start seed
        dependent_id,   # look for this
    )
    if cycle_row is not None:
        return [
            InvariantViolation(
                "C6",
                "depends_on edge would create a cycle",
                dependent=str(dependent_id),
                dependency=str(dependency_id),
            )
        ]
    return []


async def _check_c6_existing_cycle(
    conn: asyncpg.Connection, commitment_id: UUID
) -> list[InvariantViolation]:
    """
    C6 audit mode: detect if `commitment_id` participates in any
    existing cycle. Used by validate_commitment_invariants.
    """
    row = await conn.fetchrow(
        """
        WITH RECURSIVE reach(commitment_id, depth) AS (
          SELECT dependency_commitment_id, 1
          FROM depends_on
          WHERE dependent_commitment_id = $1
          UNION
          SELECT d.dependency_commitment_id, r.depth + 1
          FROM depends_on d
          JOIN reach r ON r.commitment_id = d.dependent_commitment_id
          WHERE r.depth < 1000
        )
        SELECT 1 FROM reach WHERE commitment_id = $1 LIMIT 1
        """,
        commitment_id,
    )
    if row is not None:
        return [
            InvariantViolation(
                "C6",
                "commitment is part of a depends_on cycle",
                commitment_id=str(commitment_id),
            )
        ]
    return []


async def _check_c9_due_date_future(
    conn: asyncpg.Connection, commitment_id: UUID
) -> list[InvariantViolation]:
    """C9: due_date at creation > now(). Already-past due_dates on
    existing rows are fine — this only fires when creation just happened
    (heuristic: created_at and last_state_change_at within 1s of each
    other, which is true on insert)."""
    row = await conn.fetchrow(
        """
        SELECT due_date, created_at, last_state_change_at
        FROM commitments WHERE id = $1
        """,
        commitment_id,
    )
    if row is None or row["due_date"] is None:
        return []
    # Only enforce at creation time.
    if abs(
        (row["last_state_change_at"] - row["created_at"]).total_seconds()
    ) > 1.0:
        return []
    if row["due_date"] <= row["created_at"]:
        return [
            InvariantViolation(
                "C9",
                "due_date at creation must be in the future",
                commitment_id=str(commitment_id),
                due_date=row["due_date"].isoformat(),
                created_at=row["created_at"].isoformat(),
            )
        ]
    return []


async def _check_c10_contributes_or_maintenance(
    conn: asyncpg.Connection, commitment_id: UUID
) -> list[InvariantViolation]:
    """
    C10: every active Commitment has >=1 contributes_to OR the typed
    `is_maintenance` flag. Per AUDIT-REVIEW-1-FIXES I1, the canonical
    encoding is the typed `commitments.is_maintenance BOOLEAN` column
    (migration 0021). For backwards compatibility with rows created
    before that migration, we also honor the legacy JSONB-encoded flag
    `estimated_capacity["maintenance"] is True`.

    Only enforced for non-terminal non-proposed states (active, blocked,
    paused, doneunverified). Terminal states may legitimately lack edges
    at close time.
    """
    row = await conn.fetchrow(
        """
        SELECT state, is_maintenance, estimated_capacity
        FROM commitments WHERE id = $1
        """,
        commitment_id,
    )
    if row is None:
        return []
    state = row["state"]
    if state in ("proposed", "doneverified", "closed"):
        return []

    maintenance = bool(row["is_maintenance"])
    if not maintenance:
        # Legacy fallback for rows predating migration 0021.
        capacity = row["estimated_capacity"]
        if isinstance(capacity, str):
            import json as _json
            try:
                capacity = _json.loads(capacity)
            except Exception:
                capacity = None
        if isinstance(capacity, dict) and capacity.get("maintenance") is True:
            maintenance = True

    if maintenance:
        return []
    n_edges = await conn.fetchval(
        "SELECT COUNT(*) FROM contributes_to WHERE commitment_id = $1",
        commitment_id,
    )
    if (n_edges or 0) == 0:
        return [
            InvariantViolation(
                "C10",
                "commitment has no contributes_to edges and is not maintenance",
                commitment_id=str(commitment_id),
                state=state,
            )
        ]
    return []


async def is_unsatisfied_dependency(
    conn: asyncpg.Connection, dependency_commitment_id: UUID
) -> bool:
    """
    A depends_on edge is UNSATISFIED if the dependency is not in a
    terminal success state. For Wave 1, we treat any commitment not in
    `doneverified` as unsatisfied (blocked / closed / active / etc. all
    still leave the dependent waiting or impossible).
    """
    row = await conn.fetchrow(
        "SELECT state FROM commitments WHERE id = $1",
        dependency_commitment_id,
    )
    if row is None:
        return True
    return row["state"] != "doneverified"


async def count_unsatisfied_dependencies(
    conn: asyncpg.Connection, commitment_id: UUID
) -> int:
    """Number of depends_on dependencies not in doneverified."""
    n = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM depends_on d
        JOIN commitments c ON c.id = d.dependency_commitment_id
        WHERE d.dependent_commitment_id = $1
          AND c.state <> 'doneverified'
        """,
        commitment_id,
    )
    return int(n or 0)


async def count_revisited_constraining_decisions(
    conn: asyncpg.Connection, commitment_id: UUID
) -> int:
    """Number of constrained_by decisions currently in 'revisited'."""
    n = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM constrained_by cb
        JOIN decisions d ON d.id = cb.decision_id
        WHERE cb.commitment_id = $1
          AND d.state = 'revisited'
        """,
        commitment_id,
    )
    return int(n or 0)


async def _check_c2_blocked_reason(
    conn: asyncpg.Connection, commitment_id: UUID
) -> list[InvariantViolation]:
    """C2: blocked requires >=1 unsatisfied depends_on OR >=1
    constrained_by Decision in 'revisited'."""
    row = await conn.fetchrow(
        "SELECT state FROM commitments WHERE id = $1", commitment_id
    )
    if row is None or row["state"] != "blocked":
        return []
    n_deps = await count_unsatisfied_dependencies(conn, commitment_id)
    n_rev = await count_revisited_constraining_decisions(conn, commitment_id)
    if n_deps == 0 and n_rev == 0:
        return [
            InvariantViolation(
                "C2",
                "blocked commitment has no unsatisfied dependency "
                "or revisited constraining decision",
                commitment_id=str(commitment_id),
            )
        ]
    return []


async def validate_commitment_invariants(
    commitment_id: UUID,
    tx: asyncpg.Connection,
    *,
    raise_on_violation: bool = False,
) -> tuple[bool, list[InvariantViolation]]:
    """
    Run C1-C10 on a single Commitment row (using `tx`). Returns
    (ok, violations). If `raise_on_violation=True`, raises the FIRST
    violation — invariants are deterministic so first vs. all is a
    caller preference.
    """
    violations: list[InvariantViolation] = []
    violations += await _check_c1_owner_required(tx, commitment_id)
    violations += await _check_c2_blocked_reason(tx, commitment_id)
    violations += await _check_c3_doneverified_resolved(tx, commitment_id)
    violations += await _check_c5_active_owner_contributors(tx, commitment_id)
    violations += await _check_c6_existing_cycle(tx, commitment_id)
    violations += await _check_c9_due_date_future(tx, commitment_id)
    violations += await _check_c10_contributes_or_maintenance(tx, commitment_id)
    # C4, C7, C8 are enforced at the caller (transition / insert) — no
    # ambient check possible after the fact. C7 is enforced by DB NOT NULL.
    ok = not violations
    if raise_on_violation and violations:
        raise violations[0]
    return ok, violations


# =====================================================================
# Goal invariants (G1-G4)
# =====================================================================

async def _check_g1_active_has_work(
    conn: asyncpg.Connection, goal_id: UUID
) -> list[InvariantViolation]:
    """G1: active Goal has >=1 contributing Commitment OR >=1 sub-Goal."""
    row = await conn.fetchrow(
        "SELECT state FROM goals WHERE id = $1", goal_id
    )
    if row is None or row["state"] != "active":
        return []
    n_commitments = await conn.fetchval(
        "SELECT COUNT(*) FROM contributes_to WHERE goal_id = $1", goal_id
    )
    n_subgoals = await conn.fetchval(
        "SELECT COUNT(*) FROM goals WHERE parent_goal_id = $1", goal_id
    )
    if (n_commitments or 0) + (n_subgoals or 0) == 0:
        return [
            InvariantViolation(
                "G1",
                "active goal has neither contributing commitments "
                "nor sub-goals",
                goal_id=str(goal_id),
            )
        ]
    return []


async def check_g2_tree_acyclic(
    conn: asyncpg.Connection,
    goal_id: UUID,
    parent_goal_id: UUID | None,
) -> list[InvariantViolation]:
    """
    G2: inserting or reparenting `goal_id` under `parent_goal_id` is
    legal iff parent is not already a descendant of goal_id.

    Used as a PRE-INSERT / PRE-UPDATE guard.
    """
    if parent_goal_id is None:
        return []
    if parent_goal_id == goal_id:
        return [
            InvariantViolation(
                "G2",
                "goal cannot be its own parent",
                goal_id=str(goal_id),
            )
        ]
    # Walk up from parent_goal_id; if we hit goal_id, it's a cycle.
    cycle_row = await conn.fetchrow(
        """
        WITH RECURSIVE ancestors(id, depth) AS (
          SELECT $1::uuid, 1
          UNION
          SELECT g.parent_goal_id, a.depth + 1
          FROM goals g
          JOIN ancestors a ON a.id = g.id
          WHERE g.parent_goal_id IS NOT NULL AND a.depth < 1000
        )
        SELECT 1 FROM ancestors WHERE id = $2 LIMIT 1
        """,
        parent_goal_id,
        goal_id,
    )
    if cycle_row is not None:
        return [
            InvariantViolation(
                "G2",
                "goal tree cycle detected",
                goal_id=str(goal_id),
                parent_goal_id=str(parent_goal_id),
            )
        ]
    return []


async def _check_g2_existing_cycle(
    conn: asyncpg.Connection, goal_id: UUID
) -> list[InvariantViolation]:
    """G2 audit: does `goal_id` reach itself via parent_goal_id?"""
    row = await conn.fetchrow(
        """
        WITH RECURSIVE anc(id, depth) AS (
          SELECT parent_goal_id, 1 FROM goals WHERE id = $1
          UNION
          SELECT g.parent_goal_id, a.depth + 1
          FROM goals g
          JOIN anc a ON a.id = g.id
          WHERE g.parent_goal_id IS NOT NULL AND a.depth < 1000
        )
        SELECT 1 FROM anc WHERE id = $1 LIMIT 1
        """,
        goal_id,
    )
    if row is not None:
        return [
            InvariantViolation(
                "G2",
                "goal is part of a parent_goal_id cycle",
                goal_id=str(goal_id),
            )
        ]
    return []


async def compute_worst_of_health(
    conn: asyncpg.Connection, goal_id: UUID
) -> str:
    """
    G3: worst-of-critical-path-commitment-state.

    Wave 1 rule (direct children only):
      - any critical-path in state 'blocked'   → 'degraded'
      - any critical-path in 'closed' without
        matching 'doneverified' sibling(s)    → 'critical' if others incomplete
      - all critical-path doneverified         → 'healthy'
      - otherwise                              → 'healthy' (default)
    """
    rows = await conn.fetch(
        """
        SELECT c.state
        FROM contributes_to ct
        JOIN commitments c ON c.id = ct.commitment_id
        WHERE ct.goal_id = $1 AND ct.is_critical_path = TRUE
        """,
        goal_id,
    )
    if not rows:
        return "healthy"
    states = [r["state"] for r in rows]
    if any(s == "blocked" for s in states):
        return "degraded"
    # If any critical-path is closed (non-success terminal) and there
    # are other critical-path commitments not doneverified, that is
    # critical: a required path failed before siblings completed.
    has_closed = any(s == "closed" for s in states)
    if has_closed:
        siblings_incomplete = any(s != "doneverified" for s in states)
        if siblings_incomplete:
            return "critical"
    if all(s == "doneverified" for s in states):
        return "healthy"
    # Mixed state without block/closed — default healthy.
    return "healthy"


async def _check_g3_cached_health_fresh(
    conn: asyncpg.Connection, goal_id: UUID
) -> list[InvariantViolation]:
    """G3 audit: cached_health matches computed-worst-of."""
    row = await conn.fetchrow(
        "SELECT cached_health FROM goals WHERE id = $1", goal_id
    )
    if row is None:
        return []
    expected = await compute_worst_of_health(conn, goal_id)
    if row["cached_health"] != expected:
        return [
            InvariantViolation(
                "G3",
                "cached_health is stale",
                goal_id=str(goal_id),
                cached=row["cached_health"],
                expected=expected,
            )
        ]
    return []


async def _check_g4_achieved_requires_done(
    conn: asyncpg.Connection, goal_id: UUID
) -> list[InvariantViolation]:
    """
    G4: achieved requires all critical-path Commitments in 'doneverified'.

    Wave 1 implementation: direct children only. Full cascade (sub-goal
    trees) is Wave 3-B.
    """
    row = await conn.fetchrow(
        "SELECT state FROM goals WHERE id = $1", goal_id
    )
    if row is None or row["state"] != "achieved":
        return []
    bad = await conn.fetchrow(
        """
        SELECT COUNT(*) AS n
        FROM contributes_to ct
        JOIN commitments c ON c.id = ct.commitment_id
        WHERE ct.goal_id = $1
          AND ct.is_critical_path = TRUE
          AND c.state <> 'doneverified'
        """,
        goal_id,
    )
    if bad and bad["n"] > 0:
        return [
            InvariantViolation(
                "G4",
                "achieved goal has critical-path commitments not doneverified",
                goal_id=str(goal_id),
                incomplete_count=int(bad["n"]),
            )
        ]
    return []


async def validate_goal_invariants(
    goal_id: UUID,
    tx: asyncpg.Connection,
    *,
    raise_on_violation: bool = False,
) -> tuple[bool, list[InvariantViolation]]:
    """Run G1-G4 on a single Goal row."""
    violations: list[InvariantViolation] = []
    violations += await _check_g1_active_has_work(tx, goal_id)
    violations += await _check_g2_existing_cycle(tx, goal_id)
    violations += await _check_g3_cached_health_fresh(tx, goal_id)
    violations += await _check_g4_achieved_requires_done(tx, goal_id)
    ok = not violations
    if raise_on_violation and violations:
        raise violations[0]
    return ok, violations


__all__ = [
    "validate_commitment_invariants",
    "validate_goal_invariants",
    "check_c6_depends_on_acyclic",
    "check_g2_tree_acyclic",
    "is_unsatisfied_dependency",
    "count_unsatisfied_dependencies",
    "count_revisited_constraining_decisions",
    "compute_worst_of_health",
]
