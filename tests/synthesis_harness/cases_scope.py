"""Scope routing test cases.

The scope-routing layer is `services/think/region_locks.py`:
- `compute_primary_entity` deterministic precedence
- `region_lock_key` stable hash
- `touched_entity_ids` extraction from a retrieval-like object
- The validator's out-of-region rejection (covered indirectly here via
  the primitive functions; full validator round-trip is deferred to
  the reconciliation stage which exercises Think end-to-end)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg

from services.think.region_locks import (
    compute_primary_entity,
    region_lock_key,
    touched_entity_ids,
)
from lib.shared.ids import uuid7

from . import _fixtures as F
from ._runner import Case


# =====================================================================
# S1 — primary entity precedence: commitment > goal > decision > resource > actor
# =====================================================================


async def _setup_precedence(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    cid = uuid7()
    gid = uuid7()
    did = uuid7()
    rid = uuid7()
    aid = uuid7()
    return {"cid": cid, "gid": gid, "did": did, "rid": rid, "aid": aid}


async def _run_precedence(_pool: asyncpg.Pool, ctx: dict) -> dict:
    # Mixed entity list, deliberately out of order
    entities = [
        {"type": "actor", "id": str(ctx["aid"])},
        {"type": "resource", "id": str(ctx["rid"])},
        {"type": "decision", "id": str(ctx["did"])},
        {"type": "goal", "id": str(ctx["gid"])},
        {"type": "commitment", "id": str(ctx["cid"])},
    ]
    primary = compute_primary_entity(entities)
    return {"primary": primary}


def _expected_precedence(ctx: dict) -> dict:
    return {"primary": ("commitment", str(ctx["cid"]))}


def _assert_precedence(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual["primary"] != expected["primary"]:
        return False, f"got {actual['primary']!r} expected {expected['primary']!r}"
    return True, ""


CASE_PRECEDENCE = Case(
    stage="scope",
    name="primary_entity_precedence",
    intent="Mixed entity list resolves to commitment via type precedence",
    setup=_setup_precedence,
    run=_run_precedence,
    expected=_expected_precedence,
    assertion=_assert_precedence,
)


# =====================================================================
# S2 — region_lock_key stability under list permutation
# =====================================================================


async def _setup_lock_stable(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    tenant = uuid7()
    e = [
        ("commitment", uuid7()),
        ("goal", uuid7()),
        ("actor", uuid7()),
    ]
    return {"tenant": tenant, "entities": e}


async def _run_lock_stable(_pool: asyncpg.Pool, ctx: dict) -> dict:
    a = region_lock_key(ctx["tenant"], ctx["entities"])
    # Permute: reverse, plus shuffle middle
    permuted = [ctx["entities"][i] for i in (2, 0, 1)]
    b = region_lock_key(ctx["tenant"], permuted)
    return {"a": list(a), "b": list(b)}


def _expected_lock_stable(_ctx: dict) -> dict:
    return {"equal": True}


def _assert_lock_stable(actual: dict, _expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual["a"] != actual["b"]:
        return False, f"keys diverged across permutation: a={actual['a']} b={actual['b']}"
    return True, ""


CASE_LOCK_STABLE = Case(
    stage="scope",
    name="region_lock_permutation_stable",
    intent="region_lock_key is invariant under entity-list permutation",
    setup=_setup_lock_stable,
    run=_run_lock_stable,
    expected=_expected_lock_stable,
    assertion=_assert_lock_stable,
)


# =====================================================================
# S3 — region_lock_key partitions distinct tenants
# =====================================================================


async def _setup_lock_tenant(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    e = [("commitment", uuid7())]
    return {"e": e, "t1": uuid7(), "t2": uuid7()}


async def _run_lock_tenant(_pool: asyncpg.Pool, ctx: dict) -> dict:
    a = region_lock_key(ctx["t1"], ctx["e"])
    b = region_lock_key(ctx["t2"], ctx["e"])
    return {"tenant_hashes_differ": a[0] != b[0], "entity_hashes_equal": a[1] == b[1]}


def _expected_lock_tenant(_ctx: dict) -> dict:
    return {"tenant_hashes_differ": True, "entity_hashes_equal": True}


def _assert_lock_tenant(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual != expected:
        return False, f"got {actual} expected {expected}"
    return True, ""


CASE_LOCK_TENANT = Case(
    stage="scope",
    name="region_lock_tenant_isolation",
    intent="Different tenants → different tenant_hash; same entities → same entity_hash",
    setup=_setup_lock_tenant,
    run=_run_lock_tenant,
    expected=_expected_lock_tenant,
    assertion=_assert_lock_tenant,
)


# =====================================================================
# S4 — touched_entity_ids gathers Models, Acts, Resources, and trigger seeds
# =====================================================================


@dataclass
class _StubModel:
    id: UUID
    scope_entities: list[dict] = field(default_factory=list)


@dataclass
class _StubGoal:
    id: UUID


@dataclass
class _StubTrigger:
    seed_entity_ids: list[dict] = field(default_factory=list)
    model_id: UUID | None = None
    observation_id: UUID | None = None


@dataclass
class _StubResult:
    models: list[Any] = field(default_factory=list)
    acts: dict[str, list] = field(default_factory=dict)
    resources: list[Any] = field(default_factory=list)
    trigger: Any | None = None


async def _setup_touched(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    return {
        "model_id": uuid7(),
        "scope_commit_id": uuid7(),
        "goal_id": uuid7(),
        "trigger_obs_id": uuid7(),
        "trigger_seed_commit": uuid7(),
    }


async def _run_touched(_pool: asyncpg.Pool, ctx: dict) -> dict:
    m = _StubModel(
        id=ctx["model_id"],
        scope_entities=[{"type": "commitment", "id": str(ctx["scope_commit_id"])}],
    )
    g = _StubGoal(id=ctx["goal_id"])
    t = _StubTrigger(
        seed_entity_ids=[{"type": "commitment", "id": str(ctx["trigger_seed_commit"])}],
        observation_id=ctx["trigger_obs_id"],
    )
    res = _StubResult(models=[m], acts={"goals": [g]}, resources=[], trigger=t)
    touched = touched_entity_ids(res)
    return {"touched": [(t, str(i)) for (t, i) in touched]}


def _expected_touched(ctx: dict) -> dict:
    expected_set = {
        ("model", str(ctx["model_id"])),
        ("commitment", str(ctx["scope_commit_id"])),
        ("goal", str(ctx["goal_id"])),
        ("commitment", str(ctx["trigger_seed_commit"])),
        ("observation", str(ctx["trigger_obs_id"])),
    }
    return {"set": expected_set}


def _assert_touched(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    actual_set = {tuple(x) for x in actual["touched"]}
    if actual_set != expected["set"]:
        miss = expected["set"] - actual_set
        leak = actual_set - expected["set"]
        return False, f"missing={miss} leaked={leak}"
    return True, ""


CASE_TOUCHED = Case(
    stage="scope",
    name="touched_entity_ids_aggregation",
    intent="touched_entity_ids gathers Models (id+scope), Acts, trigger seeds, observation",
    setup=_setup_touched,
    run=_run_touched,
    expected=_expected_touched,
    assertion=_assert_touched,
)


# =====================================================================
# S5 — primary entity stable under id-string ordering at same precedence tier
# =====================================================================


async def _setup_tie(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    # Two commitments at same precedence; deterministic by id ascending
    c1 = "00000000-0000-0000-0000-000000000001"
    c2 = "00000000-0000-0000-0000-000000000002"
    return {"c1": c1, "c2": c2}


async def _run_tie(_pool: asyncpg.Pool, ctx: dict) -> dict:
    forward = compute_primary_entity([
        {"type": "commitment", "id": ctx["c1"]},
        {"type": "commitment", "id": ctx["c2"]},
    ])
    backward = compute_primary_entity([
        {"type": "commitment", "id": ctx["c2"]},
        {"type": "commitment", "id": ctx["c1"]},
    ])
    return {"forward": forward, "backward": backward}


def _expected_tie(ctx: dict) -> dict:
    return {"primary": ("commitment", ctx["c1"])}


def _assert_tie(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual["forward"] != expected["primary"] or actual["backward"] != expected["primary"]:
        return False, f"got fwd={actual['forward']} bwd={actual['backward']} expected={expected['primary']}"
    return True, ""


CASE_TIE = Case(
    stage="scope",
    name="primary_entity_tiebreak_id_asc",
    intent="Same-tier ties resolve to lexicographically smallest id, regardless of input order",
    setup=_setup_tie,
    run=_run_tie,
    expected=_expected_tie,
    assertion=_assert_tie,
)


CASES = [
    CASE_PRECEDENCE,
    CASE_LOCK_STABLE,
    CASE_LOCK_TENANT,
    CASE_TOUCHED,
    CASE_TIE,
]
