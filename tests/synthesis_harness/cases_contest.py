"""Contestation stage test cases — standing matrix, belief vs reading, multipliers."""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg

from services.contestability.service import (
    ContestationInput,
    NoStandingError,
    OVERRIDE_FLOOR,
    PRIMARY_SUBJECT_MULTIPLIER,
    SECONDARY_SUBJECT_MULTIPLIER,
    contest_model,
)
from services.contestability.standing import actor_has_standing_on_model

from . import _fixtures as F
from ._runner import Case


# =====================================================================
# C1 — primary subject contestation (belief): confidence × 0.3, floor 0.15
# =====================================================================


async def _setup_primary(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            primary = await F.make_actor(conn, tenant, display_name="Primary")
            secondary = await F.make_actor(conn, tenant, display_name="Secondary")
            mid = await F.make_model(
                conn, tenant,
                natural="Primary is making slow progress",
                confidence=0.8,  # belief at 0.8 → expect 0.8*0.3 = 0.24
                scope_actors=[primary, secondary],
            )
            return {
                "tenant": tenant,
                "primary": primary,
                "secondary": secondary,
                "mid": mid,
                "previous_confidence": 0.8,
            }


async def _run_primary(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await contest_model(
                conn,
                ContestationInput(
                    model_id=ctx["mid"],
                    contestor_actor_id=ctx["primary"],
                    tenant_id=ctx["tenant"],
                    contestation_kind="belief",
                    rationale="I disagree with this assessment of my progress",
                ),
            )
            row = await conn.fetchrow(
                "SELECT confidence, contested_count FROM models WHERE id=$1",
                ctx["mid"],
            )
    return {
        "previous_confidence": result.previous_confidence,
        "new_confidence": result.new_confidence,
        "standing_basis": result.standing_basis,
        "override_applied": result.override_applied,
        "db_confidence": float(row["confidence"]),
        "contested_count": row["contested_count"],
    }


def _expected_primary(ctx: dict) -> dict:
    expected = max(OVERRIDE_FLOOR, ctx["previous_confidence"] * PRIMARY_SUBJECT_MULTIPLIER)
    return {
        "new_confidence": expected,
        "previous_confidence": ctx["previous_confidence"],
        "override_applied": True,
        "standing_basis": "scope",
        "contested_count": 1,
    }


def _assert_primary(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    diffs = []
    for k in ("override_applied", "standing_basis", "contested_count"):
        if actual[k] != expected[k]:
            diffs.append(f"{k}: got {actual[k]!r} expected {expected[k]!r}")
    if abs(actual["new_confidence"] - expected["new_confidence"]) > 1e-6:
        diffs.append(f"new_confidence: got {actual['new_confidence']} expected {expected['new_confidence']}")
    if abs(actual["db_confidence"] - expected["new_confidence"]) > 1e-6:
        diffs.append(f"db_confidence: got {actual['db_confidence']} expected {expected['new_confidence']}")
    return (not diffs), "; ".join(diffs)


CASE_PRIMARY = Case(
    stage="contestation",
    name="primary_subject_belief_multiplier",
    intent="Belief contestation by primary subject (scope_actors[0]) → confidence × 0.3, floor 0.15",
    setup=_setup_primary,
    run=_run_primary,
    expected=_expected_primary,
    assertion=_assert_primary,
    expected_confidence_range=(
        max(OVERRIDE_FLOOR, 0.8 * PRIMARY_SUBJECT_MULTIPLIER) - 1e-6,
        max(OVERRIDE_FLOOR, 0.8 * PRIMARY_SUBJECT_MULTIPLIER) + 1e-6,
    ),
    # T4: Ground truth here is mathematical: the override produces a
    # confidence of 0.24 by the spec's 0.3× rule, and the underlying
    # claim ("Primary is making slow progress") was contested as
    # incorrect — so a confidence of 0.24 in a contested claim is
    # deliberately *below* a "true" threshold. We label
    # ground_truth_correctness=False so a calibrated engine should
    # express low confidence in this proposition. The label is
    # objective for this case (fixture is a contested claim) but
    # the calibration measurement is structurally trivial — see
    # the harness REPORT for the broader caveat.
    ground_truth_correctness=False,
    extract_confidence=lambda actual: actual.get("new_confidence"),
    ground_truth_basis=(
        "fixture is a contested claim; engine's post-override "
        "confidence (0.24) is the engine's stated probability that "
        "the claim is true"
    ),
)


# =====================================================================
# C2 — secondary subject contestation (belief): × 0.5
# =====================================================================


async def _setup_secondary(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            primary = await F.make_actor(conn, tenant, display_name="Primary")
            secondary = await F.make_actor(conn, tenant, display_name="Secondary")
            mid = await F.make_model(
                conn, tenant,
                natural="Primary owes Secondary a deliverable",
                confidence=0.7,
                scope_actors=[primary, secondary],
            )
            return {
                "tenant": tenant,
                "primary": primary,
                "secondary": secondary,
                "mid": mid,
                "previous_confidence": 0.7,
            }


async def _run_secondary(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await contest_model(
                conn,
                ContestationInput(
                    model_id=ctx["mid"],
                    contestor_actor_id=ctx["secondary"],
                    tenant_id=ctx["tenant"],
                    contestation_kind="belief",
                    rationale="That's not what we agreed",
                ),
            )
    return {
        "new_confidence": result.new_confidence,
        "override_applied": result.override_applied,
        "standing_basis": result.standing_basis,
    }


def _expected_secondary(ctx: dict) -> dict:
    return {
        "new_confidence": max(OVERRIDE_FLOOR, ctx["previous_confidence"] * SECONDARY_SUBJECT_MULTIPLIER),
        "override_applied": True,
        "standing_basis": "scope",
    }


def _assert_secondary(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if not actual["override_applied"] or actual["standing_basis"] != "scope":
        return False, f"basis/override mismatch: {actual}"
    if abs(actual["new_confidence"] - expected["new_confidence"]) > 1e-6:
        return False, f"new_confidence got {actual['new_confidence']} expected {expected['new_confidence']}"
    return True, ""


CASE_SECONDARY = Case(
    stage="contestation",
    name="secondary_subject_belief_multiplier",
    intent="Belief contestation by secondary subject → confidence × 0.5",
    setup=_setup_secondary,
    run=_run_secondary,
    expected=_expected_secondary,
    assertion=_assert_secondary,
    expected_confidence_range=(0.7 * SECONDARY_SUBJECT_MULTIPLIER - 1e-6,
                                0.7 * SECONDARY_SUBJECT_MULTIPLIER + 1e-6),
    ground_truth_correctness=False,
    extract_confidence=lambda actual: actual.get("new_confidence"),
    ground_truth_basis="fixture is a contested claim; same as primary case",
)


# =====================================================================
# C3 — floor enforcement: low previous confidence × 0.3 < floor
# =====================================================================


async def _setup_floor(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            primary = await F.make_actor(conn, tenant)
            mid = await F.make_model(
                conn, tenant,
                natural="weak signal",
                confidence=0.3,  # 0.3 * 0.3 = 0.09 < 0.15 floor
                scope_actors=[primary],
            )
            return {"tenant": tenant, "primary": primary, "mid": mid}


async def _run_floor(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await contest_model(
                conn,
                ContestationInput(
                    model_id=ctx["mid"],
                    contestor_actor_id=ctx["primary"],
                    tenant_id=ctx["tenant"],
                    contestation_kind="belief",
                    rationale="floor test rationale enough chars",
                ),
            )
    return {"new_confidence": result.new_confidence}


def _expected_floor(_ctx: dict) -> dict:
    return {"new_confidence": OVERRIDE_FLOOR}


def _assert_floor(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if abs(actual["new_confidence"] - expected["new_confidence"]) > 1e-6:
        return False, f"got {actual['new_confidence']} expected {expected['new_confidence']}"
    return True, ""


CASE_FLOOR = Case(
    stage="contestation",
    name="confidence_floor_clamp",
    intent="Confidence × 0.3 below 0.15 clamps to OVERRIDE_FLOOR",
    setup=_setup_floor,
    run=_run_floor,
    expected=_expected_floor,
    assertion=_assert_floor,
    expected_confidence_range=(OVERRIDE_FLOOR - 1e-6, OVERRIDE_FLOOR + 1e-6),
    ground_truth_correctness=False,
    extract_confidence=lambda actual: actual.get("new_confidence"),
    ground_truth_basis="contested claim with floor-clamped confidence",
)


# =====================================================================
# C4 — no standing: outsider with no scope/owner/contributor link → NoStandingError
# =====================================================================


async def _setup_outsider(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            insider = await F.make_actor(conn, tenant, display_name="Insider")
            outsider = await F.make_actor(conn, tenant, display_name="Outsider")
            mid = await F.make_model(
                conn, tenant,
                natural="Some model",
                scope_actors=[insider],
            )
            return {"tenant": tenant, "outsider": outsider, "mid": mid}


async def _run_outsider(pool: asyncpg.Pool, ctx: dict) -> dict:
    raised = False
    err_class = None
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await contest_model(
                    conn,
                    ContestationInput(
                        model_id=ctx["mid"],
                        contestor_actor_id=ctx["outsider"],
                        tenant_id=ctx["tenant"],
                        contestation_kind="belief",
                        rationale="trying to contest from outside",
                    ),
                )
            except NoStandingError as exc:
                raised = True
                err_class = type(exc).__name__
    return {"raised": raised, "err_class": err_class}


def _expected_outsider(_ctx: dict) -> dict:
    return {"raised": True, "err_class": "NoStandingError"}


def _assert_outsider(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual != expected:
        return False, f"got {actual}"
    return True, ""


CASE_OUTSIDER = Case(
    stage="contestation",
    name="no_standing_raises",
    intent="Outsider with no scope/owner/contributor link → NoStandingError",
    setup=_setup_outsider,
    run=_run_outsider,
    expected=_expected_outsider,
    assertion=_assert_outsider,
)


# =====================================================================
# C5 — owner-based standing: actor owns a commitment in scope_entities
# =====================================================================


async def _setup_owner(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant, display_name="Owner")
            commit_id = await F.make_commitment(
                conn, tenant, title="Owned commit", owner_id=owner,
            )
            # Model scopes the commitment but NOT the owner directly
            mid = await F.make_model(
                conn, tenant,
                natural="commit progress",
                scope_actors=[],
                scope_entities=[{"type": "commitment", "id": str(commit_id)}],
            )
            return {"tenant": tenant, "owner": owner, "mid": mid}


async def _run_owner(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        standing = await actor_has_standing_on_model(
            conn,
            actor_id=ctx["owner"],
            model_id=ctx["mid"],
        )
    return {"granted": standing.granted, "basis": standing.basis}


def _expected_owner(_ctx: dict) -> dict:
    return {"granted": True, "basis": "owner"}


def _assert_owner(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual != expected:
        return False, f"got {actual} expected {expected}"
    return True, ""


CASE_OWNER = Case(
    stage="contestation",
    name="standing_via_commitment_owner",
    intent="Actor who owns a commitment in scope_entities has standing with basis='owner'",
    setup=_setup_owner,
    run=_run_owner,
    expected=_expected_owner,
    assertion=_assert_owner,
)


# =====================================================================
# C6 — contributor standing
# =====================================================================


async def _setup_contributor(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            contrib = await F.make_actor(conn, tenant, display_name="Contributor")
            commit_id = await F.make_commitment(conn, tenant, owner_id=owner)
            await F.add_contributor(conn, commitment_id=commit_id, actor_id=contrib)
            mid = await F.make_model(
                conn, tenant,
                natural="progress",
                scope_entities=[{"type": "commitment", "id": str(commit_id)}],
            )
            return {"tenant": tenant, "contrib": contrib, "mid": mid}


async def _run_contributor(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        standing = await actor_has_standing_on_model(
            conn,
            actor_id=ctx["contrib"],
            model_id=ctx["mid"],
        )
    return {"granted": standing.granted, "basis": standing.basis}


def _expected_contributor(_ctx: dict) -> dict:
    return {"granted": True, "basis": "contributor"}


def _assert_contributor(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual != expected:
        return False, f"got {actual} expected {expected}"
    return True, ""


CASE_CONTRIB = Case(
    stage="contestation",
    name="standing_via_contributor",
    intent="Contributor on a scoped commitment has standing with basis='contributor'",
    setup=_setup_contributor,
    run=_run_contributor,
    expected=_expected_contributor,
    assertion=_assert_contributor,
)


# =====================================================================
# C7 — reading contestation: signal_readings entry marked, no confidence multiplier
# =====================================================================


async def _setup_reading(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            mid = await F.make_model(
                conn, tenant,
                natural="reading model",
                confidence=0.7,
                scope_actors=[actor],
            )
            return {"tenant": tenant, "actor": actor, "mid": mid, "prev_conf": 0.7}


async def _run_reading(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await contest_model(
                conn,
                ContestationInput(
                    model_id=ctx["mid"],
                    contestor_actor_id=ctx["actor"],
                    tenant_id=ctx["tenant"],
                    contestation_kind="reading",
                    rationale="my reading was not what I meant",
                ),
            )
            row = await conn.fetchrow(
                "SELECT signal_readings, confidence FROM models WHERE id=$1",
                ctx["mid"],
            )
    sr = row["signal_readings"]
    if isinstance(sr, str):
        sr = json.loads(sr)
    elif isinstance(sr, (bytes, bytearray)):
        sr = json.loads(sr.decode())
    return {
        "override_applied": result.override_applied,
        "new_confidence": result.new_confidence,
        "previous_confidence": result.previous_confidence,
        "signal_readings": sr,
        "db_confidence": float(row["confidence"]),
    }


def _expected_reading(ctx: dict) -> dict:
    return {
        "override_applied": False,
        "confidence_unchanged": ctx["prev_conf"],
        "actor_id_str": str(ctx["actor"]),
    }


def _assert_reading(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual["override_applied"]:
        return False, "override_applied should be False for reading contestation"
    if abs(actual["db_confidence"] - expected["confidence_unchanged"]) > 1e-6:
        return False, f"confidence changed: got {actual['db_confidence']}"
    sr = actual["signal_readings"]
    matching = [e for e in sr if isinstance(e, dict) and e.get("actor_id") == expected["actor_id_str"]]
    if not matching:
        return False, f"no signal_readings entry for actor; got {sr}"
    if not matching[0].get("contested"):
        return False, f"actor entry not marked contested; got {matching[0]}"
    return True, ""


CASE_READING = Case(
    stage="contestation",
    name="reading_contestation_marks_entry",
    intent="Reading contestation marks signal_readings entry without changing confidence",
    setup=_setup_reading,
    run=_run_reading,
    expected=_expected_reading,
    assertion=_assert_reading,
)


CASES = [
    CASE_PRIMARY,
    CASE_SECONDARY,
    CASE_FLOOR,
    CASE_OUTSIDER,
    CASE_OWNER,
    CASE_CONTRIB,
    CASE_READING,
]
