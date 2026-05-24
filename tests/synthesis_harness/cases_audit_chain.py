"""Audit chain stage — PR 1 (Q5).

Each case exercises a state transition through the audit-emitting
APIs (ModelsRepo.insert / .archive / .bulk_confidence_update; or
applier.apply_diff with claim_op insert/update/archive) and asserts
on the resulting `audit_events` rows via `get_audit_chain` or direct
SQL.

The harness's `make_model` fixture bypasses the repo on purpose
(direct SQL for retrieval-only setups) — these cases instead use the
repo / applier on every state transition so the audit chain is
populated naturally.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.models.repo import ModelsRepo
from services.observations.state_change import emit_state_change
from services.think.applier import apply_diff
from services.think.audit import (
    CAUSE_ARCHIVE,
    CAUSE_CONFIDENCE_UPDATE,
    CAUSE_CREATE,
    CAUSE_FIELD_UPDATE,
    CAUSE_RECONCILIATION_MERGE,
    emit_audit_event,
    emit_reconciliation_merge_audit,
    get_audit_chain,
)
from services.think.diff_schema import ClaimOp, ValidatedDiff

from . import _fixtures as F
from ._runner import Case


# =====================================================================
# Helpers
# =====================================================================


def _model_create(
    *,
    tenant_id: UUID,
    born_from_event_id: UUID,
    natural: str = "audit-chain test model",
    confidence: float = 0.6,
    scope_actors: list[UUID] | None = None,
    scope_entities: list[dict] | None = None,
    proposition: dict | None = None,
    embed_seed: str | None = None,
) -> ModelCreate:
    """Build a minimum-viable ModelCreate that passes repo.insert."""
    return ModelCreate(
        tenant_id=tenant_id,
        born_from_event_id=born_from_event_id,
        proposition=proposition or {
            "kind": "state",
            "subject": natural,
            "assertion": "is true",
        },
        natural=natural,
        embedding=F.deterministic_vector(embed_seed or natural),
        scope_actors=scope_actors or [],
        scope_entities=scope_entities or [],
        scope_temporal={
            "valid_from": F.isoplus(0).isoformat(),
            "valid_until": None,
        },
        confidence=confidence,
        confidence_at_assertion=confidence,
    )


async def _setup_tenant_actor_obs(
    pool: asyncpg.Pool, _ctx: dict
) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            obs = await F.make_observation(
                conn, tenant, actor_id=actor,
                content_text="audit-chain seed observation",
            )
            return {"tenant": tenant, "actor": actor, "obs": obs}


async def _audit_rows_for(
    conn: asyncpg.Connection, model_id: UUID
) -> list[asyncpg.Record]:
    """Direct SQL fetch of audit_events for a model_id, ordered."""
    return await conn.fetch(
        """
        SELECT event_id, cause_type, cause_id, previous_state,
               new_state, changed_fields, re_asserts_event_id,
               source_model_ids, occurred_at
        FROM audit_events
        WHERE model_id = $1
        ORDER BY occurred_at ASC, event_id ASC
        """,
        model_id,
    )


# =====================================================================
# AC1 — create emits a single 'create' audit event
# =====================================================================


async def _run_create_emits_create(
    pool: asyncpg.Pool, ctx: dict
) -> dict:
    repo = ModelsRepo(pool=pool, embedder=None)
    async with pool.acquire() as conn:
        async with conn.transaction():
            mc = _model_create(
                tenant_id=ctx["tenant"],
                born_from_event_id=ctx["obs"],
                natural="AC1 create",
                scope_actors=[ctx["actor"]],
            )
            row = await repo.insert(mc, conn=conn)
            ctx["model_id"] = row.id
            rows = await _audit_rows_for(conn, row.id)
    return {
        "row_count": len(rows),
        "cause_type": rows[0]["cause_type"] if rows else None,
        "previous_state": rows[0]["previous_state"] if rows else None,
        "cause_id": str(rows[0]["cause_id"]) if rows and rows[0]["cause_id"] else None,
        "expected_cause_id": str(ctx["obs"]),
        "changed_fields_nonempty": (
            bool(list(rows[0]["changed_fields"])) if rows else False
        ),
    }


def _assert_create_emits_create(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    if actual["row_count"] != 1:
        return False, f"expected 1 audit row, got {actual['row_count']}"
    if actual["cause_type"] != CAUSE_CREATE:
        return False, f"expected cause_type=create, got {actual['cause_type']!r}"
    if actual["previous_state"] is not None:
        return False, "create event must have NULL previous_state"
    if actual["cause_id"] != actual["expected_cause_id"]:
        return False, (
            f"cause_id mismatch: got {actual['cause_id']}, "
            f"expected {actual['expected_cause_id']}"
        )
    if not actual["changed_fields_nonempty"]:
        return False, "create event should populate changed_fields with all keys"
    return True, ""


CASE_CREATE_EMITS_CREATE = Case(
    stage="audit_chain",
    name="create_emits_single_create_event",
    intent=(
        "ModelsRepo.insert emits exactly one audit_events row with "
        "cause_type='create', NULL previous_state, cause_id=born_from_event_id"
    ),
    setup=_setup_tenant_actor_obs,
    run=_run_create_emits_create,
    expected=lambda _ctx: {},
    assertion=_assert_create_emits_create,
)


# =====================================================================
# AC2 — bulk_confidence_update emits 'confidence_update' with diff
# =====================================================================


async def _run_confidence_update_emits(
    pool: asyncpg.Pool, ctx: dict
) -> dict:
    repo = ModelsRepo(pool=pool, embedder=None)
    async with pool.acquire() as conn:
        async with conn.transaction():
            mc = _model_create(
                tenant_id=ctx["tenant"],
                born_from_event_id=ctx["obs"],
                natural="AC2 conf update",
                scope_actors=[ctx["actor"]],
                confidence=0.5,
            )
            row = await repo.insert(mc, conn=conn)
            await repo.bulk_confidence_update(
                {row.id: 0.8},
                cause_event_id=ctx["obs"],
                conn=conn,
            )
            rows = await _audit_rows_for(conn, row.id)
    return {
        "row_count": len(rows),
        "second_cause_type": rows[1]["cause_type"] if len(rows) >= 2 else None,
        "second_changed_fields": (
            list(rows[1]["changed_fields"]) if len(rows) >= 2 else []
        ),
        "second_prev": _decode_jsonb(rows[1]["previous_state"]) if len(rows) >= 2 else None,
        "second_new": _decode_jsonb(rows[1]["new_state"]) if len(rows) >= 2 else None,
    }


def _assert_confidence_update_emits(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    if actual["row_count"] != 2:
        return False, f"expected 2 audit rows, got {actual['row_count']}"
    if actual["second_cause_type"] != CAUSE_CONFIDENCE_UPDATE:
        return False, (
            f"second event cause_type=confidence_update; got "
            f"{actual['second_cause_type']!r}"
        )
    if actual["second_changed_fields"] != ["confidence"]:
        return False, (
            f"changed_fields=['confidence']; got {actual['second_changed_fields']!r}"
        )
    prev = actual["second_prev"] or {}
    new = actual["second_new"] or {}
    if prev.get("confidence") in (None, new.get("confidence")):
        return False, (
            f"previous_state.confidence ({prev.get('confidence')!r}) must "
            f"differ from new_state.confidence ({new.get('confidence')!r})"
        )
    return True, ""


CASE_CONF_UPDATE_EMITS = Case(
    stage="audit_chain",
    name="confidence_update_emits_diff_event",
    intent=(
        "bulk_confidence_update emits an audit row with "
        "cause_type='confidence_update' and previous/new confidence diffing"
    ),
    setup=_setup_tenant_actor_obs,
    run=_run_confidence_update_emits,
    expected=lambda _ctx: {},
    assertion=_assert_confidence_update_emits,
)


# =====================================================================
# AC3 — archive emits cause='archive' with status diff
# =====================================================================


async def _run_archive_emits(pool: asyncpg.Pool, ctx: dict) -> dict:
    repo = ModelsRepo(pool=pool, embedder=None)
    async with pool.acquire() as conn:
        async with conn.transaction():
            mc = _model_create(
                tenant_id=ctx["tenant"],
                born_from_event_id=ctx["obs"],
                natural="AC3 archive",
                scope_actors=[ctx["actor"]],
            )
            row = await repo.insert(mc, conn=conn)
            await repo.archive(
                row.id,
                "deprecated",
                cause_event_id=ctx["obs"],
                conn=conn,
            )
            rows = await _audit_rows_for(conn, row.id)
    return {
        "row_count": len(rows),
        "second_cause_type": rows[1]["cause_type"] if len(rows) >= 2 else None,
        "second_prev": _decode_jsonb(rows[1]["previous_state"]) if len(rows) >= 2 else None,
        "second_new": _decode_jsonb(rows[1]["new_state"]) if len(rows) >= 2 else None,
    }


def _assert_archive_emits(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    if actual["row_count"] != 2:
        return False, f"expected 2 audit rows; got {actual['row_count']}"
    if actual["second_cause_type"] != CAUSE_ARCHIVE:
        return False, (
            f"second cause_type=archive; got {actual['second_cause_type']!r}"
        )
    prev = actual["second_prev"] or {}
    new = actual["second_new"] or {}
    if prev.get("status") != "active" or new.get("status") != "archived":
        return False, (
            f"status diff active→archived expected; got "
            f"prev={prev.get('status')!r} new={new.get('status')!r}"
        )
    return True, ""


CASE_ARCHIVE_EMITS = Case(
    stage="audit_chain",
    name="archive_emits_status_diff",
    intent=(
        "ModelsRepo.archive emits an audit event with cause_type='archive' "
        "and previous_state.status='active' → new_state.status='archived'"
    ),
    setup=_setup_tenant_actor_obs,
    run=_run_archive_emits,
    expected=lambda _ctx: {},
    assertion=_assert_archive_emits,
)


# =====================================================================
# AC4 — reversal-of-reversal: confidence A→B→A sets re_asserts_event_id
# =====================================================================


async def _run_reversal_confidence(
    pool: asyncpg.Pool, ctx: dict
) -> dict:
    """A → B → A oscillation across three confidence updates after the
    initial create. We avoid using the create's confidence value as
    the "A" because repo.insert applies calibration, which can shift
    the stored value away from what the caller passed. Four total
    audit rows: 1 create + 3 confidence_updates (A=0.6, B=0.8, A=0.6).
    """
    repo = ModelsRepo(pool=pool, embedder=None)
    async with pool.acquire() as conn:
        async with conn.transaction():
            mc = _model_create(
                tenant_id=ctx["tenant"],
                born_from_event_id=ctx["obs"],
                natural="AC4 reversal",
                scope_actors=[ctx["actor"]],
            )
            row = await repo.insert(mc, conn=conn)
            # First A
            await repo.bulk_confidence_update(
                {row.id: 0.6}, cause_event_id=ctx["obs"], conn=conn,
            )
            # B
            await repo.bulk_confidence_update(
                {row.id: 0.8}, cause_event_id=ctx["obs"], conn=conn,
            )
            # Second A (reversal)
            await repo.bulk_confidence_update(
                {row.id: 0.6}, cause_event_id=ctx["obs"], conn=conn,
            )
            rows = await _audit_rows_for(conn, row.id)
    return {
        "row_count": len(rows),
        "first_a_event_id": rows[1]["event_id"] if len(rows) >= 2 else None,
        "b_re_asserts": rows[2]["re_asserts_event_id"] if len(rows) >= 3 else None,
        "second_a_re_asserts": rows[3]["re_asserts_event_id"] if len(rows) >= 4 else None,
    }


def _assert_reversal_confidence(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    if actual["row_count"] != 4:
        return False, f"expected 4 audit rows (create + 3 updates); got {actual['row_count']}"
    # The B event must not re-assert anything (0.8 is novel).
    if actual["b_re_asserts"] is not None:
        return False, (
            f"B event (confidence 0.8) must not have re_asserts_event_id; "
            f"got {actual['b_re_asserts']!r}"
        )
    # The second A event must point back to the first A event.
    if actual["second_a_re_asserts"] != actual["first_a_event_id"]:
        return False, (
            f"second-A event re_asserts_event_id must point to first-A "
            f"event ({actual['first_a_event_id']}); got "
            f"{actual['second_a_re_asserts']}"
        )
    return True, ""


CASE_REVERSAL_CONFIDENCE = Case(
    stage="audit_chain",
    name="reversal_confidence_links_to_original",
    intent=(
        "A → B → A on confidence produces three distinct events; the "
        "third event's re_asserts_event_id points to the first"
    ),
    setup=_setup_tenant_actor_obs,
    run=_run_reversal_confidence,
    expected=lambda _ctx: {},
    assertion=_assert_reversal_confidence,
)


# =====================================================================
# AC5 — reversal preserves three events, never collapses
# =====================================================================


async def _run_reversal_preserves_three(
    pool: asyncpg.Pool, ctx: dict
) -> dict:
    return await _run_reversal_confidence(pool, ctx)


def _assert_reversal_preserves_three(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    # Strict count check: the substrate must NOT collapse A→B→A into
    # "no net change". Four rows total (create + three updates that
    # form the A→B→A pattern).
    if actual["row_count"] != 4:
        return False, (
            f"chain must preserve every event; got {actual['row_count']}. "
            f"Collapsing reversal-of-reversal hides oscillation patterns."
        )
    return True, ""


CASE_REVERSAL_THREE_EVENTS = Case(
    stage="audit_chain",
    name="reversal_preserves_three_distinct_events",
    intent=(
        "A → B → A is recorded as three distinct events, never collapsed "
        "into a single 'no net change' row"
    ),
    setup=_setup_tenant_actor_obs,
    run=_run_reversal_preserves_three,
    expected=lambda _ctx: {},
    assertion=_assert_reversal_preserves_three,
)


# =====================================================================
# AC6 — non-reversal updates do NOT set re_asserts_event_id
# =====================================================================


async def _run_no_false_reassert(
    pool: asyncpg.Pool, ctx: dict
) -> dict:
    repo = ModelsRepo(pool=pool, embedder=None)
    async with pool.acquire() as conn:
        async with conn.transaction():
            mc = _model_create(
                tenant_id=ctx["tenant"],
                born_from_event_id=ctx["obs"],
                natural="AC6 no false reassert",
                scope_actors=[ctx["actor"]],
                confidence=0.5,
            )
            row = await repo.insert(mc, conn=conn)
            # Distinct progression: 0.5 → 0.6 → 0.7 (never returns to 0.5)
            await repo.bulk_confidence_update(
                {row.id: 0.6}, cause_event_id=ctx["obs"], conn=conn,
            )
            await repo.bulk_confidence_update(
                {row.id: 0.7}, cause_event_id=ctx["obs"], conn=conn,
            )
            rows = await _audit_rows_for(conn, row.id)
    return {
        "row_count": len(rows),
        "any_re_asserts_set": any(
            r["re_asserts_event_id"] is not None
            for r in rows[1:]  # ignore the create row's NULL
        ),
    }


def _assert_no_false_reassert(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    if actual["row_count"] != 3:
        return False, f"expected 3 audit rows; got {actual['row_count']}"
    if actual["any_re_asserts_set"]:
        return False, (
            "no event in a strictly-progressing 0.5→0.6→0.7 chain should "
            "have re_asserts_event_id set"
        )
    return True, ""


CASE_NO_FALSE_REASSERT = Case(
    stage="audit_chain",
    name="strict_progression_no_re_asserts",
    intent=(
        "0.5 → 0.6 → 0.7 confidence progression: no event has "
        "re_asserts_event_id set (no value is ever re-asserted)"
    ),
    setup=_setup_tenant_actor_obs,
    run=_run_no_false_reassert,
    expected=lambda _ctx: {},
    assertion=_assert_no_false_reassert,
)


# =====================================================================
# AC7 — reconciliation_merge audit (single-pass auto_merge path)
# =====================================================================
# Two identical insert payloads under the same scope: the second is
# auto_merged into the first by the reconciler (T5). The audit chain on
# the surviving Model must record the merge with cause='reconciliation_merge'.


async def _run_recon_merge_audit(
    pool: asyncpg.Pool, ctx: dict
) -> dict:
    async def _apply_one(conn: asyncpg.Connection, trigger_id: UUID) -> UUID:
        op = ClaimOp(op="insert", entry={
            "tenant_id": str(ctx["tenant"]),
            "born_from_event_id": str(ctx["obs"]),
            "proposition": {
                "kind": "state", "subject": "ac7-merge", "assertion": "stable",
            },
            "natural": "AC7 merge candidate",
            "embedding": F.deterministic_vector("ac7-merge-stable"),
            "scope_actors": [str(ctx["actor"])],
            "scope_entities": [],
            "scope_temporal": {
                "valid_from": F.isoplus(0).isoformat(),
                "valid_until": None,
            },
            "confidence": 0.6,
            "confidence_at_assertion": 0.6,
        })
        diff = ValidatedDiff(
            trigger_ref=trigger_id,
            tenant_id=ctx["tenant"],
            claim_ops=[op],
            act_ops=[],
            resource_ops=[],
            new_predictions=[],
            reasoning_trace="audit-chain.merge",
        )
        result = await apply_diff(
            diff, conn, trigger_kind="T1",
            trigger_cause_event_id=ctx["obs"],
        )
        return result["applied_model_ids"][0] if result["applied_model_ids"] else None

    async with pool.acquire() as conn:
        async with conn.transaction():
            first_mid = await _apply_one(conn, uuid7())
        async with conn.transaction():
            second_mid = await _apply_one(conn, uuid7())
        async with conn.transaction():
            rows = await _audit_rows_for(conn, first_mid)
            recon_decisions = await conn.fetch(
                "SELECT decision FROM reconciliation_events "
                "WHERE tenant_id = $1 ORDER BY occurred_at ASC",
                ctx["tenant"],
            )
    return {
        "first_model_id": str(first_mid),
        "second_model_id": str(second_mid) if second_mid else None,
        "audit_count": len(rows),
        "audit_cause_types": [r["cause_type"] for r in rows],
        "recon_decisions": [r["decision"] for r in recon_decisions],
    }


def _assert_recon_merge_audit(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    # Reconciler should have decided auto_merge on the second insert.
    if actual["recon_decisions"] != ["no_match", "auto_merge"]:
        return False, (
            f"expected recon decisions [no_match, auto_merge]; got "
            f"{actual['recon_decisions']!r}"
        )
    # The surviving Model is `first_mid`; second_mid should be the same id
    # because auto_merge converts the insert into an update on the matched.
    if actual["first_model_id"] != actual["second_model_id"]:
        return False, (
            f"auto_merge should land on the existing model_id; got "
            f"first={actual['first_model_id']!r} second={actual['second_model_id']!r}"
        )
    # Audit chain on the surviving Model: at minimum a 'create' (from the
    # first insert) plus a 'reconciliation_merge' (from the second insert
    # being merged in).
    if CAUSE_CREATE not in actual["audit_cause_types"]:
        return False, (
            f"expected a 'create' audit event; got {actual['audit_cause_types']!r}"
        )
    if CAUSE_RECONCILIATION_MERGE not in actual["audit_cause_types"]:
        return False, (
            f"expected a 'reconciliation_merge' audit event; got "
            f"{actual['audit_cause_types']!r}"
        )
    return True, ""


CASE_RECON_MERGE_AUDIT = Case(
    stage="audit_chain",
    name="auto_merge_emits_reconciliation_merge_audit",
    intent=(
        "Two identical inserts under same scope: first creates, second "
        "auto_merges. Audit chain on surviving Model contains both "
        "'create' and 'reconciliation_merge' events."
    ),
    setup=_setup_tenant_actor_obs,
    run=_run_recon_merge_audit,
    expected=lambda _ctx: {},
    assertion=_assert_recon_merge_audit,
)


# =====================================================================
# AC8 — get_audit_chain unions source-Model chains
# =====================================================================
# Two-Model merge case: A and B exist with their own audit chains.
# Direct call to emit_reconciliation_merge_audit on B with
# source_model_ids=[A]. get_audit_chain(B) returns events from both
# A and B, ordered by occurred_at.


async def _run_chain_union(pool: asyncpg.Pool, ctx: dict) -> dict:
    repo = ModelsRepo(pool=pool, embedder=None)
    async with pool.acquire() as conn:
        async with conn.transaction():
            # A: insert + one confidence update.
            mc_a = _model_create(
                tenant_id=ctx["tenant"],
                born_from_event_id=ctx["obs"],
                natural="AC8 source A",
                scope_actors=[ctx["actor"]],
                embed_seed="ac8-a",
                confidence=0.5,
            )
            row_a = await repo.insert(mc_a, conn=conn)
            await repo.bulk_confidence_update(
                {row_a.id: 0.7}, cause_event_id=ctx["obs"], conn=conn,
            )

            # B: insert (different embedding so reconciler doesn't auto_merge).
            mc_b = _model_create(
                tenant_id=ctx["tenant"],
                born_from_event_id=ctx["obs"],
                natural="AC8 surviving B",
                scope_actors=[ctx["actor"]],
                embed_seed="ac8-b-different",
                confidence=0.6,
            )
            row_b = await repo.insert(mc_b, conn=conn)

            # Manually emit a reconciliation_merge on B with A as source.
            # Simulates PR 4's two-Model-then-merge case.
            await emit_reconciliation_merge_audit(
                conn,
                merged_model_id=row_b.id,
                source_model_ids=[row_a.id],
                tenant_id=ctx["tenant"],
                new_state={"merged_from": str(row_a.id)},
                cause_id=ctx["obs"],
                changed_fields=["merged_from"],
            )

            chain = await get_audit_chain(conn, row_b.id)
            chain_a_only = await get_audit_chain(
                conn, row_b.id, include_merged_sources=False,
            )

    chain_model_ids = sorted({str(e.model_id) for e in chain})
    return {
        "model_a": str(row_a.id),
        "model_b": str(row_b.id),
        "chain_model_ids": chain_model_ids,
        "chain_len": len(chain),
        "chain_b_only_len": len(chain_a_only),
        "chain_cause_types": [e.cause_type for e in chain],
        "merge_event_present": any(
            e.cause_type == CAUSE_RECONCILIATION_MERGE for e in chain
        ),
        "ordered_by_time": all(
            chain[i].occurred_at <= chain[i + 1].occurred_at
            for i in range(len(chain) - 1)
        ),
    }


def _assert_chain_union(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    expected_ids = sorted([actual["model_a"], actual["model_b"]])
    if actual["chain_model_ids"] != expected_ids:
        return False, (
            f"unioned chain should include both A and B model_ids; got "
            f"{actual['chain_model_ids']!r}, expected {expected_ids!r}"
        )
    # B alone has 1 event (its create). A has 2 (create + conf update).
    # Union: 4 with the merge event.
    if actual["chain_len"] != 4:
        return False, (
            f"expected 4 unioned events (1 B-create + 2 A-events + 1 merge); "
            f"got {actual['chain_len']}"
        )
    if actual["chain_b_only_len"] != 2:
        return False, (
            f"expected 2 B-only events (B-create + merge); got "
            f"{actual['chain_b_only_len']}"
        )
    if not actual["merge_event_present"]:
        return False, "merge event missing from unioned chain"
    if not actual["ordered_by_time"]:
        return False, f"chain not ordered by occurred_at: {actual['chain_cause_types']!r}"
    return True, ""


CASE_CHAIN_UNION = Case(
    stage="audit_chain",
    name="get_audit_chain_unions_source_models",
    intent=(
        "get_audit_chain on a Model with a reconciliation_merge event "
        "returns the union of source-Model chains, ordered by occurred_at"
    ),
    setup=_setup_tenant_actor_obs,
    run=_run_chain_union,
    expected=lambda _ctx: {},
    assertion=_assert_chain_union,
)


# =====================================================================
# AC9 — chain ordering across many events
# =====================================================================


async def _run_chain_ordering(
    pool: asyncpg.Pool, ctx: dict
) -> dict:
    repo = ModelsRepo(pool=pool, embedder=None)
    async with pool.acquire() as conn:
        async with conn.transaction():
            mc = _model_create(
                tenant_id=ctx["tenant"],
                born_from_event_id=ctx["obs"],
                natural="AC9 ordering",
                scope_actors=[ctx["actor"]],
                confidence=0.4,
            )
            row = await repo.insert(mc, conn=conn)
            for new_conf in (0.5, 0.6, 0.7, 0.8, 0.9):
                await repo.bulk_confidence_update(
                    {row.id: new_conf},
                    cause_event_id=ctx["obs"],
                    conn=conn,
                )
            chain = await get_audit_chain(conn, row.id)

    confidences = []
    for e in chain:
        if e.cause_type == CAUSE_CONFIDENCE_UPDATE:
            confidences.append(e.new_state.get("confidence"))
    return {
        "chain_len": len(chain),
        "confidences_in_order": confidences,
        "first_cause": chain[0].cause_type if chain else None,
        "ordered": all(
            chain[i].occurred_at <= chain[i + 1].occurred_at
            for i in range(len(chain) - 1)
        ),
    }


def _assert_chain_ordering(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    if actual["chain_len"] != 6:  # 1 create + 5 updates
        return False, f"expected 6 events; got {actual['chain_len']}"
    if actual["first_cause"] != CAUSE_CREATE:
        return False, f"first event cause must be 'create'; got {actual['first_cause']!r}"
    if actual["confidences_in_order"] != [0.5, 0.6, 0.7, 0.8, 0.9]:
        return False, (
            f"confidence updates not in monotone order; got "
            f"{actual['confidences_in_order']!r}"
        )
    if not actual["ordered"]:
        return False, "events not ordered by occurred_at"
    return True, ""


CASE_CHAIN_ORDERING = Case(
    stage="audit_chain",
    name="long_chain_returned_in_temporal_order",
    intent=(
        "A 6-event chain (1 create + 5 confidence updates 0.5→0.9) is "
        "returned by get_audit_chain in occurred_at order"
    ),
    setup=_setup_tenant_actor_obs,
    run=_run_chain_ordering,
    expected=lambda _ctx: {},
    assertion=_assert_chain_ordering,
)


# =====================================================================
# AC10 — every audit event has a populated cause_id when one was provided
# =====================================================================


async def _run_cause_linkage(
    pool: asyncpg.Pool, ctx: dict
) -> dict:
    repo = ModelsRepo(pool=pool, embedder=None)
    async with pool.acquire() as conn:
        async with conn.transaction():
            obs2 = await F.make_observation(
                conn, ctx["tenant"], actor_id=ctx["actor"],
                content_text="second cause obs",
            )
            mc = _model_create(
                tenant_id=ctx["tenant"],
                born_from_event_id=ctx["obs"],
                natural="AC10 cause linkage",
                scope_actors=[ctx["actor"]],
                confidence=0.5,
            )
            row = await repo.insert(mc, conn=conn)
            await repo.bulk_confidence_update(
                {row.id: 0.7},
                cause_event_id=obs2,
                conn=conn,
            )
            rows = await _audit_rows_for(conn, row.id)
    return {
        "create_cause_id": str(rows[0]["cause_id"]) if rows[0]["cause_id"] else None,
        "expected_create_cause": str(ctx["obs"]),
        "update_cause_id": str(rows[1]["cause_id"]) if len(rows) >= 2 and rows[1]["cause_id"] else None,
        "expected_update_cause": str(obs2),
    }


def _assert_cause_linkage(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    if actual["create_cause_id"] != actual["expected_create_cause"]:
        return False, (
            f"create event cause_id mismatch: got {actual['create_cause_id']!r} "
            f"expected {actual['expected_create_cause']!r}"
        )
    if actual["update_cause_id"] != actual["expected_update_cause"]:
        return False, (
            f"update event cause_id mismatch: got {actual['update_cause_id']!r} "
            f"expected {actual['expected_update_cause']!r}"
        )
    return True, ""


CASE_CAUSE_LINKAGE = Case(
    stage="audit_chain",
    name="cause_id_threaded_through_distinct_events",
    intent=(
        "Distinct cause_event_id values on insert vs bulk_confidence_update "
        "land verbatim in the matching audit_events.cause_id columns"
    ),
    setup=_setup_tenant_actor_obs,
    run=_run_cause_linkage,
    expected=lambda _ctx: {},
    assertion=_assert_cause_linkage,
)


# =====================================================================
# AC11 — unknown cause_type rejected loudly
# =====================================================================


async def _run_unknown_cause_rejected(
    pool: asyncpg.Pool, ctx: dict
) -> dict:
    repo = ModelsRepo(pool=pool, embedder=None)
    async with pool.acquire() as conn:
        async with conn.transaction():
            mc = _model_create(
                tenant_id=ctx["tenant"],
                born_from_event_id=ctx["obs"],
                natural="AC11 invalid cause",
                scope_actors=[ctx["actor"]],
            )
            row = await repo.insert(mc, conn=conn)
            err: str | None = None
            try:
                await emit_audit_event(
                    conn,
                    model_id=row.id,
                    tenant_id=ctx["tenant"],
                    cause_type="not_a_real_cause",
                    new_state={"x": 1},
                )
            except ValueError as exc:
                err = str(exc)
    return {"raised": err is not None, "msg_mentions_unknown": "unknown" in (err or "").lower()}


def _assert_unknown_cause_rejected(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    if not actual["raised"]:
        return False, "emit_audit_event with bogus cause_type must raise ValueError"
    if not actual["msg_mentions_unknown"]:
        return False, "raised ValueError but message didn't flag the unknown cause_type"
    return True, ""


CASE_UNKNOWN_CAUSE_REJECTED = Case(
    stage="audit_chain",
    name="unknown_cause_type_rejected",
    intent=(
        "emit_audit_event with cause_type not in the vocabulary raises "
        "ValueError before touching the DB — drift is loud"
    ),
    setup=_setup_tenant_actor_obs,
    run=_run_unknown_cause_rejected,
    expected=lambda _ctx: {},
    assertion=_assert_unknown_cause_rejected,
)


# =====================================================================
# AC12 — multi-tenant isolation: tenant A's audit doesn't leak to B
# =====================================================================


async def _setup_two_tenants(
    pool: asyncpg.Pool, _ctx: dict
) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tA = await F.make_tenant(conn)
            tB = await F.make_tenant(conn)
            actA = await F.make_actor(conn, tA, display_name="A")
            actB = await F.make_actor(conn, tB, display_name="B")
            obsA = await F.make_observation(conn, tA, actor_id=actA)
            obsB = await F.make_observation(conn, tB, actor_id=actB)
            return {
                "tenant_a": tA, "tenant_b": tB,
                "actor_a": actA, "actor_b": actB,
                "obs_a": obsA, "obs_b": obsB,
            }


async def _run_multitenant_isolation(
    pool: asyncpg.Pool, ctx: dict
) -> dict:
    repo = ModelsRepo(pool=pool, embedder=None)
    async with pool.acquire() as conn:
        async with conn.transaction():
            mc_a = _model_create(
                tenant_id=ctx["tenant_a"],
                born_from_event_id=ctx["obs_a"],
                natural="MT A",
                scope_actors=[ctx["actor_a"]],
                embed_seed="mt-a",
            )
            row_a = await repo.insert(mc_a, conn=conn)
            mc_b = _model_create(
                tenant_id=ctx["tenant_b"],
                born_from_event_id=ctx["obs_b"],
                natural="MT B",
                scope_actors=[ctx["actor_b"]],
                embed_seed="mt-b",
            )
            row_b = await repo.insert(mc_b, conn=conn)

            # Tenant A audit query.
            count_a_in_a = await conn.fetchval(
                "SELECT count(*) FROM audit_events "
                "WHERE tenant_id = $1 AND model_id = $2",
                ctx["tenant_a"], row_a.id,
            )
            count_b_in_a = await conn.fetchval(
                "SELECT count(*) FROM audit_events "
                "WHERE tenant_id = $1 AND model_id = $2",
                ctx["tenant_a"], row_b.id,
            )
            count_a_total_in_b = await conn.fetchval(
                "SELECT count(*) FROM audit_events "
                "WHERE tenant_id = $1",
                ctx["tenant_b"],
            )
    return {
        "count_a_in_a": count_a_in_a,
        "count_b_in_a": count_b_in_a,
        "count_a_total_in_b": count_a_total_in_b,
    }


def _assert_multitenant_isolation(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    if actual["count_a_in_a"] != 1:
        return False, f"tenant A's model should have 1 audit event in tenant A; got {actual['count_a_in_a']}"
    if actual["count_b_in_a"] != 0:
        return False, (
            f"tenant B's model leaked into tenant A audit count: "
            f"{actual['count_b_in_a']}"
        )
    if actual["count_a_total_in_b"] != 1:
        return False, f"tenant B should have exactly 1 audit row; got {actual['count_a_total_in_b']}"
    return True, ""


CASE_MULTITENANT_ISOLATION = Case(
    stage="audit_chain",
    name="multitenant_isolation_no_cross_leakage",
    intent=(
        "Audit events for tenant A's Model do not surface in queries "
        "scoped to tenant B and vice versa"
    ),
    setup=_setup_two_tenants,
    run=_run_multitenant_isolation,
    expected=lambda _ctx: {},
    assertion=_assert_multitenant_isolation,
)


# =====================================================================
# AC13 — tenant_id stored on every audit row matches the model
# =====================================================================


async def _run_tenant_id_stored(
    pool: asyncpg.Pool, ctx: dict
) -> dict:
    repo = ModelsRepo(pool=pool, embedder=None)
    async with pool.acquire() as conn:
        async with conn.transaction():
            mc = _model_create(
                tenant_id=ctx["tenant"],
                born_from_event_id=ctx["obs"],
                natural="AC13 tenant id check",
                scope_actors=[ctx["actor"]],
            )
            row = await repo.insert(mc, conn=conn)
            await repo.bulk_confidence_update(
                {row.id: 0.7}, cause_event_id=ctx["obs"], conn=conn,
            )
            await repo.archive(row.id, "deprecated", cause_event_id=ctx["obs"], conn=conn)
            tenants = await conn.fetch(
                "SELECT tenant_id FROM audit_events WHERE model_id = $1",
                row.id,
            )
    return {
        "tenant_ids": [str(r["tenant_id"]) for r in tenants],
        "expected": str(ctx["tenant"]),
        "row_count": len(tenants),
    }


def _assert_tenant_id_stored(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    if actual["row_count"] != 3:
        return False, f"expected 3 audit rows; got {actual['row_count']}"
    bad = [t for t in actual["tenant_ids"] if t != actual["expected"]]
    if bad:
        return False, f"some audit rows have wrong tenant_id: {bad!r}"
    return True, ""


CASE_TENANT_ID_STORED = Case(
    stage="audit_chain",
    name="tenant_id_stored_on_every_audit_row",
    intent=(
        "Every audit row produced by the create/update/archive paths "
        "has tenant_id matching the Model's tenant"
    ),
    setup=_setup_tenant_actor_obs,
    run=_run_tenant_id_stored,
    expected=lambda _ctx: {},
    assertion=_assert_tenant_id_stored,
)


# =====================================================================
# AC14 — get_audit_chain returns empty for nonexistent model
# =====================================================================


async def _run_chain_empty_for_unknown(
    pool: asyncpg.Pool, _ctx: dict
) -> dict:
    fake_id = uuid7()
    async with pool.acquire() as conn:
        chain = await get_audit_chain(conn, fake_id)
    return {"chain_len": len(chain)}


CASE_CHAIN_EMPTY = Case(
    stage="audit_chain",
    name="get_audit_chain_empty_for_unknown_model",
    intent=(
        "get_audit_chain returns an empty list for a model_id that has "
        "no audit_events rows; never errors"
    ),
    setup=lambda pool, _ctx: _async_noop({}),
    run=_run_chain_empty_for_unknown,
    expected=lambda _ctx: {"chain_len": 0},
    assertion=lambda a, e, c: (
        (a["chain_len"] == 0, "" if a["chain_len"] == 0 else f"got {a!r}")
    ),
)


async def _async_noop(d: dict) -> dict:
    return d


# =====================================================================
# AC15 — field_update on non-confidence column emits cause=field_update
# =====================================================================


async def _run_field_update_cause(
    pool: asyncpg.Pool, ctx: dict
) -> dict:
    repo = ModelsRepo(pool=pool, embedder=None)
    async with pool.acquire() as conn:
        async with conn.transaction():
            mc = _model_create(
                tenant_id=ctx["tenant"],
                born_from_event_id=ctx["obs"],
                natural="AC15 field update",
                scope_actors=[ctx["actor"]],
                confidence=0.5,
            )
            row = await repo.insert(mc, conn=conn)

            # Use apply_diff with a non-confidence update (bumps
            # confirmed_count) so the applier's field_update path runs.
            op = ClaimOp(
                op="update",
                model_id=row.id,
                changes={"confirmed_count": 3},
            )
            diff = ValidatedDiff(
                trigger_ref=uuid7(),
                tenant_id=ctx["tenant"],
                claim_ops=[op],
                act_ops=[],
                resource_ops=[],
                new_predictions=[],
                reasoning_trace="audit-chain.field-update",
            )
            await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            rows = await _audit_rows_for(conn, row.id)
    return {
        "row_count": len(rows),
        "second_cause": rows[1]["cause_type"] if len(rows) >= 2 else None,
        "second_changed_fields": (
            list(rows[1]["changed_fields"]) if len(rows) >= 2 else []
        ),
    }


def _assert_field_update_cause(
    actual: dict, _e: dict, _c: dict
) -> tuple[bool, str]:
    if actual["row_count"] != 2:
        return False, f"expected 2 audit rows; got {actual['row_count']}"
    if actual["second_cause"] != CAUSE_FIELD_UPDATE:
        return False, (
            f"second cause should be 'field_update'; got "
            f"{actual['second_cause']!r}"
        )
    if "confirmed_count" not in actual["second_changed_fields"]:
        return False, (
            f"changed_fields should include 'confirmed_count'; got "
            f"{actual['second_changed_fields']!r}"
        )
    return True, ""


CASE_FIELD_UPDATE_CAUSE = Case(
    stage="audit_chain",
    name="non_confidence_update_emits_field_update",
    intent=(
        "claim_op update on a non-confidence column (confirmed_count) "
        "produces an audit event with cause_type='field_update' and "
        "the column listed in changed_fields"
    ),
    setup=_setup_tenant_actor_obs,
    run=_run_field_update_cause,
    expected=lambda _ctx: {},
    assertion=_assert_field_update_cause,
)


# =====================================================================
# Helpers used above
# =====================================================================


def _decode_jsonb(v: Any) -> dict | None:
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        v = v.decode()
    if isinstance(v, str):
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            return None
    if isinstance(v, dict):
        return v
    return None


CASES = [
    CASE_CREATE_EMITS_CREATE,
    CASE_CONF_UPDATE_EMITS,
    CASE_ARCHIVE_EMITS,
    CASE_REVERSAL_CONFIDENCE,
    CASE_REVERSAL_THREE_EVENTS,
    CASE_NO_FALSE_REASSERT,
    CASE_RECON_MERGE_AUDIT,
    CASE_CHAIN_UNION,
    CASE_CHAIN_ORDERING,
    CASE_CAUSE_LINKAGE,
    CASE_UNKNOWN_CAUSE_REJECTED,
    CASE_MULTITENANT_ISOLATION,
    CASE_TENANT_ID_STORED,
    CASE_CHAIN_EMPTY,
    CASE_FIELD_UPDATE_CAUSE,
]
