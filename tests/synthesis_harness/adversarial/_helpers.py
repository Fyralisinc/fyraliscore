"""Shared helpers for adversarial scenarios.

This module concentrates the few patterns that come up across
multiple categories so each case file stays readable:

  * `llm_available()` — gate LLM-driven scenarios on env so the
    harness still runs locally without DEEPSEEK_API_KEY.
  * `make_state_insert_op` / `make_concern_insert_op` — small
    builders mirroring `cases_reconciliation._state_insert_op` so
    we don't leak the ClaimOp shape across every file.
  * `assert_no_crash` / `assert_did_specified_thing` — soft and
    sharp comparators. Underspecified cases assert only that the
    pipeline did not raise; specified cases assert exact behavior.
  * `run_think_with_text` — invoke the production Think pipeline
    end-to-end with synthetic content_text, returning the resulting
    Models. Used by linguistic + boundary scenarios.

We keep these out of `_fixtures.py` so the existing harness's
public surface area is unchanged.
"""
from __future__ import annotations

import math
import os
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.retrieval.primary import TriggerContext
from services.think.diff_schema import ClaimOp
from services.think.reason import think
from lib.llm.provider import LLMConfig, build_provider

from .. import _fixtures as F


# ---------------------------------------------------------------------
# LLM gating — adversarial linguistic / sequencing scenarios that drive
# the real Think pipeline get gated on this so HARNESS_SKIP_LLM=1 keeps
# the suite green for cheap local iteration.
# ---------------------------------------------------------------------


def llm_available() -> bool:
    if os.environ.get("HARNESS_SKIP_LLM") in ("1", "true", "yes"):
        return False
    if os.environ.get("LLM_PROVIDER", "").lower() != "deepseek":
        return False
    return bool(
        os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("LLM_API_KEY")
    )


# ---------------------------------------------------------------------
# ClaimOp builders — same shape `cases_reconciliation` uses, repeated
# so we don't depend on `_state_insert_op` (private to that module).
# ---------------------------------------------------------------------


def make_state_insert_op(
    *,
    tenant_id: UUID,
    born_from_event_id: UUID,
    natural: str,
    confidence: float = 0.6,
    embed_seed: str | None = None,
    scope_actors: list[UUID] | None = None,
    scope_entities: list[dict] | None = None,
    falsifier: dict | None = None,
) -> ClaimOp:
    entry: dict[str, Any] = {
        "tenant_id": str(tenant_id),
        "born_from_event_id": str(born_from_event_id),
        "proposition": {
            "kind": "state",
            "subject": natural,
            "assertion": natural,
        },
        "natural": natural,
        "embedding": F.deterministic_vector(embed_seed or natural),
        "scope_actors": [str(a) for a in (scope_actors or [])],
        "scope_entities": scope_entities or [],
        "scope_temporal": {
            "valid_from": F.isoplus(0).isoformat(),
            "valid_until": None,
        },
        "confidence": confidence,
        "confidence_at_assertion": confidence,
    }
    if falsifier is not None:
        entry["falsifier"] = falsifier
    return ClaimOp(op="insert", entry=entry)


def make_concern_insert_op(
    *,
    tenant_id: UUID,
    born_from_event_id: UUID,
    natural: str,
    confidence: float = 0.6,
    embed_seed: str | None = None,
    scope_actors: list[UUID] | None = None,
    scope_entities: list[dict] | None = None,
) -> ClaimOp:
    return ClaimOp(
        op="insert",
        entry={
            "tenant_id": str(tenant_id),
            "born_from_event_id": str(born_from_event_id),
            "proposition": {
                "kind": "concern",
                "about": natural,
                "nature": natural,
                "raised_by": "harness",
            },
            "natural": natural,
            "embedding": F.deterministic_vector(embed_seed or natural),
            "scope_actors": [str(a) for a in (scope_actors or [])],
            "scope_entities": scope_entities or [],
            "scope_temporal": {
                "valid_from": F.isoplus(0).isoformat(),
                "valid_until": None,
            },
            "confidence": confidence,
            "confidence_at_assertion": confidence,
        },
    )


# ---------------------------------------------------------------------
# Vector blending — for cases that need a deterministic cosine in a
# specific band (e.g. human-review territory, near-orthogonal pair).
# Math: two near-orthogonal unit vectors blended w·a + (1-w)·b after
# normalize give cosine ≈ w / sqrt(w² + (1-w)²) against `a`.
# ---------------------------------------------------------------------


def blend_vectors(a: list[float], b: list[float], w: float) -> list[float]:
    blend = [w * x + (1.0 - w) * y for x, y in zip(a, b)]
    n = math.sqrt(sum(x * x for x in blend)) or 1.0
    return [x / n for x in blend]


# ---------------------------------------------------------------------
# Comparators
# ---------------------------------------------------------------------


def assert_no_crash(actual: dict, _expected: dict, _ctx: dict) -> tuple[bool, str]:
    """Soft assertion for underspecified cases: pipeline did not crash.

    Use when the scenario reveals an architectural question — the
    "correct" behavior is unknown, but the substrate must at minimum
    not raise an unhandled exception. The result still flows into
    TRIAGE.md so the design question is visible.
    """
    if actual.get("crashed") is True:
        return False, f"pipeline crashed: {actual.get('error')!r}"
    return True, ""


def safe_pipeline(coro_fn):
    """Wrap a `run` coroutine so any exception lands in the actual dict
    as `{"crashed": True, "error": "..."}` instead of bubbling to the
    runner. Lets us distinguish "no-crash assertion satisfied" from
    "exception escaped" cleanly in underspecified cases.
    """
    async def _wrapped(pool: asyncpg.Pool, ctx: dict) -> dict:
        try:
            return await coro_fn(pool, ctx)
        except Exception as exc:  # noqa: BLE001
            return {
                "crashed": True,
                "error": f"{type(exc).__name__}: {exc}",
                "error_type": type(exc).__name__,
            }
    return _wrapped


# ---------------------------------------------------------------------
# Production-pipeline driver — adversarial linguistic / boundary
# scenarios run text through the real Think pipeline and inspect what
# Models came out the other side.
# ---------------------------------------------------------------------


async def run_think_with_text(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    content_text: str,
    seed_text: str | None = None,
    trust_tier: str = "authoritative",
) -> dict:
    """Drive a single observation through the Think pipeline using the
    real LLM provider. Returns the resulting Models + the outcome.

    Caller is responsible for checking `llm_available()` first; if
    not, return early with `{"skipped": True}`.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            obs = await F.make_observation(
                conn, tenant_id,
                content_text=content_text,
                actor_id=actor_id,
                trust_tier=trust_tier,
            )

    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs,
        scope_actors=[actor_id],
        seed_entity_ids=[],
        seed_natural_text=seed_text or content_text[:200],
        seed_occurred_at=F.isoplus(0),
        precomputed_seed_vector=F.deterministic_vector(
            seed_text or content_text[:200]
        ),
        seed_signature={"trigger_id": str(uuid7())},
    )
    config = LLMConfig.from_env()
    provider = build_provider(config)
    outcome = await think(
        trigger, pool, llm_provider=provider,
        triggering_content=content_text,
        reason_for_trigger="adversarial harness",
        trigger_kind_subkind="T1.event_arrival",
    )

    # Read back what landed in the substrate from this trigger.
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, proposition, "natural", confidence,
                   proposition_kind, status, falsifier
            FROM models
            WHERE tenant_id = $1 AND born_from_event_id = $2
            ORDER BY created_at ASC
            """,
            tenant_id, obs,
        )
    models = [dict(r) for r in rows]
    return {
        "obs_id": str(obs),
        "status": outcome.status,
        "ops_applied_count": outcome.ops_applied_count,
        "error": outcome.error,
        "models": models,
        "model_count": len(models),
        "model_kinds": [m["proposition_kind"] for m in models],
        "model_naturals": [m["natural"] for m in models],
    }
