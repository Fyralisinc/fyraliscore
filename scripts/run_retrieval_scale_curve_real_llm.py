#!/usr/bin/env python3
"""Run a real-LLM retrieval scale curve.

The probe builds isolated high-density tenants inside rollback-only
transactions, then runs the same five signal shapes at increasing model
universe sizes. It exercises the live LLM question-planning path and writes a
JSON report under tests/real_llm/reports/runs/.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("COMPANY_OS_ENV", "test")

import asyncpg
from dotenv import load_dotenv

from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from lib.shared.types import ModelCreate
from services.execution.inquiry import InquiryConfig, run_inquiry_retrieval
from services.models.repo import ModelsRepo, pgvector_pool_init
from services.retrieval.primary import TriggerContext
from services.retrieval.tests._fixtures import build_fixture, make_embedding
from scripts.run_1000_signal_model_layer_probe import _build_cached_provider


load_dotenv(REPO_ROOT / ".env", override=False)


@dataclass(frozen=True)
class ScaleShape:
    target_models: int
    n_actors: int
    n_goals: int
    n_commitments: int
    n_observations: int
    n_customers: int
    n_decisions: int


async def _add_model(
    repo: ModelsRepo,
    conn: asyncpg.Connection,
    *,
    tenant_id,
    born_from_event_id,
    natural: str,
    scope_entities: list[dict[str, str]],
    scope_actors: list,
) -> None:
    await repo.insert(
        ModelCreate(
            tenant_id=tenant_id,
            born_from_event_id=born_from_event_id,
            proposition={
                "kind": "concern",
                "about": natural[:80],
                "nature": "risk",
                "raised_by": "retrieval-scale-curve",
            },
            natural=natural,
            embedding=make_embedding(natural),
            scope_actors=scope_actors,
            scope_entities=scope_entities,
            scope_temporal={"type": "now"},
            confidence=0.62,
            confidence_at_assertion=0.62,
        ),
        conn=conn,
    )


async def _build_scale_universe(
    conn: asyncpg.Connection,
    pool: asyncpg.Pool,
    *,
    tenant_id,
    shape: ScaleShape,
):
    themed_extra_count = 96
    fixture_model_count = max(100, shape.target_models - themed_extra_count)
    fs = await build_fixture(
        conn,
        tenant_id,
        pool=pool,
        rng_seed=42 + shape.target_models,
        n_actors=shape.n_actors,
        n_goals=shape.n_goals,
        n_commitments=shape.n_commitments,
        n_observations=shape.n_observations,
        n_models=fixture_model_count,
        n_customers=shape.n_customers,
        n_decisions=shape.n_decisions,
    )
    repo = ModelsRepo(pool, embedder=None, run_topology_on_insert=False)
    hero_scope = [
        {"type": "commitment", "id": str(fs.hero_commitment_id)},
        {"type": "goal", "id": str(fs.hero_goal_id)},
        {"type": "customer", "id": str(fs.hero_customer_id)},
    ]
    for i in range(24):
        await _add_model(
            repo,
            conn,
            tenant_id=tenant_id,
            born_from_event_id=fs.observation_ids[i % len(fs.observation_ids)],
            natural=(
                f"Acme SSO launch blocker evidence {i}: customer-0 cannot "
                "launch while enterprise SAML permission edge case remains open."
            ),
            scope_entities=hero_scope,
            scope_actors=[fs.hero_actor_id],
        )
    for i in range(72):
        await _add_model(
            repo,
            conn,
            tenant_id=tenant_id,
            born_from_event_id=fs.observation_ids[(i + 40) % len(fs.observation_ids)],
            natural=(
                f"Board portfolio renewal risk {i}: enterprise customer "
                f"customer-{i % max(1, shape.n_customers)} has runway, billing, "
                "legal, or security pressure that may affect the renewal base."
            ),
            scope_entities=[
                {
                    "type": "commitment",
                    "id": str(fs.commitment_ids[i % len(fs.commitment_ids)]),
                },
                {"type": "goal", "id": str(fs.goal_ids[i % len(fs.goal_ids)])},
                {
                    "type": "customer",
                    "id": str(
                        fs.customer_resource_ids[i % len(fs.customer_resource_ids)]
                    ),
                },
            ],
            scope_actors=[fs.actor_ids[i % len(fs.actor_ids)]],
        )
    total_models = await conn.fetchval(
        "SELECT COUNT(*)::int FROM models WHERE tenant_id = $1 AND status = 'active'",
        tenant_id,
    )
    return fs, hero_scope, int(total_models)


def _cases(tenant_id, fs, hero_scope: list[dict[str, str]]) -> dict[str, TriggerContext]:
    now = datetime.now(timezone.utc)
    return {
        "specific_blocker": TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_entity_ids=hero_scope,
            seed_natural_text=(
                "customer-0 cannot launch because the Acme SSO SAML "
                "permission edge case is blocked."
            ),
            seed_occurred_at=now,
            scope_actors=[fs.hero_actor_id],
            precomputed_seed_vector=make_embedding("Acme SSO launch blocker"),
        ),
        "weak_noise": TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_entity_ids=hero_scope,
            seed_natural_text=(
                "customer-0 mentioned the Thursday lunch notes and general "
                "workspace chatter; no blocker, owner change, or decision."
            ),
            seed_occurred_at=now,
            scope_actors=[fs.hero_actor_id],
            precomputed_seed_vector=make_embedding("Thursday lunch workspace chatter"),
        ),
        "broad_board_update": TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_entity_ids=[],
            seed_natural_text=(
                "Board update: across all enterprise customers, renewal "
                "risk, runway pressure, billing disputes, legal review, and "
                "security approvals may affect the portfolio renewal base."
            ),
            seed_occurred_at=now,
            scope_actors=[],
            precomputed_seed_vector=make_embedding("board portfolio renewal risk"),
        ),
        "recurring_incident": TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_entity_ids=hero_scope[:2],
            seed_natural_text=(
                "The Acme SSO permission incident repeated again; this may "
                "be the same recurring launch blocker pattern."
            ),
            seed_occurred_at=now,
            scope_actors=[fs.hero_actor_id],
            precomputed_seed_vector=make_embedding("recurring SSO permission incident"),
        ),
        "unrelated_chatter": TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_entity_ids=[],
            seed_natural_text=(
                "Random hallway note about office snacks and travel plans; "
                "not related to customers, delivery, risk, or commitments."
            ),
            seed_occurred_at=now,
            scope_actors=[],
            precomputed_seed_vector=make_embedding("office snacks travel plans"),
        ),
    }


def _assert_case_shape(case_name: str, selected: int, signal_class: str) -> None:
    if case_name == "specific_blocker":
        assert 4 <= selected < 32
    elif case_name == "weak_noise":
        assert selected <= 10 and signal_class == "weak"
    elif case_name == "broad_board_update":
        assert 24 <= selected <= 64 and signal_class == "broad"
    elif case_name == "recurring_incident":
        assert 3 <= selected < 40
    elif case_name == "unrelated_chatter":
        assert selected <= 8 and signal_class == "weak"


async def _run_scale(
    pool: asyncpg.Pool,
    provider,
    shape: ScaleShape,
) -> dict[str, Any]:
    tenant_id = uuid7()
    conn = await pool.acquire()
    tx = conn.transaction()
    await tx.start()
    scale_started = time.monotonic()
    try:
        await pgvector_pool_init(conn)
        await conn.execute("SET CONSTRAINTS ALL DEFERRED")
        await conn.execute(
            """
            INSERT INTO tenants (id, name, is_demo)
            VALUES ($1, $2, true)
            ON CONFLICT (id) DO NOTHING
            """,
            tenant_id,
            f"retrieval_scale_{shape.target_models}",
        )
        build_started = time.monotonic()
        fs, hero_scope, total_models = await _build_scale_universe(
            conn,
            pool,
            tenant_id=tenant_id,
            shape=shape,
        )
        build_elapsed_ms = round((time.monotonic() - build_started) * 1000, 2)
        cfg = InquiryConfig(
            max_rounds=1,
            questions_per_round=2,
            candidate_model_limit=180,
            result_model_limit=64,
            action_model_budget_limit=40,
            action_observation_budget_limit=20,
            persist=False,
        )
        results: dict[str, Any] = {}
        for case_name, trigger in _cases(tenant_id, fs, hero_scope).items():
            case_started = time.monotonic()
            result = await run_inquiry_retrieval(
                trigger,
                conn,
                llm_provider=provider,
                mode="deep",
                top_n=180,
                config=cfg,
            )
            elapsed_ms = round((time.monotonic() - case_started) * 1000, 2)
            relevance = result.retrieval_result.notes["relevance_gate"]
            planning = result.notes["question_planning"][0]
            selected = len(result.retrieval_result.models)
            assert planning["mode"] == "llm"
            assert selected < relevance["candidate_count"]
            _assert_case_shape(
                case_name,
                selected,
                str(relevance["signal_class"]),
            )
            results[case_name] = {
                "selected": selected,
                "candidates": relevance["candidate_count"],
                "signal_class": relevance["signal_class"],
                "threshold": relevance["threshold"],
                "question_mode": planning["mode"],
                "llm_primitives": planning.get("llm_primitives", []),
                "elapsed_ms": elapsed_ms,
            }
        return {
            "target_models": shape.target_models,
            "total_models": total_models,
            "build_elapsed_ms": build_elapsed_ms,
            "scale_elapsed_ms": round((time.monotonic() - scale_started) * 1000, 2),
            "cases": results,
        }
    finally:
        await tx.rollback()
        await pool.release(conn)


def _shapes(scales: list[int]) -> list[ScaleShape]:
    out: list[ScaleShape] = []
    for scale in scales:
        if scale <= 750:
            out.append(ScaleShape(scale, 14, 28, 90, 260, 14, 12))
        elif scale <= 2000:
            out.append(ScaleShape(scale, 22, 48, 160, 520, 32, 24))
        else:
            out.append(ScaleShape(scale, 34, 90, 280, 900, 72, 48))
    return out


async def run_probe(scales: list[int], *, pool_max_size: int = 3) -> dict[str, Any]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")
    provider = _build_cached_provider()
    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=pool_max_size,
        init=pgvector_pool_init,
    )
    try:
        async with pool.acquire() as conn:
            await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
        started = time.monotonic()
        scale_results = []
        for shape in _shapes(scales):
            print(f"running retrieval scale {shape.target_models}", flush=True)
            scale_result = await _run_scale(pool, provider, shape)
            print(json.dumps(scale_result, indent=2, sort_keys=True), flush=True)
            scale_results.append(scale_result)
        report = {
            "run_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "scales": scale_results,
        }
        report_dir = REPO_ROOT / "tests" / "real_llm" / "reports" / "runs"
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"retrieval-scale-curve-{report['run_id']}.json"
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True))
        report["report_path"] = str(report_path)
        return report
    finally:
        await pool.close()


def _parse_scales(raw: str) -> list[int]:
    values = [int(part.strip()) for part in raw.split(",") if part.strip()]
    if not values:
        raise ValueError("at least one scale is required")
    return values


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scales",
        default=os.environ.get("RETRIEVAL_SCALE_CURVE_SCALES", "500,1500,5000"),
        help="Comma-separated target active model counts.",
    )
    args = parser.parse_args()
    report = asyncio.run(run_probe(_parse_scales(args.scales)))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
