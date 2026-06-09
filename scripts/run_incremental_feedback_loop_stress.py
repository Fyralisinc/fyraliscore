#!/usr/bin/env python3
"""Incremental retrieval/model feedback-loop stress runner.

This harness emulates one persistent company tenant. It seeds a large model
layer once, then runs a sequence of Ask-style retrieval cases where each
case commits writer-outcome feedback before the next case starts.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import statistics
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

os.environ.setdefault("COMPANY_OS_ENV", "test")

import asyncpg
from dotenv import load_dotenv

from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from services.execution.inquiry import InquiryConfig, run_inquiry_retrieval
from services.models.repo import ModelsRepo, pgvector_pool_init
from services.retrieval.primary import TriggerContext
from services.sage.outcome_evaluator import OutcomeEvaluator
from services.sage.topology_optimizer import TopologyOptimizer

from run_100x_5000_model_e2e_stress import (  # noqa: E402
    ARCHETYPES,
    _action_timing_summary,
    _build_case_models,
    _embedding,
    _insert_graph_edges,
    _insert_scaffold,
    _sage_stage_timing_summary,
    _sidecar_counts,
)


load_dotenv(REPO_ROOT / ".env", override=False)

LOCAL_DATABASE_URL = "postgresql://company_os:company_os@localhost:5432/company_os"
REPORT_DIR = REPO_ROOT / "tests" / "real_llm" / "reports" / "runs"

_ARCHITECTURE_SLO_THRESHOLDS = {
    "retrieval_p95_ms": 3500.0,
    "selected_ge_35_ratio": 0.05,
    "expected_evidence_ge_24_ratio": 0.25,
    "sage_rank_drift_ratio": 0.05,
    "rich_negative_memory_per_expected_case": 0.25,
    "quality_failure_ratio": 0.01,
    "reader_attributions_per_case": 200.0,
    "negative_memory_per_case": 1.0,
    "canonical_enqueue_per_case": 0.25,
}
_THIN_SLO_MARGIN_RATIO = 0.10
_TRACE_PRESSURE_ATTRIBUTIONS_PER_NEW_LEARNING = 5000.0
_TRACE_PRESSURE_MIN_ATTRIBUTION_DELTA = 1000
_LEARNING_SIGNAL_KEYS = (
    "contextual_affordance_profiles",
    "discovery_shortcuts",
    "negative_memory",
    "reinforced_affordance_profiles",
)


@dataclass(frozen=True, slots=True)
class SeededCompany:
    tenant_id: UUID
    family_cases: tuple[Any, ...]
    total_models: int
    insert_ms: float
    sidecars: dict[str, int]


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = (len(ordered) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    frac = idx - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _architecture_slo(
    value: float,
    threshold: float,
    *,
    direction: str = "max",
) -> dict[str, Any]:
    passed = value <= threshold if direction == "max" else value >= threshold
    return {
        "value": round(float(value), 6),
        "threshold": round(float(threshold), 6),
        "direction": direction,
        "passed": bool(passed),
    }


def _architecture_slos(
    *,
    results: list[dict[str, Any]],
    expected_rows: list[dict[str, Any]],
    retrieval_ms: list[float],
    final_counts: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    cases = max(len(results), 1)
    expected_cases = max(len(expected_rows), 1)
    p95_ms = _percentile(retrieval_ms, 0.95)
    selected_ge_35 = sum(
        1 for row in results if int(row.get("selected_count") or 0) >= 35
    )
    expected_evidence_ge_24 = sum(
        1 for row in expected_rows if int(row.get("evidence_count") or 0) >= 24
    )
    sage_rank_drift = sum(
        1
        for row in expected_rows
        if row.get("expected_best_sage_rank") is not None
        and int(row.get("expected_best_sage_rank") or 0) > 1
    )
    rich_negative_memory = sum(
        int((row.get("optimizer") or {}).get("negative_memory_inserts") or 0)
        for row in expected_rows
    )
    quality_failures = sum(
        1 for row in results if row.get("quality_failure_modes")
    )
    canonical_enqueues = sum(
        float(
            ((row.get("optimizer") or {}).get("metrics") or {}).get(
                "canonical_validation_enqueued"
            )
            or 0.0
        )
        for row in results
    )
    reader_attributions = int(final_counts.get("reader_decision_attributions") or 0)
    negative_memory = int(final_counts.get("negative_memory") or 0)

    return {
        "retrieval_p95_ms": _architecture_slo(
            p95_ms, _ARCHITECTURE_SLO_THRESHOLDS["retrieval_p95_ms"],
        ),
        "selected_ge_35_ratio": _architecture_slo(
            selected_ge_35 / cases,
            _ARCHITECTURE_SLO_THRESHOLDS["selected_ge_35_ratio"],
        ),
        "expected_evidence_ge_24_ratio": _architecture_slo(
            expected_evidence_ge_24 / expected_cases,
            _ARCHITECTURE_SLO_THRESHOLDS["expected_evidence_ge_24_ratio"],
        ),
        "sage_rank_drift_ratio": _architecture_slo(
            sage_rank_drift / expected_cases,
            _ARCHITECTURE_SLO_THRESHOLDS["sage_rank_drift_ratio"],
        ),
        "rich_negative_memory_per_expected_case": _architecture_slo(
            rich_negative_memory / expected_cases,
            _ARCHITECTURE_SLO_THRESHOLDS[
                "rich_negative_memory_per_expected_case"
            ],
        ),
        "quality_failure_ratio": _architecture_slo(
            quality_failures / cases,
            _ARCHITECTURE_SLO_THRESHOLDS["quality_failure_ratio"],
        ),
        "reader_attributions_per_case": _architecture_slo(
            reader_attributions / cases,
            _ARCHITECTURE_SLO_THRESHOLDS["reader_attributions_per_case"],
        ),
        "negative_memory_per_case": _architecture_slo(
            negative_memory / cases,
            _ARCHITECTURE_SLO_THRESHOLDS["negative_memory_per_case"],
        ),
        "canonical_enqueue_per_case": _architecture_slo(
            canonical_enqueues / cases,
            _ARCHITECTURE_SLO_THRESHOLDS["canonical_enqueue_per_case"],
        ),
    }


def _architecture_slo_findings(
    slos: dict[str, dict[str, Any]],
) -> list[str]:
    labels = {
        "retrieval_p95_ms": "Retrieval p95 exceeds the architecture SLO",
        "selected_ge_35_ratio": "Model selection breadth is above the architecture SLO",
        "expected_evidence_ge_24_ratio": "Expected cases are saturating evidence packets",
        "sage_rank_drift_ratio": "Sage reader rank drift is above the architecture SLO",
        "rich_negative_memory_per_expected_case": (
            "Rich expected cases are writing too much negative memory"
        ),
        "quality_failure_ratio": "Quality failure cases exceed the architecture SLO",
        "reader_attributions_per_case": "Reader attribution write amplification is high",
        "negative_memory_per_case": "Negative-memory growth per case is high",
        "canonical_enqueue_per_case": (
            "Canonical validation backlog is growing faster than the SLO"
        ),
    }
    findings: list[str] = []
    for key, slo in slos.items():
        if bool(slo.get("passed")):
            continue
        findings.append(
            f"{labels.get(key, key)}: value={slo['value']} "
            f"threshold={slo['threshold']}."
        )
    return findings


def _learning_signal_total(counts: dict[str, Any]) -> int:
    return sum(int(counts.get(key) or 0) for key in _LEARNING_SIGNAL_KEYS)


def _learning_pressure(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize whether feedback is compacting or merely tracing.

    The optimizer should eventually turn repeated successful reads into compact
    utility structures. If attribution rows keep growing while shortcuts,
    affordances, and negative memory stop growing, the system is accumulating
    audit exhaust rather than moving toward its theoretical limit.
    """

    if not results:
        return {
            "quarters": [],
            "max_attributions_per_new_learning_signal": 0.0,
            "late_trace_pressure": False,
        }

    total = len(results)
    quarter_size = max(1, total // 4)
    quarters: list[dict[str, Any]] = []
    max_ratio = 0.0
    late_pressure = False
    for idx, start in enumerate(range(0, total, quarter_size), start=1):
        chunk = results[start:min(start + quarter_size, total)]
        if not chunk:
            continue
        first = chunk[0].get("learned_counts") or {}
        last = chunk[-1].get("learned_counts") or {}
        attribution_delta = int(
            (last.get("reader_decision_attributions") or 0)
            - (first.get("reader_decision_attributions") or 0)
        )
        learning_delta = _learning_signal_total(last) - _learning_signal_total(first)
        ratio = attribution_delta / max(learning_delta, 1)
        max_ratio = max(max_ratio, ratio)
        pressured = (
            idx > 1
            and attribution_delta >= _TRACE_PRESSURE_MIN_ATTRIBUTION_DELTA
            and ratio >= _TRACE_PRESSURE_ATTRIBUTIONS_PER_NEW_LEARNING
        )
        late_pressure = late_pressure or pressured
        quarters.append({
            "quarter": idx,
            "case_start": int(chunk[0].get("case_index") or start + 1),
            "case_end": int(chunk[-1].get("case_index") or start + len(chunk)),
            "reader_decision_attribution_delta": attribution_delta,
            "compact_learning_delta": learning_delta,
            "attributions_per_new_learning_signal": round(ratio, 6),
            "trace_pressure": pressured,
        })

    return {
        "quarters": quarters,
        "max_attributions_per_new_learning_signal": round(max_ratio, 6),
        "late_trace_pressure": late_pressure,
    }


def _readiness_assessment(
    *,
    cases: int,
    passed: int,
    expected_hit_rate: float,
    expected_misses: int,
    retrieval_ms: dict[str, float],
    architecture_slos: dict[str, dict[str, Any]],
    final_counts: dict[str, Any],
    learning_pressure: dict[str, Any],
    source_realism: dict[str, Any],
) -> dict[str, Any]:
    """Conservative deployment-readiness gate for scale reports.

    This is intentionally stricter than the architecture SLO table. SLOs answer
    "did this run pass its harness contract?" Readiness answers "what kind of
    customer promise is this evidence strong enough to support?"
    """

    pass_rate = passed / max(cases, 1)
    p95 = float(retrieval_ms.get("p95") or 0.0)
    p95_slo = architecture_slos.get("retrieval_p95_ms") or {}
    p95_threshold = float(p95_slo.get("threshold") or 0.0)
    p95_passed = bool(p95_slo.get("passed"))
    p95_margin = (
        (p95_threshold - p95) / p95_threshold
        if p95_threshold > 0 else 0.0
    )
    learned_compact_routes = (
        int(final_counts.get("contextual_affordance_profiles") or 0)
        + int(final_counts.get("discovery_shortcuts") or 0)
        + int(final_counts.get("reinforced_affordance_profiles") or 0)
    )

    dimensions = {
        "correctness": {
            "passed": bool(
                pass_rate >= 0.99
                and expected_hit_rate >= 0.995
                and expected_misses == 0
            ),
            "pass_rate": round(pass_rate, 6),
            "expected_hit_rate": round(expected_hit_rate, 6),
            "expected_misses": expected_misses,
        },
        "retrieval_performance": {
            "passed": p95_passed and p95_margin > _THIN_SLO_MARGIN_RATIO,
            "p95_ms": round(p95, 3),
            "slo_ms": round(p95_threshold, 3),
            "slo_margin_ratio": round(p95_margin, 6),
        },
        "feedback_efficiency": {
            "passed": not bool(learning_pressure.get("late_trace_pressure")),
            "late_trace_pressure": bool(
                learning_pressure.get("late_trace_pressure")
            ),
            "max_attributions_per_new_learning_signal": learning_pressure.get(
                "max_attributions_per_new_learning_signal", 0.0,
            ),
        },
        "operational_learning": {
            "passed": learned_compact_routes > 0,
            "compact_route_count": learned_compact_routes,
            "negative_memory_count": int(final_counts.get("negative_memory") or 0),
        },
        "source_realism": {
            "passed": bool(source_realism.get("multi_source_ingestion_validated")),
            "mode": source_realism.get("mode", "unknown"),
        },
    }

    blockers: list[str] = []
    if not dimensions["correctness"]["passed"]:
        blockers.append("correctness_or_expected_retrieval")
    if not p95_passed:
        blockers.append("retrieval_p95_slo")
    elif p95_margin <= _THIN_SLO_MARGIN_RATIO:
        blockers.append("thin_retrieval_latency_margin")
    if learning_pressure.get("late_trace_pressure"):
        blockers.append("late_trace_pressure")
    if learned_compact_routes <= 0:
        blockers.append("no_compact_learning")
    if not source_realism.get("multi_source_ingestion_validated"):
        blockers.append("multi_source_ingestion_not_validated")

    if not dimensions["correctness"]["passed"] or not p95_passed:
        tier = "internal_dogfood"
    elif blockers:
        tier = "design_partner_controlled"
    else:
        tier = "customer_beta"

    score = 100
    score -= 40 if not dimensions["correctness"]["passed"] else 0
    score -= 20 if not p95_passed else 0
    score -= 8 if p95_passed and p95_margin <= _THIN_SLO_MARGIN_RATIO else 0
    score -= 12 if learning_pressure.get("late_trace_pressure") else 0
    score -= 6 if learned_compact_routes <= 0 else 0
    score -= (
        15 if not source_realism.get("multi_source_ingestion_validated") else 0
    )
    score = max(0, min(100, score))

    return {
        "tier": tier,
        "score": score,
        "blockers": blockers,
        "dimensions": dimensions,
        "customer_value": [
            "high_recall_company_memory_retrieval",
            "noise_suppression_for_weak_workspace_chatter",
            "incremental_positive_feedback_learning",
        ],
        "next_actions": [
            "run_multi_source_signal_probe",
            "tighten_or_downsample_nonselected_reader_attributions",
            "add_compaction_for_repeated_success_traces",
            "harden_tail_latency_for_broad_and_hidden_graph_queries",
        ],
    }


async def _ensure_migrations(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await pgvector_pool_init(conn)
        await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")


async def _seed_company(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    families: int,
    total_models: int,
) -> SeededCompany:
    models_per_family = max(1, math.ceil(total_models / families))
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        await pgvector_pool_init(conn)
        async with conn.transaction():
            await conn.execute("SET CONSTRAINTS ALL DEFERRED")
            drafts: list[Any] = []
            family_cases: list[Any] = []
            for index in range(families):
                archetype = ARCHETYPES[index % len(ARCHETYPES)]
                scaffold = await _insert_scaffold(
                    conn,
                    tenant_id=tenant_id,
                    index=index,
                    archetype=archetype,
                    now=now - timedelta(minutes=index),
                )
                family_drafts, case = _build_case_models(
                    index=index,
                    archetype=archetype,
                    scaffold=scaffold,
                    models_per_case=models_per_family,
                )
                drafts.extend(family_drafts)
                family_cases.append(case)

            repo = ModelsRepo(
                pool,
                embedder=None,
                run_topology_on_insert=False,
            )
            started = time.perf_counter()
            await repo.insert_many(
                drafts,
                conn=conn,
                apply_confidence_calibration=False,
            )
            for case in family_cases:
                await _insert_graph_edges(
                    conn,
                    tenant_id=tenant_id,
                    case=case,
                )
            insert_ms = (time.perf_counter() - started) * 1000.0
            sidecars = await _sidecar_counts(conn, tenant_id)

    return SeededCompany(
        tenant_id=tenant_id,
        family_cases=tuple(family_cases),
        total_models=len(drafts),
        insert_ms=insert_ms,
        sidecars=sidecars,
    )


def _variant_text(case: Any, repetition: int) -> str:
    base = str(case.trigger.seed_natural_text or "")
    markerless = base.replace(str(case.marker), "").replace("  ", " ").strip()
    if repetition <= 1:
        return base
    if getattr(getattr(case, "archetype", None), "key", "") == "weak_workspace_noise":
        if repetition == 2:
            return (
                f"{markerless} Follow up: confirm this remains non-actionable "
                "workspace chatter with no blocker, owner change, decision, "
                "customer risk, or commitment update."
            )
        if repetition == 3:
            return (
                "The same scoped account only has workspace chatter again: "
                "lunch notes, travel plans, and general team coordination. "
                "No blocker, no owner change, no decision, no customer risk, "
                "and no commitment update."
            )
        if repetition == 4:
            return (
                "Ask Fyralis to ignore this non-actionable workspace chatter "
                "for the same scope: lunch notes, travel plans, and general "
                "team coordination; no blocker, owner change, decision, "
                "customer risk, or commitment update."
            )
        return (
            "Same entities, only weak workspace chatter: lunch notes, travel "
            "plans, and general team coordination. No action, no commitment "
            "update, and no risk."
        )
    if repetition == 2:
        return (
            f"{markerless} Follow up: identify the current blocker, dependency, "
            "owner constraint, counterevidence, and next action for the same scope."
        )
    if repetition == 3:
        return (
            "The same scoped account is under review again. Which durable Fyralis "
            "beliefs explain the unresolved operational gate, customer impact, "
            "and dependency chain?"
        )
    if repetition == 4:
        return (
            "Ask Fyralis for the smallest sufficient memory around this scoped "
            "situation: current state, binding dependency, owner capacity, "
            "counterevidence, and required action."
        )
    return (
        "For the same entities, retrieve the compact set of model beliefs that "
        "should make the active execution risk easy to reason about next time."
    )


def _variant_trigger(case: Any, repetition: int, *, hard_followups: bool) -> TriggerContext:
    text = _variant_text(case, repetition)
    vector_key = (
        f"{case.marker}:target"
        if repetition <= 1 or not hard_followups
        else f"{case.marker}:followup:{repetition}"
    )
    return TriggerContext(
        kind=case.trigger.kind,
        tenant_id=case.trigger.tenant_id,
        observation_id=case.trigger.observation_id,
        seed_entity_ids=list(case.trigger.seed_entity_ids or []),
        seed_natural_text=text,
        seed_occurred_at=datetime.now(timezone.utc),
        scope_actors=list(case.trigger.scope_actors or []),
        precomputed_seed_vector=_embedding(vector_key),
        semantic_k=case.trigger.semantic_k,
        temporal_window=case.trigger.temporal_window,
        max_hops=case.trigger.max_hops,
    )


def _selected_expected_ids(result: Any, expected_ids: list[UUID]) -> tuple[list[UUID], dict[UUID, int]]:
    expected = set(expected_ids)
    selected = [model.id for model in result.retrieval_result.models]
    ranks = {
        model_id: rank
        for rank, model_id in enumerate(selected, start=1)
        if model_id in expected
    }
    return [model_id for model_id in selected if model_id in expected], ranks


def _sage_expected_activation_metrics(
    notes: dict[str, Any],
    expected_ids: list[UUID],
) -> dict[str, Any]:
    expected = {str(mid) for mid in expected_ids}
    source_totals: Counter[str] = Counter()
    reason_totals: Counter[str] = Counter()
    activated = 0
    selected = 0
    best_rank: int | None = None
    questions = ((notes.get("sage_reader") or {}).get("questions") or {})
    if not isinstance(questions, dict):
        return {
            "expected_activated": 0,
            "expected_sage_selected": 0,
            "expected_best_sage_rank": None,
            "expected_sources": {},
            "expected_reasons": {},
        }
    for qnote in questions.values():
        if not isinstance(qnote, dict):
            continue
        for trace in qnote.get("activations") or []:
            if not isinstance(trace, dict):
                continue
            if str(trace.get("model_id")) not in expected:
                continue
            activated += 1
            if trace.get("selected"):
                selected += 1
                raw_rank = trace.get("selection_rank")
                if raw_rank is not None:
                    rank = int(raw_rank) + 1
                    best_rank = rank if best_rank is None else min(best_rank, rank)
            for source, value in (trace.get("source_breakdown") or {}).items():
                try:
                    source_totals[str(source)] += float(value)
                except (TypeError, ValueError):
                    continue
            for reason in trace.get("activation_reasons") or []:
                prefix = str(reason).split(":", 1)[0]
                reason_totals[prefix] += 1
    return {
        "expected_activated": activated,
        "expected_sage_selected": selected,
        "expected_best_sage_rank": best_rank,
        "expected_sources": dict(source_totals.most_common()),
        "expected_reasons": dict(reason_totals.most_common()),
    }


async def _attach_writer_outcome(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    result: Any,
    trigger: TriggerContext,
    used_model_ids: list[UUID],
    expected_model_ids: list[UUID],
) -> dict[str, Any]:
    trigger_id = uuid7()
    think_run_id = uuid7()
    status = "success"
    applied_outcome = "success"
    error = None
    if expected_model_ids and not used_model_ids:
        status = "failed"
        applied_outcome = "validation_failure"
        error = "missing evidence: expected useful model was not retrieved"

    claim_ops = [
        {"op": "update", "model_id": str(model_id)}
        for model_id in used_model_ids[:4]
    ]
    ops_applied = {
        "claim_ops": claim_ops,
        "edge_ops": [],
        "act_ops": [],
        "resource_ops": [],
        "dropped_op_count": 0 if status == "success" else 1,
    }
    await conn.execute(
        """
        INSERT INTO applied_triggers (
          trigger_id, tenant_id, diff_hash, trigger_kind, outcome
        ) VALUES ($1, $2, $3, $4, $5)
        """,
        trigger_id,
        tenant_id,
        f"incremental-feedback-{trigger_id}",
        trigger.kind,
        applied_outcome,
    )
    await conn.execute(
        """
        INSERT INTO think_runs (
          id, tenant_id, trigger_id, trigger_kind, ended_at, status, error,
          retrieval_model_count, retrieval_observation_count, ops_applied
        ) VALUES (
          $1, $2, $3, $4, now(), $5, $6,
          $7, $8, $9::jsonb
        )
        """,
        think_run_id,
        tenant_id,
        trigger_id,
        trigger.kind,
        status,
        error,
        len(result.retrieval_result.models),
        len(result.retrieval_result.observations),
        json.dumps(ops_applied),
    )
    await conn.execute(
        """
        UPDATE inquiry_sessions
           SET think_run_id = $2
         WHERE id = $1
           AND tenant_id = $3
        """,
        result.session_id,
        think_run_id,
        tenant_id,
    )
    return {
        "think_run_id": str(think_run_id),
        "status": status,
        "used_model_ids": [str(mid) for mid in used_model_ids[:4]],
        "expected_miss": bool(expected_model_ids and not used_model_ids),
    }


async def _learned_layer_counts(conn: asyncpg.Connection, tenant_id: UUID) -> dict[str, int]:
    rows = await conn.fetch(
        """
        SELECT 'affordance_profiles' AS name, COUNT(*)::int AS count
          FROM retrieval_affordance_profiles WHERE tenant_id = $1
        UNION ALL
        SELECT 'reinforced_affordance_profiles', COUNT(*)::int
          FROM retrieval_affordance_profiles
         WHERE tenant_id = $1 AND utility_score > 0
        UNION ALL
        SELECT 'contextual_affordance_profiles', COUNT(*)::int
          FROM retrieval_affordance_profiles
         WHERE tenant_id = $1
           AND jsonb_typeof(activation_signatures->'entities') = 'array'
           AND jsonb_array_length(activation_signatures->'entities') > 0
        UNION ALL
        SELECT 'discovery_shortcuts', COUNT(*)::int
          FROM discovery_shortcuts WHERE tenant_id = $1
        UNION ALL
        SELECT 'negative_memory', COUNT(*)::int
          FROM negative_memory WHERE tenant_id = $1
        UNION ALL
        SELECT 'question_policy_stats', COUNT(*)::int
          FROM sage_question_policy_stats WHERE tenant_id = $1
        UNION ALL
        SELECT 'reader_decision_attributions', COUNT(*)::int
          FROM sage_reader_decision_attributions WHERE tenant_id = $1
        """,
        tenant_id,
    )
    return {str(row["name"]): int(row["count"]) for row in rows}


async def _run_one_case(
    pool: asyncpg.Pool,
    *,
    step_index: int,
    case: Any,
    repetition: int,
    hard_followups: bool,
) -> dict[str, Any]:
    trigger = _variant_trigger(case, repetition, hard_followups=hard_followups)
    cfg = InquiryConfig(
        max_rounds=1,
        questions_per_round=3,
        evidence_reservoir_limit=260,
        fast_path_evidence_limit=48,
        candidate_model_limit=180,
        result_model_limit=56,
        action_model_budget_limit=40,
        action_observation_budget_limit=28,
        relevance_min_material_models=3,
        temporal_window_days=30,
        semantic_budget=48,
        structural_max_hops=2,
        model_edge_max_hops=2,
        llm_question_planning_enabled=False,
        sage_reader_enabled=True,
        persist=True,
    )
    async with pool.acquire() as conn:
        await pgvector_pool_init(conn)
        async with conn.transaction():
            started = time.perf_counter()
            result = await run_inquiry_retrieval(
                trigger,
                conn,
                embedder=None,
                llm_provider=None,
                mode="deep",
                top_n=180,
                config=cfg,
            )
            retrieval_ms = (time.perf_counter() - started) * 1000.0
            selected_expected, selected_ranks = _selected_expected_ids(
                result,
                case.expected_model_ids,
            )
            writer = await _attach_writer_outcome(
                conn,
                tenant_id=trigger.tenant_id,
                result=result,
                trigger=trigger,
                used_model_ids=selected_expected,
                expected_model_ids=case.expected_model_ids,
            )
            outcome = await OutcomeEvaluator(
                pool=None,
                tenant_id=trigger.tenant_id,
            ).evaluate(
                inquiry_session_id=result.session_id,
                conn=conn,
            )
            optimizer = await TopologyOptimizer(
                pool=None,
                tenant_id=trigger.tenant_id,
            ).optimize(
                inquiry_session_id=result.session_id,
                trigger_event=(
                    "validated_synthesis_diff_applied"
                    if writer["status"] == "success"
                    else "reasoning_diff_failed_validation"
                ),
                conn=conn,
            )
            learned_counts = await _learned_layer_counts(conn, trigger.tenant_id)

    inquiry_notes = result.notes or {}
    sage_by_question, sage_stage_max, sage_stage_total = _sage_stage_timing_summary(
        inquiry_notes.get("sage_reader") or {}
    )
    action_timing_max, action_timing_total = _action_timing_summary(inquiry_notes)
    sage_activation = _sage_expected_activation_metrics(
        inquiry_notes,
        case.expected_model_ids,
    )
    min_rank = min(selected_ranks.values()) if selected_ranks else None
    expected_case = bool(case.expected_model_ids)
    passed = (not expected_case) or bool(selected_expected)
    metrics = {
        "case_index": step_index,
        "family_index": case.index,
        "family": case.name,
        "archetype": case.archetype.key,
        "repetition": repetition,
        "expected_case": expected_case,
        "passed": passed,
        "retrieval_ms": round(retrieval_ms, 3),
        "selected_count": len(result.retrieval_result.models),
        "evidence_count": len(result.evidence_cards),
        "expected_final_hits": len(selected_expected),
        "expected_best_final_rank": min_rank,
        "writer": writer,
        "outcome_events": outcome.events_by_type,
        "quality_bottleneck": outcome.quality_signal.primary_bottleneck,
        "quality_failure_modes": list(outcome.quality_signal.failure_modes),
        "optimizer": {
            "affordance_reinforces": optimizer.affordance_reinforces,
            "affordance_decays": optimizer.affordance_decays,
            "shortcut_creates_or_bumps": optimizer.shortcut_creates_or_bumps,
            "shortcut_decays": optimizer.shortcut_decays,
            "negative_memory_inserts": optimizer.negative_memory_inserts,
            "question_policy_updates": optimizer.question_policy_updates,
            "canonical_merge_candidates": len(optimizer.canonical_merge_candidates),
            "canonical_split_candidates": len(optimizer.canonical_split_candidates),
            "canonical_promote_candidates": len(optimizer.canonical_promote_candidates),
            "canonical_demote_candidates": len(optimizer.canonical_demote_candidates),
            "metrics": optimizer.metrics,
        },
        "learned_counts": learned_counts,
        "sage_stage_timings_ms_max": sage_stage_max,
        "sage_stage_timings_ms_total": sage_stage_total,
        "sage_question_count": len(sage_by_question),
        "retrieval_action_timings_ms_max": action_timing_max,
        "retrieval_action_timings_ms_total": action_timing_total,
        "retrieval_action_cache": inquiry_notes.get("retrieval_action_cache") or {},
        **sage_activation,
    }
    print(json.dumps(metrics, sort_keys=True), flush=True)
    return metrics


def _summarize(
    *,
    tenant_id: UUID,
    company: SeededCompany,
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    retrieval = [
        float(row["retrieval_ms"])
        for row in results
        if row.get("retrieval_ms") is not None
    ]
    expected_rows = [row for row in results if row.get("expected_case")]
    misses = [
        row for row in expected_rows
        if int(row.get("expected_final_hits") or 0) <= 0
    ]
    by_repetition: dict[int, list[dict[str, Any]]] = defaultdict(list)
    by_family: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in results:
        by_repetition[int(row["repetition"])].append(row)
        by_family[int(row["family_index"])].append(row)

    rep_summary = {}
    for rep, rows in sorted(by_repetition.items()):
        expected = [r for r in rows if r.get("expected_case")]
        latencies = [
            float(r["retrieval_ms"])
            for r in rows
            if r.get("retrieval_ms") is not None
        ]
        rep_summary[str(rep)] = {
            "cases": len(rows),
            "expected_cases": len(expected),
            "hit_rate": (
                sum(1 for r in expected if int(r.get("expected_final_hits") or 0) > 0)
                / max(len(expected), 1)
            ),
            "mean_retrieval_ms": statistics.fmean(latencies) if latencies else 0.0,
            "mean_best_rank": statistics.fmean(
                [
                    int(r["expected_best_final_rank"])
                    for r in expected
                    if r.get("expected_best_final_rank") is not None
                ]
            ) if any(r.get("expected_best_final_rank") is not None for r in expected) else None,
            "mean_expected_activated": statistics.fmean(
                [int(r.get("expected_activated") or 0) for r in expected]
            ) if expected else 0.0,
        }

    family_deltas = []
    for family_index, rows in sorted(by_family.items()):
        expected = [r for r in rows if r.get("expected_case")]
        if len(expected) < 2:
            continue
        first = expected[0]
        last = expected[-1]
        first_rank = first.get("expected_best_final_rank")
        last_rank = last.get("expected_best_final_rank")
        family_deltas.append({
            "family_index": family_index,
            "family": first.get("family"),
            "archetype": first.get("archetype"),
            "first_hit": int(first.get("expected_final_hits") or 0) > 0,
            "last_hit": int(last.get("expected_final_hits") or 0) > 0,
            "first_rank": first_rank,
            "last_rank": last_rank,
            "rank_delta": (
                int(first_rank) - int(last_rank)
                if first_rank is not None and last_rank is not None else None
            ),
            "retrieval_ms_delta": (
                float(first.get("retrieval_ms") or 0.0)
                - float(last.get("retrieval_ms") or 0.0)
            ),
        })

    final_counts = results[-1].get("learned_counts") if results else {}
    source_totals = Counter()
    for row in results:
        source_totals.update(row.get("expected_reasons") or {})
    architecture_slos = _architecture_slos(
        results=results,
        expected_rows=expected_rows,
        retrieval_ms=retrieval,
        final_counts=final_counts,
    )
    learning_pressure = _learning_pressure(results)
    source_realism = {
        "mode": "model_layer_scaffolded",
        "multi_source_ingestion_validated": False,
        "recommended_followup": (
            "scripts/run_1000_signal_model_layer_probe.py --signals 2000 "
            "--think-limit 2000"
        ),
    }
    passed_count = sum(1 for row in results if row.get("passed"))
    expected_hit_rate = (
        (len(expected_rows) - len(misses)) / max(len(expected_rows), 1)
    )
    retrieval_summary = {
        "min": min(retrieval) if retrieval else 0.0,
        "mean": statistics.fmean(retrieval) if retrieval else 0.0,
        "median": statistics.median(retrieval) if retrieval else 0.0,
        "p95": _percentile(retrieval, 0.95),
        "max": max(retrieval) if retrieval else 0.0,
    }

    structural_findings = []
    if misses:
        structural_findings.append(
            "Some hard reads still miss all expected models; missing-evidence outcomes "
            "are recorded, but they do not yet create canonical model organization."
        )
    if int(final_counts.get("contextual_affordance_profiles") or 0) == 0:
        structural_findings.append(
            "No contextual affordance profiles were learned; similar-query routing "
            "would depend on global utility rather than entity/question shape."
        )
    if int(final_counts.get("discovery_shortcuts") or 0) == 0:
        structural_findings.append(
            "No discovery shortcuts were learned; path-level credit is not closing "
            "the read/write loop for multi-model evidence."
        )
    has_canonical_candidates = any(
        int((row.get("optimizer") or {}).get("canonical_merge_candidates") or 0) > 0
        or int((row.get("optimizer") or {}).get("canonical_promote_candidates") or 0) > 0
        for row in results
    )
    has_validation_enqueues = any(
        float(
            ((row.get("optimizer") or {}).get("metrics") or {}).get(
                "canonical_validation_enqueued"
            )
            or 0.0
        )
        > 0.0
        for row in results
    )
    if has_canonical_candidates and not has_validation_enqueues:
        structural_findings.append(
            "The optimizer proposes canonical organization candidates, but the "
            "validation/enqueue path is still a no-op, so canonical model topology "
            "does not self-organize from feedback yet."
        )
    retrieval_slo = architecture_slos.get("retrieval_p95_ms") or {}
    retrieval_slo_value = float(retrieval_slo.get("value") or 0.0)
    retrieval_slo_threshold = float(retrieval_slo.get("threshold") or 0.0)
    if retrieval_slo_threshold > 0 and retrieval_slo.get("passed"):
        margin = (
            (retrieval_slo_threshold - retrieval_slo_value)
            / retrieval_slo_threshold
        )
        if margin <= _THIN_SLO_MARGIN_RATIO:
            structural_findings.append(
                "Retrieval p95 passed but has a thin latency margin; broad or "
                "hidden-graph customers may cross the SLO under real connector "
                "variance, larger tenants, or concurrent load."
            )
    if learning_pressure.get("late_trace_pressure"):
        structural_findings.append(
            "Late-run feedback mostly adds reader attribution trace rows rather "
            "than compact learned structures. Tighten attribution retention or "
            "add consolidation before treating repeated success as efficient."
        )
    if int(final_counts.get("negative_memory") or 0) == 0 and any(
        row.get("quality_failure_modes") for row in results
    ):
        structural_findings.append(
            "Quality failures occurred but no negative memory was learned; the "
            "system is not yet remembering what to avoid."
        )
    structural_findings.append(
        "This harness uses scaffolded model-layer triggers, not real multi-source "
        "connector ingestion. Pair it with the mega single-company signal probe "
        "before treating the run as customer-source realistic."
    )
    structural_findings.extend(_architecture_slo_findings(architecture_slos))
    readiness = _readiness_assessment(
        cases=len(results),
        passed=passed_count,
        expected_hit_rate=expected_hit_rate,
        expected_misses=len(misses),
        retrieval_ms=retrieval_summary,
        architecture_slos=architecture_slos,
        final_counts=final_counts,
        learning_pressure=learning_pressure,
        source_realism=source_realism,
    )

    return {
        "tenant_id": str(tenant_id),
        "seed": {
            "families": len(company.family_cases),
            "models": company.total_models,
            "insert_ms": round(company.insert_ms, 3),
            "insert_ms_per_model": round(
                company.insert_ms / max(company.total_models, 1),
                6,
            ),
            "sidecars": company.sidecars,
        },
        "cases": len(results),
        "passed": passed_count,
        "expected_cases": len(expected_rows),
        "expected_misses": len(misses),
        "expected_hit_rate": expected_hit_rate,
        "retrieval_ms": retrieval_summary,
        "by_repetition": rep_summary,
        "family_deltas": family_deltas,
        "final_learned_counts": final_counts,
        "learning_pressure": learning_pressure,
        "source_realism": source_realism,
        "readiness": readiness,
        "expected_activation_reason_totals": dict(source_totals.most_common()),
        "architecture_slos": architecture_slos,
        "structural_findings": structural_findings,
    }


def _write_reports(
    *,
    run_id: str,
    summary: dict[str, Any],
    results: list[dict[str, Any]],
) -> tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = REPORT_DIR / f"incremental-feedback-loop-{run_id}.json"
    md_path = REPORT_DIR / f"incremental-feedback-loop-{run_id}.md"
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "results": results,
    }
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        "# Incremental Feedback Loop Stress",
        "",
        f"- Run id: `{run_id}`",
        f"- Tenant: `{summary['tenant_id']}`",
        f"- Models: {summary['seed']['models']}",
        f"- Cases: {summary['cases']}",
        f"- Expected hit rate: {summary['expected_hit_rate']:.3f}",
        f"- Expected misses: {summary['expected_misses']}",
        f"- Retrieval mean/p95/max ms: "
        f"{summary['retrieval_ms']['mean']:.1f} / "
        f"{summary['retrieval_ms']['p95']:.1f} / "
        f"{summary['retrieval_ms']['max']:.1f}",
        "",
        "## Readiness",
        "",
    ]
    readiness = summary.get("readiness") or {}
    lines.extend([
        f"- tier: `{readiness.get('tier', 'unknown')}`",
        f"- score: {readiness.get('score', 0)}",
        f"- blockers: {', '.join(readiness.get('blockers') or []) or 'none'}",
    ])
    for key, value in sorted((readiness.get("dimensions") or {}).items()):
        status = "pass" if value.get("passed") else "block"
        lines.append(f"- {key}: {status}")
    lines.extend([
        "",
        "## Learned Layer Counts",
        "",
    ])
    for key, value in sorted((summary.get("final_learned_counts") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Learning Pressure", ""])
    pressure = summary.get("learning_pressure") or {}
    lines.append(
        "- max_attributions_per_new_learning_signal: "
        f"{pressure.get('max_attributions_per_new_learning_signal', 0.0)}"
    )
    lines.append(
        "- late_trace_pressure: "
        f"{bool(pressure.get('late_trace_pressure'))}"
    )
    for row in pressure.get("quarters") or []:
        lines.append(
            f"- q{row['quarter']} cases {row['case_start']}-{row['case_end']}: "
            f"attr_delta={row['reader_decision_attribution_delta']}, "
            f"compact_learning_delta={row['compact_learning_delta']}, "
            f"ratio={row['attributions_per_new_learning_signal']}, "
            f"pressure={row['trace_pressure']}"
        )
    source_realism = summary.get("source_realism") or {}
    lines.extend([
        "",
        "## Source Realism",
        "",
        f"- mode: {source_realism.get('mode', 'unknown')}",
        "- multi_source_ingestion_validated: "
        f"{bool(source_realism.get('multi_source_ingestion_validated'))}",
        f"- recommended_followup: `{source_realism.get('recommended_followup', '')}`",
    ])
    lines.extend(["", "## Repetition Summary", ""])
    for rep, row in sorted((summary.get("by_repetition") or {}).items()):
        rank = row.get("mean_best_rank")
        rank_text = f"{rank:.2f}" if rank is not None else "n/a"
        lines.append(
            f"- rep {rep}: hit_rate={row['hit_rate']:.3f}, "
            f"mean_ms={row['mean_retrieval_ms']:.1f}, "
            f"mean_rank={rank_text}, "
            f"mean_expected_activated={row['mean_expected_activated']:.2f}"
        )
    lines.extend(["", "## Architecture SLOs", ""])
    for key, slo in sorted((summary.get("architecture_slos") or {}).items()):
        status = "pass" if slo.get("passed") else "fail"
        lines.append(
            f"- {key}: {status} value={slo['value']} "
            f"threshold={slo['threshold']}"
        )
    lines.extend(["", "## Structural Findings", ""])
    for finding in summary.get("structural_findings") or ["No structural findings flagged."]:
        lines.append(f"- {finding}")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


async def run(args: argparse.Namespace) -> int:
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tenant_id = uuid7()
    pool = await asyncpg.create_pool(
        args.database_url,
        min_size=1,
        max_size=max(4, args.pool_size),
        command_timeout=240,
    )
    try:
        if args.apply_migrations:
            await _ensure_migrations(pool)
        print(
            json.dumps({
                "event": "seed_start",
                "run_id": run_id,
                "tenant_id": str(tenant_id),
                "families": args.families,
                "target_models": args.models,
            }, sort_keys=True),
            flush=True,
        )
        company = await _seed_company(
            pool,
            tenant_id=tenant_id,
            families=args.families,
            total_models=args.models,
        )
        print(
            json.dumps({
                "event": "seed_complete",
                "tenant_id": str(tenant_id),
                "models": company.total_models,
                "insert_ms": round(company.insert_ms, 3),
                "sidecars": company.sidecars,
            }, sort_keys=True),
            flush=True,
        )

        results: list[dict[str, Any]] = []
        for step in range(args.cases):
            family = company.family_cases[step % len(company.family_cases)]
            repetition = (step // len(company.family_cases)) + 1
            try:
                result = await _run_one_case(
                    pool,
                    step_index=step + 1,
                    case=family,
                    repetition=repetition,
                    hard_followups=args.hard_followups,
                )
            except Exception as exc:  # noqa: BLE001
                result = {
                    "case_index": step + 1,
                    "family_index": family.index,
                    "family": family.name,
                    "archetype": family.archetype.key,
                    "repetition": repetition,
                    "passed": False,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
                print(json.dumps(result, sort_keys=True), flush=True)
            results.append(result)

        summary = _summarize(
            tenant_id=tenant_id,
            company=company,
            results=results,
        )
        json_path, md_path = _write_reports(
            run_id=run_id,
            summary=summary,
            results=results,
        )
        print(
            json.dumps({
                "event": "run_complete",
                "run_id": run_id,
                "json_report": str(json_path),
                "markdown_report": str(md_path),
                "summary": summary,
            }, sort_keys=True),
            flush=True,
        )
        return 0
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run incremental retrieval/model feedback-loop stress cases.",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL") or LOCAL_DATABASE_URL,
    )
    parser.add_argument("--cases", type=int, default=150)
    parser.add_argument("--families", type=int, default=30)
    parser.add_argument("--models", type=int, default=8200)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--pool-size", type=int, default=4)
    parser.add_argument(
        "--no-apply-migrations",
        action="store_false",
        dest="apply_migrations",
    )
    parser.add_argument(
        "--easy-followups",
        action="store_false",
        dest="hard_followups",
        help="Reuse target-like deterministic embeddings on follow-up cases.",
    )
    parser.set_defaults(apply_migrations=True, hard_followups=True)
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
