#!/usr/bin/env python3
"""Run a 100-signal real-LLM end-to-end model-layer evaluation.

This is the expensive, production-path version of the generated company
probe:

  * materialize one realistic company tenant,
  * inject 100 diverse signals through the uniform ingestion path,
  * let ingestion enqueue the real T1 triggers,
  * drain Think with the configured live LLM provider,
  * write per-signal model-effect reports.

Reports land under tests/real_llm/reports/runs/real-llm-100-e2e-*/.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("COMPANY_OS_ENV", "test")

import asyncpg
from dotenv import load_dotenv

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.shared.migrations import apply_migrations_dir
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.db_bootstrap import _register_codecs
from services.synthetic.core import SyntheticSignal, inject
from tests.real_llm.infrastructure.scenario_loader import (
    Scenario,
    _resolve_actor_ref,
    materialize,
)

from scripts.run_1000_signal_model_layer_probe import (
    COMPANY_NAME,
    build_scenario,
    collect_model_layer_report,
    drain_post_commit_actions,
    _build_cached_provider,
    _insert_extra_aliases,
    _jsonable,
    _record_to_dict,
    _render_markdown,
    _resolve_entities_hint,
    _write_json,
    _write_jsonl,
)


load_dotenv(REPO_ROOT / ".env", override=False)


@dataclass(frozen=True)
class ProbeConfig:
    signals: int = 100
    think_timeout: int = 7200
    post_commit_timeout: int = 900
    pool_max_size: int = 8
    progress_every: int = 10
    min_model_effect_cases: int = 70
    max_median_retrieved_models: int = 60
    run_id: str | None = None
    report_root: Path = (
        REPO_ROOT / "tests" / "real_llm" / "reports" / "runs"
    )


async def run_probe(config: ProbeConfig) -> dict[str, Any]:
    if config.signals <= 0:
        raise ValueError("signals must be positive")
    if config.min_model_effect_cases < 0:
        raise ValueError("min_model_effect_cases cannot be negative")

    run_id = config.run_id or (
        "100-signal-live-"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    report_dir = config.report_root / f"real-llm-100-e2e-{run_id}"
    scenario = build_scenario(config.signals, namespace=run_id)

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")

    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=config.pool_max_size,
        init=_register_codecs,
    )
    embedder = OllamaClient(OllamaConfig.from_env())
    started = time.monotonic()
    think_status = "not_run"
    post_commit_status: dict[str, int] = {}
    observation_ids: list[UUID] = []
    signal_manifest: list[dict[str, Any]] = []

    try:
        async with pool.acquire() as conn:
            await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")

        await materialize(scenario, pool=pool)
        if scenario.tenant_id is None:
            raise RuntimeError("scenario materialization did not set tenant_id")

        actor_repo = ActorRepo(pool)
        alias_repo = EntityAliasRepo(pool)
        await _insert_extra_aliases(scenario, alias_repo)

        observation_ids, signal_manifest = await inject_live_signal_cases(
            scenario,
            pool=pool,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
            run_id=run_id,
            progress_every=config.progress_every,
        )

        provider = _build_cached_provider()
        try:
            t1_processing = await run_signal_t1_triggers_until_complete(
                scenario.tenant_id,
                pool=pool,
                provider=provider,
                observation_ids=observation_ids,
                timeout_seconds=config.think_timeout,
            )
            think_status = "signal_t1_drained"
        except TimeoutError as exc:
            t1_processing = {"status": "timeout", "error": str(exc)}
            think_status = f"timeout: {exc}"

        post_commit_status = await drain_post_commit_actions(
            pool,
            tenant_id=scenario.tenant_id,
            timeout_seconds=config.post_commit_timeout,
        )

        summary = await collect_model_layer_report(
            pool,
            tenant_id=scenario.tenant_id,
            run_id=run_id,
            report_dir=report_dir,
            scenario=scenario,
            observation_ids=observation_ids,
            think_status=think_status,
            elapsed_seconds=time.monotonic() - started,
        )
        case_rows = await collect_signal_case_results(
            pool,
            tenant_id=scenario.tenant_id,
            observation_ids=observation_ids,
            manifest=signal_manifest,
        )
        case_summary = summarize_case_results(
            case_rows,
            min_model_effect_cases=config.min_model_effect_cases,
            max_median_retrieved_models=config.max_median_retrieved_models,
        )
        summary["case_summary"] = case_summary
        summary["post_commit_status"] = post_commit_status
        summary["signal_t1_processing"] = t1_processing

        report_dir.mkdir(parents=True, exist_ok=True)
        _write_json(report_dir / "run_summary.json", summary)
        _write_json(report_dir / "signal_cases_summary.json", case_summary)
        _write_jsonl(report_dir / "signal_cases.jsonl", case_rows)
        _write_jsonl(report_dir / "signal_manifest.jsonl", signal_manifest)
        (report_dir / "model_layer_summary.md").write_text(
            _render_case_markdown(summary, case_summary)
        )

        validate_summary_or_raise(summary, case_summary)
        return summary
    finally:
        await embedder.close()
        await pool.close()


async def inject_live_signal_cases(
    scenario: Scenario,
    *,
    pool: asyncpg.Pool,
    actor_repo: ActorRepo,
    alias_repo: EntityAliasRepo,
    embedder: OllamaClient,
    run_id: str,
    progress_every: int,
) -> tuple[list[UUID], list[dict[str, Any]]]:
    """Inject generated signals and preserve the product T1 enqueue."""
    if scenario.tenant_id is None:
        raise RuntimeError("scenario must be materialized")
    base = scenario.base_time or datetime.now(timezone.utc)
    all_signals = [
        signal
        for sequence in scenario.signal_sequences.values()
        for signal in sequence
    ]
    observation_ids: list[UUID] = []
    manifest: list[dict[str, Any]] = []
    started = time.monotonic()

    for index, signal_def in enumerate(all_signals, start=1):
        content_text = str(signal_def.get("content") or signal_def.get("text") or "")
        content_dict = dict(signal_def.get("content_dict") or {})
        content_dict.setdefault("text", content_text)
        occurred_at = base + timedelta(
            minutes=float(signal_def.get("delay_minutes", 0))
        )
        signal = SyntheticSignal(
            source_channel=str(signal_def["channel"]),
            content_text=content_text,
            content=content_dict,
            occurred_at=occurred_at,
            source_actor_ref=_resolve_actor_ref(signal_def.get("actor"), scenario),
            external_id=f"{run_id}:{signal_def.get('external_id') or index}",
            entities_hint=_resolve_entities_hint(scenario, signal_def),
            trust_tier=signal_def.get("trust_tier"),
            kind=signal_def.get("kind", "signal"),
            scenario_id=scenario.scenario_id,
            run_id=run_id,
        )
        result = await inject(
            signal,
            scenario.tenant_id,
            pool=pool,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
            skip_t1_enqueue=False,
        )
        observation_id = result.observation.id
        observation_ids.append(observation_id)
        manifest.append(
            {
                "case_index": index,
                "case_id": f"{index:03d}",
                "observation_id": str(observation_id),
                "trigger_queue_id": (
                    str(result.trigger_queue_id)
                    if result.trigger_queue_id is not None
                    else None
                ),
                "family": content_dict.get("family"),
                "customer": content_dict.get("customer_name"),
                "secondary_customer": content_dict.get("secondary_customer_name"),
                "commitment": content_dict.get("commitment_title"),
                "goal": content_dict.get("goal_title"),
                "decision": content_dict.get("decision_title"),
                "channel": signal.source_channel,
                "trust_tier": signal.trust_tier,
                "actor": signal_def.get("actor"),
                "occurred_at": occurred_at.isoformat(),
                "content": content_text,
                "entities_hint_count": len(signal.entities_hint),
                "deduped": result.deduped,
            }
        )
        if progress_every and index % progress_every == 0:
            elapsed = time.monotonic() - started
            print(
                f"injected {index}/{len(all_signals)} live-enqueued cases "
                f"({elapsed:.1f}s elapsed)",
                flush=True,
            )
    return observation_ids, manifest


async def run_signal_t1_triggers_until_complete(
    tenant_id: UUID,
    *,
    pool: asyncpg.Pool,
    provider: Any,
    observation_ids: list[UUID],
    timeout_seconds: int,
) -> dict[str, Any]:
    """Process only the signal-arrival T1 rows for this probe.

    The production worker prioritizes latent-relationship T4 follow-ups ahead
    of T1 rows. That is right for the live system, but it makes a bounded
    100-signal retrieval/model evaluation burn tokens on follow-up topology
    loops instead of the requested signal cases. This helper still uses the
    production ThinkWorker `_process_trigger` path; it simply selects the
    exact T1 rows created by ingestion.
    """
    from services.think.worker import ThinkWorker, WorkerConfig

    cfg = WorkerConfig.from_env()
    cfg.poll_interval_s = 0.05
    cfg.tenant_filter = tenant_id
    worker = ThinkWorker(pool=pool, config=cfg, llm_provider=provider)

    async def _noop_promote() -> None:
        return None

    worker._promote_reeval_rows = _noop_promote  # type: ignore[assignment]
    deadline = time.monotonic() + timeout_seconds
    processed = 0
    last_pending = len(observation_ids)
    while True:
        async with pool.acquire() as conn:
            pending = await conn.fetch(
                """
                SELECT id, tenant_id, trigger_kind, trigger_subkind,
                       observation_id, model_id, payload, attempts
                FROM think_trigger_queue
                WHERE tenant_id = $1
                  AND trigger_kind = 'T1'
                  AND trigger_subkind = 'event_arrival'
                  AND observation_id = ANY($2::uuid[])
                  AND completed_at IS NULL
                  AND attempts < $3
                ORDER BY enqueued_at ASC, id ASC
                """,
                tenant_id,
                observation_ids,
                cfg.trigger_max_attempts,
            )
        last_pending = len(pending)
        if not pending:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError(
                "signal T1 triggers did not complete within "
                f"{timeout_seconds}s; {last_pending} row(s) still pending"
            )
        for row in pending:
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "signal T1 trigger processing exceeded "
                    f"{timeout_seconds}s"
                )
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE think_trigger_queue
                    SET locked_by = $2, locked_at = now()
                    WHERE id = $1
                      AND completed_at IS NULL
                    """,
                    row["id"],
                    cfg.worker_id,
                )
            await worker._process_trigger(row)  # type: ignore[attr-defined]
            processed += 1
    return {
        "status": "completed",
        "processed_attempts": processed,
        "pending_signal_t1": last_pending,
    }


async def collect_signal_case_results(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    observation_ids: list[UUID],
    manifest: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    manifest_by_obs = {item["observation_id"]: item for item in manifest}
    async with pool.acquire() as conn:
        trigger_rows = await conn.fetch(
            """
            SELECT
              q.id AS trigger_id,
              q.observation_id,
              q.trigger_kind,
              q.trigger_subkind,
              q.completed_at,
              q.attempts,
              NULL::text AS last_error,
              r.id AS run_id,
              r.status AS run_status,
              r.error AS run_error,
              r.retrieval_model_count,
              r.retrieval_observation_count,
              r.llm_latency_ms,
              r.validation_error_count,
              r.ops_applied
            FROM think_trigger_queue q
            LEFT JOIN LATERAL (
              SELECT *
              FROM think_runs r
              WHERE r.trigger_id = q.id
              ORDER BY r.started_at DESC
              LIMIT 1
            ) r ON true
            WHERE q.tenant_id = $1
              AND q.trigger_kind = 'T1'
              AND q.observation_id = ANY($2::uuid[])
            ORDER BY q.enqueued_at ASC, q.id ASC
            """,
            tenant_id,
            observation_ids,
        )
        triggers_by_obs = {
            str(row["observation_id"]): row for row in trigger_rows
        }

        rows: list[dict[str, Any]] = []
        for observation_id in observation_ids:
            observation_key = str(observation_id)
            manifest_row = manifest_by_obs.get(observation_key, {})
            trigger_row = triggers_by_obs.get(observation_key)
            direct_models = await conn.fetch(
                """
                SELECT id, proposition_kind, status, confidence,
                       activation, "natural", scope_entities, scope_actors,
                       supporting_event_ids, supporting_model_ids, created_at
                FROM models
                WHERE tenant_id = $1
                  AND (
                    born_from_event_id = $2
                    OR $2 = ANY(COALESCE(supporting_event_ids, '{}'))
                  )
                ORDER BY created_at ASC
                """,
                tenant_id,
                observation_id,
            )
            state_change_count = await conn.fetchval(
                """
                SELECT COUNT(*)::bigint
                FROM observations
                WHERE tenant_id = $1
                  AND kind = 'state_change'
                  AND cause_id = $2
                """,
                tenant_id,
                observation_id,
            )
            if trigger_row is None:
                rows.append(
                    {
                        **manifest_row,
                        "trigger_found": False,
                        "run_status": None,
                        "model_effect": False,
                        "diagnosis": "no T1 trigger found for observation",
                    }
                )
                continue

            ops = trigger_row["ops_applied"] or {}
            if not isinstance(ops, dict):
                ops = {}
            claim_ops = list(ops.get("claim_ops") or [])
            edge_ops = list(ops.get("edge_ops") or [])
            act_ops = list(ops.get("act_ops") or [])
            resource_ops = list(ops.get("resource_ops") or [])
            applied_model_ids = [
                str(mid) for mid in (ops.get("applied_model_ids") or [])
            ]
            inserted_model_ids = [
                str(item.get("model_id"))
                for item in claim_ops
                if item.get("op") == "insert" and item.get("model_id")
            ]
            updated_model_ids = [
                str(item.get("model_id"))
                for item in claim_ops
                if item.get("op") == "update" and item.get("model_id")
            ]
            skipped_claim_ops = [
                item for item in claim_ops if item.get("op") == "skip"
            ]
            direct_model_rows = [_record_to_dict(row) for row in direct_models]
            context_use = ops.get("context_use") or {}
            model_effect = bool(
                inserted_model_ids
                or updated_model_ids
                or direct_model_rows
                or int(state_change_count or 0) > 0
            )
            rows.append(
                {
                    **manifest_row,
                    "trigger_found": True,
                    "trigger_id": str(trigger_row["trigger_id"]),
                    "trigger_kind": trigger_row["trigger_kind"],
                    "trigger_subkind": trigger_row["trigger_subkind"],
                    "trigger_completed": trigger_row["completed_at"] is not None,
                    "trigger_attempts": int(trigger_row["attempts"] or 0),
                    "trigger_last_error": trigger_row["last_error"],
                    "run_id": (
                        str(trigger_row["run_id"])
                        if trigger_row["run_id"] is not None
                        else None
                    ),
                    "run_status": trigger_row["run_status"],
                    "run_error": trigger_row["run_error"],
                    "retrieval_model_count": int(
                        trigger_row["retrieval_model_count"] or 0
                    ),
                    "retrieval_observation_count": int(
                        trigger_row["retrieval_observation_count"] or 0
                    ),
                    "llm_latency_ms": trigger_row["llm_latency_ms"],
                    "validation_error_count": int(
                        trigger_row["validation_error_count"] or 0
                    ),
                    "claim_ops_count": len(claim_ops),
                    "edge_ops_count": len(edge_ops),
                    "act_ops_count": len(act_ops),
                    "resource_ops_count": len(resource_ops),
                    "claim_insert_count": len(inserted_model_ids),
                    "claim_update_count": len(updated_model_ids),
                    "claim_skip_count": len(skipped_claim_ops),
                    "inserted_model_ids": inserted_model_ids,
                    "updated_model_ids": updated_model_ids,
                    "applied_model_ids": applied_model_ids,
                    "direct_model_count": len(direct_model_rows),
                    "direct_models": direct_model_rows[:5],
                    "state_changes_from_signal": int(state_change_count or 0),
                    "model_effect": model_effect,
                    "context_use_grade": context_use.get("context_use_grade"),
                    "selected_context_used": context_use.get(
                        "selected_context_used"
                    ),
                    "selected_context_reference_ratio": context_use.get(
                        "selected_context_reference_ratio"
                    ),
                    "graph_selected_reference_ratio": context_use.get(
                        "graph_selected_reference_ratio"
                    ),
                    "reconcile_summary": ops.get("reconcile_summary") or {},
                    "quality_summary": ops.get("quality_summary") or {},
                    "split_summary": ops.get("split_summary") or {},
                    "apply_dropped_op_count": int(
                        ops.get("apply_dropped_op_count") or 0
                    ),
                    "reasoning_trace": ops.get("reasoning_trace"),
                }
            )
    return rows


def summarize_case_results(
    case_rows: list[dict[str, Any]],
    *,
    min_model_effect_cases: int,
    max_median_retrieved_models: int,
) -> dict[str, Any]:
    by_family = Counter(str(row.get("family") or "<none>") for row in case_rows)
    effect_by_family = Counter(
        str(row.get("family") or "<none>")
        for row in case_rows
        if row.get("model_effect")
    )
    status_counts = Counter(str(row.get("run_status") or "<none>") for row in case_rows)
    context_grades = Counter(
        str(row.get("context_use_grade") or "<none>") for row in case_rows
    )
    failed_cases = [
        _case_failure(row)
        for row in case_rows
        if row.get("run_status") not in {"success", "skipped_idempotent"}
    ]
    missing_effect_cases = [
        {
            "case_id": row.get("case_id"),
            "family": row.get("family"),
            "customer": row.get("customer"),
            "run_status": row.get("run_status"),
            "claim_ops_count": row.get("claim_ops_count"),
            "claim_skip_count": row.get("claim_skip_count"),
            "validation_error_count": row.get("validation_error_count"),
            "content": str(row.get("content") or "")[:300],
        }
        for row in case_rows
        if not row.get("model_effect")
    ]
    model_effect_cases = sum(1 for row in case_rows if row.get("model_effect"))
    retrieval_counts = [
        int(row.get("retrieval_model_count") or 0)
        for row in case_rows
    ]
    selected_ratios = [
        float(row.get("selected_context_reference_ratio") or 0.0)
        for row in case_rows
    ]
    retrieval_efficiency = _efficiency_summary(
        retrieval_counts,
        selected_ratios=selected_ratios,
        max_median_retrieved_models=max_median_retrieved_models,
    )
    return {
        "cases_total": len(case_rows),
        "trigger_found_cases": sum(1 for row in case_rows if row.get("trigger_found")),
        "trigger_completed_cases": sum(
            1 for row in case_rows if row.get("trigger_completed")
        ),
        "model_effect_cases": model_effect_cases,
        "min_model_effect_cases": min_model_effect_cases,
        "failed_case_count": len(failed_cases),
        "failed_cases": failed_cases[:25],
        "missing_model_effect_count": len(missing_effect_cases),
        "missing_model_effect_cases": missing_effect_cases[:25],
        "run_status_distribution": dict(status_counts),
        "case_family_distribution": dict(by_family),
        "model_effect_by_family": dict(effect_by_family),
        "context_use_grade_distribution": dict(context_grades),
        "claim_insert_total": sum(int(row.get("claim_insert_count") or 0) for row in case_rows),
        "claim_update_total": sum(int(row.get("claim_update_count") or 0) for row in case_rows),
        "edge_ops_total": sum(int(row.get("edge_ops_count") or 0) for row in case_rows),
        "act_ops_total": sum(int(row.get("act_ops_count") or 0) for row in case_rows),
        "resource_ops_total": sum(int(row.get("resource_ops_count") or 0) for row in case_rows),
        "state_changes_from_signals_total": sum(
            int(row.get("state_changes_from_signal") or 0) for row in case_rows
        ),
        "selected_context_used_cases": sum(
            1 for row in case_rows if row.get("selected_context_used")
        ),
        "retrieval_efficiency": retrieval_efficiency,
    }


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    pos = (len(ordered) - 1) * pct
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    frac = pos - lo
    return float(ordered[lo] * (1.0 - frac) + ordered[hi] * frac)


def _efficiency_summary(
    retrieval_counts: list[int],
    *,
    selected_ratios: list[float],
    max_median_retrieved_models: int,
) -> dict[str, Any]:
    retrieved = [float(value) for value in retrieval_counts]
    median = _percentile(retrieved, 0.5)
    return {
        "retrieved_model_count": {
            "min": min(retrieval_counts) if retrieval_counts else 0,
            "p50": median,
            "p90": _percentile(retrieved, 0.9),
            "max": max(retrieval_counts) if retrieval_counts else 0,
            "avg": (
                sum(retrieval_counts) / len(retrieval_counts)
                if retrieval_counts else 0.0
            ),
        },
        "selected_context_reference_ratio": {
            "p50": _percentile(selected_ratios, 0.5),
            "p90": _percentile(selected_ratios, 0.9),
        },
        "max_median_retrieved_models": max_median_retrieved_models,
        "passes": median <= max_median_retrieved_models,
    }


def validate_summary_or_raise(
    summary: dict[str, Any],
    case_summary: dict[str, Any],
) -> None:
    failures: list[str] = []
    expected = int(summary.get("signal_count") or 0)
    if int(summary.get("observation_count") or 0) != expected:
        failures.append(
            "observation_count mismatch: "
            f"{summary.get('observation_count')} != {expected}"
        )
    if summary.get("think_status") not in {"drained", "signal_t1_drained"}:
        failures.append(
            f"signal T1 processing did not drain: {summary.get('think_status')}"
        )
    if int(summary.get("think_runs_failed") or 0) != 0:
        failures.append(f"failed think runs: {summary.get('think_runs_failed')}")
    if case_summary["trigger_found_cases"] != expected:
        failures.append(
            "not every signal had a T1 trigger: "
            f"{case_summary['trigger_found_cases']} / {expected}"
        )
    if case_summary["trigger_completed_cases"] != expected:
        failures.append(
            "not every signal trigger completed: "
            f"{case_summary['trigger_completed_cases']} / {expected}"
        )
    if case_summary["failed_case_count"]:
        failures.append(
            f"{case_summary['failed_case_count']} per-signal cases failed"
        )
    if (
        case_summary["model_effect_cases"]
        < case_summary["min_model_effect_cases"]
    ):
        failures.append(
            "model-effect coverage below threshold: "
            f"{case_summary['model_effect_cases']} < "
            f"{case_summary['min_model_effect_cases']}"
        )
    efficiency = case_summary.get("retrieval_efficiency") or {}
    if efficiency and not efficiency.get("passes", True):
        retrieved = efficiency.get("retrieved_model_count") or {}
        failures.append(
            "retrieval median above efficiency gate: "
            f"{retrieved.get('p50')} > "
            f"{efficiency.get('max_median_retrieved_models')}"
        )
    if failures:
        raise AssertionError("; ".join(failures))


def _case_failure(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "case_id": row.get("case_id"),
        "family": row.get("family"),
        "customer": row.get("customer"),
        "observation_id": row.get("observation_id"),
        "trigger_id": row.get("trigger_id"),
        "run_status": row.get("run_status"),
        "run_error": row.get("run_error"),
        "trigger_last_error": row.get("trigger_last_error"),
        "content": str(row.get("content") or "")[:300],
    }


def _render_case_markdown(
    summary: dict[str, Any],
    case_summary: dict[str, Any],
) -> str:
    base = _render_markdown(summary)
    lines = [
        base,
        "",
        "## 100-Signal Case Results",
        "",
        f"- Cases total: {case_summary['cases_total']}",
        f"- T1 triggers found: {case_summary['trigger_found_cases']}",
        f"- T1 triggers completed: {case_summary['trigger_completed_cases']}",
        f"- Cases with model effect: {case_summary['model_effect_cases']}",
        f"- Failed cases: {case_summary['failed_case_count']}",
        f"- Claim inserts: {case_summary['claim_insert_total']}",
        f"- Claim updates: {case_summary['claim_update_total']}",
        f"- Edge ops: {case_summary['edge_ops_total']}",
        f"- Act ops: {case_summary['act_ops_total']}",
        f"- Resource ops: {case_summary['resource_ops_total']}",
        f"- Signal-caused state changes: "
        f"{case_summary['state_changes_from_signals_total']}",
        f"- Retrieval median models: "
        f"{case_summary['retrieval_efficiency']['retrieved_model_count']['p50']}",
        f"- Retrieval p90 models: "
        f"{case_summary['retrieval_efficiency']['retrieved_model_count']['p90']}",
        "",
        "### Run Status Distribution",
        "",
        _markdown_table(case_summary["run_status_distribution"]),
        "",
        "### Family Distribution",
        "",
        _markdown_table(case_summary["case_family_distribution"]),
        "",
        "### Model Effect By Family",
        "",
        _markdown_table(case_summary["model_effect_by_family"]),
        "",
        "### Context Use Grades",
        "",
        _markdown_table(case_summary["context_use_grade_distribution"]),
    ]
    if case_summary["missing_model_effect_cases"]:
        lines.extend(
            [
                "",
                "### First Missing Model-Effect Cases",
                "",
                "| Case | Family | Customer | Status |",
                "| --- | --- | --- | --- |",
            ]
        )
        for row in case_summary["missing_model_effect_cases"][:10]:
            lines.append(
                f"| {row['case_id']} | {row['family']} | "
                f"{row['customer']} | {row['run_status']} |"
            )
    return "\n".join(lines) + "\n"


def _markdown_table(dist: dict[str, int]) -> str:
    if not dist:
        return "_No rows._"
    lines = ["| Key | Count |", "| --- | ---: |"]
    for key, value in sorted(dist.items()):
        lines.append(f"| {key} | {value} |")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signals", type=int, default=100)
    parser.add_argument("--think-timeout", type=int, default=7200)
    parser.add_argument("--post-commit-timeout", type=int, default=900)
    parser.add_argument("--pool-max-size", type=int, default=8)
    parser.add_argument("--progress-every", type=int, default=10)
    parser.add_argument("--min-model-effect-cases", type=int, default=70)
    parser.add_argument("--max-median-retrieved-models", type=int, default=60)
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--report-root",
        type=Path,
        default=REPO_ROOT / "tests" / "real_llm" / "reports" / "runs",
    )
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    config = ProbeConfig(
        signals=args.signals,
        think_timeout=args.think_timeout,
        post_commit_timeout=args.post_commit_timeout,
        pool_max_size=args.pool_max_size,
        progress_every=args.progress_every,
        min_model_effect_cases=args.min_model_effect_cases,
        max_median_retrieved_models=args.max_median_retrieved_models,
        run_id=args.run_id,
        report_root=args.report_root,
    )
    print(
        f"running {config.signals}-signal real-LLM E2E for {COMPANY_NAME}",
        flush=True,
    )
    summary = await run_probe(config)
    printable = {
        "tenant_id": summary["tenant_id"],
        "run_id": summary["run_id"],
        "signals": summary["signal_count"],
        "think_status": summary["think_status"],
        "think_runs_success": summary["think_runs_success"],
        "think_runs_failed": summary["think_runs_failed"],
        "pending_triggers": summary["pending_triggers"],
        "active_models": summary["active_models"],
        "model_edges": summary["model_edges"],
        "case_summary": summary["case_summary"],
        "cost": summary.get("cost"),
    }
    print(json.dumps(_jsonable(printable), indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
