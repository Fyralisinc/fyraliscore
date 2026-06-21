"""Operational quality report for Think retrieval/context use.

This module turns persisted `think_runs.ops_applied["context_use"]`
records into a compact operator report. It answers a deliberately
production-shaped question: not just whether retrieval found memory,
but whether successful Think runs used selected memory, graph memory,
and evidence observations in the diffs they committed.
"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg


_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _context_use(ops_applied: Any) -> dict[str, Any]:
    ops = _json_obj(ops_applied)
    context = ops.get("context_use")
    if not isinstance(context, dict):
        return {}
    return _augment_noop_trace_context(context, ops)


def _uuid_strings(values: Any) -> set[str]:
    if not isinstance(values, list):
        return set()
    out: set[str] = set()
    for value in values:
        try:
            out.add(str(UUID(str(value))))
        except (TypeError, ValueError):
            continue
    return out


def _augment_noop_trace_context(
    context: dict[str, Any],
    ops: dict[str, Any],
) -> dict[str, Any]:
    """Backfill trace-based no-op context use for stored run records.

    New runs persist this in `context_use` directly. Existing runs may
    have a strong no-op `reasoning_trace` that references selected memory
    but older telemetry counted only applied-op references. The report
    should classify those as justified no-ops without rewriting history.
    """
    if context.get("reasoning_trace_context_used"):
        return context
    total_ops = (
        _as_int(context.get("edge_ops_count"))
        + _as_int(context.get("relation_claim_ops_count"))
        + _as_int(context.get("relation_frame_ops_count"))
        + _as_int(context.get("ontology_gap_ops_count"))
        + _as_int(context.get("claim_ops_count"))
        + _as_int(context.get("memory_lifecycle_ops_count"))
        + _as_int(context.get("act_ops_count"))
        + _as_int(context.get("resource_ops_count"))
    )
    if total_ops != 0:
        return context
    trace = str(ops.get("reasoning_trace") or "")
    if not trace:
        return context

    trace_ids = {
        str(UUID(match.group(0)))
        for match in _UUID_RE.finditer(trace)
    }
    selected_models = _uuid_strings(context.get("selected_model_ids"))
    selected_observations = _uuid_strings(context.get("selected_observation_ids"))
    graph_models = _uuid_strings(context.get("graph_selected_model_ids"))
    trace_models = trace_ids & selected_models
    trace_observations = trace_ids & selected_observations
    if not trace_models and not trace_observations:
        return context

    augmented = dict(context)
    referenced_models = _uuid_strings(context.get("referenced_model_ids"))
    referenced_observations = _uuid_strings(context.get("referenced_observation_ids"))
    referenced_models |= trace_models
    referenced_observations |= trace_observations
    graph_referenced = referenced_models & graph_models
    selected_ref_count = len(referenced_models & selected_models) + len(
        referenced_observations & selected_observations
    )
    selected_count = _as_int(
        context.get("selected_context_count"),
        len(selected_models) + len(selected_observations),
    )
    graph_count = _as_int(context.get("graph_selected_model_count"), len(graph_models))
    augmented.update(
        {
            "context_use_grade": "justified_noop_context_used",
            "selected_context_used": True,
            "reasoning_trace_context_used": True,
            "trace_referenced_model_ids": sorted(trace_models),
            "trace_referenced_observation_ids": sorted(trace_observations),
            "referenced_model_ids": sorted(referenced_models),
            "referenced_observation_ids": sorted(referenced_observations),
            "selected_model_reference_count": len(referenced_models & selected_models),
            "selected_observation_reference_count": len(
                referenced_observations & selected_observations
            ),
            "selected_context_reference_count": selected_ref_count,
            "selected_context_reference_ratio": (
                selected_ref_count / selected_count if selected_count else 1.0
            ),
            "graph_context_used": bool(graph_referenced),
            "graph_selected_reference_count": len(graph_referenced),
            "graph_selected_reference_ratio": (
                len(graph_referenced) / graph_count if graph_count else 1.0
            ),
            "model_context_used": bool(referenced_models & selected_models),
            "observation_context_used": bool(
                referenced_observations & selected_observations
            ),
            "unused_selected_model_ids": sorted(selected_models - referenced_models),
            "unused_graph_model_ids": sorted(graph_models - referenced_models),
            "unused_selected_observation_ids": sorted(
                selected_observations - referenced_observations
            ),
        }
    )
    return augmented


def _as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _graph_relation_contract_satisfied(context: dict[str, Any]) -> bool:
    if "graph_relation_contract_satisfied" in context:
        return bool(context.get("graph_relation_contract_satisfied"))
    return (
        _as_int(context.get("graph_relation_op_count")) > 0
        or _as_int(context.get("graph_non_relation_op_count")) > 0
        or bool(context.get("graph_no_edge_rationale_present"))
        or bool(context.get("reasoning_trace_context_used"))
    )


def _iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) if value is not None else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    return value


def _run_summary(row: asyncpg.Record, context: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": str(row["id"]),
        "trigger_id": str(row["trigger_id"]),
        "trigger_kind": row["trigger_kind"],
        "started_at": _iso(row["started_at"]),
        "status": row["status"],
        "context_use_grade": context.get("context_use_grade"),
        "selected_context_reference_ratio": _as_float(
            context.get("selected_context_reference_ratio")
        ),
        "selected_model_reference_ratio": _as_float(
            context.get("selected_model_reference_ratio")
        ),
        "graph_selected_reference_ratio": _as_float(
            context.get("graph_selected_reference_ratio")
        ),
        "selected_context_count": _as_int(
            context.get("selected_context_count")
        ),
        "graph_selected_model_count": _as_int(
            context.get("graph_selected_model_count")
        ),
        "edge_ops_count": _as_int(context.get("edge_ops_count")),
        "relation_claim_ops_count": _as_int(
            context.get("relation_claim_ops_count")
        ),
        "relation_frame_ops_count": _as_int(
            context.get("relation_frame_ops_count")
        ),
        "ontology_gap_ops_count": _as_int(context.get("ontology_gap_ops_count")),
        "claim_ops_count": _as_int(context.get("claim_ops_count")),
        "memory_lifecycle_ops_count": _as_int(
            context.get("memory_lifecycle_ops_count")
        ),
        "act_ops_count": _as_int(context.get("act_ops_count")),
        "retrieval_model_count": row["retrieval_model_count"],
        "retrieval_observation_count": row["retrieval_observation_count"],
        "llm_latency_ms": row["llm_latency_ms"],
        "validation_error_count": row["validation_error_count"],
    }


def _add_counter_values(
    counter: Counter[str],
    values: list[Any],
) -> None:
    for value in values:
        if value is None:
            continue
        counter[str(value)] += 1


def _flags_for_context(
    context: dict[str, Any],
    *,
    low_context_ratio: float,
) -> list[str]:
    grade = str(context.get("context_use_grade") or "unknown")
    selected_ratio = _as_float(
        context.get("selected_context_reference_ratio")
    )
    graph_selected = _as_int(context.get("graph_selected_model_count"))
    edge_ops = _as_int(context.get("edge_ops_count"))
    relation_claim_ops = _as_int(context.get("relation_claim_ops_count"))
    relation_frame_ops = _as_int(context.get("relation_frame_ops_count"))
    ontology_gap_ops = _as_int(context.get("ontology_gap_ops_count"))
    claim_ops = _as_int(context.get("claim_ops_count"))
    memory_lifecycle_ops = _as_int(context.get("memory_lifecycle_ops_count"))
    act_ops = _as_int(context.get("act_ops_count"))
    resource_ops = _as_int(context.get("resource_ops_count"))
    total_ops = (
        relation_claim_ops
        + relation_frame_ops
        + edge_ops
        + ontology_gap_ops
        + claim_ops
        + memory_lifecycle_ops
        + act_ops
        + resource_ops
    )
    selected_count = _as_int(context.get("selected_context_count"))
    graph_used = bool(context.get("graph_context_used"))
    trace_used = bool(context.get("reasoning_trace_context_used"))

    flags: list[str] = []
    if total_ops == 0 and selected_count > 0 and not trace_used:
        flags.append("no_ops_with_selected_context")
    if grade == "unused_selected_context":
        flags.append("unused_selected_context")
    if graph_selected > 0 and not graph_used and total_ops > 0:
        flags.append("graph_context_ignored")
    if (
        graph_selected > 0
        and edge_ops == 0
        and ontology_gap_ops == 0
        and total_ops > 0
        and not _graph_relation_contract_satisfied(context)
    ):
        flags.append("graph_context_without_edge_ops")
    if (
        selected_count >= 5
        and selected_ratio < low_context_ratio
        and total_ops > 0
        and not graph_used
    ):
        flags.append("low_selected_context_use")
    return flags


def _gate(
    name: str,
    *,
    value: float,
    warn_at: float,
    fail_at: float,
    direction: str,
    detail: str,
) -> dict[str, Any]:
    if direction == "min":
        status = "fail" if value < fail_at else "warn" if value < warn_at else "pass"
    else:
        status = "fail" if value > fail_at else "warn" if value > warn_at else "pass"
    return {
        "name": name,
        "status": status,
        "value": value,
        "warn_at": warn_at,
        "fail_at": fail_at,
        "direction": direction,
        "detail": detail,
    }


def _quality_gates(summary: dict[str, Any]) -> dict[str, Any]:
    successful = int(summary.get("successful_runs") or 0)
    successful_with_context = int(summary.get("successful_runs_with_context_use") or 0)
    graph_applicable = int(summary.get("graph_applicable_successful_runs") or 0)
    flagged = int(summary.get("flagged_successful_runs") or 0)
    unused = int(summary.get("unused_selected_context_runs") or 0)
    graph_ignored = int(summary.get("graph_context_ignored_runs") or 0)
    graph_relation_failed = int(
        summary.get("graph_relation_contract_failed_runs") or 0
    )

    flagged_rate = flagged / successful if successful else 0.0
    unused_rate = unused / successful if successful else 0.0
    graph_ignored_rate = (
        graph_ignored / graph_applicable if graph_applicable else 0.0
    )
    graph_relation_failed_rate = (
        graph_relation_failed / graph_applicable if graph_applicable else 0.0
    )
    gates = [
        _gate(
            "context_use_coverage",
            value=(successful_with_context / successful if successful else 1.0),
            warn_at=0.98,
            fail_at=0.95,
            direction="min",
            detail="Successful runs should persist context_use telemetry.",
        ),
        _gate(
            "flagged_success_rate",
            value=flagged_rate,
            warn_at=0.10,
            fail_at=0.25,
            direction="max",
            detail="Successful Think runs should rarely need quality review.",
        ),
        _gate(
            "unused_selected_context_rate",
            value=unused_rate,
            warn_at=0.02,
            fail_at=0.10,
            direction="max",
            detail="Selected prompt context should usually affect the diff.",
        ),
        _gate(
            "graph_context_ignored_rate",
            value=graph_ignored_rate,
            warn_at=0.10,
            fail_at=0.25,
            direction="max",
            detail="Graph-selected context should be used in graph-applicable runs.",
        ),
        _gate(
            "graph_relation_contract_failed_rate",
            value=graph_relation_failed_rate,
            warn_at=0.15,
            fail_at=0.35,
            direction="max",
            detail=(
                "Graph-selected context should produce edge/ontology structure, "
                "a stronger model mutation, or an explicit no-edge rationale."
            ),
        ),
    ]
    if any(g["status"] == "fail" for g in gates):
        overall = "fail"
    elif any(g["status"] == "warn" for g in gates):
        overall = "warn"
    else:
        overall = "pass"
    return {"overall_status": overall, "gates": gates}


async def build_think_quality_report(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    since_hours: int = 24,
    limit: int = 200,
    low_context_ratio: float = 0.20,
) -> dict[str, Any]:
    """Build a JSON-safe Think/Retrieval quality report for one tenant."""
    since_hours = max(1, min(int(since_hours), 24 * 30))
    limit = max(1, min(int(limit), 1000))
    low_context_ratio = max(0.0, min(float(low_context_ratio), 1.0))

    rows = await conn.fetch(
        """
        SELECT id, trigger_id, trigger_kind, started_at, status,
               retrieval_model_count, retrieval_observation_count,
               llm_latency_ms, validation_error_count, ops_applied
        FROM think_runs
        WHERE tenant_id = $1
          AND started_at >= now() - ($2::int * interval '1 hour')
        ORDER BY started_at DESC
        LIMIT $3
        """,
        tenant_id,
        since_hours,
        limit,
    )

    cost = await conn.fetchrow(
        """
        SELECT count(*)::int AS rows,
               COALESCE(sum(llm_calls_count), 0)::int AS llm_calls,
               COALESCE(sum(llm_input_tokens_total), 0)::int AS input_tokens,
               COALESCE(sum(llm_output_tokens_total), 0)::int AS output_tokens,
               COALESCE(sum(llm_cost_usd), 0)::float8 AS cost_usd,
               COALESCE(avg(latency_total_ms), 0)::float8 AS avg_latency_ms
        FROM think_run_costs
        WHERE tenant_id = $1
          AND computed_at >= now() - ($2::int * interval '1 hour')
        """,
        tenant_id,
        since_hours,
    )

    grade_counts: Counter[str] = Counter()
    trigger_grade_counts: dict[str, Counter[str]] = defaultdict(Counter)
    ignored_models: Counter[str] = Counter()
    ignored_graph_models: Counter[str] = Counter()
    ignored_observations: Counter[str] = Counter()
    bad_runs: list[dict[str, Any]] = []
    ratios: list[float] = []
    graph_ratios: list[float] = []

    successful_runs = 0
    successful_runs_with_context_use = 0
    successful_missing_context_use = 0
    runs_with_context_use = 0
    missing_context_use = 0
    graph_applicable_successful_runs = 0
    graph_context_ignored_runs = 0
    graph_relation_contract_failed_runs = 0
    unused_selected_context_runs = 0
    low_selected_context_runs = 0

    for row in rows:
        if row["status"] == "success":
            successful_runs += 1
        context = _context_use(row["ops_applied"])
        if not context:
            missing_context_use += 1
            if row["status"] == "success":
                successful_missing_context_use += 1
                summary = _run_summary(row, {})
                summary["flags"] = ["missing_context_use"]
                bad_runs.append(summary)
            continue

        runs_with_context_use += 1
        if row["status"] == "success":
            successful_runs_with_context_use += 1
        grade = str(context.get("context_use_grade") or "unknown")
        grade_counts[grade] += 1
        trigger_grade_counts[str(row["trigger_kind"])][grade] += 1
        selected_ratio = _as_float(
            context.get("selected_context_reference_ratio")
        )
        graph_ratio = _as_float(context.get("graph_selected_reference_ratio"))
        ratios.append(selected_ratio)
        graph_ratios.append(graph_ratio)

        _add_counter_values(
            ignored_models,
            _as_list(context.get("unused_selected_model_ids")),
        )
        _add_counter_values(
            ignored_graph_models,
            _as_list(context.get("unused_graph_model_ids")),
        )
        _add_counter_values(
            ignored_observations,
            _as_list(context.get("unused_selected_observation_ids")),
        )

        flags = _flags_for_context(
            context, low_context_ratio=low_context_ratio
        )
        if row["status"] == "success":
            if _as_int(context.get("graph_selected_model_count")) > 0:
                graph_applicable_successful_runs += 1
            if "graph_context_ignored" in flags:
                graph_context_ignored_runs += 1
            if "graph_context_without_edge_ops" in flags:
                graph_relation_contract_failed_runs += 1
            if "unused_selected_context" in flags:
                unused_selected_context_runs += 1
            if "low_selected_context_use" in flags:
                low_selected_context_runs += 1

        if row["status"] == "success" and flags:
            summary = _run_summary(row, context)
            summary["flags"] = flags
            bad_runs.append(summary)

    def _top(counter: Counter[str], n: int = 20) -> list[dict[str, Any]]:
        return [
            {"id": key, "count": count}
            for key, count in counter.most_common(n)
        ]

    total_runs = len(rows)
    average_ratio = sum(ratios) / len(ratios) if ratios else None
    average_graph_ratio = (
        sum(graph_ratios) / len(graph_ratios) if graph_ratios else None
    )
    summary = {
        "total_runs": total_runs,
        "successful_runs": successful_runs,
        "successful_runs_with_context_use": successful_runs_with_context_use,
        "successful_missing_context_use": successful_missing_context_use,
        "runs_with_context_use": runs_with_context_use,
        "missing_context_use": missing_context_use,
        "context_use_coverage_ratio": (
            successful_runs_with_context_use / successful_runs
            if successful_runs
            else 1.0
        ),
        "average_selected_context_reference_ratio": average_ratio,
        "average_graph_selected_reference_ratio": average_graph_ratio,
        "grade_counts": dict(grade_counts),
        "trigger_grade_counts": {
            kind: dict(counts)
            for kind, counts in sorted(trigger_grade_counts.items())
        },
        "flagged_successful_runs": len(bad_runs),
        "graph_applicable_successful_runs": graph_applicable_successful_runs,
        "graph_context_ignored_runs": graph_context_ignored_runs,
        "graph_relation_contract_failed_runs": (
            graph_relation_contract_failed_runs
        ),
        "unused_selected_context_runs": unused_selected_context_runs,
        "low_selected_context_runs": low_selected_context_runs,
    }

    return {
        "tenant_id": str(tenant_id),
        "window": {
            "since_hours": since_hours,
            "limit": limit,
            "low_context_ratio": low_context_ratio,
        },
        "summary": summary,
        "quality_gates": _quality_gates(summary),
        "cost": {
            "rows": int(cost["rows"] if cost else 0),
            "llm_calls": int(cost["llm_calls"] if cost else 0),
            "input_tokens": int(cost["input_tokens"] if cost else 0),
            "output_tokens": int(cost["output_tokens"] if cost else 0),
            "cost_usd": float(cost["cost_usd"] if cost else 0.0),
            "avg_latency_ms": float(cost["avg_latency_ms"] if cost else 0.0),
        },
        "ignored_memory": {
            "selected_models": _top(ignored_models),
            "graph_models": _top(ignored_graph_models),
            "observations": _top(ignored_observations),
        },
        "flagged_runs": bad_runs[:50],
    }


async def build_think_quality_cases(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    since_hours: int = 24,
    limit: int = 25,
    low_context_ratio: float = 0.20,
    include_artifacts: bool = True,
) -> dict[str, Any]:
    """Return replay-ready cases for recent flagged successful runs."""
    since_hours = max(1, min(int(since_hours), 24 * 30))
    limit = max(1, min(int(limit), 100))
    low_context_ratio = max(0.0, min(float(low_context_ratio), 1.0))

    rows = await conn.fetch(
        """
        SELECT id, trigger_id, trigger_kind, started_at, status,
               retrieval_model_count, retrieval_observation_count,
               llm_latency_ms, validation_error_count, ops_applied
        FROM think_runs
        WHERE tenant_id = $1
          AND status = 'success'
          AND started_at >= now() - ($2::int * interval '1 hour')
        ORDER BY started_at DESC
        LIMIT 500
        """,
        tenant_id,
        since_hours,
    )

    cases: list[dict[str, Any]] = []
    for row in rows:
        context = _context_use(row["ops_applied"])
        flags = (
            ["missing_context_use"]
            if not context
            else _flags_for_context(context, low_context_ratio=low_context_ratio)
        )
        if not flags:
            continue

        trigger = await conn.fetchrow(
            """
            SELECT id, trigger_kind, trigger_subkind, observation_id,
                   model_id, payload, enqueued_at, scheduled_for, attempts
            FROM think_trigger_queue
            WHERE id = $1 AND tenant_id = $2
            """,
            row["trigger_id"],
            tenant_id,
        )
        observation = None
        if trigger is not None and trigger["observation_id"] is not None:
            observation = await conn.fetchrow(
                """
                SELECT id, kind, source_channel, actor_id, occurred_at,
                       content_text, trust_tier, entities_mentioned
                FROM observations
                WHERE id = $1 AND tenant_id = $2
                """,
                trigger["observation_id"],
                tenant_id,
            )

        artifacts: list[dict[str, Any]] = []
        if include_artifacts:
            artifact_rows = await conn.fetch(
                """
                SELECT stage, payload, captured_at
                FROM think_run_artifacts
                WHERE run_id = $1 AND tenant_id = $2
                ORDER BY captured_at
                """,
                row["id"],
                tenant_id,
            )
            artifacts = [
                {
                    "stage": artifact["stage"],
                    "captured_at": _iso(artifact["captured_at"]),
                    "payload": _json_obj(artifact["payload"]),
                }
                for artifact in artifact_rows
            ]

        case = {
            "case_id": f"think-quality:{row['id']}",
            "flags": flags,
            "run": _run_summary(row, context),
            "context_use": context,
            "trigger": _json_safe(dict(trigger)) if trigger is not None else None,
            "observation": (
                _json_safe(dict(observation)) if observation is not None else None
            ),
            "artifacts": artifacts,
            "promotion_hint": {
                "recommended_eval": "context_use_replay",
                "expected_failure_modes": flags,
                "assertions": [
                    "selected_context_reference_ratio improves or remains justified",
                    "graph-selected Models are used when graph context is applicable",
                    "edge_ops are emitted when the useful output is relational",
                ],
            },
        }
        cases.append(case)
        if len(cases) >= limit:
            break

    return {
        "tenant_id": str(tenant_id),
        "window": {
            "since_hours": since_hours,
            "limit": limit,
            "low_context_ratio": low_context_ratio,
            "include_artifacts": include_artifacts,
        },
        "cases": cases,
    }


__all__ = ["build_think_quality_report", "build_think_quality_cases"]
