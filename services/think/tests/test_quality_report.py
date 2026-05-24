from __future__ import annotations

import json
from uuid import UUID

import pytest

from lib.shared.ids import uuid7
from services.think.quality_report import (
    build_think_quality_cases,
    build_think_quality_report,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _context(
    *,
    grade: str,
    selected_count: int = 8,
    selected_ratio: float = 0.0,
    graph_count: int = 0,
    graph_ratio: float = 1.0,
    graph_used: bool = False,
    edge_ops: int = 0,
    unused_models: list[str] | None = None,
    unused_graph: list[str] | None = None,
    unused_observations: list[str] | None = None,
) -> dict:
    return {
        "context_use_grade": grade,
        "selected_context_count": selected_count,
        "selected_context_reference_ratio": selected_ratio,
        "selected_model_reference_ratio": selected_ratio,
        "graph_selected_model_count": graph_count,
        "graph_selected_reference_ratio": graph_ratio,
        "graph_context_used": graph_used,
        "edge_ops_count": edge_ops,
        "claim_ops_count": 1,
        "act_ops_count": 0,
        "unused_selected_model_ids": unused_models or [],
        "unused_graph_model_ids": unused_graph or [],
        "unused_selected_observation_ids": unused_observations or [],
    }


async def _insert_run(
    conn,
    *,
    tenant: UUID,
    trigger_kind: str,
    status: str = "success",
    context_use: dict | None = None,
    reasoning_trace: str | None = None,
):
    run_id = uuid7()
    trigger_id = uuid7()
    ops_applied = (
        {"claim_ops": [], "edge_ops": [], "context_use": context_use}
        if context_use is not None
        else {"claim_ops": [], "edge_ops": []}
    )
    if reasoning_trace is not None:
        ops_applied["reasoning_trace"] = reasoning_trace
    await conn.execute(
        """
        INSERT INTO think_runs (
            id, tenant_id, trigger_id, trigger_kind, started_at, ended_at,
            status, retrieval_model_count, retrieval_observation_count,
            llm_latency_ms, validation_error_count, ops_applied
        )
        VALUES (
            $1, $2, $3, $4, now(), now(), $5, 12, 5, 300, 0, $6::jsonb
        )
        """,
        run_id,
        tenant,
        trigger_id,
        trigger_kind,
        status,
        json.dumps(ops_applied),
    )
    return run_id, trigger_id


async def test_quality_report_flags_context_and_graph_failures(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    ignored_model = str(uuid7())
    ignored_graph = str(uuid7())
    ignored_obs = str(uuid7())

    async with fresh_db.acquire() as conn:
        graph_run, graph_trigger = await _insert_run(
            conn,
            tenant=tenant,
            trigger_kind="T2",
            context_use=_context(
                grade="graph_context_used",
                selected_ratio=0.75,
                graph_count=3,
                graph_ratio=1.0,
                graph_used=True,
                edge_ops=1,
            ),
        )
        flagged_run, flagged_trigger = await _insert_run(
            conn,
            tenant=tenant,
            trigger_kind="T1",
            context_use=_context(
                grade="unused_selected_context",
                selected_count=9,
                selected_ratio=0.0,
                unused_models=[ignored_model],
                unused_observations=[ignored_obs],
            ),
        )
        await conn.execute(
            """
            INSERT INTO think_trigger_queue (
                id, tenant_id, trigger_kind, trigger_subkind, payload
            )
            VALUES ($1, $2, 'T1', 'event_arrival', $3::jsonb)
            """,
            flagged_trigger,
            tenant,
            json.dumps({"reason": "quality replay"}),
        )
        await conn.execute(
            """
            INSERT INTO think_run_artifacts (
                id, run_id, tenant_id, stage, payload
            )
            VALUES ($1, $2, $3, 'response', $4::jsonb)
            """,
            uuid7(),
            flagged_run,
            tenant,
            json.dumps({"raw_diff": {"claim_ops": []}}),
        )
        await _insert_run(
            conn,
            tenant=tenant,
            trigger_kind="T2",
            context_use=_context(
                grade="model_context_used",
                selected_ratio=0.10,
                graph_count=2,
                graph_ratio=0.0,
                graph_used=False,
                edge_ops=0,
                unused_graph=[ignored_graph],
            ),
        )
        justified_noop_model = str(uuid7())
        justified_noop, _ = await _insert_run(
            conn,
            tenant=tenant,
            trigger_kind="T2",
            context_use={
                **_context(
                    grade="unused_selected_context",
                    selected_count=4,
                    selected_ratio=0.0,
                    graph_count=1,
                    graph_ratio=0.0,
                    graph_used=False,
                    unused_models=[justified_noop_model],
                    unused_graph=[justified_noop_model],
                ),
                "claim_ops_count": 0,
                "selected_model_ids": [justified_noop_model],
                "graph_selected_model_ids": [justified_noop_model],
            },
            reasoning_trace=(
                f"Model {justified_noop_model} already captures this signal; "
                "empty diff is correct."
            ),
        )
        await _insert_run(conn, tenant=tenant, trigger_kind="T4")
        await _insert_run(
            conn,
            tenant=tenant,
            trigger_kind="T1",
            status="failed",
            context_use=_context(grade="observation_context_used"),
        )
        await conn.execute(
            """
            INSERT INTO think_run_costs (
                trigger_id, tenant_id, trigger_kind, llm_calls_count,
                llm_input_tokens_total, llm_output_tokens_total,
                llm_cost_usd, latency_total_ms, outcome, model_name
            )
            VALUES ($1, $2, 'T2', 2, 1000, 300, 0.42, 900, 'success', 'm')
            """,
            graph_trigger,
            tenant,
        )

        report = await build_think_quality_report(
            conn,
            tenant_id=tenant,
            since_hours=24,
            low_context_ratio=0.20,
        )

    assert report["summary"]["total_runs"] == 6
    assert report["summary"]["successful_runs"] == 5
    assert report["summary"]["runs_with_context_use"] == 5
    assert report["summary"]["missing_context_use"] == 1
    assert report["quality_gates"]["overall_status"] == "fail"
    assert report["summary"]["grade_counts"]["graph_context_used"] == 1
    assert report["summary"]["grade_counts"]["unused_selected_context"] == 1
    assert report["summary"]["grade_counts"]["justified_noop_context_used"] == 1
    assert report["summary"]["trigger_grade_counts"]["T2"]["graph_context_used"] == 1
    assert report["cost"]["llm_calls"] == 2
    assert report["cost"]["input_tokens"] == 1000
    assert report["cost"]["cost_usd"] == pytest.approx(0.42)

    flagged = {run["run_id"]: set(run["flags"]) for run in report["flagged_runs"]}
    assert str(graph_run) not in flagged
    assert str(justified_noop) not in flagged
    assert any("unused_selected_context" in flags for flags in flagged.values())
    assert any("missing_context_use" in flags for flags in flagged.values())
    assert any("graph_context_ignored" in flags for flags in flagged.values())
    assert any("graph_context_without_edge_ops" in flags for flags in flagged.values())
    assert any("low_selected_context_use" in flags for flags in flagged.values())

    ignored = report["ignored_memory"]
    assert ignored["selected_models"][0] == {"id": ignored_model, "count": 1}
    assert ignored["graph_models"][0] == {"id": ignored_graph, "count": 1}
    assert ignored["observations"][0] == {"id": ignored_obs, "count": 1}

    async with fresh_db.acquire() as conn:
        cases = await build_think_quality_cases(conn, tenant_id=tenant)

    by_id = {case["run"]["run_id"]: case for case in cases["cases"]}
    assert str(flagged_run) in by_id
    replay = by_id[str(flagged_run)]
    assert replay["case_id"] == f"think-quality:{flagged_run}"
    assert "unused_selected_context" in replay["flags"]
    assert replay["trigger"]["id"] == str(flagged_trigger)
    assert replay["artifacts"][0]["stage"] == "response"
    assert replay["promotion_hint"]["recommended_eval"] == "context_use_replay"


async def test_quality_report_empty_window_is_well_formed(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        report = await build_think_quality_report(conn, tenant_id=tenant)

    assert report["summary"]["total_runs"] == 0
    assert report["summary"]["context_use_coverage_ratio"] == 1.0
    assert report["flagged_runs"] == []
