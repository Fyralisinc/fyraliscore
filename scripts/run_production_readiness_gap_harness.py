#!/usr/bin/env python3
"""Run focused production-readiness probes for the learning feedback loop.

This harness is intentionally narrower than the planted-storyline benchmark.
It exercises the proof gaps that can remain hidden after a successful large
batch run:

* stale Model archival
* evidence attachment to existing memory
* clean no-op context-use grading
* compressed Model/graph context-use grading
* SAGE question-policy learning counters

The probes are fast and mostly deterministic, so this script is suitable as a
promotion gate alongside the slower real-LLM storyline harness.
"""
from __future__ import annotations

# The harness mutates sys.path before importing repo modules so it can be run
# directly from the command line.
# ruff: noqa: E402

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("COMPANY_OS_ENV", "test")

import asyncpg
from dotenv import load_dotenv

from lib.shared.ids import uuid7
from services.app.gateway.db_bootstrap import _register_codecs
from services.domain.models.decay import archive_decayed
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.sage.inquiry_traces.types import OutcomeEventRow
from services.reasoning.sage.topology_optimizer.optimizer import TopologyOptimizer
from services.reasoning.think.applier import apply_diff
from services.reasoning.think.context_use import summarize_context_use
from services.reasoning.think.diff_schema import ClaimOp, EdgeOp, RawDiff, ValidatedDiff
from services.reasoning.think.text_embedding import deterministic_text_embedding

load_dotenv(REPO_ROOT / ".env", override=False)

DEFAULT_REPORT_ROOT = REPO_ROOT / "tests" / "real_llm" / "reports" / "runs"


@dataclass
class GateResult:
    name: str
    passed: bool
    metrics: dict[str, Any] = field(default_factory=dict)
    details: str = ""


@dataclass
class HarnessReport:
    run_id: str
    tenant_id: str
    passed: bool
    elapsed_seconds: float
    report_dir: str
    gates: list[GateResult]


def _json_default(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=_json_default))


def _render_markdown(report: HarnessReport) -> str:
    lines = [
        "# Production Readiness Gap Harness",
        "",
        f"- Run: `{report.run_id}`",
        f"- Tenant: `{report.tenant_id}`",
        f"- Passed: `{str(report.passed).lower()}`",
        f"- Elapsed seconds: `{report.elapsed_seconds:.3f}`",
        "",
        "| Gate | Status | Key Metrics |",
        "| --- | --- | --- |",
    ]
    for gate in report.gates:
        status = "PASS" if gate.passed else "FAIL"
        compact_metrics = ", ".join(
            f"{key}={value}" for key, value in gate.metrics.items()
            if key in _KEY_METRIC_ORDER.get(gate.name, gate.metrics.keys())
        )
        lines.append(f"| {gate.name} | {status} | {compact_metrics} |")
    failed = [gate for gate in report.gates if not gate.passed]
    if failed:
        lines.extend(["", "## Failures"])
        lines.extend(f"- **{gate.name}**: {gate.details}" for gate in failed)
    return "\n".join(lines) + "\n"


_KEY_METRIC_ORDER: dict[str, tuple[str, ...]] = {
    "archival_stale_cleanup": ("archived_rows", "model_status", "archive_reason"),
    "evidence_attachment": (
        "evidence_attachments",
        "model_inserts",
        "sidecar_readings",
    ),
    "clean_noise_noop": (
        "state_changes",
        "context_use_grade",
        "graph_relation_contract_basis",
    ),
    "compressed_graph_context_use": (
        "context_use_grade",
        "graph_selected_reference_ratio",
        "edge_ops_touching_graph_models",
    ),
    "question_policy_learning": (
        "question_policy_updates",
        "stats_rows",
        "attempts",
        "successes",
    ),
}


async def _ensure_tenant(conn: asyncpg.Connection, tenant_id: UUID, run_id: str) -> None:
    await conn.execute(
        """
        INSERT INTO tenants (id, name, is_demo)
        VALUES ($1, $2, FALSE)
        ON CONFLICT (id) DO NOTHING
        """,
        tenant_id,
        f"prod-readiness-gap-{run_id}",
    )


async def _cleanup_tenant(pool: asyncpg.Pool, tenant_id: UUID) -> None:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT c.relname AS table_name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE n.nspname = 'public'
              AND c.relkind IN ('r', 'p')
              AND a.attname = 'tenant_id'
              AND NOT a.attisdropped
              AND c.relname <> 'tenants'
            ORDER BY c.relkind = 'p', c.relname
            """
        )
        pending = [str(row["table_name"]) for row in rows]
        blocked: dict[str, str] = {}
        for _ in range(8):
            if not pending:
                break
            next_pending: list[str] = []
            blocked = {}
            for table in pending:
                quoted = _quote_ident(table)
                try:
                    await conn.execute(
                        f"DELETE FROM {quoted} WHERE tenant_id = $1",
                        tenant_id,
                    )
                except asyncpg.ForeignKeyViolationError as exc:
                    next_pending.append(table)
                    blocked[table] = str(exc).splitlines()[0]
            if len(next_pending) == len(pending):
                break
            pending = next_pending
        if pending:
            joined = ", ".join(f"{table}: {blocked.get(table, 'blocked')}" for table in pending)
            raise RuntimeError(f"cleanup blocked by FK references: {joined}")
        await conn.execute("DELETE FROM tenants WHERE id = $1", tenant_id)


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


async def _insert_model(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    natural: str,
    born_from_event_id: UUID | None = None,
    proposition: dict[str, Any] | None = None,
    scope_entities: list[dict[str, Any]] | None = None,
    confidence: float = 0.65,
    activation: float = 1.0,
    extra_sql: str = "",
) -> UUID:
    model_id = uuid7()
    event_id = born_from_event_id or uuid7()
    await conn.execute(
        f"""
        INSERT INTO models (
          id, tenant_id, born_from_event_id, proposition, "natural",
          embedding, scope_actors, scope_entities, scope_temporal,
          confidence, activation, status, confidence_at_assertion,
          activation_coefficient {', ' + extra_sql if extra_sql else ''}
        ) VALUES (
          $1, $2, $3, $4::jsonb, $5, $6,
          '{{}}'::uuid[], $7::jsonb, '{{}}'::jsonb,
          $8, $9, 'active', $8, 1.0
          {', now() - interval ' + repr('31 days') if extra_sql else ''}
        )
        """,
        model_id,
        tenant_id,
        event_id,
        json.dumps(proposition or {
            "kind": "belief",
            "claim_role": "fact",
            "subject": natural,
            "assertion": natural,
        }),
        natural,
        deterministic_text_embedding(natural),
        json.dumps(scope_entities or []),
        confidence,
        activation,
    )
    return model_id


async def _insert_observation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    content_text: str,
) -> UUID:
    observation_id = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel,
          content, content_text, embedding, embedding_pending,
          trust_tier, entities_mentioned
        ) VALUES (
          $1, $2, now(), 'signal', 'readiness_harness',
          $3::jsonb, $4, $5, FALSE,
          'authoritative', '[]'::jsonb
        )
        """,
        observation_id,
        tenant_id,
        json.dumps({"text": content_text}),
        content_text,
        deterministic_text_embedding(content_text),
    )
    return observation_id


async def _probe_archival_stale_cleanup(
    pool: asyncpg.Pool,
    tenant_id: UUID,
) -> GateResult:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _ensure_tenant(conn, tenant_id, "archival")
            model_id = await _insert_model(
                conn,
                tenant_id=tenant_id,
                natural="Stale launch-risk memory should be archived by decay.",
                activation=0.01,
                extra_sql="last_retrieved_at",
            )
            archived_rows = await archive_decayed(conn=conn)
            row = await conn.fetchrow(
                """
                SELECT status, archive_reason, archived_at
                FROM models
                WHERE id = $1
                """,
                model_id,
            )
    metrics = {
        "archived_rows": archived_rows,
        "model_status": row["status"] if row else None,
        "archive_reason": row["archive_reason"] if row else None,
        "archived_at_present": bool(row and row["archived_at"]),
    }
    passed = (
        archived_rows >= 1
        and row is not None
        and row["status"] == "archived"
        and row["archive_reason"] == "decay"
        and row["archived_at"] is not None
    )
    return GateResult(
        name="archival_stale_cleanup",
        passed=passed,
        metrics=metrics,
        details="Expected decay archival to archive one stale low-activation model.",
    )


async def _probe_evidence_attachment(
    pool: asyncpg.Pool,
    tenant_id: UUID,
) -> GateResult:
    scope_entity = {"type": "customer", "id": str(uuid7())}
    anchor_natural = "Acme renewal call felt rough after the customer review."
    signal_natural = "Yesterday's call with Acme felt rough."
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _ensure_tenant(conn, tenant_id, "evidence")
            old_event = await _insert_observation(
                conn,
                tenant_id=tenant_id,
                content_text=anchor_natural,
            )
            new_event = await _insert_observation(
                conn,
                tenant_id=tenant_id,
                content_text=signal_natural,
            )
            anchor_model = await _insert_model(
                conn,
                tenant_id=tenant_id,
                natural=anchor_natural,
                born_from_event_id=old_event,
                scope_entities=[scope_entity],
                confidence=0.62,
            )
            diff = ValidatedDiff(
                trigger_ref=uuid7(),
                tenant_id=tenant_id,
                claim_ops=[
                    ClaimOp(
                        op="insert",
                        entry={
                            "tenant_id": str(tenant_id),
                            "born_from_event_id": str(new_event),
                            "proposition": {
                                "kind": "belief",
                                "claim_role": "fact",
                                "subject": "Acme",
                                "assertion": signal_natural,
                            },
                            "natural": signal_natural,
                            "scope_actors": [],
                            "scope_entities": [scope_entity],
                            "scope_temporal": {},
                            "confidence": 0.5,
                            "confidence_at_assertion": 0.5,
                        },
                    )
                ],
            )
            result = await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=new_event,
            )
            sidecar_readings = await conn.fetchval(
                """
                SELECT count(*)::int
                FROM model_signal_readings
                WHERE model_id = $1 AND source_event_id = $2
                """,
                anchor_model,
                new_event,
            )
    aggregation = result.get("memory_aggregation") or {}
    metrics = {
        "evidence_attachments": int(aggregation.get("evidence_attachments") or 0),
        "model_inserts": int(aggregation.get("model_inserts") or 0),
        "quality_downgrades": int(
            (result.get("quality_summary") or {}).get("downgrade_to_evidence")
            or 0
        ),
        "sidecar_readings": int(sidecar_readings or 0),
    }
    passed = (
        metrics["evidence_attachments"] >= 1
        and metrics["model_inserts"] == 0
        and metrics["sidecar_readings"] >= 1
    )
    return GateResult(
        name="evidence_attachment",
        passed=passed,
        metrics=metrics,
        details="Expected low-durability signal to attach to an existing model.",
    )


def _bundle_with_selection(
    *,
    selected: list[UUID],
    graph_selected: list[UUID] | None = None,
    observations: list[Any] | None = None,
) -> ContextBundle:
    return ContextBundle(
        observations=list(observations or []),
        notes={
            "model_selection": {
                "selected_model_ids": [str(mid) for mid in selected],
                "pathway_survival": {
                    "G": {
                        "selected_model_ids": [
                            str(mid) for mid in (graph_selected or [])
                        ]
                    }
                },
            }
        },
    )


def _observation_row(obs_id: UUID, tenant_id: UUID) -> Any:
    from lib.shared.types import ObservationRow

    now = datetime.now(UTC)
    return ObservationRow(
        id=obs_id,
        tenant_id=tenant_id,
        occurred_at=now,
        ingested_at=now,
        kind="signal",
        source_channel="readiness_harness",
        source_actor_ref=None,
        actor_id=None,
        content={"text": "noise reminder: dashboard link was posted twice"},
        content_text="noise reminder: dashboard link was posted twice",
        embedding=None,
        embedding_pending=False,
        trust_tier="derived",
        external_id=None,
        cause_id=None,
        sequence_num=1,
        entities_mentioned=[],
    )


async def _probe_clean_noise_noop(
    pool: asyncpg.Pool,
    tenant_id: UUID,
) -> GateResult:
    del pool
    trigger_id = uuid7()
    graph_model = uuid7()
    other_model = uuid7()
    obs_id = uuid7()
    bundle = _bundle_with_selection(
        selected=[graph_model, other_model],
        graph_selected=[graph_model],
        observations=[_observation_row(obs_id, tenant_id)],
    )
    diff = RawDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        reasoning_trace=(
            f"Model {graph_model} already captures this operational context; "
            f"observation {obs_id} is noise and adds no new state transition."
        ),
    )
    report = summarize_context_use(bundle, diff)
    metrics = {
        "state_changes": 0,
        "context_use_grade": report.get("context_use_grade"),
        "graph_relation_contract_basis": report.get(
            "graph_relation_contract_basis"
        ),
        "reasoning_trace_context_used": report.get("reasoning_trace_context_used"),
    }
    passed = (
        metrics["context_use_grade"] == "justified_noop_context_used"
        and metrics["graph_relation_contract_basis"] == "noop_trace_accounted"
        and metrics["reasoning_trace_context_used"] is True
    )
    return GateResult(
        name="clean_noise_noop",
        passed=passed,
        metrics=metrics,
        details="Expected noise no-op to be explicitly justified against selected context.",
    )


async def _probe_compressed_graph_context_use(
    pool: asyncpg.Pool,
    tenant_id: UUID,
) -> GateResult:
    del pool
    source = uuid7()
    target = uuid7()
    support = uuid7()
    obs_id = uuid7()
    bundle = _bundle_with_selection(
        selected=[source, target, support],
        graph_selected=[source, target],
    )
    diff = RawDiff(
        trigger_ref=uuid7(),
        tenant_id=tenant_id,
        edge_ops=[
            EdgeOp(
                op="add",
                source_model_id=source,
                target_model_id=target,
                edge_kind="early_warning_for",
                confidence=0.86,
                evidence_event_ids=[obs_id],
                evidence_model_ids=[support],
                explanation="The selected compressed graph models form an early warning.",
            )
        ],
    )
    report = summarize_context_use(bundle, diff)
    metrics = {
        "context_use_grade": report.get("context_use_grade"),
        "graph_selected_reference_ratio": report.get(
            "graph_selected_reference_ratio"
        ),
        "edge_ops_touching_graph_models": report.get(
            "edge_ops_touching_graph_models"
        ),
        "graph_relation_contract_satisfied": report.get(
            "graph_relation_contract_satisfied"
        ),
    }
    passed = (
        metrics["context_use_grade"] == "graph_context_used"
        and float(metrics["graph_selected_reference_ratio"] or 0.0) >= 1.0
        and int(metrics["edge_ops_touching_graph_models"] or 0) >= 1
        and metrics["graph_relation_contract_satisfied"] is True
    )
    return GateResult(
        name="compressed_graph_context_use",
        passed=passed,
        metrics=metrics,
        details="Expected relation op over selected compressed graph models.",
    )


async def _probe_question_policy_learning(
    pool: asyncpg.Pool,
    tenant_id: UUID,
) -> GateResult:
    session_id = uuid7()
    model_id = uuid7()
    question_id = "readiness-question-1"
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _ensure_tenant(conn, tenant_id, "question-policy")
            await conn.execute(
                """
                INSERT INTO inquiry_sessions (
                  id, tenant_id, signal_ref_type, signal_ref_id,
                  route, status, stop_status
                ) VALUES (
                  $1, $2, 'internal', NULL,
                  'DEEP_INQUIRY_PATH', 'completed', 'sufficient_for_reasoning'
                )
                """,
                session_id,
                tenant_id,
            )
            await _insert_model(
                conn,
                tenant_id=tenant_id,
                natural="Question-policy readiness model",
                born_from_event_id=uuid7(),
                confidence=0.7,
            )
            # Reuse the generated id as the attribution target.
            model_id = await conn.fetchval(
                """
                SELECT id
                FROM models
                WHERE tenant_id = $1
                  AND "natural" = 'Question-policy readiness model'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                tenant_id,
            )
            await conn.execute(
                """
                INSERT INTO sage_reader_decision_attributions (
                  id, tenant_id, inquiry_session_id, question_id,
                  question_primitive, question, question_score,
                  expected_value, expected_cost, signal_type, entities,
                  model_id, selected, selection_rank, activation_score,
                  activation_reasons, source_breakdown, retrieval_actions,
                  projected_evidence_refs, evidence_in_packet_count
                ) VALUES (
                  $1, $2, $3, $4,
                  'DEPENDENCY', 'What dependency changed?', 0.8,
                  0.9, 0.1, 'T1:event_batch', '[]'::jsonb,
                  $5, TRUE, 1, 0.88,
                  '[\"readiness_probe\"]'::jsonb, '{}'::jsonb, '[]'::jsonb,
                  '[]'::jsonb, 1
                )
                """,
                uuid7(),
                tenant_id,
                session_id,
                question_id,
                model_id,
            )
            optimizer = TopologyOptimizer(pool=pool, tenant_id=tenant_id)
            updates = await optimizer._update_question_policy_stats(  # noqa: SLF001
                inquiry_session_id=session_id,
                events=[
                    OutcomeEventRow(
                        tenant_id=tenant_id,
                        inquiry_session_id=session_id,
                        event_type="reader_decision_used_in_valid_diff",
                        payload={
                            "signal_type": "T1:event_batch",
                            "question_primitive": "DEPENDENCY",
                            "credit_score": 0.7,
                        },
                    )
                ],
                conn=conn,
            )
            row = await conn.fetchrow(
                """
                SELECT attempts, successes, total_credit, total_cost, utility_score
                FROM sage_question_policy_stats
                WHERE tenant_id = $1
                  AND signal_type = 'T1:event_batch'
                  AND question_primitive = 'DEPENDENCY'
                """,
                tenant_id,
            )
            stats_rows = await conn.fetchval(
                "SELECT count(*)::int FROM sage_question_policy_stats WHERE tenant_id = $1",
                tenant_id,
            )
    metrics = {
        "question_policy_updates": int(updates or 0),
        "stats_rows": int(stats_rows or 0),
        "attempts": int(row["attempts"] or 0) if row else 0,
        "successes": int(row["successes"] or 0) if row else 0,
        "utility_score": float(row["utility_score"] or 0.0) if row else 0.0,
    }
    passed = (
        metrics["question_policy_updates"] >= 1
        and metrics["stats_rows"] >= 1
        and metrics["attempts"] >= 1
        and metrics["successes"] >= 1
    )
    return GateResult(
        name="question_policy_learning",
        passed=passed,
        metrics=metrics,
        details="Expected SAGE question-policy stats to be updated from reader attribution.",
    )


PROBES = (
    _probe_archival_stale_cleanup,
    _probe_evidence_attachment,
    _probe_clean_noise_noop,
    _probe_compressed_graph_context_use,
    _probe_question_policy_learning,
)


async def run_harness(args: argparse.Namespace) -> HarnessReport:
    started = time.monotonic()
    tenant_id = UUID(args.tenant_id) if args.tenant_id else uuid7()
    report_dir = Path(args.report_dir) if args.report_dir else (
        DEFAULT_REPORT_ROOT / args.run_id
    )
    report_dir.mkdir(parents=True, exist_ok=True)
    pool = await asyncpg.create_pool(
        args.database_url,
        min_size=1,
        max_size=max(2, int(args.pool_size)),
        init=_register_codecs,
    )
    gates: list[GateResult] = []
    try:
        async with pool.acquire() as conn:
            await _ensure_tenant(conn, tenant_id, args.run_id)
        for probe in PROBES:
            gate = await probe(pool, tenant_id)
            gates.append(gate)
            if args.fail_fast and not gate.passed:
                break
        passed = all(gate.passed for gate in gates)
        report = HarnessReport(
            run_id=args.run_id,
            tenant_id=str(tenant_id),
            passed=passed,
            elapsed_seconds=round(time.monotonic() - started, 3),
            report_dir=str(report_dir),
            gates=gates,
        )
        _write_json(report_dir / "production_readiness_gap_report.json", asdict(report))
        (report_dir / "production_readiness_gap_summary.md").write_text(
            _render_markdown(report)
        )
        return report
    finally:
        if args.cleanup:
            await _cleanup_tenant(pool, tenant_id)
        await pool.close()


def inspect_report(args: argparse.Namespace) -> HarnessReport:
    path = Path(args.report)
    data = json.loads(path.read_text())
    gates = [GateResult(**gate) for gate in data["gates"]]
    return HarnessReport(
        run_id=str(data["run_id"]),
        tenant_id=str(data["tenant_id"]),
        passed=bool(data["passed"]),
        elapsed_seconds=float(data["elapsed_seconds"]),
        report_dir=str(data["report_dir"]),
        gates=gates,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Focused production-readiness gap harness for learning loops.",
    )
    parser.add_argument(
        "--mode",
        choices=("run", "inspect-report"),
        default="run",
    )
    parser.add_argument(
        "--run-id",
        default=f"prod-readiness-gap-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}",
    )
    parser.add_argument("--tenant-id")
    parser.add_argument("--report-dir")
    parser.add_argument("--report")
    parser.add_argument("--pool-size", type=int, default=4)
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument(
        "--cleanup",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Delete the isolated harness tenant data after the run.",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DATABASE_URL. Defaults to environment DATABASE_URL.",
    )
    args = parser.parse_args()
    if args.mode == "run" and not args.database_url:
        parser.error("DATABASE_URL is required for --mode run")
    if args.mode == "inspect-report" and not args.report:
        parser.error("--report is required for --mode inspect-report")
    return args


def main() -> int:
    args = parse_args()
    if args.mode == "inspect-report":
        report = inspect_report(args)
    else:
        report = asyncio.run(run_harness(args))
    print(json.dumps(asdict(report), indent=2, sort_keys=True, default=_json_default))
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
