#!/usr/bin/env python3
"""Replay the Truss authored scenario as a measured Fyralis benchmark.

Modes:
  * adapter-audit: validate fixture counts and frozen fact coverage.
  * retrieval: run the generic benchmark QA lane over Truss facts.
  * think-drain: inject Truss signals through production ingestion, drain T1
    Think rows for the replay tenant, then grade active Models against the
    frozen signal-derivable fact checklist and typed fixture deltas.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import asyncpg
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmarks.adapters.truss_adapter import TrussAdapter


load_dotenv(REPO_ROOT / ".env", override=False)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.mode == "adapter-audit":
        report = adapter_audit(args)
    elif args.mode == "retrieval":
        report = run_retrieval_lane(args)
    else:
        report = asyncio.run(run_think_drain(args))
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "truss_replay_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (args.out / "truss_replay_report.md").write_text(
        render_markdown(report),
        encoding="utf-8",
    )
    print(f"wrote {args.out / 'truss_replay_report.md'}")
    return 0


def adapter_audit(args: argparse.Namespace) -> dict[str, Any]:
    adapter = _adapter(args)
    observations = list(adapter.iter_observations())
    queries = list(adapter.iter_queries())
    return {
        "report_kind": "truss_adapter_audit",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "fixture": _fixture_counts(args.data),
        "adapter": {
            "observations": len(observations),
            "queries": len(queries),
            "requires_run1_memory_queries": sum(
                1 for query in queries
                if query.metadata.get("requires_run1_memory")
            ),
        },
        "typed_deltas": _typed_delta_counts(args.data),
        "fact_filter": str(args.truss_facts),
    }


def run_retrieval_lane(args: argparse.Namespace) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "-m",
        "benchmarks.run_benchmark",
        "--benchmark",
        args.benchmark,
        "--data",
        str(args.data),
        "--truss-facts",
        str(args.truss_facts),
        "--system",
        args.system,
        "--top-k",
        str(args.top_k),
        "--evidence-k",
        str(args.evidence_k),
        "--out",
        str(args.out / "benchmark"),
    ]
    if args.max_cases is not None:
        cmd.extend(["--max-cases", str(args.max_cases)])
    if args.embedding_mode:
        cmd.extend(["--embedding-mode", args.embedding_mode])
    if args.apply_migrations:
        cmd.append("--apply-migrations")
    if args.progress:
        cmd.append("--progress")
    print("$ " + " ".join(cmd), flush=True)
    if not args.dry_run:
        subprocess.run(cmd, cwd=REPO_ROOT, check=True)
    metrics_path = args.out / "benchmark" / "metrics_summary.json"
    metrics = (
        json.loads(metrics_path.read_text(encoding="utf-8"))
        if metrics_path.exists()
        else {}
    )
    return {
        "report_kind": "truss_retrieval_lane",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "command": cmd,
        "metrics": metrics,
        "fixture": _fixture_counts(args.data),
        "typed_deltas": _typed_delta_counts(args.data),
    }


async def run_think_drain(args: argparse.Namespace) -> dict[str, Any]:
    from lib.embeddings.ollama import OllamaClient, OllamaConfig
    from lib.shared.migrations import apply_migrations_dir
    from services.app.gateway.db_bootstrap import _register_codecs
    from services.domain.actors.repo import ActorRepo
    from services.domain.entity_aliases.repo import EntityAliasRepo
    from services.ingest.synthetic.core import inject

    from scripts.run_100_signal_real_llm_e2e import (
        run_signal_t1_triggers_until_complete,
    )
    from scripts.run_1000_signal_model_layer_probe import (
        _build_cached_provider,
        drain_post_commit_actions,
    )

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is required for --mode think-drain")
    run_id = args.run_id or datetime.now(timezone.utc).strftime("truss-%Y%m%dT%H%M%SZ")
    tenant_id = uuid5(NAMESPACE_URL, f"fyralis:truss-replay:{run_id}")
    adapter = _adapter(args)
    observations = list(adapter.iter_observations())
    if args.max_signals is not None:
        observations = observations[: args.max_signals]

    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=args.pool_max_size,
        init=_register_codecs,
    )
    embedder = (
        OllamaClient(OllamaConfig.from_env())
        if args.replay_embedding_mode == "ollama"
        else None
    )
    started = time.monotonic()
    observation_ids: list[UUID] = []
    try:
        if args.apply_migrations:
            async with pool.acquire() as conn:
                await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
        await _ensure_tenant(pool, tenant_id, run_id)
        actor_repo = ActorRepo(pool)
        alias_repo = EntityAliasRepo(pool)
        for index, observation in enumerate(observations, start=1):
            result = await inject(
                _synthetic_signal(observation, run_id=run_id),
                tenant_id,
                pool=pool,
                actor_repo=actor_repo,
                alias_repo=alias_repo,
                embedder=embedder,
                skip_t1_enqueue=False,
            )
            observation_ids.append(result.observation.id)
            if args.progress and index % args.progress_every == 0:
                print(f"injected {index}/{len(observations)}", flush=True)

        provider = _build_cached_provider()
        drain_status = await run_signal_t1_triggers_until_complete(
            tenant_id,
            pool=pool,
            provider=provider,
            observation_ids=observation_ids,
            timeout_seconds=args.think_timeout,
        )
        post_commit_status = await drain_post_commit_actions(
            pool,
            tenant_id=tenant_id,
            timeout_seconds=args.post_commit_timeout,
        )
        fact_grade = await _grade_facts(pool, tenant_id=tenant_id, adapter=adapter)
        summary = await _tenant_summary(pool, tenant_id)
    finally:
        if embedder is not None:
            await embedder.close()
        await pool.close()

    return {
        "report_kind": "truss_think_drain",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "tenant_id": str(tenant_id),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "signals_injected": len(observation_ids),
        "fixture": _fixture_counts(args.data),
        "typed_deltas": _typed_delta_counts(args.data),
        "drain_status": drain_status,
        "post_commit_status": post_commit_status,
        "fact_grade": fact_grade,
        "tenant_summary": summary,
    }


def _adapter(args: argparse.Namespace) -> TrussAdapter:
    return TrussAdapter(
        args.data,
        include_run1=args.benchmark in {"truss", "truss_r1", "truss_full"},
        include_run2=args.benchmark in {"truss", "truss_r2", "truss_full"},
        fact_filter_path=args.truss_facts,
        max_cases=args.max_cases,
    )


def _synthetic_signal(observation: Any, *, run_id: str) -> Any:
    from services.ingest.synthetic.core import SyntheticSignal

    metadata = dict(observation.metadata or {})
    return SyntheticSignal(
        source_channel=f"truss:{observation.source}",
        content_text=observation.content,
        content={
            "text": observation.content,
            "benchmark": "truss",
            **metadata,
        },
        occurred_at=observation.occurred_at,
        source_actor_ref=metadata.get("source_actor_ref"),
        external_id=f"{run_id}:{observation.observation_id}",
        entities_hint=_entity_hints(observation.entities),
        trust_tier=_runtime_trust_tier(observation.trust_tier),
        kind="signal",
        scenario_id="truss_authored_scenario",
        run_id=run_id,
    )


def _entity_hints(entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    hints: list[dict[str, Any]] = []
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        entity_type = entity.get("type")
        entity_id = entity.get("id") or entity.get("ref")
        if entity_type and entity_id:
            hints.append({"type": str(entity_type), "id": str(entity_id)})
    return hints


def _runtime_trust_tier(value: str) -> str:
    normalized = str(value or "").casefold()
    if normalized in {"benchmark_gold", "direct"}:
        return "authoritative"
    if normalized in {
        "authoritative",
        "attested_agent",
        "authoritative_external",
        "reputable",
        "inferential",
        "inferential_external",
        "unvetted",
        "derived",
    }:
        return normalized
    return "authoritative"


async def _ensure_tenant(pool: asyncpg.Pool, tenant_id: UUID, run_id: str) -> None:
    await pool.execute(
        """
        INSERT INTO tenants (id, name, is_demo)
        VALUES ($1, $2, FALSE)
        ON CONFLICT (id) DO NOTHING
        """,
        tenant_id,
        f"truss-replay:{run_id}",
    )


async def _grade_facts(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    adapter: TrussAdapter,
) -> dict[str, Any]:
    queries = list(adapter.iter_queries())
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, "natural", proposition, supporting_event_ids
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
            """,
            tenant_id,
        )
    model_texts = [
        (
            str(row["id"]),
            (
                str(row["natural"] or "")
                + "\n"
                + json.dumps(row["proposition"], sort_keys=True, default=str)
            ).casefold(),
        )
        for row in rows
    ]
    facts: list[dict[str, Any]] = []
    for query in queries:
        gold = adapter.gold(query.query_id)
        answer_terms = _answer_terms(gold.answer or "")
        matched_models = [
            model_id for model_id, text in model_texts
            if answer_terms and all(term in text for term in answer_terms)
        ][:5]
        facts.append({
            "query_id": query.query_id,
            "answer": gold.answer,
            "requires_run1_memory": bool(query.metadata.get("requires_run1_memory")),
            "covered": bool(matched_models),
            "matched_model_ids": matched_models,
        })
    covered = sum(1 for fact in facts if fact["covered"])
    return {
        "facts": facts,
        "n": len(facts),
        "covered": covered,
        "coverage": round(covered / len(facts), 4) if facts else 0.0,
        "requires_run1_memory": {
            "n": sum(1 for fact in facts if fact["requires_run1_memory"]),
            "covered": sum(
                1 for fact in facts
                if fact["requires_run1_memory"] and fact["covered"]
            ),
        },
    }


def _answer_terms(answer: str) -> list[str]:
    terms = [
        token.strip(" ,.;:()[]{}").casefold()
        for token in answer.replace("$", " ").replace(",", "").split()
    ]
    return [
        term for term in terms
        if len(term) >= 2 and term not in {"and", "the", "per", "year"}
    ][:6]


async def _tenant_summary(pool: asyncpg.Pool, tenant_id: UUID) -> dict[str, Any]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              (SELECT count(*)::bigint FROM observations WHERE tenant_id = $1) AS observations,
              (SELECT count(*)::bigint FROM models WHERE tenant_id = $1) AS models,
              (SELECT count(*)::bigint FROM model_edges WHERE tenant_id = $1) AS model_edges,
              (SELECT count(*)::bigint FROM think_runs WHERE tenant_id = $1 AND status = 'success') AS think_success,
              (SELECT count(*)::bigint FROM think_runs WHERE tenant_id = $1 AND status = 'failed') AS think_failed,
              (SELECT count(*)::bigint FROM think_trigger_queue WHERE tenant_id = $1 AND completed_at IS NULL) AS pending_triggers
            """,
            tenant_id,
        )
    return {key: int(row[key] or 0) for key in row.keys()}


def _fixture_counts(data_path: Path) -> dict[str, Any]:
    root = data_path.resolve()
    if (root / "truss_run").exists():
        base = root
    else:
        base = root.parent if root.name in {"truss_run", "truss_run_2"} else root
    return {
        "run1_signals": _jsonl_count(base / "truss_run" / "signals"),
        "run2_signals": _jsonl_count(base / "truss_run_2" / "signals"),
        "run1_ground_truth_rows": _line_count(base / "truss_run" / "ground_truth.jsonl"),
        "run2_ground_truth_rows": _line_count(base / "truss_run_2" / "ground_truth_r2.jsonl"),
    }


def _typed_delta_counts(data_path: Path) -> dict[str, Any]:
    root = data_path.resolve()
    if (root / "truss_run").exists():
        base = root
    else:
        base = root.parent if root.name in {"truss_run", "truss_run_2"} else root
    run1 = base / "truss_run" / "model_events.jsonl"
    run2 = base / "truss_run_2" / "model_events_r2.jsonl"
    return {
        "source": "model_events_jsonl",
        "run1": _line_count(run1),
        "run2": _line_count(run2),
        "total": _line_count(run1) + _line_count(run2),
        "plan_expected_total": 308,
    }


def _jsonl_count(directory: Path) -> int:
    if not directory.exists():
        return 0
    return sum(_line_count(path) for path in directory.glob("*.jsonl"))


def _line_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Truss Replay Report",
        "",
        f"- Kind: {report.get('report_kind')}",
        f"- Generated: {report.get('generated_at')}",
    ]
    if report.get("tenant_id"):
        lines.append(f"- Tenant: `{report.get('tenant_id')}`")
    if report.get("signals_injected") is not None:
        lines.append(f"- Signals injected: {report.get('signals_injected')}")
    fact_grade = report.get("fact_grade") or {}
    if fact_grade:
        lines.append(
            f"- Fact coverage: {fact_grade.get('covered')}/{fact_grade.get('n')} "
            f"({fact_grade.get('coverage')})"
        )
    lines.extend(["", "## Details", "```json"])
    lines.append(json.dumps(report, indent=2, sort_keys=True, default=str))
    lines.extend(["```", ""])
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=["adapter-audit", "retrieval", "think-drain"],
        default="adapter-audit",
    )
    parser.add_argument("--data", type=Path, default=REPO_ROOT)
    parser.add_argument(
        "--benchmark",
        choices=["truss", "truss_r1", "truss_r2", "truss_full"],
        default="truss_full",
    )
    parser.add_argument(
        "--truss-facts",
        type=Path,
        default=REPO_ROOT / "benchmarks" / "truss_signal_derivable_facts.json",
    )
    parser.add_argument("--system", default="bm25_session")
    parser.add_argument("--embedding-mode", choices=["hash", "provider", "ollama", "openai"], default=None)
    parser.add_argument("--replay-embedding-mode", choices=["none", "ollama"], default="none")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--evidence-k", type=int, default=10)
    parser.add_argument("--max-cases", type=int, default=None)
    parser.add_argument("--max-signals", type=int, default=None)
    parser.add_argument("--run-id")
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "benchmarks" / "reports" / "generated" / "truss_replay")
    parser.add_argument("--pool-max-size", type=int, default=8)
    parser.add_argument("--think-timeout", type=int, default=7200)
    parser.add_argument("--post-commit-timeout", type=int, default=900)
    parser.add_argument("--apply-migrations", action="store_true")
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--progress-every", type=int, default=25)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
