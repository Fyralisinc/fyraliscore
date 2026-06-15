#!/usr/bin/env python3
"""Run CAPABILITY-PLAN B5 chained Ask evaluations.

For Truss, this wraps the existing ``fyralis_ask_current`` benchmark lane. For
storyline runs, it asks the real ``AskOrchestrator`` against an already
materialized storyline tenant using one thesis question per storyline.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmarks.fyralis_eval.metrics import token_f1
from services.app.gateway.db_bootstrap import _register_codecs
from services.product.ask.orchestrator import AskOrchestrator
from services.product.ask.schemas import AskScope, AskSessionCreateRequest, AskTurnRequest
from services.product.ask.store import InMemoryAskStore
from services.reasoning.sage.reader import SynthesisReader


load_dotenv(REPO_ROOT / ".env", override=False)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.mode == "truss":
        report = run_truss_ask(args)
    else:
        report = asyncio.run(run_storyline_ask(args))
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "chained_ask_eval.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (args.out / "chained_ask_eval.md").write_text(
        render_markdown(report),
        encoding="utf-8",
    )
    print(f"wrote {args.out / 'chained_ask_eval.md'}")
    return 0


def run_truss_ask(args: argparse.Namespace) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(REPO_ROOT / "benchmarks" / "run_benchmark.py"),
        "--benchmark",
        args.truss_benchmark,
        "--data",
        str(args.data),
        "--truss-facts",
        str(args.truss_facts),
        "--system",
        "fyralis_ask_current",
        "--answerer",
        "passthrough",
        "--top-k",
        str(args.top_k),
        "--evidence-k",
        str(args.evidence_k),
        "--out",
        str(args.out / "truss_ask"),
    ]
    if args.max_cases is not None:
        cmd.extend(["--max-cases", str(args.max_cases)])
    if args.embedding_mode:
        cmd.extend(["--embedding-mode", args.embedding_mode])
    if args.apply_migrations:
        cmd.append("--apply-migrations")
    if args.judge_answers:
        cmd.append("--judge-answers")
    if args.progress:
        cmd.append("--progress")
    print("$ " + " ".join(cmd), flush=True)
    if not args.dry_run:
        subprocess.run(cmd, cwd=REPO_ROOT, check=True)
    metrics_path = args.out / "truss_ask" / "metrics_summary.json"
    return {
        "report_kind": "truss_chained_ask_eval",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "command": cmd,
        "metrics": (
            json.loads(metrics_path.read_text(encoding="utf-8"))
            if metrics_path.exists()
            else {}
        ),
    }


async def run_storyline_ask(args: argparse.Namespace) -> dict[str, Any]:
    if args.tenant_id is None or args.viewer_id is None:
        raise SystemExit("--tenant-id and --viewer-id are required for --mode storyline")
    gold_path = args.storyline_run_dir / "storyline_gold.json"
    if not gold_path.exists():
        raise SystemExit(f"missing storyline gold: {gold_path}")
    gold_rows = json.loads(gold_path.read_text(encoding="utf-8"))
    questions = [
        {
            "storyline_id": row["id"],
            "title": row["title"],
            "query": f"What is the core company-memory thesis for {row['title']}?",
            "gold": row.get("thesis") or "",
            "expected_terms": row.get("expected_terms") or [],
        }
        for row in gold_rows
        if isinstance(row, dict)
    ]
    if args.max_cases is not None:
        questions = questions[: args.max_cases]

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is required for storyline Ask eval")
    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=args.pool_max_size,
        init=_register_codecs,
    )
    store = InMemoryAskStore()
    orchestrator = AskOrchestrator(
        store=store,
        conn_provider=pool.acquire,
        reader=SynthesisReader(pool=pool),
    )
    tenant_id = UUID(args.tenant_id)
    viewer_id = UUID(args.viewer_id)
    results: list[dict[str, Any]] = []
    try:
        for question in questions:
            scope = AskScope(
                type="whole_company",
                label=f"Storyline {question['storyline_id']}",
                filters={
                    "benchmark": "storyline_batch",
                    "storyline_id": question["storyline_id"],
                },
                access_mode="full",
            )
            session = await orchestrator.create_session(
                tenant_id=tenant_id,
                viewer_id=viewer_id,
                body=AskSessionCreateRequest(
                    initial_scope=scope,
                    source_route="/benchmarks/storyline-ask",
                ),
            )
            response = await orchestrator.answer_turn(
                tenant_id=tenant_id,
                viewer_id=viewer_id,
                session_id=session.id,
                body=AskTurnRequest(
                    query=question["query"],
                    requested_mode="direct_synthesis_read",
                ),
            )
            answer = response.payload.answer
            expected_terms = [str(term).casefold() for term in question["expected_terms"]]
            answer_l = answer.casefold()
            results.append({
                **question,
                "answer": answer,
                "confidence": response.payload.confidence,
                "evidence_count": len(response.payload.evidence),
                "unknown_count": len(response.payload.unknowns),
                "token_f1": token_f1(answer, question["gold"]),
                "expected_term_recall": (
                    sum(1 for term in expected_terms if term in answer_l)
                    / len(expected_terms)
                    if expected_terms else 0.0
                ),
            })
    finally:
        await pool.close()

    return {
        "report_kind": "storyline_chained_ask_eval",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tenant_id": args.tenant_id,
        "viewer_id": args.viewer_id,
        "storyline_run_dir": str(args.storyline_run_dir),
        "n": len(results),
        "average_token_f1": _mean(result["token_f1"] for result in results),
        "average_expected_term_recall": _mean(
            result["expected_term_recall"] for result in results
        ),
        "results": results,
    }


def _mean(values: Any) -> float:
    rows = [float(value) for value in values if isinstance(value, (int, float))]
    return round(sum(rows) / len(rows), 4) if rows else 0.0


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Chained Ask Eval",
        "",
        f"- Kind: {report.get('report_kind')}",
        f"- Generated: {report.get('generated_at')}",
    ]
    if "average_token_f1" in report:
        lines.append(f"- Average token F1: {report.get('average_token_f1')}")
        lines.append(
            "- Average expected-term recall: "
            f"{report.get('average_expected_term_recall')}"
        )
    metrics = report.get("metrics") or {}
    if metrics:
        lines.append(f"- Queries: {metrics.get('queries')}")
        recall_key = next((key for key in metrics if key.startswith("evidence_recall_at_")), "")
        lines.append(f"- Evidence recall: {metrics.get(recall_key)}")
    lines.extend(["", "## Details", "```json"])
    lines.append(json.dumps(report, indent=2, sort_keys=True, default=str))
    lines.extend(["```", ""])
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["truss", "storyline"], default="truss")
    parser.add_argument("--data", type=Path, default=REPO_ROOT)
    parser.add_argument("--truss-benchmark", choices=["truss", "truss_r1", "truss_r2", "truss_full"], default="truss_full")
    parser.add_argument(
        "--truss-facts",
        type=Path,
        default=REPO_ROOT / "benchmarks" / "truss_signal_derivable_facts.json",
    )
    parser.add_argument("--storyline-run-dir", type=Path, default=REPO_ROOT / "tests" / "real_llm" / "reports" / "runs")
    parser.add_argument("--tenant-id")
    parser.add_argument("--viewer-id")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--evidence-k", type=int, default=10)
    parser.add_argument("--max-cases", type=int, default=None)
    parser.add_argument("--embedding-mode", choices=["hash", "provider", "ollama", "openai"], default="hash")
    parser.add_argument("--pool-max-size", type=int, default=4)
    parser.add_argument("--apply-migrations", action="store_true")
    parser.add_argument("--judge-answers", action="store_true")
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--out", type=Path, default=REPO_ROOT / "benchmarks" / "reports" / "generated" / "chained_ask")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
