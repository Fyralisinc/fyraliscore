#!/usr/bin/env python3
"""Run CAPABILITY-PLAN B4 golden-day CEO surface eval.

Given a materialized tenant and founder actor, this invokes the real greeting
snapshot composer and recommendation list, serializes the rendered substrate,
and scores planted-risk coverage from an optional gold file.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[2]
import sys
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.app.gateway.db_bootstrap import _register_codecs
from services.product.greeting.snapshot import SnapshotComposer
from services.product.recommendations.repo import list_for_actor


load_dotenv(REPO_ROOT / ".env", override=False)


def main() -> int:
    args = parse_args()
    report = asyncio.run(run_eval(args))
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "golden_day_surface_eval.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    (args.out / "golden_day_surface_eval.md").write_text(
        render_markdown(report),
        encoding="utf-8",
    )
    print(f"wrote {args.out / 'golden_day_surface_eval.md'}")
    return 0


async def run_eval(args: argparse.Namespace) -> dict[str, Any]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is required")
    tenant_id = UUID(args.tenant_id)
    actor_id = UUID(args.actor_id)
    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=args.pool_max_size,
        init=_register_codecs,
    )
    try:
        composer = SnapshotComposer(pool)
        async with pool.acquire() as conn:
            greeting = await composer.compose_greeting_snapshot(tenant_id, conn=conn)
            cards = {
                kind: [
                    snapshot.to_json()
                    for snapshot in await composer.compose_card_snapshot(
                        tenant_id,
                        kind,  # type: ignore[arg-type]
                        conn=conn,
                    )
                ]
                for kind in ("observation", "decision", "question")
            }
            query_grid = await composer.compose_query_grid_snapshot(tenant_id, conn=conn)
            recommendations = [
                _serialize(view)
                for view in await list_for_actor(
                    tenant_id=tenant_id,
                    target_actor_id=actor_id,
                    limit=args.recommendation_limit,
                    conn=conn,
                )
            ]
            gold = _load_gold(args.gold)
            score = await _score_gold(
                conn,
                tenant_id=tenant_id,
                gold=gold,
                surface_items=_surface_items(greeting.to_json(), cards, recommendations),
            )
    finally:
        await pool.close()
    return {
        "report_kind": "golden_day_surface_eval",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tenant_id": args.tenant_id,
        "actor_id": args.actor_id,
        "gold_path": str(args.gold) if args.gold else None,
        "surfaces": {
            "greeting": greeting.to_json(),
            "cards": cards,
            "query_grid": query_grid.to_json() if hasattr(query_grid, "to_json") else _serialize(query_grid),
            "recommendations": recommendations,
        },
        "score": score,
    }


def _surface_items(
    greeting: dict[str, Any],
    cards: dict[str, list[dict[str, Any]]],
    recommendations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for index, rec in enumerate(recommendations[:10], start=1):
        items.append({"surface": "recommendations", "rank": index, "payload": rec})
    rank = len(items) + 1
    for kind, rows in cards.items():
        for row in rows:
            items.append({"surface": f"card:{kind}", "rank": rank, "payload": row})
            rank += 1
    items.append({"surface": "greeting", "rank": rank, "payload": greeting})
    return items


async def _score_gold(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    gold: dict[str, Any],
    surface_items: list[dict[str, Any]],
) -> dict[str, Any]:
    risks = gold.get("risks") if isinstance(gold, dict) else None
    if not isinstance(risks, list):
        risks = []
    results: list[dict[str, Any]] = []
    for risk in risks:
        if not isinstance(risk, dict):
            continue
        terms = [str(term).casefold() for term in risk.get("terms") or []]
        matched_items = [
            item for item in surface_items
            if _contains_terms(json.dumps(item["payload"], default=str).casefold(), terms)
        ]
        if matched_items:
            decomposition = "rendered"
        else:
            decomposition = await _failure_decomposition(conn, tenant_id, terms)
        results.append({
            "id": risk.get("id"),
            "terms": terms,
            "covered_top10": any(int(item["rank"]) <= 10 for item in matched_items),
            "matched_surfaces": [
                {"surface": item["surface"], "rank": item["rank"]}
                for item in matched_items[:5]
            ],
            "failure_decomposition": decomposition,
        })
    covered = sum(1 for result in results if result["covered_top10"])
    precision_denominator = min(10, len(surface_items))
    precision_hits = sum(
        1 for item in surface_items[:10]
        if any(
            _contains_terms(
                json.dumps(item["payload"], default=str).casefold(),
                result["terms"],
            )
            for result in results
        )
    )
    question_items = [
        item for item in surface_items
        if item["surface"] == "card:question"
    ]
    question_hits = sum(
        1 for item in question_items
        if any(
            _contains_terms(
                json.dumps(item["payload"], default=str).casefold(),
                result["terms"],
            )
            for result in results
        )
    )
    return {
        "planted_risk_count": len(results),
        "planted_risk_top10_covered": covered,
        "planted_risk_top10_coverage": round(covered / len(results), 4) if results else None,
        "top10_precision_vs_planted_risks": (
            round(precision_hits / precision_denominator, 4)
            if precision_denominator else None
        ),
        "uncertainty_correspondence": (
            round(question_hits / len(question_items), 4)
            if question_items else None
        ),
        "risks": results,
    }


async def _failure_decomposition(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    terms: list[str],
) -> str:
    if not terms:
        return "no_gold_terms"
    rows = await conn.fetch(
        """
        SELECT id, "natural", proposition
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
        LIMIT 5000
        """,
        tenant_id,
    )
    for row in rows:
        text = (
            str(row["natural"] or "")
            + " "
            + json.dumps(row["proposition"], sort_keys=True, default=str)
        ).casefold()
        if _contains_terms(text, terms):
            return "present_but_unrendered"
    return "belief_absent"


def _contains_terms(text: str, terms: list[str]) -> bool:
    if not terms:
        return False
    return all(term in text for term in terms)


def _load_gold(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {"risks": []}
    return json.loads(path.read_text(encoding="utf-8"))


def _serialize(value: Any) -> Any:
    if is_dataclass(value):
        return _serialize(asdict(value))
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {str(key): _serialize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    return value


def render_markdown(report: dict[str, Any]) -> str:
    score = report.get("score") or {}
    lines = [
        "# Golden-Day CEO Surface Eval",
        "",
        f"- Tenant: `{report.get('tenant_id')}`",
        f"- Actor: `{report.get('actor_id')}`",
        f"- Planted risk coverage top-10: {score.get('planted_risk_top10_coverage')}",
        f"- Top-10 precision: {score.get('top10_precision_vs_planted_risks')}",
        f"- Uncertainty correspondence: {score.get('uncertainty_correspondence')}",
        "",
        "## Risk Results",
        "| Risk | Covered Top 10 | Failure Decomposition | Surfaces |",
        "| --- | --- | --- | --- |",
    ]
    for risk in score.get("risks") or []:
        lines.append(
            "| {id} | {covered} | {decomp} | {surfaces} |".format(
                id=risk.get("id"),
                covered=risk.get("covered_top10"),
                decomp=risk.get("failure_decomposition"),
                surfaces=", ".join(
                    item.get("surface", "")
                    for item in risk.get("matched_surfaces") or []
                ) or "-",
            )
        )
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--actor-id", required=True)
    parser.add_argument("--gold", type=Path)
    parser.add_argument("--recommendation-limit", type=int, default=15)
    parser.add_argument("--pool-max-size", type=int, default=4)
    parser.add_argument(
        "--out",
        type=Path,
        default=REPO_ROOT / "benchmarks" / "reports" / "generated" / "golden_day_surface",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
