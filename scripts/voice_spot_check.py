"""scripts/voice_spot_check.py — §7 Test 7 smoke + observability sweep.

Two jobs in one script (both aligned with Week 7-8 work items):

  1. Voice-rules spot check. Pulls the most recent N rendered outputs
     cached in `view_ceo_cache` (greeting / cards / query_grid /
     conversation_turn / close_line) and runs `voice_rules.check_all`
     across them. Prints reject-severity violations loud (exit 1) and
     flag-severity violations quietly (exit 0).

  2. Cost-per-render-kind dashboard. Reads `view_render_costs` and
     aggregates count / total / mean cost and token throughput per
     render_kind + outcome. No new migration — this is a read-only
     dashboard over the existing table.

Run:

    source .venv/bin/activate
    export $(cat .env | xargs)
    python scripts/voice_spot_check.py             # both reports
    python scripts/voice_spot_check.py --voice-only
    python scripts/voice_spot_check.py --costs-only
    python scripts/voice_spot_check.py --limit 40

Exits 1 if any reject-severity voice violation is found, 0 otherwise.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


DOGFOOD_TENANT = UUID("00000000-0000-7000-8000-000000000dd1")


# =====================================================================
# Voice spot check — pulls recent rendered bodies, runs voice rules.
# =====================================================================


_COST_SQL = """
SELECT
  render_kind,
  COUNT(*)                           AS runs,
  SUM(llm_cost_usd)                  AS total_cost_usd,
  AVG(llm_cost_usd)                  AS avg_cost_usd,
  AVG(llm_input_tokens_total)        AS avg_input_tokens,
  AVG(llm_output_tokens_total)       AS avg_output_tokens,
  AVG(latency_total_ms)              AS avg_latency_ms,
  COUNT(*) FILTER (WHERE flagged)    AS flagged,
  COUNT(*) FILTER (WHERE outcome != 'success') AS non_success,
  MIN(computed_at)                   AS oldest,
  MAX(computed_at)                   AS newest
FROM view_render_costs
WHERE tenant_id = $1
GROUP BY render_kind
ORDER BY render_kind;
"""


# Cache rows live under these cache_keys; each has a `body_html` the
# voice rules can check. We iterate card IDs inside the `cards` row.
_VOICE_SQL = """
SELECT cache_key, cached_content, cached_at
FROM view_ceo_cache
WHERE tenant_id = $1
ORDER BY cached_at DESC;
"""


async def _voice_spot_check(tenant_id: UUID, limit: int) -> int:
    import asyncpg
    from services.gateway.db_bootstrap import _register_codecs
    from services.rendering.voice_rules import (
        RuleContext,
        Severity,
        check_all,
    )

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set; voice spot check skipped.", file=sys.stderr)
        return 0

    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=2, init=_register_codecs,
    )
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(_VOICE_SQL, tenant_id)
    finally:
        await pool.close()

    # Extract renderable bodies. Each (kind, html) pair is one sample.
    samples: list[tuple[str, str, str]] = []  # (kind, label, html)
    for row in rows:
        key = row["cache_key"]
        try:
            content = row["cached_content"]
            if isinstance(content, str):
                content = json.loads(content)
        except Exception:
            continue
        if key == "greeting" and content.get("body_html"):
            samples.append(("greeting", "greeting", content["body_html"]))
        elif key == "close_line" and content.get("body"):
            samples.append((
                "close_line", "close_line",
                content["body"],
            ))
        elif key == "query_grid":
            for q in content.get("queries", []) or []:
                if q.get("label"):
                    samples.append((
                        "query_grid_item",
                        f"query_grid_item[{q.get('id','?')}]",
                        q["label"],
                    ))
        elif key == "cards":
            card_list = content if isinstance(content, list) else content.get("cards", [])
            for c in card_list or []:
                kind = {
                    "observation": "card_observation",
                    "decision": "card_decision",
                    "question": "card_question",
                }.get(c.get("kind", ""), "card_observation")
                if c.get("body_html"):
                    samples.append((
                        kind,
                        f"{kind}[{c.get('id','?')}]",
                        c["body_html"],
                    ))

    samples = samples[:limit]
    if not samples:
        print("(no rendered bodies in cache yet)")
        return 0

    print(f"Voice spot check — {len(samples)} rendered sample(s)")
    print("-" * 68)
    rejects = 0
    flags = 0
    for kind, label, html in samples:
        context = RuleContext(kind=kind)
        violations = check_all(html, context=context)
        reject_vs = [v for v in violations if v.severity == Severity.REJECT]
        flag_vs = [v for v in violations if v.severity == Severity.FLAG]
        if reject_vs:
            rejects += len(reject_vs)
            print(f"[REJECT] {label}")
            for v in reject_vs:
                print(f"    - {v.rule}: {v.message}")
                if v.offending_text:
                    print(f"      at: {v.offending_text[:80]!r}")
        elif flag_vs:
            flags += len(flag_vs)
            print(f"[flag]   {label}")
            for v in flag_vs:
                print(f"    - {v.rule}: {v.message}")
        else:
            print(f"[ok]     {label}")
    print("-" * 68)
    print(f"rejects={rejects}  flags={flags}  checked={len(samples)}")
    return 1 if rejects > 0 else 0


async def _cost_dashboard(tenant_id: UUID) -> int:
    import asyncpg
    from services.gateway.db_bootstrap import _register_codecs

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL not set; cost dashboard skipped.", file=sys.stderr)
        return 0

    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=2, init=_register_codecs,
    )
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(_COST_SQL, tenant_id)
    finally:
        await pool.close()

    if not rows:
        print("(no view_render_costs rows for this tenant yet)")
        return 0

    print(f"Cost-per-render-kind — tenant {tenant_id}")
    print("-" * 78)
    header = f"{'kind':<18}{'runs':>6}{'total$':>10}{'mean$':>10}{'μ_in':>8}{'μ_out':>8}{'μ_ms':>8}"
    print(header)
    print("-" * 78)
    total_runs = 0
    total_cost = Decimal("0")
    for r in rows:
        kind = r["render_kind"]
        runs = int(r["runs"] or 0)
        total = Decimal(r["total_cost_usd"] or 0)
        avg_c = Decimal(r["avg_cost_usd"] or 0)
        avg_in = float(r["avg_input_tokens"] or 0)
        avg_out = float(r["avg_output_tokens"] or 0)
        avg_ms = float(r["avg_latency_ms"] or 0)
        total_runs += runs
        total_cost += total
        print(
            f"{kind:<18}{runs:>6}{'$' + f'{float(total):.4f}':>10}"
            f"{'$' + f'{float(avg_c):.4f}':>10}{avg_in:>8.0f}{avg_out:>8.0f}"
            f"{avg_ms:>8.0f}"
        )
    print("-" * 78)
    print(f"{'total':<18}{total_runs:>6}{'$' + f'{float(total_cost):.4f}':>10}")
    return 0


async def main_async(args: argparse.Namespace) -> int:
    tenant_id = UUID(args.tenant)
    ret = 0
    if not args.costs_only:
        r = await _voice_spot_check(tenant_id, args.limit)
        if r != 0:
            ret = r
    if not args.voice_only:
        print()  # separator
        r = await _cost_dashboard(tenant_id)
        if r != 0 and ret == 0:
            ret = r
    return ret


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    # Default to the dogfood / simulation tenant. The env's DEFAULT_TENANT_ID
    # usually points at the legacy/test tenant, which doesn't carry the
    # CEO-view cache rows. Explicit --tenant or SPOT_CHECK_TENANT_ID wins.
    ap.add_argument(
        "--tenant",
        default=os.environ.get("SPOT_CHECK_TENANT_ID", str(DOGFOOD_TENANT)),
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max rendered samples to inspect (default: 20).",
    )
    ap.add_argument("--voice-only", action="store_true")
    ap.add_argument("--costs-only", action="store_true")
    args = ap.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
