"""Run the Think model layer on a sample of Sandbox observations via the
PRODUCTION Think drain (batched, codex), then emit a Company-Intelligence-style
model-layer scorecard.

Reuses the real probe machinery:
  - enqueue_t1_for_observations  (services-grade T1 enqueue)
  - run_think_until_drain        (production ThinkWorker, batches 20-30 / window)
and measures the resulting model layer directly (models, edges, review debt,
think-run cost/latency, and the core scorecard ratios).

Run:  DATABASE_URL=postgresql://company_os:company_os@localhost:5432/company_os_sandbox \
      .venv/bin/python scripts/sandbox_think_eval.py [N_observations]
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("COMPANY_OS_ENV", "test")

import asyncpg
from dotenv import load_dotenv

load_dotenv(REPO_ROOT / ".env")

import lib.shared.db as _db
from lib.llm.provider import build_provider
from services.app.gateway.db_bootstrap import _register_codecs
from tests.real_llm.infrastructure.durability_flow import run_think_until_drain
from scripts.run_1000_signal_model_layer_probe import enqueue_t1_for_observations

TENANT = UUID(os.environ.get(
    "SANDBOX_TENANT_ID",
    "90864cdd-731b-44b3-96c5-78f0004af3e2",
))


async def _report(conn, tenant, *, signals, enqueued, elapsed):
    models = await conn.fetchval(
        "SELECT count(*) FROM models WHERE tenant_id=$1 AND status='active'", tenant)
    by_kind = await conn.fetch(
        "SELECT proposition->>'kind' k, count(*) c FROM models "
        "WHERE tenant_id=$1 AND status='active' GROUP BY 1 ORDER BY 2 DESC", tenant)
    by_role = await conn.fetch(
        "SELECT claim_role r, count(*) c FROM models "
        "WHERE tenant_id=$1 AND status='active' GROUP BY 1 ORDER BY 2 DESC", tenant)
    edges = await conn.fetchval(
        "SELECT count(*) FROM model_edges WHERE tenant_id=$1", tenant)
    review_debt = await conn.fetchval(
        "SELECT count(*) FROM model_edges WHERE tenant_id=$1 "
        "AND review_status IN ('needs_review','disputed')", tenant) or 0
    cost = await conn.fetchrow(
        "SELECT count(*) think_runs, COALESCE(sum(llm_calls_count),0) calls, "
        "COALESCE(sum(llm_input_tokens_total),0) in_tok, "
        "COALESCE(sum(llm_output_tokens_total),0) out_tok, "
        "COALESCE(sum(llm_cost_usd),0)::numeric(12,4) usd, "
        "COALESCE(avg(latency_total_ms),0)::int avg_ms "
        "FROM think_run_costs WHERE tenant_id=$1", tenant)
    outcomes = await conn.fetch(
        "SELECT outcome, count(*) c FROM think_run_costs WHERE tenant_id=$1 "
        "GROUP BY 1 ORDER BY 2 DESC", tenant)
    pending = await conn.fetchval(
        "SELECT count(*) FROM think_trigger_queue WHERE tenant_id=$1 "
        "AND completed_at IS NULL", tenant)

    def ratio(a, b):
        return round(a / b, 3) if b else 0.0

    runs = cost["think_runs"] or 0
    print("\n" + "=" * 62)
    print("  SANDBOX MODEL-LAYER SCORECARD (Think e2e, codex)")
    print("=" * 62)
    print(f"  signals (observations)   : {signals}")
    print(f"  T1 triggers enqueued     : {enqueued}")
    print(f"  triggers still pending   : {pending}")
    print(f"  think runs (batches/ops) : {runs}")
    print(f"  wall-clock               : {elapsed:.1f}s")
    print("  -- model layer produced --")
    print(f"  active models            : {models}")
    print(f"    by kind   : {', '.join(f'{r['k']}={r['c']}' for r in by_kind) or '(none)'}")
    print(f"    by role   : {', '.join(f'{r['r']}={r['c']}' for r in by_role) or '(none)'}")
    print(f"  graph edges              : {edges}  (review debt: {review_debt})")
    print("  -- intelligence ratios (scorecard) --")
    print(f"  compression  models/signal   : {ratio(models, signals)}")
    print(f"  amplification runs/signal    : {ratio(runs, signals)}  (lower=calmer)")
    print(f"  review debt  / signal        : {ratio(review_debt, signals)}")
    print(f"  llm calls    / signal        : {ratio(cost['calls'], signals)}")
    print("  -- cost / health --")
    print(f"  llm calls total          : {cost['calls']}")
    print(f"  tokens in/out            : {cost['in_tok']}/{cost['out_tok']}")
    print(f"  cost (usd)               : ${cost['usd']}")
    print(f"  avg think latency        : {cost['avg_ms']} ms")
    print(f"  outcomes                 : {', '.join(f'{r['outcome']}={r['c']}' for r in outcomes) or '(none)'}")
    print("=" * 62)


async def main():
    raw_n = sys.argv[1] if len(sys.argv) > 1 else "200"
    n = None if raw_n.lower() == "all" else int(raw_n)
    timeout = int(os.environ.get("THINK_TIMEOUT_S", "1800"))
    # run_think_until_drain (durability harness) zeroes the T1 batch window by
    # default -> per-signal. Re-enable batching so the worker pulls 20-30
    # triggers into ONE batched Think run (production THINK_T1_BATCH_* path).
    os.environ.setdefault("DURABILITY_T1_BATCH_WINDOW_S",
                          os.environ.get("THINK_T1_BATCH_WINDOW_S", "30"))
    os.environ.setdefault("DURABILITY_DOWNSTREAM_BATCH_WINDOW_S",
                          os.environ.get("THINK_DOWNSTREAM_BATCH_WINDOW_S", "60"))
    dsn = os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=8, init=_register_codecs)
    _db._pool = pool  # Think sub-components call get_pool()
    provider = build_provider()
    print(f"provider: {type(provider).__name__} model={provider.config.model}")
    run_id = "sandbox-think-eval"

    async with pool.acquire() as conn:
        if n is None:
            rows = await conn.fetch(
                "SELECT id FROM observations WHERE tenant_id=$1 "
                "ORDER BY occurred_at DESC",
                TENANT,
            )
        else:
            rows = await conn.fetch(
                "SELECT id FROM observations WHERE tenant_id=$1 "
                "ORDER BY occurred_at DESC LIMIT $2",
                TENANT, n)
    obs_ids = [r["id"] for r in rows]
    print(f"sample: {len(obs_ids)} most-recent cross-source observations")

    enqueued = await enqueue_t1_for_observations(
        pool, tenant_id=TENANT, observation_ids=obs_ids,
        limit=len(obs_ids), run_id=run_id)
    print(f"enqueued {enqueued} T1 triggers; draining Think (batched, codex)...")

    t0 = time.monotonic()
    status = "drained"
    try:
        await run_think_until_drain(
            TENANT, pool=pool, provider=provider, timeout_seconds=timeout)
    except Exception as exc:  # noqa: BLE001
        status = f"stopped: {type(exc).__name__}: {exc}"
        print(f"drain stopped early: {status}")
    elapsed = time.monotonic() - t0

    async with pool.acquire() as conn:
        await _report(conn, TENANT, signals=len(obs_ids),
                      enqueued=enqueued, elapsed=elapsed)
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
