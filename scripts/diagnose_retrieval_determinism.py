"""Run primary_retrieve N times against identical state and compare results.

Used by VARIANCE-INVESTIGATION-LOG.md Step 4 to determine whether retrieval
itself contributes to Think output variance.

Usage:
    .venv/bin/python scripts/diagnose_retrieval_determinism.py [iterations]
        default iterations = 20

Approach:
1. Materialize scenario_02 with a fresh tenant.
2. Inject the first signal of `alice_ships_refund_flow` (one observation).
3. Build a TriggerContext for that observation as the worker would.
4. Snapshot Models' (id, activation, last_retrieved_at, retrieval_count).
5. Loop N times:
    a. Restore the snapshot (counteract reconsolidation side-effects).
    b. Call primary_retrieve(trigger, conn).
    c. Record sorted Model IDs, sorted Observation IDs, per-pathway counts.
6. Print uniqueness summary.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("COMPANY_OS_ENV", "test")
from dotenv import load_dotenv  # noqa: E402

load_dotenv(REPO_ROOT / ".env")
if os.environ.get("DEEPSEEK_API_KEY"):
    os.environ["LLM_PROVIDER"] = "deepseek"
    os.environ["LLM_MODEL"] = "deepseek-chat"


async def main() -> int:
    iterations = int(sys.argv[1]) if len(sys.argv) > 1 else 20

    import asyncpg
    from lib.embeddings.ollama import OllamaClient, OllamaConfig
    from services.actors.repo import ActorRepo
    from services.entity_aliases.repo import EntityAliasRepo
    from services.retrieval.primary import primary_retrieve, TriggerContext
    from tests.real_llm.conftest import _register_codecs
    from tests.real_llm.infrastructure.scenario_loader import (
        load_scenario,
        materialize,
        inject_sequence,
    )

    dsn = os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3, init=_register_codecs)

    # Truncate everything first.
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='public' AND c.relkind IN ('r','p') AND c.relispartition=FALSE"
        )
        if rows:
            names = ", ".join(f'"{r["relname"]}"' for r in rows)
            await conn.execute(f"TRUNCATE {names} RESTART IDENTITY CASCADE")

    scenario = load_scenario("growth_saas")
    await materialize(scenario, pool=pool)
    actor_repo = ActorRepo(pool)
    alias_repo = EntityAliasRepo(pool)
    embedder = OllamaClient(OllamaConfig.from_env())

    # Inject the first signal only.
    original = scenario.signal_sequences["alice_ships_refund_flow"]
    scenario.signal_sequences["alice_ships_refund_flow"] = original[:1]
    obs_ids = await inject_sequence(
        scenario,
        "alice_ships_refund_flow",
        pool=pool,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        time_compression=0.0,
    )
    seed_obs_id = obs_ids[0]

    # Build a TriggerContext as the worker would for this observation.
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, tenant_id, occurred_at, content_text, actor_id "
            "FROM observations WHERE id=$1",
            seed_obs_id,
        )

    trigger = TriggerContext(
        kind="T1",
        tenant_id=scenario.tenant_id,
        subkind="event_arrival",
        observation_id=seed_obs_id,
        seed_signature={"trigger_id": str(seed_obs_id)},
    )
    trigger.seed_occurred_at = row["occurred_at"]
    trigger.seed_natural_text = row["content_text"]
    trigger.scope_actors = [row["actor_id"]] if row["actor_id"] else []

    # Snapshot Model state (for reset between iterations to neutralise
    # reconsolidation side-effects from primary_retrieve).
    async with pool.acquire() as conn:
        snap_rows = await conn.fetch(
            "SELECT id, activation, last_retrieved_at, retrieval_count "
            "FROM models WHERE tenant_id=$1",
            scenario.tenant_id,
        )
    snapshot = {r["id"]: dict(r) for r in snap_rows}

    async def restore_snapshot() -> None:
        if not snapshot:
            return
        async with pool.acquire() as conn:
            async with conn.transaction():
                for mid, vals in snapshot.items():
                    await conn.execute(
                        "UPDATE models SET activation=$2, last_retrieved_at=$3, "
                        "retrieval_count=$4 WHERE id=$1",
                        mid, vals["activation"], vals["last_retrieved_at"],
                        vals["retrieval_count"],
                    )

    results = []
    for i in range(iterations):
        await restore_snapshot()
        async with pool.acquire() as conn:
            r = await primary_retrieve(trigger, conn)
        per_pathway = {}
        for pr in r.pathway_results:
            p = pr.source_pathway
            per_pathway[p] = per_pathway.get(p, 0) + len(pr.models) + len(pr.observations)
        results.append({
            "iteration": i,
            "model_ids": sorted([str(m.id) for m in r.models]),
            "obs_ids": sorted([str(o.id) for o in r.observations]),
            "model_count": len(r.models),
            "obs_count": len(r.observations),
            "per_pathway_size": per_pathway,
        })

    Path("/tmp/variance_step4_retrieval.json").write_text(json.dumps(results, indent=2, default=str))

    unique_model_sets = set(tuple(r["model_ids"]) for r in results)
    unique_obs_sets = set(tuple(r["obs_ids"]) for r in results)
    print(f"Iterations: {iterations}")
    print(f"Unique Model ID sets: {len(unique_model_sets)}")
    print(f"Unique Observation ID sets: {len(unique_obs_sets)}")
    print(f"Model count distribution: {sorted(set(r['model_count'] for r in results))}")
    print(f"Obs count distribution: {sorted(set(r['obs_count'] for r in results))}")
    print("Per-pathway size variance:")
    for p in ["A", "B", "C", "D"]:
        sizes = [r["per_pathway_size"].get(p, 0) for r in results]
        print(f"  {p}: min={min(sizes)} max={max(sizes)} unique={len(set(sizes))}")

    await embedder.close()
    await pool.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
