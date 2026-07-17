"""Production-backed staged arm execution for P7.

Every reasoning-enabled batch crosses the real T1 Think boundary.  Arms are
isolated by tenant, progress in strict batch order, and may run concurrently.
The evaluator records production outcomes; it never fabricates lifecycle ops.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import time
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.evaluation.epistemic_repair.p6_population import P6Population
from lib.evaluation.epistemic_repair.p6_think_runner import (
    _init_p6_connection,
    _persist_runtime_batch,
    _snapshot,
)
from lib.evaluation.epistemic_repair.p7_evolution import (
    P7EvolutionArm,
    arm_allows_reasoning,
    bridge_validated_think_lifecycle,
)
from lib.evaluation.epistemic_repair.p7_retrieval_policy import (
    production_retrieval_policy,
)
from lib.llm.provider import build_provider, close_codex_app_server_client, set_response_cache
from lib.shared.errors import InvariantViolation
from services.reasoning.think.worker import ThinkWorker, WorkerConfig


P7_ARMS: tuple[P7EvolutionArm, ...] = (
    "adaptive", "frozen", "observation_only", "memory_hidden", "corrupted"
)


@dataclass(frozen=True, slots=True)
class P7ArmRuntime:
    arm: P7EvolutionArm
    tenant_id: UUID
    worker: ThinkWorker | None


def _run_id(execution: dict[str, Any]) -> UUID:
    run = execution.get("run") or {}
    if run.get("status") != "success" or not run.get("id"):
        raise InvariantViolation(
            "P7_PRODUCTION_THINK_NOT_SUCCESSFUL",
            "P7 does not score or evolve an arm without a successful durable Think run",
            status=run.get("status"),
        )
    return UUID(str(run["id"]))


async def _find_governed_corruption_candidate(
    conn: asyncpg.Connection, *, tenant_id: UUID,
) -> UUID | None:
    """Find a production-admitted optimistic claim, without consulting P6 gold."""

    return await conn.fetchval(
        """SELECT id FROM accepted_current_models
           WHERE tenant_id=$1
             AND (natural_text ILIKE '% is ready%' OR proposition::text ILIKE '%ready%')
           ORDER BY truth_advanced_at DESC,id LIMIT 1""",
        tenant_id,
    )


async def _run_arm(
    *,
    pool: asyncpg.Pool,
    runtime: P7ArmRuntime,
    population: P6Population,
    per_batch_timeout_s: float,
) -> dict[str, Any]:
    from scripts.run_1000_signal_model_layer_probe import enqueue_t1_for_observations
    from scripts.run_storyline_batch_benchmark import _process_one_t1_batch

    waves: list[dict[str, Any]] = []
    corruption_model_ids: frozenset[UUID] = frozenset()
    corruption_injected_batch: int | None = None
    for batch in population.batches:
        started = time.monotonic()
        async with pool.acquire() as conn:
            observation_ids = await _persist_runtime_batch(
                conn, tenant_id=runtime.tenant_id, batch=batch
            )
        execution: dict[str, Any] | None = None
        receipts: tuple[Any, ...] = ()
        if arm_allows_reasoning(runtime.arm, batch.batch_number):
            assert runtime.worker is not None
            await enqueue_t1_for_observations(
                pool,
                tenant_id=runtime.tenant_id,
                observation_ids=list(observation_ids.values()),
                limit=len(observation_ids),
                run_id=f"p7-{runtime.arm}-batch-{batch.batch_number}",
            )
            policy = "hide_models" if runtime.arm == "memory_hidden" else "normal"
            async with production_retrieval_policy(policy):
                async with asyncio.timeout(per_batch_timeout_s):
                    execution = await _process_one_t1_batch(
                        pool,
                        runtime.worker,
                        tenant_id=runtime.tenant_id,
                        force_window_elapsed_s=1.0,
                        retry_attempts=0,
                    )
            run_id = _run_id(execution)
            async with pool.acquire() as conn:
                receipts = await bridge_validated_think_lifecycle(
                    conn,
                    tenant_id=runtime.tenant_id,
                    arm=runtime.arm,
                    batch_number=batch.batch_number,
                    think_run_id=run_id,
                    corruption_model_ids=corruption_model_ids,
                    corruption_injected_batch=corruption_injected_batch or 4,
                )
                # The corrupted arm accepts only a claim admitted by production
                # Think from ordinary signals. The evaluator merely registers
                # its identity for prospective recovery measurement.
                if runtime.arm == "corrupted" and not corruption_model_ids:
                    candidate = await _find_governed_corruption_candidate(
                        conn, tenant_id=runtime.tenant_id
                    )
                    if candidate is not None:
                        corruption_model_ids = frozenset({candidate})
                        corruption_injected_batch = batch.batch_number
        snapshot = await _snapshot(pool, runtime.tenant_id)
        waves.append({
            "batch_number": batch.batch_number,
            "reasoning_executed": execution is not None,
            "retrieval_policy": (
                "hide_models" if runtime.arm == "memory_hidden" else "normal"
            ) if execution is not None else "not_executed",
            "think_run_id": str(_run_id(execution)) if execution else None,
            "lifecycle_receipts": [receipt.model_dump(mode="json") for receipt in receipts],
            "accepted_model_count": len(snapshot["accepted_models"]),
            "elapsed_s": round(time.monotonic() - started, 3),
        })
    recovered = any(
        receipt["within_two_batch_recovery_bound"] is True
        for wave in waves for receipt in wave["lifecycle_receipts"]
    )
    if runtime.arm == "corrupted" and not corruption_model_ids:
        raise InvariantViolation(
            "P7_CORRUPTION_INTERVENTION_NOT_ADMITTED",
            "production Think admitted no observable optimistic claim for the corrupted arm",
        )
    expected_reasoning_batches = 0 if runtime.arm == "observation_only" else (
        3 if runtime.arm == "frozen" else 12
    )
    reasoning_batch_count = sum(wave["reasoning_executed"] for wave in waves)
    arm_contract_satisfied = (
        len(waves) == 12
        and reasoning_batch_count == expected_reasoning_batches
        and (runtime.arm != "corrupted" or recovered)
    )
    return {
        "arm": runtime.arm,
        "tenant_id": str(runtime.tenant_id),
        "completed_batches": len(waves),
        "reasoning_batch_count": reasoning_batch_count,
        "expected_reasoning_batches": expected_reasoning_batches,
        "arm_contract_satisfied": arm_contract_satisfied,
        "waves": waves,
        "corruption_model_ids": tuple(map(str, corruption_model_ids)),
        "corruption_injected_batch": corruption_injected_batch,
        "corruption_recovered_within_two_batches": recovered,
        "frozen_outputs": await _snapshot(pool, runtime.tenant_id),
    }


async def run_p7_production_staged(
    *,
    database_url: str,
    population: P6Population,
    per_batch_timeout_s: float = 180.0,
) -> dict[str, Any]:
    """Run five isolated production arms concurrently, each ordered 1 through 12."""

    pool = await asyncpg.create_pool(
        database_url, min_size=5, max_size=12, init=_init_p6_connection
    )
    embedder = OllamaClient(OllamaConfig.from_env())
    set_response_cache(None)
    provider = build_provider()
    runtimes: list[P7ArmRuntime] = []
    try:
        for arm in P7_ARMS:
            tenant_id = uuid4()
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO tenants(id,name,is_demo) VALUES($1,$2,FALSE)",
                    tenant_id,
                    f"p7-production-{arm}-{tenant_id}",
                )
            worker = None
            if arm != "observation_only":
                worker = ThinkWorker(
                    pool,
                    config=WorkerConfig(
                        poll_batch=30,
                        max_concurrency_per_tenant=1,
                        tenant_filter=tenant_id,
                        worker_id=f"p7-{arm}-{tenant_id}",
                        t1_batch_window_s=1.0,
                        t1_batch_min_size=25,
                        t1_batch_max_size=25,
                        run_timeout_s=per_batch_timeout_s,
                        process_background_triggers=False,
                    ),
                    llm_provider=provider,
                    mention_discovery_provider=provider,
                    embedder=embedder,
                )
            runtimes.append(P7ArmRuntime(arm=arm, tenant_id=tenant_id, worker=worker))
        results = await asyncio.gather(*(
            _run_arm(
                pool=pool,
                runtime=runtime,
                population=population,
                per_batch_timeout_s=per_batch_timeout_s,
            )
            for runtime in runtimes
        ))
        return {
            "schema_version": "epistemic-repair-p7-production-staged-v1",
            "population_version": population.version,
            "population_digest": population.population_digest,
            "gold_visible_during_execution": False,
            "arm_results": results,
            "complete": all(result["arm_contract_satisfied"] for result in results),
        }
    finally:
        await close_codex_app_server_client()
        await embedder.close()
        await pool.close()


__all__ = ["P7_ARMS", "P7ArmRuntime", "run_p7_production_staged"]
