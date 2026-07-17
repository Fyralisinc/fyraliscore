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
from lib.evaluation.epistemic_repair.p6_population import P6Batch, P6Population
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


@dataclass(frozen=True, slots=True)
class P7SealedExecutionStream:
    """Gold-free transport object accepted by the production executor."""

    version: str
    population_digest: str
    batches: tuple[P6Batch, ...]


def seal_execution_stream(population: P6Population) -> P7SealedExecutionStream:
    return P7SealedExecutionStream(
        version=population.version,
        population_digest=population.population_digest,
        batches=population.batches,
    )


def assess_provider_identity_receipts(
    *,
    logical_receipts: list[dict[str, Any]],
    attempt_receipts: list[dict[str, Any]],
    required_provider: str,
    required_model: str,
) -> dict[str, Any]:
    """Reconcile every durable logical call and physical attempt identity."""

    missing_identity = [
        f"logical:{row.get('logical_call_id')}"
        for row in logical_receipts
        if not row.get("provider") or not row.get("model") or not row.get("purpose")
    ] + [
        f"attempt:{row.get('physical_attempt_id')}"
        for row in attempt_receipts
        if not row.get("provider") or not row.get("model") or not row.get("purpose")
    ]
    mismatches = [
        {
            "receipt_kind": "logical",
            "receipt_id": str(row.get("logical_call_id")),
            "provider": row.get("provider"),
            "model": row.get("model"),
            "purpose": row.get("purpose"),
        }
        for row in logical_receipts
        if (row.get("provider"), row.get("model"))
        != (required_provider, required_model)
    ] + [
        {
            "receipt_kind": "physical_attempt",
            "receipt_id": str(row.get("physical_attempt_id")),
            "provider": row.get("provider"),
            "model": row.get("model"),
            "purpose": row.get("purpose"),
        }
        for row in attempt_receipts
        if (row.get("provider"), row.get("model"))
        != (required_provider, required_model)
    ]
    declared_attempts = sum(int(row.get("physical_attempt_count") or 0) for row in logical_receipts)
    call_ids = {str(row.get("logical_call_id")) for row in logical_receipts}
    orphan_attempts = [
        str(row.get("physical_attempt_id"))
        for row in attempt_receipts
        if str(row.get("logical_call_id")) not in call_ids
    ]
    errors = []
    if not logical_receipts or not attempt_receipts:
        errors.append("missing durable logical or physical provider receipts")
    if declared_attempts != len(attempt_receipts):
        errors.append(
            f"logical physical_attempt_count={declared_attempts} but attempts={len(attempt_receipts)}"
        )
    if missing_identity:
        errors.append("receipts lack provider/model/purpose identity")
    if mismatches:
        errors.append("provider/model identity mismatch")
    if orphan_attempts:
        errors.append("physical attempts lack a matching logical receipt")
    return {
        "valid": not errors,
        "required_provider": required_provider,
        "required_model": required_model,
        "logical_call_count": len(logical_receipts),
        "physical_attempt_count": len(attempt_receipts),
        "input_tokens": sum(int(row.get("input_tokens") or 0) for row in attempt_receipts),
        "output_tokens": sum(int(row.get("output_tokens") or 0) for row in attempt_receipts),
        "purposes": sorted({str(row.get("purpose")) for row in attempt_receipts}),
        "missing_identity_receipts": missing_identity,
        "identity_mismatches": mismatches,
        "orphan_attempts": orphan_attempts,
        "errors": errors,
    }


async def _validate_provider_identity_ledger(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    required_provider: str,
    required_model: str,
) -> dict[str, Any]:
    logical = [dict(row) for row in await conn.fetch(
        """SELECT logical_call_id,provider,model,purpose,physical_attempt_count
           FROM llm_logical_call_receipts WHERE tenant_id=$1""",
        tenant_id,
    )]
    attempts = [dict(row) for row in await conn.fetch(
        """SELECT physical_attempt_id,logical_call_id,provider,model,purpose,
                  input_tokens,output_tokens
           FROM llm_provider_attempt_receipts WHERE tenant_id=$1""",
        tenant_id,
    )]
    assessment = assess_provider_identity_receipts(
        logical_receipts=logical,
        attempt_receipts=attempts,
        required_provider=required_provider,
        required_model=required_model,
    )
    if not assessment["valid"]:
        raise InvariantViolation(
            "P7_PROVIDER_IDENTITY_LEDGER_INVALID",
            "durable provider receipts do not prove a single preregistered model",
            tenant_id=str(tenant_id),
            assessment=assessment,
        )
    return assessment


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
    population: P7SealedExecutionStream,
    per_batch_timeout_s: float,
    required_provider: str,
    required_model: str,
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
                provider_identity = await _validate_provider_identity_ledger(
                    conn,
                    tenant_id=runtime.tenant_id,
                    required_provider=required_provider,
                    required_model=required_model,
                )
        else:
            provider_identity = None
        snapshot = await _snapshot(pool, runtime.tenant_id)
        if batch.batch_number in {3, 6, 10, 12}:
            async with pool.acquire() as conn:
                model_rows = await conn.fetch(
                    """SELECT model.id,model.truth_version_id,model.truth_version,
                              model.proposition,model.natural_text,model.confidence,
                              model.scope_entities,model.truth_lifecycle,
                              model.truth_advanced_at,
                              COALESCE((SELECT array_agg(ref.evidence_id ORDER BY ref.evidence_id)
                                FROM model_truth_evidence_references ref
                                WHERE ref.tenant_id=model.tenant_id
                                  AND ref.model_version_id=model.truth_version_id
                                  AND ref.evidence_kind='observation'), ARRAY[]::text[])
                                AS evidence_observation_ids
                       FROM accepted_current_models model WHERE model.tenant_id=$1
                       ORDER BY truth_advanced_at,id""",
                    runtime.tenant_id,
                )
                relation_rows = await conn.fetch(
                    """SELECT relation.id,relation.truth_relation_kind,
                              relation.truth_rationale,
                              COALESCE((SELECT jsonb_agg(jsonb_build_object(
                                  'model_id',participant.model_id,
                                  'participant_role',participant.role
                                ) ORDER BY participant.role,participant.model_id)
                                FROM relation_truth_participants participant
                                WHERE participant.tenant_id=relation.tenant_id
                                  AND participant.relation_version_id=relation.truth_relation_version_id),
                                '[]'::jsonb) AS participants,
                              COALESCE((SELECT array_agg(evidence.evidence_id ORDER BY evidence.evidence_id)
                                FROM relation_truth_evidence evidence
                                WHERE evidence.tenant_id=relation.tenant_id
                                  AND evidence.relation_version_id=relation.truth_relation_version_id),
                                ARRAY[]::text[]) AS evidence_ids
                       FROM accepted_current_relations relation WHERE relation.tenant_id=$1
                       ORDER BY truth_advanced_at,id""",
                    runtime.tenant_id,
                )
            stage_snapshot = {
                **snapshot,
                "accepted_models": [dict(row) for row in model_rows],
                "accepted_relations": [dict(row) for row in relation_rows],
            }
        else:
            stage_snapshot = None
        waves.append({
            "batch_number": batch.batch_number,
            "reasoning_executed": execution is not None,
            "retrieval_policy": (
                "hide_models" if runtime.arm == "memory_hidden" else "normal"
            ) if execution is not None else "not_executed",
            "think_run_id": str(_run_id(execution)) if execution else None,
            "lifecycle_receipts": [receipt.model_dump(mode="json") for receipt in receipts],
            "provider_identity_ledger": provider_identity,
            "accepted_model_count": len(snapshot["accepted_models"]),
            "stage_snapshot": stage_snapshot,
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
    population: P7SealedExecutionStream,
    per_batch_timeout_s: float = 180.0,
    required_provider: str = "codex",
    required_model: str = "gpt-5.4",
    world_id: str = "p7-world-01",
) -> dict[str, Any]:
    """Run five isolated production arms concurrently, each ordered 1 through 12."""

    pool = await asyncpg.create_pool(
        database_url, min_size=5, max_size=12, init=_init_p6_connection
    )
    embedder = OllamaClient(OllamaConfig.from_env())
    set_response_cache(None)
    provider = build_provider()
    provider_config = getattr(provider, "config", None)
    observed_provider = str(getattr(provider_config, "provider", ""))
    observed_model = str(getattr(provider_config, "model", ""))
    if (observed_provider, observed_model) != (required_provider, required_model):
        await embedder.close()
        await pool.close()
        raise InvariantViolation(
            "P7_PROVIDER_IDENTITY_MISMATCH",
            "every P7 LLM role must use the preregistered provider and model",
            expected_provider=required_provider,
            expected_model=required_model,
            observed_provider=observed_provider,
            observed_model=observed_model,
        )
    runtimes: list[P7ArmRuntime] = []
    try:
        for arm in P7_ARMS:
            tenant_id = uuid4()
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO tenants(id,name,is_demo) VALUES($1,$2,FALSE)",
                    tenant_id,
                    f"p7-production-{world_id}-{arm}-{tenant_id}",
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
                required_provider=required_provider,
                required_model=required_model,
            )
            for runtime in runtimes
        ))
        return {
            "schema_version": "epistemic-repair-p7-production-staged-v1",
            "population_version": population.version,
            "population_digest": population.population_digest,
            "world_id": world_id,
            "gold_visible_during_execution": False,
            "provider_identity": {
                "provider": observed_provider,
                "model": observed_model,
                "question_planner_uses_same_provider_instance": True,
                "question_planner_fallback_disabled": True,
            },
            "arm_results": results,
            "complete": all(result["arm_contract_satisfied"] for result in results),
        }
    finally:
        await close_codex_app_server_client()
        await embedder.close()
        await pool.close()


async def run_p7_production_worlds(
    *,
    database_url: str,
    worlds: tuple[tuple[str, P7SealedExecutionStream], ...],
    per_batch_timeout_s: float = 180.0,
    required_provider: str = "codex",
    required_model: str = "gpt-5.4",
) -> dict[str, Any]:
    """Execute preregistered world variants concurrently with isolated tenants."""

    if len(worlds) < 3 or len({world_id for world_id, _ in worlds}) != len(worlds):
        raise InvariantViolation(
            "P7_WORLD_POPULATION_INVALID",
            "P7 requires at least three unique preregistered world variants",
            world_count=len(worlds),
        )
    results = await asyncio.gather(*(
        run_p7_production_staged(
            database_url=database_url,
            population=stream,
            per_batch_timeout_s=per_batch_timeout_s,
            required_provider=required_provider,
            required_model=required_model,
            world_id=world_id,
        )
        for world_id, stream in worlds
    ))
    tenant_ids = [
        arm["tenant_id"]
        for result in results
        for arm in result["arm_results"]
    ]
    return {
        "schema_version": "epistemic-repair-p7-production-worlds-v1",
        "world_count": len(results),
        "arm_execution_count": len(tenant_ids),
        "isolated_tenant_count": len(set(tenant_ids)),
        "provider": required_provider,
        "model": required_model,
        "gold_visible_during_execution": False,
        "world_results": results,
        "complete": (
            len(set(tenant_ids)) == len(tenant_ids) == len(results) * len(P7_ARMS)
            and all(result["complete"] for result in results)
        ),
    }


__all__ = [
    "P7_ARMS",
    "P7ArmRuntime",
    "P7SealedExecutionStream",
    "assess_provider_identity_receipts",
    "run_p7_production_staged",
    "run_p7_production_worlds",
    "seal_execution_stream",
]
