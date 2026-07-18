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
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import asyncpg

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.evaluation.epistemic_repair.p6_population import P6Batch, P6Population
from services.evaluation.epistemic_repair.p6_think_runner import (
    _init_p6_connection,
    _persist_runtime_batch,
    _snapshot,
)
from services.evaluation.epistemic_repair.p7_evolution import (
    P7EvolutionArm,
    arm_allows_reasoning,
    bridge_validated_think_lifecycle,
)
from services.evaluation.epistemic_repair.p7_retrieval_policy import (
    production_retrieval_policy,
)
from services.evaluation.epistemic_repair.p7_bootstrap_clone import (
    BootstrapCassette,
    checkpoint_digest,
    clone_receipt,
)
from lib.llm.provider import build_provider, close_codex_app_server_client, set_response_cache
from lib.shared.errors import InvariantViolation
from services.reasoning.think.worker import ThinkWorker, WorkerConfig
from services.reasoning.think.execution_policy import (
    NORMAL_EXECUTION_POLICY,
    issue_evaluation_validate_only_policy,
)
from services.reasoning.retrieval.config import CONFIG as RETRIEVAL_CONFIG


P7_ARMS: tuple[P7EvolutionArm, ...] = (
    "adaptive", "frozen", "observation_only", "memory_hidden", "corrupted"
)
P7_ATTEMPT_TIMEOUT_S = 300.0
P7_BATCH_DEADLINE_S = 650.0
P7_MAX_ATTEMPTS = 2


def _arm_tenant_id(
    *, execution_id: UUID, world_id: str, arm: P7EvolutionArm,
    population_digest: str,
) -> UUID:
    """Keep preregistered world/arm membership stable across orchestration retries."""

    return uuid5(
        NAMESPACE_URL,
        f"fyralis:p7:{execution_id}:{population_digest}:{world_id}:{arm}",
    )


def _validate_deadlines(*, attempt_timeout_s: float, batch_deadline_s: float) -> None:
    """Prevent the outer envelope from shadowing the one bounded retry."""

    if attempt_timeout_s <= 0:
        raise ValueError("attempt_timeout_s must be positive")
    if batch_deadline_s <= attempt_timeout_s * P7_MAX_ATTEMPTS:
        raise ValueError(
            "batch_deadline_s must exceed the two-attempt timeout budget"
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
    duplicate_attempt_ids = sorted({
        str(row.get("physical_attempt_id"))
        for row in attempt_receipts
        if sum(
            str(other.get("physical_attempt_id"))
            == str(row.get("physical_attempt_id"))
            for other in attempt_receipts
        ) > 1
    })
    over_budget_calls = [
        str(row.get("logical_call_id")) for row in logical_receipts
        if int(row.get("physical_attempt_count") or 0) > P7_MAX_ATTEMPTS
    ]
    nonreported_usage = [
        str(row.get("physical_attempt_id")) for row in attempt_receipts
        if row.get("usage_exactness") != "reported"
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
    if duplicate_attempt_ids:
        errors.append("duplicate physical attempt receipts")
    if over_budget_calls:
        errors.append("logical calls exceed the two-attempt budget")
    if nonreported_usage:
        errors.append("Codex economics require provider-reported token usage")
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
        "duplicate_attempt_ids": duplicate_attempt_ids,
        "over_budget_logical_calls": over_budget_calls,
        "nonreported_usage_attempts": nonreported_usage,
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
                  input_tokens,output_tokens,usage_exactness
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
    batch_deadline_s: float,
    required_provider: str,
    required_model: str,
    batches: tuple[P6Batch, ...] | None = None,
    initial_waves: tuple[dict[str, Any], ...] = (),
    finalize: bool = True,
) -> dict[str, Any]:
    from scripts.run_1000_signal_model_layer_probe import enqueue_t1_for_observations
    from scripts.run_storyline_batch_benchmark import _process_one_t1_batch

    waves: list[dict[str, Any]] = list(initial_waves)
    corruption_model_ids: frozenset[UUID] = frozenset()
    corruption_injected_batch: int | None = None
    for batch in batches if batches is not None else population.batches:
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
            policy = (
                "hide_models"
                if runtime.arm in {"memory_hidden", "observation_only"}
                else "normal"
            )
            async with production_retrieval_policy(policy):
                async with asyncio.timeout(batch_deadline_s):
                    execution = await _process_one_t1_batch(
                        pool,
                        runtime.worker,
                        tenant_id=runtime.tenant_id,
                        force_window_elapsed_s=1.0,
                        retry_attempts=P7_MAX_ATTEMPTS - 1,
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
                              model.truth_semantic_digest,
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
                              relation.truth_semantic_digest,
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
                validate_only_rows = await conn.fetch(
                    """SELECT id,validation_result FROM think_runs
                       WHERE tenant_id=$1 AND status='success'
                         AND execution_mode='validate_only'
                       ORDER BY started_at,id""",
                    runtime.tenant_id,
                )
                write_counts = {
                    "canonical_model_versions": int(await conn.fetchval(
                        "SELECT count(*) FROM model_truth_versions WHERE tenant_id=$1",
                        runtime.tenant_id,
                    )),
                    "canonical_relation_versions": int(await conn.fetchval(
                        "SELECT count(*) FROM relation_truth_versions WHERE tenant_id=$1",
                        runtime.tenant_id,
                    )),
                    "derived_relation_projections": int(await conn.fetchval(
                        "SELECT count(*) FROM relation_edge_projections WHERE tenant_id=$1",
                        runtime.tenant_id,
                    )),
                    "derived_projection_snapshots": int(await conn.fetchval(
                        "SELECT count(*) FROM projection_snapshots WHERE tenant_id=$1",
                        runtime.tenant_id,
                    )),
                }
            stage_snapshot = {
                **snapshot,
                "accepted_models": [dict(row) for row in model_rows],
                "accepted_relations": [dict(row) for row in relation_rows],
                "validated_only_runs": [dict(row) for row in validate_only_rows],
                "write_counts": write_counts,
            }
        else:
            stage_snapshot = None
        waves.append({
            "batch_number": batch.batch_number,
            "reasoning_executed": execution is not None,
            "retrieval_policy": (
                "hide_models"
                if runtime.arm in {"memory_hidden", "observation_only"}
                else "normal"
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
    if finalize and runtime.arm == "corrupted" and not corruption_model_ids:
        raise InvariantViolation(
            "P7_CORRUPTION_INTERVENTION_NOT_ADMITTED",
            "production Think admitted no observable optimistic claim for the corrupted arm",
        )
    expected_reasoning_batches = 3 if runtime.arm == "frozen" else 12
    reasoning_batch_count = sum(wave["reasoning_executed"] for wave in waves)
    arm_contract_satisfied = finalize and (
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
        "budget_contract": {
            "provider": required_provider,
            "model": required_model,
            "context_budget_tokens": int(RETRIEVAL_CONFIG.context_budget_tokens),
            "provider_max_retries": int(
                getattr(getattr(runtime.worker, "llm_provider", None), "config", None).max_retries
            ),
            "batch_signal_count": 25,
            "attempt_timeout_s": float(runtime.worker.config.run_timeout_s),
            "max_attempts": P7_MAX_ATTEMPTS,
            "batch_deadline_s": float(batch_deadline_s),
        },
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
    attempt_timeout_s: float = P7_ATTEMPT_TIMEOUT_S,
    batch_deadline_s: float = P7_BATCH_DEADLINE_S,
    required_provider: str = "codex",
    required_model: str = "gpt-5.4",
    world_id: str = "p7-world-01",
    execution_id: UUID | None = None,
) -> dict[str, Any]:
    """Run five isolated production arms concurrently, each ordered 1 through 12."""

    _validate_deadlines(
        attempt_timeout_s=attempt_timeout_s, batch_deadline_s=batch_deadline_s,
    )
    execution_id = execution_id or uuid4()

    pool = await asyncpg.create_pool(
        database_url, min_size=5, max_size=12, init=_init_p6_connection
    )
    embedder = OllamaClient(OllamaConfig.from_env())
    set_response_cache(None)
    provider = build_provider()
    provider_config = getattr(provider, "config", None)
    observed_provider = str(getattr(provider_config, "provider", ""))
    observed_model = str(getattr(provider_config, "model", ""))
    observed_max_retries = int(getattr(provider_config, "max_retries", -1))
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
    if observed_max_retries != P7_MAX_ATTEMPTS - 1:
        await embedder.close()
        await pool.close()
        raise InvariantViolation(
            "P7_PROVIDER_RETRY_BUDGET_MISMATCH",
            "P7 requires exactly one bounded retry per logical provider call",
            expected_max_retries=P7_MAX_ATTEMPTS - 1,
            observed_max_retries=observed_max_retries,
        )
    runtimes: list[P7ArmRuntime] = []
    try:
        for arm in P7_ARMS:
            tenant_id = _arm_tenant_id(
                execution_id=execution_id, world_id=world_id, arm=arm,
                population_digest=population.population_digest,
            )
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO tenants(id,name,is_demo) VALUES($1,$2,FALSE)",
                    tenant_id,
                    f"p7-production-{world_id}-{arm}-{tenant_id}",
                )
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
                        run_timeout_s=attempt_timeout_s,
                        process_background_triggers=False,
                    ),
                    llm_provider=provider,
                    mention_discovery_provider=provider,
                    embedder=embedder,
                    execution_policy=(
                        issue_evaluation_validate_only_policy()
                        if arm == "observation_only"
                        else NORMAL_EXECUTION_POLICY
                    ),
                )
            runtimes.append(P7ArmRuntime(arm=arm, tenant_id=tenant_id, worker=worker))
        # Establish exactly one real adaptive bootstrap, then deterministically
        # replay its provider transcript into each isolated tenant.  Interventions
        # start only after an equality digest proves the batch-3 canonical state
        # is identical across arms.
        bootstrap_batches = tuple(
            batch for batch in population.batches if batch.batch_number <= 3
        )
        intervention_batches = tuple(
            batch for batch in population.batches if batch.batch_number > 3
        )
        adaptive_runtime = next(item for item in runtimes if item.arm == "adaptive")
        cassette = BootstrapCassette()
        async with cassette.record(provider):
            adaptive_bootstrap = await _run_arm(
                pool=pool, runtime=adaptive_runtime, population=population,
                batch_deadline_s=batch_deadline_s,
                required_provider=required_provider, required_model=required_model,
                batches=bootstrap_batches, finalize=False,
            )
        source_snapshot = adaptive_bootstrap["waves"][-1]["stage_snapshot"]
        source_digest = checkpoint_digest(source_snapshot)
        bootstrap_waves: dict[P7EvolutionArm, tuple[dict[str, Any], ...]] = {
            "adaptive": tuple(adaptive_bootstrap["waves"]),
        }
        clone_receipts = {
            "adaptive": clone_receipt(
                source_tenant_id=str(adaptive_runtime.tenant_id),
                target_tenant_id=str(adaptive_runtime.tenant_id),
                source_digest=source_digest, target_digest=source_digest,
                cassette=cassette,
            )
        }
        for runtime in runtimes:
            if runtime.arm == "adaptive":
                continue
            assert runtime.worker is not None
            intervention_policy = runtime.worker.execution_policy
            runtime.worker.execution_policy = NORMAL_EXECUTION_POLICY
            try:
                bootstrap_runtime = P7ArmRuntime(
                    arm="adaptive", tenant_id=runtime.tenant_id, worker=runtime.worker,
                )
                async with cassette.replay(provider):
                    replayed = await _run_arm(
                        pool=pool, runtime=bootstrap_runtime, population=population,
                        batch_deadline_s=batch_deadline_s,
                        required_provider=required_provider,
                        required_model=required_model,
                        batches=bootstrap_batches, finalize=False,
                    )
            finally:
                runtime.worker.execution_policy = intervention_policy
            target_digest = checkpoint_digest(
                replayed["waves"][-1]["stage_snapshot"]
            )
            receipt = clone_receipt(
                source_tenant_id=str(adaptive_runtime.tenant_id),
                target_tenant_id=str(runtime.tenant_id),
                source_digest=source_digest, target_digest=target_digest,
                cassette=cassette,
            )
            if not receipt.equality_proven:
                raise InvariantViolation(
                    "P7_BOOTSTRAP_CHECKPOINT_MISMATCH",
                    "replayed arm did not reproduce the adaptive batch-3 checkpoint",
                    arm=runtime.arm,
                    source_digest=source_digest,
                    target_digest=target_digest,
                )
            bootstrap_waves[runtime.arm] = tuple(replayed["waves"])
            clone_receipts[runtime.arm] = receipt

        results = await asyncio.gather(*(
            _run_arm(
                pool=pool, runtime=runtime, population=population,
                batch_deadline_s=batch_deadline_s,
                required_provider=required_provider,
                required_model=required_model,
                batches=intervention_batches,
                initial_waves=bootstrap_waves[runtime.arm],
            )
            for runtime in runtimes
        ))
        results = [
            {
                **result,
                "bootstrap_clone_receipt": clone_receipts[result["arm"]].model_dump(
                    mode="json"
                ),
            }
            for result in results
        ]
        return {
            "schema_version": "epistemic-repair-p7-production-staged-v1",
            "population_version": population.version,
            "population_digest": population.population_digest,
            "world_id": world_id,
            "execution_id": str(execution_id),
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
    attempt_timeout_s: float = P7_ATTEMPT_TIMEOUT_S,
    batch_deadline_s: float = P7_BATCH_DEADLINE_S,
    required_provider: str = "codex",
    required_model: str = "gpt-5.4",
    execution_id: UUID | None = None,
) -> dict[str, Any]:
    """Execute preregistered world variants concurrently with isolated tenants."""

    if len(worlds) < 3 or len({world_id for world_id, _ in worlds}) != len(worlds):
        raise InvariantViolation(
            "P7_WORLD_POPULATION_INVALID",
            "P7 requires at least three unique preregistered world variants",
            world_count=len(worlds),
        )
    execution_id = execution_id or uuid4()
    results = await asyncio.gather(*(
        run_p7_production_staged(
            database_url=database_url,
            population=stream,
            attempt_timeout_s=attempt_timeout_s,
            batch_deadline_s=batch_deadline_s,
            required_provider=required_provider,
            required_model=required_model,
            world_id=world_id,
            execution_id=execution_id,
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
        "execution_id": str(execution_id),
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
    "P7_ATTEMPT_TIMEOUT_S",
    "P7_BATCH_DEADLINE_S",
    "P7_MAX_ATTEMPTS",
    "P7ArmRuntime",
    "P7SealedExecutionStream",
    "assess_provider_identity_receipts",
    "run_p7_production_staged",
    "run_p7_production_worlds",
    "seal_execution_stream",
]
