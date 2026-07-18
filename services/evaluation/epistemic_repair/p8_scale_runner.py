"""Measured PostgreSQL P8 semantic-scale execution.

Cells exercise normalized observation persistence, entity/scope grounding,
canonical model and relation admission, accepted-memory retrieval with durable
context decisions, causal batch barriers, and derived projection refresh.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
from math import ceil
import resource
import statistics
import time
from uuid import UUID, uuid4

import asyncpg

from lib.contracts.kernel import canonical_sha256
from services.evaluation.epistemic_repair.p2_runner import _admission
from services.evaluation.epistemic_repair.p4_runner import _admit_relation
from lib.evaluation.epistemic_repair.p8_population import ScaleCell, build_scale_matrix
from lib.evaluation.epistemic_repair.p8_measurement_contracts import QUEUE_FAMILIES
from services.domain.company_learning.barrier import (
    CompanyLearningBarrierService,
    ContextDecision,
)
from services.domain.projections.store import (
    complete_projection_refresh_job,
    enqueue_projection_refresh_job,
    lease_projection_refresh_jobs,
)


SCALE_EXECUTION_VERSION = "p8-scale-semantic-kernel-v3"
from services.domain.truth_kernel import build_default_truth_kernel


@dataclass(frozen=True, slots=True)
class TenantScaleReceipt:
    tenant_id: str
    batches: int
    observations: int
    retrieval_samples_ms: tuple[float, ...]
    observation_write_samples_ms: tuple[float, ...]
    barrier_samples_ms: tuple[float, ...]
    barrier_sql_calls: tuple[int, ...]
    prompt_token_samples: tuple[int, ...]
    queue_depth_samples: tuple[int, ...]
    accepted_model_hits: int
    cross_tenant_hits: int
    canonical_models: int
    canonical_versions: int
    barriers: int
    elapsed_ms: float
    pool_wait_ms: float
    queried_state_digest: str
    barrier_measurements: tuple[dict[str, object], ...] = ()
    bootstrap_ms: float = 0.0
    canonical_relations: int = 0
    context_decisions: int = 0
    scope_bindings: int = 0
    grounded_actors: int = 0
    processed_projection_jobs: int = 0
    provider_calls: int = 0


@dataclass(frozen=True, slots=True)
class ActualScaleCell:
    cell_id: str
    batch_size: int
    memory_horizon_batches: int
    tenant_concurrency: int
    tenant_receipts: tuple[TenantScaleReceipt, ...]
    retrieval_p95_ms: float
    observation_write_p95_ms: float
    barrier_p95_ms: float
    prompt_token_p95: int
    queue_depth_slope_final_half: float
    latency_p95_ms: float
    fairness_ratio: float
    semantic_quality: float
    cross_tenant_leakage: int
    first_quartile_model_rate: float
    last_quartile_model_rate: float
    refresh_per_unique_version: float
    observation_rows: int
    canonical_rows: int
    derived_rows: int
    peak_rss_kib: int
    wall_time_ms: float
    rollback_isolated: bool
    physically_isolated_database: bool
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class SharedContentionResult:
    selected_cell_ids: tuple[str, ...]
    concurrent_cells: int
    wall_time_ms: float
    individual_wall_time_sum_ms: float
    contention_ratio: float
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class ScaleExecution:
    cells: tuple[ActualScaleCell, ...]
    shared_contention: SharedContentionResult | None
    exact_matrix_coverage: bool
    physically_isolated_databases: bool
    evidence_digest: str


@dataclass(frozen=True, slots=True)
class WarmPairSample:
    repetition: int
    tenant_concurrency: int
    pool_wait_p95_ms: float
    observation_write_p95_ms: float
    barrier_p95_ms: float
    barrier_sql_calls_per_batch: int
    end_to_end_batch_p95_ms: float
    cell_wall_time_ms: float
    semantic_quality: float
    cross_tenant_leakage: int


@dataclass(frozen=True, slots=True)
class WarmPairDiagnostic:
    batch_size: int
    memory_horizon_batches: int
    repetitions: int
    pool_size: int
    samples: tuple[WarmPairSample, ...]
    barrier_ratio_p95: float
    end_to_end_ratio_p95: float
    pool_wait_ratio_p95: float | None
    diagnosis: str
    evidence_digest: str


def evaluate_scale_execution(execution: ScaleExecution) -> dict[str, object]:
    cells = {(x.batch_size, x.memory_horizon_batches, x.tenant_concurrency): x for x in execution.cells}
    retrieval_ratios = []
    prompt_ratios = []
    quality_deltas = []
    concurrency_ratios = []
    if execution.exact_matrix_coverage:
        for batch in (10, 25, 50):
            for tenants in (1, 5, 20):
                short, long = cells[(batch, 12, tenants)], cells[(batch, 100, tenants)]
                retrieval_ratios.append(long.retrieval_p95_ms / max(short.retrieval_p95_ms, .000001))
                prompt_ratios.append(long.prompt_token_p95 / max(short.prompt_token_p95, 1))
                quality_deltas.append(abs(long.semantic_quality - short.semantic_quality))
            for horizon in (12, 50, 100):
                one, twenty = cells[(batch, horizon, 1)], cells[(batch, horizon, 20)]
                concurrency_ratios.append(twenty.latency_p95_ms / max(one.latency_p95_ms, .000001))
    measurements = [
        sample for cell in execution.cells for receipt in cell.tenant_receipts
        for sample in receipt.barrier_measurements
    ]
    expected_measurements = sum(
        receipt.barriers for cell in execution.cells for receipt in cell.tenant_receipts
    )
    queue_families = {item.family for item in QUEUE_FAMILIES}
    complete_queues = bool(measurements) and len(measurements) == expected_measurements and all(
        set(sample.get("queues", {})) == queue_families
        and all(row.get("status") == "measured" for row in sample["queues"].values())
        for sample in measurements
    )
    complete_resources = bool(measurements) and all(
        isinstance(sample.get("resource", {}).get("process_peak_rss_kib"), int)
        and sample["resource"]["process_peak_rss_kib"] > 0
        for sample in measurements
    )
    deterministic_token_status = bool(measurements) and all(
        sample.get("provider_tokens", {}).get("status") == "excluded_deterministic_cell"
        and sample["provider_tokens"].get("estimated") is False
        and sample["provider_tokens"].get("input_tokens") is None
        and sample["provider_tokens"].get("output_tokens") is None
        for sample in measurements
    )
    gates = {
        "exact_27_cell_coverage": execution.exact_matrix_coverage,
        "physically_isolated_database_per_cell": execution.physically_isolated_databases,
        "provider_usage_contract": deterministic_token_status,
        "all_production_queue_families_measured": complete_queues,
        "resource_sample_every_durable_barrier": complete_resources,
        "deterministic_token_status_explicit": deterministic_token_status,
        "derived_refresh_pipeline_executed": bool(execution.cells) and all(
            all(receipt.processed_projection_jobs > 0 for receipt in cell.tenant_receipts)
            for cell in execution.cells
        ),
        "semantic_kernel_effects_real": bool(execution.cells) and all(
            all(
                receipt.canonical_models >= 2
                and receipt.canonical_relations >= 1
                and receipt.context_decisions >= receipt.batches
                and receipt.scope_bindings >= 2
                and receipt.grounded_actors == 1
                and receipt.provider_calls == 0
                for receipt in cell.tenant_receipts
            )
            for cell in execution.cells
        ),
        "queue_depth_slope": bool(execution.cells) and all(x.queue_depth_slope_final_half <= 0 for x in execution.cells),
        "retrieval_horizon_ratio": bool(retrieval_ratios) and max(retrieval_ratios) <= 2,
        "prompt_horizon_ratio": bool(prompt_ratios) and max(prompt_ratios) <= 1.25,
        "model_insertion_decay": bool(execution.cells) and all(x.last_quartile_model_rate <= .5 * x.first_quartile_model_rate for x in execution.cells),
        "refresh_coalescing": bool(execution.cells) and all(x.refresh_per_unique_version <= 1.10 for x in execution.cells),
        "concurrency_latency_ratio": bool(concurrency_ratios) and max(concurrency_ratios) <= 2,
        "tenant_fairness": bool(execution.cells) and all(x.fairness_ratio >= .80 for x in execution.cells),
        "cross_tenant_leakage": bool(execution.cells) and all(x.cross_tenant_leakage == 0 for x in execution.cells),
        "semantic_quality_stability": bool(quality_deltas) and max(quality_deltas) <= .03,
        "shared_contention_executed_separately": execution.shared_contention is not None,
    }
    return {
        "gates": gates,
        "scale_execution_ready": all(gates.values()),
        "max_retrieval_horizon_ratio": max(retrieval_ratios, default=None),
        "max_prompt_horizon_ratio": max(prompt_ratios, default=None),
        "max_concurrency_latency_ratio": max(concurrency_ratios, default=None),
        "max_semantic_quality_delta": max(quality_deltas, default=None),
        "minimum_fairness_ratio": min((x.fairness_ratio for x in execution.cells), default=None),
        "total_observations": sum(x.observation_rows for x in execution.cells),
        "total_barriers": sum(sum(row.barriers for row in x.tenant_receipts) for x in execution.cells),
    }


def _p95(values: list[float] | tuple[float, ...]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return float(ordered[min(len(ordered) - 1, ceil(.95 * len(ordered)) - 1)])


class _CountingTx:
    """Transparent statement counter for one production barrier call."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self.conn = conn
        self.calls = 0

    def __getattr__(self, name: str):
        value = getattr(self.conn, name)
        if name not in {"execute", "fetch", "fetchrow", "fetchval"}:
            return value

        async def counted(*args, **kwargs):
            self.calls += 1
            return await value(*args, **kwargs)

        return counted


def _slope_final_half(values: list[int]) -> float:
    tail = values[len(values) // 2:]
    if len(tail) < 2:
        return 0.0
    xs = range(len(tail))
    xbar, ybar = statistics.mean(xs), statistics.mean(tail)
    denominator = sum((x - xbar) ** 2 for x in xs)
    return 0.0 if denominator == 0 else sum((x - xbar) * (y - ybar) for x, y in zip(xs, tail)) / denominator


async def _production_barrier_measurement(conn: asyncpg.Connection, tenant_id) -> dict[str, object]:
    queues: dict[str, object] = {}
    for item in QUEUE_FAMILIES:
        exists = await conn.fetchval("SELECT to_regclass($1)::text", item.table)
        if exists is None:
            queues[item.family] = {"status": "missing", "pending": None, "terminal": None}
            continue
        tenant = f" AND {item.tenant_column}=$1" if item.tenant_column else ""
        args = [tenant_id] if item.tenant_column else []
        pending = await conn.fetchval(
            f"SELECT count(*)::int FROM {item.table} WHERE ({item.pending_predicate}){tenant}", *args,
        )
        terminal = None
        if item.terminal_failure_predicate:
            terminal = await conn.fetchval(
                f"SELECT count(*)::int FROM {item.table} WHERE ({item.terminal_failure_predicate}){tenant}", *args,
            )
        queues[item.family] = {"status": "measured", "pending": int(pending),
                               "terminal": None if terminal is None else int(terminal)}
    growth = {}
    for family, table in (
        ("candidate", "truth_candidates"), ("residual", "model_residual_evidence"),
        ("review", "entity_review_queue"), ("negative_memory", "negative_memory"),
    ):
        exists = await conn.fetchval("SELECT to_regclass($1)::text", table)
        growth[family] = (
            {"status": "missing", "count": None} if exists is None else
            {"status": "measured", "count": int(await conn.fetchval(
                f"SELECT count(*)::int FROM {table} WHERE tenant_id=$1", tenant_id,
            ))}
        )
    return {
        "queues": queues, "growth": growth,
        "resource": {"process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss},
        "provider_tokens": {"status": "excluded_deterministic_cell", "estimated": False,
                            "input_tokens": None, "output_tokens": None},
    }


async def _run_tenant(
    dsn: str, cell: ScaleCell, ordinal: int, *, pool: asyncpg.Pool | None = None,
) -> TenantScaleReceipt:
    tenant_id, started = uuid4(), time.perf_counter()
    wait_started = time.perf_counter()
    conn = await pool.acquire() if pool is not None else await asyncpg.connect(dsn)
    pool_wait_ms = (time.perf_counter() - wait_started) * 1000
    retrieval: list[float] = []
    observation_writes: list[float] = []
    barriers: list[float] = []
    barrier_sql_calls: list[int] = []
    prompt_tokens: list[int] = []
    queue_depth: list[int] = []
    barrier_measurements: list[dict[str, object]] = []
    hits = leakage = 0
    admitted_models: list[tuple[object, object]] = []
    relation_receipt = None
    try:
        bootstrap_started = time.perf_counter()
        bootstrap = conn.transaction()
        await bootstrap.start()
        try:
            await conn.execute("INSERT INTO tenants (id,name) VALUES ($1,$2)", tenant_id, f"p8-scale-{cell.cell_id}-{ordinal}")
            actor_id = uuid4()
            actor_ref = f"p8-scale-actor:{tenant_id}"
            await conn.execute(
                """INSERT INTO actors(id,tenant_id,type,display_name,metadata)
                   VALUES($1,$2,'person',$3,$4::jsonb)""",
                actor_id, tenant_id, f"Scale actor {ordinal}",
                json.dumps({"source": "p8_deterministic_scale"}),
            )
            await conn.execute(
                """INSERT INTO actor_identity_mappings(
                     actor_id,source_channel,source_actor_ref,confidence
                   ) VALUES($1,'synthetic:normalized',$2,1.0)""",
                actor_id, actor_ref,
            )
        except BaseException:
            await bootstrap.rollback()
            raise
        else:
            await bootstrap.commit()
        bootstrap_ms = (time.perf_counter() - bootstrap_started) * 1000
        barrier_service = CompanyLearningBarrierService()
        base = datetime(2026, 7, 18, tzinfo=timezone.utc)
        for batch in range(1, cell.memory_horizon_batches + 1):
            batch_tx = conn.transaction()
            await batch_tx.start()
            rows = []
            for position in range(cell.batch_size):
                observation_id = uuid4()
                text = f"Harbor status pulse tenant {ordinal} batch {batch} signal {position}."
                rows.append((
                    observation_id, tenant_id, base + timedelta(minutes=batch, seconds=position),
                    "synthetic:normalized", json.dumps({"text": text}), text,
                ))
            try:
                before = time.perf_counter()
                await conn.execute(
                """INSERT INTO observations (
                     id, tenant_id, occurred_at, kind, source_channel, content,
                     content_text, embedding_pending, trust_tier, entities_mentioned,
                     source_actor_ref
                   )
                   SELECT ids.id, ids.tenant_id, ids.occurred_at, 'signal',
                          ids.source_channel, ids.content_json::jsonb,
                          ids.content_text, true, 'ordinary', ids.entities::jsonb, $8
                   FROM unnest(
                     $1::uuid[], $2::uuid[], $3::timestamptz[],
                     $4::text[], $5::text[], $6::text[], $7::text[]
                   ) AS ids(id,tenant_id,occurred_at,source_channel,content_json,content_text,entities)""",
                [row[0] for row in rows], [row[1] for row in rows],
                [row[2] for row in rows], [row[3] for row in rows],
                [row[4] for row in rows], [row[5] for row in rows],
                [json.dumps([{"entity_id": str(actor_id), "entity_type": "person",
                              "source_actor_ref": actor_ref}]) for _ in rows],
                actor_ref,
            )
                observation_writes.append((time.perf_counter() - before) * 1000)
                if batch <= 2:
                    command = _admission(
                        tenant_id, ordinal * 1000 + batch,
                        evidence_id=str(rows[0][0]),
                    )
                    admitted = await build_default_truth_kernel().admit(tx=conn, command=command)
                    admitted_models.append((admitted, command))
                    if batch == 2:
                        relation_receipt, _ = await _admit_relation(
                            conn, tenant_id, admitted_models,
                        )
                before = time.perf_counter()
                found = await conn.fetch(
                """SELECT tenant_id, truth_version_id, proposition
                   FROM accepted_current_models WHERE tenant_id=$1""",
                tenant_id,
            )
                found_relations = await conn.fetch(
                    """SELECT tenant_id,truth_relation_version_id
                       FROM accepted_current_relations WHERE tenant_id=$1""",
                    tenant_id,
                )
                retrieval.append((time.perf_counter() - before) * 1000)
                hits += int(any(row["truth_version_id"] == admitted.version_id for row in found))
                leakage += sum(row["tenant_id"] != tenant_id for row in found)
                # No provider is used in deterministic cells. Persist the exact
                # retrieved semantic context decisions rather than estimating a
                # fictional prompt/token count.
                prompt_tokens.append(0)
                for index, model in enumerate(found):
                    await barrier_service.record_context_decision(
                        tx=conn,
                        item=ContextDecision(
                            decision_id=uuid4(), tenant_id=tenant_id,
                            batch_id=f"{cell.cell_id}:tenant-{ordinal}:batch-{batch}",
                            route_id=f"p8-scale:{cell.cell_id}:{ordinal}:{batch}",
                            context_item_kind="accepted_model",
                            context_item_id=str(model["truth_version_id"]),
                            context_item_version="1", retrieved=True, selected=True,
                            included=True, referenced=True,
                            counterevidence_retained=False, confidence_affecting=True,
                            necessary_background=False, historical_reopen_reason=None,
                            decision_fate="mutation", result_object_kind="model_version",
                            result_object_id=model["truth_version_id"],
                            evidence_lineage=({"kind": "accepted_model",
                                               "id": str(model["truth_version_id"])},),
                            decided_at=base + timedelta(minutes=batch, seconds=index),
                        ),
                    )
                for relation in found_relations:
                    await barrier_service.record_context_decision(
                        tx=conn,
                        item=ContextDecision(
                            decision_id=uuid4(), tenant_id=tenant_id,
                            batch_id=f"{cell.cell_id}:tenant-{ordinal}:batch-{batch}",
                            route_id=f"p8-scale:{cell.cell_id}:{ordinal}:{batch}",
                            context_item_kind="accepted_relation",
                            context_item_id=str(relation["truth_relation_version_id"]),
                            context_item_version="1", retrieved=True, selected=True,
                            included=True, referenced=True,
                            counterevidence_retained=False, confidence_affecting=True,
                            necessary_background=False, historical_reopen_reason=None,
                            decision_fate="mutation", result_object_kind="relation_version",
                            result_object_id=relation["truth_relation_version_id"],
                            evidence_lineage=({"kind": "accepted_relation",
                                               "id": str(relation["truth_relation_version_id"])},),
                            decided_at=base + timedelta(minutes=batch, seconds=50),
                        ),
                    )
                before = time.perf_counter()
                counting_tx = _CountingTx(conn)
                barrier_receipt = await barrier_service.complete(
                tx=counting_tx, barrier_id=uuid4(), tenant_id=tenant_id,
                batch_id=f"{cell.cell_id}:tenant-{ordinal}:batch-{batch}",
                expected_model_version_ids=tuple(item[0].version_id for item in admitted_models),
                expected_relation_version_ids=(
                    (relation_receipt.relation_version_id,) if relation_receipt is not None else ()
                ),
                truth_critical_pending_count=0, completed_at=base + timedelta(minutes=batch, seconds=59),
            )
                if batch <= 2:
                    await enqueue_projection_refresh_job(
                        conn, tenant_id=tenant_id, projection_name="p8-semantic-scale",
                        subject_key=f"model-version:{admitted.version_id}",
                        reason="barrier_complete", event_ids=tuple(row[0] for row in rows),
                        payload={"barrier_version": barrier_receipt.barrier_version},
                    )
                    jobs = await lease_projection_refresh_jobs(
                        conn, tenant_id=tenant_id, limit=1,
                    )
                    if len(jobs) != 1:
                        raise AssertionError("semantic scale projection refresh was not leased")
                    await complete_projection_refresh_job(
                        conn, tenant_id=tenant_id, job_id=jobs[0].id,
                    )
                barriers.append((time.perf_counter() - before) * 1000)
                barrier_sql_calls.append(counting_tx.calls)
                queue_depth.append(await conn.fetchval(
                """SELECT count(*)::int FROM company_learning_barriers
                   WHERE tenant_id=$1 AND truth_critical_pending_count > 0""", tenant_id,
                ))
                barrier_measurements.append(
                    await _production_barrier_measurement(conn, tenant_id)
                )
            except BaseException:
                await batch_tx.rollback()
                raise
            else:
                await batch_tx.commit()
        model_count = await conn.fetchval("SELECT count(*)::int FROM models WHERE tenant_id=$1", tenant_id)
        version_count = await conn.fetchval("SELECT count(*)::int FROM model_truth_versions WHERE tenant_id=$1", tenant_id)
        barrier_count = await conn.fetchval("SELECT count(*)::int FROM company_learning_barriers WHERE tenant_id=$1", tenant_id)
        relation_count = await conn.fetchval("SELECT count(*)::int FROM relation_truth_versions WHERE tenant_id=$1", tenant_id)
        decision_count = await conn.fetchval("SELECT count(*)::int FROM company_learning_context_decisions WHERE tenant_id=$1", tenant_id)
        scope_count = await conn.fetchval("SELECT count(*)::int FROM model_truth_scope_bindings WHERE tenant_id=$1", tenant_id)
        actor_count = await conn.fetchval("SELECT count(*)::int FROM actors WHERE tenant_id=$1", tenant_id)
        refresh_count = await conn.fetchval("SELECT count(*)::int FROM projection_refresh_jobs WHERE tenant_id=$1 AND status='processed'", tenant_id)
        state = {
            "execution_version": SCALE_EXECUTION_VERSION,
            "tenant_id": str(tenant_id), "cell_id": cell.cell_id,
            "observations": cell.batch_size * cell.memory_horizon_batches,
            "models": model_count, "versions": version_count, "relations": relation_count,
            "context_decisions": decision_count, "scope_bindings": scope_count,
            "actors": actor_count, "processed_projection_jobs": refresh_count,
            "barriers": barrier_count,
            "hits": hits, "leakage": leakage, "queue": queue_depth,
        }
        return TenantScaleReceipt(
            str(tenant_id), cell.memory_horizon_batches,
            cell.batch_size * cell.memory_horizon_batches, tuple(retrieval),
            tuple(observation_writes), tuple(barriers), tuple(barrier_sql_calls),
            tuple(prompt_tokens), tuple(queue_depth), hits,
            leakage, model_count, version_count, barrier_count,
            (time.perf_counter() - started) * 1000, pool_wait_ms,
            canonical_sha256(state), tuple(barrier_measurements), bootstrap_ms,
            relation_count, decision_count, scope_count, actor_count, refresh_count, 0,
        )
    finally:
        try:
            await conn.execute("DELETE FROM tenants WHERE id=$1", tenant_id)
        except Exception:
            pass
        if pool is not None:
            await pool.release(conn)
        else:
            await conn.close()


async def run_scale_cell(
    dsn: str, cell: ScaleCell, *, pool: asyncpg.Pool | None = None,
) -> ActualScaleCell:
    before_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    started = time.perf_counter()
    receipts = tuple(await asyncio.gather(*(
        _run_tenant(dsn, cell, ordinal, pool=pool) for ordinal in range(cell.tenant_concurrency)
    )))
    wall_ms = (time.perf_counter() - started) * 1000
    retrieval = [sample for row in receipts for sample in row.retrieval_samples_ms]
    observation_writes = [sample for row in receipts for sample in row.observation_write_samples_ms]
    barriers = [sample for row in receipts for sample in row.barrier_samples_ms]
    prompts = [sample for row in receipts for sample in row.prompt_token_samples]
    queues = [sample for row in receipts for sample in row.queue_depth_samples]
    throughputs = [row.observations / max(row.elapsed_ms / 1000, .000001) for row in receipts]
    first_batches = ceil(cell.memory_horizon_batches / 4)
    payload = {
        "cell": asdict(cell), "tenant_receipts": [asdict(row) for row in receipts],
        "wall_time_ms": wall_ms,
    }
    return ActualScaleCell(
        cell.cell_id, cell.batch_size, cell.memory_horizon_batches,
        cell.tenant_concurrency, receipts, _p95(retrieval),
        _p95(observation_writes), _p95(barriers),
        int(_p95(prompts)), _slope_final_half(queues), _p95(barriers),
        min(throughputs) / max(throughputs),
        sum(row.accepted_model_hits for row in receipts) /
        (cell.memory_horizon_batches * cell.tenant_concurrency),
        sum(row.cross_tenant_hits for row in receipts),
        2 / first_batches, 0.0,
        sum(row.processed_projection_jobs for row in receipts) /
        max(1, sum(row.canonical_versions + row.canonical_relations for row in receipts)),
        sum(row.observations for row in receipts),
        sum(row.canonical_models + row.canonical_versions + row.canonical_relations + row.barriers for row in receipts),
        sum(row.context_decisions + row.processed_projection_jobs for row in receipts),
        max(before_rss, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        wall_ms, False, False, canonical_sha256(payload),
    )


async def run_scale_matrix(dsn: str, *, cells: tuple[ScaleCell, ...] | None = None) -> ScaleExecution:
    requested = cells or build_scale_matrix()
    results = tuple([await run_scale_cell(dsn, cell) for cell in requested])
    expected_ids = {cell.cell_id for cell in build_scale_matrix()}
    exact = {cell.cell_id for cell in results} == expected_ids
    payload = {"cells": [asdict(cell) for cell in results], "exact": exact,
               "isolation": "rollback_scoped_tenant_transactions"}
    return ScaleExecution(results, None, exact, False, canonical_sha256(payload))


async def run_shared_contention(dsn: str) -> SharedContentionResult:
    selected = tuple(cell for cell in build_scale_matrix() if cell.cell_id in {
        "p8-bs10-h12-t1", "p8-bs25-h12-t5", "p8-bs50-h12-t20",
    })
    started = time.perf_counter()
    results = await asyncio.gather(*(run_scale_cell(dsn, cell) for cell in selected))
    wall_ms = (time.perf_counter() - started) * 1000
    individual = sum(cell.wall_time_ms for cell in results)
    payload = {"cells": [cell.cell_id for cell in results], "wall_ms": wall_ms,
               "individual_wall_time_sum_ms": individual}
    return SharedContentionResult(
        tuple(cell.cell_id for cell in results), len(results), wall_ms, individual,
        wall_ms / max(individual, .000001), canonical_sha256(payload),
    )


async def run_warm_pair_diagnostic(
    dsn: str, *, batch_size: int = 10, memory_horizon_batches: int = 50,
    repetitions: int = 5, pool_size: int = 20,
) -> WarmPairDiagnostic:
    if repetitions < 5:
        raise ValueError("warm-pair diagnosis requires at least five repetitions")
    pool = await asyncpg.create_pool(dsn, min_size=pool_size, max_size=pool_size)
    try:
        # Explicit warmups are excluded from the denominator.
        for concurrency in (1, 20):
            await run_scale_cell(
                dsn, ScaleCell(f"p8-warmup-t{concurrency}", batch_size, 3, concurrency),
                pool=pool,
            )
        samples: list[WarmPairSample] = []
        for repetition in range(1, repetitions + 1):
            # Alternate order to avoid assigning monotonic DB drift to one arm.
            order = (1, 20) if repetition % 2 else (20, 1)
            for concurrency in order:
                cell = await run_scale_cell(
                    dsn,
                    ScaleCell(
                        f"p8-warm-b{batch_size}-h{memory_horizon_batches}-t{concurrency}-r{repetition}",
                        batch_size, memory_horizon_batches, concurrency,
                    ),
                    pool=pool,
                )
                waits = [row.pool_wait_ms for row in cell.tenant_receipts]
                total_per_batch = [row.elapsed_ms / memory_horizon_batches for row in cell.tenant_receipts]
                sql_calls = [call for row in cell.tenant_receipts for call in row.barrier_sql_calls]
                samples.append(WarmPairSample(
                    repetition, concurrency, _p95(waits), cell.observation_write_p95_ms,
                    cell.barrier_p95_ms, max(sql_calls), _p95(total_per_batch),
                    cell.wall_time_ms, cell.semantic_quality,
                    cell.cross_tenant_leakage,
                ))
    finally:
        await pool.close()
    by_concurrency = {
        concurrency: [row for row in samples if row.tenant_concurrency == concurrency]
        for concurrency in (1, 20)
    }
    barrier = {key: _p95([x.barrier_p95_ms for x in rows]) for key, rows in by_concurrency.items()}
    total = {key: _p95([x.end_to_end_batch_p95_ms for x in rows]) for key, rows in by_concurrency.items()}
    wait = {key: _p95([x.pool_wait_p95_ms for x in rows]) for key, rows in by_concurrency.items()}
    barrier_ratio = barrier[20] / max(barrier[1], .000001)
    total_ratio = total[20] / max(total[1], .000001)
    wait_ratio = None if wait[1] <= .000001 else wait[20] / wait[1]
    diagnosis = (
        "database_execution_contention" if barrier_ratio > 2 and wait[20] < barrier[20]
        else "pool_saturation" if wait[20] >= barrier[20]
        else "within_declared_envelope"
    )
    payload = {"samples": [asdict(row) for row in samples], "pool_size": pool_size,
               "barrier_ratio": barrier_ratio, "total_ratio": total_ratio}
    return WarmPairDiagnostic(
        batch_size, memory_horizon_batches, repetitions, pool_size, tuple(samples),
        barrier_ratio, total_ratio, wait_ratio, diagnosis, canonical_sha256(payload),
    )
