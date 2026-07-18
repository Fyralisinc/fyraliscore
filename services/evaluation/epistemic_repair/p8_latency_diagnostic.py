"""Preregistered repeated warm-pair diagnostic for the red P8 latency gate."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import os
import platform
import resource
from math import ceil
from typing import Any

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p8_population import ScaleCell
from services.evaluation.epistemic_repair.p8_scale_runner import run_scale_cell


CONTROLS = ((25, 12), (25, 100))
MIN_REPETITIONS = 5


def _percentile(values: list[float], q: float) -> float:
    if not values:
        raise ValueError("latency percentile requires a nonempty denominator")
    ordered = sorted(values)
    return float(ordered[min(len(ordered) - 1, ceil(q * len(ordered)) - 1)])


def _distribution(values: list[float]) -> dict[str, Any]:
    return {"denominator": len(values), "p50_ms": _percentile(values, .50),
            "p95_ms": _percentile(values, .95), "p99_ms": _percentile(values, .99)}


@dataclass(frozen=True)
class ArmReceipt:
    batch_size: int
    horizon: int
    repetition: int
    execution_order: int
    concurrency: int
    tenant_denominator: int
    batch_denominator: int
    bootstrap: dict[str, Any]
    pool_wait: dict[str, Any]
    first_barrier: dict[str, Any]
    steady_barrier: dict[str, Any]
    all_barriers: dict[str, Any]
    observation_write: dict[str, Any]
    retrieval: dict[str, Any]
    sql_call_denominator: int
    sql_calls_per_barrier_max: int
    wall_time_ms: float
    semantic_quality: float
    cross_tenant_leakage: int
    evidence_digest: str


def arm_receipt(cell, *, repetition: int, execution_order: int) -> ArmReceipt:
    rows = list(cell.tenant_receipts)
    first = [row.barrier_samples_ms[0] for row in rows if row.barrier_samples_ms]
    steady = [sample for row in rows for sample in row.barrier_samples_ms[1:]]
    barriers = [sample for row in rows for sample in row.barrier_samples_ms]
    writes = [sample for row in rows for sample in row.observation_write_samples_ms]
    retrieval = [sample for row in rows for sample in row.retrieval_samples_ms]
    sql = [sample for row in rows for sample in row.barrier_sql_calls]
    payload = {
        "cell": cell.cell_id, "repetition": repetition, "order": execution_order,
        "receipt_digests": [row.queried_state_digest for row in rows],
    }
    return ArmReceipt(
        cell.batch_size, cell.memory_horizon_batches, repetition, execution_order,
        cell.tenant_concurrency, len(rows), sum(row.barriers for row in rows),
        _distribution([row.bootstrap_ms for row in rows]),
        _distribution([row.pool_wait_ms for row in rows]), _distribution(first),
        _distribution(steady), _distribution(barriers), _distribution(writes),
        _distribution(retrieval), len(sql), max(sql), cell.wall_time_ms,
        cell.semantic_quality, cell.cross_tenant_leakage, canonical_sha256(payload),
    )


async def _server_provenance(conn: asyncpg.Connection) -> dict[str, Any]:
    identity = await conn.fetchrow(
        """SELECT current_database() AS database_name,
                  current_setting('server_version') AS server_version,
                  inet_server_addr()::text AS server_address,
                  inet_server_port() AS server_port"""
    )
    pgss = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements')")
    return {"database": dict(identity), "pg_stat_statements_available": bool(pgss),
            "host": {"node": platform.node(), "system": platform.system(),
                     "release": platform.release(), "machine": platform.machine(),
                     "cpu_count": os.cpu_count(),
                     "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}}


async def _pgss(conn: asyncpg.Connection) -> dict[str, Any]:
    available = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements')")
    if not available:
        return {"status": "unavailable", "rows": []}
    rows = await conn.fetch(
        """SELECT queryid::text,calls,total_exec_time,mean_exec_time,rows
           FROM pg_stat_statements
           WHERE query ILIKE '%company_learning_barriers%'
           ORDER BY total_exec_time DESC LIMIT 20"""
    )
    return {"status": "measured", "rows": [dict(row) for row in rows]}


async def run_repeated_warm_pairs(dsn: str, *, repetitions: int = MIN_REPETITIONS) -> dict[str, Any]:
    if repetitions < MIN_REPETITIONS:
        raise ValueError("at least five repetitions are preregistered")
    conn = await asyncpg.connect(dsn)
    try:
        server = await _server_provenance(conn)
        pgss_before = await _pgss(conn)
    finally:
        await conn.close()
    arms = []
    for batch, horizon in CONTROLS:
        # Explicit warmups are excluded from every measured denominator.
        for concurrency in (1, 20):
            await run_scale_cell(dsn, ScaleCell(f"p8-latency-warmup-b{batch}-h{horizon}-t{concurrency}", batch, 3, concurrency))
        for repetition in range(1, repetitions + 1):
            order = (1, 20) if repetition % 2 else (20, 1)
            for ordinal, concurrency in enumerate(order, 1):
                measured = await run_scale_cell(
                    dsn, ScaleCell(f"p8-latency-b{batch}-h{horizon}-r{repetition}-t{concurrency}",
                                   batch, horizon, concurrency),
                )
                arms.append(asdict(arm_receipt(measured, repetition=repetition, execution_order=ordinal)))
    conn = await asyncpg.connect(dsn)
    try:
        pgss_after = await _pgss(conn)
    finally:
        await conn.close()
    body = {"schema_version": "p8-repeated-warm-pair-v1",
            "preregistration": {"controls": CONTROLS, "repetitions": repetitions,
                                "concurrencies": (1, 20), "alternating_order": True,
                                "warmups_excluded": True, "existing_scale_gate_unchanged": True},
            "server_provenance": server, "pg_stat_statements_before": pgss_before,
            "pg_stat_statements_after": pgss_after, "arms": arms}
    analysis = analyze_repeated_warm_pairs(body)
    body["analysis"] = analysis
    body["artifact_digest"] = canonical_sha256(body)
    return body


def analyze_repeated_warm_pairs(artifact: dict[str, Any]) -> dict[str, Any]:
    prereg = artifact.get("preregistration", {})
    repetitions = int(prereg.get("repetitions", 0))
    arms = artifact.get("arms") if isinstance(artifact.get("arms"), list) else []
    expected = {(b, h, r, c) for b, h in CONTROLS for r in range(1, repetitions + 1) for c in (1, 20)}
    observed = {(row.get("batch_size"), row.get("horizon"), row.get("repetition"), row.get("concurrency")) for row in arms}
    denominators_ok = all(
        row.get("tenant_denominator") == row.get("concurrency")
        and row.get("batch_denominator") == row.get("concurrency") * row.get("horizon")
        and row.get("sql_call_denominator") == row.get("batch_denominator")
        and all(row.get(name, {}).get("denominator", 0) > 0 for name in (
            "bootstrap", "pool_wait", "first_barrier", "steady_barrier", "all_barriers",
            "observation_write", "retrieval"))
        and row.get("wall_time_ms", 0) > 0
        and len(row.get("evidence_digest", "")) == 64
        for row in arms
    )
    order_ok = all(
        sorted((row["concurrency"], row["execution_order"]) for row in arms
               if row["batch_size"] == b and row["horizon"] == h and row["repetition"] == r)
        == ([(1, 1), (20, 2)] if r % 2 else [(1, 2), (20, 1)])
        for b, h in CONTROLS for r in range(1, repetitions + 1)
    )
    complete = bool(
        repetitions >= MIN_REPETITIONS and observed == expected and len(arms) == len(expected)
        and denominators_ok and order_ok and artifact.get("server_provenance")
        and isinstance(artifact.get("pg_stat_statements_before"), dict)
        and isinstance(artifact.get("pg_stat_statements_after"), dict)
        and prereg.get("existing_scale_gate_unchanged") is True
    )
    return {"diagnostic_complete": complete, "expected_arm_count": len(expected),
            "observed_arm_count": len(arms), "exact_denominators": denominators_ok,
            "alternating_order_verified": order_ok,
            "existing_scale_gate_modified": False,
            "interpretation_status": "ready_for_structural_diagnosis" if complete else "insufficient_evidence"}


__all__ = ["CONTROLS", "MIN_REPETITIONS", "analyze_repeated_warm_pairs",
           "arm_receipt", "run_repeated_warm_pairs"]
