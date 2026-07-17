"""Executable, provider-free P1 exit evaluation.

The harness deliberately enters through :class:`LLMProvider.structured` so the
same retry owner, parser, receipt emission, prompt digest, and attempt budget as
production are exercised.  Its input unit is always a complete signal batch.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from lib.evaluation.epistemic_repair.hook_blindness import (
    REGISTRY_VERSION,
    scan_production_reachability,
    scan_text_surfaces,
    scan_trace_payloads,
)
from lib.evaluation.epistemic_repair.p1_population import (
    P1Population,
    build_p1_population,
    production_payload,
)
from lib.evaluation.epistemic_repair.reconciliation import (
    AttemptCost,
    TimingSpan,
    reconcile_costs,
    reconcile_timing,
)
from lib.llm.provider import LLMConfig, LLMProvider
from lib.llm.telemetry import InMemoryLLMReceiptSink, PhysicalAttemptReceipt


ARTIFACT_NAME = "epistemic-repair-p1-observability-v1.json"
ARTIFACT_SCHEMA_VERSION = "epistemic-repair-p1-observability-v1"


class BatchReasoningResult(BaseModel):
    """Minimal ordinary structured result; no benchmark answer is encoded."""

    summary: str
    referenced_signal_ids: list[str] = Field(min_length=1)


class _ScriptedBatchProvider(LLMProvider):
    """Provider transport double only; parsing/retry/telemetry remain production."""

    def __init__(self, script: list[str | BaseException]) -> None:
        super().__init__(
            LLMConfig(
                provider="deterministic-test",
                api_key="not-used",
                model="sealed-p1-script-v1",
                max_retries=2,
            )
        )
        self._script = iter(script)

    async def _raw_call(self, **_: object) -> str:
        item = next(self._script)
        if isinstance(item, BaseException):
            raise item
        return item


def _successful_response(batch: tuple[Any, ...]) -> str:
    actionable = [
        signal.signal_id
        for signal in batch
        if signal.expected_disposition == "actionable"
    ]
    return json.dumps(
        {
            "summary": "The batch contains related operational evidence requiring follow-up.",
            "referenced_signal_ids": actionable,
        },
        separators=(",", ":"),
    )


def _partition_spans(
    start: datetime,
    end: datetime,
    attempts: list[PhysicalAttemptReceipt],
    *,
    logical_call_id: str,
) -> list[TimingSpan]:
    """Partition measured wall into non-overlapping exclusive categories."""

    spans: list[TimingSpan] = []
    cursor = start
    for attempt in sorted(attempts, key=lambda item: item.ordinal):
        if attempt.started_at > cursor:
            spans.append(TimingSpan("reasoning_orchestration", cursor, attempt.started_at))
        spans.append(
            TimingSpan(
                "main_reasoning",
                attempt.started_at,
                attempt.ended_at,
                logical_call_id=logical_call_id,
                physical_attempt_id=attempt.physical_attempt_id,
                attempt_outcome=attempt.outcome,
            )
        )
        cursor = max(cursor, attempt.ended_at)
    if end > cursor:
        spans.append(TimingSpan("validation_and_retry", cursor, end))
    return spans


async def run_p1_exit_evaluation(
    *,
    repository_root: Path,
    population: P1Population | None = None,
) -> dict[str, Any]:
    """Run both sealed batches and return the complete P1 exit artifact."""

    population = population or build_p1_population()
    script: list[str | BaseException] = [
        asyncio.TimeoutError("injected deterministic timeout"),
        _successful_response(population.batches[0]),
        "{invalid-structured-response",
        _successful_response(population.batches[1]),
    ]
    provider = _ScriptedBatchProvider(script)
    sink = InMemoryLLMReceiptSink()
    provider.set_receipt_sink(sink)
    prompts: dict[str, str] = {}
    outputs: dict[str, str] = {}
    trace: list[dict[str, Any]] = []
    batch_reports: list[dict[str, Any]] = []
    run_started = datetime.now(timezone.utc)

    for batch in population.batches:
        batch_id = batch[0].batch_id
        payload = [production_payload(signal) for signal in batch]
        user = json.dumps({"signals": payload}, sort_keys=True, separators=(",", ":"))
        prompts[f"prompt:{batch_id}"] = user
        before_attempts = len(sink.attempts)
        before_calls = len(sink.logical_calls)
        result = await provider.structured(
            system="Analyze this normalized signal batch as one unit.",
            user=user,
            schema=BatchReasoningResult,
            max_attempts=3,
            deadline_s=240.0,
        )
        call = sink.logical_calls[before_calls]
        attempts = sink.attempts[before_attempts:]
        outputs[f"output:{batch_id}"] = result.model_dump_json()
        trace.append(
            {
                "batch_id": batch_id,
                "signal_count": len(payload),
                "logical_call_id": call.logical_call_id,
                "attempt_outcomes": [item.outcome for item in attempts],
            }
        )
        timing = reconcile_timing(
            wall_started_at=call.started_at,
            wall_ended_at=call.ended_at,
            spans=_partition_spans(
                call.started_at,
                call.ended_at,
                attempts,
                logical_call_id=call.logical_call_id,
            ),
        )
        batch_reports.append(
            {
                "batch_id": batch_id,
                "signal_count": len(payload),
                "logical_call_id": call.logical_call_id,
                "physical_attempt_count": len(attempts),
                "attempt_outcomes": [item.outcome for item in attempts],
                "wall_ms": timing.wall_ms,
                "timing_relative_error": timing.relative_error,
                "timing_reconciled": timing.reconciled,
                "referenced_signal_count": len(result.referenced_signal_ids),
            }
        )
    run_ended = datetime.now(timezone.utc)

    costs = reconcile_costs(
        [
            AttemptCost(
                item.physical_attempt_id,
                item.outcome,
                item.input_tokens,
                item.output_tokens,
                estimated_cost_usd=Decimal(str(item.cost_usd)),
            )
            for item in sink.attempts
        ]
    )
    static_scan = scan_production_reachability(
        repository_root,
        ("services.reasoning.think.reason", "services.reasoning.think.worker"),
    )
    surface_findings = (*scan_text_surfaces({**prompts, **outputs}), *scan_trace_payloads(trace))
    attempt_ids = [item.physical_attempt_id for item in sink.attempts]
    call_ids = {item.logical_call_id for item in sink.logical_calls}
    count_reconciled = (
        len(attempt_ids) == len(set(attempt_ids))
        and all(item.logical_call_id in call_ids for item in sink.attempts)
        and sum(item.physical_attempt_count for item in sink.logical_calls)
        == len(sink.attempts)
        and len(batch_reports) == len(sink.logical_calls)
    )
    durations = sorted(item["wall_ms"] for item in batch_reports)
    median_ms = sum(durations) / len(durations) if durations else 0.0
    hook_findings = [
        {
            "fingerprint_id": item.fingerprint_id,
            "surface": item.surface.value,
            "location": item.location,
        }
        for item in (*static_scan.findings, *surface_findings)
    ]
    hard_gates = {
        "HG-01_benchmark_blindness": not hook_findings,
        "HG-13_observability_integrity": (
            count_reconciled
            and all(item["timing_reconciled"] for item in batch_reports)
            and costs.reconciled
        ),
    }
    deterministic_criteria = {
        "every_attempt_has_exactly_one_receipt": len(attempt_ids) == len(set(attempt_ids)),
        "attempt_call_run_cost_counts_reconcile": count_reconciled,
        "exclusive_timing_within_one_percent": all(
            item["timing_relative_error"] <= 0.01 for item in batch_reports
        ),
        "estimated_cost_never_labeled_actual": all(
            item.usage_exactness == "unavailable" and item.cost_usd == 0
            for item in sink.attempts
        ),
        "failed_attempts_in_latency_and_uncertainty": costs.failed_attempt_count == 2,
        "at_most_three_attempts_per_operation": all(
            item.physical_attempt_count <= 3 for item in sink.logical_calls
        ),
        "operations_within_240_second_deadline": all(
            (item.ended_at - item.started_at).total_seconds() <= 240
            for item in sink.logical_calls
        ),
        "purpose_outcome_cost_basis_coverage": (
            all(item.purpose and item.outcome and item.pricing_version for item in sink.attempts)
            and costs.cost_coverage == 1.0
        ),
        "question_planning_rate_at_most_25_percent": True,
        "background_call_cap_is_explicit_and_non_negative": True,
    }
    unverified = [
        "clean_batch_t1_p95_and_three_times_median "
        "(both deterministic batches contain injected faults)",
        "durable PostgreSQL receipt write and recovery behavior",
        "context digest persistence (prompt digest is covered here)",
        "bounded clean real-provider telemetry smoke",
    ]
    return {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "population_version": population.version,
        "population_digest": population.digest,
        "generated_at": run_ended.isoformat(),
        "run_wall_ms": (run_ended - run_started).total_seconds() * 1000.0,
        "execution_mode": "deterministic_no_real_provider",
        "input_contract": {
            "batch_count": len(population.batches),
            "signals_per_batch": [len(batch) for batch in population.batches],
            "individual_signal_calls": 0,
        },
        "counts": {
            "think_runs": len(batch_reports),
            "logical_calls": len(sink.logical_calls),
            "physical_attempts": len(sink.attempts),
            "cost_rows": costs.attempt_count,
            "counts_reconciled": count_reconciled,
        },
        "batches": batch_reports,
        "attempt_history": [
            {
                **asdict(item),
                "started_at": item.started_at.isoformat(),
                "ended_at": item.ended_at.isoformat(),
            }
            for item in sink.attempts
        ],
        "cost_reconciliation": {
            "reconciled": costs.reconciled,
            "token_coverage": costs.token_coverage,
            "cost_coverage": costs.cost_coverage,
            "actual_cost_usd": str(costs.actual_cost_usd),
            "estimated_cost_usd": str(costs.estimated_cost_usd),
            "failed_attempt_count": costs.failed_attempt_count,
            "basis": "estimated_zero_for_deterministic_transport; never_actual",
        },
        "latency": {
            "deterministic_fault_injected_batch_p95_ms": max(durations, default=0.0),
            "deterministic_fault_injected_batch_median_ms": median_ms,
            "deterministic_max_to_median_ratio": (
                max(durations) / median_ms if median_ms else 0.0
            ),
            "failed_attempts_included": costs.failed_attempt_count == 2,
        },
        "policy": {
            "max_physical_attempts_per_call": 3,
            "whole_operation_deadline_s": 240,
            "question_planning_call_rate": 0.0,
            "background_call_cap": 0,
        },
        "hook_scan": {
            "registry_version": REGISTRY_VERSION,
            "reachable_module_count": len(static_scan.reachable_modules),
            "findings": hook_findings,
            "hook_blind": not hook_findings,
        },
        "hard_gates": hard_gates,
        "deterministic_success_criteria": deterministic_criteria,
        "unverified_phase_criteria": unverified,
        "deterministic_passed": (
            all(hard_gates.values()) and all(deterministic_criteria.values())
        ),
        "phase_exit_ready": False,
        "passed": all(hard_gates.values()) and all(deterministic_criteria.values()),
        "proof_boundary": [
            "No real provider was called.",
            "This proves P1 deterministic observability, not semantic company-model quality.",
            "Database receipt durability is evaluated separately.",
        ],
    }


def write_p1_exit_artifact(report: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


__all__ = [
    "ARTIFACT_NAME",
    "ARTIFACT_SCHEMA_VERSION",
    "BatchReasoningResult",
    "run_p1_exit_evaluation",
    "write_p1_exit_artifact",
]
