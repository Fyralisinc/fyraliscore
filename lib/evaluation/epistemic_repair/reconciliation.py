"""Deterministic reconciliation for logical-call time and provider cost.

The additive timing ledger contains only ``exclusive`` leaf spans. ``inclusive``
spans are useful diagnostics (for example, a whole retrieval stage) but are not
summed because doing so would double count their children.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal, Sequence


TimingSemantics = Literal["exclusive", "inclusive"]
AttemptOutcome = Literal[
    "success", "cache_hit", "timeout", "provider_error", "parse_failure", "exhausted"
]


@dataclass(frozen=True)
class TimingSpan:
    category: str
    started_at: datetime
    ended_at: datetime
    semantics: TimingSemantics = "exclusive"
    logical_call_id: str | None = None
    physical_attempt_id: str | None = None
    attempt_outcome: AttemptOutcome | None = None

    @property
    def duration_ms(self) -> float:
        return (self.ended_at - self.started_at).total_seconds() * 1000.0


@dataclass(frozen=True)
class TimingReconciliation:
    wall_ms: float
    exclusive_sum_ms: float
    covered_ms: float
    gap_ms: float
    overlap_ms: float
    absolute_error_ms: float
    relative_error: float
    tolerance: float
    failed_attempt_ids: tuple[str, ...]
    errors: tuple[str, ...]

    @property
    def reconciled(self) -> bool:
        return not self.errors and self.relative_error <= self.tolerance


def reconcile_timing(
    *,
    wall_started_at: datetime,
    wall_ended_at: datetime,
    spans: Sequence[TimingSpan],
    tolerance: float = 0.01,
) -> TimingReconciliation:
    """Prove exclusive categories cover logical wall time without double counting."""

    if not 0 <= tolerance <= 1:
        raise ValueError("tolerance must be between zero and one")
    wall_ms = (wall_ended_at - wall_started_at).total_seconds() * 1000.0
    if wall_ms <= 0:
        raise ValueError("logical wall interval must have positive duration")

    errors: list[str] = []
    exclusive: list[TimingSpan] = []
    failed_ids: set[str] = set()
    for index, span in enumerate(spans):
        if span.ended_at < span.started_at:
            errors.append(f"span[{index}] has negative duration")
        if span.started_at < wall_started_at or span.ended_at > wall_ended_at:
            errors.append(f"span[{index}] falls outside logical wall interval")
        if span.attempt_outcome not in (None, "success", "cache_hit"):
            if not span.physical_attempt_id:
                errors.append(f"failed span[{index}] lacks physical_attempt_id")
            else:
                failed_ids.add(span.physical_attempt_id)
        if span.semantics == "exclusive":
            exclusive.append(span)

    ordered = sorted(exclusive, key=lambda item: (item.started_at, item.ended_at))
    exclusive_sum_ms = sum(max(0.0, item.duration_ms) for item in ordered)
    covered_ms = 0.0
    cursor = wall_started_at
    overlap_ms = 0.0
    for span in ordered:
        start = max(span.started_at, wall_started_at)
        end = min(span.ended_at, wall_ended_at)
        if end <= start:
            continue
        if start < cursor:
            overlap_ms += (min(end, cursor) - start).total_seconds() * 1000.0
        if end > cursor:
            newly_covered_start = max(start, cursor)
            covered_ms += (end - newly_covered_start).total_seconds() * 1000.0
            cursor = end

    gap_ms = max(0.0, wall_ms - covered_ms)
    absolute_error_ms = abs(exclusive_sum_ms - wall_ms)
    relative_error = absolute_error_ms / wall_ms
    if overlap_ms > wall_ms * tolerance:
        errors.append("exclusive timing spans overlap beyond tolerance")
    if gap_ms > wall_ms * tolerance:
        errors.append("exclusive timing spans leave wall-time gaps beyond tolerance")

    return TimingReconciliation(
        wall_ms=wall_ms,
        exclusive_sum_ms=exclusive_sum_ms,
        covered_ms=covered_ms,
        gap_ms=gap_ms,
        overlap_ms=overlap_ms,
        absolute_error_ms=absolute_error_ms,
        relative_error=relative_error,
        tolerance=tolerance,
        failed_attempt_ids=tuple(sorted(failed_ids)),
        errors=tuple(errors),
    )


@dataclass(frozen=True)
class AttemptCost:
    physical_attempt_id: str
    outcome: AttemptOutcome
    input_tokens: int | None
    output_tokens: int | None
    actual_cost_usd: Decimal | None = None
    estimated_cost_usd: Decimal | None = None


@dataclass(frozen=True)
class CostReconciliation:
    attempt_count: int
    failed_attempt_count: int
    input_tokens: int
    output_tokens: int
    actual_cost_usd: Decimal
    estimated_cost_usd: Decimal
    best_available_cost_usd: Decimal
    token_coverage: float
    cost_coverage: float
    actual_estimate_delta_usd: Decimal
    errors: tuple[str, ...]

    @property
    def reconciled(self) -> bool:
        return not self.errors and self.token_coverage == 1.0 and self.cost_coverage == 1.0


def reconcile_costs(attempts: Sequence[AttemptCost]) -> CostReconciliation:
    """Reconcile every physical attempt, including failures, into token/cost totals."""

    ids: set[str] = set()
    errors: list[str] = []
    for attempt in attempts:
        if attempt.physical_attempt_id in ids:
            errors.append(f"duplicate physical_attempt_id: {attempt.physical_attempt_id}")
        ids.add(attempt.physical_attempt_id)
        for name, value in (
            ("input_tokens", attempt.input_tokens),
            ("output_tokens", attempt.output_tokens),
        ):
            if value is not None and value < 0:
                errors.append(f"{attempt.physical_attempt_id} has negative {name}")
        for name, value in (
            ("actual_cost_usd", attempt.actual_cost_usd),
            ("estimated_cost_usd", attempt.estimated_cost_usd),
        ):
            if value is not None and value < 0:
                errors.append(f"{attempt.physical_attempt_id} has negative {name}")

    count = len(attempts)
    token_known = sum(
        item.input_tokens is not None and item.output_tokens is not None for item in attempts
    )
    cost_known = sum(
        item.actual_cost_usd is not None or item.estimated_cost_usd is not None
        for item in attempts
    )
    actual = sum((item.actual_cost_usd or Decimal("0") for item in attempts), Decimal("0"))
    estimated = sum(
        (item.estimated_cost_usd or Decimal("0") for item in attempts), Decimal("0")
    )
    best = sum(
        (
            item.actual_cost_usd
            if item.actual_cost_usd is not None
            else item.estimated_cost_usd or Decimal("0")
        )
        for item in attempts
    )
    paired_actual = sum(
        (item.actual_cost_usd or Decimal("0"))
        for item in attempts
        if item.actual_cost_usd is not None and item.estimated_cost_usd is not None
    )
    paired_estimated = sum(
        (item.estimated_cost_usd or Decimal("0"))
        for item in attempts
        if item.actual_cost_usd is not None and item.estimated_cost_usd is not None
    )
    return CostReconciliation(
        attempt_count=count,
        failed_attempt_count=sum(
            item.outcome not in ("success", "cache_hit") for item in attempts
        ),
        input_tokens=sum(item.input_tokens or 0 for item in attempts),
        output_tokens=sum(item.output_tokens or 0 for item in attempts),
        actual_cost_usd=actual,
        estimated_cost_usd=estimated,
        best_available_cost_usd=best,
        token_coverage=1.0 if count == 0 else token_known / count,
        cost_coverage=1.0 if count == 0 else cost_known / count,
        actual_estimate_delta_usd=paired_actual - paired_estimated,
        errors=tuple(errors),
    )
