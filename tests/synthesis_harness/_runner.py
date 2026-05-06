"""Harness runner: case dataclass, parallel executor, report formatter.

T4 (calibration): cases may optionally carry calibration metadata:

  * `expected_confidence_range` — the stated-confidence bracket the
    engine should fall in for this scenario (informational; not
    enforced by the assertion).
  * `ground_truth_correctness` — human label: is the proposition the
    engine is asserting actually true in this scenario, independent
    of the confidence it claimed? This is the calibration anchor.
  * `extract_confidence(actual) -> float | None` — pulls the stated
    confidence from the actual output. Different stages stash it in
    different keys (`new_confidence`, `confidence`, etc.), so cases
    declare their own extractor. Returning None excludes the scenario
    from the calibration computation.

Cases without calibration metadata are excluded from
`harness/calibration.py`'s computation but still run normally.
"""
from __future__ import annotations

import asyncio
import json
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import asyncpg


CaseFn = Callable[[asyncpg.Pool, dict], Awaitable[dict]]


@dataclass
class Case:
    stage: str
    name: str
    intent: str  # one-sentence description of what's being asserted
    setup: CaseFn  # returns a ctx dict (synthetic input)
    run: CaseFn  # returns actual output
    expected: Callable[[dict], dict]  # expected(ctx) -> dict
    assertion: Callable[[dict, dict, dict], tuple[bool, str]]
    # ↑ assertion(actual, expected, ctx) -> (passed, diff_str)

    # T4: optional calibration metadata. See module docstring.
    expected_confidence_range: tuple[float, float] | None = None
    ground_truth_correctness: bool | None = None
    extract_confidence: Callable[[dict], float | None] | None = None
    # Free-form note on *why* this case has its ground-truth label.
    # Surfaced in the calibration table so a future reader can audit.
    ground_truth_basis: str | None = None


@dataclass
class CaseResult:
    stage: str
    name: str
    intent: str
    passed: bool
    elapsed_ms: int
    diff: str = ""
    actual: Any = None
    expected: Any = None
    error: str | None = None
    learnings: list[str] = field(default_factory=list)
    # T4: copied from the Case so calibration.py can read everything
    # off the result list without re-walking the cases.
    expected_confidence_range: tuple[float, float] | None = None
    ground_truth_correctness: bool | None = None
    stated_confidence: float | None = None
    ground_truth_basis: str | None = None


async def _run_case(pool: asyncpg.Pool, case: Case) -> CaseResult:
    t0 = time.monotonic()
    try:
        ctx = await case.setup(pool, {})
        actual = await case.run(pool, ctx)
        expected = case.expected(ctx)
        passed, diff = case.assertion(actual, expected, ctx)
        stated_conf: float | None = None
        if case.extract_confidence is not None:
            try:
                stated_conf = case.extract_confidence(actual)
            except Exception:  # noqa: BLE001
                stated_conf = None
        return CaseResult(
            stage=case.stage,
            name=case.name,
            intent=case.intent,
            passed=passed,
            elapsed_ms=int((time.monotonic() - t0) * 1000),
            diff=diff,
            actual=_safe_json(actual),
            expected=_safe_json(expected),
            expected_confidence_range=case.expected_confidence_range,
            ground_truth_correctness=case.ground_truth_correctness,
            stated_confidence=stated_conf,
            ground_truth_basis=case.ground_truth_basis,
        )
    except Exception as exc:
        return CaseResult(
            stage=case.stage,
            name=case.name,
            intent=case.intent,
            passed=False,
            elapsed_ms=int((time.monotonic() - t0) * 1000),
            error=f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
            expected_confidence_range=case.expected_confidence_range,
            ground_truth_correctness=case.ground_truth_correctness,
            ground_truth_basis=case.ground_truth_basis,
        )


def _safe_json(o: Any) -> Any:
    try:
        return json.loads(json.dumps(o, default=str))
    except Exception:
        return str(o)


async def run_cases(
    pool: asyncpg.Pool,
    cases: list[Case],
    *,
    concurrency: int = 8,
) -> list[CaseResult]:
    sem = asyncio.Semaphore(concurrency)

    async def _bounded(c: Case) -> CaseResult:
        async with sem:
            return await _run_case(pool, c)

    return await asyncio.gather(*[_bounded(c) for c in cases])


def render_report(results: list[CaseResult]) -> str:
    lines = []
    by_stage: dict[str, list[CaseResult]] = {}
    for r in results:
        by_stage.setdefault(r.stage, []).append(r)

    total_pass = sum(1 for r in results if r.passed)
    total = len(results)
    lines.append("=" * 78)
    lines.append(f"SYNTHESIS HARNESS — {total_pass}/{total} cases passed")
    lines.append("=" * 78)

    for stage, group in by_stage.items():
        sp = sum(1 for r in group if r.passed)
        lines.append(f"\n[{stage}] {sp}/{len(group)} passed")
        for r in group:
            mark = "PASS" if r.passed else "FAIL"
            lines.append(f"  [{mark}] ({r.elapsed_ms:>5} ms) {r.name}")
            lines.append(f"         intent: {r.intent}")
            if not r.passed:
                if r.error:
                    err_lines = r.error.strip().split("\n")
                    lines.append(f"         error: {err_lines[0]}")
                    for el in err_lines[1:6]:
                        lines.append(f"           {el}")
                if r.diff:
                    diff_lines = r.diff.strip().split("\n")
                    for dl in diff_lines[:8]:
                        lines.append(f"         diff:  {dl}")

    lines.append("\n" + "=" * 78)
    return "\n".join(lines)
