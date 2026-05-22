"""Calibration measurement for the synthesis harness.

What this measures
------------------

For every harness case that carries calibration metadata
(`expected_confidence_range` + `ground_truth_correctness` +
`extract_confidence`), we capture the engine's stated confidence
and the human-labeled ground truth. We then bucket cases by stated
confidence and compute the empirical correctness rate per bucket
plus the Expected Calibration Error (ECE) across all buckets.

What this does NOT measure
--------------------------

Calibration is only as good as the ground-truth labels. Those are
human judgments — sometimes obvious (math-driven contestation
multipliers always produce a known confidence; the underlying claim
is structurally true), sometimes genuinely uncertain (an LLM-produced
state Model whose proposition we choose to call "true" by inspection).

ECE values from this module should be read as **directional**: rising
ECE between two harness runs indicates calibration is drifting,
falling ECE indicates it is improving, but the absolute number is
not a claim that the engine is calibrated to any specific level.
The point of this layer is regression detection over time, not a
quality certificate.

The label set is also small. As of T4, only a handful of cases carry
calibration metadata. Adding more covered cases — especially LLM-
driven ones with non-trivial ground truth — is what turns this from
a smoke test into a real calibration trail.
"""
from __future__ import annotations

import dataclasses
import datetime
import json
import pathlib
from typing import Any, Iterable, Sequence


# Bucket edges: 10 evenly-spaced buckets covering [0, 1].
# A stated confidence of exactly 1.0 lands in the last bucket; the
# right edge is treated as inclusive only for the final bucket.
BUCKET_EDGES: tuple[float, ...] = tuple(round(x * 0.1, 1) for x in range(11))


def _bucket_index(conf: float) -> int:
    if conf >= 1.0:
        return len(BUCKET_EDGES) - 2  # last [0.9, 1.0]
    if conf < 0.0:
        return 0
    return min(int(conf * 10), len(BUCKET_EDGES) - 2)


def _bucket_label(idx: int) -> str:
    lo = BUCKET_EDGES[idx]
    hi = BUCKET_EDGES[idx + 1]
    return f"{lo:.1f} - {hi:.1f}"


@dataclasses.dataclass
class BucketStats:
    label: str
    lo: float
    hi: float
    n_scenarios: int
    n_correct: int
    sum_stated_confidence: float

    @property
    def empirical_correctness(self) -> float | None:
        if self.n_scenarios == 0:
            return None
        return self.n_correct / self.n_scenarios

    @property
    def avg_stated_confidence(self) -> float | None:
        if self.n_scenarios == 0:
            return None
        return self.sum_stated_confidence / self.n_scenarios

    @property
    def calibration_error(self) -> float | None:
        emp = self.empirical_correctness
        avg = self.avg_stated_confidence
        if emp is None or avg is None:
            return None
        # Signed: negative = engine underconfident, positive = overconfident.
        return avg - emp


@dataclasses.dataclass
class CalibrationReport:
    timestamp: str
    total_scenarios_with_labels: int
    buckets: list[BucketStats]
    ece: float | None
    skipped: list[dict[str, Any]] = dataclasses.field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "total_scenarios_with_labels": self.total_scenarios_with_labels,
            "ece": self.ece,
            "buckets": [
                {
                    "label": b.label,
                    "lo": b.lo,
                    "hi": b.hi,
                    "n_scenarios": b.n_scenarios,
                    "n_correct": b.n_correct,
                    "empirical_correctness": b.empirical_correctness,
                    "avg_stated_confidence": b.avg_stated_confidence,
                    "calibration_error": b.calibration_error,
                }
                for b in self.buckets
            ],
            "skipped": self.skipped,
        }


def compute_calibration(case_results: Sequence[Any]) -> CalibrationReport:
    """Build a `CalibrationReport` from a list of CaseResult objects.

    Cases are *included* iff they have:
      * `ground_truth_correctness` not None
      * `stated_confidence` not None (the case's `extract_confidence`
        callable returned a number)

    Cases with `ground_truth_correctness` set but `stated_confidence`
    None are reported in `skipped` with a reason — typically the case
    didn't run successfully or the extractor returned None.
    """
    buckets = [
        BucketStats(
            label=_bucket_label(i),
            lo=BUCKET_EDGES[i],
            hi=BUCKET_EDGES[i + 1],
            n_scenarios=0,
            n_correct=0,
            sum_stated_confidence=0.0,
        )
        for i in range(len(BUCKET_EDGES) - 1)
    ]
    skipped: list[dict[str, Any]] = []
    n_total = 0
    for r in case_results:
        gt = getattr(r, "ground_truth_correctness", None)
        if gt is None:
            continue
        conf = getattr(r, "stated_confidence", None)
        if conf is None:
            skipped.append({
                "name": getattr(r, "name", "?"),
                "stage": getattr(r, "stage", "?"),
                "reason": (
                    "no stated_confidence — extract_confidence returned None"
                    if getattr(r, "passed", False)
                    else "case failed before confidence could be extracted"
                ),
            })
            continue
        n_total += 1
        idx = _bucket_index(float(conf))
        b = buckets[idx]
        b.n_scenarios += 1
        b.sum_stated_confidence += float(conf)
        if gt:
            b.n_correct += 1

    # ECE = weighted mean of |avg_stated_confidence - empirical_correctness|
    # weighted by the bucket's share of total scenarios. Skips empty buckets.
    if n_total == 0:
        ece: float | None = None
    else:
        ece_sum = 0.0
        for b in buckets:
            if b.n_scenarios == 0:
                continue
            err = b.calibration_error
            if err is None:
                continue
            ece_sum += abs(err) * (b.n_scenarios / n_total)
        ece = ece_sum

    return CalibrationReport(
        timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        total_scenarios_with_labels=n_total,
        buckets=buckets,
        ece=ece,
        skipped=skipped,
    )


# ---------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------


def render_calibration_table(report: CalibrationReport) -> str:
    lines: list[str] = []
    lines.append("=" * 78)
    lines.append("CALIBRATION REPORT")
    lines.append("=" * 78)
    lines.append(
        f"  total scenarios labeled : {report.total_scenarios_with_labels}"
    )
    if report.ece is None:
        lines.append("  ECE                     : (no labeled scenarios)")
    else:
        lines.append(f"  ECE                     : {report.ece:.4f}")
    if report.skipped:
        lines.append(f"  skipped (no confidence) : {len(report.skipped)}")
    lines.append("")
    header = (
        f"  {'Stated Confidence':<20}{'Scenarios':>11}{'Empirical':>11}"
        f"{'Avg Stated':>12}{'Cal. Error':>12}"
    )
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for b in report.buckets:
        if b.n_scenarios == 0:
            continue
        emp = b.empirical_correctness or 0.0
        avg = b.avg_stated_confidence or 0.0
        err = b.calibration_error or 0.0
        lines.append(
            f"  {b.label:<20}"
            f"{b.n_scenarios:>11d}"
            f"{emp:>11.2f}"
            f"{avg:>12.2f}"
            f"{err:>+12.3f}"
        )
    if report.skipped:
        lines.append("")
        lines.append("  Skipped scenarios:")
        for s in report.skipped[:10]:
            lines.append(
                f"    - [{s['stage']}] {s['name']}: {s['reason']}"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------
# Persistence + baseline diff
# ---------------------------------------------------------------------


def save_run_artifact(
    report: CalibrationReport,
    runs_dir: pathlib.Path,
) -> pathlib.Path:
    runs_dir.mkdir(parents=True, exist_ok=True)
    # ISO-ish timestamp safe for filenames.
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d-%H%M")
    path = runs_dir / f"{ts}-calibration.json"
    path.write_text(json.dumps(report.to_json(), indent=2))
    return path


REGRESSION_THRESHOLD_ECE: float = 0.05


def diff_against_baseline(
    report: CalibrationReport,
    baseline_path: pathlib.Path,
) -> tuple[bool, str]:
    """Return (regression: bool, message: str).

    A regression is flagged when ECE has risen by more than
    REGRESSION_THRESHOLD_ECE (0.05) since the baseline was committed.
    Falling ECE is never a regression.
    """
    if not baseline_path.exists():
        return False, "(no baseline yet — first calibration run)"
    try:
        baseline = json.loads(baseline_path.read_text())
    except json.JSONDecodeError as exc:
        return True, f"baseline file unreadable: {exc}"

    baseline_ece = baseline.get("ece")
    if baseline_ece is None:
        return False, "(baseline has no ECE — skipping regression check)"
    if report.ece is None:
        return True, "current run has no ECE; baseline did"

    drift = report.ece - baseline_ece
    if drift > REGRESSION_THRESHOLD_ECE:
        return True, (
            f"ECE rose by {drift:+.4f} (current {report.ece:.4f}, "
            f"baseline {baseline_ece:.4f}); threshold {REGRESSION_THRESHOLD_ECE:.2f}"
        )
    return False, (
        f"ECE drift {drift:+.4f} (current {report.ece:.4f}, "
        f"baseline {baseline_ece:.4f}); within threshold"
    )
