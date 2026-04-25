"""Calibration harness: Cohen's kappa between judge and human labels."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lsob_contracts import DiffOp, Trigger

from lsob_evaluator_l6.llm_judge.client import JudgeResult, LLMJudge

DEFAULT_CALIBRATION_DIR = (
    Path(__file__).resolve().parent.parent.parent.parent
    / "fixtures"
    / "judge_calibration"
)

LABELS = ("reference_wins", "tie", "sut_wins")


def _label_from_winner(winner: str) -> str:
    if winner == "reference":
        return "reference_wins"
    if winner == "sut":
        return "sut_wins"
    return "tie"


@dataclass
class CalibrationItem:
    id: str
    reference_diff: DiffOp
    sut_diff: DiffOp
    human_label: str
    notes: str | None = None
    trigger: Trigger | None = None


@dataclass
class CalibrationReport:
    judge_name: str
    model: str
    prompt_hash: str
    n_items: int
    cohens_kappa: float
    agreement_rate: float
    confusion_matrix: dict[str, dict[str, int]]
    estimated_usd: float
    input_tokens: int
    output_tokens: int
    per_item: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "judge_name": self.judge_name,
            "model": self.model,
            "prompt_hash": self.prompt_hash,
            "n_items": self.n_items,
            "cohens_kappa": self.cohens_kappa,
            "agreement_rate": self.agreement_rate,
            "confusion_matrix": self.confusion_matrix,
            "cost": {
                "estimated_usd": round(self.estimated_usd, 6),
                "input_tokens": self.input_tokens,
                "output_tokens": self.output_tokens,
            },
            "per_item": self.per_item,
        }


def load_calibration_fixtures(
    fixtures_dir: Path | str | None = None,
) -> list[CalibrationItem]:
    """Load every `*.json` calibration fixture from `fixtures_dir`."""
    base = Path(fixtures_dir) if fixtures_dir is not None else DEFAULT_CALIBRATION_DIR
    if not base.exists():
        return []
    items: list[CalibrationItem] = []
    for path in sorted(base.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        trig_raw = data.get("trigger")
        trig = Trigger.model_validate(trig_raw) if trig_raw else None
        items.append(
            CalibrationItem(
                id=data["id"],
                reference_diff=DiffOp.model_validate(data["reference_diff"]),
                sut_diff=DiffOp.model_validate(data["sut_diff"]),
                human_label=data["human_label"],
                notes=data.get("notes"),
                trigger=trig,
            )
        )
    return items


def cohens_kappa(human: list[str], judge: list[str]) -> float:
    """Cohen's kappa for two equal-length categorical label sequences.

    Returns 1.0 for perfect agreement, 0.0 for chance-only, can be negative
    for worse-than-chance. Uses the finite set `LABELS` as the category set.
    """
    if len(human) != len(judge):
        raise ValueError("human and judge must be equal length")
    n = len(human)
    if n == 0:
        return 1.0
    # Observed agreement.
    agree = sum(1 for h, j in zip(human, judge) if h == j)
    p_o = agree / n
    # Expected agreement by chance.
    p_e = 0.0
    for lbl in LABELS:
        p_h = human.count(lbl) / n
        p_j = judge.count(lbl) / n
        p_e += p_h * p_j
    if p_e >= 1.0:
        # Perfect marginal concentration; kappa is defined only when p_e < 1.
        return 1.0 if p_o == 1.0 else 0.0
    return (p_o - p_e) / (1.0 - p_e)


def _default_trigger(item_id: str) -> Trigger:
    from datetime import datetime, timezone

    return Trigger(
        trigger_id=f"cal-trigger-{item_id}",
        kind="calibration",
        payload={"item_id": item_id},
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


async def _run_one(
    judge: LLMJudge, item: CalibrationItem
) -> tuple[str, JudgeResult]:
    trig = item.trigger or _default_trigger(item.id)
    outcome = await judge.compare(trig, item.reference_diff, item.sut_diff)
    return _label_from_winner(outcome.winner), outcome


async def run_calibration(
    judge: LLMJudge,
    items: list[CalibrationItem] | None = None,
    fixtures_dir: Path | str | None = None,
) -> CalibrationReport:
    """Run `judge` against all calibration items and build a report.

    The report includes Cohen's kappa, a confusion matrix (rows=human,
    cols=judge), per-item labels, and an aggregate cost estimate.
    """
    fixtures = items if items is not None else load_calibration_fixtures(fixtures_dir)
    humans: list[str] = []
    judges: list[str] = []
    per_item: list[dict[str, Any]] = []
    total_input_tokens = 0
    total_output_tokens = 0
    total_usd = 0.0
    for item in fixtures:
        judge_label, outcome = await _run_one(judge, item)
        humans.append(item.human_label)
        judges.append(judge_label)
        per_item.append(
            {
                "id": item.id,
                "human_label": item.human_label,
                "judge_label": judge_label,
                "raw_votes": outcome.raw_votes,
                "low_confidence": outcome.low_confidence,
                "scores_reference": outcome.scores_reference,
                "scores_sut": outcome.scores_sut,
                "ordering": outcome.ordering,
            }
        )
        total_input_tokens += outcome.cost.input_tokens
        total_output_tokens += outcome.cost.output_tokens
        total_usd += outcome.cost.estimated_usd

    cm = {h: {j: 0 for j in LABELS} for h in LABELS}
    for h, j in zip(humans, judges):
        if h in cm and j in cm[h]:
            cm[h][j] += 1
    agreement = sum(1 for h, j in zip(humans, judges) if h == j) / max(1, len(humans))
    k = cohens_kappa(humans, judges)

    return CalibrationReport(
        judge_name=getattr(judge.client, "name", "unknown"),
        model=judge.model,
        prompt_hash=judge.prompt_hash,
        n_items=len(fixtures),
        cohens_kappa=k,
        agreement_rate=agreement,
        confusion_matrix=cm,
        estimated_usd=total_usd,
        input_tokens=total_input_tokens,
        output_tokens=total_output_tokens,
        per_item=per_item,
    )


def run_calibration_sync(
    judge: LLMJudge,
    items: list[CalibrationItem] | None = None,
    fixtures_dir: Path | str | None = None,
) -> CalibrationReport:
    return asyncio.run(run_calibration(judge, items, fixtures_dir))


__all__ = [
    "CalibrationItem",
    "CalibrationReport",
    "DEFAULT_CALIBRATION_DIR",
    "LABELS",
    "cohens_kappa",
    "load_calibration_fixtures",
    "run_calibration",
    "run_calibration_sync",
]
