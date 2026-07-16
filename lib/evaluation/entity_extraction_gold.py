"""Gold-corpus evaluation for entity extraction from persisted signal batches.

This module deliberately starts after transport: callers provide normalized signals,
gold annotations, and system predictions.  It measures semantic correctness separately
from the operational candidate-fate closure measured by ``entity_grounding``.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Literal, Sequence

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.evaluation.entity_pipeline_gold import EntityPipelineMetrics


class _Record(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class GoldSignal(_Record):
    signal_id: str = Field(min_length=1)
    batch_id: str = Field(min_length=1)
    source_type: str = Field(min_length=1)
    text: str
    slack_context: Literal[
        "not_slack", "standalone", "threaded", "cross_thread", "temporally_distributed"
    ] = "not_slack"


class GoldMention(_Record):
    mention_id: str = Field(min_length=1)
    signal_id: str = Field(min_length=1)
    start: int = Field(ge=0)
    end: int = Field(gt=0)
    entity_type: str = Field(min_length=1)
    canonical_referent: str | None = None

    @model_validator(mode="after")
    def valid_span(self) -> "GoldMention":
        if self.end <= self.start:
            raise ValueError("end must follow start")
        return self


class PredictedMention(_Record):
    prediction_id: str = Field(min_length=1)
    signal_id: str = Field(min_length=1)
    start: int = Field(ge=0)
    end: int = Field(gt=0)
    entity_type: str = Field(min_length=1)
    canonical_referent: str | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    abstained: bool = False
    candidate_fate: str | None = None

    @model_validator(mode="after")
    def valid_span(self) -> "PredictedMention":
        if self.end <= self.start:
            raise ValueError("end must follow start")
        if self.abstained and self.canonical_referent is not None:
            raise ValueError("an abstained prediction cannot select a referent")
        return self


class EntityExtractionMetrics(_Record):
    signal_count: int = Field(ge=0)
    batch_count: int = Field(ge=0)
    gold_count: int = Field(ge=0)
    prediction_count: int = Field(ge=0)
    exact_match_count: int = Field(ge=0)
    matched_count: int = Field(ge=0)
    span_precision: float | None = Field(default=None, ge=0, le=1)
    span_recall: float | None = Field(default=None, ge=0, le=1)
    span_f1: float | None = Field(default=None, ge=0, le=1)
    mean_boundary_iou: float | None = Field(default=None, ge=0, le=1)
    boundary_credit_precision: float | None = Field(default=None, ge=0, le=1)
    boundary_credit_recall: float | None = Field(default=None, ge=0, le=1)
    type_accuracy: float | None = Field(default=None, ge=0, le=1)
    canonical_link_accuracy: float | None = Field(default=None, ge=0, le=1)
    canonical_link_coverage: float | None = Field(default=None, ge=0, le=1)
    duplicate_rate: float | None = Field(default=None, ge=0, le=1)
    candidate_fate_coverage: float | None = Field(default=None, ge=0, le=1)
    selective_risk: tuple[dict[str, float], ...] = ()
    area_under_risk_coverage: float | None = Field(default=None, ge=0, le=1)


class GoldEntityExtractionReport(_Record):
    schema_version: Literal["gold-entity-extraction-v1"] = "gold-entity-extraction-v1"
    overall: EntityExtractionMetrics
    by_source: dict[str, EntityExtractionMetrics]
    by_slack_context: dict[str, EntityExtractionMetrics]
    entity_pipeline: EntityPipelineMetrics | None = None
    uncertainties: tuple[str, ...] = ()


def evaluate_gold_entity_extraction(
    *,
    signals: Sequence[GoldSignal],
    gold_mentions: Sequence[GoldMention],
    predictions: Sequence[PredictedMention],
    entity_pipeline: EntityPipelineMetrics | None = None,
    partial_match_iou: float = 0.01,
) -> GoldEntityExtractionReport:
    """Score predictions against gold spans using deterministic one-to-one matching.

    Matching prioritizes highest boundary IoU within each signal. Exact span P/R/F1 remains
    strict; IoU metrics provide continuous partial credit. Linking accuracy is
    conditioned on matched gold mentions with a known referent, while coverage
    exposes abstention instead of silently treating it as a wrong link.
    """
    if not 0 < partial_match_iou <= 1:
        raise ValueError("partial_match_iou must be in (0, 1]")
    signal_by_id = {item.signal_id: item for item in signals}
    if len(signal_by_id) != len(signals):
        raise ValueError("signal_id values must be unique")
    _validate_mentions(signal_by_id, gold_mentions, "gold")
    _validate_mentions(signal_by_id, predictions, "prediction")
    if len({item.mention_id for item in gold_mentions}) != len(gold_mentions):
        raise ValueError("mention_id values must be unique")
    if len({item.prediction_id for item in predictions}) != len(predictions):
        raise ValueError("prediction_id values must be unique")

    overall = _metrics(signals, gold_mentions, predictions, partial_match_iou)
    by_source = _stratify(
        signals, gold_mentions, predictions, lambda item: item.source_type,
        partial_match_iou,
    )
    by_slack = _stratify(
        signals, gold_mentions, predictions, lambda item: item.slack_context,
        partial_match_iou,
    )
    uncertainty: list[str] = []
    if not gold_mentions:
        uncertainty.append("no_gold_mentions_in_scope")
    if any(item.canonical_referent is None for item in gold_mentions):
        uncertainty.append("canonical_link_metrics_exclude_gold_without_referents")
    return GoldEntityExtractionReport(
        overall=overall,
        by_source=by_source,
        by_slack_context=by_slack,
        entity_pipeline=entity_pipeline,
        uncertainties=tuple(uncertainty),
    )


def _validate_mentions(signal_by_id: dict[str, GoldSignal], mentions: Sequence, kind: str) -> None:
    for mention in mentions:
        signal = signal_by_id.get(mention.signal_id)
        if signal is None:
            raise ValueError(f"{kind} mention references unknown signal {mention.signal_id!r}")
        if mention.end > len(signal.text):
            raise ValueError(f"{kind} mention span exceeds signal text")


def _stratify(signals, gold, predictions, key, threshold):
    result = {}
    for value in sorted({key(item) for item in signals}):
        subset = [item for item in signals if key(item) == value]
        ids = {item.signal_id for item in subset}
        result[value] = _metrics(
            subset,
            [item for item in gold if item.signal_id in ids],
            [item for item in predictions if item.signal_id in ids],
            threshold,
        )
    return result


def _metrics(signals, gold, predictions, threshold):
    gold_by_signal = defaultdict(list)
    pred_by_signal = defaultdict(list)
    for item in gold:
        gold_by_signal[item.signal_id].append(item)
    for item in predictions:
        pred_by_signal[item.signal_id].append(item)

    matched = []
    for signal in signals:
        candidates = []
        for gi, gold_item in enumerate(gold_by_signal[signal.signal_id]):
            for pi, predicted in enumerate(pred_by_signal[signal.signal_id]):
                iou = _iou(gold_item.start, gold_item.end, predicted.start, predicted.end)
                if iou >= threshold:
                    candidates.append((iou, gi, pi, gold_item, predicted))
        used_gold, used_prediction = set(), set()
        for candidate in sorted(candidates, key=lambda item: (-item[0], item[1], item[2])):
            iou, gi, pi, gold_item, predicted = candidate
            if gi not in used_gold and pi not in used_prediction:
                used_gold.add(gi)
                used_prediction.add(pi)
                matched.append((gold_item, predicted, iou))

    exact = sum(g.start == p.start and g.end == p.end for g, p, _ in matched)
    precision = _rate(exact, len(predictions))
    recall = _rate(exact, len(gold))
    iou_sum = sum(item[2] for item in matched)
    link_pairs = [(g, p) for g, p, _ in matched if g.canonical_referent is not None]
    linked = [(g, p) for g, p in link_pairs if not p.abstained and p.canonical_referent is not None]
    link_correct = sum(g.canonical_referent == p.canonical_referent for g, p in linked)
    duplicate_count = len(predictions) - len(
        {(p.signal_id, p.start, p.end, p.entity_type, p.canonical_referent) for p in predictions}
    )
    risk_curve, aurc = _selective_risk(link_pairs)
    return EntityExtractionMetrics(
        signal_count=len(signals),
        batch_count=len({item.batch_id for item in signals}),
        gold_count=len(gold),
        prediction_count=len(predictions),
        exact_match_count=exact, matched_count=len(matched),
        span_precision=precision, span_recall=recall, span_f1=_f1(precision, recall),
        mean_boundary_iou=_rate(iou_sum, len(matched)),
        boundary_credit_precision=_rate(iou_sum, len(predictions)),
        boundary_credit_recall=_rate(iou_sum, len(gold)),
        type_accuracy=_rate(sum(g.entity_type == p.entity_type for g, p, _ in matched), len(matched)),
        canonical_link_accuracy=_rate(link_correct, len(linked)),
        canonical_link_coverage=_rate(len(linked), len(link_pairs)),
        duplicate_rate=_rate(duplicate_count, len(predictions)),
        candidate_fate_coverage=_rate(
            sum(p.candidate_fate is not None for p in predictions), len(predictions)
        ),
        selective_risk=risk_curve, area_under_risk_coverage=aurc,
    )


def _selective_risk(pairs):
    decisions = [
        (p.confidence, float(g.canonical_referent != p.canonical_referent))
        for g, p in pairs if not p.abstained and p.canonical_referent is not None
    ]
    decisions.sort(reverse=True)
    if not pairs:
        return (), None
    curve, errors = [], 0.0
    for index, (confidence, error) in enumerate(decisions, 1):
        errors += error
        curve.append(
            {
                "coverage": index / len(pairs),
                "risk": errors / index,
                "threshold": confidence,
            }
        )
    if not curve:
        return (), None
    return tuple(curve), sum(point["risk"] for point in curve) / len(curve)


def _iou(a_start, a_end, b_start, b_end):
    intersection = max(0, min(a_end, b_end) - max(a_start, b_start))
    union = max(a_end, b_end) - min(a_start, b_start)
    return intersection / union if union else 0.0


def _rate(numerator, denominator):
    return numerator / denominator if denominator else None


def _f1(precision, recall):
    if precision is None or recall is None:
        return None
    return 2 * precision * recall / (precision + recall) if precision + recall else 0.0
