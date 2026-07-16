#!/usr/bin/env python3
"""Execute the frozen final entity-surface holdout exactly once."""

from __future__ import annotations

from collections import defaultdict
import hashlib
import json
from pathlib import Path

from lib.entity_mention_detection import locate_explicit_surface_spans
from services.domain.entity_grounding.mention_fates import _persisted_mention_opportunities


ROOT = Path(__file__).parents[1]
CORPUS = ROOT / "benchmarks/datasets/final_entity_surface_discovery_holdout_v1.jsonl"
OUTPUT = ROOT / "artifacts/evaluation/final_entity_surface_discovery_holdout_v1_report.json"
FROZEN_SHA256 = "183f7b65f5dd8102b291ec561b211d85dea648f59a5117977a3e35b0f042e74c"


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def main() -> int:
    payload = CORPUS.read_bytes()
    actual_hash = hashlib.sha256(payload).hexdigest()
    if actual_hash != FROZEN_SHA256:
        raise RuntimeError(f"frozen corpus hash mismatch: {actual_hash}")
    records = [json.loads(line) for line in payload.decode("utf-8").splitlines()]
    batches: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        batches[record["batch"]].append(record)
        for span in record["gold_spans"]:
            assert record["signal"][span["start"] : span["end"]] == span["text"]
    if any(len(batch) != 10 for batch in batches.values()):
        raise RuntimeError("all frozen batches must contain exactly ten unique signals")
    if len({record["signal"] for record in records}) != len(records):
        raise RuntimeError("holdout signals must be unique")

    predictions: dict[str, list[dict]] = defaultdict(list)
    # This is the only prediction pass. No hints or structural context are supplied.
    for batch_id in sorted(batches):
        for record in batches[batch_id]:
            surfaces = _persisted_mention_opportunities(
                content={},
                content_text=record["signal"],
                source_channel=("slack:message" if record["channel"] == "slack" else f'{record["channel"]}:message' if record["channel"] == "email" else "jira:issue"),
                has_structural_context=False,
            )
            seen: set[tuple[int, int]] = set()
            for surface in surfaces:
                for start, end in locate_explicit_surface_spans(record["signal"], surface):
                    if (start, end) not in seen:
                        predictions[record["id"]].append({"start": start, "end": end, "text": record["signal"][start:end]})
                        seen.add((start, end))

    errors: list[dict] = []
    totals = {"tp": 0, "fp": 0, "fn": 0}
    source_totals: dict[str, dict[str, int]] = defaultdict(lambda: {"tp": 0, "fp": 0, "fn": 0})
    negative_predictions = 0
    clean_negatives = 0
    for record in records:
        gold = {(span["start"], span["end"]): span for span in record["gold_spans"]}
        predicted = {(span["start"], span["end"]): span for span in predictions[record["id"]]}
        exact = set(gold) & set(predicted)
        missed = set(gold) - set(predicted)
        extra = set(predicted) - set(gold)
        counts = source_totals[record["channel"]]
        for key, value in (("tp", len(exact)), ("fn", len(missed)), ("fp", len(extra))):
            totals[key] += value
            counts[key] += value
        if not gold:
            negative_predictions += len(predicted)
            clean_negatives += not predicted
        for key in sorted(missed):
            span = gold[key]
            overlaps = [item for pkey, item in predicted.items() if key[0] < pkey[1] and pkey[0] < key[1]]
            errors.append({"kind": "boundary_mismatch_missed" if overlaps else "missed_gold_span", "signal_id": record["id"], "batch": record["batch"], "channel": record["channel"], "signal": record["signal"], "expected": span, "overlapping_predictions": overlaps})
        for key in sorted(extra):
            span = predicted[key]
            overlaps = [item for gkey, item in gold.items() if key[0] < gkey[1] and gkey[0] < key[1]]
            errors.append({"kind": "boundary_mismatch_extra" if overlaps else "false_positive_span", "signal_id": record["id"], "batch": record["batch"], "channel": record["channel"], "signal": record["signal"], "predicted": span, "overlapping_gold": overlaps})

    def metrics(counts: dict[str, int]) -> dict:
        precision = _ratio(counts["tp"], counts["tp"] + counts["fp"])
        recall = _ratio(counts["tp"], counts["tp"] + counts["fn"])
        return {**counts, "precision": precision, "recall": recall, "f1": _ratio(2 * precision * recall, precision + recall)}

    negatives = sum(not record["gold_spans"] for record in records)
    report = {
        "corpus": {"path": str(CORPUS.relative_to(ROOT)), "sha256": actual_hash, "signals": len(records), "batches": len(batches), "gold_spans": sum(len(record["gold_spans"]) for record in records), "negative_signals": negatives},
        "prediction_adapter": {"name": "persisted_batch_bootstrap_locator", "content_hints": False, "structural_context": False, "capability_boundary": "surface discovery only"},
        "overall": metrics(totals),
        "by_source": {source: metrics(counts) for source, counts in sorted(source_totals.items())},
        "negative_control": {"signals": negatives, "predictions": negative_predictions, "clean_signals": clean_negatives, "clean_signal_rate": _ratio(clean_negatives, negatives)},
        "prediction_count": sum(len(items) for items in predictions.values()),
        "error_count": len(errors),
        "errors": errors,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: report[key] for key in ("corpus", "prediction_adapter", "overall", "by_source", "negative_control", "prediction_count", "error_count")}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
