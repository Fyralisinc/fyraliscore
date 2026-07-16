#!/usr/bin/env python3
"""Run the frozen second adversarial entity-extraction holdout once."""

from __future__ import annotations

import json
import importlib.util
from collections import defaultdict
from pathlib import Path

from lib.entity_mention_detection import locate_explicit_surface_spans
from lib.evaluation.entity_extraction_gold import PredictedMention, evaluate_gold_entity_extraction
from services.domain.entity_grounding.mention_fates import _persisted_mention_opportunities

_CORPUS_PATH = Path(__file__).parents[1] / "tests/evaluation/entity_extraction_adversarial_holdout_2.py"
_SPEC = importlib.util.spec_from_file_location("entity_extraction_adversarial_holdout_2", _CORPUS_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"cannot load holdout corpus {_CORPUS_PATH}")
_CORPUS = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_CORPUS)
FROZEN_CORPUS_SHA256 = _CORPUS.FROZEN_CORPUS_SHA256
GOLD_MENTIONS = _CORPUS.GOLD_MENTIONS
SIGNALS = _CORPUS.SIGNALS
corpus_sha256 = _CORPUS.corpus_sha256


OUTPUT = Path("artifacts/evaluation/entity_extraction_adversarial_holdout_2_report.json")


def main() -> int:
    actual_hash = corpus_sha256()
    if actual_hash != FROZEN_CORPUS_SHA256:
        raise RuntimeError(f"frozen corpus hash mismatch: {actual_hash}")

    predictions: list[PredictedMention] = []
    batches: dict[str, list] = defaultdict(list)
    for signal in SIGNALS:
        batches[signal.batch_id].append(signal)
    for batch_id, batch in sorted(batches.items()):
        if len(batch) != 10:
            raise RuntimeError(f"{batch_id} is not a genuine ten-signal batch")
        for signal in batch:
            surfaces = _persisted_mention_opportunities(
                content={},
                content_text=signal.text,
                source_channel=signal.source_type,
                has_structural_context=False,
            )
            for surface_index, surface in enumerate(surfaces):
                for occurrence, (start, end) in enumerate(locate_explicit_surface_spans(signal.text, surface)):
                    predictions.append(PredictedMention(
                        prediction_id=f"h2-p-{signal.signal_id}-{surface_index}-{occurrence}",
                        signal_id=signal.signal_id,
                        start=start,
                        end=end,
                        entity_type="unknown",
                        canonical_referent=None,
                        confidence=0.5,
                        abstained=True,
                        candidate_fate="detected_unresolved",
                    ))

    scored = evaluate_gold_entity_extraction(
        signals=SIGNALS,
        gold_mentions=GOLD_MENTIONS,
        predictions=tuple(predictions),
    )
    signal_by_id = {item.signal_id: item for item in SIGNALS}
    gold_exact = {(m.signal_id, m.start, m.end): m for m in GOLD_MENTIONS}
    pred_exact = {(p.signal_id, p.start, p.end): p for p in predictions}
    errors = []
    for key, gold in gold_exact.items():
        if key not in pred_exact:
            signal = signal_by_id[gold.signal_id]
            errors.append({
                "kind": "missed_gold_span", "signal_id": gold.signal_id,
                "source": signal.source_type, "text": signal.text,
                "expected": {"surface": signal.text[gold.start:gold.end], "start": gold.start, "end": gold.end, "type": gold.entity_type, "referent": gold.canonical_referent},
            })
    for key, prediction in pred_exact.items():
        if key not in gold_exact:
            signal = signal_by_id[prediction.signal_id]
            overlaps = [m for m in GOLD_MENTIONS if m.signal_id == prediction.signal_id and prediction.start < m.end and m.start < prediction.end]
            errors.append({
                "kind": "boundary_mismatch" if overlaps else "false_positive_span",
                "signal_id": prediction.signal_id, "source": signal.source_type,
                "text": signal.text,
                "predicted": {"surface": signal.text[prediction.start:prediction.end], "start": prediction.start, "end": prediction.end},
                "overlapping_gold": [{"surface": signal.text[m.start:m.end], "start": m.start, "end": m.end, "type": m.entity_type, "referent": m.canonical_referent} for m in overlaps],
            })
    for key in sorted(set(gold_exact) & set(pred_exact)):
        gold = gold_exact[key]
        errors.append({
            "kind": "unavailable_type_and_link", "signal_id": gold.signal_id,
            "surface": signal_by_id[gold.signal_id].text[gold.start:gold.end],
            "expected_type": gold.entity_type, "expected_referent": gold.canonical_referent,
            "predicted_type": "unknown", "abstained": True,
        })

    report = scored.model_dump(mode="json")
    negative_ids = {s.signal_id for s in SIGNALS if not any(m.signal_id == s.signal_id for m in GOLD_MENTIONS)}
    report.update({
        "corpus": {"sha256": actual_hash, "signals": len(SIGNALS), "batches": len(batches), "gold_mentions": len(GOLD_MENTIONS), "negative_signals": len(negative_ids)},
        "prediction_adapter": {"name": "persisted_batch_bootstrap_locator", "content_hints": False, "structural_context": False, "capability_boundary": "surface discovery only; type and link unavailable"},
        "negative_control": {"signals": len(negative_ids), "predictions": sum(p.signal_id in negative_ids for p in predictions), "clean_signal_rate": sum(not any(p.signal_id == signal_id for p in predictions) for signal_id in negative_ids) / len(negative_ids)},
        "error_count": len(errors),
        "errors": errors,
    })
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"output": str(OUTPUT), "corpus": report["corpus"], "overall": report["overall"], "by_source": report["by_source"], "by_slack_context": report["by_slack_context"], "negative_control": report["negative_control"], "error_count": len(errors)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
