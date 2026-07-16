#!/usr/bin/env python3
"""Run the frozen learned entity-discovery corpus exactly once per batch."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.contracts.entity_mentions import EntityMentionDetectionFate
from lib.evaluation.entity_extraction_gold import (
    GoldMention,
    GoldSignal,
    PredictedMention,
    evaluate_gold_entity_extraction,
)
from lib.llm.provider import (
    LLMUsageAggregator,
    build_provider,
    close_codex_app_server_client,
    set_response_cache,
    using_usage_aggregator,
)
from services.domain.entity_grounding.learned_discovery import (
    PersistedSignalText,
    discover_batch_mentions,
)
from tests.evaluation.learned_entity_discovery_quality_corpus_v1 import (
    FROZEN_CORPUS,
    FROZEN_SHA256,
    computed_sha256,
)

REPORT_PATH = Path("/tmp/learned_entity_discovery_quality_v1_report.json")


def _exact_errors(gold_mentions, predictions, text_by_id):
    gold_exact = {(g.signal_id, g.start, g.end): g for g in gold_mentions}
    pred_exact = {(p.signal_id, p.start, p.end): p for p in predictions}
    false_negatives = []
    false_positives = []
    type_errors = []
    for key, gold in gold_exact.items():
        predicted = pred_exact.get(key)
        if predicted is None:
            false_negatives.append({
                "mention_id": gold.mention_id,
                "signal_id": gold.signal_id,
                "span": [gold.start, gold.end],
                "surface": text_by_id[gold.signal_id][gold.start:gold.end],
                "gold_type": gold.entity_type,
            })
        elif predicted.entity_type != gold.entity_type:
            type_errors.append({
                "mention_id": gold.mention_id,
                "prediction_id": predicted.prediction_id,
                "signal_id": gold.signal_id,
                "surface": text_by_id[gold.signal_id][gold.start:gold.end],
                "gold_type": gold.entity_type,
                "predicted_type": predicted.entity_type,
            })
    for key, predicted in pred_exact.items():
        if key not in gold_exact:
            false_positives.append({
                "prediction_id": predicted.prediction_id,
                "signal_id": predicted.signal_id,
                "span": [predicted.start, predicted.end],
                "surface": text_by_id[predicted.signal_id][predicted.start:predicted.end],
                "predicted_type": predicted.entity_type,
            })
    return {"false_negatives": false_negatives, "false_positives": false_positives,
            "exact_span_type_errors": type_errors}


async def main() -> None:
    actual_sha = computed_sha256()
    if actual_sha != FROZEN_SHA256:
        raise SystemExit(f"frozen corpus hash mismatch: {actual_sha} != {FROZEN_SHA256}")
    if len(FROZEN_CORPUS) != 60 or len({x["signal_id"] for x in FROZEN_CORPUS}) != 60:
        raise SystemExit("corpus must contain exactly 60 unique signals")
    batches = {f"batch-{i}": [x for x in FROZEN_CORPUS if x["batch_id"] == f"batch-{i}"]
               for i in range(1, 7)}
    if any(len(rows) != 10 for rows in batches.values()):
        raise SystemExit("each frozen batch must contain exactly 10 signals")
    if sum(not x["gold"] for x in FROZEN_CORPUS) < 20:
        raise SystemExit("corpus must contain at least 20 negative signals")

    # Pin transport/model/retry policy before provider construction and before calls.
    os.environ["LLM_PROVIDER"] = "codex"
    os.environ["CODEX_TRANSPORT"] = "app-server"
    os.environ["CODEX_MODEL"] = "gpt-5.4"
    os.environ["LLM_MAX_RETRIES"] = "0"
    set_response_cache(None)
    provider = build_provider()
    if (provider.config.model, provider.config.max_retries) != ("gpt-5.4", 0):
        raise SystemExit("provider pinning failed")

    all_candidates = []
    batch_runs = []
    try:
        for batch_id, rows in batches.items():
            signals = tuple(PersistedSignalText(
                signal_id=UUID(row["signal_id"]),
                source_channel=row["source_type"],
                content_text=row["text"],
            ) for row in rows)
            usage = LLMUsageAggregator()
            started = time.perf_counter()
            with using_usage_aggregator(usage):
                # The sole invocation for this frozen batch. Never retry here.
                result = await discover_batch_mentions(provider=provider, signals=signals)
            latency = time.perf_counter() - started
            all_candidates.extend(result.candidates)
            batch_runs.append({
                "batch_id": batch_id,
                "signal_count": len(signals),
                "mode": result.mode,
                "provider_error": result.provider_error,
                "latency_seconds": latency,
                "provider_calls_recorded": usage.call_count,
                "input_tokens_estimated": usage.total_input_tokens,
                "output_tokens_estimated": usage.total_output_tokens,
                "cost_usd_estimated": usage.total_cost_usd,
                "candidates": [{
                    "signal_id": str(c.signal_id), "surface": c.surface,
                    "start": c.span_start, "end": c.span_end,
                    "entity_type": c.entity_type, "confidence": c.confidence,
                    "fate": c.fate.value, "reason_codes": list(c.reason_codes),
                } for c in result.candidates],
            })
    finally:
        await close_codex_app_server_client()

    signals_gold = [GoldSignal(
        signal_id=r["signal_id"], batch_id=r["batch_id"],
        source_type=r["source_type"], text=r["text"],
        slack_context=r["slack_context"],
    ) for r in FROZEN_CORPUS]
    mentions_gold = [GoldMention(
        mention_id=m["mention_id"], signal_id=r["signal_id"],
        start=m["start"], end=m["end"], entity_type=m["entity_type"],
        canonical_referent=None,
    ) for r in FROZEN_CORPUS for m in r["gold"]]
    accepted = [c for c in all_candidates
                if c.fate is EntityMentionDetectionFate.DETECTED]
    predictions = [PredictedMention(
        prediction_id=f"p-{i:03d}", signal_id=str(c.signal_id),
        start=c.span_start, end=c.span_end, entity_type=c.entity_type,
        canonical_referent=None, confidence=c.confidence, abstained=False,
        candidate_fate=c.fate.value,
    ) for i, c in enumerate(accepted, 1)]
    scored = evaluate_gold_entity_extraction(
        signals=signals_gold, gold_mentions=mentions_gold, predictions=predictions,
    )
    negative_ids = {r["signal_id"] for r in FROZEN_CORPUS if not r["gold"]}
    dirty_negative_ids = sorted({str(c.signal_id) for c in accepted
                                 if str(c.signal_id) in negative_ids})
    fates = Counter(c.fate.value for c in all_candidates)
    rejected_fates = Counter(c.fate.value for c in all_candidates
                             if c.fate is not EntityMentionDetectionFate.DETECTED)
    totals = {
        "discover_batch_invocations": 6,
        "provider_calls_recorded": sum(x["provider_calls_recorded"] for x in batch_runs),
        "input_tokens_estimated": sum(x["input_tokens_estimated"] for x in batch_runs),
        "output_tokens_estimated": sum(x["output_tokens_estimated"] for x in batch_runs),
        "cost_usd_estimated": sum(x["cost_usd_estimated"] for x in batch_runs),
        "latency_seconds": sum(x["latency_seconds"] for x in batch_runs),
    }
    report = {
        "benchmark": "learned-entity-discovery-quality-v1",
        "frozen_corpus_sha256": FROZEN_SHA256,
        "freeze_verified_before_provider_construction": True,
        "model": provider.config.model,
        "transport": "app-server",
        "parse_retries": provider.config.max_retries,
        "corpus": {"signals": 60, "batches": 6, "batch_size": 10,
                   "negative_signals": len(negative_ids), "gold_mentions": len(mentions_gold),
                   "sources": dict(Counter(r["source_type"] for r in FROZEN_CORPUS))},
        "scope": {"scoring_boundary": "verified DETECTED candidates only",
                  "rejected_candidates_retained_for_fate_diagnostics": True,
                  "canonical_link_claim": False},
        "extraction_metrics": scored.model_dump(mode="json"),
        "negative_cleanliness": {
            "clean_negative_signals": len(negative_ids) - len(dirty_negative_ids),
            "negative_signal_count": len(negative_ids),
            "rate": (len(negative_ids) - len(dirty_negative_ids)) / len(negative_ids),
            "dirty_signal_ids": dirty_negative_ids,
        },
        "candidate_fate_distribution": dict(sorted(fates.items())),
        "rejected_fate_distribution": dict(sorted(rejected_fates.items())),
        "exact_errors": _exact_errors(mentions_gold, predictions,
                                      {r["signal_id"]: r["text"] for r in FROZEN_CORPUS}),
        "operational_totals": totals,
        "batch_runs": batch_runs,
    }
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                           encoding="utf-8")
    print(json.dumps({"report_path": str(REPORT_PATH), "summary": report},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
