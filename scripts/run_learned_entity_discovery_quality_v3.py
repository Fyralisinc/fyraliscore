#!/usr/bin/env python3
"""Execute the sealed v3 entity-discovery holdout exactly once.

Integrity and one-shot metadata are checked before provider construction. Every
genuine ten-signal batch is checkpointed atomically with its raw output, exact
error, usage, and production-verification result. A completion receipt fences
all ordinary reruns.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel

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
from tests.evaluation.learned_entity_discovery_quality_corpus_v3 import (
    FROZEN_CORPUS_V3,
    FROZEN_SHA256_V3,
    ONE_SHOT_EVIDENCE_METADATA,
    ONTOLOGY_TYPES,
    computed_sha256_v3,
)

ARTIFACT_DIR = Path("/tmp/learned_entity_discovery_quality_v3")
CHECKPOINT_PATH = ARTIFACT_DIR / "checkpoint.json"
REPORT_PATH = ARTIFACT_DIR / "report.json"
RECEIPT_PATH = ARTIFACT_DIR / "completion_receipt.json"
EXPECTED_METADATA = {
    "benchmark": "learned-entity-discovery-quality-v3",
    "evidence_class": "sealed_untouched_holdout",
    "sealed_before_first_provider_call": True,
    "provider_execution_count_at_seal": 0,
    "evidence_status": "not_executed",
    "allowed_provider_executions": 1,
    "split_policy": "organization_entity_time_text_disjoint_from_v1_v2_and_development",
    "time_window": "2031-01-01/2032-12-31",
    "canonical_link_claim_permitted": False,
}
SLACK_CONTEXT_MAP = {
    "standalone": "standalone", "thread_reply": "threaded",
    "thread_reply_delayed": "threaded", "cross_thread_reference": "cross_thread",
    "temporal_sequence": "temporally_distributed",
    "channel_followup": "temporally_distributed",
    "cross_channel_temporal": "temporally_distributed", "not_slack": "not_slack",
}


class _CaptureProvider:
    def __init__(self, delegate: Any) -> None:
        self.delegate = delegate
        self.call_count = 0
        self.response: Any | None = None
        self.error: dict[str, str] | None = None

    async def structured(
        self, *, system: str, user: str, schema: type[BaseModel],
        temperature: float, max_tokens: int,
    ) -> Any:
        self.call_count += 1
        try:
            self.response = await self.delegate.structured(
                system=system, user=user, schema=schema,
                temperature=temperature, max_tokens=max_tokens,
            )
            return self.response
        except Exception as exc:
            self.error = {
                "exception_type": type(exc).__name__, "message": str(exc),
                "stage": "provider_structured_call",
            }
            raise


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def validate_pre_provider() -> dict[str, Any]:
    if RECEIPT_PATH.exists():
        raise SystemExit(f"completion receipt exists; sealed v3 rerun refused: {RECEIPT_PATH}")
    if CHECKPOINT_PATH.exists():
        raise SystemExit(
            "sealed v3 checkpoint exists; fresh provider execution refused: "
            f"{CHECKPOINT_PATH}"
        )
    if ONE_SHOT_EVIDENCE_METADATA != EXPECTED_METADATA:
        raise SystemExit("sealed v3 one-shot metadata mismatch")
    actual = computed_sha256_v3()
    if actual != FROZEN_SHA256_V3:
        raise SystemExit(f"sealed v3 SHA mismatch: {actual} != {FROZEN_SHA256_V3}")
    if len(FROZEN_CORPUS_V3) != 40:
        raise SystemExit("sealed v3 must contain exactly 40 signals")
    if len({row["signal_id"] for row in FROZEN_CORPUS_V3}) != 40:
        raise SystemExit("sealed v3 signal IDs must be unique")
    batches = {
        f"v3-batch-{index}": tuple(
            row for row in FROZEN_CORPUS_V3
            if row["batch_id"] == f"v3-batch-{index}"
        ) for index in range(1, 5)
    }
    if any(len(rows) != 10 for rows in batches.values()):
        raise SystemExit("sealed v3 requires four genuine ten-signal batches")
    if any(sum(bool(row["gold"]) for row in rows) != 5 for rows in batches.values()):
        raise SystemExit("each sealed v3 batch requires five positive signals")
    gold = [mention for row in FROZEN_CORPUS_V3 for mention in row["gold"]]
    if {item["entity_type"] for item in gold} != ONTOLOGY_TYPES:
        raise SystemExit("sealed v3 ontology coverage mismatch")
    for row in FROZEN_CORPUS_V3:
        for mention in row["gold"]:
            if row["text"][mention["start"]:mention["end"]] != mention["surface"]:
                raise SystemExit("sealed v3 gold boundary mismatch")
            if mention["canonical_referent"] is not None:
                raise SystemExit("sealed v3 canonical referents must remain null")
    return {"sha256": actual, "batches": batches, "gold_mentions": len(gold)}


def _gold_objects() -> tuple[list[GoldSignal], list[GoldMention], dict[str, str]]:
    signals = [GoldSignal(
        signal_id=row["signal_id"], batch_id=row["batch_id"],
        source_type=row["source_type"], text=row["text"],
        slack_context=SLACK_CONTEXT_MAP[row["slack_context"]],
    ) for row in FROZEN_CORPUS_V3]
    mentions = [GoldMention(
        mention_id=item["mention_id"], signal_id=row["signal_id"],
        start=item["start"], end=item["end"], entity_type=item["entity_type"],
        canonical_referent=None,
    ) for row in FROZEN_CORPUS_V3 for item in row["gold"]]
    # Exercise evaluator validation before provider construction.
    evaluate_gold_entity_extraction(signals=signals, gold_mentions=mentions, predictions=[])
    return signals, mentions, {row["signal_id"]: row["text"] for row in FROZEN_CORPUS_V3}


def _score(predictions: list[PredictedMention]) -> dict[str, Any]:
    signals, gold, _ = _gold_objects()
    report = evaluate_gold_entity_extraction(
        signals=signals, gold_mentions=gold, predictions=predictions
    ).model_dump(mode="json")
    report["by_entity_type"] = {
        entity_type: evaluate_gold_entity_extraction(
            signals=signals,
            gold_mentions=[item for item in gold if item.entity_type == entity_type],
            predictions=[item for item in predictions if item.entity_type == entity_type],
        ).overall.model_dump(mode="json")
        for entity_type in sorted(ONTOLOGY_TYPES)
    }
    return report


def _prediction(
    *, prefix: str, index: int, signal_id: str, start: int, end: int,
    entity_type: str, confidence: float, fate: str,
) -> PredictedMention:
    return PredictedMention(
        prediction_id=f"{prefix}-{index:04d}", signal_id=signal_id,
        start=start, end=end, entity_type=entity_type, confidence=confidence,
        canonical_referent=None, candidate_fate=fate,
    )


async def main() -> None:
    integrity = validate_pre_provider()
    _, _, text_by_id = _gold_objects()
    os.environ.update({
        "LLM_PROVIDER": "codex", "CODEX_TRANSPORT": "app-server",
        "CODEX_MODEL": "gpt-5.4", "LLM_MAX_RETRIES": "0",
    })
    set_response_cache(None)
    provider = build_provider()
    if (provider.config.model, provider.config.max_retries) != ("gpt-5.4", 0):
        raise SystemExit("provider pinning failed")

    batch_runs: list[dict[str, Any]] = []
    raw_predictions: list[PredictedMention] = []
    verified_predictions: list[PredictedMention] = []
    all_candidates = []
    try:
        for batch_id, rows in integrity["batches"].items():
            signals = tuple(PersistedSignalText(
                signal_id=UUID(row["signal_id"]), source_channel=row["source_type"],
                content_text=row["text"],
            ) for row in rows)
            capture, usage = _CaptureProvider(provider), LLMUsageAggregator()
            started = time.perf_counter()
            result = None
            outer_error = None
            try:
                with using_usage_aggregator(usage):
                    result = await discover_batch_mentions(provider=capture, signals=signals)
            except Exception as exc:
                outer_error = {
                    "exception_type": type(exc).__name__, "message": str(exc),
                    "stage": "discover_batch_mentions",
                }
            latency = time.perf_counter() - started
            raw = capture.response.model_dump(mode="json") if capture.response else None
            raw_exclusions = []
            if capture.response is not None:
                for item in capture.response.mentions:
                    signal_id, reason = str(item.signal_id), None
                    text = text_by_id.get(signal_id)
                    if item.abstain:
                        reason = "model_abstained"
                    elif text is None:
                        reason = "unknown_signal_id"
                    elif not (0 <= item.span_start < item.span_end <= len(text)):
                        reason = "out_of_bounds"
                    if reason:
                        raw_exclusions.append({
                            "signal_id": signal_id, "surface": item.surface,
                            "start": item.span_start, "end": item.span_end,
                            "reason": reason,
                        })
                    else:
                        raw_predictions.append(_prediction(
                            prefix="v3-raw", index=len(raw_predictions) + 1,
                            signal_id=signal_id, start=item.span_start,
                            end=item.span_end, entity_type=item.entity_type,
                            confidence=item.confidence, fate="raw_model_candidate",
                        ))
            candidates = list(result.candidates) if result is not None else []
            all_candidates.extend(candidates)
            for item in candidates:
                if item.fate is EntityMentionDetectionFate.DETECTED:
                    verified_predictions.append(_prediction(
                        prefix="v3-verified", index=len(verified_predictions) + 1,
                        signal_id=str(item.signal_id), start=item.span_start,
                        end=item.span_end, entity_type=item.entity_type,
                        confidence=item.confidence, fate=item.fate.value,
                    ))
            cardinality_error = None
            if capture.call_count != 1:
                cardinality_error = {
                    "exception_type": "RunnerContractError",
                    "message": f"{batch_id} made {capture.call_count} calls; expected one",
                    "stage": "structured_call_cardinality",
                }
            run = {
                "batch_id": batch_id, "signal_count": len(signals),
                "structured_calls_observed": capture.call_count,
                "raw_structured_output": raw,
                "exact_error": cardinality_error or outer_error or capture.error,
                "production_provider_error": result.provider_error if result else None,
                "mode": result.mode if result else None,
                "latency_seconds": latency,
                "usage": {
                    "calls": usage.call_count,
                    "input_tokens_estimated": usage.total_input_tokens,
                    "output_tokens_estimated": usage.total_output_tokens,
                    "cost_usd_estimated": usage.total_cost_usd,
                },
                "raw_exclusions": raw_exclusions,
                "post_verification_candidates": [{
                    "signal_id": str(item.signal_id), "surface": item.surface,
                    "start": item.span_start, "end": item.span_end,
                    "entity_type": item.entity_type, "confidence": item.confidence,
                    "fate": item.fate.value, "reason_codes": list(item.reason_codes),
                } for item in candidates],
            }
            batch_runs.append(run)
            _atomic_json(CHECKPOINT_PATH, {
                "benchmark": EXPECTED_METADATA["benchmark"],
                "frozen_corpus_sha256": FROZEN_SHA256_V3,
                "one_shot_metadata": ONE_SHOT_EVIDENCE_METADATA,
                "model": "gpt-5.4", "provider_retries": 0,
                "completed_batches": len(batch_runs), "batch_runs": batch_runs,
            })
            if cardinality_error or outer_error:
                raise RuntimeError((cardinality_error or outer_error)["message"])
    finally:
        await close_codex_app_server_client()

    negative_ids = {row["signal_id"] for row in FROZEN_CORPUS_V3 if not row["gold"]}
    dirty = sorted({
        item.signal_id for item in verified_predictions if item.signal_id in negative_ids
    })
    report = {
        "benchmark": EXPECTED_METADATA["benchmark"],
        "evidence_class": "sealed_untouched_holdout_one_shot_completed",
        "frozen_corpus_sha256": FROZEN_SHA256_V3,
        "freeze_and_metadata_verified_before_provider_construction": True,
        "model": provider.config.model, "transport": "app-server",
        "provider_retries": provider.config.max_retries,
        "corpus": {
            "signals": 40, "batches": 4, "batch_size": 10,
            "gold_mentions": integrity["gold_mentions"],
            "negative_signals": len(negative_ids),
        },
        "scope": {"canonical_link_claim": False},
        "pre_verification": {"metrics": _score(raw_predictions)},
        "post_verification": {
            "metrics": _score(verified_predictions),
            "candidate_fate_distribution": dict(sorted(Counter(
                item.fate.value for item in all_candidates
            ).items())),
            "negative_cleanliness": {
                "negative_signal_count": len(negative_ids),
                "clean_negative_signals": len(negative_ids) - len(dirty),
                "rate": (len(negative_ids) - len(dirty)) / len(negative_ids),
                "dirty_signal_ids": dirty,
            },
        },
        "operational_totals": {
            "structured_calls_observed": sum(r["structured_calls_observed"] for r in batch_runs),
            "provider_errors": sum(bool(r["exact_error"] or r["production_provider_error"]) for r in batch_runs),
            "input_tokens_estimated": sum(r["usage"]["input_tokens_estimated"] for r in batch_runs),
            "output_tokens_estimated": sum(r["usage"]["output_tokens_estimated"] for r in batch_runs),
            "cost_usd_estimated": sum(r["usage"]["cost_usd_estimated"] for r in batch_runs),
            "latency_seconds": sum(r["latency_seconds"] for r in batch_runs),
        },
        "batch_runs": batch_runs,
    }
    _atomic_json(REPORT_PATH, report)
    report_sha = hashlib.sha256(REPORT_PATH.read_bytes()).hexdigest()
    _atomic_json(RECEIPT_PATH, {
        "benchmark": EXPECTED_METADATA["benchmark"],
        "status": "completed", "provider_executions": 1,
        "structured_calls": len(batch_runs), "model": "gpt-5.4",
        "provider_retries": 0, "frozen_corpus_sha256": FROZEN_SHA256_V3,
        "report_path": str(REPORT_PATH), "report_sha256": report_sha,
    })
    print(json.dumps({
        "report_path": str(REPORT_PATH), "receipt_path": str(RECEIPT_PATH),
        "post_verification": report["post_verification"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
