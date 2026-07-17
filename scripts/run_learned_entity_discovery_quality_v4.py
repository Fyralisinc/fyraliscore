#!/usr/bin/env python3
"""Run the precommitted broad v4 holdout once, in genuine signal batches."""

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
    GoldMention, GoldSignal, PredictedMention, evaluate_gold_entity_extraction,
)
from lib.llm.provider import (
    LLMUsageAggregator, build_provider, close_codex_app_server_client,
    set_response_cache, using_usage_aggregator,
)
from services.domain.entity_grounding.learned_discovery import (
    PersistedSignalText, discover_batch_mentions,
)
from tests.evaluation.learned_entity_discovery_quality_corpus_v4 import (
    FROZEN_CORPUS_V4, FROZEN_SHA256_V4, ONE_SHOT_EVIDENCE_METADATA,
    ONTOLOGY_TYPES, computed_sha256_v4,
)

ARTIFACT_DIR = Path("/tmp/learned_entity_discovery_quality_v4")
CHECKPOINT_PATH = ARTIFACT_DIR / "checkpoint.json"
REPORT_PATH = ARTIFACT_DIR / "report.json"
RECEIPT_PATH = ARTIFACT_DIR / "execution_receipt.json"
EXPECTED_METADATA = {
    "benchmark": "learned-entity-discovery-quality-v4",
    "evidence_class": "precommitted_untouched_broad_holdout",
    "sealed_before_first_provider_call": True,
    "provider_execution_count_at_seal": 0,
    "evidence_status": "not_executed",
    "allowed_provider_executions": 1,
    "split_policy": "organization_entity_time_text_disjoint_from_v1_v2_v3_and_development",
    "time_window": "2034-01-01/2034-12-31",
    "canonical_link_claim_permitted": False,
    "implicit_reference_resolution_claim_permitted": False,
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

    async def structured(self, *, system: str, user: str, schema: type[BaseModel],
                         temperature: float, max_tokens: int) -> Any:
        self.call_count += 1
        try:
            self.response = await self.delegate.structured(
                system=system, user=user, schema=schema,
                temperature=temperature, max_tokens=max_tokens,
            )
            return self.response
        except Exception as exc:
            self.error = {"type": type(exc).__name__, "message": str(exc)}
            raise


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def validate_pre_provider() -> dict[str, Any]:
    if RECEIPT_PATH.exists() or CHECKPOINT_PATH.exists() or REPORT_PATH.exists():
        raise SystemExit("v4 execution artifact exists; all reruns are refused")
    if ONE_SHOT_EVIDENCE_METADATA != EXPECTED_METADATA:
        raise SystemExit("v4 metadata mismatch")
    if computed_sha256_v4() != FROZEN_SHA256_V4:
        raise SystemExit("v4 frozen-corpus digest mismatch")
    batches = {
        f"v4-batch-{index}": tuple(
            row for row in FROZEN_CORPUS_V4
            if row["batch_id"] == f"v4-batch-{index}"
        ) for index in range(1, 5)
    }
    if any(len(rows) != 10 for rows in batches.values()):
        raise SystemExit("v4 requires four ten-signal batches")
    if any(sum(bool(row["gold"]) for row in rows) != 5 for rows in batches.values()):
        raise SystemExit("v4 requires five positive signals per batch")
    gold = [mention for row in FROZEN_CORPUS_V4 for mention in row["gold"]]
    if {mention["entity_type"] for mention in gold} != ONTOLOGY_TYPES:
        raise SystemExit("v4 ontology coverage mismatch")
    return {"batches": batches, "gold_count": len(gold)}


def _score(predictions: list[PredictedMention]) -> dict[str, Any]:
    signals = [GoldSignal(
        signal_id=row["signal_id"], batch_id=row["batch_id"],
        source_type=row["source_type"], text=row["text"],
        slack_context=SLACK_CONTEXT_MAP[row["slack_context"]],
    ) for row in FROZEN_CORPUS_V4]
    gold = [GoldMention(
        mention_id=item["mention_id"], signal_id=row["signal_id"],
        start=item["start"], end=item["end"], entity_type=item["entity_type"],
    ) for row in FROZEN_CORPUS_V4 for item in row["gold"]]
    report = evaluate_gold_entity_extraction(
        signals=signals, gold_mentions=gold, predictions=predictions,
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


async def main() -> None:
    integrity = validate_pre_provider()
    _atomic_json(RECEIPT_PATH, {
        "status": "running", "benchmark": EXPECTED_METADATA["benchmark"],
        "frozen_corpus_sha256": FROZEN_SHA256_V4,
        "started_unix_seconds": time.time(), "attempt": 1,
    })
    os.environ.update({
        "LLM_PROVIDER": "codex", "CODEX_TRANSPORT": "app-server",
        "CODEX_MODEL": "gpt-5.4", "LLM_MAX_RETRIES": "0",
    })
    set_response_cache(None)
    provider = build_provider()
    predictions: list[PredictedMention] = []
    batch_runs: list[dict[str, Any]] = []
    try:
        for batch_id, rows in integrity["batches"].items():
            signals = tuple(PersistedSignalText(
                signal_id=UUID(row["signal_id"]),
                source_channel=row["source_type"], content_text=row["text"],
            ) for row in rows)
            capture, usage = _CaptureProvider(provider), LLMUsageAggregator()
            started = time.perf_counter()
            with using_usage_aggregator(usage):
                result = await discover_batch_mentions(provider=capture, signals=signals)
            detected = [
                candidate for candidate in result.candidates
                if candidate.fate is EntityMentionDetectionFate.DETECTED
            ]
            for candidate in detected:
                predictions.append(PredictedMention(
                    prediction_id=f"v4-{len(predictions)+1:04d}",
                    signal_id=str(candidate.signal_id), start=candidate.span_start,
                    end=candidate.span_end, entity_type=candidate.entity_type,
                    confidence=candidate.confidence,
                    candidate_fate=candidate.fate.value,
                ))
            run = {
                "batch_id": batch_id, "signal_count": len(rows),
                "structured_calls_observed": capture.call_count,
                "raw_structured_output": (
                    capture.response.model_dump(mode="json") if capture.response else None
                ),
                "error": capture.error or result.provider_error,
                "mode": result.mode, "latency_seconds": time.perf_counter() - started,
                "usage": {"calls": usage.call_count,
                          "input_tokens": usage.total_input_tokens,
                          "output_tokens": usage.total_output_tokens},
                "verified_candidates": [{
                    "signal_id": str(item.signal_id), "surface": item.surface,
                    "start": item.span_start, "end": item.span_end,
                    "entity_type": item.entity_type, "confidence": item.confidence,
                    "type_confidence": item.type_confidence,
                    "fate": item.fate.value, "reason_codes": list(item.reason_codes),
                } for item in result.candidates],
            }
            if capture.call_count != 1:
                run["error"] = f"expected one structured call, observed {capture.call_count}"
            batch_runs.append(run)
            _atomic_json(CHECKPOINT_PATH, {
                "benchmark": EXPECTED_METADATA["benchmark"],
                "frozen_corpus_sha256": FROZEN_SHA256_V4,
                "completed_batch_count": len(batch_runs), "batch_runs": batch_runs,
            })
            if run["error"]:
                raise RuntimeError(f"{batch_id} failed: {run['error']}")

        metrics = _score(predictions)
        negative_ids = {
            row["signal_id"] for row in FROZEN_CORPUS_V4 if not row["gold"]
        }
        dirty = sorted({item.signal_id for item in predictions} & negative_ids)
        report = {
            "schema_version": "learned-entity-discovery-quality-v4",
            "evidence_class": EXPECTED_METADATA["evidence_class"],
            "frozen_corpus_sha256": FROZEN_SHA256_V4,
            "precommit_commit": "6f9da6a2",
            "model": "gpt-5.4", "provider_retries": 0,
            "batch_only": True, "batch_count": 4, "signal_count": 40,
            "gold_count": integrity["gold_count"], "batch_runs": batch_runs,
            "metrics": metrics,
            "negative_cleanliness": {
                "negative_signal_count": len(negative_ids),
                "clean_negative_signals": len(negative_ids) - len(dirty),
                "rate": (len(negative_ids) - len(dirty)) / len(negative_ids),
                "dirty_signal_ids": dirty,
            },
            "operational": {
                "structured_calls": sum(x["structured_calls_observed"] for x in batch_runs),
                "provider_errors": sum(bool(x["error"]) for x in batch_runs),
                "candidate_fates": dict(Counter(
                    item["fate"] for run in batch_runs for item in run["verified_candidates"]
                )),
            },
            "proof_boundary": [
                "literal mention discovery and role-grounded typing only",
                "no canonical alias-link or implicit-reference-resolution claim",
                "bounded synthetic normalized signals; no connector claim",
            ],
        }
        _atomic_json(REPORT_PATH, report)
        _atomic_json(RECEIPT_PATH, {
            "status": "completed", "attempt": 1,
            "benchmark": EXPECTED_METADATA["benchmark"],
            "frozen_corpus_sha256": FROZEN_SHA256_V4,
            "report_sha256": hashlib.sha256(REPORT_PATH.read_bytes()).hexdigest(),
            "completed_unix_seconds": time.time(),
        })
        print(json.dumps({"report": str(REPORT_PATH),
                          "overall": metrics["overall"],
                          "negative_cleanliness": report["negative_cleanliness"]}, indent=2))
    except Exception as exc:
        current = json.loads(RECEIPT_PATH.read_text())
        current.update({"status": "failed", "error": f"{type(exc).__name__}: {exc}"})
        _atomic_json(RECEIPT_PATH, current)
        raise
    finally:
        await close_codex_app_server_client()


if __name__ == "__main__":
    asyncio.run(main())
