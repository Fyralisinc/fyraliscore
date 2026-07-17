#!/usr/bin/env python3
"""Execute sealed boundary/type holdout v2 exactly once in three batches."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import sys
from uuid import UUID

from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.contracts.entity_mentions import EntityMentionDetectionFate  # noqa: E402
from lib.evaluation.entity_extraction_gold import (  # noqa: E402
    GoldMention, GoldSignal, PredictedMention, evaluate_gold_entity_extraction,
)
from lib.llm.provider import (  # noqa: E402
    build_provider, close_codex_app_server_client, set_response_cache,
)
from services.domain.entity_grounding.learned_discovery import (  # noqa: E402
    PersistedSignalText, discover_batch_mentions,
)
from tests.evaluation.learned_entity_discovery_boundary_type_holdout_v2 import (  # noqa: E402
    FROZEN_CORPUS_V2, FROZEN_SHA256_V2, ONE_SHOT_METADATA, VERSION,
    computed_sha256_v2,
)

ARTIFACT_DIR = Path("/tmp/learned_entity_boundary_type_holdout_v2")
REPORT = ARTIFACT_DIR / "report.json"
RECEIPT = ARTIFACT_DIR / "completion_receipt.json"
CHECKPOINT = ARTIFACT_DIR / "checkpoint.json"
EXCEPTIONAL_OVERALL_F1 = 0.90
EXCEPTIONAL_WORST_TYPE_F1 = 0.80


def _atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def _inputs():
    if RECEIPT.exists() or REPORT.exists() or CHECKPOINT.exists():
        raise SystemExit("sealed v2 already executed; rerun refused")
    if computed_sha256_v2() != FROZEN_SHA256_V2:
        raise SystemExit("sealed v2 corpus SHA mismatch")
    if len(FROZEN_CORPUS_V2) != 30:
        raise SystemExit("sealed v2 must contain 30 signals")
    batches = {batch: tuple(r for r in FROZEN_CORPUS_V2 if r["batch_id"] == batch)
        for batch in sorted({r["batch_id"] for r in FROZEN_CORPUS_V2})}
    if len(batches) != 3 or any(len(rows) != 10 for rows in batches.values()):
        raise SystemExit("sealed v2 requires three genuine ten-signal batches")
    signals = [GoldSignal(signal_id=r["signal_id"], batch_id=r["batch_id"],
        source_type=r["source_type"], text=r["text"],
        slack_context="threaded" if r["source_type"] == "slack" else "not_slack")
        for r in FROZEN_CORPUS_V2]
    gold = [GoldMention(mention_id=m["mention_id"], signal_id=r["signal_id"],
        start=m["start"], end=m["end"], entity_type=m["entity_type"],
        canonical_referent=None) for r in FROZEN_CORPUS_V2 for m in r["gold"]]
    evaluate_gold_entity_extraction(signals=signals, gold_mentions=gold, predictions=[])
    return batches, signals, gold


class _CaptureProvider:
    def __init__(self, delegate) -> None:
        self.delegate = delegate
        self.response = None

    async def structured(
        self, *, system: str, user: str, schema: type[BaseModel],
        temperature: float, max_tokens: int,
    ):
        self.response = await self.delegate.structured(
            system=system, user=user, schema=schema,
            temperature=temperature, max_tokens=max_tokens,
        )
        return self.response


def _score(signals, gold, predictions):
    overall = evaluate_gold_entity_extraction(
        signals=signals, gold_mentions=gold, predictions=predictions,
    ).model_dump(mode="json")
    by_type = {}
    for entity_type in sorted({item.entity_type for item in gold}):
        typed_gold = [item for item in gold if item.entity_type == entity_type]
        typed_predictions = [item for item in predictions if item.entity_type == entity_type]
        by_type[entity_type] = evaluate_gold_entity_extraction(
            signals=signals, gold_mentions=typed_gold, predictions=typed_predictions,
        ).overall.model_dump(mode="json")
    return overall, by_type


async def main() -> None:
    batches, signals, gold = _inputs()
    _atomic(RECEIPT, {"schema_version": "boundary-type-holdout-v2-receipt-v1",
        "status": "running", "run_attempts": 1,
        "corpus_sha256": FROZEN_SHA256_V2})
    os.environ.update({"LLM_PROVIDER": "codex", "CODEX_TRANSPORT": "app-server",
        "CODEX_MODEL": "gpt-5.4", "LLM_MAX_RETRIES": "0"})
    set_response_cache(None)
    provider = build_provider()
    predictions, candidates, runs = [], [], []
    try:
        for batch_id, rows in batches.items():
            capture = _CaptureProvider(provider)
            result = await discover_batch_mentions(provider=capture, signals=tuple(
                PersistedSignalText(UUID(r["signal_id"]), r["source_type"], r["text"])
                for r in rows))
            runs.append({"batch_id": batch_id, "mode": result.mode,
                "provider_error": result.provider_error, "signal_count": len(rows),
                "raw_structured_output": (
                    capture.response.model_dump(mode="json")
                    if capture.response is not None else None)})
            for candidate in result.candidates:
                candidates.append({"signal_id": str(candidate.signal_id),
                    "surface": candidate.surface, "start": candidate.span_start,
                    "end": candidate.span_end, "entity_type": candidate.entity_type,
                    "confidence": candidate.confidence,
                    "type_confidence": candidate.type_confidence,
                    "fate": candidate.fate.value,
                    "reason_codes": list(candidate.reason_codes)})
                if candidate.fate is EntityMentionDetectionFate.DETECTED:
                    predictions.append(PredictedMention(
                        prediction_id=f"h2-{len(predictions)+1}",
                        signal_id=str(candidate.signal_id), start=candidate.span_start,
                        end=candidate.span_end, entity_type=candidate.entity_type,
                        confidence=candidate.confidence, canonical_referent=None,
                        candidate_fate=candidate.fate.value))
            _atomic(CHECKPOINT, {"schema_version": VERSION,
                "corpus_sha256": FROZEN_SHA256_V2,
                "completed_batches": len(runs), "batch_runs": runs,
                "verified_candidates": candidates})
    except Exception as exc:
        _atomic(RECEIPT, {"schema_version": "boundary-type-holdout-v2-receipt-v1",
            "status": "failed", "run_attempts": 1,
            "corpus_sha256": FROZEN_SHA256_V2,
            "error": f"{type(exc).__name__}: {exc}"[:500]})
        raise
    finally:
        await close_codex_app_server_client()
    metrics, by_type = _score(signals, gold, predictions)
    type_f1 = {kind: row["span_f1"] for kind, row in by_type.items()}
    worst_type = min(type_f1, key=lambda kind: type_f1[kind] if type_f1[kind] is not None else -1)
    exceptional = (metrics["overall"]["span_f1"] >= EXCEPTIONAL_OVERALL_F1
        and type_f1[worst_type] is not None
        and type_f1[worst_type] >= EXCEPTIONAL_WORST_TYPE_F1)
    artifact = {"schema_version": VERSION, "evidence_class": "sealed_untouched_holdout",
        "one_shot_metadata": ONE_SHOT_METADATA, "corpus_sha256": FROZEN_SHA256_V2,
        "provider": "codex-app-server/gpt-5.4", "batch_runs": runs,
        "metrics": metrics, "by_entity_type": by_type,
        "exceptional_policy": {"minimum_overall_span_f1": EXCEPTIONAL_OVERALL_F1,
            "minimum_worst_type_span_f1": EXCEPTIONAL_WORST_TYPE_F1},
        "worst_type": worst_type, "worst_type_span_f1": type_f1[worst_type],
        "meets_exceptional_threshold": exceptional, "verified_candidates": candidates}
    _atomic(REPORT, artifact)
    _atomic(RECEIPT, {"schema_version": "boundary-type-holdout-v2-receipt-v1",
        "status": "completed", "run_attempts": 1,
        "report_sha256": hashlib.sha256(REPORT.read_bytes()).hexdigest(),
        "corpus_sha256": FROZEN_SHA256_V2, "provider_executions": 1})
    print(REPORT)
    print(json.dumps({"overall": metrics["overall"], "worst_type": worst_type,
        "worst_type_span_f1": type_f1[worst_type],
        "meets_exceptional_threshold": exceptional}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
