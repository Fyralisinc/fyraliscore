#!/usr/bin/env python3
"""Execute sealed small boundary/type holdout v3 exactly once."""

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
from tests.evaluation.learned_entity_discovery_boundary_type_holdout_v3 import (  # noqa: E402
    FROZEN_CORPUS_V3, FROZEN_SHA256_V3, ONE_SHOT_METADATA, VERSION,
    computed_sha256_v3,
)

DIR = Path("/tmp/learned_entity_boundary_type_holdout_v3")
REPORT, RECEIPT, CHECKPOINT = (DIR / name for name in
    ("report.json", "completion_receipt.json", "checkpoint.json"))


def _atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n")
    temporary.replace(path)


class Capture:
    def __init__(self, delegate) -> None:
        self.delegate, self.response = delegate, None

    async def structured(self, *, system: str, user: str, schema: type[BaseModel],
        temperature: float, max_tokens: int):
        self.response = await self.delegate.structured(system=system, user=user,
            schema=schema, temperature=temperature, max_tokens=max_tokens)
        return self.response


async def main() -> None:
    if any(path.exists() for path in (REPORT, RECEIPT, CHECKPOINT)):
        raise SystemExit("sealed v3 already attempted; rerun refused")
    if computed_sha256_v3() != FROZEN_SHA256_V3 or len(FROZEN_CORPUS_V3) != 10:
        raise SystemExit("sealed v3 corpus contract mismatch")
    signals = [GoldSignal(signal_id=r["signal_id"], batch_id=r["batch_id"],
        source_type=r["source_type"], text=r["text"],
        slack_context="threaded" if r["source_type"] == "slack" else "not_slack")
        for r in FROZEN_CORPUS_V3]
    gold = [GoldMention(mention_id=m["mention_id"], signal_id=r["signal_id"],
        start=m["start"], end=m["end"], entity_type=m["entity_type"],
        canonical_referent=None) for r in FROZEN_CORPUS_V3 for m in r["gold"]]
    evaluate_gold_entity_extraction(signals=signals, gold_mentions=gold, predictions=[])
    _atomic(RECEIPT, {"schema_version": "boundary-type-holdout-v3-receipt-v1",
        "status": "running", "run_attempts": 1, "corpus_sha256": FROZEN_SHA256_V3})
    os.environ.update({"LLM_PROVIDER": "codex", "CODEX_TRANSPORT": "app-server",
        "CODEX_MODEL": "gpt-5.4", "LLM_MAX_RETRIES": "0"})
    set_response_cache(None)
    predictions, candidates = [], []
    try:
        provider = build_provider()
        capture = Capture(provider)
        result = await discover_batch_mentions(provider=capture, signals=tuple(
            PersistedSignalText(UUID(r["signal_id"]), r["source_type"], r["text"])
            for r in FROZEN_CORPUS_V3))
        for c in result.candidates:
            candidates.append({"signal_id": str(c.signal_id), "surface": c.surface,
                "start": c.span_start, "end": c.span_end, "entity_type": c.entity_type,
                "confidence": c.confidence, "type_confidence": c.type_confidence,
                "fate": c.fate.value, "reason_codes": list(c.reason_codes)})
            if c.fate is EntityMentionDetectionFate.DETECTED:
                predictions.append(PredictedMention(prediction_id=f"h3-{len(predictions)+1}",
                    signal_id=str(c.signal_id), start=c.span_start, end=c.span_end,
                    entity_type=c.entity_type, confidence=c.confidence,
                    canonical_referent=None, candidate_fate=c.fate.value))
        raw = capture.response.model_dump(mode="json") if capture.response else None
        _atomic(CHECKPOINT, {"schema_version": VERSION, "completed_batches": 1,
            "raw_structured_output": raw, "verified_candidates": candidates})
    except Exception as exc:
        _atomic(RECEIPT, {"schema_version": "boundary-type-holdout-v3-receipt-v1",
            "status": "failed", "run_attempts": 1, "corpus_sha256": FROZEN_SHA256_V3,
            "error": f"{type(exc).__name__}: {exc}"[:500]})
        raise
    finally:
        await close_codex_app_server_client()
    metrics = evaluate_gold_entity_extraction(
        signals=signals, gold_mentions=gold, predictions=predictions,
    ).model_dump(mode="json")
    artifact = {"schema_version": VERSION, "evidence_class": "sealed_untouched_holdout",
        "one_shot_metadata": ONE_SHOT_METADATA, "corpus_sha256": FROZEN_SHA256_V3,
        "raw_structured_output": raw, "verified_candidates": candidates,
        "metrics": metrics, "meets_exceptional_threshold": (
            metrics["overall"]["span_f1"] >= .9
            and metrics["overall"]["type_accuracy"] >= .95)}
    _atomic(REPORT, artifact)
    _atomic(RECEIPT, {"schema_version": "boundary-type-holdout-v3-receipt-v1",
        "status": "completed", "run_attempts": 1, "provider_executions": 1,
        "corpus_sha256": FROZEN_SHA256_V3,
        "report_sha256": hashlib.sha256(REPORT.read_bytes()).hexdigest()})
    print(REPORT)
    print(json.dumps({"overall": metrics["overall"],
        "meets_exceptional_threshold": artifact["meets_exceptional_threshold"]}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
