#!/usr/bin/env python3
"""Reverify sealed v3 raw outputs through the current production verifier.

This is a deterministic historical-artifact replay, never a new holdout or a
provider rerun.  It diagnoses whether old failures remain runtime failures.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from pathlib import Path
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.contracts.entity_mentions import EntityMentionDetectionFate
from lib.evaluation.entity_extraction_gold import PredictedMention
from services.domain.entity_grounding.learned_discovery import (
    DISCOVERY_VERSION, LearnedMentionBatch, PersistedSignalText,
    discover_batch_mentions,
)
from scripts.run_learned_entity_discovery_quality_v3 import _score
from tests.evaluation.learned_entity_discovery_quality_corpus_v3 import (
    FROZEN_CORPUS_V3, FROZEN_SHA256_V3, computed_sha256_v3,
)

SOURCE_REPORT = Path("/tmp/learned_entity_discovery_quality_v3/report.json")
OUTPUT_PATH = Path("/tmp/learned_entity_discovery_quality_v3/latest_runtime_replay.json")


class _ReplayProvider:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.calls = 0

    async def structured(self, **_: Any) -> LearnedMentionBatch:
        self.calls += 1
        return LearnedMentionBatch.model_validate(self.payload)


async def replay(source_path: Path = SOURCE_REPORT) -> dict[str, Any]:
    if computed_sha256_v3() != FROZEN_SHA256_V3:
        raise ValueError("sealed v3 corpus digest mismatch")
    source_bytes = source_path.read_bytes()
    source = json.loads(source_bytes)
    if source.get("frozen_corpus_sha256") != FROZEN_SHA256_V3:
        raise ValueError("saved v3 report does not bind the frozen corpus")
    rows_by_batch = {
        f"v3-batch-{index}": tuple(
            row for row in FROZEN_CORPUS_V3
            if row["batch_id"] == f"v3-batch-{index}"
        ) for index in range(1, 5)
    }
    predictions: list[PredictedMention] = []
    replay_runs = []
    for saved_run in source["batch_runs"]:
        batch_id = saved_run["batch_id"]
        raw = saved_run.get("raw_structured_output")
        if not raw or saved_run.get("exact_error"):
            raise ValueError(f"{batch_id} lacks a successful raw structured output")
        provider = _ReplayProvider(raw)
        result = await discover_batch_mentions(
            provider=provider,
            signals=tuple(PersistedSignalText(
                signal_id=UUID(row["signal_id"]),
                source_channel=row["source_type"], content_text=row["text"],
            ) for row in rows_by_batch[batch_id]),
        )
        if provider.calls != 1 or result.provider_error:
            raise ValueError(f"{batch_id} replay contract failed")
        detected = [
            item for item in result.candidates
            if item.fate is EntityMentionDetectionFate.DETECTED
        ]
        for item in detected:
            predictions.append(PredictedMention(
                prediction_id=f"v3-latest-{len(predictions)+1:04d}",
                signal_id=str(item.signal_id), start=item.span_start,
                end=item.span_end, entity_type=item.entity_type,
                confidence=item.confidence, candidate_fate=item.fate.value,
            ))
        replay_runs.append({
            "batch_id": batch_id,
            "raw_output_sha256": hashlib.sha256(
                json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
            "detected_count": len(detected),
            "expanded_designation_count": sum(
                "learned_span_expanded_attached_type_designator" in item.reason_codes
                for item in result.candidates
            ),
        })
    latest = _score(predictions)
    return {
        "schema_version": "entity-discovery-v3-latest-runtime-replay-v1",
        "evidence_class": "historical_saved_raw_output_reverification",
        "source_report": str(source_path),
        "source_report_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "frozen_corpus_sha256": FROZEN_SHA256_V3,
        "runtime_discovery_version": DISCOVERY_VERSION,
        "provider_calls": 0,
        "replay_runs": replay_runs,
        "original_post_verification": source["post_verification"]["metrics"],
        "latest_runtime_post_verification": latest,
        "proof_boundary": (
            "Deterministic reverification of already-seen saved model outputs; "
            "diagnostic correction evidence only, never untouched generalization evidence."
        ),
    }


async def main() -> None:
    report = await replay()
    OUTPUT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({
        "output": str(OUTPUT_PATH),
        "original": report["original_post_verification"]["overall"],
        "latest": report["latest_runtime_post_verification"]["overall"],
        "original_workstream": report["original_post_verification"]["by_entity_type"]["workstream"],
        "latest_workstream": report["latest_runtime_post_verification"]["by_entity_type"]["workstream"],
    }, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
