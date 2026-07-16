#!/usr/bin/env python3
"""Recover the sealed v2 holdout from already-completed Codex app-server logs.

This exists for the 2026-07-17 evaluator incident in which all eight provider
turns completed but report rendering failed afterward. It never invokes a
provider. Recovery is admissible only when exactly one output covers each
frozen batch and the supplied process UUID identifies exactly eight turns.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.contracts.entity_mentions import EntityMentionDetectionFate
from lib.evaluation.entity_extraction_gold import PredictedMention
from scripts.run_learned_entity_discovery_quality_v2 import (
    FROZEN_CORPUS_V2,
    FROZEN_SHA256_V2,
    REPORT_PATH,
    _gold_objects,
    _raw_predictions,
    _score,
    validate_frozen_corpus,
)
from services.domain.entity_grounding.learned_discovery import (
    LearnedMentionBatch,
    PersistedSignalText,
    _verify_candidates,
)


def _decode_output(body: str) -> dict[str, Any]:
    marker = 'OutputText { text: "'
    start = body.find(marker)
    end = body.find('" }], phase:', start + len(marker))
    if start < 0 or end < 0:
        raise ValueError("Codex output log has an unknown envelope")
    rust_escaped = body[start + len(marker):end]
    # Rust's Debug formatter uses ``\u{301}``, while JSON string escaping
    # requires ``\u0301``. Decode those scalar escapes before asking the JSON
    # decoder to unwrap the logged string.
    rust_escaped = re.sub(
        r"\\u\{([0-9a-fA-F]+)\}",
        lambda match: chr(int(match.group(1), 16)),
        rust_escaped,
    )
    decoded_text = json.loads('"' + rust_escaped + '"')
    payload = json.loads(decoded_text)
    if not isinstance(payload, dict):
        raise ValueError("recovered structured output is not an object")
    return payload


def recover(*, log_db: Path, process_uuid: str) -> dict[str, Any]:
    integrity = validate_frozen_corpus()
    _gold_objects()
    conn = sqlite3.connect(f"file:{log_db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            """
            SELECT thread_id, ts, feedback_log_body
            FROM logs
            WHERE process_uuid=?
              AND target='codex_core::stream_events_utils'
              AND feedback_log_body LIKE '%OutputText%'
              AND feedback_log_body LIKE '%mentions%'
            ORDER BY ts, ts_nanos, id
            """,
            (process_uuid,),
        ).fetchall()
    finally:
        conn.close()
    if len(rows) != 8 or len({row[0] for row in rows}) != 8:
        raise SystemExit(
            f"recovery requires exactly eight unique completed turns; found {len(rows)}"
        )

    raw_outputs = [_decode_output(str(row[2])) for row in rows]
    by_signal = {
        UUID(row["signal_id"]): PersistedSignalText(
            signal_id=UUID(row["signal_id"]),
            source_channel=row["source_type"],
            content_text=row["text"],
        )
        for row in FROZEN_CORPUS_V2
    }
    expected_batches = {
        batch_id: {UUID(row["signal_id"]) for row in batch_rows}
        for batch_id, batch_rows in integrity["batches"].items()
    }
    recovered_batches: dict[str, tuple[LearnedMentionBatch | None, str | None, int]] = {}
    for row_index, payload in enumerate(raw_outputs):
        signal_ids = {
            UUID(str(mention["signal_id"]))
            for mention in payload.get("mentions", [])
            if isinstance(mention, dict) and mention.get("signal_id")
        }
        matching = [
            batch_id for batch_id, expected in expected_batches.items()
            if signal_ids <= expected
        ]
        if len(matching) != 1:
            raise SystemExit("a recovered response does not map to exactly one batch")
        batch_id = matching[0]
        if batch_id in recovered_batches:
            raise SystemExit(f"duplicate recovered output for {batch_id}")
        try:
            response = LearnedMentionBatch.model_validate(payload)
            provider_error = None
        except Exception as exc:
            # This mirrors the original structured provider boundary: an
            # out-of-schema response never reached production verification and
            # the batch entered deterministic-fallback mode.
            response = None
            provider_error = f"{type(exc).__name__}: {exc}"[:500]
        recovered_batches[batch_id] = (response, provider_error, row_index)
    if set(recovered_batches) != set(expected_batches):
        raise SystemExit("recovered outputs do not cover every frozen batch")

    candidates = []
    ordered_responses = []
    batch_runs = []
    for batch_id, batch_rows in integrity["batches"].items():
        response, provider_error, row_index = recovered_batches[batch_id]
        ordered_responses.append(response)
        signals = tuple(by_signal[UUID(row["signal_id"])] for row in batch_rows)
        verified = (
            _verify_candidates(response.mentions, signals)
            if response is not None
            else ()
        )
        candidates.extend(verified)
        batch_runs.append({
            "batch_id": batch_id,
            "signal_count": len(signals),
            "structured_calls_recovered": 1,
            "codex_thread_id": rows[row_index][0],
            "mode": "learned" if response is not None else "deterministic_fallback",
            "provider_error": provider_error,
            "post_verification_candidates": [
                {
                    "signal_id": str(item.signal_id),
                    "surface": item.surface,
                    "start": item.span_start,
                    "end": item.span_end,
                    "entity_type": item.entity_type,
                    "confidence": item.confidence,
                    "fate": item.fate.value,
                    "reason_codes": list(item.reason_codes),
                }
                for item in verified
            ],
        })

    _, _, text_by_id = _gold_objects()
    raw_predictions, raw_exclusions = _raw_predictions(
        ordered_responses, text_by_id
    )
    accepted = [
        item for item in candidates
        if item.fate is EntityMentionDetectionFate.DETECTED
    ]
    post_predictions = [PredictedMention(
        prediction_id=f"v2-recovered-{index:04d}",
        signal_id=str(item.signal_id),
        start=item.span_start,
        end=item.span_end,
        entity_type=item.entity_type,
        canonical_referent=None,
        confidence=item.confidence,
        abstained=False,
        candidate_fate=item.fate.value,
    ) for index, item in enumerate(accepted, 1)]
    negative_ids = {
        row["signal_id"] for row in FROZEN_CORPUS_V2 if not row["gold"]
    }
    dirty = sorted({
        str(item.signal_id) for item in accepted
        if str(item.signal_id) in negative_ids
    })
    report = {
        "benchmark": "learned-entity-discovery-quality-v2",
        "frozen_corpus_sha256": FROZEN_SHA256_V2,
        "freeze_verified_before_original_provider_construction": True,
        "model": "gpt-5.4",
        "transport": "app-server",
        "provider_retries": 0,
        "evidence_status": {
            "kind": "exact_log_recovery_after_postprocessing_failure",
            "provider_rerun": False,
            "structured_turns_recovered": len(rows),
            "process_uuid": process_uuid,
            "operational_usage_and_latency_unavailable": True,
        },
        "corpus": {
            "signals": 80,
            "batches": 8,
            "batch_size": 10,
            "negative_signals": integrity["negative_signals"],
            "gold_mentions": integrity["gold_mentions"],
            "sources": integrity["sources"],
            "entity_types": integrity["entity_types"],
            "slack_context_strata": integrity["slack_context_strata"],
        },
        "scope": {
            "canonical_referents": "all_null",
            "canonical_link_claim": False,
            "pre_verification": "raw non-abstained recovered model coordinates",
            "post_verification": "production verification and admission policy",
        },
        "pre_verification": {
            "metrics": _score(raw_predictions),
            "excluded_raw_candidates": raw_exclusions,
        },
        "post_verification": {
            "metrics": _score(post_predictions),
            "candidate_fate_distribution": dict(sorted(Counter(
                item.fate.value for item in candidates
            ).items())),
            "negative_cleanliness": {
                "negative_signal_count": len(negative_ids),
                "clean_negative_signals": len(negative_ids) - len(dirty),
                "rate": (len(negative_ids) - len(dirty)) / len(negative_ids),
                "dirty_signal_ids": dirty,
            },
        },
        "batch_runs": batch_runs,
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-db", type=Path, required=True)
    parser.add_argument("--process-uuid", required=True)
    parser.add_argument("--output", type=Path, default=REPORT_PATH)
    args = parser.parse_args()
    report = recover(log_db=args.log_db, process_uuid=args.process_uuid)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "report_path": str(args.output),
        "post_verification": report["post_verification"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
