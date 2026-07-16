#!/usr/bin/env python3
"""Execute the sealed v2 learned-discovery holdout once, one call per batch.

The corpus hash and structure are verified before provider construction. This
runner has intentionally not been executed as part of corpus construction.
"""

from __future__ import annotations

import asyncio
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
from tests.evaluation.learned_entity_discovery_quality_corpus_v2 import (
    FROZEN_CORPUS_V2,
    FROZEN_SHA256_V2,
    computed_sha256_v2,
)

REPORT_PATH = Path("/tmp/learned_entity_discovery_quality_v2_report.json")
EXPECTED_SIGNALS = 80
EXPECTED_BATCHES = 8
EXPECTED_BATCH_SIZE = 10


class _CaptureProvider:
    """Transparent provider facade retaining the sole raw structured result."""

    def __init__(self, delegate: Any) -> None:
        self.delegate = delegate
        self.call_count = 0
        self.response: Any | None = None

    async def structured(
        self,
        *,
        system: str,
        user: str,
        schema: type[BaseModel],
        temperature: float,
        max_tokens: int,
    ) -> Any:
        self.call_count += 1
        self.response = await self.delegate.structured(
            system=system,
            user=user,
            schema=schema,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return self.response


def validate_frozen_corpus() -> dict[str, Any]:
    actual_hash = computed_sha256_v2()
    if actual_hash != FROZEN_SHA256_V2:
        raise SystemExit(
            f"frozen corpus hash mismatch: {actual_hash} != {FROZEN_SHA256_V2}"
        )
    if len(FROZEN_CORPUS_V2) != EXPECTED_SIGNALS:
        raise SystemExit("v2 corpus must contain exactly 80 signals")
    if len({row["signal_id"] for row in FROZEN_CORPUS_V2}) != EXPECTED_SIGNALS:
        raise SystemExit("v2 signal IDs must be unique")
    if len({row["text"] for row in FROZEN_CORPUS_V2}) != EXPECTED_SIGNALS:
        raise SystemExit("v2 signal texts must be unique")
    batches = {
        f"v2-batch-{index}": tuple(
            row for row in FROZEN_CORPUS_V2
            if row["batch_id"] == f"v2-batch-{index}"
        )
        for index in range(1, EXPECTED_BATCHES + 1)
    }
    if any(len(rows) != EXPECTED_BATCH_SIZE for rows in batches.values()):
        raise SystemExit("v2 must have eight genuine ten-signal batches")
    if any(sum(not row["gold"] for row in rows) != 5 for rows in batches.values()):
        raise SystemExit("each batch must contain exactly five hard-negative signals")
    source_counts = Counter(row["source_type"] for row in FROZEN_CORPUS_V2)
    if set(source_counts) != {"slack", "jira", "email"}:
        raise SystemExit("v2 sources must be Slack, Jira, and email")
    if max(source_counts.values()) - min(source_counts.values()) > 1:
        raise SystemExit("v2 source distribution is not balanced")
    slack_strata = {
        row["slack_context"] for row in FROZEN_CORPUS_V2
        if row["source_type"] == "slack"
    }
    if not {
        "thread_reply_delayed", "temporal_sequence", "cross_channel_temporal",
        "cross_thread_reference",
    } <= slack_strata:
        raise SystemExit("v2 is missing temporal/contextual Slack strata")
    all_gold = [mention for row in FROZEN_CORPUS_V2 for mention in row["gold"]]
    if any(mention["canonical_referent"] is not None for mention in all_gold):
        raise SystemExit("canonical referents must remain null in extraction v2")
    return {
        "hash": actual_hash,
        "batches": batches,
        "sources": dict(sorted(source_counts.items())),
        "negative_signals": sum(not row["gold"] for row in FROZEN_CORPUS_V2),
        "gold_mentions": len(all_gold),
        "entity_types": dict(sorted(Counter(
            mention["entity_type"] for mention in all_gold
        ).items())),
        "slack_context_strata": sorted(slack_strata),
    }


def _gold_objects() -> tuple[list[GoldSignal], list[GoldMention], dict[str, str]]:
    signals = [GoldSignal(
        signal_id=row["signal_id"],
        batch_id=row["batch_id"],
        source_type=row["source_type"],
        text=row["text"],
        slack_context=row["slack_context"],
    ) for row in FROZEN_CORPUS_V2]
    mentions = [GoldMention(
        mention_id=mention["mention_id"],
        signal_id=row["signal_id"],
        start=mention["start"],
        end=mention["end"],
        entity_type=mention["entity_type"],
        canonical_referent=None,
    ) for row in FROZEN_CORPUS_V2 for mention in row["gold"]]
    return signals, mentions, {row["signal_id"]: row["text"] for row in FROZEN_CORPUS_V2}


def _score(predictions: list[PredictedMention]) -> dict[str, Any]:
    signals, mentions, _ = _gold_objects()
    return evaluate_gold_entity_extraction(
        signals=signals,
        gold_mentions=mentions,
        predictions=predictions,
    ).model_dump(mode="json")


def _raw_predictions(
    responses: list[Any | None], text_by_id: dict[str, str]
) -> tuple[list[PredictedMention], list[dict[str, Any]]]:
    predictions: list[PredictedMention] = []
    exclusions: list[dict[str, Any]] = []
    index = 0
    for response in responses:
        for mention in getattr(response, "mentions", ()):
            index += 1
            signal_id = str(mention.signal_id)
            text = text_by_id.get(signal_id)
            reason = "model_abstained" if mention.abstain else None
            if text is None:
                reason = "unknown_signal_id"
            elif not (0 <= mention.span_start < mention.span_end <= len(text)):
                reason = "out_of_bounds"
            if reason:
                exclusions.append({
                    "raw_index": index,
                    "signal_id": signal_id,
                    "surface": mention.surface,
                    "start": mention.span_start,
                    "end": mention.span_end,
                    "reason": reason,
                })
                continue
            predictions.append(PredictedMention(
                prediction_id=f"v2-raw-{index:04d}",
                signal_id=signal_id,
                start=mention.span_start,
                end=mention.span_end,
                entity_type=mention.entity_type,
                canonical_referent=None,
                confidence=mention.confidence,
                abstained=False,
                candidate_fate="raw_model_candidate",
            ))
    return predictions, exclusions


async def main() -> None:
    # This occurs before environment/provider setup by design and is reportable.
    integrity = validate_frozen_corpus()

    os.environ["LLM_PROVIDER"] = "codex"
    os.environ["CODEX_TRANSPORT"] = "app-server"
    os.environ["CODEX_MODEL"] = "gpt-5.4"
    os.environ["LLM_MAX_RETRIES"] = "0"
    set_response_cache(None)
    provider = build_provider()
    if (provider.config.model, provider.config.max_retries) != ("gpt-5.4", 0):
        raise SystemExit("provider pinning failed")

    all_candidates = []
    raw_responses: list[Any | None] = []
    batch_runs: list[dict[str, Any]] = []
    try:
        for batch_id, rows in integrity["batches"].items():
            signals = tuple(PersistedSignalText(
                signal_id=UUID(row["signal_id"]),
                source_channel=row["source_type"],
                content_text=row["text"],
            ) for row in rows)
            capture = _CaptureProvider(provider)
            usage = LLMUsageAggregator()
            started = time.perf_counter()
            with using_usage_aggregator(usage):
                result = await discover_batch_mentions(provider=capture, signals=signals)
            latency = time.perf_counter() - started
            if capture.call_count != 1:
                raise RuntimeError(
                    f"{batch_id} made {capture.call_count} structured calls; expected one"
                )
            raw_responses.append(capture.response)
            all_candidates.extend(result.candidates)
            batch_runs.append({
                "batch_id": batch_id,
                "signal_count": len(signals),
                "discover_batch_invocations": 1,
                "structured_calls_observed": capture.call_count,
                "usage_calls_recorded": usage.call_count,
                "mode": result.mode,
                "provider_error": result.provider_error,
                "latency_seconds": latency,
                "input_tokens_estimated": usage.total_input_tokens,
                "output_tokens_estimated": usage.total_output_tokens,
                "cost_usd_estimated": usage.total_cost_usd,
                "post_verification_candidates": [{
                    "signal_id": str(candidate.signal_id),
                    "surface": candidate.surface,
                    "start": candidate.span_start,
                    "end": candidate.span_end,
                    "entity_type": candidate.entity_type,
                    "confidence": candidate.confidence,
                    "fate": candidate.fate.value,
                    "reason_codes": list(candidate.reason_codes),
                } for candidate in result.candidates],
            })
    finally:
        await close_codex_app_server_client()

    _, _, text_by_id = _gold_objects()
    pre_predictions, pre_exclusions = _raw_predictions(raw_responses, text_by_id)
    accepted = [
        candidate for candidate in all_candidates
        if candidate.fate is EntityMentionDetectionFate.DETECTED
    ]
    post_predictions = [PredictedMention(
        prediction_id=f"v2-verified-{index:04d}",
        signal_id=str(candidate.signal_id),
        start=candidate.span_start,
        end=candidate.span_end,
        entity_type=candidate.entity_type,
        canonical_referent=None,
        confidence=candidate.confidence,
        abstained=False,
        candidate_fate=candidate.fate.value,
    ) for index, candidate in enumerate(accepted, 1)]
    negative_ids = {
        row["signal_id"] for row in FROZEN_CORPUS_V2 if not row["gold"]
    }
    dirty_negative_ids = sorted({
        str(candidate.signal_id) for candidate in accepted
        if str(candidate.signal_id) in negative_ids
    })
    fate_counts = Counter(candidate.fate.value for candidate in all_candidates)
    totals = {
        "discover_batch_invocations": len(batch_runs),
        "structured_calls_observed": sum(
            run["structured_calls_observed"] for run in batch_runs
        ),
        "usage_calls_recorded": sum(run["usage_calls_recorded"] for run in batch_runs),
        "input_tokens_estimated": sum(
            run["input_tokens_estimated"] for run in batch_runs
        ),
        "output_tokens_estimated": sum(
            run["output_tokens_estimated"] for run in batch_runs
        ),
        "cost_usd_estimated": sum(run["cost_usd_estimated"] for run in batch_runs),
        "latency_seconds": sum(run["latency_seconds"] for run in batch_runs),
        "provider_errors": sum(bool(run["provider_error"]) for run in batch_runs),
    }
    report = {
        "benchmark": "learned-entity-discovery-quality-v2",
        "frozen_corpus_sha256": FROZEN_SHA256_V2,
        "freeze_verified_before_provider_construction": True,
        "model": provider.config.model,
        "transport": "app-server",
        "provider_retries": provider.config.max_retries,
        "corpus": {
            "signals": EXPECTED_SIGNALS,
            "batches": EXPECTED_BATCHES,
            "batch_size": EXPECTED_BATCH_SIZE,
            "negative_signals": integrity["negative_signals"],
            "gold_mentions": integrity["gold_mentions"],
            "sources": integrity["sources"],
            "entity_types": integrity["entity_types"],
            "slack_context_strata": integrity["slack_context_strata"],
        },
        "scope": {
            "canonical_referents": "all_null",
            "canonical_link_claim": False,
            "pre_verification": "raw non-abstained model coordinates that are scoreable",
            "post_verification": "production DETECTED fates only",
        },
        "pre_verification": {
            "metrics": _score(pre_predictions),
            "excluded_raw_candidates": pre_exclusions,
        },
        "post_verification": {
            "metrics": _score(post_predictions),
            "candidate_fate_distribution": dict(sorted(fate_counts.items())),
            "negative_cleanliness": {
                "negative_signal_count": len(negative_ids),
                "clean_negative_signals": len(negative_ids) - len(dirty_negative_ids),
                "rate": (len(negative_ids) - len(dirty_negative_ids)) / len(negative_ids),
                "dirty_signal_ids": dirty_negative_ids,
            },
        },
        "operational_totals": totals,
        "batch_runs": batch_runs,
    }
    REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"report_path": str(REPORT_PATH), "report": report},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
