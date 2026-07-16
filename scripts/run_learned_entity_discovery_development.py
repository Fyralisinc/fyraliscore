#!/usr/bin/env python3
"""Run mutable learned-discovery development feedback once.

This is deliberately *not* a holdout. The corpus is inspectable prompt-development
material, so its scores are diagnostic feedback and never generalization evidence.
Each completed batch is checkpointed before the next provider call.
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
from tests.evaluation.learned_entity_discovery_development_corpus import (
    DEVELOPMENT_CORPUS,
    DEVELOPMENT_ONLY,
    EVIDENCE_CLASS,
    canonical_development_bytes,
)

REPORT_PATH = Path("/tmp/learned_entity_discovery_development_report.json")
CHECKPOINT_PATH = Path("/tmp/learned_entity_discovery_development_checkpoint.json")
BATCH_SIZE = 10
EVIDENCE_WARNING = (
    "Mutable inspected development data; scores measure fit only and provide "
    "no generalization evidence."
)


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
                "exception_type": type(exc).__name__,
                "message": str(exc),
                "stage": "provider_structured_call",
            }
            raise


def _slack_context(value: str) -> str:
    return {
        "standalone": "standalone",
        "thread_reply": "threaded",
        "thread_reply_delayed": "threaded",
        "cross_thread_reference": "cross_thread",
        "temporal_sequence": "temporally_distributed",
        "channel_followup": "temporally_distributed",
        "cross_channel_temporal": "temporally_distributed",
        "not_slack": "not_slack",
    }[value]


def validate_contract() -> tuple[list[GoldSignal], list[GoldMention], dict[str, tuple[dict, ...]]]:
    """Validate fixture invariants and the real evaluator contract pre-provider."""
    if DEVELOPMENT_ONLY is not True or "not_generalization" not in EVIDENCE_CLASS:
        raise SystemExit("development corpus lacks explicit non-generalization marker")
    if not DEVELOPMENT_CORPUS or len(DEVELOPMENT_CORPUS) % BATCH_SIZE:
        raise SystemExit("development corpus must contain genuine ten-signal batches")
    if len({row["signal_id"] for row in DEVELOPMENT_CORPUS}) != len(DEVELOPMENT_CORPUS):
        raise SystemExit("development signal IDs must be unique")
    batch_ids = sorted({row["batch_id"] for row in DEVELOPMENT_CORPUS})
    batches = {
        batch_id: tuple(row for row in DEVELOPMENT_CORPUS if row["batch_id"] == batch_id)
        for batch_id in batch_ids
    }
    if any(len(rows) != BATCH_SIZE for rows in batches.values()):
        raise SystemExit("every development batch must contain exactly ten signals")
    for row in DEVELOPMENT_CORPUS:
        for item in row["gold"]:
            if row["text"][item["start"]:item["end"]] != item["surface"]:
                raise SystemExit(
                    f"gold span does not reproduce surface {item['mention_id']}"
                )
            if item["canonical_referent"] is not None:
                raise SystemExit("development extraction gold must remain unlinked")
    signals = [GoldSignal(
        signal_id=row["signal_id"], batch_id=row["batch_id"],
        source_type=row["source_type"], text=row["text"],
        slack_context=_slack_context(row["slack_context"]),
    ) for row in DEVELOPMENT_CORPUS]
    mentions = [GoldMention(
        mention_id=item["mention_id"], signal_id=row["signal_id"],
        start=item["start"], end=item["end"], entity_type=item["entity_type"],
        canonical_referent=None,
    ) for row in DEVELOPMENT_CORPUS for item in row["gold"]]
    # Executes all evaluator-side validation, not merely Pydantic construction.
    evaluate_gold_entity_extraction(signals=signals, gold_mentions=mentions, predictions=[])
    return signals, mentions, batches


def _score(signals, mentions, predictions) -> dict[str, Any]:
    return evaluate_gold_entity_extraction(
        signals=signals, gold_mentions=mentions, predictions=predictions,
    ).model_dump(mode="json")


def _evidence_classification() -> dict[str, Any]:
    return {
        "evidence_class": EVIDENCE_CLASS,
        "development_only": True,
        "generalization_claim_permitted": False,
        "warning": EVIDENCE_WARNING,
    }


def _write_checkpoint(payload: dict[str, Any]) -> None:
    temporary = CHECKPOINT_PATH.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(CHECKPOINT_PATH)


async def main() -> None:
    signals_gold, mentions_gold, batches = validate_contract()
    corpus_sha = hashlib.sha256(canonical_development_bytes()).hexdigest()

    os.environ.update({
        "LLM_PROVIDER": "codex", "CODEX_TRANSPORT": "app-server",
        "CODEX_MODEL": "gpt-5.4", "LLM_MAX_RETRIES": "0",
    })
    set_response_cache(None)
    provider = build_provider()
    if (provider.config.model, provider.config.max_retries) != ("gpt-5.4", 0):
        raise SystemExit("provider pinning failed")

    text_by_id = {row["signal_id"]: row["text"] for row in DEVELOPMENT_CORPUS}
    raw_predictions: list[PredictedMention] = []
    post_predictions: list[PredictedMention] = []
    batch_runs: list[dict[str, Any]] = []
    all_fates: Counter[str] = Counter()
    try:
        for batch_id, rows in batches.items():
            inputs = tuple(PersistedSignalText(
                signal_id=UUID(row["signal_id"]), source_channel=row["source_type"],
                content_text=row["text"],
            ) for row in rows)
            capture, usage = _CaptureProvider(provider), LLMUsageAggregator()
            started = time.perf_counter()
            with using_usage_aggregator(usage):
                result = await discover_batch_mentions(provider=capture, signals=inputs)
            latency = time.perf_counter() - started
            raw = capture.response.model_dump(mode="json") if capture.response else None
            raw_exclusions: list[dict[str, Any]] = []
            if capture.response:
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
                        raw_predictions.append(PredictedMention(
                            prediction_id=f"dev-raw-{len(raw_predictions)+1:04d}",
                            signal_id=signal_id, start=item.span_start, end=item.span_end,
                            entity_type=item.entity_type, confidence=item.confidence,
                            canonical_referent=None, candidate_fate="raw_model_candidate",
                        ))
            for candidate in result.candidates:
                all_fates[candidate.fate.value] += 1
                if candidate.fate is EntityMentionDetectionFate.DETECTED:
                    post_predictions.append(PredictedMention(
                        prediction_id=f"dev-verified-{len(post_predictions)+1:04d}",
                        signal_id=str(candidate.signal_id), start=candidate.span_start,
                        end=candidate.span_end, entity_type=candidate.entity_type,
                        confidence=candidate.confidence, canonical_referent=None,
                        candidate_fate=candidate.fate.value,
                    ))
            run = {
                "batch_id": batch_id, "signal_count": len(inputs),
                "structured_calls_observed": capture.call_count,
                "raw_structured_output": raw,
                "exact_error": capture.error,
                "production_provider_error": result.provider_error,
                "mode": result.mode, "latency_seconds": latency,
                "usage": {
                    "calls": usage.call_count,
                    "input_tokens_estimated": usage.total_input_tokens,
                    "output_tokens_estimated": usage.total_output_tokens,
                    "cost_usd_estimated": usage.total_cost_usd,
                },
                "raw_exclusions": raw_exclusions,
                "post_verification_candidates": [{
                    "signal_id": str(c.signal_id), "surface": c.surface,
                    "start": c.span_start, "end": c.span_end,
                    "entity_type": c.entity_type, "confidence": c.confidence,
                    "fate": c.fate.value, "reason_codes": list(c.reason_codes),
                } for c in result.candidates],
            }
            contract_error = None
            if capture.call_count != 1:
                contract_error = {
                    "exception_type": "RunnerContractError",
                    "message": (
                        f"{batch_id} made {capture.call_count} structured calls; "
                        "expected exactly one"
                    ),
                    "stage": "runner_call_cardinality",
                }
                run["exact_error"] = run["exact_error"] or contract_error
            batch_runs.append(run)
            completed_batch_ids = {item["batch_id"] for item in batch_runs}
            completed_signal_ids = {
                signal.signal_id for signal in signals_gold
                if signal.batch_id in completed_batch_ids
            }
            completed_signals = [
                signal for signal in signals_gold
                if signal.signal_id in completed_signal_ids
            ]
            completed_mentions = [
                mention for mention in mentions_gold
                if mention.signal_id in completed_signal_ids
            ]
            _write_checkpoint({
                **_evidence_classification(),
                "corpus_sha256": corpus_sha,
                "completed_batches": len(batch_runs),
                "pre_verification_metrics_so_far": _score(
                    completed_signals, completed_mentions, raw_predictions
                ),
                "post_verification_metrics_so_far": _score(
                    completed_signals, completed_mentions, post_predictions
                ),
                "batch_runs": batch_runs,
            })
            if contract_error is not None:
                raise RuntimeError(contract_error["message"])
    finally:
        await close_codex_app_server_client()

    negative_ids = {row["signal_id"] for row in DEVELOPMENT_CORPUS if not row["gold"]}
    dirty = {p.signal_id for p in post_predictions if p.signal_id in negative_ids}
    error_taxonomy = Counter(
        run["exact_error"]["exception_type"]
        for run in batch_runs if run["exact_error"] is not None
    )
    report = {
        "benchmark": "learned-entity-discovery-development-feedback",
        **_evidence_classification(),
        "contract_validated_before_provider_construction": True,
        "corpus_sha256": corpus_sha,
        "model": provider.config.model, "transport": "app-server",
        "provider_retries": provider.config.max_retries,
        "corpus": {
            "signals": len(DEVELOPMENT_CORPUS), "batches": len(batches),
            "batch_size": BATCH_SIZE, "gold_mentions": len(mentions_gold),
            "negative_signals": len(negative_ids),
        },
        "pre_verification": {"metrics": _score(signals_gold, mentions_gold, raw_predictions)},
        "post_verification": {
            "metrics": _score(signals_gold, mentions_gold, post_predictions),
            "candidate_fate_distribution": dict(sorted(all_fates.items())),
            "negative_cleanliness": {
                "negative_signal_count": len(negative_ids),
                "clean_negative_signals": len(negative_ids) - len(dirty),
                "rate": (len(negative_ids) - len(dirty)) / len(negative_ids),
                "dirty_signal_ids": sorted(dirty),
            },
        },
        "operational_totals": {
            "structured_calls_observed": sum(r["structured_calls_observed"] for r in batch_runs),
            "provider_errors": sum(bool(r["exact_error"]) for r in batch_runs),
            "input_tokens_estimated": sum(r["usage"]["input_tokens_estimated"] for r in batch_runs),
            "output_tokens_estimated": sum(r["usage"]["output_tokens_estimated"] for r in batch_runs),
            "cost_usd_estimated": sum(r["usage"]["cost_usd_estimated"] for r in batch_runs),
            "latency_seconds": sum(r["latency_seconds"] for r in batch_runs),
            "error_taxonomy": dict(sorted(error_taxonomy.items())),
        },
        "batch_runs": batch_runs,
    }
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"report_path": str(REPORT_PATH), "report": report}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
