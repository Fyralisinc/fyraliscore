"""Optional bounded provider evidence for P6 entity extraction.

This lane uses the production learned batch extractor exactly once per transport
batch.  It does not upgrade unrelated P6 metrics or the overall phase verdict.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import time
from uuid import NAMESPACE_URL, uuid5

from lib.contracts.entity_mentions import EntityMentionDetectionFate
from lib.evaluation.entity_extraction_gold import (
    GoldMention, GoldSignal, PredictedMention, evaluate_gold_entity_extraction,
)
from lib.evaluation.epistemic_repair.p6_population import P6Population
from lib.llm.provider import build_provider, close_codex_app_server_client
from services.domain.entity_grounding.learned_discovery import (
    PersistedSignalText, discover_batch_mentions,
)


async def run_p6_provider_entity_evaluation(
    population: P6Population, *, checkpoint_path: Path | None = None,
    per_batch_timeout_s: float = 120.0, total_timeout_s: float = 600.0,
) -> dict:
    provider = build_provider()
    signal_by_id = {s.signal_id: s for s in population.signals}
    uuid_by_id = {signal_id: uuid5(NAMESPACE_URL, f"p6-provider:{signal_id}")
                  for signal_id in signal_by_id}
    id_by_uuid = {value: key for key, value in uuid_by_id.items()}
    predictions: list[PredictedMention] = []
    batch_receipts: list[dict] = []
    started = time.monotonic()
    terminal_reason: str | None = None
    try:
        for batch in population.batches:
            remaining = total_timeout_s - (time.monotonic() - started)
            if remaining <= 0:
                terminal_reason = "total_timeout"
                break
            try:
                async with asyncio.timeout(min(per_batch_timeout_s, remaining)):
                    result = await discover_batch_mentions(
                        provider=provider,
                        signals=tuple(PersistedSignalText(
                            uuid_by_id[item.signal_id], item.source_channel, item.text,
                        ) for item in batch.signals),
                    )
            except TimeoutError:
                terminal_reason = f"batch_{batch.batch_number}_timeout"
                batch_receipts.append({
                    "batch_number": batch.batch_number, "mode": "timeout",
                    "provider_error": terminal_reason, "candidate_count": 0,
                })
                break
            batch_receipts.append({
                "batch_number": batch.batch_number,
                "mode": result.mode,
                "provider_error": result.provider_error,
                "candidate_count": len(result.candidates),
            })
            for candidate in result.candidates:
                if candidate.fate is not EntityMentionDetectionFate.DETECTED:
                    continue
                signal_id = id_by_uuid[candidate.signal_id]
                predictions.append(PredictedMention(
                    prediction_id=f"p6-provider-{len(predictions)+1}",
                    signal_id=signal_id, start=candidate.span_start,
                    end=candidate.span_end, entity_type=candidate.entity_type,
                    canonical_referent=None, confidence=candidate.confidence,
                    candidate_fate=candidate.fate.value,
                ))
            if checkpoint_path is not None:
                checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
                checkpoint_path.write_text(json.dumps({
                    "schema_version": "epistemic-repair-p6-provider-checkpoint-v1",
                    "population_digest": population.population_digest,
                    "completed_batches": len(batch_receipts),
                    "batch_receipts": batch_receipts,
                    "predictions": [item.model_dump(mode="json") for item in predictions],
                    "elapsed_seconds": time.monotonic() - started,
                }, indent=2, sort_keys=True) + "\n")
            print(f"p6_provider_batch={batch.batch_number}/12 mode={result.mode} candidates={len(result.candidates)}", flush=True)
    finally:
        await close_codex_app_server_client()
    signals = [GoldSignal(
        signal_id=s.signal_id, batch_id=f"p6-batch-{s.batch_number}",
        source_type=s.source_channel.split(":", 1)[0], text=s.text,
        slack_context="temporally_distributed" if s.source_channel.startswith("slack:") else "not_slack",
    ) for s in population.signals]
    gold: list[GoldMention] = []
    for item in population.gold:
        signal = signal_by_id[item.signal_id]
        expected = []
        if item.entity_surface is not None:
            expected.append((item.entity_surface, item.entity_type or "other"))
        expected.extend(
            (mention.surface, mention.entity_types[0])
            for mention in item.local_mentions if mention.required
        )
        for index, (surface, entity_type) in enumerate(expected, start=1):
            start = signal.text.find(surface)
            if start < 0:
                raise ValueError(
                    f"required mention {surface!r} absent from {item.signal_id}"
                )
            gold.append(GoldMention(
                mention_id=f"p6-gold-{item.signal_id}-{index}",
                signal_id=item.signal_id,
                start=start,
                end=start + len(surface),
                entity_type=entity_type,
                canonical_referent=None,
            ))
    report = evaluate_gold_entity_extraction(
        signals=signals, gold_mentions=gold, predictions=predictions,
    ).model_dump(mode="json")
    return {
        "schema_version": "epistemic-repair-p6-provider-entity-v1",
        "population_digest": population.population_digest,
        "provider_call_budget": 12,
        "provider_call_count": len(batch_receipts),
        "complete": len(batch_receipts) == 12 and terminal_reason is None,
        "terminal_reason": terminal_reason,
        "batch_receipts": batch_receipts,
        "entity_extraction": report,
        "proof_boundary": (
            "Measures production learned mention extraction on all 12 intact batches.",
            "Does not measure canonical linking, thesis synthesis, relations, or lifecycle behavior.",
            "A partial or timed-out run is evidence only for completed batches and never upgrades the P6 verdict.",
        ),
    }


def write_p6_provider_report(report: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


__all__ = ["run_p6_provider_entity_evaluation", "write_p6_provider_report"]
