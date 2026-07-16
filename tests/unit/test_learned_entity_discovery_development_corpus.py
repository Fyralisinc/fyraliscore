"""Structural and offline-wiring tests for the mutable discovery dev set."""

from __future__ import annotations

import json
from collections import Counter
from uuid import UUID

import pytest

from lib.contracts.entity_mentions import EntityMentionDetectionFate
from lib.evaluation.entity_extraction_gold import (
    GoldMention,
    GoldSignal,
    PredictedMention,
    evaluate_gold_entity_extraction,
)
from services.domain.entity_grounding.learned_discovery import (
    LearnedMentionBatch,
    PersistedSignalText,
    discover_batch_mentions,
)
from tests.evaluation.learned_entity_discovery_development_corpus import (
    DEVELOPMENT_CORPUS,
    DEVELOPMENT_ONLY,
    EVIDENCE_CLASS,
    canonical_development_bytes,
    offline_structured_response,
)
from tests.evaluation.learned_entity_discovery_quality_corpus_v1 import FROZEN_CORPUS
from tests.evaluation.learned_entity_discovery_quality_corpus_v2 import FROZEN_CORPUS_V2


class _GoldReplayProvider:
    """Fake provider proving batch/prompt/schema plumbing without model claims."""

    def __init__(self) -> None:
        self.call_count = 0
        self.batch_sizes: list[int] = []

    async def structured(self, *, system, user, schema, temperature, max_tokens):
        assert "Extract every explicit named company-entity mention" in system
        payload = json.loads(user)
        self.call_count += 1
        self.batch_sizes.append(len(payload["signals"]))
        signal_ids = {row["signal_id"] for row in payload["signals"]}
        batch_ids = {
            row["batch_id"] for row in DEVELOPMENT_CORPUS
            if row["signal_id"] in signal_ids
        }
        assert len(batch_ids) == 1
        return schema.model_validate(offline_structured_response(batch_ids.pop()))


def test_development_corpus_is_unique_batched_broad_and_non_evidentiary() -> None:
    assert DEVELOPMENT_ONLY is True
    assert EVIDENCE_CLASS == "development_feedback_only_not_generalization_evidence"
    assert len(DEVELOPMENT_CORPUS) == 40
    assert len({row["signal_id"] for row in DEVELOPMENT_CORPUS}) == 40
    assert len({row["text"] for row in DEVELOPMENT_CORPUS}) == 40
    assert len(canonical_development_bytes()) > 1_000
    batches = Counter(row["batch_id"] for row in DEVELOPMENT_CORPUS)
    assert batches == {f"development-batch-{index}": 10 for index in range(1, 5)}
    assert not ({row["text"] for row in DEVELOPMENT_CORPUS}
                & {row["text"] for row in FROZEN_CORPUS})
    assert not ({row["text"] for row in DEVELOPMENT_CORPUS}
                & {row["text"] for row in FROZEN_CORPUS_V2})
    assert sum(not row["gold"] for row in DEVELOPMENT_CORPUS) == 20
    assert sum(len(row["gold"]) >= 3 for row in DEVELOPMENT_CORPUS) >= 15


def test_development_gold_has_exact_minimal_typed_spans_and_target_strata() -> None:
    types = {mention["entity_type"]
             for row in DEVELOPMENT_CORPUS for mention in row["gold"]}
    assert types == {
        "person", "team", "customer", "project", "product", "system",
        "workstream", "goal", "commitment", "decision", "resource",
    }
    slack_strata = {row["slack_context"] for row in DEVELOPMENT_CORPUS
                    if row["source_type"] == "slack"}
    assert {
        "thread_reply_delayed", "cross_thread_reference", "temporal_sequence",
        "cross_channel_temporal", "thread_reply", "channel_followup", "standalone",
    } <= slack_strata
    assert any(any(ord(char) > 127 for char in row["text"])
               for row in DEVELOPMENT_CORPUS)
    assert any(any(marker in row["text"] for marker in ("::", "@", "λ", "π", "`"))
               for row in DEVELOPMENT_CORPUS)
    for row in DEVELOPMENT_CORPUS:
        for mention in row["gold"]:
            assert row["text"][mention["start"]:mention["end"]] == mention["surface"]
            assert mention["canonical_referent"] is None


@pytest.mark.asyncio
async def test_offline_gold_replay_exercises_four_real_batches_and_evaluator() -> None:
    provider = _GoldReplayProvider()
    predictions: list[PredictedMention] = []
    for batch_index in range(1, 5):
        rows = [row for row in DEVELOPMENT_CORPUS
                if row["batch_id"] == f"development-batch-{batch_index}"]
        result = await discover_batch_mentions(
            provider=provider,
            signals=tuple(PersistedSignalText(
                signal_id=UUID(row["signal_id"]),
                source_channel=row["source_type"],
                content_text=row["text"],
            ) for row in rows),
        )
        assert result.mode == "learned"
        predictions.extend(PredictedMention(
            prediction_id=f"dev-p-{len(predictions) + 1:03d}",
            signal_id=str(candidate.signal_id),
            start=candidate.span_start,
            end=candidate.span_end,
            entity_type=candidate.entity_type,
            confidence=candidate.confidence,
            candidate_fate=candidate.fate.value,
        ) for candidate in result.candidates
          if candidate.fate is EntityMentionDetectionFate.DETECTED)

    context_map = {
        "not_slack": "not_slack", "standalone": "standalone",
        "thread_reply": "threaded", "thread_reply_delayed": "threaded",
        "cross_thread_reference": "cross_thread",
        "temporal_sequence": "temporally_distributed",
        "cross_channel_temporal": "temporally_distributed",
        "channel_followup": "temporally_distributed",
    }
    signals = [GoldSignal(
        signal_id=row["signal_id"], batch_id=row["batch_id"],
        source_type=row["source_type"], text=row["text"],
        slack_context=context_map[row["slack_context"]],
    ) for row in DEVELOPMENT_CORPUS]
    gold = [GoldMention(
        mention_id=mention["mention_id"], signal_id=row["signal_id"],
        start=mention["start"], end=mention["end"],
        entity_type=mention["entity_type"], canonical_referent=None,
    ) for row in DEVELOPMENT_CORPUS for mention in row["gold"]]
    report = evaluate_gold_entity_extraction(
        signals=signals, gold_mentions=gold, predictions=predictions,
    )
    assert provider.call_count == 4
    assert provider.batch_sizes == [10, 10, 10, 10]
    assert report.overall.span_precision == 1.0
    assert report.overall.span_recall == 1.0
    assert report.overall.type_accuracy == 1.0
    assert "canonical_link_metrics_exclude_gold_without_referents" in report.uncertainties


def test_offline_fixture_conforms_to_production_structured_schema() -> None:
    for index in range(1, 5):
        response = offline_structured_response(f"development-batch-{index}")
        parsed = LearnedMentionBatch.model_validate(response)
        assert parsed.mentions
