"""Structural proof for the untouched learned-discovery v2 benchmark."""

from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

from scripts.run_learned_entity_discovery_quality_v2 import validate_frozen_corpus
from tests.evaluation.learned_entity_discovery_quality_corpus_v2 import (
    FROZEN_CORPUS_V2,
    FROZEN_SHA256_V2,
    computed_sha256_v2,
)
from tests.evaluation.learned_entity_discovery_quality_corpus_v1 import FROZEN_CORPUS


def test_v2_corpus_is_frozen_unique_balanced_and_batch_shaped() -> None:
    integrity = validate_frozen_corpus()
    assert computed_sha256_v2() == FROZEN_SHA256_V2
    assert integrity["hash"] == FROZEN_SHA256_V2
    assert len(FROZEN_CORPUS_V2) == 80
    assert len({row["signal_id"] for row in FROZEN_CORPUS_V2}) == 80
    assert len({row["text"] for row in FROZEN_CORPUS_V2}) == 80
    assert Counter(row["source_type"] for row in FROZEN_CORPUS_V2) == {
        "slack": 27, "jira": 27, "email": 26,
    }
    assert integrity["negative_signals"] == 40
    assert integrity["gold_mentions"] >= 80
    assert not ({row["text"] for row in FROZEN_CORPUS_V2}
                & {row["text"] for row in FROZEN_CORPUS})
    assert len(integrity["batches"]) == 8
    assert all(len(rows) == 10 for rows in integrity["batches"].values())
    assert all(sum(not row["gold"] for row in rows) == 5
               for rows in integrity["batches"].values())


def test_v2_gold_is_exact_typed_unlinked_and_adversarial() -> None:
    types = {
        mention["entity_type"] for row in FROZEN_CORPUS_V2 for mention in row["gold"]
    }
    assert {
        "person", "team", "customer", "project", "product", "system",
        "workstream", "goal", "commitment", "decision", "resource",
    } <= types
    assert sum(len(row["gold"]) >= 3 for row in FROZEN_CORPUS_V2) >= 20
    assert any(any(ord(character) > 127 for character in row["text"])
               for row in FROZEN_CORPUS_V2)
    assert any(any(marker in row["text"] for marker in ("::", "#", "@", "`", "λ", "β"))
               for row in FROZEN_CORPUS_V2)
    for row in FROZEN_CORPUS_V2:
        for mention in row["gold"]:
            assert row["text"][mention["start"]:mention["end"]] == mention["surface"]
            assert mention["canonical_referent"] is None
    assert {
        "thread_reply_delayed", "temporal_sequence", "cross_channel_temporal",
        "cross_thread_reference",
    } <= {
        row["slack_context"] for row in FROZEN_CORPUS_V2
        if row["source_type"] == "slack"
    }


def test_v2_runner_pins_one_production_call_per_batch_without_execution() -> None:
    runner = Path("scripts/run_learned_entity_discovery_quality_v2.py")
    source = runner.read_text(encoding="utf-8")
    tree = ast.parse(source)
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    discover_calls = [call for call in calls
                      if isinstance(call.func, ast.Name)
                      and call.func.id == "discover_batch_mentions"]
    assert len(discover_calls) == 1
    assert 'os.environ["CODEX_MODEL"] = "gpt-5.4"' in source
    assert 'os.environ["LLM_MAX_RETRIES"] = "0"' in source
    assert "if capture.call_count != 1" in source
    assert "integrity = validate_frozen_corpus()" in source
    assert source.index("integrity = validate_frozen_corpus()") < source.index(
        "provider = build_provider()"
    )
    assert "pre_verification" in source and "post_verification" in source
