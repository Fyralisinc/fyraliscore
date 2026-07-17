"""Static contracts for the one-shot current-runtime entity holdout v5."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

from tests.evaluation.learned_entity_current_runtime_holdout_v5 import (
    FROZEN_CORPUS_V5,
    FROZEN_SHA256_V5,
    computed_sha256_v5,
)


def test_v5_corpus_is_frozen_batched_and_balanced() -> None:
    assert computed_sha256_v5() == FROZEN_SHA256_V5
    assert len(FROZEN_CORPUS_V5) == 24
    batches = Counter(row["batch_id"] for row in FROZEN_CORPUS_V5)
    assert batches == {"v5-batch-1": 8, "v5-batch-2": 8, "v5-batch-3": 8}
    for batch_id in batches:
        rows = [row for row in FROZEN_CORPUS_V5 if row["batch_id"] == batch_id]
        assert sum(bool(row["gold"]) for row in rows) == 4
        assert sum(not row["gold"] for row in rows) == 4


def test_v5_has_required_weak_slices_sources_and_hard_negatives() -> None:
    types = Counter(
        mention["entity_type"] for row in FROZEN_CORPUS_V5 for mention in row["gold"]
    )
    assert types["person"] >= 4
    assert types["system"] >= 6
    assert types["project"] >= 5
    assert {row["source_type"] for row in FROZEN_CORPUS_V5} == {"slack", "jira", "email"}
    assert sum(row["source_type"] == "slack" for row in FROZEN_CORPUS_V5) >= 12
    assert sum(not row["gold"] for row in FROZEN_CORPUS_V5) == 12


def test_v5_runner_requires_seal_and_has_no_recovery_mode() -> None:
    source = Path("scripts/run_learned_entity_current_runtime_holdout_v5.py").read_text()
    assert "--seal" in source
    assert "--execute" in source
    assert "reruns_allowed\": 0" in source
    assert "recovery" not in source.casefold().replace("no recovery", "")
