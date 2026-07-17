from collections import Counter
from pathlib import Path

from tests.evaluation.learned_entity_discovery_boundary_type_holdout_v2 import (
    FROZEN_CORPUS_V2, FROZEN_SHA256_V2, ONE_SHOT_METADATA, computed_sha256_v2,
)


def test_holdout_v2_is_sealed_batched_and_source_mixed() -> None:
    assert computed_sha256_v2() == FROZEN_SHA256_V2
    assert ONE_SHOT_METADATA["provider_execution_count_at_seal"] == 0
    assert ONE_SHOT_METADATA["allowed_provider_executions"] == 1
    assert Counter(row["batch_id"] for row in FROZEN_CORPUS_V2) == {
        f"boundary-type-holdout-v2-batch-{index}": 10 for index in range(1, 4)}
    assert {row["source_type"] for row in FROZEN_CORPUS_V2} == {"slack", "email", "jira"}


def test_holdout_v2_boundaries_are_exact_and_negatives_are_adversarial() -> None:
    for row in FROZEN_CORPUS_V2:
        for mention in row["gold"]:
            assert row["text"][mention["start"]:mention["end"]] == mention["surface"]
    negatives = "\n".join(row["text"] for row in FROZEN_CORPUS_V2 if not row["gold"])
    assert "no ticket, goal, or decision role" in negatives
    assert "generic heading" in negatives
    assert "schema field" in negatives
    assert "GET /v2/workstreams" in negatives


def test_runner_fences_attempt_before_provider_and_checkpoints_raw_batches() -> None:
    source = Path(
        "scripts/run_learned_entity_boundary_type_holdout_v2.py"
    ).read_text(encoding="utf-8")
    assert source.index('"status": "running"') < source.index("provider = build_provider()")
    assert '"run_attempts": 1' in source
    assert '"status": "failed"' in source
    assert '"status": "completed"' in source
    assert '"raw_structured_output"' in source
    assert "_atomic(CHECKPOINT" in source
