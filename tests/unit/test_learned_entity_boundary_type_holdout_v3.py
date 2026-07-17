from pathlib import Path

from tests.evaluation.learned_entity_discovery_boundary_type_holdout_v3 import (
    FROZEN_CORPUS_V3, FROZEN_SHA256_V3, computed_sha256_v3,
)


def test_v3_is_frozen_small_mixed_and_exact() -> None:
    assert computed_sha256_v3() == FROZEN_SHA256_V3
    assert len(FROZEN_CORPUS_V3) == 10
    assert {row["source_type"] for row in FROZEN_CORPUS_V3} == {"slack", "email", "jira"}
    for row in FROZEN_CORPUS_V3:
        for mention in row["gold"]:
            assert row["text"][mention["start"]:mention["end"]] == mention["surface"]


def test_v3_runner_fences_before_provider_and_saves_raw_checkpoint() -> None:
    source = Path("scripts/run_learned_entity_boundary_type_holdout_v3.py").read_text()
    assert source.index('"status": "running"') < source.index("provider = build_provider()")
    assert '"raw_structured_output"' in source
    assert "_atomic(CHECKPOINT" in source
    assert '"status": "failed"' in source
