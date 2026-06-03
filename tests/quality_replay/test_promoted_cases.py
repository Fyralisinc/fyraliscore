from __future__ import annotations

from pathlib import Path

from services.reasoning.think.quality_promoter import (
    DEFAULT_CASE_DIR,
    evaluate_promoted_case,
    load_promoted_cases,
)


def test_promoted_quality_cases_are_well_formed() -> None:
    docs = load_promoted_cases(DEFAULT_CASE_DIR)
    for doc in docs:
        result = evaluate_promoted_case(doc)
        assert result["status"] == "pass", {
            "path": doc.get("_path"),
            "result": result,
        }


def test_quality_replay_case_directory_is_documented() -> None:
    assert Path("tests/quality_replay/README.md").exists()
