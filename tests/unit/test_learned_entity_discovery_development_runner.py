"""Contract tests for the development-only learned-discovery runner."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from scripts import run_learned_entity_discovery_development as runner


def test_contract_materializes_real_evaluator_inputs_before_provider() -> None:
    signals, mentions, batches = runner.validate_contract()

    assert len(signals) == 40
    assert len(mentions) == 64
    assert list(batches) == [f"development-batch-{index}" for index in range(1, 5)]
    assert all(len(rows) == 10 for rows in batches.values())
    assert len({signal.signal_id for signal in signals}) == 40
    assert all(mention.canonical_referent is None for mention in mentions)


def test_contract_rejects_a_gold_span_that_does_not_reproduce_surface(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    corrupt = [dict(row) for row in runner.DEVELOPMENT_CORPUS]
    corrupt[0] = dict(corrupt[0])
    corrupt[0]["gold"] = [dict(item) for item in corrupt[0]["gold"]]
    corrupt[0]["gold"][0]["start"] += 1
    monkeypatch.setattr(runner, "DEVELOPMENT_CORPUS", tuple(corrupt))

    with pytest.raises(SystemExit, match="gold span does not reproduce surface"):
        runner.validate_contract()


def test_checkpoint_is_atomic_and_carries_non_generalization_classification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkpoint = tmp_path / "checkpoint.json"
    monkeypatch.setattr(runner, "CHECKPOINT_PATH", checkpoint)
    payload = {
        **runner._evidence_classification(),
        "completed_batches": 1,
        "batch_runs": [{"raw_structured_output": {"mentions": []}}],
    }

    runner._write_checkpoint(payload)

    saved = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert saved == payload
    assert saved["development_only"] is True
    assert saved["generalization_claim_permitted"] is False
    assert "no generalization evidence" in saved["warning"]
    assert not checkpoint.with_suffix(".tmp").exists()


def test_runner_enforces_order_cardinality_checkpoints_and_report_classification() -> None:
    path = Path("scripts/run_learned_entity_discovery_development.py")
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    discovery_calls = [
        call for call in calls
        if isinstance(call.func, ast.Name)
        and call.func.id == "discover_batch_mentions"
    ]

    assert len(discovery_calls) == 1
    assert source.index("validate_contract()") < source.index("provider = build_provider()")
    assert '"CODEX_MODEL": "gpt-5.4"' in source
    assert '"LLM_MAX_RETRIES": "0"' in source
    assert "if capture.call_count != 1" in source
    assert source.index("_write_checkpoint({") < source.index(
        "if contract_error is not None:"
    )
    assert '"raw_structured_output": raw' in source
    assert '"error_taxonomy"' in source
    assert source.count("**_evidence_classification()") == 2
