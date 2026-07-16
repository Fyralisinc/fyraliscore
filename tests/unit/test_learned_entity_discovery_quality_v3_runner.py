from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from scripts import run_learned_entity_discovery_quality_v3 as runner


def test_pre_provider_contract_validates_frozen_design(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "RECEIPT_PATH", tmp_path / "absent.json")
    monkeypatch.setattr(runner, "CHECKPOINT_PATH", tmp_path / "absent-checkpoint.json")
    result = runner.validate_pre_provider()
    assert result["sha256"] == runner.FROZEN_SHA256_V3
    assert list(result["batches"]) == [f"v3-batch-{i}" for i in range(1, 5)]
    assert all(len(rows) == 10 for rows in result["batches"].values())


def test_completion_receipt_refuses_rerun_before_provider(tmp_path, monkeypatch):
    receipt = tmp_path / "completion_receipt.json"
    receipt.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(runner, "RECEIPT_PATH", receipt)
    with pytest.raises(SystemExit, match="rerun refused"):
        runner.validate_pre_provider()


def test_partial_checkpoint_refuses_fresh_execution_before_provider(
    tmp_path, monkeypatch
):
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_text('{"completed_batches": 1}\n', encoding="utf-8")
    monkeypatch.setattr(runner, "RECEIPT_PATH", tmp_path / "absent-receipt.json")
    monkeypatch.setattr(runner, "CHECKPOINT_PATH", checkpoint)

    with pytest.raises(SystemExit, match="checkpoint exists"):
        runner.validate_pre_provider()


def test_metadata_or_hash_drift_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "RECEIPT_PATH", tmp_path / "absent.json")
    monkeypatch.setattr(runner, "CHECKPOINT_PATH", tmp_path / "absent-checkpoint.json")
    monkeypatch.setattr(runner, "ONE_SHOT_EVIDENCE_METADATA", {"changed": True})
    with pytest.raises(SystemExit, match="metadata mismatch"):
        runner.validate_pre_provider()


def test_atomic_json_leaves_complete_artifact(tmp_path):
    target = tmp_path / "nested" / "checkpoint.json"
    runner._atomic_json(target, {"raw_structured_output": {"mentions": []}})
    assert json.loads(target.read_text()) == {
        "raw_structured_output": {"mentions": []}
    }
    assert not target.with_name(f".{target.name}.tmp").exists()


def test_runner_static_one_shot_and_checkpoint_contract():
    source = Path("scripts/run_learned_entity_discovery_quality_v3.py").read_text()
    tree = ast.parse(source)
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    discovery = [
        node for node in calls
        if isinstance(node.func, ast.Name) and node.func.id == "discover_batch_mentions"
    ]
    assert len(discovery) == 1
    assert source.index("validate_pre_provider()") < source.index("provider = build_provider()")
    assert '"CODEX_MODEL": "gpt-5.4"' in source
    assert '"LLM_MAX_RETRIES": "0"' in source
    assert "if capture.call_count != 1" in source
    assert source.index("_atomic_json(CHECKPOINT_PATH") < source.index(
        "if cardinality_error or outer_error"
    )
    assert source.index("_atomic_json(REPORT_PATH") < source.index(
        "_atomic_json(RECEIPT_PATH"
    )
    assert '"raw_structured_output": raw' in source
    assert 'report["by_entity_type"]' in source
