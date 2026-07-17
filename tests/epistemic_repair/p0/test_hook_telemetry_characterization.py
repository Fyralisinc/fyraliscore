"""P0-B/P0-D characterization: inventory must track current production seams.

These tests intentionally do not assert desired P1 behavior. They fail if the
inventory drifts away from the source surfaces it claims to characterize.
"""
from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
INVENTORY_DIR = ROOT / "docs/plans/epistemic-repair/p0"


def _load(name: str) -> dict:
    return json.loads((INVENTORY_DIR / name).read_text(encoding="utf-8"))


def _source(ref: str) -> tuple[Path, int]:
    raw_path, raw_line = ref.rsplit(":", 1)
    path = ROOT / raw_path
    line = int(raw_line)
    assert path.is_file(), ref
    assert 1 <= line <= len(path.read_text(encoding="utf-8").splitlines()), ref
    return path, line


def test_benchmark_hook_inventory_characterizes_reachable_hooks() -> None:
    inventory = _load("benchmark-hook-inventory.json")
    hooks = {item["id"]: item for item in inventory["hooks"]}

    assert inventory["characterization_only"] is True
    assert inventory["production_changes"] is False
    assert inventory["summary"]["hg_01_current_result"] == "fail"
    assert set(hooks) == {"BH-001", "BH-002", "BH-003", "BH-004"}
    assert all(item["production_reachable"] is True for item in hooks.values())
    assert hooks["BH-002"]["benchmark_specific"] is True

    for item in hooks.values():
        source_text = "\n".join(
            _source(ref)[0].read_text(encoding="utf-8")
            for ref in item["source"].values()
        )
        for anchor in item["evidence_anchors"]:
            assert anchor in source_text, (item["id"], anchor)


def test_production_pipeline_still_calls_semantic_injectors_unconditionally() -> None:
    pipeline = (ROOT / "services/reasoning/think/run_pipeline.py").read_text(
        encoding="utf-8"
    )
    assert "raw_diff = maybe_inject_latent_bridge(raw_diff, trigger)" in pipeline
    assert (
        "raw_diff = maybe_inject_capability_probe_ops(raw_diff, trigger, state.bundle)"
        in pipeline
    )
    assert "if benchmark" not in pipeline


def test_telemetry_inventory_exposes_nonreconcilable_levels() -> None:
    inventory = _load("telemetry-inventory.json")
    surfaces = {item["level"]: item for item in inventory["surfaces"]}

    assert inventory["characterization_only"] is True
    assert inventory["summary"]["hg_13_current_result"] == "fail"
    assert surfaces["physical_provider_attempt"]["durable_store"] is None
    assert surfaces["logical_llm_request"]["coverage"] == "not_identified"
    assert surfaces["successful_usage_call"]["coverage"].endswith("only")
    assert all(
        item["current_result"] in {"unknown", "invalid"}
        for item in inventory["reconciliation_failures"]
    )

    for item in inventory["surfaces"]:
        for ref in item["source_refs"]:
            _source(ref)


def test_usage_counter_is_response_recording_not_attempt_recording() -> None:
    provider = (ROOT / "lib/llm/provider.py").read_text(encoding="utf-8")
    reason = (ROOT / "services/reasoning/think/reason.py").read_text(
        encoding="utf-8"
    )

    assert "agg.record(" in provider
    assert "outcome.llm_calls_count = agg.call_count" in reason
    assert "physical_attempt_id" not in provider
    assert "logical_call_id" not in provider


def test_inventory_reports_have_matching_machine_readable_sources() -> None:
    for stem in ("benchmark-hook-inventory", "telemetry-inventory"):
        report = (INVENTORY_DIR / f"{stem}.md").read_text(encoding="utf-8")
        assert f"[{stem}.json]({stem}.json)" in report
        assert _load(f"{stem}.json")["work_package"] in {"P0-B", "P0-D"}
