import ast
from pathlib import Path
import re

from lib.evaluation.epistemic_repair.reader_cutover import scan_reader_cutover


ROOT = Path(__file__).resolve().parents[3]
MANIFEST = (
    ROOT
    / "docs/plans/epistemic-repair/p2/reader-authority-manifest-v1.json"
)
RAW_MODEL_READ = re.compile(r"\b(?:FROM|JOIN)\s+models\b", re.IGNORECASE)


def _function_source(relative: str, name: str) -> str:
    path = ROOT / relative
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    node = next(
        item
        for item in ast.walk(tree)
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
        and item.name == name
    )
    return ast.get_source_segment(source, node) or ""


def test_manifest_reconciles_every_p0_reader_module() -> None:
    report = scan_reader_cutover(ROOT, MANIFEST)
    assert len(report.results) == 86
    assert all(item.reason for item in report.results)
    assert all(item.authority != "uncovered" for item in report.results)


def test_historical_and_audit_exemptions_are_explicit() -> None:
    report = scan_reader_cutover(ROOT, MANIFEST)
    exemptions = {item.module: item.authority for item in report.results if item.classification == "exempt"}
    assert exemptions["services/product/history/aggregator.py"] == "historical"
    assert exemptions["services/product/model_trace/repo.py"] == "audit"
    assert exemptions["services/domain/projections/store.py"] == "historical_projection"


def test_current_coverage_is_truthful_and_names_all_debt() -> None:
    report = scan_reader_cutover(ROOT, MANIFEST)
    assert report.coverage == 1.0
    assert report.remaining_debt == ()


def test_central_cutover_tokens_are_ratcheted() -> None:
    report = scan_reader_cutover(ROOT, MANIFEST)
    central = {
        item.module: item
        for item in report.consequential
        if item.authority != "uncovered"
    }
    assert central
    assert all(item.compliant for item in central.values())


def test_shared_accepted_read_shape_satisfies_additional_reader_authority() -> None:
    report = scan_reader_cutover(ROOT, MANIFEST)
    governed = {
        item.module: item for item in report.consequential
        if item.module in {
            "services/domain/actors/operating_context.py",
            "services/reasoning/dynamics/detectors.py",
            "services/reasoning/think/cascade.py",
            "services/reasoning/think/edge_semantics.py",
            "services/reasoning/think/reconciler.py",
        }
    }
    assert len(governed) == 5
    assert all(item.compliant for item in governed.values())


def test_p2_exit_requires_complete_reader_cutover() -> None:
    source = (
        ROOT / "services/evaluation/epistemic_repair/p2_runner.py"
    ).read_text(encoding="utf-8")
    assert 'report["reader_cutover_coverage"] = reader_report.coverage' in source
    assert 'report["reader_cutover_coverage"] == 1.0' in source


def test_cut_over_central_sql_seams_cannot_regress_to_raw_models() -> None:
    surfaces = (
        ("services/domain/models/repo.py", "retrieve"),
        ("services/domain/models/repo.py", "get_by_id"),
        ("services/reasoning/retrieval/assembler.py", "_supplement_exact_batch_anchor_models"),
        ("services/product/ask/orchestrator.py", "_fallback_models"),
    )
    for module, function in surfaces:
        source = _function_source(module, function)
        assert "ACCEPTED_MODEL_ROWS_SQL" in source, (module, function)
        assert not RAW_MODEL_READ.search(source), (module, function)


def test_newly_classified_consequential_readers_have_no_raw_model_sql() -> None:
    import json

    manifest = json.loads(MANIFEST.read_text())
    for item in manifest["additional_consequential"]:
        source = (ROOT / item["module"]).read_text(encoding="utf-8")
        tree = ast.parse(source)
        sql_literals = "\n".join(
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        )
        assert not RAW_MODEL_READ.search(sql_literals), item["module"]
