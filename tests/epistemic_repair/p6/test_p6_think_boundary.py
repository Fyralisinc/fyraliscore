import ast
from pathlib import Path


def test_production_think_runner_cannot_import_or_read_sealed_gold() -> None:
    source = Path(
        "lib/evaluation/epistemic_repair/p6_think_runner.py"
    ).read_text()
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    attributes = {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }
    assert "P6Gold" not in imported
    assert not {"gold", "synthesis_signal_by_storyline", "thesis_by_storyline"} & attributes


def test_production_think_runner_requires_real_batch_worker() -> None:
    source = Path(
        "lib/evaluation/epistemic_repair/p6_think_runner.py"
    ).read_text()
    assert "_process_one_t1_batch" in source
    assert "ThinkWorker" in source
    assert "t1_batch_max_size=25" in source
