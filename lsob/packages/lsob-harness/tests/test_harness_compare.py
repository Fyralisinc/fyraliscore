"""Compare two runs -> markdown table."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from lsob_contracts import AblationConfig, EvalResult, RunManifest

from lsob_harness import db as dbmod
from lsob_harness.compare import compare_runs


def _seed_run(db_path: Path, run_id: str, metric_values: dict[tuple[int, str], float]) -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    manifest = RunManifest(
        run_id=run_id,
        company="MiniA",
        months_simulated=1,
        baseline="mock",
        ablation=AblationConfig(name="none"),
        seed=42,
        git_sha="nogit",
        started_at=now,
        finished_at=now,
        corpus_uri="fixtures/mini_corpus_a.json",
        layers=[1, 2, 3],
    )
    results = [
        EvalResult(layer_id=layer, metric_name=metric, value=v, run_id=run_id)
        for (layer, metric), v in metric_values.items()
    ]
    with dbmod.open_db(db_path) as conn:
        dbmod.write_manifest(conn, manifest)
        dbmod.write_eval_results(conn, run_id, results)


def test_compare_runs_renders_deltas(tmp_path: Path) -> None:
    db_a = tmp_path / "a" / "results.db"
    db_b = tmp_path / "b" / "results.db"
    _seed_run(
        db_a,
        "run-a",
        {(1, "L1.noop"): 0.50, (2, "L2.noop"): 1.00, (3, "L3.noop"): 2.00},
    )
    _seed_run(
        db_b,
        "run-b",
        {(1, "L1.noop"): 0.80, (2, "L2.noop"): 1.00, (3, "L3.noop"): 1.50},
    )

    md = compare_runs(db_a, db_b)
    assert "run-a" in md
    assert "run-b" in md
    # Header / columns
    assert "| Layer | Metric | A | B | Δ (B−A) |" in md
    # Deltas for each metric
    assert "+0.3000" in md  # L1
    assert "+0.0000" in md  # L2
    assert "-0.5000" in md  # L3
    # All three metrics appear as rows
    for name in ("L1.noop", "L2.noop", "L3.noop"):
        assert name in md


def test_compare_handles_missing_metric(tmp_path: Path) -> None:
    db_a = tmp_path / "a" / "results.db"
    db_b = tmp_path / "b" / "results.db"
    _seed_run(db_a, "run-a", {(1, "only-in-a"): 0.4})
    _seed_run(db_b, "run-b", {(1, "only-in-b"): 0.9})
    md = compare_runs(db_a, db_b)
    # Rows should mark missing sides with an em-dash
    assert "only-in-a" in md
    assert "only-in-b" in md
    assert "—" in md
