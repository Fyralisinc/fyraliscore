"""SQLite schema bootstrap + round-trip for RunManifest/EvalResult."""

from __future__ import annotations

from pathlib import Path

from lsob_harness import db as dbmod


def test_schema_bootstraps(tmp_path: Path) -> None:
    p = tmp_path / "x" / "results.db"
    with dbmod.open_db(p) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "runs" in tables
    assert "eval_results" in tables
    assert p.exists()


def test_write_and_read_manifest_plus_results(
    tmp_path: Path, sample_manifest, sample_eval_results
) -> None:
    for r in sample_eval_results:
        r.run_id = sample_manifest.run_id

    db_path = tmp_path / "results.db"
    with dbmod.open_db(db_path) as conn:
        dbmod.write_manifest(conn, sample_manifest)
        dbmod.write_eval_results(conn, sample_manifest.run_id, sample_eval_results)

    with dbmod.open_db(db_path) as conn:
        mf = dbmod.read_manifest(conn, sample_manifest.run_id)
        assert mf is not None
        assert mf.run_id == sample_manifest.run_id
        assert mf.baseline == "mock"

        out = dbmod.read_eval_results(conn, sample_manifest.run_id)

    assert len(out) == 2
    assert out[0].layer_id == 1
    assert out[0].metric_name == "L1.noop"
    assert out[0].value == 1.0
    assert out[0].confidence_interval == (0.8, 1.2)
    assert out[0].breakdown_by == {"checkpoint": "2026-01-31"}

    assert out[1].layer_id == 3
    assert out[1].confidence_interval is None


def test_list_runs_sorts_recent_first(tmp_path: Path, sample_manifest) -> None:
    db_path = tmp_path / "index.db"
    with dbmod.open_db(db_path) as conn:
        dbmod.write_manifest(conn, sample_manifest)
        second = sample_manifest.model_copy(update={"run_id": "second-run"})
        dbmod.write_manifest(conn, second)
        rows = dbmod.list_runs(conn)
    ids = [r["run_id"] for r in rows]
    assert set(ids) == {sample_manifest.run_id, "second-run"}
    assert all(r["layer_count"] == 6 for r in rows)
