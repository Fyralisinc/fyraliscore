"""Smoke-tests for every ``lsob`` subcommand via Typer's ``CliRunner``."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from typer.testing import CliRunner

from lsob_harness.cli import app


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
FIXTURES = WORKSPACE_ROOT / "fixtures"


def test_cli_doctor_runs() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["doctor", "--workspace", str(WORKSPACE_ROOT)])
    assert result.exit_code == 0
    assert "lsob doctor" in result.stdout


def test_cli_doctor_json() -> None:
    runner = CliRunner()
    result = runner.invoke(
        app, ["doctor", "--workspace", str(WORKSPACE_ROOT), "--json"]
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert "ok" in payload and "checks" in payload
    assert any(c["name"] == "fixtures" for c in payload["checks"])


def test_cli_run_and_list_and_compare(tmp_path: Path) -> None:
    runner = CliRunner()
    runs_root = tmp_path / "runs"

    # First run
    r1 = runner.invoke(
        app,
        [
            "run",
            "--corpus", str(FIXTURES / "mini_corpus_a.json"),
            "--sut", "mock",
            "--layers", "1,2",
            "--ablation", "none",
            "--runs-root", str(runs_root),
        ],
    )
    assert r1.exit_code == 0, r1.stdout
    assert "run complete" in r1.stdout

    # Second run
    r2 = runner.invoke(
        app,
        [
            "run",
            "--corpus", str(FIXTURES / "mini_corpus_a.json"),
            "--sut", "mock",
            "--layers", "1,2",
            "--ablation", "no_bridge",
            "--runs-root", str(runs_root),
        ],
    )
    assert r2.exit_code == 0, r2.stdout

    # list-runs picks them up
    lst = runner.invoke(app, ["list-runs", "--runs-root", str(runs_root)])
    assert lst.exit_code == 0
    assert "mock" in lst.stdout

    # Compare the two
    run_ids = sorted(p.name for p in runs_root.iterdir() if p.is_dir())
    assert len(run_ids) == 2
    cmp = runner.invoke(
        app,
        ["compare", run_ids[0], run_ids[1], "--runs-root", str(runs_root)],
    )
    assert cmp.exit_code == 0, cmp.stdout
    assert "Run comparison" in cmp.stdout


def test_cli_bulk_run(tmp_path: Path) -> None:
    runner = CliRunner()
    runs_root = tmp_path / "runs"

    matrix_yaml = {
        "suts": ["mock"],
        "corpora": [
            str(FIXTURES / "mini_corpus_a.json"),
            str(FIXTURES / "mini_corpus_b.json"),
        ],
        "ablations": [{"name": "none"}],
        "layers": [1, 2],
        "seeds": [42],
        "runs_root": str(runs_root),
        "concurrency": 2,
    }
    mpath = tmp_path / "matrix.yaml"
    mpath.write_text(yaml.safe_dump(matrix_yaml))

    result = runner.invoke(app, ["bulk-run", "--matrix", str(mpath)])
    assert result.exit_code == 0, result.stdout
    assert "bulk-run complete" in result.stdout
    assert "2 runs" in result.stdout
