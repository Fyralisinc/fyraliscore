"""CLI smoke tests using typer's CliRunner."""

from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from lsob_simulation.cli import app

runner = CliRunner()

CONFIGS_DIR = Path(__file__).resolve().parents[1] / "configs"
FIXTURE = Path(__file__).resolve().parents[3] / "fixtures" / "mini_corpus_a.json"


def test_cli_validate_corpus_on_fixture():
    result = runner.invoke(app, ["validate-corpus", str(FIXTURE)])
    # Fixture should be a valid corpus.
    assert result.exit_code == 0, result.output
    assert "OK" in result.output


def test_cli_run_produces_valid_corpus(tmp_path: Path):
    import yaml

    # Build a tiny in-line YAML so tests stay fast.
    cfg = tmp_path / "tiny.yaml"
    cfg.write_text(
        yaml.safe_dump(
            {
                "company_id": "CLITest",
                "num_actors": 2,
                "commitment_generation_rate": 0.1,
                "customer_count": 1,
                "seed": 11,
                "start_date": "2026-01-01T00:00:00Z",
                "duration_months": 1,
                "actor_personality_distribution": {
                    "reliable": 0.5,
                    "optimistic": 0.25,
                    "pessimistic": 0.15,
                    "flaky": 0.1,
                },
            }
        )
    )
    out = tmp_path / "corpus.jsonl.zst"
    result = runner.invoke(app, ["run", "--config", str(cfg), "--output", str(out)])
    assert result.exit_code == 0, result.output
    assert out.exists()
    validate_result = runner.invoke(app, ["validate-corpus", str(out)])
    assert validate_result.exit_code == 0, validate_result.output
