"""Typer CLI for lsob-simulation."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer

from lsob_simulation.config_loader import load_config
from lsob_simulation.io import write_corpus
from lsob_simulation.simulator import Simulator
from lsob_simulation.validator import validate_corpus_file

app = typer.Typer(add_completion=False, help="LSOB simulation engine CLI.")


@app.command("validate-corpus")
def validate_corpus_cmd(path: str = typer.Argument(..., help="Path to corpus file (.json or .jsonl.zst).")) -> None:
    """Validate internal consistency of a corpus file."""
    report = validate_corpus_file(path)
    typer.echo(report.summary())
    raise typer.Exit(code=0 if report.ok else 1)


@app.command("run")
def run_cmd(
    config: str = typer.Option(..., "--config", "-c", help="YAML config path."),
    output: str = typer.Option(..., "--output", "-o", help="Output corpus path (.json or .jsonl.zst)."),
) -> None:
    """Run the simulator with a YAML config and write a corpus."""
    cfg = load_config(config)
    sim = Simulator(cfg)
    corpus = sim.run()
    written = write_corpus(corpus, output)
    typer.echo(
        f"wrote corpus: signals={len(corpus.signals)} ground_truth={len(corpus.ground_truth)} -> {written}"
    )


if __name__ == "__main__":  # pragma: no cover
    app()
