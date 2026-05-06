"""Typer CLI for lsob-simulation."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer

from lsob_simulation.config_loader import load_config, load_spec_filtered
from lsob_simulation.io import write_corpus
from lsob_simulation.sharded_runner import ShardedRunner, assemble_corpus
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
    output: Optional[str] = typer.Option(
        None, "--output", "-o",
        help="Single-file corpus output path (.json or .jsonl.zst). Mutually exclusive with --shards.",
    ),
    shards: Optional[str] = typer.Option(
        None, "--shards", "-s",
        help="Sharded run directory. Resumable; writes per-day shards + per-tick state checkpoints.",
    ),
    resume: bool = typer.Option(
        False, "--resume",
        help="When using --shards, resume from the last completed tick instead of starting fresh.",
    ),
) -> None:
    """Run the simulator with a YAML config and write a corpus.

    Modes:
      - `--output PATH`           single-file corpus (no resume support)
      - `--shards DIR`            sharded directory layout (resumable)
      - `--shards DIR --resume`   continue an existing sharded run
    """
    cfg = load_spec_filtered(config)  # tolerant of demo-bridge extras
    if shards and output:
        typer.echo("error: --output and --shards are mutually exclusive", err=True)
        raise typer.Exit(code=2)
    if not shards and not output:
        typer.echo("error: pass either --output or --shards", err=True)
        raise typer.Exit(code=2)

    if shards:
        runner = ShardedRunner(
            cfg, Path(shards), resume=resume, progress_label=cfg.company_id,
        )
        manifest = runner.run()
        typer.echo(
            f"shards run complete: ticks={manifest['last_completed_tick']+1} "
            f"signals={manifest['signal_count']} "
            f"ground_truth={manifest['ground_truth_count']} "
            f"-> {Path(shards)}/"
        )
        return

    sim = Simulator(cfg)
    corpus = sim.run()
    written = write_corpus(corpus, output)  # type: ignore[arg-type]
    typer.echo(
        f"wrote corpus: signals={len(corpus.signals)} "
        f"ground_truth={len(corpus.ground_truth)} -> {written}"
    )


@app.command("assemble")
def assemble_cmd(
    shards: str = typer.Option(..., "--shards", "-s", help="Sharded run directory."),
    output: str = typer.Option(..., "--output", "-o", help="Final corpus path (.jsonl.zst, .jsonl, or .json)."),
) -> None:
    """Assemble per-day shards + ground truth into a single corpus file."""
    out = assemble_corpus(Path(shards), Path(output))
    typer.echo(f"assembled: {out}")


if __name__ == "__main__":  # pragma: no cover
    app()
