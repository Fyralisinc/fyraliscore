"""`lsob-eval-l5` CLI — evaluate a Corpus JSON file with the L5 evaluator."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import typer
from lsob_contracts import Corpus, EvaluationContext

from lsob_evaluator_l5.evaluator import LayerFiveEvaluator
from lsob_evaluator_l5.mock_sut import MockTemporalSUT

app = typer.Typer(
    add_completion=False,
    help="LSOB Layer 5 (temporal dynamics) evaluator CLI.",
    no_args_is_help=True,
)


@app.callback()
def _main() -> None:
    """Entry point; ensures typer treats subcommands explicitly."""
    return None


def _load_corpus(path: Path) -> Corpus:
    data = json.loads(path.read_text())
    return Corpus.model_validate(data)


def _run_eval(corpus: Corpus, sut_name: str) -> list[dict]:
    if sut_name != "mock":
        raise typer.BadParameter(f"unknown sut: {sut_name!r}")
    sut = MockTemporalSUT()
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=(
            corpus.ground_truth[-1].timestamp
            if corpus.ground_truth
            else corpus.meta.end_date
        ),
        run_id="cli-l5",
        extras={},
    )
    evaluator = LayerFiveEvaluator()
    results = asyncio.run(evaluator.evaluate(ctx))
    return [r.model_dump(mode="json") for r in results]


@app.command()
def run(
    corpus: Path = typer.Option(..., exists=True, readable=True, help="Corpus JSON"),
    sut: str = typer.Option("mock", help="SUT identifier (only 'mock' supported)"),
) -> None:
    """Run the Layer 5 evaluator and print JSON results to stdout."""
    corpus_model = _load_corpus(corpus)
    results = _run_eval(corpus_model, sut)
    typer.echo(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":  # pragma: no cover
    app()
