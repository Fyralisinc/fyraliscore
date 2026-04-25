"""Typer CLI for the Layer 3 calibration evaluator."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import typer
from lsob_contracts import Corpus, EvaluationContext

from lsob_evaluator_l3.evaluator import DEFAULT_OUTPUT_DIR, LayerThreeEvaluator
from lsob_evaluator_l3.mock_sut import MockCalibratedSUT

app = typer.Typer(
    add_completion=False,
    help="LSOB Layer 3 (calibration) evaluator CLI",
    no_args_is_help=True,
)


@app.callback()
def _main() -> None:
    """Entry point; forces typer to require a subcommand name."""
    return None


def _load_corpus(path: Path) -> Corpus:
    with path.open("r") as f:
        raw = json.load(f)
    return Corpus.model_validate(raw)


def _build_sut(kind: str, corpus: Corpus) -> MockCalibratedSUT:
    if kind == "mock":
        preds = [p for gt in corpus.ground_truth for p in gt.predictions_that_will_resolve]
        actor_hint = "unknown"
        if corpus.ground_truth and corpus.ground_truth[-1].actors:
            actor_hint = corpus.ground_truth[-1].actors[0].get("id", "unknown")
        return MockCalibratedSUT.from_predictions(preds, actor_id=actor_hint)
    raise typer.BadParameter(f"unsupported SUT kind: {kind}")


@app.command()
def run(
    corpus: Path = typer.Option(..., exists=True, readable=True, help="Path to corpus JSON"),
    sut: str = typer.Option("mock", help="SUT kind (mock)"),
    output_dir: Path = typer.Option(
        Path(DEFAULT_OUTPUT_DIR), help="Directory for reliability-diagram PNGs"
    ),
    run_id: str = typer.Option("l3-run", help="Run identifier"),
) -> None:
    """Run Layer 3 evaluation and print a JSON-encoded EvalResult list."""
    corpus_obj = _load_corpus(corpus)
    output_dir.mkdir(parents=True, exist_ok=True)
    sut_obj = _build_sut(sut, corpus_obj)
    checkpoint = (
        corpus_obj.ground_truth[-1].timestamp
        if corpus_obj.ground_truth
        else corpus_obj.meta.end_date
    )
    ctx = EvaluationContext(
        corpus=corpus_obj,
        sut=sut_obj,
        ground_truth_checkpoint=checkpoint,
        run_id=run_id,
        extras={"output_dir": str(output_dir)},
    )
    evaluator = LayerThreeEvaluator()
    results = asyncio.run(evaluator.evaluate(ctx))
    payload = [r.model_dump(mode="json") for r in results]
    typer.echo(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":  # pragma: no cover
    app()
