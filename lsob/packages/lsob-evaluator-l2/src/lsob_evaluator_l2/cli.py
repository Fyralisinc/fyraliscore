"""`lsob-eval-l2` console entry point."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import typer
from lsob_contracts import Corpus, EvaluationContext

from lsob_evaluator_l2.evaluator import LayerTwoEvaluator
from lsob_evaluator_l2.mock_sut import mock_from_ground_truth

app = typer.Typer(help="Layer 2 belief-correctness evaluator.")


@app.callback()
def _root() -> None:
    """Force Typer to keep `run` as a subcommand even when it's the only one."""


def _load_corpus(path: Path) -> Corpus:
    raw = json.loads(path.read_text())
    return Corpus.model_validate(raw)


def _result_as_json(results: list) -> str:
    return json.dumps(
        [r.model_dump(mode="json") for r in results],
        indent=2,
        sort_keys=True,
        default=str,
    )


@app.command("run")
def run(
    corpus: Path = typer.Option(..., help="Path to corpus JSON."),
    sut: str = typer.Option(
        "mock", help="SUT selector: 'mock' or 'none'."
    ),
    run_id: str = typer.Option("cli-run", help="Run identifier to stamp."),
) -> None:
    """Run L2 evaluator, print EvalResult list as JSON."""
    corpus_obj = _load_corpus(corpus)
    if sut == "mock":
        sut_impl = mock_from_ground_truth(list(corpus_obj.ground_truth))
    elif sut == "none":
        # "none" is a SUT that returns nothing; useful for smoke-testing the
        # layer_not_applicable path.
        from lsob_evaluator_l2.mock_sut import MockBeliefSUT

        sut_impl = MockBeliefSUT(canned={})
    else:
        raise typer.BadParameter(f"unknown sut selector {sut!r}")

    ctx = EvaluationContext(
        corpus=corpus_obj,
        sut=sut_impl,
        ground_truth_checkpoint=corpus_obj.meta.end_date,
        run_id=run_id,
    )
    evaluator = LayerTwoEvaluator()
    results = asyncio.run(evaluator.evaluate(ctx))
    typer.echo(_result_as_json(results))


if __name__ == "__main__":  # pragma: no cover
    app()
