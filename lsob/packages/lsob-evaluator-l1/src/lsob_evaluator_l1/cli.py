"""`lsob-eval-l1` CLI — emits a JSON list of EvalResult to stdout."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import typer
from lsob_contracts import Corpus, EvaluationContext

from lsob_evaluator_l1.composite import LayerOneEvaluator
from lsob_evaluator_l1.mock_sut import MockNonRetrievalSUT, MockRetrievalSUT

app = typer.Typer(help="LSOB Layer 1 (retrieval) evaluator CLI.")


@app.callback()
def _root() -> None:
    """Root callback — forces Typer to treat subcommands as subcommands.

    Without this, a Typer app with a single `@app.command()` collapses that
    command into the root, which breaks `lsob-eval-l1 run ...` usage.
    """


def _load_corpus(corpus_path: Path) -> Corpus:
    return Corpus.model_validate_json(corpus_path.read_text())


def _build_sut(kind: str, corpus: Corpus):
    if kind == "mock":
        return MockRetrievalSUT(corpus)
    if kind == "none":
        return MockNonRetrievalSUT()
    raise typer.BadParameter(f"unknown sut kind: {kind}")


async def _run(corpus_path: Path, sut_kind: str, run_id: str) -> list[dict]:
    corpus = _load_corpus(corpus_path)
    sut = _build_sut(sut_kind, corpus)
    evaluator = LayerOneEvaluator()
    # Checkpoint: last ground-truth timestamp if available, else now.
    checkpoint = (
        corpus.ground_truth[-1].timestamp
        if corpus.ground_truth
        else datetime.now(tz=timezone.utc)
    )
    ctx = EvaluationContext(
        corpus=corpus,
        sut=sut,
        ground_truth_checkpoint=checkpoint,
        run_id=run_id,
    )
    results = await evaluator.evaluate(ctx)
    return [r.model_dump(mode="json") for r in results]


@app.command()
def run(
    corpus: Path = typer.Option(..., help="Path to a Corpus JSON fixture."),
    sut: str = typer.Option(
        "mock", help="SUT variant to evaluate against: mock | none."
    ),
    run_id: str = typer.Option(
        "l1-cli-run", help="Run identifier attached to every EvalResult."
    ),
) -> None:
    """Run the Layer 1 evaluator and write JSON results to stdout."""
    payload = asyncio.run(_run(corpus, sut, run_id))
    typer.echo(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":  # pragma: no cover
    app()
