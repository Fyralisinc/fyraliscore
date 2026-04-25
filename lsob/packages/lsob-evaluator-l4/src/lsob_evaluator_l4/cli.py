"""``lsob-eval-l4`` CLI entrypoint.

Usage:

    lsob-eval-l4 run --corpus fixtures/mini_corpus_a.json --sut mock

Prints a JSON list of EvalResult rows — one per metric.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import typer
from lsob_contracts import Corpus, EvaluationContext

from lsob_evaluator_l4.evaluator import LayerFourEvaluator
from lsob_evaluator_l4.metrics import derive_positive_commitments
from lsob_evaluator_l4.mock_sut import (
    MockSurfacingSUT,
    make_commitment_at_risk,
    make_customer_at_risk,
)

app = typer.Typer(
    help="Run the LSOB Layer 4 surfacing-quality evaluator.",
    no_args_is_help=True,
)


@app.callback()
def _root() -> None:
    """LSOB Layer 4 surfacing-quality evaluator."""


def _load_corpus(path: Path) -> Corpus:
    data = json.loads(path.read_text())
    return Corpus.model_validate(data)


def _mock_sut_for_corpus(corpus: Corpus) -> MockSurfacingSUT:
    """Build a mock that mirrors ground truth — useful as a smoke baseline."""
    at_risk: dict = {}
    for gt in corpus.ground_truth:
        items = []
        positives = derive_positive_commitments([gt], gt.timestamp)
        for cid in sorted(positives):
            items.append(make_commitment_at_risk(cid))
        # Surface customers with a degraded true_health.
        for cust in gt.customers:
            if cust.get("true_health") in {"degraded", "critical", "churned"}:
                items.append(make_customer_at_risk(cust["id"]))
        at_risk[gt.timestamp] = items
    return MockSurfacingSUT(canned_at_risk=at_risk, canned_anomalies=[])


@app.command("run")
def run(
    corpus: Path = typer.Option(..., exists=True, readable=True),
    sut: str = typer.Option("mock"),
    run_id: str = typer.Option("l4-cli-run"),
) -> None:
    """Run the L4 evaluator and dump EvalResults as JSON to stdout."""
    corpus_obj = _load_corpus(corpus)
    if sut != "mock":
        raise typer.BadParameter(
            f"Only `--sut mock` is wired up in the skeleton CLI; got {sut!r}."
        )
    mock = _mock_sut_for_corpus(corpus_obj)
    evaluator = LayerFourEvaluator()
    ctx = EvaluationContext(
        corpus=corpus_obj,
        sut=mock,
        ground_truth_checkpoint=corpus_obj.ground_truth[0].timestamp
        if corpus_obj.ground_truth
        else corpus_obj.meta.end_date,
        run_id=run_id,
    )
    results = asyncio.run(evaluator.evaluate(ctx))
    typer.echo(json.dumps([r.model_dump(mode="json") for r in results], indent=2))


if __name__ == "__main__":  # pragma: no cover
    app()
