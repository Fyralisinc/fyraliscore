"""`lsob-eval-l6` CLI — emits a JSON list of EvalResult to stdout."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import typer
from lsob_contracts import Corpus, EvaluationContext

from lsob_evaluator_l6.evaluator import LayerSixEvaluator
from lsob_evaluator_l6.mock_sut import MockDiffProducingSUT

app = typer.Typer(help="LSOB Layer 6 (decision-support quality) evaluator CLI.")


@app.callback()
def _root() -> None:
    """Root callback — forces Typer to treat `run` as a subcommand.

    Without this, a Typer app with a single `@app.command()` collapses that
    command into the root, which breaks the documented `lsob-eval-l6 run ...`
    usage.
    """


def _load_corpus(corpus_path: Path) -> Corpus:
    return Corpus.model_validate_json(corpus_path.read_text())


def _build_sut(kind: str, corpus_path: Path):
    if kind == "mock":
        # If a sibling canned-diffs JSON exists next to the corpus, pick it up.
        canned_path = corpus_path.parent / "l6_canned_sut_diffs.json"
        canned = None
        if canned_path.exists():
            canned = json.loads(canned_path.read_text())
        return MockDiffProducingSUT(canned=canned)
    raise typer.BadParameter(f"unknown sut kind: {kind}")


async def _run(
    corpus_path: Path,
    sut_kind: str,
    run_id: str,
    enable_judge: bool,
    max_judge_calls: int,
) -> list[dict]:
    corpus = _load_corpus(corpus_path)
    sut = _build_sut(sut_kind, corpus_path)
    evaluator = LayerSixEvaluator(max_judge_calls=max_judge_calls)
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
        extras={"enable_llm_judge": enable_judge},
    )
    results = await evaluator.evaluate(ctx)
    return [r.model_dump(mode="json") for r in results]


@app.command("run")
def run(
    corpus: Path = typer.Option(..., help="Path to a Corpus JSON fixture."),
    sut: str = typer.Option("mock", help="SUT variant: mock."),
    run_id: str = typer.Option(
        "l6-cli-run", help="Run identifier attached to every EvalResult."
    ),
    enable_judge: bool = typer.Option(
        False,
        "--enable-judge/--no-enable-judge",
        help="Run Phase 6b with the (default mock) LLM judge.",
    ),
    max_judge_calls: int = typer.Option(
        500, help="Cap on judge comparisons per run."
    ),
) -> None:
    """Run the Layer 6 evaluator and write JSON results to stdout."""
    payload = asyncio.run(
        _run(corpus, sut, run_id, enable_judge, max_judge_calls)
    )
    typer.echo(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":  # pragma: no cover
    app()
