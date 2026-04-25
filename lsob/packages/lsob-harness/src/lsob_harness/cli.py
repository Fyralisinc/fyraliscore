"""Typer CLI: ``lsob run``, ``bulk-run``, ``compare``, ``doctor``, ``list-runs``."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from lsob_contracts import AblationConfig

from lsob_harness import db as dbmod
from lsob_harness.ablation import REGISTRY as ABLATION_REGISTRY
from lsob_harness.compare import compare_runs, find_run_db
from lsob_harness.doctor import run_doctor
from lsob_harness.matrix import MatrixSpec, expand
from lsob_harness.checkpoint import CorpusHashMismatch
from lsob_harness.runner import RunRequest, resume_run, run_once, run_sync

app = typer.Typer(add_completion=False, no_args_is_help=True, help="LSOB harness CLI")
console = Console()


def _parse_layers(raw: str) -> list[int]:
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def _ablation_from_name(name: str) -> AblationConfig:
    """Look the ablation up in the harness registry.

    Accepts either the canonical dash form (``no-bridge``, ``all-off``)
    or the underscore form (``no_bridge``) — the registry normalises
    both. Kept as a thin wrapper so callers keep getting Typer-friendly
    ``BadParameter`` errors.
    """
    try:
        return ABLATION_REGISTRY.get(name)
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc


@app.command("run")
def cmd_run(
    corpus: Optional[Path] = typer.Option(None, "--corpus", exists=True, readable=True),
    sut: Optional[str] = typer.Option(None, "--sut"),
    layers: str = typer.Option("1,2,3,4,5,6", "--layers"),
    ablation: str = typer.Option("none", "--ablation"),
    runs_root: Path = typer.Option(Path("runs"), "--runs-root"),
    seed: int = typer.Option(42, "--seed"),
    parallel: bool = typer.Option(False, "--parallel", help="Use the parallel ingester."),
    checkpoint_every_n: Optional[int] = typer.Option(None, "--checkpoint-every-n"),
    resume: Optional[str] = typer.Option(
        None, "--resume", help="Resume a prior run by run_id."
    ),
) -> None:
    """Execute a single run against a corpus, or resume one with ``--resume``."""
    if resume:
        try:
            outcome = asyncio.run(resume_run(resume, runs_root))
        except CorpusHashMismatch as exc:
            console.print(f"[bold red]corpus hash mismatch[/] {exc}")
            raise typer.Exit(code=2) from exc
        console.print(f"[bold green]resume complete[/] run_id={outcome.run_id}")
        console.print(f"  results.db : {outcome.results_db}")
        console.print(f"  summary    : {outcome.summary_path}")
        console.print(f"  index.db   : {outcome.index_db}")
        return

    if corpus is None or sut is None:
        raise typer.BadParameter("--corpus and --sut are required unless --resume is set")
    req = RunRequest(
        corpus_path=corpus,
        sut_name=sut,
        layers=_parse_layers(layers),
        ablation=_ablation_from_name(ablation),
        runs_root=runs_root,
        seed=seed,
        use_parallel_ingester=parallel,
        checkpoint_every_n=checkpoint_every_n,
    )
    outcome = run_sync(req)
    console.print(f"[bold green]run complete[/] run_id={outcome.run_id}")
    console.print(f"  results.db : {outcome.results_db}")
    console.print(f"  summary    : {outcome.summary_path}")
    console.print(f"  index.db   : {outcome.index_db}")


@app.command("bulk-run")
def cmd_bulk_run(
    matrix: Path = typer.Option(..., "--matrix", exists=True, readable=True),
    concurrency: Optional[int] = typer.Option(None, "--concurrency"),
) -> None:
    """Expand a matrix YAML and run every combination with bounded concurrency."""
    spec = MatrixSpec.from_yaml(matrix)
    if concurrency is not None:
        spec.concurrency = concurrency
    requests = expand(spec)
    console.print(f"[bold]expanded[/] {len(requests)} runs, concurrency={spec.concurrency}")

    write_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(max(1, spec.concurrency))

    async def _one(req: RunRequest) -> str:
        async with semaphore:
            outcome = await run_once(req)
            async with write_lock:
                # run_once already persists; the lock here just serialises stdout.
                console.print(f"  done: {outcome.run_id}")
            return outcome.run_id

    async def _all() -> list[str]:
        return await asyncio.gather(*[_one(r) for r in requests])

    ids = asyncio.run(_all())
    console.print(f"[bold green]bulk-run complete[/] {len(ids)} runs")


@app.command("compare")
def cmd_compare(
    run_a: str = typer.Argument(...),
    run_b: str = typer.Argument(...),
    runs_root: Path = typer.Option(Path("runs"), "--runs-root"),
    output: Optional[Path] = typer.Option(None, "--output", "-o"),
) -> None:
    """Render a markdown comparison table for two runs."""
    db_a = find_run_db(runs_root, run_a)
    db_b = find_run_db(runs_root, run_b)
    md = compare_runs(db_a, db_b)
    if output is not None:
        output.write_text(md)
        console.print(f"[bold green]wrote[/] {output}")
    console.print(md)


@app.command("doctor")
def cmd_doctor(
    workspace: Path = typer.Option(Path.cwd(), "--workspace"),
    json_output: bool = typer.Option(False, "--json"),
) -> None:
    """Print a summary table of environment checks."""
    report = run_doctor(workspace_root=workspace)
    if json_output:
        payload = {
            "ok": report.ok,
            "checks": [c.__dict__ for c in report.checks],
        }
        console.print_json(json.dumps(payload))
        raise typer.Exit(code=report.exit_code())

    table = Table(title="lsob doctor")
    table.add_column("check")
    table.add_column("required")
    table.add_column("status")
    table.add_column("detail")
    for c in report.checks:
        status = "[green]OK[/]" if c.ok else ("[red]FAIL[/]" if c.required else "[yellow]WARN[/]")
        table.add_row(c.name, "yes" if c.required else "no", status, c.message)
    console.print(table)
    raise typer.Exit(code=report.exit_code())


@app.command("list-runs")
def cmd_list_runs(
    runs_root: Path = typer.Option(Path("runs"), "--runs-root"),
) -> None:
    """Print all runs recorded in ``<runs_root>/index.db``."""
    index_db = runs_root / "index.db"
    if not index_db.exists():
        console.print(f"[yellow]no index.db at {index_db}[/]")
        return
    with dbmod.open_db(index_db) as conn:
        rows = dbmod.list_runs(conn)
    if not rows:
        console.print("[yellow]no runs recorded[/]")
        return
    table = Table(title="runs")
    for col in ("run_id", "started_at", "finished_at", "sut", "corpus", "ablation", "layer_count"):
        table.add_column(col)
    for row in rows:
        table.add_row(
            row["run_id"],
            row["started_at"] or "",
            row["finished_at"] or "",
            row["sut"],
            row["corpus"],
            row["ablation"],
            str(row["layer_count"]),
        )
    console.print(table)


def main() -> None:  # pragma: no cover
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
