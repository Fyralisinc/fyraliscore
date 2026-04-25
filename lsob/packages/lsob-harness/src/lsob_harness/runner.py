"""End-to-end run orchestration.

The :func:`run_once` coroutine is the canonical "one benchmark run" entry
point. It:

1. Loads the corpus.
2. Constructs the SUT (dynamic registry -- mock if real baselines aren't
   installed) and applies the requested ablation.
3. Splits signals into monthly checkpoint windows.
4. Streams each window through a :class:`~lsob_harness.ingester.Ingester`
   and runs per-month evaluators at every boundary.
5. After the full corpus, runs final-phase evaluators (layers 3/5/6
   typically) once.
6. Persists a :class:`RunManifest` and the collected :class:`EvalResult`
   list to ``<runs_root>/<run_id>/results.db`` and mirrors the manifest
   into ``<runs_root>/index.db``.

Phase 2.2 additions:
- Parallel-ingester support via ``RunRequest.use_parallel_ingester``.
- Checkpoint file persistence after each monthly evaluation.
- ``resume_run`` entry point to continue from the last checkpoint.
- Per-signal / per-evaluator / wall-clock timings persisted to the
  ``timings`` table (bucketed to 1-second windows for >10k signals).
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from lsob_contracts import (
    AblationConfig,
    Corpus,
    EvalResult,
    EvaluationContext,
    RunManifest,
    Signal,
    SUTConfig,
)

from lsob_harness import db as dbmod
from lsob_harness.ablation import apply_ablation as _apply_ablation
from lsob_harness.checkpoint import (
    CheckpointState,
    CorpusHashMismatch,
    corpus_file_hash,
    read_checkpoint_file,
    write_checkpoint_file,
)
from lsob_harness.corpus_io import load_corpus
from lsob_harness.ingester import Ingester, ParallelIngester, SequentialIngester
from lsob_harness.phases import EvaluatorPhase, evaluator_phase
from lsob_harness.registry import construct_evaluators, construct_sut


BUCKET_TIMINGS_THRESHOLD = 10_000


@dataclass
class RunRequest:
    corpus_path: Path
    sut_name: str
    layers: list[int]
    ablation: AblationConfig = field(default_factory=AblationConfig)
    runs_root: Path = field(default_factory=lambda: Path("runs"))
    sut_params: dict[str, Any] = field(default_factory=dict)
    tenant_id: str | None = None
    judge_model: str | None = None
    seed: int = 42
    rate_limit: float | None = None
    use_parallel_ingester: bool = False
    checkpoint_every_n: int | None = None
    # hooks exposed for tests:
    sut_override: Any | None = None
    evaluators_override: list[Any] | None = None
    ingester_override: Ingester | None = None


@dataclass
class RunOutcome:
    run_id: str
    manifest: RunManifest
    results: list[EvalResult]
    results_db: Path
    summary_path: Path
    index_db: Path
    checkpoint_path: Path | None = None
    timings: list[dict[str, Any]] = field(default_factory=list)


def _short_git_sha() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
        return out.stdout.strip() or "nogit"
    except Exception:
        return "nogit"


def _ts_slug(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%S")


def build_run_id(
    sut: str, corpus_id: str, ablation_name: str, started_at: datetime, sha: str
) -> str:
    return f"{sut}-{corpus_id}-{ablation_name}-{_ts_slug(started_at)}-{sha}"


def monthly_checkpoints(corpus: Corpus) -> list[datetime]:
    """Month-end UTC timestamps covering the corpus range (inclusive)."""
    start = corpus.meta.start_date
    end = corpus.meta.end_date
    checkpoints: list[datetime] = []
    cursor = start
    while cursor <= end:
        next_month = _add_month(cursor)
        boundary = min(next_month - timedelta(microseconds=1), end)
        checkpoints.append(boundary)
        if next_month > end:
            break
        cursor = next_month
    if not checkpoints:
        checkpoints.append(end)
    return checkpoints


def _add_month(dt: datetime) -> datetime:
    # Month-end is approximated by shifting to the first of the next month.
    year = dt.year + (1 if dt.month == 12 else 0)
    month = 1 if dt.month == 12 else dt.month + 1
    return dt.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)


def split_signals_by_checkpoint(
    signals: Iterable[Signal], checkpoints: list[datetime]
) -> list[list[Signal]]:
    """Return one list of signals per checkpoint (inclusive upper bound)."""
    buckets: list[list[Signal]] = [[] for _ in checkpoints]
    sorted_sig = sorted(signals, key=lambda s: s.timestamp)
    i = 0
    for sig in sorted_sig:
        while i < len(checkpoints) - 1 and sig.timestamp > checkpoints[i]:
            i += 1
        buckets[i].append(sig)
    return buckets


def _evaluator_label(ev: Any) -> str:
    layer = getattr(ev, "layer_id", "?")
    names = getattr(ev, "metric_names", None) or ["?"]
    return f"L{layer}:{names[0]}"


def _default_ingester(req: RunRequest) -> Ingester:
    if req.ingester_override is not None:
        return req.ingester_override
    if req.use_parallel_ingester:
        return ParallelIngester(checkpoint_every_n=req.checkpoint_every_n)
    return SequentialIngester(
        rate_limit=req.rate_limit, checkpoint_every_n=req.checkpoint_every_n
    )


def _bucket_timings(timings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse per-signal timings into 1-second buckets (keyed on finished_at)."""
    signal_rows = [t for t in timings if t.get("phase") == "ingest_signal"]
    if len(signal_rows) <= BUCKET_TIMINGS_THRESHOLD:
        return timings
    buckets: dict[str, dict[str, Any]] = {}
    for t in signal_rows:
        fin = t.get("finished_at")
        if isinstance(fin, datetime):
            key = fin.replace(microsecond=0).isoformat()
        elif isinstance(fin, str) and len(fin) >= 19:
            key = fin[:19]
        else:
            key = "unknown"
        slot = buckets.setdefault(
            key,
            {
                "phase": "ingest_signal_bucket_1s",
                "detail": key,
                "started_at": None,
                "finished_at": None,
                "duration_ms": 0.0,
                "_count": 0,
            },
        )
        slot["duration_ms"] += float(t.get("duration_ms", 0.0))
        slot["_count"] += 1
    out: list[dict[str, Any]] = [t for t in timings if t.get("phase") != "ingest_signal"]
    for slot in buckets.values():
        slot["detail"] = f"{slot['detail']} n={slot.pop('_count')}"
        out.append(slot)
    return out


@dataclass
class _RunState:
    """Mutable state shared between run_once and resume_run."""

    req: RunRequest
    corpus: Corpus
    run_id: str
    started_at: datetime
    sha: str
    run_dir: Path
    results_db: Path
    index_db: Path
    summary_path: Path
    corpus_hash: str


def _init_state(req: RunRequest) -> _RunState:
    corpus = load_corpus(req.corpus_path)
    started_at = datetime.now(timezone.utc)
    sha = _short_git_sha()
    run_id = build_run_id(
        req.sut_name, corpus.meta.corpus_id, req.ablation.name, started_at, sha
    )
    run_dir = req.runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return _RunState(
        req=req,
        corpus=corpus,
        run_id=run_id,
        started_at=started_at,
        sha=sha,
        run_dir=run_dir,
        results_db=run_dir / "results.db",
        index_db=req.runs_root / "index.db",
        summary_path=run_dir / "summary.json",
        corpus_hash=corpus_file_hash(req.corpus_path),
    )


async def _drive_run(
    state: _RunState,
    *,
    resume_from: CheckpointState | None = None,
    existing_results: list[EvalResult] | None = None,
) -> RunOutcome:
    req = state.req
    corpus = state.corpus
    run_id = state.run_id

    sut_config = SUTConfig(
        sut_name=req.sut_name, tenant_id=req.tenant_id, params=dict(req.sut_params)
    )
    sut = req.sut_override or construct_sut(req.sut_name, sut_config)
    evaluators = (
        req.evaluators_override
        if req.evaluators_override is not None
        else construct_evaluators(req.layers)
    )
    ingester = _default_ingester(req)

    await sut.startup(sut_config)
    await _apply_ablation(sut, req.ablation)

    checkpoints = monthly_checkpoints(corpus)
    buckets = split_signals_by_checkpoint(corpus.signals, checkpoints)
    per_month_evals = [e for e in evaluators if evaluator_phase(e) == EvaluatorPhase.per_month]
    final_evals = [e for e in evaluators if evaluator_phase(e) == EvaluatorPhase.final]

    results: list[EvalResult] = list(existing_results or [])
    timings: list[dict[str, Any]] = []

    skip_to: str | None = None
    evaluators_already_run: set[str] = set()
    ingested_offset = 0
    if resume_from is not None:
        skip_to = resume_from.last_signal_id
        evaluators_already_run = set(resume_from.evaluators_run)
        ingested_offset = resume_from.ingested_count

    total_ingested = ingested_offset

    wall_start = time.monotonic()
    wall_started_at = datetime.now(timezone.utc)

    for checkpoint, bucket in zip(checkpoints, buckets):
        if skip_to is not None:
            filtered: list[Signal] = []
            saw = False
            for sig in bucket:
                if not saw:
                    if sig.signal_id == skip_to:
                        saw = True
                    continue
                filtered.append(sig)
            if saw:
                skip_to = None
                bucket = filtered
            else:
                bucket = []

        if bucket:
            ingest_t0 = time.monotonic()
            ingest_started_at = datetime.now(timezone.utc)
            timings_before = len(getattr(ingester.stats, "timings", []))
            await ingester.ingest(sut, bucket)
            ingest_ended_at = datetime.now(timezone.utc)
            ingest_duration_ms = (time.monotonic() - ingest_t0) * 1000.0
            new_timings = getattr(ingester.stats, "timings", [])[timings_before:]
            for sig_t in new_timings:
                timings.append(
                    {
                        "phase": "ingest_signal",
                        "detail": sig_t.signal_id,
                        "duration_ms": sig_t.latency_ms,
                        "started_at": ingest_started_at,
                        "finished_at": ingest_ended_at,
                    }
                )
            timings.append(
                {
                    "phase": "ingest_bucket",
                    "detail": checkpoint.isoformat(),
                    "duration_ms": ingest_duration_ms,
                    "started_at": ingest_started_at,
                    "finished_at": ingest_ended_at,
                }
            )
            total_ingested += len(bucket)

        for ev in per_month_evals:
            label = f"{_evaluator_label(ev)}@{checkpoint.isoformat()}"
            if label in evaluators_already_run:
                continue
            ev_start = time.monotonic()
            ev_started_at = datetime.now(timezone.utc)
            ctx = EvaluationContext(
                corpus=corpus,
                sut=sut,
                ground_truth_checkpoint=checkpoint,
                run_id=run_id,
            )
            part = await ev.evaluate(ctx)
            ev_duration_ms = (time.monotonic() - ev_start) * 1000.0
            for r in part:
                r.run_id = run_id
            results.extend(part)
            evaluators_already_run.add(label)
            timings.append(
                {
                    "phase": f"evaluator_{getattr(ev, 'layer_id', '?')}",
                    "detail": label,
                    "duration_ms": ev_duration_ms,
                    "started_at": ev_started_at,
                    "finished_at": datetime.now(timezone.utc),
                }
            )

        last_sig_id = getattr(ingester, "last_signal_id", None)
        checkpoint_state = CheckpointState(
            run_id=run_id,
            corpus_hash=state.corpus_hash,
            last_signal_id=last_sig_id,
            last_timestamp=checkpoint.isoformat(),
            ingested_count=total_ingested,
            evaluators_run=sorted(evaluators_already_run),
        )
        write_checkpoint_file(state.run_dir, checkpoint_state)
        with dbmod.open_db(state.results_db) as conn:
            dbmod.write_checkpoint(
                conn,
                run_id,
                checkpoint_at=checkpoint,
                ingested_count=total_ingested,
                signal_id=last_sig_id,
                last_timestamp=checkpoint,
                evaluators_run=sorted(evaluators_already_run),
            )

    final_checkpoint = checkpoints[-1] if checkpoints else corpus.meta.end_date
    for ev in final_evals:
        label = f"{_evaluator_label(ev)}@final"
        if label in evaluators_already_run:
            continue
        ev_start = time.monotonic()
        ev_started_at = datetime.now(timezone.utc)
        ctx = EvaluationContext(
            corpus=corpus,
            sut=sut,
            ground_truth_checkpoint=final_checkpoint,
            run_id=run_id,
        )
        part = await ev.evaluate(ctx)
        ev_duration_ms = (time.monotonic() - ev_start) * 1000.0
        for r in part:
            r.run_id = run_id
        results.extend(part)
        evaluators_already_run.add(label)
        timings.append(
            {
                "phase": f"evaluator_{getattr(ev, 'layer_id', '?')}",
                "detail": label,
                "duration_ms": ev_duration_ms,
                "started_at": ev_started_at,
                "finished_at": datetime.now(timezone.utc),
            }
        )

    await sut.shutdown()
    wall_ended_at = datetime.now(timezone.utc)
    total_wall_ms = (time.monotonic() - wall_start) * 1000.0
    timings.append(
        {
            "phase": "total_wall_clock",
            "detail": None,
            "duration_ms": total_wall_ms,
            "started_at": wall_started_at,
            "finished_at": wall_ended_at,
        }
    )

    finished_at = datetime.now(timezone.utc)

    totals: dict[str, float] = {}
    ingest_total = 0.0
    for t in timings:
        phase = str(t.get("phase", ""))
        totals[phase] = totals.get(phase, 0.0) + float(t.get("duration_ms", 0.0))
        if phase == "ingest_signal":
            ingest_total += float(t.get("duration_ms", 0.0))
    timings_extras = {
        "total_wall_clock_ms": total_wall_ms,
        "ingest_signal_total_ms": ingest_total,
        "by_phase_ms": totals,
        "throughput_signals_per_sec": (
            ingester.throughput_signals_per_sec()
            if hasattr(ingester, "throughput_signals_per_sec")
            else 0.0
        ),
    }

    manifest_extras: dict[str, Any] = {"timings": timings_extras}

    manifest = RunManifest(
        run_id=run_id,
        company=corpus.meta.company_id,
        months_simulated=corpus.meta.months_simulated,
        baseline=req.sut_name,
        ablation=req.ablation,
        seed=req.seed,
        git_sha=state.sha,
        started_at=state.started_at,
        finished_at=finished_at,
        corpus_uri=str(req.corpus_path),
        layers=list(req.layers),
        judge_model=req.judge_model,
    )

    persisted_timings = _bucket_timings(timings)
    with dbmod.open_db(state.results_db) as conn:
        dbmod.write_manifest(conn, manifest)
        # Overwrite any prior eval_results/timings (safe on resume).
        dbmod.delete_eval_results(conn, run_id)
        dbmod.delete_timings(conn, run_id)
        dbmod.write_eval_results(conn, run_id, results)
        dbmod.write_timings_batch(conn, run_id, persisted_timings)
    with dbmod.open_db(state.index_db) as conn:
        dbmod.write_manifest(conn, manifest)

    summary = {
        "run_id": run_id,
        "sut": req.sut_name,
        "corpus": corpus.meta.corpus_id,
        "ablation": req.ablation.name,
        "layers": list(req.layers),
        "started_at": state.started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "num_eval_results": len(results),
        "metrics": [
            {
                "layer_id": r.layer_id,
                "metric_name": r.metric_name,
                "value": r.value,
                "ci": list(r.confidence_interval) if r.confidence_interval else None,
            }
            for r in results
        ],
        "extras": manifest_extras,
    }
    state.summary_path.write_text(json.dumps(summary, indent=2))

    checkpoint_path = state.run_dir / "checkpoint.json"
    return RunOutcome(
        run_id=run_id,
        manifest=manifest,
        results=results,
        results_db=state.results_db,
        summary_path=state.summary_path,
        index_db=state.index_db,
        checkpoint_path=checkpoint_path if checkpoint_path.exists() else None,
        timings=persisted_timings,
    )


async def run_once(req: RunRequest) -> RunOutcome:
    state = _init_state(req)
    return await _drive_run(state)


async def resume_run(run_id: str, runs_root: Path) -> RunOutcome:
    """Resume a previously interrupted run from its last flushed checkpoint.

    Raises :class:`CorpusHashMismatch` if the corpus under ``corpus_uri`` has
    changed since the original run.
    """
    run_dir = runs_root / run_id
    checkpoint = read_checkpoint_file(run_dir)
    if checkpoint is None:
        raise FileNotFoundError(f"no checkpoint.json found for run {run_id}")

    results_db = run_dir / "results.db"
    with dbmod.open_db(results_db) as conn:
        manifest = dbmod.read_manifest(conn, run_id)
        if manifest is None:
            raise FileNotFoundError(f"no manifest in {results_db} for {run_id}")
        prior_results = dbmod.read_eval_results(conn, run_id)

    corpus_path = Path(manifest.corpus_uri)
    current_hash = corpus_file_hash(corpus_path)
    if current_hash != checkpoint.corpus_hash:
        raise CorpusHashMismatch(
            f"corpus {corpus_path} hash changed since checkpoint: "
            f"{checkpoint.corpus_hash} -> {current_hash}"
        )

    corpus = load_corpus(corpus_path)
    req = RunRequest(
        corpus_path=corpus_path,
        sut_name=manifest.baseline,
        layers=list(manifest.layers),
        ablation=manifest.ablation,
        runs_root=runs_root,
        seed=manifest.seed,
        judge_model=manifest.judge_model,
    )
    state = _RunState(
        req=req,
        corpus=corpus,
        run_id=run_id,
        started_at=manifest.started_at,
        sha=manifest.git_sha,
        run_dir=run_dir,
        results_db=results_db,
        index_db=runs_root / "index.db",
        summary_path=run_dir / "summary.json",
        corpus_hash=current_hash,
    )
    return await _drive_run(state, resume_from=checkpoint, existing_results=prior_results)


def run_sync(req: RunRequest) -> RunOutcome:
    """Synchronous helper -- used by the CLI."""
    return asyncio.run(run_once(req))
