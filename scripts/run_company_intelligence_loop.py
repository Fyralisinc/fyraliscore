#!/usr/bin/env python3
"""Run the benchmark -> vitals -> ranked-fixes company intelligence loop."""
from __future__ import annotations

import argparse
import asyncio
import codecs
import json
import os
import selectors
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.company_vitals import (
    collect_db_trace_for_report_dir,
    render_vitals_markdown,
    write_vitals_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BENCHMARK_SCRIPT = REPO_ROOT / "scripts" / "run_storyline_batch_benchmark.py"
BENCHMARK_TIMEOUT_EXIT_CODE = 124

FIX_BY_VITAL = {
    "metabolism_yield": (
        "Measure and close signal-to-model loss first. Inspect leak fates, "
        "then fix the largest class before tuning prompts or retrieval."
    ),
    "control_plane_health": (
        "Fix skipped metabolism and queue drain before reading semantic scores."
    ),
    "retrieval_roi": (
        "Reward retrieval by downstream durable fates, not packet survival."
    ),
    "reasoning_throughput": (
        "Find whether value dies in Think, validation, reconciliation, or apply."
    ),
    "compression_health": (
        "Keep models primary; add residuals only where compression loses value."
    ),
    "model_coherence": (
        "Run coherence repair on duplicate, contradictory, isolated, or "
        "unanchored fragments."
    ),
    "temporal_learning": (
        "Prefer future-validation loops that confirm, revise, or falsify old "
        "models."
    ),
    "projection_freshness": (
        "Trace model events into projection snapshots and repair projection lag."
    ),
    "product_utility": (
        "Tie product-surface correctness to model/projection provenance and "
        "decision outcomes."
    ),
    "human_loop_closure": (
        "Ensure human accepts, contests, and answers become model or policy "
        "updates."
    ),
    "decision_outcome_learning": (
        "Use acted-on and ignored recommendations as reward signals."
    ),
    "self_improvement": (
        "Convert repeated misses into retrieval policy, negative memory, or "
        "question-policy changes."
    ),
    "governance_health": (
        "Repair stale, unsupported, ownerless, or unsafe beliefs before they "
        "become product facts."
    ),
    "authority_safety": (
        "Block or downgrade unverifiable surfaces until authorization and "
        "provenance are explicit."
    ),
    "efficiency": (
        "Optimize dollars, tokens, and latency per useful durable fate, not per "
        "raw signal."
    ),
}


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    report_dir = args.report_dir
    if report_dir is None:
        report_dir = _run_benchmark_and_find_report_dir(args)
        if report_dir is None:
            return 2

    db_trace = None
    if args.database_url:
        db_trace = asyncio.run(
            collect_db_trace_for_report_dir(
                report_dir,
                database_url=args.database_url,
                tenant_id=args.tenant_id,
            )
        )

    result = write_vitals_artifacts(
        report_dir,
        output_dir=args.output_dir,
        db_trace=db_trace,
    )
    if not args.no_propose_fixes:
        (result.output_dir / "highest_leverage_fixes.md").write_text(
            render_highest_leverage_fixes(result.scorecard),
            encoding="utf-8",
        )

    if args.print_summary:
        sys.stdout.write(render_vitals_markdown(result.scorecard))
    else:
        print(f"report_dir={result.report_dir}")
        print(f"vitals_dir={result.output_dir}")
        print(
            "status={status} overall_score={score} hard_failures={failures}".format(
                status=result.scorecard.get("status"),
                score=result.scorecard.get("overall_score"),
                failures=len(result.scorecard.get("hard_failures") or []),
            )
        )

    if args.fail_on_hard_gates and result.scorecard.get("hard_failures"):
        return 1
    score = result.scorecard.get("overall_score")
    if (
        args.min_overall_score is not None
        and isinstance(score, (int, float))
        and score < args.min_overall_score
    ):
        print(
            f"overall_score {score:.4f} below --min-overall-score "
            f"{args.min_overall_score:.4f}",
            file=sys.stderr,
        )
        return 1
    return 0


def render_highest_leverage_fixes(scorecard: dict[str, Any]) -> str:
    """Render an operator-facing fix plan from measured vitals."""
    hard_failures = list(scorecard.get("hard_failures") or [])
    findings = [
        item for item in list(scorecard.get("ranked_findings") or [])
        if isinstance(item, dict)
    ]
    vitals = (
        scorecard.get("vitals")
        if isinstance(scorecard.get("vitals"), dict)
        else {}
    )
    weakest = _weakest_vitals(vitals)

    lines = [
        "# Highest Leverage Company Understanding Fixes",
        "",
        f"- Run: `{scorecard.get('run_id')}`",
        f"- Status: **{scorecard.get('status')}**",
        f"- Overall score: {scorecard.get('overall_score')}",
        "",
        "## Validation-First Rule",
        "",
        (
            "Each fix below is valuable only when a future vitals run moves the "
            "named metric, leak count, or hard gate in the expected direction."
        ),
        "",
    ]
    if hard_failures:
        lines.extend(["## Hard Gates", ""])
        for failure in hard_failures:
            lines.append(f"- {failure}")
        lines.append("")

    lines.extend(["## Ranked Fixes", ""])
    ranked = findings[:10] if findings else []
    if ranked:
        for index, finding in enumerate(ranked, start=1):
            vital = str(finding.get("vital") or "run")
            recommendation = FIX_BY_VITAL.get(
                vital,
                "Inspect this measured leak directly.",
            )
            lines.extend(
                [
                    f"{index}. `{vital}`: {finding.get('finding')}",
                    f"   - Success: the next DB-backed vitals run improves `{vital}` "
                    "or reduces the associated leak count.",
                    f"   - Fix direction: {recommendation}",
                ]
            )
    else:
        lines.append(
            "1. No ranked findings were emitted. Run DB-backed vitals for "
            "deeper leak attribution."
        )
    lines.append("")

    if weakest:
        lines.extend(["## Weakest Vitals", ""])
        for vital, payload in weakest[:8]:
            recommendation = FIX_BY_VITAL.get(
                vital,
                "Inspect this measured leak directly.",
            )
            lines.append(
                f"- `{vital}` score={payload.get('score')} "
                f"status={payload.get('status')}: {recommendation}"
            )
        lines.append("")

    proof_gaps = list(scorecard.get("proof_gaps") or [])
    if proof_gaps:
        lines.extend(["## Proof Gaps", ""])
        lines.extend(f"- {gap}" for gap in proof_gaps[:12])
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _weakest_vitals(vitals: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    scored: list[tuple[str, dict[str, Any]]] = []
    for name, payload in vitals.items():
        if not isinstance(payload, dict):
            continue
        score = payload.get("score")
        if isinstance(score, (int, float)):
            scored.append((name, payload))
    return sorted(
        scored,
        key=lambda item: (float(item[1].get("score") or 0.0), item[0]),
    )


def _run_benchmark_and_find_report_dir(args: argparse.Namespace) -> Path | None:
    _ensure_seed_baseline_if_requested(args)
    command = _benchmark_command(args)
    timeout = float(args.benchmark_timeout or 0.0)
    output = _run_benchmark_streaming(command, timeout_seconds=timeout)
    if output["returncode"] != 0:
        print(
            f"benchmark command failed with exit code {output['returncode']}",
            file=sys.stderr,
        )
        raise SystemExit(int(output["returncode"]))
    report_dir = extract_report_dir(str(output["stdout"]))
    if report_dir is None:
        print("benchmark output did not include report_dir", file=sys.stderr)
        return None
    return report_dir


def _ensure_seed_baseline_if_requested(args: argparse.Namespace) -> None:
    baseline_run_id = getattr(args, "seed_baseline_run_id", None)
    if not baseline_run_id:
        return
    passthrough = _normalized_benchmark_args(args.benchmark_args)
    if _passthrough_has_option(passthrough, "--append-to-run-id"):
        return

    report_root = _benchmark_report_root(passthrough)
    baseline_dir = report_root / str(baseline_run_id)
    if not _seed_baseline_ready(baseline_dir):
        command = _seed_baseline_command(args, report_root, str(baseline_run_id), passthrough)
        timeout = float(args.seed_baseline_timeout or args.benchmark_timeout or 0.0)
        output = _run_benchmark_streaming(command, timeout_seconds=timeout)
        if output["returncode"] != 0:
            print(
                f"seed baseline command failed with exit code {output['returncode']}",
                file=sys.stderr,
            )
            raise SystemExit(int(output["returncode"]))
        if not _seed_baseline_ready(baseline_dir):
            print(
                "seed baseline did not produce an append-ready run_summary.json: "
                f"{baseline_dir}",
                file=sys.stderr,
            )
            raise SystemExit(2)

    args.benchmark_args = passthrough + ["--append-to-run-id", str(baseline_run_id)]


def _seed_baseline_command(
    args: argparse.Namespace,
    report_root: Path,
    baseline_run_id: str,
    passthrough: list[str],
) -> list[str]:
    command = [
        args.python,
        str(args.benchmark_script),
        "--mode",
        "seed-only",
        "--run-id",
        baseline_run_id,
        "--report-root",
        str(report_root),
        "--seed-models",
        str(args.seed_baseline_models),
        "--seed-families",
        str(args.seed_baseline_families),
    ]
    if _passthrough_has_option(passthrough, "--skip-migrations"):
        command.append("--skip-migrations")
    pool_max_size = _passthrough_option_value(passthrough, "--pool-max-size")
    if pool_max_size is not None:
        command.extend(["--pool-max-size", pool_max_size])
    return command


def _seed_baseline_ready(baseline_dir: Path) -> bool:
    summary_path = baseline_dir / "run_summary.json"
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    return (
        payload.get("mode") == "seed-only"
        and bool(payload.get("append_ready"))
        and bool(payload.get("tenant_id"))
    )


def _run_benchmark_streaming(
    command: list[str],
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    started = time.monotonic()
    deadline = started + timeout_seconds if timeout_seconds > 0 else None
    output_parts: list[str] = []
    process = subprocess.Popen(
        command,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    selector = selectors.DefaultSelector()
    try:
        if process.stdout is not None:
            os.set_blocking(process.stdout.fileno(), False)
            selector.register(process.stdout.fileno(), selectors.EVENT_READ)
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                _terminate_process_tree(process)
                print(
                    "benchmark command timed out after "
                    f"{timeout_seconds:.1f}s",
                    file=sys.stderr,
                )
                raise SystemExit(BENCHMARK_TIMEOUT_EXIT_CODE)

            select_timeout = 0.1
            if deadline is not None:
                select_timeout = max(0.0, min(select_timeout, deadline - time.monotonic()))
            for key, _mask in selector.select(timeout=select_timeout):
                text = _read_available_output(int(key.fd), decoder)
                if not text:
                    continue
                output_parts.append(text)
                sys.stdout.write(text)
                sys.stdout.flush()

            returncode = process.poll()
            if returncode is not None:
                if process.stdout is not None:
                    remainder = _read_available_output(
                        process.stdout.fileno(),
                        decoder,
                        final=True,
                    )
                    if remainder:
                        output_parts.append(remainder)
                        sys.stdout.write(remainder)
                        sys.stdout.flush()
                return {
                    "returncode": returncode,
                    "stdout": "".join(output_parts),
                    "elapsed_seconds": time.monotonic() - started,
                }
    finally:
        selector.close()
        if process.stdout is not None:
            process.stdout.close()


def _read_available_output(
    fd: int,
    decoder: codecs.IncrementalDecoder,
    *,
    final: bool = False,
) -> str:
    parts: list[str] = []
    while True:
        try:
            chunk = os.read(fd, 65536)
        except BlockingIOError:
            break
        if not chunk:
            break
        parts.append(decoder.decode(chunk))
    if final:
        parts.append(decoder.decode(b"", final=True))
    return "".join(parts)


def _terminate_process_tree(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except Exception:
        process.terminate()
    try:
        process.wait(timeout=10)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except Exception:
        process.kill()
    process.wait(timeout=5)


def _benchmark_command(args: argparse.Namespace) -> list[str]:
    passthrough = _normalized_benchmark_args(args.benchmark_args)
    command = [args.python, str(args.benchmark_script)]
    if "--mode" not in passthrough:
        command.extend(["--mode", args.benchmark_mode])
    command.extend(passthrough)
    return command


def _normalized_benchmark_args(raw: list[str] | None) -> list[str]:
    passthrough = list(raw or [])
    if passthrough and passthrough[0] == "--":
        passthrough = passthrough[1:]
    return passthrough


def _benchmark_report_root(passthrough: list[str]) -> Path:
    value = _passthrough_option_value(passthrough, "--report-root")
    if value:
        return Path(value)
    return REPO_ROOT / "tests" / "real_llm" / "reports" / "runs"


def _passthrough_has_option(passthrough: list[str], option: str) -> bool:
    return _passthrough_option_value(passthrough, option) is not None or option in passthrough


def _passthrough_option_value(passthrough: list[str], option: str) -> str | None:
    prefix = option + "="
    for index, value in enumerate(passthrough):
        if value.startswith(prefix):
            return value.split("=", 1)[1]
        if value == option:
            if index + 1 < len(passthrough) and not passthrough[index + 1].startswith("--"):
                return passthrough[index + 1]
            return ""
    return None


def extract_report_dir(output: str) -> Path | None:
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    for line in reversed(lines):
        if line.startswith("report_dir="):
            value = line.split("=", 1)[1].strip()
            return Path(value) if value else None
    for index in range(len(lines) - 1, -1, -1):
        if not lines[index].startswith("{"):
            continue
        try:
            payload = json.loads("\n".join(lines[index:]))
        except json.JSONDecodeError:
            continue
        value = payload.get("report_dir") if isinstance(payload, dict) else None
        if value:
            return Path(str(value))
    return None


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a Fyralis company-intelligence benchmark, render DB-backed "
            "vitals, and write a ranked fix plan. Pass benchmark flags after --."
        )
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=None,
        help="Existing benchmark report directory. Skips benchmark execution.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Vitals output directory. Defaults to <report-dir>/vitals.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Optional Postgres URL for DB-backed per-signal metabolism tracing.",
    )
    parser.add_argument(
        "--tenant-id",
        default=None,
        help="Override tenant id for DB-backed tracing.",
    )
    parser.add_argument(
        "--fail-on-hard-gates",
        action="store_true",
        help="Exit nonzero when vitals hard gates fail.",
    )
    parser.add_argument(
        "--min-overall-score",
        type=float,
        default=None,
        help="Exit nonzero when vitals overall score is below this threshold.",
    )
    parser.add_argument(
        "--print-summary",
        action="store_true",
        help="Print vitals summary markdown to stdout.",
    )
    parser.add_argument(
        "--no-propose-fixes",
        action="store_true",
        help="Do not write highest_leverage_fixes.md.",
    )
    parser.add_argument(
        "--benchmark-script",
        type=Path,
        default=DEFAULT_BENCHMARK_SCRIPT,
        help="Benchmark script to run when --report-dir is not provided.",
    )
    parser.add_argument(
        "--benchmark-mode",
        default="run",
        help="Benchmark mode to pass when passthrough args do not include --mode.",
    )
    parser.add_argument(
        "--benchmark-timeout",
        type=float,
        default=3600.0,
        help=(
            "Maximum seconds to wait for the benchmark subprocess before "
            f"terminating it and exiting {BENCHMARK_TIMEOUT_EXIT_CODE}. Use 0 to disable."
        ),
    )
    parser.add_argument(
        "--seed-baseline-run-id",
        default=None,
        help=(
            "Ensure this seed-only storyline baseline exists once, then append "
            "the benchmark run to it unless passthrough args already specify "
            "--append-to-run-id."
        ),
    )
    parser.add_argument(
        "--seed-baseline-models",
        type=int,
        default=5000,
        help="Model count used when creating a missing seed baseline.",
    )
    parser.add_argument(
        "--seed-baseline-families",
        type=int,
        default=100,
        help="Model family count used when creating a missing seed baseline.",
    )
    parser.add_argument(
        "--seed-baseline-timeout",
        type=float,
        default=0.0,
        help=(
            "Maximum seconds for missing baseline creation. Defaults to "
            "--benchmark-timeout; use 0 to disable the seed-specific override."
        ),
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable used for the benchmark subprocess.",
    )
    parser.add_argument(
        "benchmark_args",
        nargs=argparse.REMAINDER,
        help="Arguments after -- are passed to run_storyline_batch_benchmark.py.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
