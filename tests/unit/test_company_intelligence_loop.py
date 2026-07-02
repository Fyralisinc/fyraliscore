from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from scripts import run_company_intelligence_loop as loop
from scripts.run_company_intelligence_loop import (
    BENCHMARK_TIMEOUT_EXIT_CODE,
    extract_report_dir,
    main as loop_main,
    render_highest_leverage_fixes,
)
from tests.unit.test_company_vitals import _write_report_dir


def test_extract_report_dir_prefers_explicit_handoff() -> None:
    output = """
    warmup
    {"report_dir": "/tmp/from-json"}
    report_dir=/tmp/from-line
    """

    assert extract_report_dir(output) == Path("/tmp/from-line")


def test_extract_report_dir_falls_back_to_json_summary() -> None:
    output = """
    benchmark chatter
    {
      "report_dir": "/tmp/from-json",
      "status": "passed"
    }
    """

    assert extract_report_dir(output) == Path("/tmp/from-json")


def test_render_highest_leverage_fixes_includes_validation_rule() -> None:
    scorecard = {
        "run_id": "run-1",
        "status": "watch",
        "overall_score": 0.72,
        "hard_failures": ["trigger queue did not drain"],
        "ranked_findings": [
            {
                "vital": "metabolism_yield",
                "severity": "high",
                "finding": "2 valuable signals had no durable fate.",
            }
        ],
        "vitals": {
            "metabolism_yield": {"score": 0.4, "status": "watch"},
            "retrieval_roi": {"score": 0.8, "status": "ok"},
        },
        "proof_gaps": ["DB-backed trace was not available."],
    }

    rendered = render_highest_leverage_fixes(scorecard)

    assert "Validation-First Rule" in rendered
    assert "trigger queue did not drain" in rendered
    assert "Measure and close signal-to-model loss first" in rendered
    assert "DB-backed trace was not available" in rendered


def test_loop_existing_report_dir_writes_vitals_and_fix_plan(tmp_path: Path) -> None:
    report_dir = _write_report_dir(tmp_path)

    exit_code = loop_main(["--report-dir", str(report_dir)])

    assert exit_code == 0
    assert (report_dir / "vitals" / "vitals_scorecard.json").exists()
    fix_plan = report_dir / "vitals" / "highest_leverage_fixes.md"
    assert fix_plan.exists()
    assert "Highest Leverage Company Understanding Fixes" in fix_plan.read_text()
    scorecard = json.loads(
        (report_dir / "vitals" / "vitals_scorecard.json").read_text()
    )
    assert scorecard["status"] == "ok"


def test_loop_stops_when_benchmark_fails(tmp_path: Path) -> None:
    benchmark = tmp_path / "failing_benchmark.py"
    benchmark.write_text(
        "import sys\nprint('benchmark failed')\nsys.exit(7)\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as excinfo:
        loop_main(["--benchmark-script", str(benchmark), "--python", sys.executable])

    assert excinfo.value.code == 7
    assert not (tmp_path / "vitals").exists()


def test_loop_streams_benchmark_and_runs_vitals(tmp_path: Path) -> None:
    report_dir = _write_report_dir(tmp_path)
    benchmark = tmp_path / "successful_benchmark.py"
    benchmark.write_text(
        "\n".join(
            [
                "import json",
                "print('wave=1', flush=True)",
                f"print(json.dumps({{'report_dir': {str(report_dir)!r}, 'status': 'passed'}}), flush=True)",
            ]
        ),
        encoding="utf-8",
    )

    exit_code = loop_main(
        [
            "--benchmark-script",
            str(benchmark),
            "--python",
            sys.executable,
            "--benchmark-timeout",
            "5",
        ]
    )

    assert exit_code == 0
    assert (report_dir / "vitals" / "vitals_scorecard.json").exists()


def test_loop_creates_missing_seed_baseline_once_then_appends(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report_root = tmp_path / "runs"
    report_root.mkdir()
    report_dir = _write_report_dir(tmp_path)
    calls: list[list[str]] = []

    def fake_stream(command: list[str], *, timeout_seconds: float):
        calls.append(command)
        if "--mode" in command and command[command.index("--mode") + 1] == "seed-only":
            baseline_id = command[command.index("--run-id") + 1]
            baseline_dir = report_root / baseline_id
            baseline_dir.mkdir(parents=True)
            (baseline_dir / "run_summary.json").write_text(
                json.dumps(
                    {
                        "mode": "seed-only",
                        "append_ready": True,
                        "tenant_id": "tenant-baseline",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            return {"returncode": 0, "stdout": f"report_dir={baseline_dir}\n"}
        return {"returncode": 0, "stdout": f"report_dir={report_dir}\n"}

    monkeypatch.setattr(loop, "_run_benchmark_streaming", fake_stream)

    exit_code = loop_main(
        [
            "--seed-baseline-run-id",
            "company-metabolism-seed",
            "--benchmark-timeout",
            "10",
            "--",
            "--report-root",
            str(report_root),
            "--target-t1-batches",
            "5",
            "--signals-per-storyline",
            "25",
        ]
    )

    assert exit_code == 0
    assert len(calls) == 2
    seed_call, run_call = calls
    assert seed_call[:4] == [sys.executable, str(loop.DEFAULT_BENCHMARK_SCRIPT), "--mode", "seed-only"]
    assert "--append-to-run-id" not in seed_call
    assert "--append-to-run-id" in run_call
    assert run_call[run_call.index("--append-to-run-id") + 1] == "company-metabolism-seed"
    assert (report_dir / "vitals" / "vitals_scorecard.json").exists()


def test_loop_reuses_append_ready_seed_baseline_without_reseeding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report_root = tmp_path / "runs"
    baseline_dir = report_root / "company-metabolism-seed"
    baseline_dir.mkdir(parents=True)
    (baseline_dir / "run_summary.json").write_text(
        json.dumps(
            {
                "mode": "seed-only",
                "append_ready": True,
                "tenant_id": "tenant-baseline",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    report_dir = _write_report_dir(tmp_path)
    calls: list[list[str]] = []

    def fake_stream(command: list[str], *, timeout_seconds: float):
        calls.append(command)
        return {"returncode": 0, "stdout": f"report_dir={report_dir}\n"}

    monkeypatch.setattr(loop, "_run_benchmark_streaming", fake_stream)

    exit_code = loop_main(
        [
            "--seed-baseline-run-id",
            "company-metabolism-seed",
            "--",
            "--report-root",
            str(report_root),
            "--target-t1-batches",
            "5",
            "--signals-per-storyline",
            "25",
        ]
    )

    assert exit_code == 0
    assert len(calls) == 1
    run_call = calls[0]
    assert "--mode" in run_call
    assert run_call[run_call.index("--mode") + 1] == "run"
    assert "--append-to-run-id" in run_call
    assert run_call[run_call.index("--append-to-run-id") + 1] == "company-metabolism-seed"


def test_loop_times_out_stuck_benchmark(tmp_path: Path) -> None:
    benchmark = tmp_path / "stuck_benchmark.py"
    benchmark.write_text(
        "import sys, time\nsys.stdout.write('started-without-newline')\n"
        "sys.stdout.flush()\ntime.sleep(30)\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as excinfo:
        loop_main(
            [
                "--benchmark-script",
                str(benchmark),
                "--python",
                sys.executable,
                "--benchmark-timeout",
                "0.2",
            ]
        )

    assert excinfo.value.code == BENCHMARK_TIMEOUT_EXIT_CODE
