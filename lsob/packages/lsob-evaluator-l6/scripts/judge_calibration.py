"""CLI entry point: run the Layer 6 judge against the calibration fixtures.

Defaults to `MockJudge` so CI can exercise the harness hermetically. Switches
to `AnthropicJudge` when both `ANTHROPIC_API_KEY` and `LSOB_RUN_REAL_JUDGE=1`
are present. Writes a JSON report to
`reports/judge_calibration/<timestamp>.json` and exits non-zero when the real
judge produces kappa < 0.75.

Usage:
  uv run python packages/lsob-evaluator-l6/scripts/judge_calibration.py
  uv run python packages/lsob-evaluator-l6/scripts/judge_calibration.py --out reports/...
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

# When invoked directly (`python scripts/judge_calibration.py`) the package's
# src/ directory is not on sys.path. Add it manually so the script works from
# any CWD without needing to `uv run -m ...`.
_PKG_SRC = Path(__file__).resolve().parent.parent / "src"
if _PKG_SRC.exists() and str(_PKG_SRC) not in sys.path:
    sys.path.insert(0, str(_PKG_SRC))

from lsob_evaluator_l6.llm_judge import (  # noqa: E402
    AnthropicJudge,
    JudgeConfig,
    LLMJudge,
    MockJudge,
    load_calibration_fixtures,
    run_calibration,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REPORTS_DIR = REPOSITORY_ROOT / "reports" / "judge_calibration"
DEFAULT_KAPPA_FLOOR = 0.75


def _use_real_judge() -> bool:
    return (
        os.environ.get("LSOB_RUN_REAL_JUDGE") == "1"
        and bool(os.environ.get("ANTHROPIC_API_KEY"))
    )


async def _run(args: argparse.Namespace) -> int:
    items = load_calibration_fixtures(args.fixtures)
    if not items:
        print(f"No calibration fixtures found under {args.fixtures}", file=sys.stderr)
        return 2

    if args.real or _use_real_judge():
        config = JudgeConfig()
        inner = AnthropicJudge(config=config)
        judge = LLMJudge(judge_client=inner, config=config)
        mode = "real"
    else:
        judge = LLMJudge(judge_client=MockJudge())
        mode = "mock"

    report = await run_calibration(judge, items)

    out_dir = Path(args.out) if args.out else DEFAULT_REPORTS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"{stamp}-{mode}.json"
    payload = report.to_dict() | {
        "mode": mode,
        "timestamp": stamp,
        "fixtures_dir": str(args.fixtures) if args.fixtures else "default",
    }
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    print(
        f"mode={mode} kappa={report.cohens_kappa:.3f} "
        f"agreement={report.agreement_rate:.3f} n={report.n_items} "
        f"report={out_path}"
    )

    if mode == "real" and report.cohens_kappa < args.kappa_floor:
        print(
            f"kappa {report.cohens_kappa:.3f} < floor {args.kappa_floor:.2f}",
            file=sys.stderr,
        )
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--fixtures",
        type=Path,
        default=None,
        help="Directory of calibration fixtures (default: packaged).",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output directory for JSON report (default: reports/judge_calibration).",
    )
    p.add_argument(
        "--real",
        action="store_true",
        help="Force real AnthropicJudge (otherwise gated on LSOB_RUN_REAL_JUDGE=1).",
    )
    p.add_argument(
        "--kappa-floor",
        type=float,
        default=DEFAULT_KAPPA_FLOOR,
        help="Minimum kappa for the real judge to be considered calibrated.",
    )
    args = p.parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
