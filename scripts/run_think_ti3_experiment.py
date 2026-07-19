#!/usr/bin/env python3
"""Run TI3 from pre-captured responses; this CLI never constructs a provider."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from services.evaluation.epistemic_repair.think_ti3_experiment import (
    HistoricalBaselineBinding,
    ProviderAttempt,
    run_ti3_experiment,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--responses", type=Path, required=True,
                        help="JSON object keyed phase:arm:case_id:sample_index")
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--quality-tolerance", type=float, default=.03)
    for name in ("raw", "evidence", "report"):
        parser.add_argument(f"--atlas-baseline-{name}", type=Path, required=True)
        parser.add_argument(f"--atlas-baseline-{name}-digest", required=True)
    return parser.parse_args()


async def _main() -> int:
    args = _arguments()
    responses = json.loads(args.responses.read_text(encoding="utf-8"))

    async def captured(capture):
        key = f"{capture.phase}:{capture.arm}:{capture.case_id}:{capture.sample_index}"
        if key not in responses:
            raise KeyError(f"missing captured response {key}")
        return ProviderAttempt.model_validate(responses[key])

    artifact = await run_ti3_experiment(
        output_root=args.output_root, run_id=args.run_id, provider=captured,
        commit=args.commit,
        quality_tolerance=args.quality_tolerance,
        historical_atlas_baseline=HistoricalBaselineBinding(
            raw_path=args.atlas_baseline_raw,
            evidence_path=args.atlas_baseline_evidence,
            report_path=args.atlas_baseline_report,
            raw_digest=args.atlas_baseline_raw_digest,
            evidence_digest=args.atlas_baseline_evidence_digest,
            report_digest=args.atlas_baseline_report_digest,
        ),
    )
    print(json.dumps(artifact, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
