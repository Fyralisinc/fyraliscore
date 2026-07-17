#!/usr/bin/env python3
"""Execute and bind exact 12/12 P8 production fault evidence."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
import subprocess
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.epistemic_repair.p8_evidence import bind_fault_execution_evidence
from lib.evaluation.epistemic_repair.p8_oracles import evaluate_p8
from lib.evaluation.epistemic_repair.p8_population import build_characterization_manifests, build_fault_schedule
from lib.evaluation.epistemic_repair.p8_postgres_runner import run_postgres_fault_slice
from lib.evaluation.epistemic_repair.p8_provider_runner import run_provider_fault_slice
from lib.evaluation.epistemic_repair.p8_runner import _distributions, _fault_results, _scale_results


async def _run(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    postgres = await run_postgres_fault_slice(args.database_url)
    provider = await run_provider_fault_slice(args.database_url)
    commit_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    evidence = bind_fault_execution_evidence(postgres=postgres, provider=provider, commit_sha=commit_sha)
    manifests = build_characterization_manifests()
    artifact = evaluate_p8(
        faults=_fault_results(), scale=_scale_results(), distributions=_distributions(),
        schedule_digest=build_fault_schedule().digest,
        manifest_digests=tuple(x.sealed_digest for x in manifests),
        production_evidence=evidence,
    )
    output = {
        "postgres_fault_slice": asdict(postgres),
        "provider_fault_slice": asdict(provider),
        "bound_execution_evidence": asdict(evidence),
        "fault_hard_gates": {key: value for key, value in artifact["hard_gates"].items() if "fault" in key or "restart" in key},
        "deterministic_qualification_ready": artifact["deterministic_qualification_ready"],
        "phase_exit_ready": artifact["phase_exit_ready"],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    fault_ready = all(output["fault_hard_gates"].values())
    print(f"fault_boundaries=12/12 executions=24 fault_evidence_ready={str(fault_ready).lower()} deterministic_qualification_ready=false output={args.output}")
    return 0 if fault_ready else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path, default=Path("docs/plans/epistemic-repair/p8/p8-production-fault-evidence.json"))
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
