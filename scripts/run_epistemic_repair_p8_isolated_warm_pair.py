#!/usr/bin/env python3
"""Run the optimized 5x warm pair in one disposable evaluator DB clone."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p8_database_isolation import run_isolated_warm_pair


async def _run(args: argparse.Namespace) -> int:
    proof = await run_isolated_warm_pair(
        args.database_url, template_name=args.template_database,
        migrations_dir=ROOT / "db" / "migrations", repetitions=5,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(asdict(proof), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    result = proof.diagnostic
    print(
        f"clone_oid={proof.clone_identity.database_oid} external_backends={proof.clone_external_backends_before} "
        f"barrier_ratio={result.barrier_ratio_p95:.3f} end_to_end_ratio={result.end_to_end_ratio_p95:.3f} "
        f"clone_dropped={str(proof.clone_database_dropped).lower()} output={args.output}"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default="postgresql:///company_os")
    parser.add_argument("--template-database", default="fyralis_epistemic_repair_work")
    parser.add_argument("--output", type=Path, default=Path("docs/plans/epistemic-repair/p8/p8-isolated-warm-pair.json"))
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
