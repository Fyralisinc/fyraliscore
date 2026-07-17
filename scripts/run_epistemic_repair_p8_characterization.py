#!/usr/bin/env python3
"""Run label-blind P8 component characterization packages."""

from __future__ import annotations

import asyncio
import argparse
import asyncpg
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p8_characterization_runner import run_characterization_contract
from lib.evaluation.epistemic_repair.p8_characterization_db import run_db_characterization
from lib.evaluation.epistemic_repair.p8_measurement_contracts import projection_refresh_measure_is_usable
from lib.contracts.kernel import canonical_sha256


async def _run(output: Path, database_url: str) -> int:
    artifact = await run_characterization_contract(ROOT)
    conn = await asyncpg.connect(database_url)
    tx = conn.transaction()
    await tx.start()
    try:
        db_result = await run_db_characterization(conn)
    finally:
        await tx.rollback()
        await conn.close()
    artifact["retrieval"] = db_result["retrieval"]
    artifact["feedback"] = db_result["feedback"]
    artifact["queue_measurement"] = {
        "status": "executed",
        "complete": db_result["queue_measurement_complete"],
        "samples": db_result["queue_samples"],
    }
    artifact["projection_refresh"] = {
        "status": "executed",
        **db_result["projection_refresh"],
        "usable": projection_refresh_measure_is_usable(db_result["projection_refresh"]),
    }
    artifact["characterization_ready"] = False  # canonical grounding + exact provider token evidence remain absent
    artifact.pop("artifact_digest", None)
    artifact["artifact_digest"] = canonical_sha256(artifact)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"characterization_ready=false executed=boundary,context,mention_detection,retrieval,feedback,queues,projection output={output}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("docs/plans/epistemic-repair/p8/p8-component-characterization.json"))
    parser.add_argument("--database-url", default="postgresql:///fyralis_epistemic_repair_work")
    args = parser.parse_args()
    return asyncio.run(_run(args.output, args.database_url))


if __name__ == "__main__":
    raise SystemExit(main())
