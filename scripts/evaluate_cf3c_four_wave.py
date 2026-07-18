#!/usr/bin/env python3
"""Freeze PostgreSQL evidence and evaluate one completed CF3-C four-wave run."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from uuid import UUID

import asyncpg

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.contracts.kernel import canonical_sha256  # noqa: E402
from lib.evaluation.epistemic_repair.p6_population import (  # noqa: E402
    build_p6_population,
)
from lib.evaluation.epistemic_repair.p6_postfreeze_evidence import (  # noqa: E402
    extract_p6_postfreeze_evidence,
)
from services.evaluation.epistemic_repair.cf3c_four_wave import (  # noqa: E402
    evaluate_cf3c_four_wave,
)


async def _run(args: argparse.Namespace) -> int:
    raw = json.loads(args.raw.read_text(encoding="utf-8"))
    if not raw.get("complete") or raw.get("completed_batches") != 4:
        raise SystemExit("raw CF3-C execution is not a complete four-wave artifact")
    population = build_p6_population()
    if raw.get("population_digest") != population.population_digest:
        raise SystemExit("raw execution population digest mismatch")
    conn = await asyncpg.connect(args.database_url)
    try:
        evidence = await extract_p6_postfreeze_evidence(
            conn,
            tenant_id=UUID(raw["tenant_id"]),
            signal_ids=tuple(
                signal.signal_id
                for batch in population.batches[:4]
                for signal in batch.signals
            ),
            boundary_decisions=tuple(raw.get("boundary_decisions") or ()),
        )
    finally:
        await conn.close()
    frozen = {**raw, "postfreeze_evidence": evidence}
    evidence_body = {
        "schema_version": "cf3c-four-wave-postfreeze-evidence-v1",
        "commit": (raw.get("run_provenance") or {}).get("git_commit"),
        "raw_execution_digest": canonical_sha256(raw),
        "postfreeze_evidence": evidence,
    }
    evidence_artifact = {
        **evidence_body,
        "content_digest": canonical_sha256(evidence_body),
    }
    report = evaluate_cf3c_four_wave(frozen)
    args.evidence_output.parent.mkdir(parents=True, exist_ok=True)
    args.evidence_output.write_text(
        json.dumps(evidence_artifact, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        f"verdict={report['verdict']} "
        f"failed_gates={','.join(report['failed_gates']) or 'none'} "
        f"output={args.output}"
    )
    return 0 if report["verdict"] == "green" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--evidence-output", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.database_url:
        parser.error("DATABASE_URL or --database-url is required")
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
