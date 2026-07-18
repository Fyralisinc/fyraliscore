#!/usr/bin/env python3
"""Freeze DB evidence and independently score one completed P6 Think run."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys
from uuid import UUID

import asyncpg

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from lib.evaluation.epistemic_repair.p6_postfreeze_evidence import (
    extract_p6_postfreeze_evidence,
)
from lib.evaluation.epistemic_repair.p6_postfreeze_scorer import (
    score_p6_frozen_execution,
)
from lib.contracts.kernel import canonical_sha256


async def _run(args: argparse.Namespace) -> int:
    raw = json.loads(args.raw.read_text(encoding="utf-8"))
    if not raw.get("complete"):
        raise SystemExit("raw P6 execution is incomplete; refusing semantic scoring")
    # Gold is opened only after raw execution has been read and frozen.
    population = build_p6_population()
    if raw.get("population_digest") != population.population_digest:
        raise SystemExit("raw execution population digest mismatch")
    conn = await asyncpg.connect(args.database_url)
    try:
        evidence = await extract_p6_postfreeze_evidence(
            conn, tenant_id=UUID(raw["tenant_id"]),
            signal_ids=tuple(signal.signal_id for signal in population.signals),
            boundary_decisions=tuple(raw.get("boundary_decisions") or ()),
        )
    finally:
        await conn.close()
    frozen = {**raw, "postfreeze_evidence": evidence}
    report = score_p6_frozen_execution(
        raw_execution=frozen, sealed_population=population,
    )
    evidence_body = {
        "schema_version": "epistemic-repair-p6-postfreeze-evidence-artifact-v1",
        "commit": (raw.get("run_provenance") or {}).get("git_commit"),
        "raw_execution_digest": canonical_sha256(raw),
        "postfreeze_evidence": evidence,
    }
    evidence_artifact = {
        **evidence_body, "content_digest": canonical_sha256(evidence_body),
    }
    args.evidence_output.parent.mkdir(parents=True, exist_ok=True)
    args.evidence_output.write_text(
        json.dumps(evidence_artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    print(
        f"phase_exit_ready={str(report['phase_exit_ready']).lower()} "
        f"missing_evidence={len(report['missing_evidence'])} output={args.output}"
    )
    return 0 if report["phase_exit_ready"] else 1


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
