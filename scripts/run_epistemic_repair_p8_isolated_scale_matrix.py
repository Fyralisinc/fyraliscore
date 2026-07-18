#!/usr/bin/env python3
"""Run each P8 scale cell in a fresh template clone, then contention separately."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, replace
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p8_database_isolation import prove_existing_template_cells
from lib.evaluation.epistemic_repair.p8_population import build_scale_matrix
from lib.evaluation.epistemic_repair.p8_scale_runner import (
    SCALE_EXECUTION_VERSION,
    ScaleExecution,
    evaluate_scale_execution,
    run_shared_contention,
)


async def _run(args: argparse.Namespace) -> int:
    proof = await prove_existing_template_cells(
        args.admin_database_url, template_name=args.template_database,
        migrations_dir=ROOT / "db" / "migrations", cells=build_scale_matrix(),
    )
    cells = tuple(
        replace(item.cell, physically_isolated_database=item.identities_distinct)
        for item in proof.cells
    )
    contention = await run_shared_contention(args.admin_database_url)
    execution = ScaleExecution(
        cells=cells, shared_contention=contention,
        exact_matrix_coverage=len(cells) == 27,
        physically_isolated_databases=proof.all_database_oids_distinct and proof.all_cell_databases_dropped,
        evidence_digest=canonical_sha256({
            "isolation_proof": proof.evidence_digest,
            "cells": [cell.evidence_digest for cell in cells],
            "contention": contention.evidence_digest,
        }),
    )
    artifact = {
        "schema_version": "p8-isolated-scale-matrix-v1",
        "execution": asdict(execution), "evaluation": evaluate_scale_execution(execution),
        "isolation_proof": asdict(proof), "commit_sha": args.expected_head,
    }
    artifact["artifact_digest"] = canonical_sha256(artifact)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    contention_artifact = {
        "schema_version": "p8-shared-contention-v2",
        "scale_execution_version": SCALE_EXECUTION_VERSION,
        "result": asdict(contention), "commit_sha": args.expected_head,
    }
    contention_artifact["artifact_digest"] = canonical_sha256(contention_artifact)
    args.contention_output.parent.mkdir(parents=True, exist_ok=True)
    args.contention_output.write_text(json.dumps(contention_artifact, indent=2, sort_keys=True) + "\n")
    ready = execution.exact_matrix_coverage and execution.physically_isolated_databases
    print(f"isolated_cells={len(cells)}/27 isolation_ready={str(ready).lower()} output={args.output}")
    return 0 if ready else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admin-database-url", required=True)
    parser.add_argument("--template-database", required=True)
    parser.add_argument("--expected-head", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--contention-output", type=Path, required=True)
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
