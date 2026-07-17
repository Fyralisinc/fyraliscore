#!/usr/bin/env python3
"""Materialize the immutable failed v5 one-shot receipt with persisted DB evidence."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from datetime import datetime
from pathlib import Path

import asyncpg

from lib.contracts.kernel import canonical_sha256


async def _run(*, dsn: str, receipt_path: Path, output: Path) -> dict:
    receipt_raw = receipt_path.read_bytes()
    receipt = json.loads(receipt_raw)
    if receipt.get("status") != "failed" or receipt.get("run_attempts") != 1:
        raise ValueError("materializer requires the immutable failed one-shot receipt")
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            """SELECT run.id, run.tenant_id, tenant.name AS tenant_name,
                      run.trigger_id, run.trigger_kind, run.started_at, run.ended_at,
                      run.status, run.error, run.retrieval_model_count,
                      run.retrieval_observation_count, run.validation_error_count,
                      run.lane,
                      (SELECT count(*) FROM observations observation
                       WHERE observation.tenant_id=run.tenant_id) AS observation_count,
                      (SELECT count(*) FROM models model
                       WHERE model.tenant_id=run.tenant_id) AS model_count
               FROM think_runs run JOIN tenants tenant ON tenant.id=run.tenant_id
               WHERE tenant.name LIKE 'ablation-learned%'
                 AND run.started_at >= $1::timestamptz
                 AND run.started_at <= $2::timestamptz
               ORDER BY run.started_at""",
            datetime.fromisoformat(receipt["started_at"]),
            datetime.fromisoformat(receipt["completed_at"]),
        )
    finally:
        await conn.close()
    persisted = [
        {key: (value.isoformat() if hasattr(value, "isoformat") else str(value)
               if key in {"id", "tenant_id", "trigger_id"} else value)
         for key, value in dict(row).items()}
        for row in rows
    ]
    artifact = {
        "schema_version": "bounded-company-model-holdout-v5-failure-artifact-v1",
        "receipt_sha256": hashlib.sha256(receipt_raw).hexdigest(),
        "receipt": receipt,
        "persisted_think_runs": persisted,
        "population": {
            "planned_hidden_theses": 5, "planned_batches_per_arm": 5,
            "planned_signals_per_batch": 10, "actual_think_runs": len(persisted),
            "completed_batches": sum(row["status"] == "success" for row in persisted),
            "failed_batches": sum(row["status"] == "failed" for row in persisted),
            "judge_predictions": 0,
        },
        "verdict": "inconclusive_runtime_contract_failure",
        "generalization_claim": "unproven",
        "failure_stage": "first_learned_arm_batch_before_model_admission",
        "proof_boundary": (
            "The one-shot holdout did not reach semantic generalization judging. "
            "It proves a runtime response-contract incompatibility between the unchanged "
            "v4 generic producer and the active compiled batch-memory path. No rerun or "
            "post-result tuning was performed."
        ),
    }
    artifact["objective_sha256"] = canonical_sha256(artifact)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    return artifact


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = asyncio.run(_run(dsn=args.dsn, receipt_path=args.receipt, output=args.output))
    print(json.dumps({"output": str(args.output.resolve()),
                      "objective_sha256": result["objective_sha256"],
                      "verdict": result["verdict"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
