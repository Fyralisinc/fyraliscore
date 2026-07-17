#!/usr/bin/env python3
"""Execute the frozen small active-batch-memory v6 holdout exactly once."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncpg

from lib.evaluation.company_model_ablation import evaluate_company_model_ablation
from lib.shared.migrations import apply_migrations_dir
from scripts.compiled_facet_decision_provider import CompiledFacetDecisionProvider
from scripts.run_bounded_company_model_ablation_db import (
    REPO_ROOT, _arm, _run_frozen, _run_learned, _runtime_runs,
)
from services.domain.models.repo import pgvector_pool_init
from tests.evaluation.company_model_holdout_v6 import (
    BATCHES_V6, CORPUS_DIGEST_V6, MANIFEST_DIGEST_V6, MANIFEST_V6,
)

PRODUCER_VERSION = "active-batch-memory-generic-facet-v1"


async def run_once(*, dsn: str, output: Path, receipt: Path) -> dict:
    if output.exists() or receipt.exists():
        raise RuntimeError("v6 holdout is one-shot: output or receipt already exists")
    os.environ["INQUIRY_LLM_QUESTION_PLANNING_ENABLED"] = "0"
    os.environ["THINK_COMPILED_BATCH_MEMORY_REASONING"] = "1"
    receipt_payload = {
        "schema_version": "company-model-holdout-v6-receipt-v1",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None, "status": "running", "run_attempts": 1,
        "manifest_digest": MANIFEST_DIGEST_V6, "corpus_digest": CORPUS_DIGEST_V6,
        "producer_version": PRODUCER_VERSION, "output_path": str(output.resolve()),
        "scope": "small active batch-memory mechanics holdout; not the authoritative 45-batch simulation",
        "prior_evidence": {"v4": "legacy-lane proof", "v5": "inconclusive contract failure"},
    }
    try:
        conn = await asyncpg.connect(dsn)
        try:
            await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
        finally:
            await conn.close()
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=8, init=pgvector_pool_init)
        try:
            kwargs = {"consume_model_summaries": True, "batch_definitions": BATCHES_V6,
                      "provider_factory": CompiledFacetDecisionProvider}
            lb, lm, lr = await _run_learned(pool, **kwargs)
            fb, fm, fr = await _run_frozen(pool, **kwargs)
            learned = _arm("learned_memory", lb, lm, await _runtime_runs(pool, lr),
                           manifest=MANIFEST_V6, producer_version=PRODUCER_VERSION)
            frozen = _arm("frozen_memory", fb, fm, await _runtime_runs(pool, fr),
                          manifest=MANIFEST_V6, producer_version=PRODUCER_VERSION)
            evaluation = evaluate_company_model_ablation(
                manifest=MANIFEST_V6, learned=learned, frozen=frozen)
            artifact = {
                "schema_version": "bounded-company-model-holdout-v6-artifact-v1",
                "sealing": {"manifest_digest": MANIFEST_DIGEST_V6,
                            "corpus_digest": CORPUS_DIGEST_V6, "one_shot": True,
                            "active_batch_memory_enabled": True,
                            "authoritative_45_batch_simulation_replaced": False},
                "manifest": MANIFEST_V6, "learned_arm": learned,
                "frozen_arm": frozen, "evaluation": evaluation,
            }
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
        finally:
            await pool.close()
        receipt_payload.update({
            "completed_at": datetime.now(timezone.utc).isoformat(), "status": "completed",
            "artifact_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
            "verdict": artifact["evaluation"]["verdict"],
            "continuous_score": artifact["evaluation"]["continuous_score"],
        })
        return artifact
    except Exception as exc:
        receipt_payload.update({"completed_at": datetime.now(timezone.utc).isoformat(),
                                "status": "failed", "error_type": type(exc).__name__,
                                "error_message": str(exc)})
        raise
    finally:
        receipt.parent.mkdir(parents=True, exist_ok=True)
        receipt.write_text(json.dumps(receipt_payload, indent=2, sort_keys=True) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    if not args.dsn:
        parser.error("--dsn or DATABASE_URL is required")
    artifact = asyncio.run(run_once(dsn=args.dsn, output=args.output, receipt=args.receipt))
    print(json.dumps({"output": str(args.output.resolve()),
                      "receipt": str(args.receipt.resolve()),
                      "verdict": artifact["evaluation"]["verdict"],
                      "continuous_score": artifact["evaluation"]["continuous_score"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
