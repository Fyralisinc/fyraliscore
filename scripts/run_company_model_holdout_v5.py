#!/usr/bin/env python3
"""Execute the frozen untouched v5 holdout exactly once."""

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
from scripts.run_bounded_company_model_ablation_db import (
    REPO_ROOT, _arm, _models, _run_frozen, _run_learned, _runtime_runs,
)
from services.domain.models.repo import pgvector_pool_init
from tests.evaluation.company_model_holdout_v5 import (
    BATCHES_V5, CORPUS_DIGEST_V5, MANIFEST_DIGEST_V5, MANIFEST_V5,
)


PRODUCER_VERSION = "v4-generic-model-consuming-frozen-for-v5"


async def run_once(*, dsn: str, output: Path, receipt: Path) -> dict:
    if output.exists() or receipt.exists():
        raise RuntimeError("v5 holdout is one-shot: output or receipt already exists")
    started = datetime.now(timezone.utc).isoformat()
    receipt_payload = {
        "schema_version": "company-model-holdout-v5-receipt-v1",
        "started_at": started, "completed_at": None, "status": "running",
        "run_attempts": 1, "manifest_digest": MANIFEST_DIGEST_V5,
        "corpus_digest": CORPUS_DIGEST_V5, "producer_version": PRODUCER_VERSION,
        "output_path": str(output.resolve()),
        "launcher_bootstrap_failures_before_experiment": 1,
        "launcher_bootstrap_failure": "ModuleNotFoundError:scripts",
    }
    try:
        bootstrap = await asyncpg.connect(dsn)
        try:
            await apply_migrations_dir(bootstrap, REPO_ROOT / "db" / "migrations")
        finally:
            await bootstrap.close()
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=8, init=pgvector_pool_init)
        try:
            learned_batches, learned_models, learned_runs = await _run_learned(
                pool, consume_model_summaries=True, batch_definitions=BATCHES_V5)
            frozen_batches, frozen_models, frozen_runs = await _run_frozen(
                pool, consume_model_summaries=True, batch_definitions=BATCHES_V5)
            learned = _arm(
                "learned_memory", learned_batches, learned_models,
                await _runtime_runs(pool, learned_runs), manifest=MANIFEST_V5,
                producer_version=PRODUCER_VERSION)
            frozen = _arm(
                "frozen_memory", frozen_batches, frozen_models,
                await _runtime_runs(pool, frozen_runs), manifest=MANIFEST_V5,
                producer_version=PRODUCER_VERSION)
            evaluation = evaluate_company_model_ablation(
                manifest=MANIFEST_V5, learned=learned, frozen=frozen)
            artifact = {
                "schema_version": "bounded-company-model-holdout-v5-artifact-v1",
                "sealing": {"manifest_digest": MANIFEST_DIGEST_V5,
                            "corpus_digest": CORPUS_DIGEST_V5,
                            "one_shot": True, "producer_unchanged_from_v4": True},
                "manifest": MANIFEST_V5, "learned_arm": learned,
                "frozen_arm": frozen, "evaluation": evaluation,
            }
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
        finally:
            await pool.close()
        raw = output.read_bytes()
        receipt_payload.update({
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": "completed", "artifact_sha256": hashlib.sha256(raw).hexdigest(),
            "verdict": artifact["evaluation"]["verdict"],
            "continuous_score": artifact["evaluation"]["continuous_score"],
        })
        return artifact
    except Exception as exc:
        receipt_payload.update({
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": "failed", "error_type": type(exc).__name__,
            "error_message": str(exc),
        })
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
                      "continuous_score": artifact["evaluation"]["continuous_score"]},
                     sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
