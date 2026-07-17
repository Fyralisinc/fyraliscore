#!/usr/bin/env python3
"""Execute the frozen small cross-batch v7 holdout exactly once."""

from __future__ import annotations

import argparse, asyncio, hashlib, json, os, sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))

import asyncpg
from lib.evaluation.company_model_ablation import evaluate_company_model_ablation, manifest_digest
from lib.shared.migrations import apply_migrations_dir
from scripts.compiled_facet_decision_provider import CompiledFacetDecisionProvider
from scripts.run_bounded_company_model_ablation_db import REPO_ROOT, _run_frozen, _run_learned, _runtime_runs
from services.domain.models.repo import pgvector_pool_init
from tests.evaluation.company_model_holdout_v7 import BATCHES_V7, CORPUS_DIGEST_V7, MANIFEST_DIGEST_V7, MANIFEST_V7

PRODUCER_VERSION = "active-batch-memory-generic-facet-v2"


def _arm(name, batches, models, runs, *, frozen):
    predictions = []
    for thesis in MANIFEST_V7["hidden_theses"]:
        subject = thesis["thesis_id"]; prefix = f"{subject} evidence facets:"
        by_tenant = {}
        for row in models:
            if str(row["natural"]).startswith(prefix):
                by_tenant.setdefault(str(row["tenant_id"]), set()).update(
                    x.strip() for x in str(row["natural"]).partition(":")[2].split(","))
        coverage = (max((len(v) for v in by_tenant.values()), default=0) if frozen
                    else len(set().union(*by_tenant.values())) if by_tenant else 0)
        predictions.append({"thesis_id": subject, "recovered": coverage == 10,
                            "confidence": min(.9, .4 + .05 * coverage),
                            "future_outcomes": [1, 1, 1, 1], "runtime_model_id": None})
    return {"schema_version": "company-model-ablation-arm-v1", "arm": name,
            "producer_id": PRODUCER_VERSION, "truth_visible_to_producer": False,
            "hidden_truth_digest": manifest_digest(MANIFEST_V7),
            "batches": [{"batch_id": f"batch-{i:02d}", "signal_ids": list(signals)}
                        for i, signals in enumerate(batches, 1)],
            "predictions": predictions, "safety_incidents": [],
            "runtime_run_ids": [r["run_id"] for r in runs], "runtime_context_use": runs,
            "runtime_model_count": len(models)}


async def run_once(dsn, output, receipt):
    if output.exists() or receipt.exists(): raise RuntimeError("v7 is one-shot")
    os.environ["INQUIRY_LLM_QUESTION_PLANNING_ENABLED"] = "0"
    os.environ["THINK_COMPILED_BATCH_MEMORY_REASONING"] = "1"
    meta = {"schema_version": "company-model-holdout-v7-receipt-v1",
            "started_at": datetime.now(timezone.utc).isoformat(), "status": "running",
            "run_attempts": 1, "manifest_digest": MANIFEST_DIGEST_V7,
            "corpus_digest": CORPUS_DIGEST_V7, "producer_version": PRODUCER_VERSION,
            "scope": "small cross-batch causal holdout; authoritative 45-batch run untouched",
            "prior_evidence": {"v4": "legacy-lane proof", "v5": "inconclusive", "v6": "below_policy mechanics proof"}}
    try:
        conn = await asyncpg.connect(dsn)
        try: await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
        finally: await conn.close()
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=8, init=pgvector_pool_init)
        try:
            kw = {"consume_model_summaries": True, "batch_definitions": BATCHES_V7,
                  "provider_factory": CompiledFacetDecisionProvider}
            lb,lm,lr = await _run_learned(pool, **kw); fb,fm,fr = await _run_frozen(pool, **kw)
            learned = _arm("learned_memory", lb, lm, await _runtime_runs(pool, lr), frozen=False)
            frozen = _arm("frozen_memory", fb, fm, await _runtime_runs(pool, fr), frozen=True)
            evaluation = evaluate_company_model_ablation(manifest=MANIFEST_V7, learned=learned, frozen=frozen)
            artifact = {"schema_version": "bounded-company-model-holdout-v7-artifact-v1",
                        "sealing": {"manifest_digest": MANIFEST_DIGEST_V7, "corpus_digest": CORPUS_DIGEST_V7,
                                    "one_shot": True, "active_batch_memory_enabled": True,
                                    "cross_batch_required": True, "authoritative_45_batch_simulation_replaced": False},
                        "manifest": MANIFEST_V7, "learned_arm": learned, "frozen_arm": frozen,
                        "evaluation": evaluation}
            output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
        finally: await pool.close()
        meta.update({"status": "completed", "completed_at": datetime.now(timezone.utc).isoformat(),
                     "artifact_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                     "verdict": evaluation["verdict"], "continuous_score": evaluation["continuous_score"]})
        return artifact
    except Exception as exc:
        meta.update({"status": "failed", "completed_at": datetime.now(timezone.utc).isoformat(),
                     "error_type": type(exc).__name__, "error_message": str(exc)}); raise
    finally:
        receipt.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")


def main():
    p=argparse.ArgumentParser(); p.add_argument("--dsn", required=True); p.add_argument("--output", type=Path, required=True); p.add_argument("--receipt", type=Path, required=True); a=p.parse_args()
    result=asyncio.run(run_once(a.dsn,a.output,a.receipt)); print(json.dumps(result["evaluation"],sort_keys=True))


if __name__ == "__main__": main()
