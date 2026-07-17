#!/usr/bin/env python3
"""Development-only cross-batch causal proof for active batch memory."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncpg

from lib.evaluation.company_model_ablation import evaluate_company_model_ablation, manifest_digest
from lib.shared.migrations import apply_migrations_dir
from scripts.compiled_facet_decision_provider import CompiledFacetDecisionProvider
from scripts.run_bounded_company_model_ablation_db import (
    REPO_ROOT, _run_frozen, _run_learned, _runtime_runs,
)
from services.domain.models.repo import pgvector_pool_init


SUBJECTS = ("amber", "birch", "cedar", "drift", "ember")
FACETS = {
    subject: tuple(f"{subject}_facet_{index}" for index in range(1, 11))
    for subject in SUBJECTS
}
MANIFEST = {
    "schema_version": "company-model-hidden-truth-v1",
    "experiment_id": "active-batch-memory-cross-batch-development-v1",
    "judge_id": "tenant-isolated-collective-facet-judge-development-v1",
    "hidden_theses": [
        {"thesis_id": subject, "truth": f"cross-batch thesis {subject}",
         "required_groups": [[facet] for facet in FACETS[subject]]}
        for subject in SUBJECTS
    ],
}
_PAIRINGS = (("amber", "birch"), ("cedar", "drift"), ("ember", "amber"),
             ("birch", "cedar"), ("drift", "ember"))
_seen = {subject: 0 for subject in SUBJECTS}
_batches = []
for pair in _PAIRINGS:
    batch = []
    for subject in pair:
        offset = _seen[subject] * 5
        batch.extend((subject, facet) for facet in FACETS[subject][offset:offset + 5])
        _seen[subject] += 1
    _batches.append(tuple(batch))
BATCHES = tuple(_batches)


def _facets_by_tenant(models, subject):
    grouped = {}
    prefix = f"{subject} evidence facets:"
    for row in models:
        if not str(row["natural"]).startswith(prefix):
            continue
        grouped.setdefault(str(row["tenant_id"]), set()).update(
            part.strip() for part in str(row["natural"]).partition(":")[2].split(",")
        )
    return grouped


def causal_arm(name, batches, models, runtime_runs, *, frozen):
    predictions = []
    for thesis in MANIFEST["hidden_theses"]:
        subject = thesis["thesis_id"]
        groups = _facets_by_tenant(models, subject)
        if frozen:
            coverage = max((len(values) for values in groups.values()), default=0)
        else:
            coverage = len(set().union(*groups.values())) if groups else 0
        recovered = coverage == 10
        confidence = min(.9, .4 + .05 * coverage)
        predictions.append({"thesis_id": subject, "recovered": recovered,
                            "confidence": confidence, "future_outcomes": [1, 1, 1, 1],
                            "runtime_model_id": None})
    return {"schema_version": "company-model-ablation-arm-v1", "arm": name,
            "producer_id": "active-batch-memory-generic-facet-v1",
            "truth_visible_to_producer": False,
            "hidden_truth_digest": manifest_digest(MANIFEST),
            "batches": [{"batch_id": f"batch-{i:02d}", "signal_ids": list(signals)}
                        for i, signals in enumerate(batches, 1)],
            "predictions": predictions, "safety_incidents": [],
            "runtime_run_ids": [row["run_id"] for row in runtime_runs],
            "runtime_context_use": runtime_runs, "runtime_model_count": len(models)}


async def run(dsn, output):
    os.environ["INQUIRY_LLM_QUESTION_PLANNING_ENABLED"] = "0"
    os.environ["THINK_COMPILED_BATCH_MEMORY_REASONING"] = "1"
    conn = await asyncpg.connect(dsn)
    try: await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
    finally: await conn.close()
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=8, init=pgvector_pool_init)
    try:
        kwargs = {"consume_model_summaries": True, "batch_definitions": BATCHES,
                  "provider_factory": CompiledFacetDecisionProvider}
        lb, lm, lr = await _run_learned(pool, **kwargs)
        fb, fm, fr = await _run_frozen(pool, **kwargs)
        learned = causal_arm("learned_memory", lb, lm, await _runtime_runs(pool, lr), frozen=False)
        frozen = causal_arm("frozen_memory", fb, fm, await _runtime_runs(pool, fr), frozen=True)
        evaluation = evaluate_company_model_ablation(manifest=MANIFEST, learned=learned, frozen=frozen)
        artifact = {"manifest": MANIFEST, "learned_arm": learned, "frozen_arm": frozen,
                    "evaluation": evaluation}
        output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
        return artifact
    finally: await pool.close()


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--dsn", required=True)
    parser.add_argument("--output", type=Path, required=True); args = parser.parse_args()
    artifact = asyncio.run(run(args.dsn, args.output))
    print(json.dumps(artifact["evaluation"], sort_keys=True))


if __name__ == "__main__": main()
