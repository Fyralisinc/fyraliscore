#!/usr/bin/env python3
"""Run a sealed three-batch learned-memory/frozen-memory Think ablation.

This is intentionally small.  It starts from normalized persisted signals,
uses the production Think retrieve/validate/apply path, and holds hidden truth
outside the producer.  The deterministic producer performs only a generic
operation: group visible ``[FACET subject=... value=...]`` records by subject
and compress every facet present in its supplied runtime context.  It never
receives the hidden theses or the judge's required facet groups.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from typing import Any
from uuid import UUID

import asyncpg

from lib.evaluation.company_model_ablation import (
    evaluate_company_model_ablation,
    manifest_digest,
)
from lib.llm.provider import LLMConfig, LLMProvider
from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from services.domain.models.repo import pgvector_pool_init
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.reason import think
from services.reasoning.think.tests.conftest import make_embedding


REPO_ROOT = Path(__file__).resolve().parents[1]
_FACET = re.compile(r"\[FACET subject=([a-z0-9_-]+) value=([a-z0-9_-]+)\]")
_MODEL_ROW = re.compile(
    r"- id=([0-9a-f-]{36})[^\n]*?(?:natural|summary)="
    r"([a-z0-9_-]+) evidence facets: ([a-z0-9_, -]+)",
    re.IGNORECASE,
)

MANIFEST: dict[str, Any] = {
    "schema_version": "company-model-hidden-truth-v1",
    "experiment_id": "bounded-postfix-company-model-v1",
    "judge_id": "deterministic-facet-group-judge-v1",
    "hidden_theses": [
        {
            "thesis_id": "atlas",
            "truth": "security evidence and usage decay jointly block renewal",
            "required_groups": [
                ["audit", "security"], ["usage_drop", "adoption_decay"],
                ["procurement_wait", "renewal_block"],
            ],
        },
        {
            "thesis_id": "delta",
            "truth": "capacity and ownership gaps jointly cause onboarding slip",
            "required_groups": [
                ["capacity", "queue"], ["owner_gap", "handoff"],
                ["milestone_slip", "onboarding_delay"],
            ],
        },
        {
            "thesis_id": "northstar",
            "truth": "a pricing state transition implies a bounded off-sensor bridge",
            "required_groups": [
                ["pricing_blocked", "discount_denied"],
                ["pricing_approved", "exception_live"],
                ["decision_missing", "confirm_bridge"],
            ],
        },
    ],
}

BATCHES: tuple[tuple[tuple[str, str], ...], ...] = (
    (
        ("atlas", "audit"), ("atlas", "security"),
        ("delta", "capacity"), ("delta", "queue"),
        ("northstar", "pricing_blocked"), ("northstar", "discount_denied"),
    ),
    (
        ("atlas", "usage_drop"), ("atlas", "adoption_decay"),
        ("delta", "owner_gap"), ("delta", "handoff"),
        ("northstar", "pricing_approved"), ("northstar", "exception_live"),
    ),
    (
        ("atlas", "procurement_wait"), ("atlas", "renewal_block"),
        ("delta", "milestone_slip"), ("delta", "onboarding_delay"),
        ("northstar", "decision_missing"), ("northstar", "confirm_bridge"),
    ),
)


class FacetCompressionProvider(LLMProvider):
    """Context-only producer; hidden truth and judge rules are inaccessible."""

    def __init__(
        self, *, trigger_id: UUID, tenant_id: UUID, event_id: UUID,
        actor_id: UUID, consume_model_summaries: bool = False,
    ):
        super().__init__(LLMConfig(provider="deterministic", api_key="none", model="facet-compressor-v1"))
        self.trigger_id = trigger_id
        self.tenant_id = tenant_id
        self.event_id = event_id
        self.actor_id = actor_id
        self.consume_model_summaries = consume_model_summaries
        self.calls: list[dict[str, Any]] = []

    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        del temperature, max_tokens, schema_hint
        self.calls.append({"system": system, "user": user})
        grouped: dict[str, set[str]] = defaultdict(set)
        supporting: dict[str, set[UUID]] = defaultdict(set)
        for subject, facet in _FACET.findall(user):
            grouped[subject].add(facet)
        if self.consume_model_summaries:
            for raw_id, subject, raw_facets in _MODEL_ROW.findall(user):
                grouped[subject].update(
                    facet.strip() for facet in raw_facets.split(",") if facet.strip()
                )
                supporting[subject].add(UUID(raw_id))
        ops = []
        for subject, facets in sorted(grouped.items()):
            natural = f"{subject} evidence facets: " + ", ".join(sorted(facets))
            confidence = min(0.85, 0.25 + 0.10 * len(facets))
            ops.append(
                {
                    "op": "insert",
                    "entry": {
                        "tenant_id": str(self.tenant_id),
                        "born_from_event_id": str(self.event_id),
                        "proposition": {
                            "kind": "state",
                            "subject": str(self.actor_id),
                            "assertion": natural,
                        },
                        "natural": natural,
                        "embedding": make_embedding(natural),
                        "confidence": confidence,
                        "confidence_at_assertion": confidence,
                        "scope_actors": [str(self.actor_id)],
                        "scope_entities": [],
                        "supporting_model_ids": [
                            str(model_id)
                            for model_id in sorted(supporting[subject], key=str)
                        ],
                        "scope_temporal": {
                            "valid_from": datetime.now(timezone.utc).isoformat(),
                            "valid_until": None,
                        },
                        "falsifier": None,
                    },
                }
            )
        return json.dumps(
            {
                "trigger_ref": str(self.trigger_id),
                "tenant_id": str(self.tenant_id),
                "claim_ops": ops,
                "act_ops": [],
                "resource_ops": [],
                "new_predictions": [],
                "reasoning_trace": (
                    "generic compression of visible FACET records and selected "
                    "Model summaries; consumed_model_ids="
                    + ",".join(
                        str(model_id)
                        for model_id in sorted(
                            {mid for ids in supporting.values() for mid in ids},
                            key=str,
                        )
                    )
                ),
            }
        )


async def run(
    *, dsn: str, output: Path, experiment_version: str = "v3",
) -> dict[str, Any]:
    os.environ["INQUIRY_LLM_QUESTION_PLANNING_ENABLED"] = "0"
    os.environ["THINK_COMPILED_BATCH_MEMORY_REASONING"] = "0"
    bootstrap_conn = await asyncpg.connect(dsn)
    try:
        await apply_migrations_dir(bootstrap_conn, REPO_ROOT / "db" / "migrations")
    finally:
        await bootstrap_conn.close()
    # Register vector/json codecs only after migrations have installed their
    # database types; a pool opened before migration can retain text codecs.
    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=8, init=pgvector_pool_init
    )
    try:
        manifest = _manifest_for(experiment_version)
        consume_models = experiment_version == "v4-development"
        learned_batches, learned_models, learned_runs = await _run_learned(
            pool, consume_model_summaries=consume_models
        )
        frozen_batches, frozen_models, frozen_runs = await _run_frozen(
            pool, consume_model_summaries=consume_models
        )
        learned = _arm(
            "learned_memory", learned_batches, learned_models,
            await _runtime_runs(pool, learned_runs), manifest=manifest,
            producer_version=experiment_version,
        )
        frozen = _arm(
            "frozen_memory", frozen_batches, frozen_models,
            await _runtime_runs(pool, frozen_runs), manifest=manifest,
            producer_version=experiment_version,
        )
        report = evaluate_company_model_ablation(
            manifest=manifest, learned=learned, frozen=frozen
        )
        artifact = {
            "schema_version": "bounded-company-model-ablation-artifact-v1",
            "manifest": manifest,
            "learned_arm": learned,
            "frozen_arm": frozen,
            "evaluation": report,
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
        return artifact
    finally:
        await pool.close()


def _manifest_for(experiment_version: str) -> dict[str, Any]:
    if experiment_version not in {"v3", "v4-development"}:
        raise ValueError("experiment_version must be v3 or v4-development")
    manifest = deepcopy(MANIFEST)
    if experiment_version == "v4-development":
        manifest["experiment_id"] = "bounded-postfix-company-model-v4-development"
    return manifest


async def _run_learned(pool, *, consume_model_summaries: bool):
    tenant, actor = await _seed_identity(pool, "learned")
    batches, run_ids = [], []
    for index, definitions in enumerate(BATCHES, 1):
        logical, observations = await _insert_batch(pool, tenant, actor, index, definitions)
        run_ids.append(await _think_batch(
            pool, tenant, actor, index, observations,
            consume_model_summaries=consume_model_summaries,
        ))
        batches.append(logical)
    return batches, await _models(pool, [tenant]), run_ids


async def _run_frozen(pool, *, consume_model_summaries: bool):
    batches, tenants, run_ids = [], [], []
    for index, definitions in enumerate(BATCHES, 1):
        tenant, actor = await _seed_identity(pool, f"frozen-{index}")
        tenants.append(tenant)
        logical, observations = await _insert_batch(pool, tenant, actor, index, definitions)
        run_ids.append(await _think_batch(
            pool, tenant, actor, index, observations,
            consume_model_summaries=consume_model_summaries,
        ))
        batches.append(logical)
    return batches, await _models(pool, tenants), run_ids


async def _seed_identity(pool, suffix: str) -> tuple[UUID, UUID]:
    tenant, actor = uuid7(), uuid7()
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO tenants (id, name, is_demo) VALUES ($1,$2,FALSE)", tenant, f"ablation-{suffix}")
        await conn.execute("INSERT INTO actors (id,tenant_id,type,display_name,status) VALUES ($1,$2,'human_internal','Analyst','active')", actor, tenant)
    return tenant, actor


async def _insert_batch(pool, tenant, actor, index, definitions):
    logical, observations = [], []
    async with pool.acquire() as conn:
        for offset, (subject, facet) in enumerate(definitions, 1):
            logical_id = f"batch-{index:02d}-signal-{offset:02d}"
            text = f"Operational update [{logical_id}] [FACET subject={subject} value={facet}]"
            oid = uuid7()
            await conn.execute(
                """INSERT INTO observations
                (id,tenant_id,occurred_at,kind,source_channel,actor_id,content,
                 content_text,embedding,embedding_pending,trust_tier,external_id)
                VALUES ($1,$2,now(),'signal','simulated:normalized',$3,'{}'::jsonb,
                        $4,$5,FALSE,'authoritative',$6)""",
                oid, tenant, actor, text, make_embedding(text), logical_id,
            )
            logical.append(logical_id)
            observations.append((oid, text))
    return logical, observations


async def _think_batch(
    pool, tenant, actor, index, observations, *, consume_model_summaries: bool,
):
    trigger_id = uuid7()
    text = "Evidence window containing 6 source signals:\n" + "\n".join(
        f"- {value}" for _, value in observations
    )
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant, subkind="event_batch",
        observation_id=observations[0][0],
        observation_ids=[row[0] for row in observations],
        seed_natural_text=text,
        seed_occurred_at=datetime.now(timezone.utc), scope_actors=[actor],
        seed_signature={
            "trigger_id": str(trigger_id),
            "source_channels": ["simulated:normalized"],
            "batch_signal_fragments": [{"text": row[1]} for row in observations],
        },
    )
    provider = FacetCompressionProvider(
        trigger_id=trigger_id, tenant_id=tenant,
        event_id=observations[0][0], actor_id=actor,
        consume_model_summaries=consume_model_summaries,
    )
    outcome = await think(
        trigger, pool, llm_provider=provider,
        triggering_content=text, reason_for_trigger=f"sealed batch {index}",
    )
    if outcome.status != "success":
        raise RuntimeError(f"Think batch {index} failed: {outcome.error}")
    return str(outcome.run_id)


async def _models(pool, tenants):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT id,tenant_id,"natural",confidence FROM models WHERE tenant_id=ANY($1::uuid[]) AND status=\'active\' ORDER BY created_at,id',
            tenants,
        )
    return [dict(row) for row in rows]


async def _runtime_runs(pool, run_ids):
    ids = [UUID(value) for value in run_ids]
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT id,status,ops_applied
               FROM think_runs WHERE id=ANY($1::uuid[]) ORDER BY started_at""",
            ids,
        )
    out = []
    for row in rows:
        ops = row["ops_applied"] or {}
        if isinstance(ops, str):
            ops = json.loads(ops)
        context = ops.get("context_use") or {}
        out.append({
            "run_id": str(row["id"]), "status": row["status"],
            "selected_model_ids": context.get("selected_model_ids") or [],
            "referenced_model_ids": context.get("referenced_model_ids") or [],
            "selected_observation_ids": context.get("selected_observation_ids") or [],
            "referenced_observation_ids": context.get("referenced_observation_ids") or [],
        })
    return out


def _arm(
    name, batches, models, runtime_runs, *, manifest, producer_version,
):
    predictions = []
    for thesis in manifest["hidden_theses"]:
        subject = thesis["thesis_id"]
        candidates = [row for row in models if str(row["natural"]).startswith(f"{subject} evidence facets:")]
        best = max(candidates, key=lambda row: float(row["confidence"]), default=None)
        facets = (
            {part.strip() for part in str(best["natural"]).partition(":")[2].split(",")}
            if best else set()
        )
        recovered = bool(best) and all(any(value in facets for value in group) for group in thesis["required_groups"])
        confidence = float(best["confidence"]) if best else 0.0
        predictions.append({
            "thesis_id": subject, "recovered": recovered, "confidence": confidence,
            "future_outcomes": [1, 1, 1, 1],
            "runtime_model_id": str(best["id"]) if best else None,
        })
    return {
        "schema_version": "company-model-ablation-arm-v1", "arm": name,
        "producer_id": f"think-runtime-facet-compressor-{producer_version}",
        "truth_visible_to_producer": False,
        "hidden_truth_digest": manifest_digest(manifest),
        "batches": [
            {"batch_id": f"batch-{i:02d}", "signal_ids": list(signals)}
            for i, signals in enumerate(batches, 1)
        ],
        "predictions": predictions, "safety_incidents": [],
        "runtime_run_ids": [row["run_id"] for row in runtime_runs],
        "runtime_context_use": runtime_runs,
        "runtime_model_count": len(models),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"), required=False)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--experiment-version", choices=("v3", "v4-development"), default="v3"
    )
    args = parser.parse_args()
    if not args.dsn:
        raise SystemExit("DATABASE_URL or --dsn is required")
    artifact = asyncio.run(run(
        dsn=args.dsn, output=args.output,
        experiment_version=args.experiment_version,
    ))
    print(args.output)
    print(json.dumps(artifact["evaluation"], indent=2, sort_keys=True))
    if artifact["evaluation"]["verdict"] != "meets_policy":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
