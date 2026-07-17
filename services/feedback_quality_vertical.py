"""Matched DB proof that governed correction improves later company-model quality."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.llm.provider import LLMConfig, LLMProvider
from lib.shared.ids import uuid7
from scripts.run_bounded_company_model_ablation_db import (
    _insert_batch,
    _runtime_runs,
    _think_batch,
)
from services.company_physics_vertical import run_company_physics_vertical
from services.domain.correction_propagation.service import CorrectionPropagationService
from services.domain.models.repo import pgvector_pool_init


LATER_BATCHES = (
    (("blocked_project", "review"), ("blocked_project", "current")),
    (("blocked_project", "followup"), ("blocked_project", "status")),
    (("blocked_project", "decision"), ("blocked_project", "latest")),
)
_CANDIDATE = re.compile(r"<candidate>\s*(.*?)\s*</candidate>", re.I | re.S)
_CANDIDATE_ID = re.compile(r"candidate_id:\s*\"?([^\"\s,]+)", re.I)
_MODEL_EVIDENCE = re.compile(
    r"([0-9a-f]{8}-[0-9a-f-]{27,36})[^\n]{0,1200}?"
    r"(Venus is blocked\.|Mercury is blocked\.)",
    re.I,
)
_FACET_VALUE = re.compile(
    r"\[FACET subject=blocked_project value=([a-z0-9_-]+)\]", re.I
)


class _CorrectedCompanyModelProvider(LLMProvider):
    """Sealed judge-independent consumer of the Models visible to Think."""

    def __init__(self) -> None:
        super().__init__(LLMConfig(
            provider="deterministic", api_key="none",
            model="corrected-company-model-consumer-v1",
        ))

    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        del system, temperature, max_tokens, schema_hint
        visible_models = [
            (model_id, natural.strip())
            for model_id, natural in _MODEL_EVIDENCE.findall(user)
        ]
        lower = user.lower()
        # The contested predecessor wins while it remains visible. Governed
        # correction removes it from active retrieval in the adaptive arm.
        conclusion = "venus" if "venus is blocked" in lower else (
            "mercury" if "mercury is blocked" in lower else "unknown"
        )
        evidence_window = ",".join(sorted(set(_FACET_VALUE.findall(user))))
        candidates = []
        for block in _CANDIDATE.findall(user):
            match = _CANDIDATE_ID.search(block)
            if match:
                candidates.append(match.group(1).rstrip(","))
        decisions = []
        for index, candidate_id in enumerate(candidates):
            if index == 0:
                decisions.append({
                    "candidate_id": candidate_id,
                    "decision": "accept",
                    "operation": "claim",
                    "confidence": 0.82 if conclusion != "unknown" else 0.51,
                    "claim_role": "fact",
                    "claim_text": (
                        f"blocked_project conclusion: {conclusion}; "
                        f"evidence_window={evidence_window}"
                    ),
                    "reason": "Conclusion derived from currently visible company Models.",
                })
            else:
                decisions.append({
                    "candidate_id": candidate_id, "decision": "reject",
                    "operation": "no_op", "confidence": 0.6,
                    "claim_role": "fact", "reason": "One conclusion per batch.",
                })
        referenced = sorted({
            model_id
            for model_id, natural in visible_models
            if (
                (conclusion == "venus" and "venus is blocked" in natural.lower())
                or (
                    conclusion == "mercury"
                    and "mercury is blocked" in natural.lower()
                )
            )
        })
        return json.dumps({
            "decisions": decisions,
            "reasoning_trace": "Consumed visible company Models: " + ", ".join(referenced),
        })


async def run_feedback_quality_vertical(
    *, dsn: str, output_path: Path | None = None,
) -> dict[str, Any]:
    arms = {
        "adaptive": {"tenant_id": uuid7(), "actor_id": uuid7()},
        "frozen": {"tenant_id": uuid7(), "actor_id": uuid7()},
    }
    for name, arm in arms.items():
        pool = await asyncpg.create_pool(
            dsn, min_size=1, max_size=8, init=_json_pool_init
        )
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO tenants(id,name,is_demo) VALUES($1,$2,FALSE)",
                    arm["tenant_id"], f"feedback-quality-{name}-{arm['tenant_id']}",
                )
                await conn.execute(
                    """INSERT INTO actors(id,tenant_id,type,display_name,status)
                       VALUES($1,$2,'human_internal','Analyst','active')""",
                    arm["actor_id"], arm["tenant_id"],
                )
            arm["physics"] = await run_company_physics_vertical(
                pool=pool, tenant_id=arm["tenant_id"]
            )
            arm.update(await _load_correction_fixture(pool, arm["tenant_id"]))
        finally:
            await pool.close()

    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=8, init=pgvector_pool_init
    )
    try:
        adaptive = arms["adaptive"]
        async with pool.acquire() as conn, conn.transaction():
            correction = await CorrectionPropagationService().propagate_direct_correction(
                conn,
                tenant_id=adaptive["tenant_id"],
                predecessor_grounding_trace_id=adaptive["wrong_trace_id"],
                successor_grounding_trace_id=adaptive["correct_trace_id"],
                cause_event_id=adaptive["correct_observation_id"],
                corrected_model_id=adaptive["correct_model_id"],
            )
        adaptive["correction"] = correction

        for arm in arms.values():
            run_ids = []
            logical_batches = []
            for index, definitions in enumerate(LATER_BATCHES, 1):
                logical, observations = await _insert_batch(
                    pool, arm["tenant_id"], arm["actor_id"], index, definitions
                )
                logical_batches.append(logical)
                run_ids.append(await _think_batch(
                    pool, arm["tenant_id"], arm["actor_id"], index, observations,
                    consume_model_summaries=True,
                    provider_factory=_CorrectedCompanyModelProvider,
                ))
            arm["logical_batches"] = logical_batches
            arm["runtime"] = await _runtime_runs(pool, run_ids)
            arm["outcome"] = await _load_arm_outcome(pool, arm)

        artifact = _evaluate(arms)
        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = output_path.with_suffix(".tmp")
            temporary.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
            temporary.replace(output_path)
        return artifact
    finally:
        await pool.close()


async def _load_correction_fixture(pool, tenant_id: UUID) -> dict[str, Any]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT gt.id AS trace_id, gt.source_observation_id AS observation_id,
                      a.admitted_model_id AS model_id, o.content_text
               FROM grounding_traces gt
               JOIN source_semantic_interpretations i
                 ON i.tenant_id=gt.tenant_id AND i.grounding_trace_id=gt.id
               JOIN source_semantic_admission_decisions a
                 ON a.tenant_id=i.tenant_id AND a.interpretation_id=i.id
               JOIN observations o
                 ON o.tenant_id=gt.tenant_id AND o.id=gt.source_observation_id
               WHERE gt.tenant_id=$1
                 AND o.content_text IN ('Venus is blocked.','Mercury is blocked.')
                 AND a.admitted_model_id IS NOT NULL""",
            tenant_id,
        )
    by_text = {row["content_text"]: row for row in rows}
    wrong, correct = by_text["Venus is blocked."], by_text["Mercury is blocked."]
    return {
        "wrong_trace_id": wrong["trace_id"],
        "wrong_observation_id": wrong["observation_id"],
        "wrong_model_id": wrong["model_id"],
        "correct_trace_id": correct["trace_id"],
        "correct_observation_id": correct["observation_id"],
        "correct_model_id": correct["model_id"],
    }


async def _load_arm_outcome(pool, arm: dict[str, Any]) -> dict[str, Any]:
    tenant_id = arm["tenant_id"]
    async with pool.acquire() as conn:
        models = await conn.fetch(
            """SELECT id,"natural",status,supporting_model_ids,born_from_event_id
               FROM models WHERE tenant_id=$1 ORDER BY created_at,id""",
            tenant_id,
        )
        edge = await conn.fetchrow(
            """SELECT id,status,status_reason,metadata FROM model_edges
               WHERE tenant_id=$1 AND source_model_id=$2 AND target_model_id=$3
               ORDER BY created_at LIMIT 1""",
            tenant_id, arm["wrong_model_id"], arm["correct_model_id"],
        )
        reeval = await conn.fetchval(
            """SELECT count(*) FROM model_reeval_queue
               WHERE tenant_id=$1 AND cause_model_id=$2""",
            tenant_id, arm["wrong_model_id"],
        )
        foreign_selected = await conn.fetchval(
            """SELECT count(*) FROM models
               WHERE tenant_id<>$1 AND id=ANY($2::uuid[])""",
            tenant_id,
            [UUID(value) for run in arm["runtime"] for value in run["selected_model_ids"]],
        )
        observation_texts = await conn.fetchval(
            """SELECT array_agg(content_text ORDER BY content_text)
               FROM observations WHERE tenant_id=$1
                 AND source_channel IN (
                   'slack:message','email:message','jira:issue','simulated:normalized'
                 )""",
            tenant_id,
        )
        observation_digest = hashlib.sha256(
            "|".join(observation_texts or []).encode()
        ).hexdigest()
    conclusions = [
        row for row in models
        if str(row["natural"]).startswith("blocked_project conclusion:")
    ]
    return {
        "models": [{
            "id": str(row["id"]), "natural": row["natural"], "status": row["status"],
            "supporting_model_ids": [str(value) for value in row["supporting_model_ids"]],
            "born_from_event_id": str(row["born_from_event_id"]),
        } for row in models],
        "correct_conclusions": sum(
            "conclusion: mercury;" in row["natural"] for row in conclusions
        ),
        "wrong_conclusions": sum(
            "conclusion: venus;" in row["natural"] for row in conclusions
        ),
        "conclusion_count": len(conclusions),
        "wrong_model_status": next(
            row["status"] for row in models if row["id"] == arm["wrong_model_id"]
        ),
        "correct_model_status": next(
            row["status"] for row in models if row["id"] == arm["correct_model_id"]
        ),
        "relation_status": edge["status"] if edge else None,
        "relation_status_reason": edge["status_reason"] if edge else None,
        "relation_lineaged": bool(edge and _json(edge["metadata"])),
        "reevaluation_count": int(reeval or 0),
        "foreign_selected_models": int(foreign_selected or 0),
        "observation_content_sha256": observation_digest,
    }


def _evaluate(arms: dict[str, dict[str, Any]]) -> dict[str, Any]:
    adaptive, frozen = arms["adaptive"], arms["frozen"]
    adaptive_out, frozen_out = adaptive["outcome"], frozen["outcome"]
    n = len(LATER_BATCHES)
    adaptive_quality = adaptive_out["correct_conclusions"] / max(
        1, adaptive_out["conclusion_count"]
    )
    frozen_quality = frozen_out["correct_conclusions"] / max(
        1, frozen_out["conclusion_count"]
    )
    quality_lift = adaptive_quality - frozen_quality
    checks = {
        "matched_later_batches": adaptive["logical_batches"] == frozen["logical_batches"],
        "adaptive_correction_archived_wrong_model": adaptive_out["wrong_model_status"] != "active",
        "adaptive_correct_model_remains_active": adaptive_out["correct_model_status"] == "active",
        "frozen_wrong_model_remains_active": frozen_out["wrong_model_status"] == "active",
        "adaptive_relation_fenced": adaptive_out["relation_status"] != "active",
        "frozen_relation_unchanged": frozen_out["relation_status"] == "active",
        "adaptive_correction_lineage_exact": (
            adaptive["wrong_model_id"] in adaptive["correction"].old_model_ids
            and adaptive["wrong_model_id"]
            in adaptive["correction"].archived_model_ids
        ),
        "later_quality_improves": quality_lift > 0,
        "adaptive_later_quality_is_correct": adaptive_quality == 1.0,
        "both_arms_emit_later_models": (
            adaptive_out["conclusion_count"] > 0
            and frozen_out["conclusion_count"] > 0
        ),
        "frozen_preserves_negative_control": (
            frozen_out["wrong_conclusions"] == frozen_out["conclusion_count"]
        ),
        "selected_models_are_tenant_isolated": (
            adaptive_out["foreign_selected_models"] == 0
            and frozen_out["foreign_selected_models"] == 0
        ),
        "source_truth_is_immutable_and_matched": (
            adaptive_out["observation_content_sha256"]
            == frozen_out["observation_content_sha256"]
        ),
        "all_think_runs_succeed": all(
            run["status"] == "success"
            for arm in arms.values() for run in arm["runtime"]
        ),
        "later_models_reference_selected_context": all(
            run["referenced_model_ids"]
            and set(run["referenced_model_ids"]).issubset(run["selected_model_ids"])
            for arm in arms.values() for run in arm["runtime"]
        ),
        "adaptive_reasoning_lineage_corrected_model": all(
            str(adaptive["wrong_model_id"]) not in run["referenced_model_ids"]
            for run in adaptive["runtime"]
        ) and any(
            str(adaptive["correct_model_id"]) in run["referenced_model_ids"]
            for run in adaptive["runtime"]
        ),
        "frozen_reasoning_lineage_wrong_model": any(
            str(frozen["wrong_model_id"]) in run["referenced_model_ids"]
            for run in frozen["runtime"]
        ),
    }
    score = sum(float(value) for value in checks.values()) / len(checks)
    artifact: dict[str, Any] = {
        "schema_version": "feedback-quality-matched-db-objective-v1",
        "population": {
            "arms": 2, "later_batches_per_arm": n,
            "signals_per_later_batch": len(LATER_BATCHES[0]),
            "correction_episodes": 1,
        },
        "measurements": {
            "adaptive_later_quality": adaptive_quality,
            "frozen_later_quality": frozen_quality,
            "adaptive_minus_frozen_quality": quality_lift,
            "adaptive_wrong_conclusion_rate": adaptive_out["wrong_conclusions"] / max(
                1, adaptive_out["conclusion_count"]
            ),
            "frozen_wrong_conclusion_rate": frozen_out["wrong_conclusions"] / max(
                1, frozen_out["conclusion_count"]
            ),
            "continuous_score": score,
        },
        "checks": checks,
        "arms": {
            name: {
                "tenant_id": str(arm["tenant_id"]),
                "wrong_model_id": str(arm["wrong_model_id"]),
                "correct_model_id": str(arm["correct_model_id"]),
                "runtime": arm["runtime"],
                "outcome": arm["outcome"],
                "correction_lineage": (
                    {
                        "old_model_ids": [
                            str(value) for value in arm["correction"].old_model_ids
                        ],
                        "archived_model_ids": [
                            str(value)
                            for value in arm["correction"].archived_model_ids
                        ],
                    }
                    if name == "adaptive" else None
                ),
            } for name, arm in arms.items()
        },
        "continuous_score": score,
        "verdict": "meets_policy" if all(checks.values()) else "below_policy",
        "proof_boundary": [
            "bounded matched synthetic company world after connector transport",
            "sealed deterministic company-model consumer; production retrieval, Think apply, correction, graph fencing, and persistence paths",
            "does not establish open-world semantic judgment or task autonomy",
        ],
    }
    artifact["objective_sha256"] = canonical_sha256(artifact)
    return artifact


def _json(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    return value if isinstance(value, dict) else {}


async def _json_pool_init(conn: asyncpg.Connection) -> None:
    for type_name in ("json", "jsonb"):
        await conn.set_type_codec(
            type_name,
            encoder=lambda value: (
                json.dumps(value) if not isinstance(value, str) else value
            ),
            decoder=json.loads,
            schema="pg_catalog",
        )


__all__ = ["run_feedback_quality_vertical"]
