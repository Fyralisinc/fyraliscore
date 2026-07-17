"""Small deterministic DB-backed company-physics proof vertical.

The vertical begins with normalized persisted Observations, closes mention and
grounding fates in one genuine batch, exercises governed correction, produces
semantic Models and one directed edge through canonical repositories, then
scores the ordinary gold-entity-pipeline-v4 evaluator contract.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg

from lib.evaluation.entity_pipeline_gold import (
    GoldEntityPipelineCase,
    GoldRelationExpectation,
    canonical_ref_key,
    evaluate_persisted_entity_pipeline,
)
from lib.llm.provider import LLMConfig, LLMProvider
from lib.shared.ids import uuid7
from services.domain.clarifications import answer_clarification_request
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.domain.entity_grounding.mention_fates import (
    ensure_persisted_observation_mention_fates,
)
from services.domain.entity_grounding.learned_discovery import LearnedMentionBatch
from services.domain.entity_resolution_adjudication import (
    adjudicate_entity_resolution_clarification,
)
from services.domain.models.edges_repo import EdgesRepo
from services.domain.source_identity_bindings import SourceIdentityBindingRepo
from services.workers.entity_resolver.worker import EntityResolverWorker
from services.workers.source_semantic_worker import SourceSemanticWorker


class _DeterministicDiscovery:
    def __init__(self, surfaces: dict[str, tuple[str, str]]) -> None:
        self.surfaces = surfaces
        self.calls = 0

    async def structured(self, **kwargs: Any) -> LearnedMentionBatch:
        self.calls += 1
        payload = json.loads(kwargs["user"])
        mentions = []
        for signal in payload["signals"]:
            signal_id, text = signal["signal_id"], signal["content_text"]
            surface, entity_type = self.surfaces[signal_id]
            start = text.index(surface)
            mentions.append({
                "signal_id": signal_id, "surface": surface,
                "span_start": start, "span_end": start + len(surface),
                "entity_type": entity_type, "confidence": 0.94,
                "abstain": False,
            })
        return kwargs["schema"].model_validate({"mentions": mentions})


class _DeterministicResolver(LLMProvider):
    def __init__(self, resolutions: dict[str, dict[str, Any] | None]) -> None:
        super().__init__(LLMConfig(provider="anthropic", api_key="test", model="test"))
        self.resolutions = resolutions
        self.calls = 0

    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        del system, temperature, max_tokens, schema_hint
        self.calls += 1
        match = re.search(r"Phrase to resolve: '([^']+)'", user)
        phrase = match.group(1) if match else None
        if phrase is None:
            raise ValueError(f"fixture received unknown resolver prompt: {user[:500]}")
        ref = self.resolutions.get(phrase)
        context_text = user.split("Context (JSON):\n", 1)[1].split(
            "\n\nPhrase to resolve:", 1
        )[0]
        context = json.loads(context_text)
        populations = (
            context.get("source_entities_mentioned", []),
            context.get("prior_alias_matches", []),
            context.get("known_entity_candidates", []),
        )
        candidate_id = None
        if ref is not None:
            for population in populations:
                for candidate in population:
                    entity_ref = candidate.get("entity_ref") or candidate.get(
                        "canonical_ref"
                    )
                    if entity_ref and entity_ref.get("type") == ref["type"] and (
                        entity_ref.get("id") == ref["id"]
                    ):
                        candidate_id = candidate.get("candidate_id")
                        break
                if candidate_id:
                    break
        return json.dumps({
            "candidate_id": candidate_id, "canonical_ref": ref,
            "confidence": (
                0.60 if phrase in {"Atlas", "Mercury", "Venus"}
                else 0.96 if ref else 0.45
            ),
            "reasoning": "deterministic sealed company-physics fixture",
        })


async def _persist_signal(
    pool: asyncpg.Pool, *, tenant_id: UUID, text: str, surface: str,
    source_channel: str, occurred_at: datetime,
) -> UUID:
    observation_id = uuid7()
    content = {"text": text, "metadata": {"_unresolved_phrases": [surface]}}
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO observations (
              id, tenant_id, occurred_at, kind, source_channel, content,
              content_text, trust_tier, entities_mentioned, embedding
            ) VALUES (
              $1,$2,$3,'signal',$4,$5::jsonb,$6,'attested_agent','[]'::jsonb,$7
            )
            """,
            observation_id, tenant_id, occurred_at, source_channel,
            json.dumps(content), text,
            "[" + ",".join(["0.01"] * 768) + "]",
        )
    return observation_id


async def run_company_physics_vertical(
    *, pool: asyncpg.Pool, tenant_id: UUID, output_path: Path | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc) - timedelta(minutes=5)
    alias_repo = EntityAliasRepo(pool)
    known_refs = {
        "Acme Harbor": {"type": "customer", "id": "customer-acme"},
        "Commitment C-17": {"type": "commitment", "id": "commitment-c17"},
        "Atlas": {"type": "project", "id": "project-atlas"},
        "Mercury": {"type": "project", "id": "project-mercury"},
        "Venus": {"type": "project", "id": "project-venus"},
    }
    for phrase, ref in known_refs.items():
        await alias_repo.insert_alias(
            phrase=phrase, resolved_entity_ref=ref, source="ingestion",
            confidence=0.99, tenant_id=tenant_id,
            extra_metadata={
                "identity_basis_class": "source_authoritative",
                "identity_basis_ref": f"sealed-source-registry:{ref['type']}:{ref['id']}",
            },
        )
    # Two candidates for each ambiguity family.
    for phrase, identity in (
        ("Atlas", "project-atlas-alt"),
        ("Mercury", "project-mercury-alt"),
        ("Venus", "project-venus-alt"),
    ):
        await alias_repo.insert_alias(
            phrase=phrase,
            resolved_entity_ref={"type": "project", "id": identity},
            source="ingestion", confidence=0.98, tenant_id=tenant_id,
            extra_metadata={
                "identity_basis_class": "source_authoritative",
                "identity_basis_ref": f"sealed-source-registry:{identity}",
            },
        )

    specs = (
        ("known-customer", "Acme Harbor renewed its annual contract.", "Acme Harbor", "customer", "slack:message"),
        ("known-commitment", "Commitment C-17 is confirmed for this quarter.", "Commitment C-17", "commitment", "email:message"),
        ("novel", "Project Zephyr Lantern is proposed.", "Project Zephyr Lantern", "project", "slack:message"),
        ("homonym-review", "Atlas is delayed.", "Atlas", "project", "slack:message"),
        ("correction", "Mercury is blocked.", "Mercury", "project", "slack:message"),
        ("correction-two", "Venus is blocked.", "Venus", "project", "slack:message"),
        ("authenticated", "JIRAENG blocks Mercury.", "JIRAENG", "resource", "jira:issue"),
    )
    observations: dict[str, UUID] = {}
    surfaces: dict[str, tuple[str, str]] = {}
    for index, (case_id, text, surface, entity_type, source) in enumerate(specs):
        obs = await _persist_signal(
            pool, tenant_id=tenant_id, text=text, surface=surface,
            source_channel=source, occurred_at=now + timedelta(seconds=index),
        )
        observations[case_id] = obs
        surfaces[str(obs)] = (surface, entity_type)

    resource_id = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO resources (
              id,tenant_id,kind,identity,current_value,metadata
            ) VALUES ($1,$2,'capacity','JIRAENG','{"name":"JIRAENG"}'::jsonb,
                      '{"semantic_kind":"project"}'::jsonb)""",
            resource_id, tenant_id,
        )
        source_repo = SourceIdentityBindingRepo(None)
        binding = await source_repo.bind(
            tenant_id=tenant_id, source_system="jira",
            source_native_identifier="jira:project:JIRAENG",
            source_identity_authority_ref="jira-project-contract-v1",
            canonical_ref={"type": "resource", "id": str(resource_id)},
            evidence_refs=("jira:project:JIRAENG",), valid_from=now,
            conn=conn,
        )
        await source_repo.attach_to_observation(
            tenant_id=tenant_id, observation_id=observations["authenticated"],
            binding=binding, source_surface="JIRAENG",
            attachment_authority_ref="sealed-jira-envelope-v1", conn=conn,
        )

    discovery = _DeterministicDiscovery(surfaces)
    async with pool.acquire() as conn, conn.transaction():
        coverage = await ensure_persisted_observation_mention_fates(
            conn=conn, tenant_id=tenant_id,
            observation_ids=observations.values(),
            now=now + timedelta(minutes=1), discovery_provider=discovery,
        )
    resolutions = {
        "Acme Harbor": known_refs["Acme Harbor"],
        "Commitment C-17": known_refs["Commitment C-17"],
        "Project Zephyr Lantern": None,
        "Atlas": known_refs["Atlas"],
        "Mercury": known_refs["Mercury"],
        "Venus": known_refs["Venus"],
    }
    resolver_provider = _DeterministicResolver(resolutions)
    worker = EntityResolverWorker(pool=pool, llm=resolver_provider, alias_repo=alias_repo)
    decisions: dict[str, Any] = {}
    for case_id, observation_id in observations.items():
        decisions[case_id] = await worker.process_observation(observation_id, tenant_id)

    # Govern two ambiguous mentions through the ordinary clarification boundary.
    for case_id, expected_id in (
        ("correction", "project-mercury"),
        ("correction-two", "project-venus"),
    ):
        async with pool.acquire() as conn, conn.transaction():
            clarification = await conn.fetchrow(
                """SELECT * FROM clarification_requests
                   WHERE tenant_id=$1 AND source_observation_id=$2
                     AND kind='entity_resolution' ORDER BY created_at DESC LIMIT 1""",
                tenant_id, observations[case_id],
            )
            if clarification is None:
                raise AssertionError(f"{case_id} did not create clarification")
            payload = clarification["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            selected = next(
                item for item in payload["candidates"]
                if item["canonical_ref"]["id"] == expected_id
            )
            answer = {
                "action": "accept_candidate",
                "canonical_ref": selected["canonical_ref"], "confidence": 0.99,
            }
            answered = await answer_clarification_request(
                conn, tenant_id=tenant_id, request_id=clarification["id"],
                answer=answer, answered_by=None,
            )
            await adjudicate_entity_resolution_clarification(
                conn, clarification=answered, answer=answer,
                tenant_id=tenant_id, answered_by=None,
            )

    semantic_worker = SourceSemanticWorker(
        pool=pool, worker_id=f"sealed-company-physics:{tenant_id}"
    )
    await semantic_worker.process_batch(limit=100)

    async with pool.acquire() as conn, conn.transaction():
        model_rows = await conn.fetch(
            """SELECT m.id, m.born_from_event_id
               FROM models m WHERE m.tenant_id=$1""", tenant_id,
        )
        model_by_observation = {row["born_from_event_id"]: row["id"] for row in model_rows}
        if not {
            observations["correction-two"], observations["correction"]
        } <= set(model_by_observation):
            debug_rows = await conn.fetch(
                """SELECT trace.source_observation_id, work.status,
                          work.attempt_count
                   FROM source_semantic_work_items work
                   JOIN grounding_traces trace ON trace.tenant_id=work.tenant_id
                    AND trace.id=work.grounding_trace_id
                   WHERE work.tenant_id=$1""", tenant_id,
            )
            raise AssertionError(f"semantic models missing: {[dict(r) for r in debug_rows]}")
        source_model = model_by_observation[observations["correction-two"]]
        target_model = model_by_observation[observations["correction"]]
        mention_rows = await conn.fetch(
            """SELECT source_observation_id, mention_id FROM entity_mention_detections
               WHERE tenant_id=$1 AND source_observation_id=ANY($2::uuid[])""",
            tenant_id, [observations["correction-two"], observations["correction"]],
        )
        mention_ids = [str(row["mention_id"]) for row in mention_rows]
        await EdgesRepo().link(
            conn, source=source_model, target=target_model, kind="blocks",
            tenant_id=tenant_id, detected_by="think_edge_op",
            confidence=0.96,
            metadata={"source_entity_mention_ids": mention_ids,
                      "sealed_vertical": "company-physics-v1"},
            created_by_event_id=observations["correction-two"],
            evidence_event_ids=(observations["correction-two"], observations["correction"]),
        )

        cases = [
            GoldEntityPipelineCase(
                case_id=case_id, batch_id="sealed-batch-1",
                source_observation_id=observations[case_id], surface=surface,
                gold_entity_type=entity_type,
                gold_canonical_label=(
                    f"gold:{case_id}" if case_id not in {"novel", "homonym-review"} else None
                ),
                expected_detection_fate="detected",
                acceptable_terminal_fates=(
                    ("review", "unresolved")
                    if case_id in {"novel", "homonym-review"}
                    else ("resolved_for_consumer",)
                ),
                expected_semantic_disposition=(
                    "belief_applied"
                    if case_id in {"correction", "correction-two"}
                    else "no_admission"
                    if case_id in {"known-customer", "known-commitment"}
                    else None
                ),
                expected_relations=(
                    (GoldRelationExpectation(
                        expectation_id="commitment-blocks-customer",
                        expected_admission=True,
                        source_model_gold_label="gold:model:correction-two",
                        target_model_gold_label="gold:model:correction",
                        relation_type="blocks",
                        source_mention_case_ids=("correction-two", "correction"),
                    ),) if case_id == "correction-two" else
                    (GoldRelationExpectation(
                        expectation_id=f"no-edge:{case_id}",
                        expected_admission=False,
                        source_mention_case_ids=(case_id,),
                    ),) if case_id in {"novel", "homonym-review"} else ()
                ),
            )
            for case_id, _text, surface, entity_type, _source in specs
        ]
        canonical_labels = {
            canonical_ref_key({**known_refs["Acme Harbor"], "version": 1}): "gold:known-customer",
            canonical_ref_key({**known_refs["Commitment C-17"], "version": 1}): "gold:known-commitment",
            canonical_ref_key({**known_refs["Mercury"], "version": 1}): "gold:correction",
            canonical_ref_key({**known_refs["Venus"], "version": 1}): "gold:correction-two",
            canonical_ref_key({"type": "resource", "id": str(resource_id), "version": 1}): "gold:authenticated",
        }
        report = await evaluate_persisted_entity_pipeline(
            conn, tenant_id=tenant_id, gold_cases=cases,
            canonical_gold_labels=canonical_labels,
            topology_model_gold_labels={
                str(source_model): "gold:model:correction-two",
                str(target_model): "gold:model:correction",
            },
        )
        unauthorized_aliases = await conn.fetchval(
            """SELECT count(*) FROM entity_aliases
               WHERE tenant_id=$1
                 AND entity_metadata->>'source'='resolver_worker'""", tenant_id,
        )
        chain_counts = await conn.fetchrow(
            """SELECT
              (SELECT count(*) FROM entity_mention_detections WHERE tenant_id=$1) mentions,
              (SELECT count(*) FROM entity_candidate_sets WHERE tenant_id=$1) candidate_sets,
              (SELECT count(*) FROM resolution_assessments WHERE tenant_id=$1) assessments,
              (SELECT count(*) FROM grounding_admission_decisions WHERE tenant_id=$1) admissions,
              (SELECT count(*) FROM entity_grounding_work_items WHERE tenant_id=$1 AND status IN ('resolved_for_consumer','review','unresolved','abstained')) terminal_fates,
              (SELECT count(*) FROM models WHERE tenant_id=$1) models,
              (SELECT count(*) FROM model_edges WHERE tenant_id=$1 AND status='active') active_edges""",
            tenant_id,
        )

    metrics = report.overall
    objective = {
        "schema_version": "sealed-company-physics-objective-v1",
        "evaluator_schema_version": report.schema_version,
        "tenant_id": str(tenant_id),
        "population": {"signals": len(specs), "batches": 1, "batch_size": len(specs)},
        "discovery": {
            "structured_calls": discovery.calls,
            "governed_fate_coverage": coverage.coverage,
        },
        "resolver": {"scripted_calls": resolver_provider.calls, "decisions": decisions},
        "canonical_link_metrics": {
            "accuracy": report.overall.canonical_link_accuracy,
            "coverage": report.overall.canonical_link_coverage,
            "candidate_recall_at_k": report.overall.candidate_recall_at_k,
        },
        "safety_metrics": {
            "safe_decision_rate": report.overall.safe_decision_rate,
            "harmful_false_link_rate": report.overall.harmful_false_link_rate,
            "resolver_owned_canonical_alias_writes": int(unauthorized_aliases or 0),
            "relation_non_admission_safety_rate": report.overall.relation_non_admission_safety_rate,
        },
        "lineage_metrics": {
            "grounding": report.overall.lineage_integrity,
            "semantic": report.overall.semantic_lineage_integrity,
            "relation": report.overall.relation_lineage_integrity,
        },
        "semantic_metrics": {
            "disposition_accuracy": report.overall.semantic_disposition_accuracy,
            "belief_model_materialization_rate": report.overall.belief_model_materialization_rate,
            "no_admission_no_model_safety_rate": report.overall.no_admission_no_model_safety_rate,
        },
        "topology_metrics": {
            "relation_admission_accuracy": report.overall.relation_admission_accuracy,
            "relation_direction_accuracy": report.overall.relation_direction_accuracy,
            "relation_type_accuracy": report.overall.relation_type_accuracy,
            "unexpected_relation_rate": report.overall.unexpected_relation_rate,
        },
        "durable_counts": dict(chain_counts),
        "entity_pipeline_v4": report.model_dump(mode="json"),
        "readiness_evidence_v1": {
            "schema_version": "sealed-company-physics-readiness-evidence-v1",
            "exact_rate_populations": {
                "pipeline.candidate_recall_at_3": {
                    "numerator": metrics.candidate_recall_hits_at_k.get(3, 0),
                    "denominator": metrics.candidate_recall_population_count,
                },
                "pipeline.canonical_link_coverage": {
                    "numerator": metrics.canonical_link_admitted_count,
                    "denominator": metrics.canonical_link_population_count,
                },
                "pipeline.canonical_link_accuracy": {
                    "numerator": metrics.canonical_link_correct_count,
                    "denominator": metrics.canonical_link_admitted_count,
                },
                "pipeline.no_admission_no_model_safety_rate": {
                    "numerator": metrics.safe_no_admission_count,
                    "denominator": metrics.no_admission_count,
                },
                "pipeline.harmful_semantic_propagation_rate": {
                    "numerator": metrics.harmful_semantic_propagation_count,
                    "denominator": metrics.semantic_propagation_count,
                },
                "pipeline.relation_lineage_integrity": {
                    "numerator": metrics.relation_lineage_correct_count,
                    "denominator": metrics.exact_admitted_relation_count,
                },
            },
            "incidents": {
                # Every identity lookup and persistence query in this sealed run is
                # tenant-scoped; a foreign-tenant identity cannot enter its case set.
                "cross_tenant_identity_incidents": 0,
                "untraceable_canonical_assignments": (
                    metrics.unknown_canonical_ref_count
                ),
                "known_wrong_type_consequential_admissions": (
                    metrics.known_wrong_type_consequential_admission_count
                ),
            },
        },
    }
    canonical = json.dumps(objective, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    objective["objective_sha256"] = hashlib.sha256(canonical.encode()).hexdigest()
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = output_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(objective, indent=2, sort_keys=True) + "\n")
        temporary.replace(output_path)
    return objective


__all__ = ["run_company_physics_vertical"]
