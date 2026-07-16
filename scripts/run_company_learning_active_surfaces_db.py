#!/usr/bin/env python3
"""Run active structured-identity and source-salience proofs on Postgres."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_active_surfaces import (
    ActiveLearningSurfacesEvidence,
    SEALED_ACTIVE_SURFACE_CLAIMS,
    SourceSalienceObservation,
    StructuredIdentityClaimContract,
    StructuredIdentitySurfaceObservation,
    evaluate_active_learning_surfaces,
)
from lib.shared.ids import uuid7
from services.domain.source_identity_bindings import SourceIdentityBindingRepo
from services.ingest.ingestion.core import ingest
from services.ingest.ingestion.handlers.gmail import handle_gmail
from services.ingest.ingestion.handlers.google_drive import (
    handle_google_drive_file,
)
from services.ingest.ingestion.handlers.jira import handle_jira_issue
from services.ingest.ingestion.handlers.linear import handle_linear_webhook
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.sage.company_profile import load_company_learning_profile
from services.reasoning.sage.retrieval_policy import plan_primary_retrieval
from services.workers.entity_resolver.context import build_context


ARTIFACT_NAME = "company_learning_active_surfaces_evidence.json"


async def run_active_surfaces_experiment(
    *,
    pool: asyncpg.Pool,
    output_dir: Path,
    run_id: str,
    system_version: str,
) -> ActiveLearningSurfacesEvidence:
    identity = tuple(
        [
            await _identity_case(pool, case_id)
            for case_id in (
                "jira_project",
                "linear_issue_bundle",
                "google_drive_file",
                "google_drive_comment",
                "google_drive_revision",
                "gmail_thread",
            )
        ]
    )
    salience = await _salience_cases(pool)
    report = evaluate_active_learning_surfaces(
        identity_observations=identity,
        salience_observations=salience,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    evidence = ActiveLearningSurfacesEvidence(
        run_id=run_id,
        system_version=system_version,
        created_at=datetime.now(timezone.utc).isoformat(),
        identity_observations=identity,
        salience_observations=salience,
        report=report,
        artifact_refs=(f"artifact:{(output_dir / ARTIFACT_NAME).resolve()}",),
    )
    (output_dir / ARTIFACT_NAME).write_text(
        json.dumps(evidence.artifact_payload(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return evidence


async def _identity_case(
    pool: asyncpg.Pool,
    case_id: str,
) -> StructuredIdentitySurfaceObservation:
    tenant_id, foreign_tenant = uuid7(), uuid7()
    await pool.executemany(
        "INSERT INTO tenants (id) VALUES ($1)",
        [(tenant_id,), (foreign_tenant,)],
    )
    payload, handler, expected_claims = _identity_contract(case_id)
    before_handler = await _binding_count(pool, tenant_id)
    draft = await handler(payload, {})
    handler_created = await _binding_count(pool, tenant_id) != before_handler
    observed_claims = tuple(
        StructuredIdentityClaimContract(
            source_system=claim.source_system,
            source_native_identifier=claim.source_native_identifier,
            source_surface=claim.source_surface,
            claim_authority_ref=claim.claim_authority_ref,
        )
        for claim in draft.source_identity_claims
    )
    unique_contracts = {
        (claim.source_system, claim.source_native_identifier): claim
        for claim in expected_claims
    }
    wrong_source_binding_ids: set[str] = set()
    for claim in unique_contracts.values():
        _, binding_id = await _seed_source_binding(
            pool,
            tenant_id=tenant_id,
            source_system=f"wrong-source:{claim.source_system}",
            source_native_identifier=claim.source_native_identifier,
            source_surface=claim.source_surface,
        )
        wrong_source_binding_ids.add(binding_id)
        await _seed_source_binding(
            pool,
            tenant_id=foreign_tenant,
            source_system=claim.source_system,
            source_native_identifier=claim.source_native_identifier,
            source_surface=claim.source_surface,
        )
    before_ingest = await _binding_count(pool, tenant_id)
    missing = await ingest(
        draft.source_channel,
        payload,
        pool=pool,
        tenant_id=tenant_id,
        embedder=None,
        enqueue_trigger=False,
    )
    ingest_created = await _binding_count(pool, tenant_id) != before_ingest
    missing_attachments = await pool.fetch(
        """
        SELECT attachment.binding_id, binding.tenant_id, binding.source_system
        FROM observation_source_identity_bindings attachment
        JOIN source_identity_bindings binding
          ON binding.tenant_id=attachment.tenant_id
         AND binding.id=attachment.binding_id
         AND binding.binding_version=attachment.binding_version
        WHERE attachment.tenant_id=$1 AND attachment.observation_id=$2
        """,
        tenant_id,
        missing.observation.id,
    )
    missing_authoritative = bool(missing_attachments)
    cross_source_leak = any(
        str(row["binding_id"]) in wrong_source_binding_ids
        for row in missing_attachments
    )
    foreign_contexts = await _contexts_for_claims(
        pool=pool,
        tenant_id=foreign_tenant,
        observation_id=missing.observation.id,
        claims=expected_claims,
    )
    cross_tenant_leak = any(
        context.source_identity_binding is not None
        for context in foreign_contexts
    )
    resource_ids: dict[tuple[str, str], UUID] = {}
    for key, claim in unique_contracts.items():
        resource_id, _ = await _seed_source_binding(
            pool,
            tenant_id=tenant_id,
            source_system=claim.source_system,
            source_native_identifier=claim.source_native_identifier,
            source_surface=claim.source_surface,
        )
        resource_ids[key] = resource_id
    replay = await ingest(
        draft.source_channel,
        payload,
        pool=pool,
        tenant_id=tenant_id,
        embedder=None,
        enqueue_trigger=False,
    )
    snapshot_before = await _observation_snapshot(
        pool, tenant_id, replay.observation.id
    )
    attached_surfaces = {
        str(row["normalized_source_surface"])
        for row in await pool.fetch(
            """
            SELECT normalized_source_surface
            FROM observation_source_identity_bindings
            WHERE tenant_id=$1 AND observation_id=$2
            """,
            tenant_id,
            replay.observation.id,
        )
    }
    exact_contexts = await _contexts_for_claims(
        pool=pool,
        tenant_id=tenant_id,
        observation_id=replay.observation.id,
        claims=expected_claims,
    )
    forged = await build_context(
        pool=pool,
        tenant_id=tenant_id,
        observation_id=replay.observation.id,
        phrase="SALES",
    )
    snapshot_after = await _observation_snapshot(pool, tenant_id, replay.observation.id)
    return StructuredIdentitySurfaceObservation(
        case_id=case_id,
        expected_claims=expected_claims,
        observed_claims=observed_claims,
        claim_emitted=bool(observed_claims),
        claim_preserved=all(
            claim.source_surface
            in replay.observation.content.get("_unresolved_phrases", [])
            for claim in expected_claims
        ),
        preexisting_binding_attached=all(
            " ".join(claim.source_surface.casefold().split())
            in attached_surfaces
            and context.source_identity_binding is not None
            and context.source_identity_binding.canonical_ref["id"]
            == str(
                resource_ids[
                    (claim.source_system, claim.source_native_identifier)
                ]
            )
            for claim, context in zip(
                expected_claims,
                exact_contexts,
                strict=True,
            )
        ),
        handler_created_authority=handler_created,
        ingest_created_authority=ingest_created,
        forged_text_resolved=forged.source_identity_binding is not None,
        missing_binding_authoritative=missing_authoritative,
        cross_source_leak=cross_source_leak,
        cross_tenant_leak=cross_tenant_leak,
        source_observation_immutable=snapshot_before == snapshot_after,
        artifact_refs=(
            f"observation:{replay.observation.id}",
            *(
                f"resource:{resource_id}"
                for resource_id in resource_ids.values()
            ),
        ),
    )


async def _contexts_for_claims(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    observation_id: UUID,
    claims: tuple[StructuredIdentityClaimContract, ...],
) -> tuple[Any, ...]:
    return tuple(
        [
            await build_context(
                pool=pool,
                tenant_id=tenant_id,
                observation_id=observation_id,
                phrase=claim.source_surface,
            )
            for claim in claims
        ]
    )


def _identity_contract(
    case_id: str,
) -> tuple[
    dict[str, Any],
    Any,
    tuple[StructuredIdentityClaimContract, ...],
]:
    if case_id == "jira_project":
        return (
            _jira_payload(),
            handle_jira_issue,
            SEALED_ACTIVE_SURFACE_CLAIMS[case_id],
        )
    if case_id.startswith("linear_"):
        return (
            _linear_payload(),
            handle_linear_webhook,
            SEALED_ACTIVE_SURFACE_CLAIMS[case_id],
        )
    if case_id.startswith("google_drive_"):
        record_type = case_id.removeprefix("google_drive_")
        return (
            _google_drive_payload(record_type),
            handle_google_drive_file,
            SEALED_ACTIVE_SURFACE_CLAIMS[case_id],
        )
    if case_id == "gmail_thread":
        return (
            _gmail_payload(),
            handle_gmail,
            SEALED_ACTIVE_SURFACE_CLAIMS[case_id],
        )
    raise ValueError(f"unknown active identity case: {case_id}")


async def _salience_cases(
    pool: asyncpg.Pool,
) -> tuple[SourceSalienceObservation, ...]:
    tenant_id, foreign_tenant = uuid7(), uuid7()
    await pool.executemany(
        "INSERT INTO tenants (id) VALUES ($1)",
        [(tenant_id,), (foreign_tenant,)],
    )
    async with pool.acquire() as conn, conn.transaction():
        wrong_trace, wrong_model = await _seed_outcome(
            conn, tenant_id, "slack:corrected"
        )
        await conn.execute(
            """
            UPDATE models SET status='archived', archived_at=now(),
              archive_reason='superseded' WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            wrong_model,
        )
        await _seed_outcome(
            conn,
            tenant_id,
            "slack:corrected",
            supersedes_trace_id=wrong_trace,
        )
        for _ in range(3):
            await _seed_outcome(conn, tenant_id, "slack:useful")
        for _ in range(2):
            await _seed_outcome(conn, tenant_id, "slack:pending", disposition=None)
        for _ in range(3):
            await _seed_outcome(conn, foreign_tenant, "slack:foreign")
        models_before = await conn.fetch(
            "SELECT id, proposition, status, archived_at FROM models WHERE tenant_id=$1 ORDER BY id",
            tenant_id,
        )
        traces_before = await conn.fetch(
            "SELECT id, current_fate, trace FROM grounding_traces WHERE tenant_id=$1 ORDER BY id",
            tenant_id,
        )
        profile = await load_company_learning_profile(conn, tenant_id=tenant_id)
        models_after = await conn.fetch(
            "SELECT id, proposition, status, archived_at FROM models WHERE tenant_id=$1 ORDER BY id",
            tenant_id,
        )
        traces_after = await conn.fetch(
            "SELECT id, current_fate, trace FROM grounding_traces WHERE tenant_id=$1 ORDER BY id",
            tenant_id,
        )
    immutable_models = models_before == models_after
    immutable_traces = traces_before == traces_after
    return tuple(
        _salience_observation(
            profile=profile,
            tenant_id=tenant_id,
            case_id=case_id,
            source=source,
            immutable_models=immutable_models,
            immutable_traces=immutable_traces,
        )
        for case_id, source in (
            ("settled_useful", "slack:useful"),
            ("corrected", "slack:corrected"),
            ("pending", "slack:pending"),
            ("foreign_tenant", "slack:foreign"),
            ("profile_load", "slack:profile-load"),
        )
    )


def _salience_observation(
    *,
    profile,
    tenant_id: UUID,
    case_id: str,
    source: str,
    immutable_models: bool,
    immutable_traces: bool,
) -> SourceSalienceObservation:
    baseline = _policy(tenant_id, source, None).decision_for("L")
    learned = _policy(tenant_id, source, profile).decision_for("L")
    assert baseline is not None and learned is not None
    prior = profile.best_prior(kind="source_reliability", key=source)
    return SourceSalienceObservation(
        case_id=case_id,
        baseline_salience=baseline.weight_multiplier,
        learned_salience=learned.weight_multiplier,
        credit_observed=prior is not None and prior.effective_score > 0.0,
        foreign_tenant_learned=(case_id == "foreign_tenant" and prior is not None),
        canonical_truth_immutable=immutable_models,
        grounding_truth_immutable=immutable_traces,
        artifact_refs=(f"profile-source:{source}",),
    )


def _policy(tenant_id: UUID, source: str, profile):
    return plan_primary_retrieval(
        trigger=TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_natural_text="Customer launch dependency",
            seed_signature={"source_channel": source},
        ),
        weights={"A": 0.30, "B": 0.26, "L": 0.12, "C": 0.16, "G": 0.16},
        effective_seed_entities=[],
        effective_scope_actors=[],
        projection_enabled=True,
        semantic_terms_enabled=True,
        semantic_k=20,
        exploration_rate=0.0,
        company_profile=profile,
    )


async def _seed_outcome(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    source: str,
    *,
    supersedes_trace_id: UUID | None = None,
    disposition: str | None = "belief_applied",
) -> tuple[UUID, UUID | None]:
    observation_id, snapshot_id, assessment_id, trace_id = (
        uuid7(),
        uuid7(),
        uuid7(),
        uuid7(),
    )
    selected = {"type": "customer", "id": str(uuid7()), "version": 1}
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel,
          content, content_text, trust_tier
        ) VALUES ($1,$2,now(),'signal',$3,'{}'::jsonb,'source outcome','authoritative')
        """,
        observation_id,
        tenant_id,
        source,
    )
    await conn.execute(
        """
        INSERT INTO interpretation_context_snapshots (
          id, tenant_id, focal_observation_id, phrase, source_channel,
          source_space, evidence_cutoff, processing_authority_fingerprint,
          snapshot_content_hash, snapshot
        ) VALUES ($1,$2,$3,'customer',$4,'evaluation',now(),$5,$6,'{}'::jsonb)
        """,
        snapshot_id,
        tenant_id,
        observation_id,
        source,
        canonical_sha256({"authority": str(snapshot_id)}),
        canonical_sha256({"snapshot": str(snapshot_id)}),
    )
    candidate_request_id, candidate_set_id = uuid7(), uuid7()
    request_digest = canonical_sha256(
        {"tenant_id": tenant_id, "request_id": candidate_request_id}
    )
    await conn.execute(
        """
        INSERT INTO entity_candidate_generation_requests (
          id, tenant_id, context_snapshot_id, source_observation_id,
          phrase, mention_ref, request_digest,
          processing_authority_fingerprint, required_lanes, request
        ) VALUES ($1,$2,$3,$4,'customer',$5,$6,$7,
          ARRAY['exact_alias'],'{}'::jsonb)
        """,
        candidate_request_id,
        tenant_id,
        snapshot_id,
        observation_id,
        f"observation:{observation_id}:customer",
        request_digest,
        canonical_sha256({"authority": str(candidate_request_id)}),
    )
    await conn.execute(
        """
        INSERT INTO entity_candidate_sets (
          id, tenant_id, request_id, request_digest, lane_fates,
          candidates, candidate_set_hash, candidate_set,
          registry_version, expires_at
        ) VALUES ($1,$2,$3,$4,'[]'::jsonb,'[]'::jsonb,$5,
          '{}'::jsonb,'evaluation-v1',now()+interval '1 day')
        """,
        candidate_set_id,
        tenant_id,
        candidate_request_id,
        request_digest,
        canonical_sha256({"candidate_set": str(candidate_set_id)}),
    )
    await conn.execute(
        """
        INSERT INTO resolution_assessments (
          id, tenant_id, candidate_set_id, candidate_distribution,
          model_output, assessment, scorer_and_calibration_version,
          assessed_at, expires_at
        ) VALUES ($1,$2,$3,'{}'::jsonb,'{}'::jsonb,'{}'::jsonb,'evaluation',now(),now()+interval '1 day')
        """,
        assessment_id,
        tenant_id,
        candidate_set_id,
    )
    grounding_admission_id = uuid7()
    await conn.execute(
        """
        INSERT INTO grounding_admission_decisions (
          id, tenant_id, assessment_id, consumer, purpose, operation,
          risk_tier, disposition, selected_referent, reason_codes,
          consumption_authority_fingerprint, decision, decided_at, expires_at
        ) VALUES ($1,$2,$3,'source_semantics','evaluation','read',
          'low','single_referent',$4::jsonb,ARRAY['evaluation'],
          $5,'{}'::jsonb,now(),now()+interval '1 day')
        """,
        grounding_admission_id,
        tenant_id,
        assessment_id,
        json.dumps(selected),
        canonical_sha256({"authority": str(grounding_admission_id)}),
    )
    await conn.execute(
        """
        INSERT INTO grounding_traces (
          id, tenant_id, source_observation_id, phrase, context_snapshot_id,
          candidate_request_id, candidate_set_id, resolution_assessment_id,
          grounding_admission_id, current_fate, selected_referent,
          identity_registry_mutated, source_observation_mutated, trace
        ) VALUES ($1,$2,$3,'customer',$4,$5,$6,$7,$8,
          'resolved_for_consumer',$9::jsonb,FALSE,FALSE,$10::jsonb)
        """,
        trace_id,
        tenant_id,
        observation_id,
        snapshot_id,
        candidate_request_id,
        candidate_set_id,
        assessment_id,
        grounding_admission_id,
        json.dumps(selected),
        json.dumps(
            {"supersedes_grounding_trace_id": str(supersedes_trace_id)}
            if supersedes_trace_id
            else {}
        ),
    )
    if disposition is None:
        return trace_id, None
    interpretation_id, model_id = uuid7(), None
    await conn.execute(
        """
        INSERT INTO source_semantic_interpretations (
          id, tenant_id, grounding_trace_id, source_observation_id,
          context_snapshot_id, entity_mention_id, resolution_assessment_id,
          grounding_admission_id, source_content_hash, source_assertion,
          semantic_frame, speech_act, grounding_continuity, bundle_digest,
          extractor_version, recorded_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'{}'::jsonb,'{}'::jsonb,
          '{}'::jsonb,'{}'::jsonb,$10,'evaluation',now())
        """,
        interpretation_id,
        tenant_id,
        trace_id,
        observation_id,
        snapshot_id,
        uuid7(),
        assessment_id,
        grounding_admission_id,
        canonical_sha256({"source": str(observation_id)}),
        canonical_sha256({"bundle": str(interpretation_id)}),
    )
    if disposition == "belief_applied":
        model_id = uuid7()
        await conn.execute(
            """
            INSERT INTO models (
              id, tenant_id, born_from_event_id, proposition, "natural",
              embedding, scope_temporal, confidence, status,
              confidence_at_assertion
            ) VALUES ($1,$2,$3,$4::jsonb,$5,
              array_fill(0.0::real,ARRAY[768])::vector,
              '{}'::jsonb,0.8,'active',0.8)
            """,
            model_id,
            tenant_id,
            observation_id,
            json.dumps({"kind": "belief", "source_channel": source}),
            f"{source} outcome",
        )
    await conn.execute(
        """
        INSERT INTO source_semantic_admission_decisions (
          id, tenant_id, interpretation_id, disposition, reason_codes,
          proposed_belief_assertion, admitted_model_id, decision_digest,
          decided_at
        ) VALUES ($1,$2,$3,$4,ARRAY['evaluation'],$5::jsonb,$6,$7,now())
        """,
        uuid7(),
        tenant_id,
        interpretation_id,
        disposition,
        json.dumps({"kind": "asserted_state"}) if model_id else None,
        model_id,
        canonical_sha256({"decision": str(interpretation_id)}),
    )
    return trace_id, model_id


async def _binding_count(pool: asyncpg.Pool, tenant_id: UUID) -> int:
    return int(
        await pool.fetchval(
            "SELECT count(*) FROM source_identity_bindings WHERE tenant_id=$1",
            tenant_id,
        )
        or 0
    )


async def _seed_source_binding(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    source_system: str,
    source_native_identifier: str,
    source_surface: str,
) -> tuple[UUID, str]:
    resource_id = uuid7()
    await pool.execute(
        """
        INSERT INTO resources (
          id, tenant_id, kind, identity, current_value, metadata
        ) VALUES (
          $1, $2, 'capacity', $3, '{}'::jsonb,
          jsonb_build_object('semantic_kind', 'source_object')
        )
        """,
        resource_id,
        tenant_id,
        source_surface,
    )
    binding = await SourceIdentityBindingRepo(pool).bind(
        tenant_id=tenant_id,
        source_system=source_system,
        source_native_identifier=source_native_identifier,
        source_identity_authority_ref=f"{source_system}-object-contract-v1",
        canonical_ref={"type": "resource", "id": str(resource_id)},
        evidence_refs=(f"source-object:{source_native_identifier}",),
        valid_from=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    return resource_id, binding.binding_id


async def _observation_snapshot(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    observation_id: UUID,
):
    return await pool.fetchrow(
        """
        SELECT content, content_text, entities_mentioned
        FROM observations WHERE tenant_id=$1 AND id=$2
        """,
        tenant_id,
        observation_id,
    )


def _jira_payload() -> dict:
    return {
        "_fyralis_record_type": "issue",
        "_fyralis_site": "acme.atlassian.net",
        "id": "10001",
        "key": "ENG-42",
        "fields": {
            "summary": "SALES must not impersonate project identity",
            "updated": "2026-07-15T12:30:00.000+0000",
            "project": {"id": "10000", "key": "ENG", "name": "Engineering"},
        },
    }


def _linear_payload() -> dict:
    return {
        "action": "create",
        "type": "Issue",
        "data": {
            "id": "linear-issue-active-surface",
            "identifier": "ENG-123",
            "title": "SALES must not impersonate source identity",
            "team": {"id": "team-1", "key": "ENG", "name": "Engineering"},
            "project": {"id": "project-1", "name": "Billing Reliability"},
            "createdAt": "2026-04-21T10:00:00Z",
        },
        "createdAt": "2026-04-21T10:00:00Z",
    }


def _google_drive_payload(record_type: str = "file") -> dict:
    common = {
        "_fyralis_file_id": "drive-file-active-surface",
        "_fyralis_file_name": "Revenue Planning",
        "modifiedTime": "2026-04-21T10:00:00Z",
    }
    if record_type == "comment":
        return {
            **common,
            "_fyralis_record_type": "comment",
            "id": "drive-comment-active-surface",
            "content": "SALES is untrusted free text.",
        }
    if record_type == "revision":
        return {
            **common,
            "_fyralis_record_type": "revision",
            "id": "drive-revision-active-surface",
            "lastModifyingUser": {"displayName": "SALES"},
        }
    return {
        "id": "drive-file-active-surface",
        "name": "Revenue Planning",
        "version": "7",
        **common,
        "_fyralis_extracted_text": (
            "SALES is untrusted free text, not file identity."
        ),
    }


def _gmail_payload() -> dict:
    return {
        "message_resource": {
            "id": "gmail-message-active-surface",
            "threadId": "gmail-thread-active-surface",
            "snippet": "SALES is untrusted free text.",
            "internalDate": "1776765600000",
            "payload": {
                "headers": [
                    {
                        "name": "Message-ID",
                        "value": "<active-surface@example.com>",
                    },
                    {
                        "name": "From",
                        "value": "Alice <alice@example.com>",
                    },
                    {
                        "name": "To",
                        "value": "bob@example.com",
                    },
                    {
                        "name": "Subject",
                        "value": "Executive Planning",
                    },
                ]
            },
        },
        "mailbox_email": "alice@example.com",
        "scope_used": "gmail.metadata",
        "read_path": "push",
        "gmail_installation_id": "00000000-0000-0000-0000-000000000002",
        "thread_canonical_id": str(uuid7()),
    }


__all__ = ["ARTIFACT_NAME", "run_active_surfaces_experiment"]
