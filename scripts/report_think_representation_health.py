#!/usr/bin/env python3
"""Report Think representation health for a tenant.

This is the large-run scoreboard we wanted after Alpen: not only "how many
models?", but also updates, evidence absorption, edge adaptiveness, coverage
roles, retrieval tags, and source repetition.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from typing import Any

import asyncpg


_HEX_RE = re.compile(r"\b[0-9a-f]{7,40}\b", re.I)
_NUMBER_RE = re.compile(r"[$]?[0-9][0-9,]*([.][0-9]+)?")
_URL_RE = re.compile(r"https?://[^\s)]+", re.I)
_WS_RE = re.compile(r"\s+")


def _normalize_text(text: str) -> str:
    value = str(text or "").casefold()
    value = _URL_RE.sub("<url>", value)
    value = _HEX_RE.sub("<hex>", value)
    value = _NUMBER_RE.sub("<num>", value)
    return _WS_RE.sub(" ", value).strip()


async def main() -> None:
    args = _parse_args()
    conn = await asyncpg.connect(args.database_url)
    try:
        report = await build_report(conn, tenant_id=args.tenant_id, days=args.days)
    finally:
        await conn.close()
    print(json.dumps(report, indent=2, sort_keys=True, default=str))


async def build_report(conn: asyncpg.Connection, *, tenant_id: str, days: int | None) -> dict[str, Any]:
    window_sql = ""
    params: list[Any] = [tenant_id]
    if days is not None:
        params.append(days)
        window_sql = f" AND created_at >= now() - (${len(params)}::int * interval '1 day')"

    model_counts = await conn.fetchrow(
        f"""
        SELECT
          count(*) FILTER (WHERE status = 'active') AS active,
          count(*) FILTER (WHERE status <> 'active') AS inactive,
          count(*) AS total,
          count(*) FILTER (
            WHERE status = 'active'
              AND (
                proposition->'coverage_roles' ? 'discovered_pattern'
                OR proposition->'retrieval_tags' ? 'discovered_pattern'
                OR claim_role = 'pattern'
                OR domain_tags && ARRAY[
                    'discovered_pattern',
                    'source_digest',
                    'major_source_window',
                    'contextual_recurrence'
                  ]::text[]
              )
          ) AS pattern_models,
          count(*) FILTER (
            WHERE status = 'active'
              AND (
                proposition->'retrieval_tags' ? 'source_digest'
                OR domain_tags && ARRAY['source_digest']::text[]
              )
          ) AS source_digest_models,
          count(*) FILTER (
            WHERE status = 'active'
              AND (
                proposition->'coverage_roles' ? 'curiosity'
                OR proposition->'retrieval_tags' ?| ARRAY[
                    'coverage_curiosity',
                    'open_question',
                    'unresolved_unknown',
                    'strategic_question',
                    'success_driver'
                  ]
                OR domain_tags && ARRAY[
                    'coverage_curiosity',
                    'open_question',
                    'unresolved_unknown',
                    'strategic_question',
                    'success_driver'
                  ]::text[]
              )
          ) AS curiosity_models
        FROM models
        WHERE tenant_id = $1
        {window_sql}
        """,
        *params,
    )

    coverage_roles = await _top_jsonb_array_values(
        conn,
        tenant_id=tenant_id,
        field="coverage_roles",
        days=days,
    )
    retrieval_tags = await _top_jsonb_array_values(
        conn,
        tenant_id=tenant_id,
        field="retrieval_tags",
        days=days,
    )
    model_retrieval_tags = await _top_model_retrieval_tags(
        conn,
        tenant_id=tenant_id,
        days=days,
    )
    domain_tags = await conn.fetch(
        f"""
        SELECT tag, count(*) AS n
        FROM models, unnest(domain_tags) AS tag
        WHERE tenant_id = $1
        {window_sql}
        GROUP BY tag
        ORDER BY n DESC, tag
        LIMIT 40
        """,
        *params,
    )

    updates = await conn.fetchrow(
        """
        SELECT
          count(*) AS signal_readings,
          count(DISTINCT model_id) AS models_with_readings,
          count(*) FILTER (WHERE reading_kind = 'confirm') AS confirms,
          count(*) FILTER (WHERE reading_kind = 'contest') AS contests,
          count(*) FILTER (WHERE detail->'reading'->'contextual_frame' IS NOT NULL
                            OR detail->'contextual_frame' IS NOT NULL) AS contextual_readings
        FROM model_signal_readings
        WHERE tenant_id = $1
        """,
        tenant_id,
    )

    edges = await conn.fetchrow(
        """
        SELECT
          count(*) AS total_edges,
          count(*) FILTER (WHERE status = 'active') AS active_edges,
          count(*) FILTER (WHERE review_status = 'accepted') AS accepted_edges,
          count(*) FILTER (WHERE review_status IN ('candidate','needs_review')) AS adaptive_candidate_edges,
          coalesce(sum(confirmed_count), 0) AS edge_confirmations,
          count(*) FILTER (
            WHERE cardinality(evidence_event_ids) > 0
               OR cardinality(evidence_model_ids) > 0
               OR confirmed_count > 0
          ) AS evidence_backed_edges
        FROM model_edges
        WHERE tenant_id = $1
        """,
        tenant_id,
    )

    observations = await _observation_repetition(conn, tenant_id=tenant_id)
    think_runs = await conn.fetchrow(
        """
        SELECT
          count(*) AS runs,
          count(*) FILTER (WHERE status = 'success') AS success,
          count(*) FILTER (WHERE status <> 'success') AS nonsuccess,
          coalesce(sum((ops_applied->'memory_aggregation'->>'model_inserts')::int), 0) AS model_inserts,
          coalesce(sum((ops_applied->'memory_aggregation'->>'model_updates')::int), 0) AS model_updates,
          coalesce(sum((ops_applied->'memory_aggregation'->>'evidence_attachments')::int), 0) AS evidence_attachments,
          coalesce(sum((ops_applied->'memory_aggregation'->>'near_duplicate_absorptions')::int), 0) AS near_duplicate_absorptions,
          coalesce(sum((ops_applied->'memory_aggregation'->>'edge_ops')::int), 0) AS edge_ops
        FROM think_runs
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    ledger = await _ledger_report(conn, tenant_id=tenant_id, days=days)
    substrate_candidates = await _substrate_candidates_report(
        conn,
        tenant_id=tenant_id,
        days=days,
    )
    substrate_readiness = await _substrate_readiness_report(
        conn,
        tenant_id=tenant_id,
        days=days,
        substrate_candidates=substrate_candidates,
    )
    model_specificity = await _model_specificity_report(
        conn,
        tenant_id=tenant_id,
        days=days,
    )
    truth_seeking = await _truth_seeking_report(
        conn,
        tenant_id=tenant_id,
        days=days,
    )
    company_question_coverage = _company_question_coverage_report(
        model_counts=dict(model_counts or {}),
        coverage_roles=[dict(row) for row in coverage_roles],
        retrieval_tags=[dict(row) for row in model_retrieval_tags],
        domain_tags=[dict(row) for row in domain_tags],
        model_updates=dict(updates or {}),
        edges=dict(edges or {}),
        substrate_readiness=substrate_readiness,
        model_specificity=model_specificity,
        truth_seeking=truth_seeking,
    )

    return {
        "tenant_id": tenant_id,
        "window_days": days,
        "models": dict(model_counts or {}),
        "coverage_roles": [dict(row) for row in coverage_roles],
        "retrieval_tags": [dict(row) for row in retrieval_tags],
        "model_retrieval_tags": [dict(row) for row in model_retrieval_tags],
        "domain_tags": [dict(row) for row in domain_tags],
        "model_updates": dict(updates or {}),
        "edges": dict(edges or {}),
        "observations": observations,
        "think_runs": dict(think_runs or {}),
        "representation_ledger": ledger,
        "substrate_candidates": substrate_candidates,
        "substrate_readiness": substrate_readiness,
        "model_specificity": model_specificity,
        "truth_seeking": truth_seeking,
        "company_question_coverage": company_question_coverage,
    }


async def _top_jsonb_array_values(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    field: str,
    days: int | None,
) -> list[asyncpg.Record]:
    params: list[Any] = [tenant_id, field]
    window_sql = ""
    if days is not None:
        params.append(days)
        window_sql = f" AND created_at >= now() - (${len(params)}::int * interval '1 day')"
    return await conn.fetch(
        f"""
        SELECT value AS tag, count(*) AS n
        FROM models,
             jsonb_array_elements_text(coalesce(proposition->$2, '[]'::jsonb)) AS value
        WHERE tenant_id = $1
        {window_sql}
        GROUP BY value
        ORDER BY n DESC, value
        LIMIT 40
        """,
        *params,
    )


async def _top_model_retrieval_tags(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    days: int | None,
) -> list[asyncpg.Record]:
    params: list[Any] = [tenant_id]
    window_sql = ""
    if days is not None:
        params.append(days)
        window_sql = f" AND created_at >= now() - (${len(params)}::int * interval '1 day')"
    return await conn.fetch(
        f"""
        SELECT tag, count(*) AS n
        FROM (
          SELECT jsonb_array_elements_text(
                   coalesce(proposition->'retrieval_tags', '[]'::jsonb)
                 ) AS tag
          FROM models
          WHERE tenant_id = $1
          {window_sql}
          UNION ALL
          SELECT unnest(domain_tags) AS tag
          FROM models
          WHERE tenant_id = $1
          {window_sql}
        ) AS tags
        WHERE tag <> ''
        GROUP BY tag
        ORDER BY n DESC, tag
        LIMIT 40
        """,
        *params,
    )


async def _observation_repetition(conn: asyncpg.Connection, *, tenant_id: str) -> dict[str, Any]:
    rows = await conn.fetch(
        """
        SELECT source_channel, content_text
        FROM observations
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    by_source: dict[str, list[str]] = {}
    for row in rows:
        by_source.setdefault(row["source_channel"] or "", []).append(row["content_text"] or "")

    source_stats = []
    total = len(rows)
    distinct_norm_all: set[tuple[str, str]] = set()
    repeated_rows = 0
    for source, texts in by_source.items():
        counts: dict[str, int] = {}
        for text in texts:
            sig = _normalize_text(text)
            counts[sig] = counts.get(sig, 0) + 1
            distinct_norm_all.add((source, sig))
        repeated = sum(n for n in counts.values() if n > 1)
        repeated_rows += repeated
        source_stats.append(
            {
                "source_channel": source,
                "total": len(texts),
                "distinct_normalized": len(counts),
                "unique_ratio": round(len(counts) / max(1, len(texts)), 4),
                "repeated_row_ratio": round(repeated / max(1, len(texts)), 4),
                "max_class": max(counts.values()) if counts else 0,
            }
        )
    source_stats.sort(key=lambda row: (row["unique_ratio"], -row["total"]))
    return {
        "total": total,
        "distinct_source_normalized": len(distinct_norm_all),
        "unique_ratio": round(len(distinct_norm_all) / max(1, total), 4),
        "repeated_row_ratio": round(repeated_rows / max(1, total), 4),
        "sources": source_stats[:30],
    }


async def _ledger_report(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    days: int | None,
) -> dict[str, Any]:
    exists = await conn.fetchval(
        "SELECT to_regclass('public.think_representation_ledger')"
    )
    if exists is None:
        return {"available": False}

    params: list[Any] = [tenant_id]
    window_sql = ""
    if days is not None:
        params.append(days)
        window_sql = f" AND created_at >= now() - (${len(params)}::int * interval '1 day')"

    totals = await conn.fetchrow(
        f"""
        SELECT
          count(*) AS audits,
          count(*) FILTER (WHERE budget_status = 'ok') AS ok,
          count(*) FILTER (WHERE budget_status = 'warning') AS warning,
          count(*) FILTER (WHERE budget_status = 'failed') AS failed,
          coalesce(sum(observation_count), 0) AS observations_audited,
          coalesce(sum(claim_insert_count), 0) AS claim_inserts,
          coalesce(sum(model_update_count), 0) AS model_updates,
          coalesce(sum(evidence_attachment_count), 0) AS evidence_attachments,
          coalesce(sum(near_duplicate_absorption_count), 0) AS near_duplicate_absorptions,
          coalesce(sum(model_adaptiveness), 0) AS model_adaptiveness,
          coalesce(sum(edge_adaptiveness), 0) AS edge_adaptiveness,
          coalesce(sum(source_digest_count), 0) AS source_digest_count,
          coalesce(sum((metrics->>'curiosity_count')::int), 0) AS curiosity_count,
          coalesce(sum((metrics->>'important_unknown_count')::int), 0) AS important_unknown_count
        FROM think_representation_ledger
        WHERE tenant_id = $1
        {window_sql}
        """,
        *params,
    )
    warning_codes = await conn.fetch(
        f"""
        SELECT warning->>'code' AS code, count(*) AS n
        FROM think_representation_ledger,
             jsonb_array_elements(warnings) AS warning
        WHERE tenant_id = $1
        {window_sql}
        GROUP BY warning->>'code'
        ORDER BY n DESC, code
        LIMIT 30
        """,
        *params,
    )
    return {
        "available": True,
        "totals": dict(totals or {}),
        "warning_codes": [dict(row) for row in warning_codes],
    }


async def _substrate_candidates_report(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    days: int | None,
) -> dict[str, Any]:
    exists = await conn.fetchval("SELECT to_regclass('public.substrate_candidates')")
    if exists is None:
        return {"available": False}

    params: list[Any] = [tenant_id]
    window_sql = ""
    if days is not None:
        params.append(days)
        window_sql = f" AND created_at >= now() - (${len(params)}::int * interval '1 day')"

    totals = await conn.fetchrow(
        f"""
        SELECT
          count(*) AS total,
          count(*) FILTER (WHERE status = 'proposed') AS proposed,
          count(*) FILTER (WHERE status = 'needs_clarification') AS needs_clarification,
          count(*) FILTER (WHERE status = 'promoted') AS promoted,
          count(*) FILTER (WHERE cardinality(evidence_observation_ids) > 0)
            AS evidence_backed,
          count(DISTINCT kind) AS kind_count
        FROM substrate_candidates
        WHERE tenant_id = $1
        {window_sql}
        """,
        *params,
    )
    by_kind = await conn.fetch(
        f"""
        SELECT
          kind,
          status,
          count(*) AS n,
          count(*) FILTER (WHERE cardinality(evidence_observation_ids) > 0)
            AS evidence_backed
        FROM substrate_candidates
        WHERE tenant_id = $1
        {window_sql}
        GROUP BY kind, status
        ORDER BY kind, status
        """,
        *params,
    )
    top = await conn.fetch(
        f"""
        SELECT
          kind,
          label,
          status,
          confidence,
          cardinality(evidence_observation_ids) AS evidence_count,
          metadata
        FROM substrate_candidates
        WHERE tenant_id = $1
        {window_sql}
        ORDER BY confidence DESC, evidence_count DESC, updated_at DESC
        LIMIT 30
        """,
        *params,
    )
    return {
        "available": True,
        "totals": dict(totals or {}),
        "by_kind": [dict(row) for row in by_kind],
        "top": [dict(row) for row in top],
    }


async def _substrate_readiness_report(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    days: int | None,
    substrate_candidates: dict[str, Any],
) -> dict[str, Any]:
    params: list[Any] = [tenant_id]
    window_sql = ""
    if days is not None:
        params.append(days)
        window_sql = f" AND created_at >= now() - (${len(params)}::int * interval '1 day')"

    observation_counts = await conn.fetchrow(
        f"""
        SELECT
          count(*) AS observations,
          count(*) FILTER (WHERE source_actor_ref IS NOT NULL) AS actor_ref_observations,
          count(DISTINCT source_actor_ref) FILTER (WHERE source_actor_ref IS NOT NULL)
            AS distinct_source_actor_refs,
          count(DISTINCT source_channel) AS distinct_source_channels,
          count(DISTINCT split_part(source_channel, ':', 1)) AS distinct_source_roots
        FROM observations
        WHERE tenant_id = $1
        {window_sql}
        """,
        *params,
    )
    actor_counts = await conn.fetchrow(
        """
        SELECT
          (SELECT count(*) FROM actors WHERE tenant_id = $1) AS actors,
          (
            SELECT count(*)
            FROM actor_identity_mappings aim
            JOIN actors a ON a.id = aim.actor_id
            WHERE a.tenant_id = $1
          ) AS actor_mappings
        """,
        tenant_id,
    )
    resource_counts = await conn.fetchrow(
        """
        SELECT
          (SELECT count(*) FROM resources WHERE tenant_id = $1) AS resources,
          (
            SELECT CASE
              WHEN to_regclass('public.customer_commitments') IS NULL THEN 0
              ELSE (SELECT count(*) FROM customer_commitments WHERE tenant_id = $1)
            END
          ) AS customer_commitments
        """,
        tenant_id,
    )
    source_roots = await conn.fetch(
        f"""
        SELECT split_part(source_channel, ':', 1) AS source_root, count(*) AS n
        FROM observations
        WHERE tenant_id = $1
        {window_sql}
        GROUP BY split_part(source_channel, ':', 1)
        ORDER BY n DESC, source_root
        LIMIT 40
        """,
        *params,
    )
    snapshot = {
        "observations": dict(observation_counts or {}),
        "canonical": {
            **dict(actor_counts or {}),
            **dict(resource_counts or {}),
        },
        "candidate_counts": _candidate_counts_by_kind(substrate_candidates),
        "source_roots": [dict(row) for row in source_roots],
    }
    snapshot["warnings"] = _substrate_readiness_warnings(snapshot)
    return snapshot


async def _model_specificity_report(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    days: int | None,
) -> dict[str, Any]:
    params: list[Any] = [tenant_id]
    window_sql = ""
    if days is not None:
        params.append(days)
        window_sql = f" AND created_at >= now() - (${len(params)}::int * interval '1 day')"

    row = await conn.fetchrow(
        f"""
        SELECT
          count(*) FILTER (WHERE status = 'active') AS active_models,
          count(*) FILTER (
            WHERE status = 'active'
              AND coalesce(array_length(scope_actors, 1), 0) = 0
          ) AS active_without_actor_scope,
          count(*) FILTER (
            WHERE status = 'active'
              AND jsonb_array_length(coalesce(scope_entities, '[]'::jsonb)) = 0
          ) AS active_without_entity_scope,
          count(*) FILTER (
            WHERE status = 'active'
              AND coalesce(array_length(scope_actors, 1), 0) = 0
              AND jsonb_array_length(coalesce(scope_entities, '[]'::jsonb)) = 0
          ) AS active_without_any_scope,
          coalesce(avg(cardinality(supporting_event_ids)), 0) AS avg_supporting_events,
          coalesce(max(cardinality(supporting_event_ids)), 0) AS max_supporting_events
        FROM models
        WHERE tenant_id = $1
        {window_sql}
        """,
        *params,
    )
    return dict(row or {})


async def _truth_seeking_report(
    conn: asyncpg.Connection,
    *,
    tenant_id: str,
    days: int | None,
) -> dict[str, Any]:
    params: list[Any] = [tenant_id]
    window_sql = ""
    if days is not None:
        params.append(days)
        window_sql = f" AND created_at >= now() - (${len(params)}::int * interval '1 day')"

    predictions = await conn.fetchrow(
        f"""
        SELECT
          count(*) FILTER (
            WHERE status = 'active'
              AND (
                claim_role = 'prediction'
                OR proposition_kind = 'prediction'
                OR proposition->>'kind' = 'prediction'
              )
          ) AS active_predictions,
          count(*) FILTER (WHERE resolved_at IS NOT NULL) AS resolved_models,
          count(*) FILTER (WHERE resolution_outcome IS TRUE) AS resolved_true,
          count(*) FILTER (WHERE resolution_outcome IS FALSE) AS resolved_false,
          count(*) FILTER (
            WHERE status = 'active'
              AND (
                falsifier IS NULL
                OR falsifier = '{{}}'::jsonb
              )
          ) AS active_without_falsifier
        FROM models
        WHERE tenant_id = $1
        {window_sql}
        """,
        *params,
    )
    relation_contradictions = await conn.fetchrow(
        """
        SELECT
          count(*) FILTER (WHERE edge_kind IN ('contradicts', 'weakens')) AS counter_relations,
          count(*) FILTER (WHERE edge_kind = 'contradicts') AS contradictions
        FROM relation_claims
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    out = dict(predictions or {})
    out.update(dict(relation_contradictions or {}))
    return out


def _company_question_coverage_report(
    *,
    model_counts: dict[str, Any],
    coverage_roles: list[dict[str, Any]],
    retrieval_tags: list[dict[str, Any]],
    domain_tags: list[dict[str, Any]],
    model_updates: dict[str, Any],
    edges: dict[str, Any],
    substrate_readiness: dict[str, Any],
    model_specificity: dict[str, Any],
    truth_seeking: dict[str, Any],
) -> dict[str, Any]:
    tags = _tag_count_map([*coverage_roles, *retrieval_tags, *domain_tags])
    canonical = substrate_readiness.get("canonical") or {}
    candidates = substrate_readiness.get("candidate_counts") or {}
    observations = substrate_readiness.get("observations") or {}
    active_models = int(model_counts.get("active") or 0)
    spaces = {
        "ownership": (
            int(canonical.get("actors") or 0) > 0
            or int(candidates.get("actor") or 0) > 0
            or tags.get("question_ownership", 0) > 0
            or tags.get("unknown_responsible_owner", 0) > 0
        ),
        "work_and_commitments": (
            int(candidates.get("commitment") or 0) > 0
            or tags.get("workstream", 0) > 0
            or tags.get("progress_signal", 0) > 0
            or tags.get("delivery_risk", 0) > 0
        ),
        "state_risks_blockers": (
            tags.get("state", 0) > 0
            or tags.get("role_concern", 0) > 0
            or tags.get("delivery_risk", 0) > 0
            or tags.get("operational_churn", 0) > 0
        ),
        "customers_and_counterparties": (
            int(canonical.get("resources") or 0) > 0
            or int(canonical.get("customer_commitments") or 0) > 0
            or int(candidates.get("customer") or 0) > 0
            or tags.get("candidate_customer_question", 0) > 0
        ),
        "systems_and_vendors": (
            int(candidates.get("system") or 0) > 0
            or int(candidates.get("vendor") or 0) > 0
            or tags.get("source_observability", 0) > 0
            or tags.get("source_finance", 0) > 0
        ),
        "patterns_and_loops": (
            int(model_counts.get("pattern_models") or 0) > 0
            or int(candidates.get("pattern") or 0) > 0
            or tags.get("discovered_pattern", 0) > 0
            or tags.get("source_digest", 0) > 0
        ),
        "truth_and_uncertainty": (
            int(model_updates.get("contests") or 0) > 0
            or int(truth_seeking.get("counter_relations") or 0) > 0
            or int(model_counts.get("curiosity_models") or 0) > 0
            or tags.get("open_question", 0) > 0
            or tags.get("unresolved_unknown", 0) > 0
        ),
        "temporal_change": (
            tags.get("temporal", 0) > 0
            or int(truth_seeking.get("active_predictions") or 0) > 0
            or int(truth_seeking.get("resolved_models") or 0) > 0
        ),
        "next_action": (
            tags.get("intervention", 0) > 0
            or tags.get("success_driver", 0) > 0
            or tags.get("role_recommendation", 0) > 0
        ),
        "relationship_reasoning": (
            int(edges.get("active_edges") or 0) > 0
            or tags.get("relationship", 0) > 0
        ),
    }
    covered = sum(1 for value in spaces.values() if value)
    score = round(covered / max(1, len(spaces)), 4)
    warnings = _company_question_coverage_warnings(
        spaces=spaces,
        score=score,
        active_models=active_models,
        observations=int(observations.get("observations") or 0),
        model_specificity=model_specificity,
        truth_seeking=truth_seeking,
    )
    return {
        "score": score,
        "covered_spaces": covered,
        "total_spaces": len(spaces),
        "spaces": spaces,
        "warnings": warnings,
    }


def _tag_count_map(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        raw = row.get("tag")
        if raw is None:
            continue
        tag = str(raw).strip().casefold().replace("-", "_").replace(" ", "_")
        if not tag:
            continue
        counts[tag] = counts.get(tag, 0) + int(row.get("n") or 0)
    return counts


def _company_question_coverage_warnings(
    *,
    spaces: dict[str, bool],
    score: float,
    active_models: int,
    observations: int,
    model_specificity: dict[str, Any],
    truth_seeking: dict[str, Any],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    missing = [name for name, covered in spaces.items() if not covered]
    if observations >= 1000 and score < 0.7:
        warnings.append(
            {
                "code": "company_question_coverage_low",
                "severity": "critical" if score < 0.5 else "warning",
                "score": score,
                "missing_spaces": missing,
            }
        )
    if active_models and int(model_specificity.get("active_without_any_scope") or 0) / max(1, active_models) > 0.5:
        warnings.append(
            {
                "code": "too_many_models_without_scope",
                "severity": "warning",
                "active_without_any_scope": int(
                    model_specificity.get("active_without_any_scope") or 0
                ),
                "active_models": active_models,
            }
        )
    if observations >= 1000 and int(truth_seeking.get("counter_relations") or 0) == 0:
        warnings.append(
            {
                "code": "truth_seeking_counterevidence_absent",
                "severity": "warning",
                "detail": "Large run has no contradiction/weakening relation layer.",
            }
        )
    if int(model_specificity.get("max_supporting_events") or 0) >= 500:
        warnings.append(
            {
                "code": "model_support_event_runaway",
                "severity": "warning",
                "max_supporting_events": int(
                    model_specificity.get("max_supporting_events") or 0
                ),
            }
        )
    return warnings


def _candidate_counts_by_kind(report: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    if not report.get("available"):
        return counts
    for row in report.get("by_kind") or []:
        kind = str(row.get("kind") or "")
        if not kind:
            continue
        counts[kind] = counts.get(kind, 0) + int(row.get("n") or 0)
    return counts


def _substrate_readiness_warnings(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    observations = snapshot.get("observations") or {}
    canonical = snapshot.get("canonical") or {}
    candidates = snapshot.get("candidate_counts") or {}
    source_roots = snapshot.get("source_roots") or []

    total_observations = int(observations.get("observations") or 0)
    distinct_actor_refs = int(observations.get("distinct_source_actor_refs") or 0)
    distinct_source_roots = int(observations.get("distinct_source_roots") or 0)
    actor_count = int(canonical.get("actors") or 0)
    actor_mapping_count = int(canonical.get("actor_mappings") or 0)
    resource_count = int(canonical.get("resources") or 0)
    customer_commitment_count = int(canonical.get("customer_commitments") or 0)
    candidate_actor_count = int(candidates.get("actor") or 0)
    candidate_customer_count = int(candidates.get("customer") or 0)
    candidate_commitment_count = int(candidates.get("commitment") or 0)
    candidate_system_count = int(candidates.get("system") or 0)
    candidate_vendor_count = int(candidates.get("vendor") or 0)
    candidate_pattern_count = int(candidates.get("pattern") or 0)

    warnings: list[dict[str, Any]] = []
    if distinct_actor_refs >= 25 and actor_count + candidate_actor_count < 25:
        warnings.append(
            {
                "code": "actor_substrate_too_thin",
                "severity": "critical" if actor_count == 0 else "warning",
                "detail": (
                    "Many source actor refs exist, but canonical plus "
                    "candidate actor substrate is still thin."
                ),
                "distinct_source_actor_refs": distinct_actor_refs,
                "actors": actor_count,
                "candidate_actors": candidate_actor_count,
                "actor_mappings": actor_mapping_count,
            }
        )
    if distinct_actor_refs > 0 and actor_mapping_count == 0:
        warnings.append(
            {
                "code": "actor_alias_mapping_absent",
                "severity": "warning",
                "detail": (
                    "Source actor refs are present but no canonical alias "
                    "mappings exist yet; expect scope_actors to stay sparse."
                ),
                "distinct_source_actor_refs": distinct_actor_refs,
            }
        )
    if distinct_source_roots >= 10 and candidate_system_count + candidate_vendor_count < (
        distinct_source_roots // 2
    ):
        warnings.append(
            {
                "code": "source_system_substrate_too_thin",
                "severity": "warning",
                "detail": (
                    "The run spans many sources, but few source/system/vendor "
                    "candidates were created."
                ),
                "distinct_source_roots": distinct_source_roots,
                "candidate_systems": candidate_system_count,
                "candidate_vendors": candidate_vendor_count,
            }
        )
    vendor_roots = {
        str(row.get("source_root") or "")
        for row in source_roots
        if str(row.get("source_root") or "")
        in {"ashby", "brex", "carta", "deel", "gusto", "hibob", "mercury", "quickbooks", "ramp"}
    }
    if vendor_roots and resource_count + candidate_vendor_count == 0:
        warnings.append(
            {
                "code": "vendor_resource_substrate_absent",
                "severity": "warning",
                "detail": (
                    "Finance/vendor-like sources exist, but no resources or "
                    "vendor candidates are present."
                ),
                "vendor_source_roots": sorted(vendor_roots),
            }
        )
    if total_observations >= 1000 and candidate_commitment_count == 0:
        warnings.append(
            {
                "code": "commitment_substrate_absent",
                "severity": "warning",
                "detail": (
                    "Large run has no provisional commitments/work items; "
                    "workstream facts may collapse into broad source digests."
                ),
            }
        )
    if total_observations >= 1000 and candidate_pattern_count == 0:
        warnings.append(
            {
                "code": "pattern_substrate_absent",
                "severity": "warning",
                "detail": (
                    "Large run has no discovered pattern candidates, which "
                    "weakens operating-signature discovery."
                ),
            }
        )
    if (
        total_observations >= 1000
        and resource_count == 0
        and customer_commitment_count == 0
        and candidate_customer_count == 0
    ):
        warnings.append(
            {
                "code": "customer_substrate_absent",
                "severity": "warning",
                "detail": (
                    "No canonical or candidate customer substrate is visible; "
                    "customer-specific questions will be hard to answer."
                ),
            }
        )
    return warnings


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("tenant_id")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", "postgresql://localhost/company_os_dev"))
    parser.add_argument("--days", type=int, default=None)
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(main())
