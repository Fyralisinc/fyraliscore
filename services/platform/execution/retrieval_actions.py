"""Specialized inquiry retrieval action executors."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.types import ModelRow
from services.domain.models.repo import ModelsRepo
from services.reasoning.retrieval.pathways import PathwayResult, pathway_b_semantic
from services.reasoning.retrieval.primary import TriggerContext

from .action_cache import action_seed_entities as _action_seed_entities
from .config import InquiryConfig
from .lexical_terms import (
    SPARSE_STRONG_SINGLE_MATCH_MAX_DF as _SPARSE_STRONG_SINGLE_MATCH_MAX_DF,
    focused_index_lookup_groups as _focused_index_lookup_groups,
    focused_index_terms as _focused_index_terms,
    hybrid_lexical_terms as _hybrid_lexical_terms,
    hybrid_sparse_lookup_terms as _hybrid_sparse_lookup_terms,
    hybrid_sparse_strong_single_match_terms as _hybrid_sparse_strong_single_match_terms,
    like_patterns_for_terms as _like_patterns_for_terms,
)
from .routing import trigger_text as _trigger_text
from .types import RetrievalAction

_LEXICAL_FALLBACK_STATEMENT_TIMEOUT_MS = 1500


@dataclass(frozen=True, slots=True)
class FocusedIndexHit:
    model_id: UUID
    score: float
    source: str
    match_count: int = 0
    scope_overlap: int = 0


async def execute_focused_index_action(
    action: RetrievalAction,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    cfg: InquiryConfig,
    *,
    model_limit: int,
) -> PathwayResult | None:
    raw_terms = action.filters.get("terms")
    terms = (
        [str(term) for term in raw_terms if str(term).strip()]
        if isinstance(raw_terms, list)
        else _focused_index_terms(
            action.query or _trigger_text(trigger),
            trigger,
            max_terms=int(cfg.focused_index_terms),
        )
    )
    primitive = str(action.filters.get("primitive") or "").upper()
    primitives = focused_answerability_primitives_for(primitive)
    seed_pairs = focused_seed_entity_pairs(_action_seed_entities(action, trigger))
    model_limit = max(1, int(model_limit))
    scope_limit = min(
        max(1, int(cfg.focused_index_scope_candidates)),
        model_limit,
    )

    hit_sources: dict[UUID, set[str]] = {}
    hit_counts: dict[UUID, int] = {}
    scope_overlaps: dict[UUID, int] = {}
    scores: dict[UUID, float] = {}

    def add_hits(hits: list[FocusedIndexHit]) -> None:
        for hit in hits:
            hit_sources.setdefault(hit.model_id, set()).add(hit.source)
            hit_counts[hit.model_id] = max(
                hit_counts.get(hit.model_id, 0),
                int(hit.match_count),
            )
            scope_overlaps[hit.model_id] = max(
                scope_overlaps.get(hit.model_id, 0),
                int(hit.scope_overlap),
            )
            scores[hit.model_id] = scores.get(hit.model_id, 0.0) + float(hit.score)

    answerability_hits = await focused_answerability_index_scan(
        conn,
        tenant_id=trigger.tenant_id,
        primitives=primitives,
        terms=terms,
        seed_pairs=seed_pairs,
        limit=model_limit,
    )
    add_hits(answerability_hits)
    scoped_sparse_hits = await focused_scope_sparse_scan(
        conn,
        tenant_id=trigger.tenant_id,
        terms=terms,
        seed_pairs=seed_pairs,
        limit=model_limit,
    )
    add_hits(scoped_sparse_hits)
    direct_scope_hits = await focused_direct_scope_scan(
        conn,
        tenant_id=trigger.tenant_id,
        seed_pairs=seed_pairs,
        limit=scope_limit,
    )
    add_hits(direct_scope_hits)

    if not scores:
        return None

    ordered_ids = sorted(
        scores,
        key=lambda model_id: (
            -scores[model_id],
            -scope_overlaps.get(model_id, 0),
            -hit_counts.get(model_id, 0),
            str(model_id),
        ),
    )[:model_limit]
    models = await ModelsRepo(None, run_topology_on_insert=False).retrieve(
        ordered_ids,
        conn=conn,
    )
    by_id = {model.id: model for model in models}
    ordered_models = [by_id[mid] for mid in ordered_ids if mid in by_id]
    if not ordered_models:
        return None

    return PathwayResult(
        models=ordered_models,
        observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        source_pathway="focused_index",
        notes={
            "target": action.target,
            "primitive": primitive,
            "primitives": list(primitives),
            "terms": terms,
            "term_groups": _focused_index_lookup_groups(terms),
            "seed_scope_pairs": len(seed_pairs),
            "answerability_hits": len(answerability_hits),
            "scoped_sparse_hits": len(scoped_sparse_hits),
            "direct_scope_hits": len(direct_scope_hits),
            "merged_hits": len(scores),
            "returned_models": len(ordered_models),
            "top_hits": [
                {
                    "model_id": str(mid),
                    "score": round(scores.get(mid, 0.0), 4),
                    "sources": sorted(hit_sources.get(mid, set())),
                    "match_count": hit_counts.get(mid, 0),
                    "scope_overlap": scope_overlaps.get(mid, 0),
                }
                for mid in ordered_ids[:8]
            ],
        },
    )


def focused_answerability_primitives_for(primitive: str) -> tuple[str, ...]:
    normalized = str(primitive or "").strip().upper()
    aliases = {
        "COMMITMENT": ("COMMITMENT", "DEPENDENCY"),
        "CONSTRAINT": ("CONSTRAINT", "COUNTEREVIDENCE"),
        "COUNTEREVIDENCE": ("COUNTEREVIDENCE", "CONSTRAINT"),
        "DEPENDENCY": ("DEPENDENCY", "COMMITMENT"),
        "GOAL_IMPACT": ("GOAL_IMPACT", "COMMITMENT"),
        "OWNERSHIP": ("OWNERSHIP", "COMMITMENT", "DEPENDENCY"),
        "RECURRENCE": ("RECURRENCE", "DEPENDENCY", "COUNTEREVIDENCE"),
    }
    return aliases.get(normalized, (normalized,)) if normalized else ()


def focused_seed_entity_pairs(raw_entities: Any) -> list[tuple[str, UUID]]:
    pairs: list[tuple[str, UUID]] = []
    seen: set[tuple[str, UUID]] = set()
    if not isinstance(raw_entities, list):
        return pairs
    for raw in raw_entities:
        if not isinstance(raw, dict):
            continue
        raw_type = raw.get("type")
        raw_id = raw.get("id")
        if raw_type is None or raw_id is None:
            continue
        try:
            entity_id = UUID(str(raw_id))
        except (TypeError, ValueError):
            continue
        entity_type = str(raw_type)
        candidates = (
            ("customer", "customer_resource", "resource")
            if entity_type in {"customer", "customer_resource", "resource"}
            else (entity_type,)
        )
        for candidate_type in candidates:
            pair = (candidate_type, entity_id)
            if pair in seen:
                continue
            seen.add(pair)
            pairs.append(pair)
    return pairs


async def focused_answerability_index_scan(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    primitives: tuple[str, ...],
    terms: list[str] | tuple[str, ...],
    seed_pairs: list[tuple[str, UUID]],
    limit: int,
) -> list[FocusedIndexHit]:
    groups = _focused_index_lookup_groups(terms)
    if not primitives or not groups or limit <= 0:
        return []
    table = await conn.fetchval(
        "SELECT to_regclass('public.model_answerability_index')"
    )
    if table is None:
        return []
    scope_types = [pair[0] for pair in seed_pairs]
    scope_ids = [pair[1] for pair in seed_pairs]
    rows = await fetch_bounded_lookup_rows(
        conn,
        """
        WITH group_tokens AS MATERIALIZED (
          SELECT g.group_ord::int,
                 token.value::text AS term
          FROM jsonb_array_elements($4::jsonb)
               WITH ORDINALITY AS g(tokens, group_ord)
          CROSS JOIN LATERAL jsonb_array_elements_text(g.tokens) AS token(value)
        ),
        group_sizes AS MATERIALIZED (
          SELECT group_ord,
                 count(DISTINCT term)::int AS token_count
          FROM group_tokens
          GROUP BY group_ord
        ),
        matched AS MATERIALIZED (
          SELECT mai.model_id,
                 mai.primitive,
                 gt.group_ord,
                 count(DISTINCT gt.term)::int AS matched_terms
          FROM group_tokens gt
          JOIN model_answerability_index mai
            ON mai.tenant_id = $1
           AND mai.status = 'active'
           AND mai.primitive = ANY($3::text[])
           AND mai.term = gt.term
          GROUP BY mai.model_id, mai.primitive, gt.group_ord
        ),
        group_hits AS MATERIALIZED (
          SELECT matched.model_id,
                 matched.primitive,
                 matched.group_ord,
                 group_sizes.token_count
          FROM matched
          JOIN group_sizes
            ON group_sizes.group_ord = matched.group_ord
          WHERE matched.matched_terms = group_sizes.token_count
        ),
        scored AS MATERIALIZED (
          SELECT model_id,
                 count(DISTINCT primitive)::int AS primitive_match_count,
                 sum(token_count)::int AS match_count,
                 min(group_ord)::int AS first_group_ord
          FROM group_hits
          GROUP BY model_id
        ),
        scope_overlap AS MATERIALIZED (
          SELECT mse.model_id,
                 count(*)::int AS overlap
          FROM unnest($5::text[], $6::uuid[]) AS seed(entity_type, entity_id)
          JOIN model_scope_entities mse
            ON mse.tenant_id = $1
           AND mse.entity_type = seed.entity_type
           AND mse.entity_id = seed.entity_id
          GROUP BY mse.model_id
        )
        SELECT m.id,
               scored.match_count,
               scored.primitive_match_count,
               coalesce(scope_overlap.overlap, 0)::int AS scope_overlap
        FROM scored
        JOIN models m
          ON m.id = scored.model_id
         AND m.tenant_id = $1
        LEFT JOIN scope_overlap
          ON scope_overlap.model_id = m.id
        WHERE m.status = 'active'
        ORDER BY coalesce(scope_overlap.overlap, 0) DESC,
                 scored.match_count DESC,
                 scored.primitive_match_count DESC,
                 scored.first_group_ord ASC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        list(primitives),
        json.dumps(groups),
        scope_types,
        scope_ids,
        label="focused_answerability_index",
    )
    return [
        FocusedIndexHit(
            model_id=row["id"],
            score=0.78
            + min(0.18, int(row["match_count"] or 0) * 0.025)
            + min(0.18, int(row["scope_overlap"] or 0) * 0.05),
            source="answerability_index",
            match_count=int(row["match_count"] or 0),
            scope_overlap=int(row["scope_overlap"] or 0),
        )
        for row in rows
    ]


async def focused_scope_sparse_scan(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    terms: list[str] | tuple[str, ...],
    seed_pairs: list[tuple[str, UUID]],
    limit: int,
) -> list[FocusedIndexHit]:
    groups = _focused_index_lookup_groups(terms)
    if not groups or not seed_pairs or limit <= 0:
        return []
    table = await conn.fetchval("SELECT to_regclass('public.model_sparse_terms')")
    if table is None:
        return []
    scope_types = [pair[0] for pair in seed_pairs]
    scope_ids = [pair[1] for pair in seed_pairs]
    rows = await fetch_bounded_lookup_rows(
        conn,
        """
        WITH group_tokens AS MATERIALIZED (
          SELECT g.group_ord::int,
                 token.value::text AS term
          FROM jsonb_array_elements($3::jsonb)
               WITH ORDINALITY AS g(tokens, group_ord)
          CROSS JOIN LATERAL jsonb_array_elements_text(g.tokens) AS token(value)
        ),
        group_sizes AS MATERIALIZED (
          SELECT group_ord,
                 count(DISTINCT term)::int AS token_count
          FROM group_tokens
          GROUP BY group_ord
        ),
        lexical AS MATERIALIZED (
          SELECT mst.model_id,
                 gt.group_ord,
                 count(DISTINCT gt.term)::int AS matched_terms
          FROM group_tokens gt
          JOIN model_sparse_terms mst
            ON mst.tenant_id = $1
           AND mst.status = 'active'
           AND mst.term = gt.term
          GROUP BY mst.model_id, gt.group_ord
        ),
        lexical_hits AS MATERIALIZED (
          SELECT lexical.model_id,
                 lexical.group_ord,
                 group_sizes.token_count
          FROM lexical
          JOIN group_sizes
            ON group_sizes.group_ord = lexical.group_ord
          WHERE lexical.matched_terms = group_sizes.token_count
        ),
        lexical_scored AS MATERIALIZED (
          SELECT model_id,
                 sum(token_count)::int AS match_count,
                 min(group_ord)::int AS first_group_ord
          FROM lexical_hits
          GROUP BY model_id
        ),
        scope_overlap AS MATERIALIZED (
          SELECT mse.model_id,
                 count(*)::int AS overlap
          FROM unnest($4::text[], $5::uuid[]) AS seed(entity_type, entity_id)
          JOIN model_scope_entities mse
            ON mse.tenant_id = $1
           AND mse.entity_type = seed.entity_type
           AND mse.entity_id = seed.entity_id
          GROUP BY mse.model_id
        )
        SELECT m.id,
               lexical_scored.match_count,
               scope_overlap.overlap::int AS scope_overlap
        FROM lexical_scored
        JOIN scope_overlap
          ON scope_overlap.model_id = lexical_scored.model_id
        JOIN models m
          ON m.id = lexical_scored.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
        ORDER BY scope_overlap.overlap DESC,
                 lexical_scored.match_count DESC,
                 lexical_scored.first_group_ord ASC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        json.dumps(groups),
        scope_types,
        scope_ids,
        label="focused_scope_sparse",
    )
    return [
        FocusedIndexHit(
            model_id=row["id"],
            score=0.70
            + min(0.20, int(row["scope_overlap"] or 0) * 0.055)
            + min(0.16, int(row["match_count"] or 0) * 0.025),
            source="scope_sparse",
            match_count=int(row["match_count"] or 0),
            scope_overlap=int(row["scope_overlap"] or 0),
        )
        for row in rows
    ]


async def focused_direct_scope_scan(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    seed_pairs: list[tuple[str, UUID]],
    limit: int,
) -> list[FocusedIndexHit]:
    if not seed_pairs or limit <= 0:
        return []
    scope_types = [pair[0] for pair in seed_pairs]
    scope_ids = [pair[1] for pair in seed_pairs]
    rows = await fetch_bounded_lookup_rows(
        conn,
        """
        WITH scope_overlap AS MATERIALIZED (
          SELECT mse.model_id,
                 count(*)::int AS overlap
          FROM unnest($3::text[], $4::uuid[]) AS seed(entity_type, entity_id)
          JOIN model_scope_entities mse
            ON mse.tenant_id = $1
           AND mse.entity_type = seed.entity_type
           AND mse.entity_id = seed.entity_id
          GROUP BY mse.model_id
        )
        SELECT m.id,
               scope_overlap.overlap::int AS scope_overlap
        FROM scope_overlap
        JOIN models m
          ON m.id = scope_overlap.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
        ORDER BY scope_overlap.overlap DESC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        scope_types,
        scope_ids,
        label="focused_direct_scope",
    )
    return [
        FocusedIndexHit(
            model_id=row["id"],
            score=0.42 + min(0.18, int(row["scope_overlap"] or 0) * 0.04),
            source="direct_scope",
            match_count=0,
            scope_overlap=int(row["scope_overlap"] or 0),
        )
        for row in rows
    ]


async def execute_semantic_hybrid_action(
    action: RetrievalAction,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
    *,
    model_limit: int,
) -> PathwayResult:
    query_text = action.query or _trigger_text(trigger)
    trigger_text = _trigger_text(trigger)
    precomputed_vector = (
        trigger.precomputed_seed_vector
        if embedder is None or query_text == trigger_text
        else None
    )
    result = await pathway_b_semantic(
        query_text,
        trigger.tenant_id,
        conn,
        k=model_limit,
        embedder=embedder,
        precomputed_vector=precomputed_vector,
        event_actors=trigger.scope_actors,
        event_entities=_action_seed_entities(action, trigger),
    )
    semantic_ids = {model.id for model in result.models}
    hybrid_note: dict[str, Any] = {
        "enabled": bool(cfg.semantic_hybrid_lexical_enabled),
        "used": False,
        "semantic_count": len(result.models),
        "lexical_count": 0,
        "merged_count": len(result.models),
    }
    if not cfg.semantic_hybrid_lexical_enabled:
        hybrid_note["reason"] = "disabled"
        result.notes["semantic_hybrid_lexical"] = hybrid_note
        return result

    terms = _hybrid_lexical_terms(
        query_text,
        trigger,
        max_terms=max(1, int(cfg.semantic_hybrid_lexical_terms)),
    )
    hybrid_note["terms"] = terms
    if not terms:
        hybrid_note["reason"] = "no_lexical_terms"
        result.notes["semantic_hybrid_lexical"] = hybrid_note
        return result

    lexical_limit = min(
        max(1, int(cfg.semantic_hybrid_lexical_max_candidates)),
        max(1, int(model_limit) * 2),
    )
    per_term_limit = max(1, int(cfg.semantic_hybrid_lexical_per_term_limit))
    lexical_hits = await hybrid_lexical_model_scan(
        trigger,
        conn,
        terms=terms,
        limit=lexical_limit,
        per_term_limit=per_term_limit,
    )
    hybrid_note.update(
        {
            "lexical_limit": lexical_limit,
            "lexical_per_term_limit": per_term_limit,
            "lexical_count": len(lexical_hits),
        }
    )
    if not lexical_hits:
        hybrid_note["reason"] = "no_lexical_hits"
        result.notes["semantic_hybrid_lexical"] = hybrid_note
        return result

    result.models = merge_hybrid_semantic_lexical_models(
        result.models,
        lexical_hits,
        limit=max(1, int(model_limit)),
    )
    hybrid_note["used"] = True
    hybrid_note["merged_count"] = len(result.models)
    hybrid_note["lexical_only_selected"] = sum(
        1 for model in result.models if model.id not in semantic_ids
    )
    result.notes["semantic_hybrid_lexical"] = hybrid_note
    return result


def cap_pathway_models(result: PathwayResult, limit: int) -> None:
    limit = max(0, int(limit))
    before = len(result.models)
    if limit <= 0 or before <= limit:
        return
    result.models = sorted(
        result.models,
        key=lambda model: (
            -float(getattr(model, "activation", 0.0) or 0.0),
            str(getattr(model, "id", "")),
        ),
    )[:limit]
    result.notes["models_before_adaptive_cap"] = before
    result.notes["models_after_adaptive_cap"] = len(result.models)


async def hybrid_lexical_model_scan(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    terms: list[str] | tuple[str, ...],
    limit: int,
    per_term_limit: int,
) -> list[tuple[ModelRow, int]]:
    sparse_hits = await hybrid_sparse_model_scan(
        trigger,
        conn,
        terms=terms,
        limit=limit,
        per_term_limit=per_term_limit,
    )
    if sparse_hits:
        return sparse_hits

    patterns = _like_patterns_for_terms(terms)
    if not patterns or limit <= 0:
        return []
    table = await conn.fetchval("SELECT to_regclass('public.model_search_documents')")
    if table is None:
        return []
    rows = await fetch_bounded_lookup_rows(
        conn,
        """
        WITH patterns AS (
          SELECT pattern, ord
          FROM unnest($3::text[]) WITH ORDINALITY AS p(pattern, ord)
        ),
        per_pattern AS MATERIALIZED (
          SELECT hit.model_id,
                 p.ord::int AS pattern_ord
          FROM patterns p
          CROSS JOIN LATERAL (
            SELECT msd.model_id
            FROM model_search_documents msd
            JOIN models m
              ON m.id = msd.model_id
             AND m.tenant_id = msd.tenant_id
            WHERE msd.tenant_id = $1
              AND msd.status = 'active'
              AND m.status = 'active'
              AND msd.search_text LIKE p.pattern ESCAPE '!'
            ORDER BY m.activation DESC, m.created_at DESC, m.id
            LIMIT $4
          ) hit
        ),
        scored AS MATERIALIZED (
          SELECT model_id,
                 count(*)::int AS match_count,
                 min(pattern_ord)::int AS first_pattern_ord
          FROM per_pattern
          GROUP BY model_id
        )
        SELECT m.id,
               scored.match_count
        FROM scored
        JOIN models m
          ON m.id = scored.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
        ORDER BY scored.match_count DESC,
                 scored.first_pattern_ord ASC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        trigger.tenant_id,
        max(1, int(limit)),
        patterns,
        max(1, int(per_term_limit)),
        label="hybrid_lexical",
    )
    ids = [row["id"] for row in rows]
    if not ids:
        return []
    models = await ModelsRepo(None, run_topology_on_insert=False).retrieve(
        ids, conn=conn
    )
    by_id = {model.id: model for model in models}
    return [
        (by_id[row["id"]], int(row["match_count"] or 1))
        for row in rows
        if row["id"] in by_id
    ]


async def hybrid_sparse_model_scan(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    terms: list[str] | tuple[str, ...],
    limit: int,
    per_term_limit: int,
) -> list[tuple[ModelRow, int]]:
    lookup_terms = _hybrid_sparse_lookup_terms(terms)
    if not lookup_terms or limit <= 0:
        return []
    table = await conn.fetchval("SELECT to_regclass('public.model_sparse_terms')")
    if table is None:
        return []
    rows = await fetch_bounded_lookup_rows(
        conn,
        """
        WITH query_terms AS MATERIALIZED (
          SELECT term::text,
                 ord::int AS term_ord
          FROM unnest($3::text[]) WITH ORDINALITY AS q(term, ord)
        ),
        query_meta AS MATERIALIZED (
          SELECT count(*)::int AS query_term_count
          FROM query_terms
        ),
        active_models AS MATERIALIZED (
          SELECT greatest(1, count(*)::int)::float8 AS active_model_count
          FROM models
          WHERE tenant_id = $1
            AND status = 'active'
        ),
        term_stats AS MATERIALIZED (
          SELECT qt.term,
                 qt.term_ord,
                 count(mstat.id)::int AS term_df
          FROM query_terms qt
          LEFT JOIN model_sparse_terms mst
            ON mst.tenant_id = $1
           AND mst.status = 'active'
           AND mst.term = qt.term
          LEFT JOIN models mstat
            ON mstat.id = mst.model_id
           AND mstat.tenant_id = mst.tenant_id
           AND mstat.status = 'active'
          GROUP BY qt.term, qt.term_ord
        ),
        term_hits AS MATERIALIZED (
          SELECT ts.term,
                 ts.term_ord,
                 ts.term_df,
                 hit.model_id,
                 hit.weight,
                 (
                   ln((am.active_model_count + 1.0) / (ts.term_df::float8 + 1.0))
                   + 1.0
                 )::float8 AS idf
          FROM term_stats ts
          CROSS JOIN active_models am
          CROSS JOIN LATERAL (
            SELECT mst.model_id,
                   mst.weight
            FROM model_sparse_terms mst
            JOIN models mhit
              ON mhit.id = mst.model_id
             AND mhit.tenant_id = mst.tenant_id
             AND mhit.status = 'active'
            WHERE mst.tenant_id = $1
              AND mst.status = 'active'
              AND mst.term = ts.term
            ORDER BY mst.weight DESC,
                     mhit.activation DESC,
                     mhit.created_at DESC,
                     mst.model_id
            LIMIT $4
          ) hit
        ),
        scored AS MATERIALIZED (
          SELECT model_id,
                 count(DISTINCT term)::int AS match_count,
                 sum(weight * idf)::real AS weighted_score,
                 min(term_ord)::int AS first_term_ord,
                 bool_or(
                   term = ANY($5::text[])
                   AND term_df <= $6::int
                 ) AS has_strong_singleton
          FROM term_hits
          GROUP BY model_id
        )
        SELECT m.id,
               scored.match_count
        FROM scored
        CROSS JOIN query_meta
        JOIN models m
          ON m.id = scored.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
          AND (
            query_meta.query_term_count <= 1
            OR scored.match_count >= LEAST(2, query_meta.query_term_count)
            OR scored.has_strong_singleton
          )
        ORDER BY scored.match_count DESC,
                 scored.weighted_score DESC,
                 scored.first_term_ord ASC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        trigger.tenant_id,
        max(1, int(limit)),
        lookup_terms,
        max(1, int(per_term_limit)),
        _hybrid_sparse_strong_single_match_terms(lookup_terms),
        _SPARSE_STRONG_SINGLE_MATCH_MAX_DF,
        label="hybrid_sparse",
    )
    ids = [row["id"] for row in rows]
    if not ids:
        return []
    models = await ModelsRepo(None, run_topology_on_insert=False).retrieve(
        ids, conn=conn
    )
    by_id = {model.id: model for model in models}
    return [
        (by_id[row["id"]], int(row["match_count"] or 1))
        for row in rows
        if row["id"] in by_id
    ]


async def fetch_bounded_lookup_rows(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
    label: str = "lookup",
) -> list[asyncpg.Record]:
    in_outer_transaction = bool(getattr(conn, "is_in_transaction", lambda: False)())
    previous_timeout: str | None = None
    if in_outer_transaction:
        previous_timeout = await conn.fetchval(
            "SELECT current_setting('statement_timeout')"
        )
    try:
        async with conn.transaction():
            if in_outer_transaction:
                await conn.fetchval(
                    "SELECT set_config('statement_timeout', $1, true)",
                    str(_LEXICAL_FALLBACK_STATEMENT_TIMEOUT_MS),
                )
            else:
                await conn.execute(
                    "SET LOCAL statement_timeout = "
                    f"{_LEXICAL_FALLBACK_STATEMENT_TIMEOUT_MS}"
                )
            return list(await conn.fetch(query, *args))
    except asyncpg.QueryCanceledError:
        import structlog

        structlog.get_logger(__name__).warning(
            "inquiry.bounded_lookup_statement_timeout",
            label=label,
            timeout_ms=_LEXICAL_FALLBACK_STATEMENT_TIMEOUT_MS,
        )
        return []
    finally:
        if (
            in_outer_transaction
            and previous_timeout is not None
            and bool(getattr(conn, "is_in_transaction", lambda: False)())
        ):
            try:
                await conn.fetchval(
                    "SELECT set_config('statement_timeout', $1, true)",
                    previous_timeout,
                )
            except Exception as exc:  # noqa: BLE001
                import structlog

                structlog.get_logger(__name__).warning(
                    "inquiry.bounded_lookup_timeout_restore_failed",
                    label=label,
                    error=str(exc),
                )


async def fetch_hybrid_lexical_fallback_rows(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
) -> list[asyncpg.Record]:
    return await fetch_bounded_lookup_rows(
        conn,
        query,
        *args,
        label="hybrid_lexical",
    )


def merge_hybrid_semantic_lexical_models(
    semantic_models: list[ModelRow],
    lexical_hits: list[tuple[ModelRow, int]],
    *,
    limit: int,
) -> list[ModelRow]:
    scores: dict[UUID, float] = {}
    by_id: dict[UUID, ModelRow] = {}
    ranks: dict[UUID, tuple[int, int]] = {}
    rrf_k = 60.0

    for rank, model in enumerate(semantic_models, start=1):
        by_id[model.id] = model
        scores[model.id] = scores.get(model.id, 0.0) + 1.0 / (rrf_k + rank)
        old = ranks.get(model.id, (10_000, 10_000))
        ranks[model.id] = (min(old[0], rank), old[1])

    for rank, (model, match_count) in enumerate(lexical_hits, start=1):
        by_id.setdefault(model.id, model)
        lexical_score = 0.92 / (rrf_k + rank)
        lexical_score += min(0.008, max(1, int(match_count)) * 0.002)
        scores[model.id] = scores.get(model.id, 0.0) + lexical_score
        old = ranks.get(model.id, (10_000, 10_000))
        ranks[model.id] = (old[0], min(old[1], rank))

    ordered_ids = sorted(
        by_id,
        key=lambda model_id: (
            -scores.get(model_id, 0.0),
            ranks.get(model_id, (10_000, 10_000))[0],
            ranks.get(model_id, (10_000, 10_000))[1],
            str(model_id),
        ),
    )
    return [by_id[model_id] for model_id in ordered_ids[: max(1, int(limit))]]


__all__ = [
    "FocusedIndexHit",
    "cap_pathway_models",
    "execute_focused_index_action",
    "execute_semantic_hybrid_action",
    "fetch_bounded_lookup_rows",
    "fetch_hybrid_lexical_fallback_rows",
    "focused_answerability_index_scan",
    "focused_answerability_primitives_for",
    "focused_direct_scope_scan",
    "focused_scope_sparse_scan",
    "focused_seed_entity_pairs",
    "hybrid_lexical_model_scan",
    "hybrid_sparse_model_scan",
    "merge_hybrid_semantic_lexical_models",
]
