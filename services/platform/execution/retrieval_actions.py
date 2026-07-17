"""Specialized inquiry retrieval action executors."""

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.types import ModelRow
from services.domain.models.read_shapes import ACCEPTED_MODEL_ROWS_SQL
from services.domain.models.repo import ModelsRepo
from services.reasoning.retrieval.pathways import (
    ModelCandidateHit,
    PathwayResult,
    hydrate_active_models_by_ids,
    pathway_b_representation_tag_candidates,
    pathway_b_representation_tags,
    pathway_b_semantic,
    pathway_l_semantic_term_candidates,
    pathway_l_semantic_terms,
)
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval.read_fanout import ReadFanoutBudget

from .action_cache import (
    action_seed_entities as _action_seed_entities,
    stable_cache_value as _stable_cache_value,
)
from .config import InquiryConfig
from .lexical_terms import (
    SPARSE_STRONG_SINGLE_MATCH_MAX_DF as _SPARSE_STRONG_SINGLE_MATCH_MAX_DF,
    focused_index_lookup_groups as _focused_index_lookup_groups,
    focused_index_terms as _focused_index_terms,
    hybrid_lookup_terms as _hybrid_lookup_terms,
    hybrid_lexical_terms as _hybrid_lexical_terms,
    hybrid_sparse_lookup_terms as _hybrid_sparse_lookup_terms,
    hybrid_sparse_strong_single_match_terms as _hybrid_sparse_strong_single_match_terms,
    like_patterns_for_terms as _like_patterns_for_terms,
)
from .routing import trigger_text as _trigger_text
from .types import RetrievalAction

_LEXICAL_FALLBACK_STATEMENT_TIMEOUT_MS = 1500
_ANSWERABILITY_TERM_DF_PROBE_CAP = 1024
_ACCEPTED_MODEL_ROWS_SQL = ACCEPTED_MODEL_ROWS_SQL


def _answerability_max_term_df(limit: int) -> int:
    return max(128, min(_ANSWERABILITY_TERM_DF_PROBE_CAP, max(1, int(limit)) * 64))


class _BoundedLookupRows(list[asyncpg.Record]):
    def __init__(
        self,
        rows: list[asyncpg.Record] | None = None,
        *,
        timed_out: bool = False,
    ) -> None:
        super().__init__(rows or [])
        self.timed_out = timed_out


class _HybridSparseLookupTimedOut(Exception):
    pass


def _bounded_lookup_timed_out(rows: object) -> bool:
    return bool(getattr(rows, "timed_out", False))


class _FocusedIndexHits(list["FocusedIndexHit"]):
    def __init__(
        self,
        hits: list["FocusedIndexHit"] | None = None,
        *,
        timed_out: bool = False,
    ) -> None:
        super().__init__(hits or [])
        self.timed_out = timed_out


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
    read_pool: asyncpg.Pool | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
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

    async def run_answerability(read_conn: asyncpg.Connection) -> list[FocusedIndexHit]:
        return await focused_answerability_index_scan(
            read_conn,
            tenant_id=trigger.tenant_id,
            primitives=primitives,
            terms=terms,
            seed_pairs=seed_pairs,
            limit=model_limit,
        )

    async def run_scoped_sparse(read_conn: asyncpg.Connection) -> list[FocusedIndexHit]:
        return await focused_scope_sparse_scan(
            read_conn,
            tenant_id=trigger.tenant_id,
            terms=terms,
            seed_pairs=seed_pairs,
            limit=model_limit,
        )

    async def run_direct_scope(read_conn: asyncpg.Connection) -> list[FocusedIndexHit]:
        return await focused_direct_scope_scan(
            read_conn,
            tenant_id=trigger.tenant_id,
            seed_pairs=seed_pairs,
            limit=scope_limit,
        )

    fanout_parallel = read_pool is not None or read_fanout_budget is not None
    if fanout_parallel:
        tasks = (
            asyncio.create_task(
                _run_with_optional_pool(
                    conn,
                    read_pool,
                    run_answerability,
                    read_fanout_budget=read_fanout_budget,
                )
            ),
            asyncio.create_task(
                _run_with_optional_pool(
                    conn,
                    read_pool,
                    run_scoped_sparse,
                    read_fanout_budget=read_fanout_budget,
                )
            ),
            asyncio.create_task(
                _run_with_optional_pool(
                    conn,
                    read_pool,
                    run_direct_scope,
                    read_fanout_budget=read_fanout_budget,
                )
            ),
        )
        try:
            answerability_hits, scoped_sparse_hits, direct_scope_hits = await asyncio.gather(
                *tasks
            )
        except Exception:
            for task in tasks:
                task.cancel()
            raise
    else:
        answerability_hits = await run_answerability(conn)
        scoped_sparse_hits = await run_scoped_sparse(conn)
        direct_scope_hits = await run_direct_scope(conn)

    add_hits(answerability_hits)
    add_hits(scoped_sparse_hits)
    add_hits(direct_scope_hits)
    scan_timeouts = {
        "answerability_index": _bounded_lookup_timed_out(answerability_hits),
        "scope_sparse": _bounded_lookup_timed_out(scoped_sparse_hits),
        "direct_scope": _bounded_lookup_timed_out(direct_scope_hits),
    }

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
            "scan_timeouts": scan_timeouts,
            "bounded_lookup_timeout_count": sum(1 for value in scan_timeouts.values() if value),
            "fanout_parallel": fanout_parallel,
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
    per_token_limit = max(16, min(48, max(1, int(limit)) * 4))
    per_scope_limit = max(24, min(160, max(1, int(limit)) * 10))
    max_term_df = _answerability_max_term_df(limit)
    rows = await fetch_bounded_lookup_rows(
        conn,
        f"""
        WITH raw_group_tokens AS MATERIALIZED (
          SELECT g.group_ord::int,
                 token.value::text AS term
          FROM jsonb_array_elements($4::jsonb)
               WITH ORDINALITY AS g(tokens, group_ord)
          CROSS JOIN LATERAL jsonb_array_elements_text(g.tokens) AS token(value)
        ),
        primitive_tokens AS MATERIALIZED (
          SELECT gt.term,
                 gt.group_ord,
                 primitive.value::text AS primitive
          FROM raw_group_tokens gt
          CROSS JOIN unnest($3::text[]) AS primitive(value)
        ),
        token_stats AS MATERIALIZED (
          SELECT pt.term,
                 pt.group_ord,
                 pt.primitive,
                 stats.term_df
          FROM primitive_tokens pt
          CROSS JOIN LATERAL (
            SELECT count(*)::int AS term_df
            FROM (
              SELECT 1
              FROM model_answerability_index mai
              WHERE mai.tenant_id = $1
                AND mai.status = 'active'
                AND mai.primitive = pt.primitive
                AND mai.term = pt.term
              LIMIT $9
            ) bounded
          ) stats
          WHERE stats.term_df > 0
            AND stats.term_df <= $10
        ),
        group_sizes AS MATERIALIZED (
          SELECT group_ord,
                 primitive,
                 count(DISTINCT term)::int AS token_count
          FROM token_stats
          GROUP BY group_ord, primitive
        ),
        token_hits AS MATERIALIZED (
          SELECT pt.term,
                 pt.group_ord,
                 hit.model_id,
                 pt.primitive,
                 hit.weight
          FROM token_stats pt
          CROSS JOIN LATERAL (
            SELECT mai.model_id,
                   mai.weight
            FROM model_answerability_index mai
            WHERE mai.tenant_id = $1
              AND mai.status = 'active'
              AND mai.primitive = pt.primitive
              AND mai.term = pt.term
            ORDER BY mai.weight DESC,
                     mai.model_id
            LIMIT $7
          ) hit
        ),
        matched AS MATERIALIZED (
          SELECT mai.model_id,
                 mai.primitive,
                 mai.group_ord,
                 count(DISTINCT mai.term)::int AS matched_terms,
                 sum(mai.weight)::real AS weighted_score
          FROM token_hits mai
          GROUP BY mai.model_id, mai.primitive, mai.group_ord
        ),
        group_hits AS MATERIALIZED (
          SELECT matched.model_id,
                 matched.primitive,
                 matched.group_ord,
                 group_sizes.token_count,
                 matched.weighted_score
          FROM matched
          JOIN group_sizes
            ON group_sizes.group_ord = matched.group_ord
           AND group_sizes.primitive = matched.primitive
          WHERE matched.matched_terms = group_sizes.token_count
        ),
        scored AS MATERIALIZED (
          SELECT model_id,
                 count(DISTINCT primitive)::int AS primitive_match_count,
                 sum(token_count)::int AS match_count,
                 min(group_ord)::int AS first_group_ord,
                 sum(weighted_score)::real AS weighted_score
          FROM group_hits
          GROUP BY model_id
        ),
        scope_hits AS MATERIALIZED (
          SELECT hit.model_id,
                 seed.seed_ord::int AS seed_ord
          FROM unnest($5::text[], $6::uuid[])
               WITH ORDINALITY AS seed(entity_type, entity_id, seed_ord)
          CROSS JOIN LATERAL (
            WITH scoped_ids AS MATERIALIZED (
              SELECT mse.model_id
              FROM model_scope_entities mse
              WHERE mse.tenant_id = $1
                AND mse.entity_type = seed.entity_type
                AND mse.entity_id = seed.entity_id
            )
            SELECT scoped_ids.model_id
            FROM scoped_ids
            JOIN {_ACCEPTED_MODEL_ROWS_SQL} AS m
              ON m.id = scoped_ids.model_id
             AND m.tenant_id = $1
             AND m.status = 'active'
            ORDER BY m.activation DESC,
                     m.created_at DESC,
                     m.id
            LIMIT $8
          ) hit
        ),
        scope_overlap AS MATERIALIZED (
          SELECT mse.model_id,
                 count(DISTINCT mse.seed_ord)::int AS overlap
          FROM scope_hits mse
          GROUP BY mse.model_id
        )
        SELECT m.id,
               scored.match_count,
               scored.primitive_match_count,
               coalesce(scope_overlap.overlap, 0)::int AS scope_overlap
        FROM scored
        JOIN LATERAL (
          SELECT models.id,
                 models.activation,
                 models.created_at
          FROM {_ACCEPTED_MODEL_ROWS_SQL} AS models
          WHERE models.id = scored.model_id
            AND models.tenant_id = $1
            AND models.status = 'active'
          LIMIT 1
        ) m ON TRUE
        LEFT JOIN scope_overlap
          ON scope_overlap.model_id = m.id
        ORDER BY coalesce(scope_overlap.overlap, 0) DESC,
                 scored.match_count DESC,
                 scored.primitive_match_count DESC,
                 scored.weighted_score DESC,
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
        per_token_limit,
        per_scope_limit,
        max_term_df + 1,
        max_term_df,
        label="focused_answerability_index",
    )
    hits = [
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
    return _FocusedIndexHits(hits, timed_out=_bounded_lookup_timed_out(rows))


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
    per_scope_limit = max(32, min(240, max(1, int(limit)) * 12))
    scope_pool_limit = max(240, min(960, max(1, int(limit)) * 20))
    rows = await fetch_bounded_lookup_rows(
        conn,
        f"""
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
        scope_candidates AS MATERIALIZED (
          SELECT hit.model_id,
                 seed.seed_ord::int AS seed_ord
          FROM unnest($4::text[], $5::uuid[])
               WITH ORDINALITY AS seed(entity_type, entity_id, seed_ord)
          CROSS JOIN LATERAL (
            WITH scoped_ids AS MATERIALIZED (
              SELECT mse.model_id
              FROM model_scope_entities mse
              WHERE mse.tenant_id = $1
                AND mse.entity_type = seed.entity_type
                AND mse.entity_id = seed.entity_id
            )
            SELECT scoped_ids.model_id
            FROM scoped_ids
            JOIN {_ACCEPTED_MODEL_ROWS_SQL} AS m
              ON m.id = scoped_ids.model_id
             AND m.tenant_id = $1
             AND m.status = 'active'
            ORDER BY m.activation DESC,
                     m.created_at DESC,
                     m.id
            LIMIT $6
          ) hit
        ),
        scope_overlap AS MATERIALIZED (
          SELECT model_id,
                 count(DISTINCT seed_ord)::int AS overlap
          FROM scope_candidates
          GROUP BY model_id
        ),
        scope_pool AS MATERIALIZED (
          SELECT m.id AS model_id,
                 scope_overlap.overlap::int AS overlap,
                 m.activation,
                 m.created_at
          FROM scope_overlap
          JOIN {_ACCEPTED_MODEL_ROWS_SQL} AS m
            ON m.id = scope_overlap.model_id
           AND m.tenant_id = $1
           AND m.status = 'active'
          ORDER BY scope_overlap.overlap DESC,
                   m.activation DESC,
                   m.created_at DESC,
                   m.id
          LIMIT $7
        ),
        lexical AS MATERIALIZED (
          SELECT sp.model_id,
                 gt.group_ord,
                 count(DISTINCT gt.term)::int AS matched_terms
          FROM group_tokens gt
          JOIN scope_pool sp
            ON TRUE
          JOIN model_sparse_terms mst
            ON mst.tenant_id = $1
           AND mst.status = 'active'
           AND mst.model_id = sp.model_id
           AND mst.term = gt.term
          GROUP BY sp.model_id, gt.group_ord
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
        )
        SELECT m.id,
               lexical_scored.match_count,
               scope_pool.overlap::int AS scope_overlap
        FROM lexical_scored
        JOIN scope_pool
          ON scope_pool.model_id = lexical_scored.model_id
        JOIN {_ACCEPTED_MODEL_ROWS_SQL} AS m
          ON m.id = lexical_scored.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
        ORDER BY scope_pool.overlap DESC,
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
        per_scope_limit,
        scope_pool_limit,
        label="focused_scope_sparse",
    )
    hits = [
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
    return _FocusedIndexHits(hits, timed_out=_bounded_lookup_timed_out(rows))


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
    per_scope_limit = max(24, min(240, max(1, int(limit)) * 16))
    rows = await fetch_bounded_lookup_rows(
        conn,
        f"""
        WITH scope_candidates AS MATERIALIZED (
          SELECT hit.model_id,
                 seed.seed_ord::int AS seed_ord
          FROM unnest($3::text[], $4::uuid[])
               WITH ORDINALITY AS seed(entity_type, entity_id, seed_ord)
          CROSS JOIN LATERAL (
            WITH scoped_ids AS MATERIALIZED (
              SELECT mse.model_id
              FROM model_scope_entities mse
              WHERE mse.tenant_id = $1
                AND mse.entity_type = seed.entity_type
                AND mse.entity_id = seed.entity_id
            )
            SELECT scoped_ids.model_id
            FROM scoped_ids
            JOIN {_ACCEPTED_MODEL_ROWS_SQL} AS m
              ON m.id = scoped_ids.model_id
             AND m.tenant_id = $1
             AND m.status = 'active'
            ORDER BY m.activation DESC,
                     m.created_at DESC,
                     m.id
            LIMIT $5
          ) hit
        ),
        scope_overlap AS MATERIALIZED (
          SELECT model_id,
                 count(DISTINCT seed_ord)::int AS overlap
          FROM scope_candidates
          GROUP BY model_id
        )
        SELECT m.id,
               scope_overlap.overlap::int AS scope_overlap
        FROM scope_overlap
        JOIN {_ACCEPTED_MODEL_ROWS_SQL} AS m
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
        per_scope_limit,
        label="focused_direct_scope",
    )
    hits = [
        FocusedIndexHit(
            model_id=row["id"],
            score=0.42 + min(0.18, int(row["scope_overlap"] or 0) * 0.04),
            source="direct_scope",
            match_count=0,
            scope_overlap=int(row["scope_overlap"] or 0),
        )
        for row in rows
    ]
    return _FocusedIndexHits(hits, timed_out=_bounded_lookup_timed_out(rows))


def _clone_semantic_pathway_result(
    result: PathwayResult,
    *,
    cache_hit: bool,
    cache_wait: bool = False,
) -> PathwayResult:
    notes = dict(result.notes or {})
    substrate_note = dict(notes.get("semantic_substrate") or {})
    substrate_note["cache_hit"] = bool(cache_hit)
    substrate_note["cache_wait"] = bool(cache_wait)
    notes["semantic_substrate"] = substrate_note
    return PathwayResult(
        models=list(result.models),
        observations=list(result.observations),
        acts={key: list(value) for key, value in (result.acts or {}).items()},
        resources=list(result.resources),
        source_pathway=result.source_pathway,
        notes=notes,
    )


def _semantic_session_cache_key(
    trigger: TriggerContext,
    cfg: InquiryConfig,
    *,
    query_text: str,
    model_limit: int,
    seed_entities: list[dict[str, Any]],
) -> tuple[Any, ...]:
    seed_signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    return (
        "semantic_substrate:v1",
        str(trigger.tenant_id),
        " ".join(str(query_text or "").lower().split()),
        max(1, int(model_limit)),
        tuple(sorted(str(actor) for actor in (trigger.scope_actors or []))),
        _stable_cache_value(seed_entities),
        _stable_cache_value(seed_signature),
        bool(cfg.semantic_hybrid_lexical_enabled),
        max(1, int(cfg.semantic_hybrid_lexical_max_candidates)),
        max(1, int(cfg.semantic_hybrid_lexical_terms)),
        max(1, int(cfg.semantic_hybrid_lexical_per_term_limit)),
    )


def _canonical_scope_entity_pairs(raw_entities: Any) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    if not isinstance(raw_entities, list):
        return pairs
    for raw in raw_entities:
        if not isinstance(raw, dict):
            continue
        entity_type = raw.get("type")
        entity_id = raw.get("id")
        if entity_type is None or entity_id is None:
            continue
        kind = str(entity_type)
        ident = str(entity_id)
        if kind in {"customer", "customer_resource", "resource"}:
            pairs.add(("customer", ident))
            pairs.add(("customer_resource", ident))
            pairs.add(("resource", ident))
        else:
            pairs.add((kind, ident))
    return pairs


def _semantic_selected_scope_accounting(
    selected_models: list[ModelRow],
    source_ids: set[UUID],
    *,
    seed_entities: list[dict[str, Any]],
    scope_actors: list[UUID],
) -> dict[str, Any]:
    selected = [model for model in selected_models if model.id in source_ids]
    seed_pairs = _canonical_scope_entity_pairs(seed_entities)
    actor_set = {str(actor) for actor in (scope_actors or [])}
    has_scope_basis = bool(seed_pairs or actor_set)
    in_scope: list[str] = []
    cross_scope: list[str] = []
    unscoped: list[str] = []
    for model in selected:
        model_pairs = _canonical_scope_entity_pairs(
            getattr(model, "scope_entities", []) or []
        )
        model_actors = {str(actor) for actor in (getattr(model, "scope_actors", []) or [])}
        if not has_scope_basis:
            unscoped.append(str(model.id))
        elif (seed_pairs and seed_pairs & model_pairs) or (
            actor_set and actor_set & model_actors
        ):
            in_scope.append(str(model.id))
        else:
            cross_scope.append(str(model.id))
    return {
        "selected": len(selected),
        "in_scope_selected": len(in_scope),
        "cross_scope_selected": len(cross_scope),
        "unscoped_selected": len(unscoped),
        "cross_scope_model_ids": sorted(cross_scope),
    }


@dataclass(slots=True)
class SemanticRetrievalSession:
    """Shared semantic retrieval substrate for one inquiry run."""

    _results: dict[tuple[Any, ...], PathwayResult] = field(default_factory=dict)
    _in_flight: dict[tuple[Any, ...], asyncio.Task[PathwayResult]] = field(
        default_factory=dict
    )
    _active_sparse_model_counts: dict[UUID, int] = field(default_factory=dict)
    _schema_capabilities: dict[str, bool] = field(default_factory=dict)

    async def execute_action(
        self,
        action: RetrievalAction,
        trigger: TriggerContext,
        conn: asyncpg.Connection,
        embedder: Any | None,
        cfg: InquiryConfig,
        *,
        model_limit: int,
        read_pool: asyncpg.Pool | None = None,
        read_fanout_budget: ReadFanoutBudget | None = None,
    ) -> PathwayResult:
        query_text = action.query or _trigger_text(trigger)
        seed_entities = _action_seed_entities(action, trigger)
        cache_key = _semantic_session_cache_key(
            trigger,
            cfg,
            query_text=query_text,
            model_limit=model_limit,
            seed_entities=seed_entities,
        )
        cached = self._results.get(cache_key)
        if cached is not None:
            return _clone_semantic_pathway_result(cached, cache_hit=True)

        task = self._in_flight.get(cache_key)
        owner = task is None
        if task is None:
            task = asyncio.create_task(
                _execute_semantic_hybrid_action_uncached(
                    action,
                    trigger,
                    conn,
                    embedder,
                    cfg,
                    model_limit=model_limit,
                    query_text=query_text,
                    seed_entities=seed_entities,
                    read_pool=read_pool,
                    read_fanout_budget=read_fanout_budget,
                    active_sparse_model_counts=self._active_sparse_model_counts,
                    schema_capabilities=self._schema_capabilities,
                )
            )
            self._in_flight[cache_key] = task
        try:
            result = await task
        finally:
            if owner:
                self._in_flight.pop(cache_key, None)
        if owner:
            self._results[cache_key] = _clone_semantic_pathway_result(
                result,
                cache_hit=False,
            )
            return _clone_semantic_pathway_result(result, cache_hit=False)
        return _clone_semantic_pathway_result(result, cache_hit=True, cache_wait=True)


async def execute_semantic_hybrid_action(
    action: RetrievalAction,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
    *,
    model_limit: int,
    semantic_session: SemanticRetrievalSession | None = None,
    read_pool: asyncpg.Pool | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
) -> PathwayResult:
    if semantic_session is not None:
        return await semantic_session.execute_action(
            action,
            trigger,
            conn,
            embedder,
            cfg,
            model_limit=model_limit,
            read_pool=read_pool,
            read_fanout_budget=read_fanout_budget,
        )
    return await _execute_semantic_hybrid_action_uncached(
        action,
        trigger,
        conn,
        embedder,
        cfg,
        model_limit=model_limit,
        read_pool=read_pool,
        read_fanout_budget=read_fanout_budget,
    )


async def execute_semantic_terms_action(
    action: RetrievalAction,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    cfg: InquiryConfig,
    *,
    model_limit: int,
    read_pool: asyncpg.Pool | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
) -> PathwayResult | None:
    query_text = action.query or _trigger_text(trigger)
    seed_entities = _action_seed_entities(action, trigger)
    result, note = await _semantic_term_rescue(
        trigger,
        conn,
        query_text=query_text,
        seed_entities=seed_entities,
        model_limit=max(1, int(model_limit)),
        read_pool=read_pool,
        read_fanout_budget=read_fanout_budget,
    )
    if result is None:
        return None
    notes = dict(result.notes or {})
    notes["semantic_terms_action"] = {
        **dict(note or {}),
        "query_text_chars": len(query_text),
        "model_limit": max(1, int(model_limit)),
    }
    result.notes = notes
    cap_pathway_models(result, max(1, int(model_limit)))
    return result


async def _timed_semantic_subtask(
    label: str,
    awaitable: Any,
) -> tuple[str, Any, int]:
    started = time.perf_counter()
    value = await awaitable
    return label, value, int((time.perf_counter() - started) * 1000)


async def _execute_semantic_hybrid_action_uncached(
    action: RetrievalAction,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    embedder: Any | None,
    cfg: InquiryConfig,
    *,
    model_limit: int,
    query_text: str | None = None,
    seed_entities: list[dict[str, Any]] | None = None,
    read_pool: asyncpg.Pool | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
    active_sparse_model_counts: dict[UUID, int] | None = None,
    schema_capabilities: dict[str, bool] | None = None,
) -> PathwayResult:
    model_limit = max(1, int(model_limit))
    query_text = query_text or action.query or _trigger_text(trigger)
    seed_entities = (
        seed_entities
        if seed_entities is not None
        else _action_seed_entities(action, trigger)
    )
    trigger_text = _trigger_text(trigger)
    precomputed_vector = (
        trigger.precomputed_seed_vector
        if embedder is None or query_text == trigger_text
        else None
    )

    async def run_dense(read_conn: asyncpg.Connection) -> PathwayResult:
        return await pathway_b_semantic(
            query_text,
            trigger.tenant_id,
            read_conn,
            k=model_limit,
            embedder=embedder,
            precomputed_vector=precomputed_vector,
            event_actors=trigger.scope_actors,
            event_entities=seed_entities,
        )

    semantic_read_budget = (
        read_fanout_budget
        if read_fanout_budget is not None
        else ReadFanoutBudget.from_pool(read_pool)
        if read_pool is not None
        else None
    )

    if read_pool is None:
        sub_timings: dict[str, int] = {}
        _label, result, sub_timings["dense_ms"] = await _timed_semantic_subtask(
            "dense",
            run_dense(conn),
        )
        (
            _label,
            (semantic_term_hits, semantic_term_note),
            sub_timings["semantic_terms_ms"],
        ) = await _timed_semantic_subtask(
            "semantic_terms",
            _semantic_term_candidate_rescue(
                trigger,
                conn,
                query_text=query_text,
                seed_entities=seed_entities,
                model_limit=model_limit,
                schema_capabilities=schema_capabilities,
            ),
        )
        (
            _label,
            (representation_hits, representation_note),
            sub_timings["representation_tags_ms"],
        ) = await _timed_semantic_subtask(
            "representation_tags",
            _representation_tag_candidate_rescue(
                trigger,
                conn,
                query_text=query_text,
                model_limit=model_limit,
                schema_capabilities=schema_capabilities,
            ),
        )
        _label, (lexical_hits, hybrid_note), sub_timings["lexical_ms"] = (
            await _timed_semantic_subtask(
                "lexical",
                _semantic_hybrid_lexical_rescue(
                    trigger,
                    conn,
                    cfg,
                    query_text=query_text,
                    model_limit=model_limit,
                    active_sparse_model_counts=active_sparse_model_counts,
                ),
            )
        )
        fanout_parallel = False
    else:
        dense_task = asyncio.create_task(
            _timed_semantic_subtask(
                "dense",
                _run_with_optional_pool(
                    conn,
                    read_pool,
                    run_dense,
                    read_fanout_budget=semantic_read_budget,
                ),
            )
        )
        semantic_term_task = asyncio.create_task(
            _timed_semantic_subtask(
                "semantic_terms",
                _semantic_term_candidate_rescue(
                    trigger,
                    conn,
                    query_text=query_text,
                    seed_entities=seed_entities,
                    model_limit=model_limit,
                    read_pool=read_pool,
                    read_fanout_budget=semantic_read_budget,
                    schema_capabilities=schema_capabilities,
                ),
            )
        )
        representation_task = asyncio.create_task(
            _timed_semantic_subtask(
                "representation_tags",
                _representation_tag_candidate_rescue(
                    trigger,
                    conn,
                    query_text=query_text,
                    model_limit=model_limit,
                    read_pool=read_pool,
                    read_fanout_budget=semantic_read_budget,
                    schema_capabilities=schema_capabilities,
                ),
            )
        )
        lexical_task = asyncio.create_task(
            _timed_semantic_subtask(
                "lexical",
                _semantic_hybrid_lexical_rescue(
                    trigger,
                    conn,
                    cfg,
                    query_text=query_text,
                    model_limit=model_limit,
                    read_pool=read_pool,
                    read_fanout_budget=semantic_read_budget,
                ),
            )
        )
        tasks = (dense_task, semantic_term_task, representation_task, lexical_task)
        try:
            results = await asyncio.gather(*tasks)
        except Exception:
            for task in tasks:
                task.cancel()
            raise
        sub_timings = {f"{label}_ms": elapsed for label, _value, elapsed in results}
        by_label = {label: value for label, value, _elapsed in results}
        result = by_label["dense"]
        semantic_term_hits, semantic_term_note = by_label["semantic_terms"]
        representation_hits, representation_note = by_label["representation_tags"]
        lexical_hits, hybrid_note = by_label["lexical"]
        fanout_parallel = True

    semantic_ids = {model.id for model in result.models}

    merge_started = time.perf_counter()
    candidate_merge_note: dict[str, Any] = {}
    result.models = await merge_semantic_substrate_candidate_models(
        trigger.tenant_id,
        conn,
        result.models,
        semantic_term_hits,
        representation_hits,
        lexical_hits,
        limit=max(1, int(model_limit)),
        notes=candidate_merge_note,
    )
    sub_timings["merge_hydrate_ms"] = int((time.perf_counter() - merge_started) * 1000)
    lexical_ids = {model.id for model, _ in lexical_hits}
    semantic_term_ids = {hit.model_id for hit in semantic_term_hits}
    representation_ids = {hit.model_id for hit in representation_hits}
    selected_ids = {model.id for model in result.models}
    rescue_scope_note = {
        "scope_basis": {
            "entity_count": len(_canonical_scope_entity_pairs(seed_entities)),
            "actor_count": len(trigger.scope_actors or []),
        },
        "semantic_terms": _semantic_selected_scope_accounting(
            result.models,
            semantic_term_ids,
            seed_entities=seed_entities,
            scope_actors=trigger.scope_actors,
        ),
        "representation_tags": _semantic_selected_scope_accounting(
            result.models,
            representation_ids,
            seed_entities=seed_entities,
            scope_actors=trigger.scope_actors,
        ),
        "lexical": _semantic_selected_scope_accounting(
            result.models,
            lexical_ids,
            seed_entities=seed_entities,
            scope_actors=trigger.scope_actors,
        ),
    }
    semantic_term_note["candidate_model_ids"] = [
        str(model_id) for model_id in sorted(semantic_term_ids, key=str)
    ]
    semantic_term_note["selected_model_ids"] = [
        str(model_id) for model_id in sorted(semantic_term_ids & selected_ids, key=str)
    ]
    semantic_term_note["rescued_model_ids"] = [
        str(model_id)
        for model_id in sorted(
            (semantic_term_ids & selected_ids) - semantic_ids, key=str
        )
    ]
    representation_note["candidate_model_ids"] = [
        str(model_id) for model_id in sorted(representation_ids, key=str)
    ]
    representation_note["selected_model_ids"] = [
        str(model_id) for model_id in sorted(representation_ids & selected_ids, key=str)
    ]
    representation_note["rescued_model_ids"] = [
        str(model_id)
        for model_id in sorted(
            (representation_ids & selected_ids) - semantic_ids, key=str
        )
    ]
    hybrid_note["used"] = bool(lexical_hits)
    hybrid_note["semantic_count"] = len(semantic_ids)
    hybrid_note["merged_count"] = len(result.models)
    hybrid_note["lexical_only_selected"] = sum(
        1
        for model in result.models
        if model.id in lexical_ids and model.id not in semantic_ids
    )
    result.notes["semantic_hybrid_lexical"] = hybrid_note
    result.notes["semantic_term_rescue"] = semantic_term_note
    result.notes["semantic_hybrid_semantic_terms"] = semantic_term_note
    result.notes["representation_tag_rescue"] = representation_note
    result.notes["semantic_rescue_scope"] = rescue_scope_note
    result.notes["semantic_hybrid_substrates"] = {
        "dense": {
            "model_ids": [str(model_id) for model_id in sorted(semantic_ids, key=str)]
        },
        "semantic_terms": semantic_term_note,
        "representation_tags": representation_note,
        "lexical": hybrid_note,
    }
    result.notes["semantic_substrate"] = {
        "cache_hit": False,
        "cache_wait": False,
        "dense_count": len(semantic_ids),
        "semantic_term_count": len(semantic_term_ids),
        "representation_tag_count": len(representation_ids),
        "lexical_count": len(lexical_hits),
        "merged_count": len(result.models),
        "fanout_parallel": fanout_parallel,
        "candidate_merge": candidate_merge_note,
        "cross_scope_rescue_selected": (
            rescue_scope_note["semantic_terms"]["cross_scope_selected"]
            + rescue_scope_note["representation_tags"]["cross_scope_selected"]
            + rescue_scope_note["lexical"]["cross_scope_selected"]
        ),
        "semantic_term_only_selected": sum(
            1
            for model in result.models
            if model.id in semantic_term_ids and model.id not in semantic_ids
        ),
        "representation_tag_only_selected": sum(
            1
            for model in result.models
            if model.id in representation_ids and model.id not in semantic_ids
        ),
        "lexical_only_selected": hybrid_note["lexical_only_selected"],
    }
    result.notes["semantic_substrate_timings_ms"] = sub_timings
    if semantic_read_budget is not None:
        budget_snapshot = semantic_read_budget.snapshot()
        result.notes["semantic_substrate"]["read_fanout_budget"] = {
            "max_concurrency": budget_snapshot.max_concurrency,
            "peak_in_use": budget_snapshot.peak_in_use,
            "acquired": budget_snapshot.acquired,
            "denied": budget_snapshot.denied,
        }
    return result


async def _run_with_optional_pool(
    conn: asyncpg.Connection,
    read_pool: asyncpg.Pool | None,
    fn: Any,
    *,
    read_fanout_budget: ReadFanoutBudget | None = None,
) -> Any:
    if read_fanout_budget is not None:
        async with read_fanout_budget.connection() as read_conn:
            return await fn(read_conn)
    if read_pool is None:
        return await fn(conn)
    async with read_pool.acquire() as read_conn:
        return await fn(read_conn)


async def _cached_schema_capability(
    conn: asyncpg.Connection,
    schema_capabilities: dict[str, bool] | None,
    *,
    key: str,
    query: str,
) -> bool | None:
    if schema_capabilities is None:
        return None
    cached = schema_capabilities.get(key)
    if cached is not None:
        return cached
    value = bool(await conn.fetchval(query))
    schema_capabilities[key] = value
    return value


async def _semantic_term_candidate_rescue(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    query_text: str,
    seed_entities: list[dict[str, Any]],
    model_limit: int,
    read_pool: asyncpg.Pool | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
    schema_capabilities: dict[str, bool] | None = None,
) -> tuple[list[ModelCandidateHit], dict[str, Any]]:
    limit = max(1, min(80, int(model_limit) * 2))

    async def run(
        read_conn: asyncpg.Connection,
    ) -> tuple[list[ModelCandidateHit], dict[str, Any]]:
        feature_postings_available = await _cached_schema_capability(
            read_conn,
            schema_capabilities,
            key="model_representation_feature_postings",
            query="SELECT to_regclass('public.model_representation_feature_postings')",
        )
        postings_available = feature_postings_available
        postings_status_column = None
        if feature_postings_available is True:
            postings_status_column = True
        elif feature_postings_available is False:
            postings_available = await _cached_schema_capability(
                read_conn,
                schema_capabilities,
                key="model_semantic_term_postings",
                query="SELECT to_regclass('public.model_semantic_term_postings')",
            )
        if postings_available and feature_postings_available is not True:
            postings_status_column = await _cached_schema_capability(
                read_conn,
                schema_capabilities,
                key="model_semantic_term_postings.status",
                query="""
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.columns
                  WHERE table_schema = 'public'
                    AND table_name = 'model_semantic_term_postings'
                    AND column_name = 'status'
                )
                """,
            )
        return await pathway_l_semantic_term_candidates(
            query_text,
            trigger.tenant_id,
            read_conn,
            seed_signature=(
                trigger.seed_signature if isinstance(trigger.seed_signature, dict) else None
            ),
            scope_actors=trigger.scope_actors,
            scope_entities=seed_entities,
            limit=limit,
            semantic_feature_postings_available=feature_postings_available,
            semantic_postings_available=postings_available,
            semantic_postings_status_column=postings_status_column,
        )

    try:
        hits, note = await _run_with_optional_pool(
            conn,
            read_pool,
            run,
            read_fanout_budget=read_fanout_budget,
        )
    except Exception as exc:  # noqa: BLE001
        return [], {"enabled": True, "error": type(exc).__name__, "limit": limit}
    note = dict(note or {})
    note["enabled"] = True
    note["used"] = bool(hits)
    return hits, note


async def _representation_tag_candidate_rescue(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    query_text: str,
    model_limit: int,
    read_pool: asyncpg.Pool | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
    schema_capabilities: dict[str, bool] | None = None,
) -> tuple[list[ModelCandidateHit], dict[str, Any]]:
    limit = max(20, min(120, int(model_limit) * 2))

    async def run(
        read_conn: asyncpg.Connection,
    ) -> tuple[list[ModelCandidateHit], dict[str, Any]]:
        feature_postings_available = await _cached_schema_capability(
            read_conn,
            schema_capabilities,
            key="model_representation_feature_postings",
            query="SELECT to_regclass('public.model_representation_feature_postings')",
        )
        postings_available = feature_postings_available
        if feature_postings_available is False:
            postings_available = await _cached_schema_capability(
                read_conn,
                schema_capabilities,
                key="model_representation_tag_postings",
                query="SELECT to_regclass('public.model_representation_tag_postings')",
            )
        return await pathway_b_representation_tag_candidates(
            query_text,
            trigger.tenant_id,
            read_conn,
            seed_signature=(
                trigger.seed_signature if isinstance(trigger.seed_signature, dict) else None
            ),
            limit=limit,
            representation_feature_postings_available=feature_postings_available,
            representation_postings_available=postings_available,
        )

    try:
        hits, note = await _run_with_optional_pool(
            conn,
            read_pool,
            run,
            read_fanout_budget=read_fanout_budget,
        )
    except Exception as exc:  # noqa: BLE001
        return [], {"enabled": True, "error": type(exc).__name__, "limit": limit}
    note = dict(note or {})
    note["enabled"] = True
    note["used"] = bool(hits)
    return hits, note


async def _semantic_term_rescue(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    query_text: str,
    seed_entities: list[dict[str, Any]],
    model_limit: int,
    read_pool: asyncpg.Pool | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
    schema_capabilities: dict[str, bool] | None = None,
) -> tuple[PathwayResult | None, dict[str, Any]]:
    limit = max(1, min(80, int(model_limit) * 2))

    async def run(read_conn: asyncpg.Connection) -> PathwayResult:
        feature_postings_available = await _cached_schema_capability(
            read_conn,
            schema_capabilities,
            key="model_representation_feature_postings",
            query="SELECT to_regclass('public.model_representation_feature_postings')",
        )
        postings_available = feature_postings_available
        postings_status_column = None
        if feature_postings_available is True:
            postings_status_column = True
        elif feature_postings_available is False:
            postings_available = await _cached_schema_capability(
                read_conn,
                schema_capabilities,
                key="model_semantic_term_postings",
                query="SELECT to_regclass('public.model_semantic_term_postings')",
            )
        if postings_available and feature_postings_available is not True:
            postings_status_column = await _cached_schema_capability(
                read_conn,
                schema_capabilities,
                key="model_semantic_term_postings.status",
                query="""
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.columns
                  WHERE table_schema = 'public'
                    AND table_name = 'model_semantic_term_postings'
                    AND column_name = 'status'
                )
                """,
            )
        return await pathway_l_semantic_terms(
            query_text,
            trigger.tenant_id,
            read_conn,
            seed_signature=(
                trigger.seed_signature
                if isinstance(trigger.seed_signature, dict)
                else None
            ),
            scope_actors=trigger.scope_actors,
            scope_entities=seed_entities,
            limit=limit,
            semantic_feature_postings_available=feature_postings_available,
            semantic_postings_available=postings_available,
            semantic_postings_status_column=postings_status_column,
        )

    try:
        result = await _run_with_optional_pool(
            conn,
            read_pool,
            run,
            read_fanout_budget=read_fanout_budget,
        )
    except Exception as exc:  # noqa: BLE001
        return None, {"enabled": True, "error": type(exc).__name__, "limit": limit}
    note = dict(result.notes or {})
    note["enabled"] = True
    note["used"] = bool(result.models)
    return result, note


async def _representation_tag_rescue(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    query_text: str,
    model_limit: int,
    read_pool: asyncpg.Pool | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
    schema_capabilities: dict[str, bool] | None = None,
) -> tuple[PathwayResult | None, dict[str, Any]]:
    limit = max(20, min(120, int(model_limit) * 2))

    async def run(read_conn: asyncpg.Connection) -> PathwayResult:
        feature_postings_available = await _cached_schema_capability(
            read_conn,
            schema_capabilities,
            key="model_representation_feature_postings",
            query="SELECT to_regclass('public.model_representation_feature_postings')",
        )
        postings_available = feature_postings_available
        if feature_postings_available is False:
            postings_available = await _cached_schema_capability(
                read_conn,
                schema_capabilities,
                key="model_representation_tag_postings",
                query="SELECT to_regclass('public.model_representation_tag_postings')",
            )
        return await pathway_b_representation_tags(
            query_text,
            trigger.tenant_id,
            read_conn,
            seed_signature=(
                trigger.seed_signature
                if isinstance(trigger.seed_signature, dict)
                else None
            ),
            limit=limit,
            representation_feature_postings_available=feature_postings_available,
            representation_postings_available=postings_available,
        )

    try:
        result = await _run_with_optional_pool(
            conn,
            read_pool,
            run,
            read_fanout_budget=read_fanout_budget,
        )
    except Exception as exc:  # noqa: BLE001
        return None, {"enabled": True, "error": type(exc).__name__, "limit": limit}
    note = dict(result.notes or {})
    note["enabled"] = True
    note["used"] = bool(result.models)
    return result, note


async def _cached_active_sparse_model_count(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    active_sparse_model_counts: dict[UUID, int] | None,
) -> int | None:
    if active_sparse_model_counts is None:
        return None
    cached = active_sparse_model_counts.get(tenant_id)
    if cached is not None:
        return cached
    table = await conn.fetchval("SELECT to_regclass('public.model_sparse_terms')")
    if table is None:
        return None
    rows = await fetch_bounded_lookup_rows(
        conn,
        f"""
        SELECT greatest(1, count(*)::int) AS active_model_count
        FROM accepted_current_models
        WHERE tenant_id = $1
        """,
        tenant_id,
        label="hybrid_sparse_active_model_count",
    )
    if _bounded_lookup_timed_out(rows) or not rows:
        return None
    count = max(1, int(rows[0]["active_model_count"] or 1))
    active_sparse_model_counts[tenant_id] = count
    return count


async def _semantic_hybrid_lexical_rescue(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    cfg: InquiryConfig,
    *,
    query_text: str,
    model_limit: int,
    read_pool: asyncpg.Pool | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
    active_sparse_model_counts: dict[UUID, int] | None = None,
) -> tuple[list[tuple[ModelRow, int]], dict[str, Any]]:
    hybrid_note: dict[str, Any] = {
        "enabled": bool(cfg.semantic_hybrid_lexical_enabled),
        "used": False,
        "lexical_count": 0,
        "merged_count": 0,
    }
    if not cfg.semantic_hybrid_lexical_enabled:
        hybrid_note["reason"] = "disabled"
        return [], hybrid_note

    terms = _hybrid_lexical_terms(
        query_text,
        trigger,
        max_terms=max(1, int(cfg.semantic_hybrid_lexical_terms)),
    )
    hybrid_note["terms"] = terms
    if not terms:
        hybrid_note["reason"] = "no_lexical_terms"
        return [], hybrid_note
    lookup_terms = _hybrid_lookup_terms(
        terms,
        max_terms=max(1, int(cfg.semantic_hybrid_lexical_terms)),
    )
    hybrid_note["lookup_terms"] = lookup_terms
    if not lookup_terms:
        hybrid_note["reason"] = "no_specific_lexical_terms"
        return [], hybrid_note

    lexical_limit = min(
        max(1, int(cfg.semantic_hybrid_lexical_max_candidates)),
        max(1, int(model_limit) * 2),
    )
    per_term_limit = max(1, int(cfg.semantic_hybrid_lexical_per_term_limit))

    async def run(read_conn: asyncpg.Connection) -> list[tuple[ModelRow, int]]:
        active_sparse_model_count = await _cached_active_sparse_model_count(
            read_conn,
            tenant_id=trigger.tenant_id,
            active_sparse_model_counts=active_sparse_model_counts,
        )
        return await hybrid_lexical_model_scan(
            trigger,
            read_conn,
            terms=lookup_terms,
            limit=lexical_limit,
            per_term_limit=per_term_limit,
            active_sparse_model_count=active_sparse_model_count,
        )

    try:
        lexical_hits = await _run_with_optional_pool(
            conn,
            read_pool,
            run,
            read_fanout_budget=read_fanout_budget,
        )
    except Exception as exc:  # noqa: BLE001
        hybrid_note.update(
            {
                "error": type(exc).__name__,
                "lexical_limit": lexical_limit,
                "lexical_per_term_limit": per_term_limit,
            }
        )
        return [], hybrid_note
    hybrid_note.update(
        {
            "lexical_limit": lexical_limit,
            "lexical_per_term_limit": per_term_limit,
            "lexical_count": len(lexical_hits),
            "active_sparse_model_count_cached": (
                active_sparse_model_counts is not None
                and trigger.tenant_id in active_sparse_model_counts
            ),
        }
    )
    if not lexical_hits:
        hybrid_note["reason"] = "no_lexical_hits"
    return lexical_hits, hybrid_note


def merge_semantic_substrate_models(
    semantic_models: list[ModelRow],
    semantic_term_models: list[ModelRow],
    representation_tag_models: list[ModelRow],
    lexical_hits: list[tuple[ModelRow, int]],
    *,
    limit: int,
) -> list[ModelRow]:
    scores: dict[UUID, float] = {}
    by_id: dict[UUID, ModelRow] = {}
    ranks: dict[UUID, list[int]] = {}
    rrf_k = 60.0

    def ensure(model: ModelRow) -> list[int]:
        by_id.setdefault(model.id, model)
        return ranks.setdefault(model.id, [10_000, 10_000, 10_000, 10_000])

    for rank, model in enumerate(semantic_models, start=1):
        current = ensure(model)
        current[0] = min(current[0], rank)
        scores[model.id] = scores.get(model.id, 0.0) + 1.0 / (rrf_k + rank)

    for rank, model in enumerate(semantic_term_models, start=1):
        current = ensure(model)
        current[1] = min(current[1], rank)
        scores[model.id] = scores.get(model.id, 0.0) + 1.08 / (rrf_k + rank)

    for rank, (model, match_count) in enumerate(lexical_hits, start=1):
        current = ensure(model)
        current[2] = min(current[2], rank)
        lexical_score = 0.92 / (rrf_k + rank)
        lexical_score += min(0.008, max(1, int(match_count)) * 0.002)
        scores[model.id] = scores.get(model.id, 0.0) + lexical_score

    for rank, model in enumerate(representation_tag_models, start=1):
        current = ensure(model)
        current[3] = min(current[3], rank)
        scores[model.id] = scores.get(model.id, 0.0) + 0.72 / (rrf_k + rank)

    ordered_ids = sorted(
        by_id,
        key=lambda model_id: (
            -scores.get(model_id, 0.0),
            *ranks.get(model_id, [10_000, 10_000, 10_000, 10_000]),
            -float(getattr(by_id[model_id], "activation", 0.0) or 0.0),
            str(model_id),
        ),
    )
    return [by_id[model_id] for model_id in ordered_ids[: max(1, int(limit))]]


async def merge_semantic_substrate_candidate_models(
    tenant_id: UUID,
    conn: asyncpg.Connection,
    semantic_models: list[ModelRow],
    semantic_term_hits: list[ModelCandidateHit],
    representation_tag_hits: list[ModelCandidateHit],
    lexical_hits: list[tuple[ModelRow, int]],
    *,
    limit: int,
    notes: dict[str, Any] | None = None,
) -> list[ModelRow]:
    scores: dict[UUID, float] = {}
    by_id: dict[UUID, ModelRow] = {}
    ranks: dict[UUID, list[int]] = {}
    activations: dict[UUID, float] = {}
    rrf_k = 60.0

    def ensure_id(model_id: UUID, activation: float = 0.0) -> list[int]:
        activations[model_id] = max(activations.get(model_id, 0.0), float(activation))
        return ranks.setdefault(model_id, [10_000, 10_000, 10_000, 10_000])

    def ensure_model(model: ModelRow) -> list[int]:
        by_id.setdefault(model.id, model)
        return ensure_id(model.id, float(getattr(model, "activation", 0.0) or 0.0))

    for rank, model in enumerate(semantic_models, start=1):
        current = ensure_model(model)
        current[0] = min(current[0], rank)
        scores[model.id] = scores.get(model.id, 0.0) + 1.0 / (rrf_k + rank)

    for rank, hit in enumerate(semantic_term_hits, start=1):
        current = ensure_id(hit.model_id, hit.activation)
        current[1] = min(current[1], rank)
        scores[hit.model_id] = scores.get(hit.model_id, 0.0) + 1.08 / (rrf_k + rank)

    for rank, (model, match_count) in enumerate(lexical_hits, start=1):
        current = ensure_model(model)
        current[2] = min(current[2], rank)
        lexical_score = 0.92 / (rrf_k + rank)
        lexical_score += min(0.008, max(1, int(match_count)) * 0.002)
        scores[model.id] = scores.get(model.id, 0.0) + lexical_score

    for rank, hit in enumerate(representation_tag_hits, start=1):
        current = ensure_id(hit.model_id, hit.activation)
        current[3] = min(current[3], rank)
        scores[hit.model_id] = scores.get(hit.model_id, 0.0) + 0.72 / (rrf_k + rank)

    ordered_ids = sorted(
        scores,
        key=lambda model_id: (
            -scores.get(model_id, 0.0),
            *ranks.get(model_id, [10_000, 10_000, 10_000, 10_000]),
            -activations.get(model_id, 0.0),
            str(model_id),
        ),
    )
    selected_ids = ordered_ids[: max(1, int(limit))]
    missing_ids = [model_id for model_id in selected_ids if model_id not in by_id]
    hydration_notes: dict[str, Any] = {}
    if missing_ids:
        hydrated = await hydrate_active_models_by_ids(
            tenant_id,
            conn,
            missing_ids,
            notes=hydration_notes,
            bucket="models",
        )
        for model in hydrated:
            by_id.setdefault(model.id, model)

    if notes is not None:
        semantic_ids = {model.id for model in semantic_models}
        lexical_ids = {model.id for model, _ in lexical_hits}
        notes.update(
            {
                "candidate_ids_considered": len(scores),
                "candidate_ids_selected": len(selected_ids),
                "candidate_ids_hydrated": len(missing_ids),
                "hydrated_model_ids": [
                    str(model_id) for model_id in missing_ids if model_id in by_id
                ],
                "deferred_candidate_model_ids": [
                    str(model_id)
                    for model_id in selected_ids
                    if model_id not in semantic_ids and model_id not in lexical_ids
                ],
                "hydration": hydration_notes,
            }
        )
    return [by_id[model_id] for model_id in selected_ids if model_id in by_id]


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
    active_sparse_model_count: int | None = None,
) -> list[tuple[ModelRow, int]]:
    lookup_terms = _hybrid_lookup_terms(terms)
    if not lookup_terms or limit <= 0:
        return []
    try:
        sparse_hits = await hybrid_sparse_model_scan(
            trigger,
            conn,
            terms=lookup_terms,
            limit=limit,
            per_term_limit=per_term_limit,
            raise_on_timeout=True,
            active_sparse_model_count=active_sparse_model_count,
        )
    except _HybridSparseLookupTimedOut:
        return []
    if sparse_hits:
        return sparse_hits

    patterns = _like_patterns_for_terms(lookup_terms)
    if not patterns or limit <= 0:
        return []
    table = await conn.fetchval("SELECT to_regclass('public.model_search_documents')")
    if table is None:
        return []
    rows = await fetch_bounded_lookup_rows(
        conn,
        f"""
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
            WHERE msd.tenant_id = $1
              AND msd.status = 'active'
              AND msd.search_text LIKE p.pattern ESCAPE '!'
            ORDER BY msd.model_id
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
        JOIN {_ACCEPTED_MODEL_ROWS_SQL} AS m
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
    raise_on_timeout: bool = False,
    active_sparse_model_count: int | None = None,
) -> list[tuple[ModelRow, int]]:
    lookup_terms = _hybrid_sparse_lookup_terms(terms)
    if not lookup_terms or limit <= 0:
        return []
    if active_sparse_model_count is None:
        table = await conn.fetchval("SELECT to_regclass('public.model_sparse_terms')")
        if table is None:
            return []
    active_models_sql = (
        """
        active_models AS MATERIALIZED (
          SELECT greatest(1, $7::int)::float8 AS active_model_count
        ),
        """
        if active_sparse_model_count is not None
        else """
        active_models AS MATERIALIZED (
          SELECT greatest(1, count(*)::int)::float8 AS active_model_count
          FROM accepted_current_models
          WHERE tenant_id = $1
        ),
        """
    )
    params: list[Any] = [
        trigger.tenant_id,
        max(1, int(limit)),
        lookup_terms,
        max(1, int(per_term_limit)),
        _hybrid_sparse_strong_single_match_terms(lookup_terms),
        _SPARSE_STRONG_SINGLE_MATCH_MAX_DF,
    ]
    if active_sparse_model_count is not None:
        params.append(max(1, int(active_sparse_model_count)))
    rows = await fetch_bounded_lookup_rows(
        conn,
        f"""
        WITH query_terms AS MATERIALIZED (
          SELECT term::text,
                 ord::int AS term_ord
          FROM unnest($3::text[]) WITH ORDINALITY AS q(term, ord)
        ),
        query_meta AS MATERIALIZED (
          SELECT count(*)::int AS query_term_count
          FROM query_terms
        ),
        {active_models_sql}
        term_stats AS MATERIALIZED (
          SELECT qt.term,
                 qt.term_ord,
                 count(mst.model_id)::int AS term_df
          FROM query_terms qt
          LEFT JOIN model_sparse_terms mst
            ON mst.tenant_id = $1
           AND mst.status = 'active'
           AND mst.term = qt.term
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
            WHERE mst.tenant_id = $1
              AND mst.status = 'active'
              AND mst.term = ts.term
            ORDER BY mst.weight DESC,
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
        JOIN {_ACCEPTED_MODEL_ROWS_SQL} AS m
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
        *params,
        label="hybrid_sparse",
    )
    if _bounded_lookup_timed_out(rows) and raise_on_timeout:
        raise _HybridSparseLookupTimedOut
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
            return _BoundedLookupRows(list(await conn.fetch(query, *args)))
    except asyncpg.QueryCanceledError:
        import structlog

        structlog.get_logger(__name__).warning(
            "inquiry.bounded_lookup_statement_timeout",
            label=label,
            timeout_ms=_LEXICAL_FALLBACK_STATEMENT_TIMEOUT_MS,
        )
        return _BoundedLookupRows(timed_out=True)
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
    return merge_semantic_substrate_models(
        semantic_models,
        [],
        [],
        lexical_hits,
        limit=limit,
    )


__all__ = [
    "FocusedIndexHit",
    "SemanticRetrievalSession",
    "cap_pathway_models",
    "execute_focused_index_action",
    "execute_semantic_hybrid_action",
    "execute_semantic_terms_action",
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
    "merge_semantic_substrate_candidate_models",
    "merge_semantic_substrate_models",
]
