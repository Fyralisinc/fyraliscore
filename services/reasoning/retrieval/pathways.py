"""
services/reasoning/retrieval/pathways.py — the primary retrieval pathways.

Spec reference: ARCHITECTURE-FINAL.md §8 "Primary pathway resolver".
BUILD-PLAN reference: §4 Prompt 3.A item 1.

Each pathway is a pure async function that takes a seed + the caller's
asyncpg connection + a tenant_id, hits the DB a small, bounded number
of times, and returns a `PathwayResult`. No pathway mutates any row.
Reconsolidation (calling `ModelsRepo.retrieve`) is the caller's
responsibility — it lives in `primary.py` so second_pass can re-use
the same transaction.

Invariants:
  - Every query filters by `tenant_id` (tenant isolation; spec §26 L1).
  - Every Model query filters by `status='active'` to hit the partial
    indexes (S2.2) and avoid surfacing archived / contested_false /
    superseded Models.
  - Empty seeds return an empty PathwayResult cleanly — never an error.
  - JSONB codec is assumed installed on `conn` by the caller (tests do
    this in conftest; production callers do this via the shared pool
    initializer).

Why this module does not own the pool: retrieval must run INSIDE the
caller's transaction. Think will open one transaction for retrieve +
reason + apply + state_change emission, and we must be on that same
connection so pre-commit state is visible.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal, Sequence
from uuid import UUID

import asyncpg

from lib.embeddings.ollama import EMBEDDING_DIM, OllamaClient, OllamaError
from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.types import (
    CommitmentRow,
    DecisionRow,
    GoalRow,
    ModelRow,
    ObservationRow,
    ResourceRow,
)


# ---------------------------------------------------------------------
# Constants + types
# ---------------------------------------------------------------------

PathwayName = Literal["A", "B", "C", "D", "G"]

_DEFAULT_K_SEMANTIC = 40
_DEFAULT_TEMPORAL_WINDOW_DAYS = 7
_DEFAULT_STRUCTURAL_MAX_HOPS = 2
_STRUCTURAL_MAX_MODELS = 200
_STRUCTURAL_MODELS_PER_SCOPE_ENTITY = 32
_STRUCTURAL_MODELS_PER_SCOPE_ACTOR = 48
_STRUCTURAL_MAX_SCOPE_ENTITY_FILTERS = 64
_TEMPORAL_MAX_OBSERVATIONS = 300
_PATTERN_MAX_INSTANCES = 200
_DEFAULT_EDGE_MAX_HOPS = 2
_EDGE_MAX_MODELS = 120
_EDGE_TRAVERSAL_KINDS = (
    "contradicts",
    "weakens",
    "blocks",
    "early_warning_for",
    "same_issue_as",
    "causes",
    "explains",
    "predicts",
    "enables",
    "supports",
    "instance_of",
    "co_occurs_with",
    "analogous_to",
    "alternative_to",
    "contributes_to_resolution",
    "superseded_by",
)


class RetrievalPathwayError(CompanyOSError):
    default_code = "retrieval_pathway_error"


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def _append_timing(
    notes: dict[str, Any],
    stage: str,
    started: float,
    **extra: Any,
) -> None:
    timing = {
        "stage": stage,
        "elapsed_ms": _elapsed_ms(started),
    }
    for key, value in extra.items():
        if value is not None:
            timing[key] = value
    notes.setdefault("timings", []).append(timing)


# ModelRow SELECT columns must match models/repo.py._SELECT_COLS exactly
# so that hydrated rows share shape. We copy the list verbatim — a
# deliberate duplication (deviation (b)) noted in the BUILD-LOG; the
# public ModelsRepo API does not yet expose a raw SQL `retrieve_by_...`
# that returns ModelRow by scope, so retrieval composes its own queries
# against the columns list. Wave 5 could refactor this into a thin
# public method on ModelsRepo.
_MODEL_SELECT_COLS = (
    "id", "tenant_id", "born_from_event_id",
    "proposition", '"natural" AS natural', "embedding",
    "scope_actors", "scope_entities", "scope_temporal",
    "confidence", "activation", "falsifier",
    "signal_readings", "reading_contestable",
    "supporting_event_ids", "supporting_model_ids", "evidential_weight",
    "status", "archived_at", "archive_reason",
    "created_at", "last_retrieved_at", "retrieval_count",
    "evaluate_at", "resolution_criteria", "contributing_models",
    "visible_to_subjects",
    "proposition_kind",
    "claim_role", "abstraction_level", "time_mode", "modality", "polarity",
    "domain_tags", "memory_grammar_version",
    "confirmed_count", "contested_count", "last_confirmed_at",
    "confidence_at_assertion",
    "resolved_at", "resolution_outcome",
    "activation_coefficient",
    "target_actor_id", "caused_act_change_id",
)
_MODEL_SELECT_SQL = ", ".join(_MODEL_SELECT_COLS)

_OBS_SELECT_COLS = (
    "id", "tenant_id", "occurred_at", "ingested_at", "kind",
    "source_channel", "source_actor_ref", "actor_id",
    "content", "content_text", "embedding", "embedding_pending",
    "trust_tier", "external_id", "cause_id", "sequence_num",
    "entities_mentioned",
)
_OBS_SELECT_SQL = ", ".join(_OBS_SELECT_COLS)


@dataclass
class PathwayResult:
    """
    Return shape for every pathway. Note:

    - `models` is always populated (every pathway's primary product).
    - `observations` is populated by pathway C (and only pathway C).
    - `acts` is populated by pathway A (the structural walker is the
      only one that enumerates Commitments / Goals / Decisions by id).
    - `resources` is populated by pathway A when the walk touches a
      customer Resource or a Capacity Resource via depends_on.
    - `source_pathway` is the literal letter, used for diagnostics
      + weighted merging in `primary.primary_retrieve`.
    - `notes` is a per-pathway diagnostics dict (hops, seeds_used,
      k_effective, etc.) for observability; tests assert against it.
    """

    models: list[ModelRow] = field(default_factory=list)
    observations: list[ObservationRow] = field(default_factory=list)
    acts: dict[str, list] = field(
        default_factory=lambda: {"goals": [], "commitments": [], "decisions": []}
    )
    resources: list[ResourceRow] = field(default_factory=list)
    source_pathway: PathwayName = "A"
    notes: dict[str, Any] = field(default_factory=dict)

    def model_ids(self) -> list[UUID]:
        return [m.id for m in self.models]


# ---------------------------------------------------------------------
# Row hydration helpers
#
# These duplicate the hydration in models/repo.py and observations/repo.py
# because those methods are not reusable from outside (they are private
# to the repo). Duplication is intentional and documented; a refactor
# to hoist into lib/shared is a Wave 5 nice-to-have.
# ---------------------------------------------------------------------


def _hydrate_model(record: asyncpg.Record) -> ModelRow:
    raw = dict(record)
    for key in list(raw.keys()):
        if str(key).startswith("_"):
            raw.pop(key, None)
    for key in (
        "proposition",
        "scope_entities",
        "scope_temporal",
        "falsifier",
        "signal_readings",
        "resolution_criteria",
    ):
        v = raw.get(key)
        if isinstance(v, (bytes, bytearray)):
            v = v.decode()
        if isinstance(v, str):
            try:
                raw[key] = json.loads(v)
            except json.JSONDecodeError:
                pass
    emb = raw.get("embedding")
    if emb is not None and not isinstance(emb, list):
        # pgvector values come back as string literals like "[0.1, 0.2, ...]"
        # when no vector codec is registered on the pool. Parse them before
        # passing into ModelRow / ObservationRow validators.
        if isinstance(emb, (bytes, bytearray)):
            emb = emb.decode()
        if isinstance(emb, str):
            try:
                raw["embedding"] = json.loads(emb)
            except (json.JSONDecodeError, ValueError):
                raw["embedding"] = None
        else:
            try:
                raw["embedding"] = [float(x) for x in emb]
            except (TypeError, ValueError):
                raw["embedding"] = None
    return ModelRow.model_validate(raw)


def _hydrate_obs(record: asyncpg.Record) -> ObservationRow:
    raw = dict(record)
    for key in ("content", "entities_mentioned"):
        v = raw.get(key)
        if isinstance(v, (bytes, bytearray)):
            v = v.decode()
        if isinstance(v, str):
            raw[key] = json.loads(v)
    emb = raw.get("embedding")
    if emb is not None and not isinstance(emb, list):
        # pgvector values come back as string literals like "[0.1, 0.2, ...]"
        # when no vector codec is registered on the pool. Parse them before
        # passing into ModelRow / ObservationRow validators.
        if isinstance(emb, (bytes, bytearray)):
            emb = emb.decode()
        if isinstance(emb, str):
            try:
                raw["embedding"] = json.loads(emb)
            except (json.JSONDecodeError, ValueError):
                raw["embedding"] = None
        else:
            try:
                raw["embedding"] = [float(x) for x in emb]
            except (TypeError, ValueError):
                raw["embedding"] = None
    return ObservationRow.model_validate(raw)


def _hydrate_resource(record: asyncpg.Record) -> ResourceRow:
    raw = dict(record)
    for key in ("current_value", "metadata"):
        v = raw.get(key)
        if isinstance(v, (bytes, bytearray)):
            v = v.decode()
        if isinstance(v, str):
            try:
                raw[key] = json.loads(v)
            except json.JSONDecodeError:
                pass
    return ResourceRow.model_validate(raw)


def _hydrate_commitment(record: asyncpg.Record) -> CommitmentRow:
    raw = dict(record)
    for key in ("success_criteria", "external_counterparty_ref", "estimated_capacity"):
        v = raw.get(key)
        if isinstance(v, (bytes, bytearray)):
            v = v.decode()
        if isinstance(v, str):
            try:
                raw[key] = json.loads(v)
            except json.JSONDecodeError:
                pass
    return CommitmentRow.model_validate(raw)


def _hydrate_goal(record: asyncpg.Record) -> GoalRow:
    raw = dict(record)
    for key in ("success_criteria",):
        v = raw.get(key)
        if isinstance(v, (bytes, bytearray)):
            v = v.decode()
        if isinstance(v, str):
            try:
                raw[key] = json.loads(v)
            except json.JSONDecodeError:
                pass
    return GoalRow.model_validate(raw)


def _hydrate_decision(record: asyncpg.Record) -> DecisionRow:
    raw = dict(record)
    for key in ("scope", "revisit_triggers"):
        v = raw.get(key)
        if isinstance(v, (bytes, bytearray)):
            v = v.decode()
        if isinstance(v, str):
            try:
                raw[key] = json.loads(v)
            except json.JSONDecodeError:
                pass
    return DecisionRow.model_validate(raw)


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _vector_to_float_list(value: Any) -> list[float] | None:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return None
    try:
        return [float(x) for x in value]
    except (TypeError, ValueError):
        return None


def _cosine_distance(a: Sequence[float] | None, b: Sequence[float] | None) -> float:
    if a is None or b is None:
        return float("inf")
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        fx = float(x)
        fy = float(y)
        dot += fx * fy
        na += fx * fx
        nb += fy * fy
    if na <= 0.0 or nb <= 0.0:
        return float("inf")
    return 1.0 - (dot / ((na ** 0.5) * (nb ** 0.5)))


def _hydrate_many(
    records: Sequence[asyncpg.Record],
    hydrate_fn: Any,
    notes: dict[str, Any],
    bucket: str,
) -> list[Any]:
    """
    Hydrate a batch defensively.

    Production snapshots can contain one legacy row whose enum/value
    drift no longer satisfies the current pydantic row type. Retrieval
    should not drop an entire pathway because one Act or Model cannot
    hydrate; it skips the bad row, reports the count, and keeps the
    rest of the context available.
    """
    out: list[Any] = []
    skipped = 0
    for r in records:
        try:
            out.append(hydrate_fn(r))
        except Exception:
            skipped += 1
    if skipped:
        notes.setdefault("hydration_skipped", {})[bucket] = skipped
    return out


# =====================================================================
# Pathway A — Structural proximity (graph walk over Acts edges)
# =====================================================================


_SEED_ENTITY_TYPES = frozenset(
    {"commitment", "goal", "decision", "actor", "customer", "customer_resource", "resource"}
)


def _canonical_seed_type(raw_type: Any) -> str | None:
    if raw_type is None:
        return None
    value = str(raw_type)
    if value == "customer":
        return "customer_resource"
    if value in _SEED_ENTITY_TYPES:
        return value
    return None


def _chunked(values: Sequence[Any], size: int) -> list[list[Any]]:
    chunk_size = max(1, int(size))
    return [list(values[i:i + chunk_size]) for i in range(0, len(values), chunk_size)]


def _scope_filter_key(entry: dict[str, Any]) -> tuple[str, UUID] | None:
    try:
        return str(entry["type"]), UUID(str(entry["id"]))
    except (KeyError, TypeError, ValueError):
        return None


def _cap_scope_entity_filters(
    filters: Sequence[dict[str, Any]],
    *,
    direct_seed_entity_pairs: set[tuple[str, UUID]],
    limit: int = _STRUCTURAL_MAX_SCOPE_ENTITY_FILTERS,
) -> tuple[list[dict[str, Any]], int]:
    """Bound model-scope lookup fanout while preserving direct seeds first."""
    cap = max(1, int(limit))
    if len(filters) <= cap:
        return list(filters), 0
    direct: list[dict[str, Any]] = []
    expanded: list[dict[str, Any]] = []
    for entry in filters:
        key = _scope_filter_key(entry)
        if key is not None and key in direct_seed_entity_pairs:
            direct.append(entry)
        else:
            expanded.append(entry)
    capped = [*direct, *expanded][:cap]
    return capped, len(filters) - len(capped)


def _record_value(row: asyncpg.Record, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, TypeError):
        return default


def _rank_sidecar_records(
    rows: Sequence[asyncpg.Record],
    *,
    limit: int,
) -> list[asyncpg.Record]:
    ranked = sorted(
        rows,
        key=lambda row: (
            int(_record_value(row, "_seed_priority", 1) or 1),
            int(_record_value(row, "_local_rank", 0) or 0),
            int(_record_value(row, "_seed_order", 0) or 0),
            -float(_record_value(row, "activation", 0.0) or 0.0),
            str(_record_value(row, "id", "") or ""),
        ),
    )
    return ranked[:max(1, int(limit))]


async def _fetch_pathway_a_entity_sidecar_rows(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    entity_types: Sequence[str],
    entity_ids: Sequence[UUID],
    entity_orders: Sequence[int],
    entity_priorities: Sequence[int],
    per_seed_limit: int,
    global_limit: int,
) -> list[asyncpg.Record]:
    if not entity_ids:
        return []
    rows = await conn.fetch(
        f"""
        WITH seeded_entities AS (
          SELECT * FROM unnest($2::text[], $3::uuid[], $4::int[], $5::int[])
            AS e(entity_type, entity_id, seed_order, seed_priority)
        ),
        candidate_ids AS (
          SELECT
            se.seed_priority,
            se.seed_order,
            scoped.local_rank,
            scoped.model_id
          FROM seeded_entities se
          CROSS JOIN LATERAL (
            SELECT
              mse.model_id,
              ROW_NUMBER() OVER (
                ORDER BY m.activation DESC, m.created_at DESC
              ) AS local_rank
            FROM model_scope_entities mse
            JOIN models m ON m.id = mse.model_id
            WHERE mse.tenant_id = $1
              AND mse.entity_type = se.entity_type
              AND mse.entity_id = se.entity_id
              AND m.tenant_id = $1
              AND m.status = 'active'
            ORDER BY m.activation DESC, m.created_at DESC
            LIMIT $6
          ) scoped
        ),
        scoped_models AS (
          SELECT
            model_id,
            MIN(seed_priority) AS seed_priority,
            MIN(seed_order) AS seed_order,
            MIN(local_rank) AS local_rank
          FROM candidate_ids
          GROUP BY model_id
        )
        SELECT sm.seed_priority AS _seed_priority,
               sm.seed_order AS _seed_order,
               sm.local_rank AS _local_rank,
               {_MODEL_SELECT_SQL}
        FROM models m
        JOIN scoped_models sm ON sm.model_id = m.id
        WHERE m.tenant_id = $1
          AND m.status = 'active'
        ORDER BY sm.seed_priority ASC, sm.local_rank ASC, sm.seed_order ASC,
                 m.activation DESC, m.created_at DESC
        LIMIT $7
        """,
        tenant_id,
        list(entity_types),
        list(entity_ids),
        list(entity_orders),
        list(entity_priorities),
        max(1, int(per_seed_limit)),
        max(1, int(global_limit)),
    )
    return list(rows)


async def _fetch_pathway_a_actor_sidecar_rows(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_ids: Sequence[UUID],
    actor_orders: Sequence[int],
    per_seed_limit: int,
    global_limit: int,
) -> list[asyncpg.Record]:
    if not actor_ids:
        return []
    rows = await conn.fetch(
        f"""
        WITH seeded_actors AS (
          SELECT * FROM unnest($2::uuid[], $3::int[])
            AS a(actor_id, seed_order)
        ),
        candidate_ids AS (
          SELECT
            sa.seed_order,
            scoped.local_rank,
            scoped.model_id
          FROM seeded_actors sa
          CROSS JOIN LATERAL (
            SELECT
              msa.model_id,
              ROW_NUMBER() OVER (
                ORDER BY m.activation DESC, m.created_at DESC
              ) AS local_rank
            FROM model_scope_actors msa
            JOIN models m ON m.id = msa.model_id
            WHERE msa.tenant_id = $1
              AND msa.actor_id = sa.actor_id
              AND m.tenant_id = $1
              AND m.status = 'active'
            ORDER BY m.activation DESC, m.created_at DESC
            LIMIT $4
          ) scoped
        ),
        scoped_models AS (
          SELECT
            model_id,
            MIN(seed_order) AS seed_order,
            MIN(local_rank) AS local_rank
          FROM candidate_ids
          GROUP BY model_id
        )
        SELECT sm.seed_order AS _seed_order,
               sm.local_rank AS _local_rank,
               {_MODEL_SELECT_SQL}
        FROM models m
        JOIN scoped_models sm ON sm.model_id = m.id
        WHERE m.tenant_id = $1
          AND m.status = 'active'
        ORDER BY sm.local_rank ASC, sm.seed_order ASC,
                 m.activation DESC, m.created_at DESC
        LIMIT $5
        """,
        tenant_id,
        list(actor_ids),
        list(actor_orders),
        max(1, int(per_seed_limit)),
        max(1, int(global_limit)),
    )
    return list(rows)


async def _fetch_pathway_a_entity_sidecar_rows_fanout(
    read_pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    entity_types: Sequence[str],
    entity_ids: Sequence[UUID],
    entity_orders: Sequence[int],
    entity_priorities: Sequence[int],
    per_seed_limit: int,
    global_limit: int,
    chunk_size: int,
) -> list[asyncpg.Record]:
    type_chunks = _chunked(entity_types, chunk_size)
    id_chunks = _chunked(entity_ids, chunk_size)
    order_chunks = _chunked(entity_orders, chunk_size)
    priority_chunks = _chunked(entity_priorities, chunk_size)

    async def fetch_chunk(
        types: list[str],
        ids: list[UUID],
        orders: list[int],
        priorities: list[int],
    ) -> list[asyncpg.Record]:
        async with read_pool.acquire() as fanout_conn:
            return await _fetch_pathway_a_entity_sidecar_rows(
                fanout_conn,
                tenant_id=tenant_id,
                entity_types=types,
                entity_ids=ids,
                entity_orders=orders,
                entity_priorities=priorities,
                per_seed_limit=per_seed_limit,
                global_limit=min(global_limit, per_seed_limit * max(1, len(ids))),
            )

    chunks = await asyncio.gather(*[
        fetch_chunk(types, ids, orders, priorities)
        for types, ids, orders, priorities in zip(
            type_chunks,
            id_chunks,
            order_chunks,
            priority_chunks,
        )
    ])
    return _rank_sidecar_records(
        [row for chunk in chunks for row in chunk],
        limit=global_limit,
    )


async def _fetch_pathway_a_actor_sidecar_rows_fanout(
    read_pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    actor_ids: Sequence[UUID],
    actor_orders: Sequence[int],
    per_seed_limit: int,
    global_limit: int,
    chunk_size: int,
) -> list[asyncpg.Record]:
    id_chunks = _chunked(actor_ids, chunk_size)
    order_chunks = _chunked(actor_orders, chunk_size)

    async def fetch_chunk(
        ids: list[UUID],
        orders: list[int],
    ) -> list[asyncpg.Record]:
        async with read_pool.acquire() as fanout_conn:
            return await _fetch_pathway_a_actor_sidecar_rows(
                fanout_conn,
                tenant_id=tenant_id,
                actor_ids=ids,
                actor_orders=orders,
                per_seed_limit=per_seed_limit,
                global_limit=min(global_limit, per_seed_limit * max(1, len(ids))),
            )

    chunks = await asyncio.gather(*[
        fetch_chunk(ids, orders)
        for ids, orders in zip(id_chunks, order_chunks)
    ])
    return _rank_sidecar_records(
        [row for chunk in chunks for row in chunk],
        limit=global_limit,
    )


async def pathway_a_structural(
    seed_entity_ids: Sequence[dict[str, Any]],
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    max_hops: int = _DEFAULT_STRUCTURAL_MAX_HOPS,
    read_pool: asyncpg.Pool | None = None,
    read_fanout_enabled: bool = False,
    read_fanout_min_seeds: int = 16,
    read_fanout_chunk_size: int = 8,
) -> PathwayResult:
    """
    Walk the Acts graph (contributes_to / depends_on / constrained_by /
    commitment_contributors / customer_commitments) up to `max_hops`
    from each seed. Collect the touched entity set. Then fetch Models
    scoped to any of those entities.

    Seed shape: `[{'type': 'commitment', 'id': UUID}, ...]`. Types are
    one of {commitment, goal, decision, actor, customer_resource,
    resource}. Unknown types are skipped with a note.

    Returns:
      - `models`: Models whose `scope_entities` overlaps the touched
        entity set, or whose `scope_actors` overlaps any actor seed.
      - `acts`: dict of {goals, commitments, decisions} — every entity
        encountered during the walk, for assembler use.
      - `resources`: Customer and Capacity resources touched on the way.
      - `notes`: hops_executed, seeds_by_type, entities_touched counts.
    """
    notes: dict[str, Any] = {
        "seeds_by_type": {},
        "hops_executed": 0,
        "entities_touched": {},
        "seeds_accepted": 0,
        "timings": [],
    }
    if not seed_entity_ids:
        return PathwayResult(source_pathway="A", notes={**notes, "reason": "empty_seed"})
    if max_hops < 0:
        raise ValidationError("max_hops must be >= 0", max_hops=max_hops)

    # Bucket seeds by type.
    seeds: dict[str, set[UUID]] = {k: set() for k in _SEED_ENTITY_TYPES}
    for raw in seed_entity_ids:
        if not isinstance(raw, dict):
            continue
        t = _canonical_seed_type(raw.get("type"))
        rid = raw.get("id")
        if t is None:
            continue
        if rid is None:
            continue
        try:
            seeds[t].add(UUID(str(rid)))
        except (ValueError, TypeError):
            continue
    notes["seeds_by_type"] = {k: len(v) for k, v in seeds.items() if v}
    notes["seeds_accepted"] = sum(len(v) for v in seeds.values())
    if notes["seeds_accepted"] == 0:
        return PathwayResult(source_pathway="A", notes={**notes, "reason": "no_valid_seed"})
    direct_seed_entity_pairs: set[tuple[str, UUID]] = set()
    for direct_type in ("commitment", "goal", "decision", "resource"):
        for direct_id in seeds[direct_type]:
            direct_seed_entity_pairs.add((direct_type, direct_id))
    for direct_id in seeds["customer_resource"]:
        direct_seed_entity_pairs.add(("customer", direct_id))
        direct_seed_entity_pairs.add(("customer_resource", direct_id))
        direct_seed_entity_pairs.add(("resource", direct_id))

    # Visited sets per type. Start from the seeds themselves (hop 0).
    visited_commits: set[UUID] = set(seeds["commitment"])
    visited_goals: set[UUID] = set(seeds["goal"])
    visited_decisions: set[UUID] = set(seeds["decision"])
    visited_actors: set[UUID] = set(seeds["actor"])
    visited_customers: set[UUID] = set(seeds["customer_resource"])
    visited_resources: set[UUID] = set(seeds["resource"])

    # Frontier set for the next hop (entity additions discovered since
    # the last hop); used to limit per-hop cost.
    frontier_commits: set[UUID] = set(seeds["commitment"])
    frontier_goals: set[UUID] = set(seeds["goal"])
    frontier_decisions: set[UUID] = set(seeds["decision"])
    frontier_customers: set[UUID] = set(seeds["customer_resource"])
    frontier_actors: set[UUID] = set(seeds["actor"])

    stage_started = time.perf_counter()
    for hop in range(max_hops):
        new_commits: set[UUID] = set()
        new_goals: set[UUID] = set()
        new_decisions: set[UUID] = set()
        new_customers: set[UUID] = set()

        # From commitments: contributes_to (Goals), depends_on (both
        # directions), constrained_by (Decisions), customer_commitments
        # (Customer Resources).
        if frontier_commits:
            commit_list = list(frontier_commits)
            # Goals via contributes_to
            goal_rows = await conn.fetch(
                """
                SELECT DISTINCT goal_id FROM contributes_to
                WHERE commitment_id = ANY($1::uuid[])
                """,
                commit_list,
            )
            for r in goal_rows:
                gid = r["goal_id"]
                if gid not in visited_goals:
                    new_goals.add(gid)
            # Dependency commitments (both directions)
            dep_rows = await conn.fetch(
                """
                SELECT dependency_commitment_id AS d, dependent_commitment_id AS t
                FROM depends_on
                WHERE dependent_commitment_id = ANY($1::uuid[])
                   OR dependency_commitment_id = ANY($1::uuid[])
                """,
                commit_list,
            )
            for r in dep_rows:
                for cid in (r["d"], r["t"]):
                    if cid is not None and cid not in visited_commits:
                        new_commits.add(cid)
            # Decisions via constrained_by
            dec_rows = await conn.fetch(
                """
                SELECT DISTINCT decision_id FROM constrained_by
                WHERE commitment_id = ANY($1::uuid[])
                """,
                commit_list,
            )
            for r in dec_rows:
                did = r["decision_id"]
                if did not in visited_decisions:
                    new_decisions.add(did)
            # Customer resources via customer_commitments
            cust_rows = await conn.fetch(
                """
                SELECT DISTINCT customer_resource_id FROM customer_commitments
                WHERE commitment_id = ANY($1::uuid[])
                """,
                commit_list,
            )
            for r in cust_rows:
                crid = r["customer_resource_id"]
                if crid not in visited_customers:
                    new_customers.add(crid)

        # From goals: parent_goal_id (upward), child goals, and
        # contributes_to (Commitments).
        if frontier_goals:
            goal_list = list(frontier_goals)
            parent_rows = await conn.fetch(
                """
                SELECT DISTINCT parent_goal_id FROM goals
                WHERE id = ANY($1::uuid[]) AND parent_goal_id IS NOT NULL
                """,
                goal_list,
            )
            for r in parent_rows:
                pid = r["parent_goal_id"]
                if pid is not None and pid not in visited_goals:
                    new_goals.add(pid)
            child_rows = await conn.fetch(
                """
                SELECT DISTINCT id FROM goals
                WHERE parent_goal_id = ANY($1::uuid[])
                """,
                goal_list,
            )
            for r in child_rows:
                cid = r["id"]
                if cid not in visited_goals:
                    new_goals.add(cid)
            commit_from_goals = await conn.fetch(
                """
                SELECT DISTINCT commitment_id FROM contributes_to
                WHERE goal_id = ANY($1::uuid[])
                """,
                goal_list,
            )
            for r in commit_from_goals:
                cid = r["commitment_id"]
                if cid not in visited_commits:
                    new_commits.add(cid)

        # From decisions: walk back to the commitments constrained by
        # the decision. This makes decision-seeded signals useful for
        # operating-memory retrieval instead of only supporting the
        # commitment -> decision direction.
        if frontier_decisions:
            decision_list = list(frontier_decisions)
            commit_from_decisions = await conn.fetch(
                """
                SELECT DISTINCT commitment_id FROM constrained_by
                WHERE decision_id = ANY($1::uuid[])
                """,
                decision_list,
            )
            for r in commit_from_decisions:
                cid = r["commitment_id"]
                if cid not in visited_commits:
                    new_commits.add(cid)

        # From customer resources: follow customer_commitments to
        # Commitments → their Goals (the spine).
        if frontier_customers:
            customer_list = list(frontier_customers)
            cust_commits = await conn.fetch(
                """
                SELECT DISTINCT commitment_id FROM customer_commitments
                WHERE customer_resource_id = ANY($1::uuid[])
                """,
                customer_list,
            )
            for r in cust_commits:
                cid = r["commitment_id"]
                if cid not in visited_commits:
                    new_commits.add(cid)

        # From actors: find owner commitments + contributor commitments.
        if frontier_actors:
            actor_list = list(frontier_actors)
            owner_rows = await conn.fetch(
                """
                SELECT id FROM commitments
                WHERE owner_id = ANY($1::uuid[])
                  AND tenant_id = $2
                """,
                actor_list,
                tenant_id,
            )
            for r in owner_rows:
                cid = r["id"]
                if cid not in visited_commits:
                    new_commits.add(cid)
            contributor_rows = await conn.fetch(
                """
                SELECT DISTINCT cc.commitment_id FROM commitment_contributors cc
                JOIN commitments c ON c.id = cc.commitment_id
                WHERE cc.actor_id = ANY($1::uuid[])
                  AND c.tenant_id = $2
                """,
                actor_list,
                tenant_id,
            )
            for r in contributor_rows:
                cid = r["commitment_id"]
                if cid not in visited_commits:
                    new_commits.add(cid)

        # Update visited sets.
        visited_commits.update(new_commits)
        visited_goals.update(new_goals)
        visited_decisions.update(new_decisions)
        visited_customers.update(new_customers)
        # Actors are seed-only (we don't discover new actors from the
        # walk; that would be a distinct semantic).
        frontier_commits = new_commits
        frontier_goals = new_goals
        frontier_decisions = new_decisions
        frontier_customers = new_customers
        frontier_actors = set()  # never expand beyond hop 0 for actors

        notes["hops_executed"] = hop + 1

        # Early exit if frontier is empty.
        if not (
            frontier_commits
            or frontier_goals
            or frontier_decisions
            or frontier_customers
        ):
            break
    _append_timing(
        notes,
        "graph_walk",
        stage_started,
        commitments=len(visited_commits),
        goals=len(visited_goals),
        decisions=len(visited_decisions),
        customers=len(visited_customers),
        actors=len(visited_actors),
        hops=notes["hops_executed"],
    )

    # Fetch full rows for the touched entities (tenant-filtered).
    stage_started = time.perf_counter()
    goals_out: list[GoalRow] = []
    if visited_goals:
        grs = await conn.fetch(
            "SELECT * FROM goals WHERE id = ANY($1::uuid[]) AND tenant_id = $2",
            list(visited_goals),
            tenant_id,
        )
        goals_out = _hydrate_many(grs, _hydrate_goal, notes, "goals")

    commitments_out: list[CommitmentRow] = []
    if visited_commits:
        crs = await conn.fetch(
            "SELECT * FROM commitments WHERE id = ANY($1::uuid[]) AND tenant_id = $2",
            list(visited_commits),
            tenant_id,
        )
        commitments_out = _hydrate_many(crs, _hydrate_commitment, notes, "commitments")

    decisions_out: list[DecisionRow] = []
    if visited_decisions:
        drs = await conn.fetch(
            "SELECT * FROM decisions WHERE id = ANY($1::uuid[]) AND tenant_id = $2",
            list(visited_decisions),
            tenant_id,
        )
        decisions_out = _hydrate_many(drs, _hydrate_decision, notes, "decisions")

    resources_out: list[ResourceRow] = []
    touched_resource_ids = visited_customers | visited_resources
    if touched_resource_ids:
        rrs = await conn.fetch(
            "SELECT * FROM resources WHERE id = ANY($1::uuid[]) AND tenant_id = $2",
            list(touched_resource_ids),
            tenant_id,
        )
        resources_out = _hydrate_many(rrs, _hydrate_resource, notes, "resources")
    _append_timing(
        notes,
        "act_row_fetch",
        stage_started,
        goals=len(goals_out),
        commitments=len(commitments_out),
        decisions=len(decisions_out),
        resources=len(resources_out),
    )

    # Scoped Model search — union of (scope_entities @> any touched
    # entity) and (scope_actors && visited actors).
    stage_started = time.perf_counter()
    scope_entity_filters: list[dict[str, Any]] = []
    for cid in visited_commits:
        scope_entity_filters.append({"type": "commitment", "id": str(cid)})
    for gid in visited_goals:
        scope_entity_filters.append({"type": "goal", "id": str(gid)})
    for did in visited_decisions:
        scope_entity_filters.append({"type": "decision", "id": str(did)})
    for crid in visited_customers:
        scope_entity_filters.append({"type": "customer", "id": str(crid)})
        scope_entity_filters.append({"type": "customer_resource", "id": str(crid)})
        # Models may use 'resource' instead of 'customer_resource' for the
        # scope entity type depending on how Think writes them. Surface
        # both shapes so retrieval catches both vocabularies.
        scope_entity_filters.append({"type": "resource", "id": str(crid)})
    for rid in visited_resources:
        scope_entity_filters.append({"type": "resource", "id": str(rid)})

    if visited_customers:
        rows = await conn.fetch(
            """
            SELECT commitment_id
            FROM customer_commitments
            WHERE tenant_id = $1
              AND customer_resource_id = ANY($2::uuid[])
            """,
            tenant_id,
            list(visited_customers),
        )
        for row in rows:
            scope_entity_filters.append(
                {"type": "commitment", "id": str(row["commitment_id"])}
            )
        identity_rows = await conn.fetch(
            """
            SELECT id, identity
            FROM resources
            WHERE tenant_id = $1
              AND id = ANY($2::uuid[])
              AND kind = 'relational'
              AND archived_at IS NULL
            """,
            tenant_id,
            list(visited_customers),
        )
        like_patterns: list[str] = []
        for row in identity_rows:
            identity = " ".join(str(row["identity"]).split()).strip()
            if not identity:
                continue
            like_patterns.append(f"%{identity}%")
            folded = identity.casefold()
            for suffix in (" inc", " corp", " llc", " ltd", " incorporated"):
                if folded.endswith(suffix):
                    like_patterns.append(f"%{identity[: -len(suffix)].strip()}%")
                    break
        if like_patterns:
            rows = await conn.fetch(
                """
                SELECT id
                FROM commitments
                WHERE tenant_id = $1
                  AND (
                    title ILIKE ANY($2::text[])
                    OR COALESCE(description, '') ILIKE ANY($2::text[])
                  )
                """,
                tenant_id,
                like_patterns,
            )
            for row in rows:
                scope_entity_filters.append(
                    {"type": "commitment", "id": str(row["id"])}
                )
    if visited_commits:
        rows = await conn.fetch(
            """
            SELECT customer_resource_id
            FROM customer_commitments
            WHERE tenant_id = $1
              AND commitment_id = ANY($2::uuid[])
            """,
            tenant_id,
            list(visited_commits),
        )
        for row in rows:
            cid = row["customer_resource_id"]
            scope_entity_filters.append({"type": "customer", "id": str(cid)})
            scope_entity_filters.append(
                {"type": "customer_resource", "id": str(cid)}
            )
            scope_entity_filters.append({"type": "resource", "id": str(cid)})

    if scope_entity_filters:
        deduped_scope_filters: list[dict[str, Any]] = []
        seen_scope_filters: set[str] = set()
        for entry in scope_entity_filters:
            key = _jsonb(entry)
            if key in seen_scope_filters:
                continue
            seen_scope_filters.add(key)
            deduped_scope_filters.append(entry)
        scope_entity_filters = deduped_scope_filters
    filters_before_cap = len(scope_entity_filters)
    scope_entity_filters, filters_dropped_by_cap = _cap_scope_entity_filters(
        scope_entity_filters,
        direct_seed_entity_pairs=direct_seed_entity_pairs,
    )
    _append_timing(
        notes,
        "scope_filter_expand",
        stage_started,
        filters=len(scope_entity_filters),
        filters_before_cap=filters_before_cap,
        filter_cap=_STRUCTURAL_MAX_SCOPE_ENTITY_FILTERS,
        filters_dropped_by_cap=filters_dropped_by_cap,
        actors=len(visited_actors),
    )

    models_out: list[ModelRow] = []
    if scope_entity_filters or visited_actors:
        sidecar_entity_types: list[str] = []
        sidecar_entity_ids: list[UUID] = []
        sidecar_entity_orders: list[int] = []
        sidecar_entity_priorities: list[int] = []
        for f in scope_entity_filters:
            try:
                entity_type = str(f["type"])
                entity_id = UUID(str(f["id"]))
                sidecar_entity_types.append(entity_type)
                sidecar_entity_ids.append(entity_id)
                sidecar_entity_orders.append(len(sidecar_entity_orders))
                sidecar_entity_priorities.append(
                    0
                    if (entity_type, entity_id) in direct_seed_entity_pairs
                    else 1
                )
            except (KeyError, TypeError, ValueError):
                continue

        seen_ids: set[UUID] = set()
        entity_sidecar_rows: list[asyncpg.Record] = []
        actor_sidecar_rows: list[asyncpg.Record] = []
        stage_started = time.perf_counter()
        if sidecar_entity_ids:
            if (
                read_pool is not None
                and read_fanout_enabled
                and len(sidecar_entity_ids) >= int(read_fanout_min_seeds)
            ):
                entity_sidecar_rows = await _fetch_pathway_a_entity_sidecar_rows_fanout(
                    read_pool,
                    tenant_id=tenant_id,
                    entity_types=sidecar_entity_types,
                    entity_ids=sidecar_entity_ids,
                    entity_orders=sidecar_entity_orders,
                    entity_priorities=sidecar_entity_priorities,
                    per_seed_limit=_STRUCTURAL_MODELS_PER_SCOPE_ENTITY,
                    global_limit=_STRUCTURAL_MAX_MODELS,
                    chunk_size=read_fanout_chunk_size,
                )
            else:
                entity_sidecar_rows = await _fetch_pathway_a_entity_sidecar_rows(
                    conn,
                    tenant_id=tenant_id,
                    entity_types=sidecar_entity_types,
                    entity_ids=sidecar_entity_ids,
                    entity_orders=sidecar_entity_orders,
                    entity_priorities=sidecar_entity_priorities,
                    per_seed_limit=_STRUCTURAL_MODELS_PER_SCOPE_ENTITY,
                    global_limit=_STRUCTURAL_MAX_MODELS,
                )
            entity_sidecar_rows = _rank_sidecar_records(
                entity_sidecar_rows,
                limit=_STRUCTURAL_MAX_MODELS,
            )
        _append_timing(
            notes,
            "sidecar_entity_lookup",
            stage_started,
            filters=len(sidecar_entity_ids),
            direct_seed_filters=sum(
                1 for priority in sidecar_entity_priorities if priority == 0
            ),
            per_seed_limit=_STRUCTURAL_MODELS_PER_SCOPE_ENTITY,
            fanout_used=(
                read_pool is not None
                and read_fanout_enabled
                and len(sidecar_entity_ids) >= int(read_fanout_min_seeds)
            ),
            fanout_chunk_size=read_fanout_chunk_size,
            rows=len(entity_sidecar_rows),
        )
        stage_started = time.perf_counter()
        if visited_actors:
            actor_ids = list(visited_actors)
            actor_orders = list(range(len(actor_ids)))
            if (
                read_pool is not None
                and read_fanout_enabled
                and len(actor_ids) >= int(read_fanout_min_seeds)
            ):
                actor_sidecar_rows = await _fetch_pathway_a_actor_sidecar_rows_fanout(
                    read_pool,
                    tenant_id=tenant_id,
                    actor_ids=actor_ids,
                    actor_orders=actor_orders,
                    per_seed_limit=_STRUCTURAL_MODELS_PER_SCOPE_ACTOR,
                    global_limit=_STRUCTURAL_MAX_MODELS,
                    chunk_size=read_fanout_chunk_size,
                )
            else:
                actor_sidecar_rows = await _fetch_pathway_a_actor_sidecar_rows(
                    conn,
                    tenant_id=tenant_id,
                    actor_ids=actor_ids,
                    actor_orders=actor_orders,
                    per_seed_limit=_STRUCTURAL_MODELS_PER_SCOPE_ACTOR,
                    global_limit=_STRUCTURAL_MAX_MODELS,
                )
            actor_sidecar_rows = _rank_sidecar_records(
                actor_sidecar_rows,
                limit=_STRUCTURAL_MAX_MODELS,
            )
        _append_timing(
            notes,
            "sidecar_actor_lookup",
            stage_started,
            actors=len(visited_actors),
            per_seed_limit=_STRUCTURAL_MODELS_PER_SCOPE_ACTOR,
            fanout_used=(
                read_pool is not None
                and read_fanout_enabled
                and len(visited_actors) >= int(read_fanout_min_seeds)
            ),
            fanout_chunk_size=read_fanout_chunk_size,
            rows=len(actor_sidecar_rows),
        )

        sidecar_rows = _rank_sidecar_records(
            list(entity_sidecar_rows) + list(actor_sidecar_rows),
            limit=_STRUCTURAL_MAX_MODELS,
        )
        stage_started = time.perf_counter()
        for r in sidecar_rows:
            if r["id"] in seen_ids:
                continue
            seen_ids.add(r["id"])
            try:
                models_out.append(_hydrate_model(r))
            except Exception:
                notes.setdefault("hydration_skipped", {}).setdefault("models", 0)
                notes["hydration_skipped"]["models"] += 1
        _append_timing(
            notes,
            "sidecar_hydrate",
            stage_started,
            rows=len(sidecar_rows),
            models=len(models_out),
        )

        notes["scope_sidecar_entity_rows"] = len(entity_sidecar_rows)
        notes["scope_sidecar_actor_rows"] = len(actor_sidecar_rows)
        notes["scope_sidecar_rows"] = len(sidecar_rows)
        notes["scope_sidecar_strategy"] = "bounded_per_scope_seed"
        notes["scope_sidecar_per_entity_limit"] = _STRUCTURAL_MODELS_PER_SCOPE_ENTITY
        notes["scope_sidecar_per_actor_limit"] = _STRUCTURAL_MODELS_PER_SCOPE_ACTOR

        # Compatibility fallback. The normalized sidecars are the scalable
        # lookup path; the JSONB predicates remain useful for older rows or
        # test fixtures that have not been sidecarized.
        params: list[Any] = [tenant_id]
        clauses: list[str] = []
        if visited_actors and not actor_sidecar_rows:
            params.append(list(visited_actors))
            clauses.append(f"scope_actors && ${len(params)}::uuid[]")
        if scope_entity_filters and not entity_sidecar_rows:
            for f in scope_entity_filters:
                params.append(_jsonb([f]))
                clauses.append(f"scope_entities @> ${len(params)}::jsonb")
        if clauses:
            where = " OR ".join(clauses)
            stage_started = time.perf_counter()
            rows = await conn.fetch(
                f"""
                SELECT {_MODEL_SELECT_SQL} FROM models
                WHERE tenant_id = $1
                  AND status = 'active'
                  AND ({where})
                ORDER BY activation DESC, created_at DESC
                LIMIT {_STRUCTURAL_MAX_MODELS}
                """,
                *params,
            )
            _append_timing(
                notes,
                "jsonb_fallback_query",
                stage_started,
                rows=len(rows),
                clauses=len(clauses),
            )
            stage_started = time.perf_counter()
            for r in rows:
                if r["id"] in seen_ids:
                    continue
                seen_ids.add(r["id"])
                try:
                    models_out.append(_hydrate_model(r))
                except Exception:
                    notes.setdefault("hydration_skipped", {}).setdefault("models", 0)
                    notes["hydration_skipped"]["models"] += 1
            notes["scope_jsonb_fallback_used"] = True
            notes["scope_jsonb_rows"] = len(rows)
            _append_timing(
                notes,
                "jsonb_fallback_hydrate",
                stage_started,
                rows=len(rows),
                models=len(models_out),
            )
        else:
            notes["scope_jsonb_fallback_used"] = False
            notes["scope_jsonb_rows"] = 0
            _append_timing(
                notes,
                "jsonb_fallback_query",
                time.perf_counter(),
                skipped=True,
                clauses=0,
            )

    notes["entities_touched"] = {
        "commitments": len(visited_commits),
        "goals": len(visited_goals),
        "decisions": len(visited_decisions),
        "actors": len(visited_actors),
        "customers": len(visited_customers),
        "resources": len(visited_resources),
    }
    notes["model_scope_filters"] = len(scope_entity_filters)
    notes["models_returned"] = len(models_out)

    return PathwayResult(
        models=models_out,
        observations=[],
        acts={
            "goals": goals_out,
            "commitments": commitments_out,
            "decisions": decisions_out,
        },
        resources=resources_out,
        source_pathway="A",
        notes=notes,
    )


def _conn_has_vector_codec(conn: asyncpg.Connection) -> bool:
    """True when the pgvector codec is registered on the connection.

    asyncpg.Connection uses __slots__, so we cannot tag the connection
    directly. Instead the gateway pool init and ModelsRepo's lazy
    register both add `id(conn)` to the module-level registry in
    services.domain.models.repo. PoolConnectionProxy.__getattr__ delegates
    `_con` to the wrapped Connection, so we identify by that id.
    """
    try:
        from services.domain.models.repo import PGVECTOR_REGISTERED_POOL_IDS
    except Exception:
        return False
    if id(conn) in PGVECTOR_REGISTERED_POOL_IDS:
        return True
    inner = getattr(conn, "_con", None)
    if inner is not None and id(inner) in PGVECTOR_REGISTERED_POOL_IDS:
        return True
    return False


# =====================================================================
# Pathway B — Semantic similarity (embedding cosine over active Models)
# =====================================================================


async def pathway_b_semantic(
    seed_natural_text: str,
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    k: int = _DEFAULT_K_SEMANTIC,
    embedder: OllamaClient | None = None,
    precomputed_vector: Sequence[float] | None = None,
    event_actors: Sequence[UUID] | None = None,
    event_entities: Sequence[dict[str, Any]] | None = None,
    hnsw_ef_search: int | None = None,
) -> PathwayResult:
    """
    HNSW cosine nearest-neighbour search over active Models.

    One integration test uses real Ollama; the rest pass
    `precomputed_vector` to avoid the network round-trip. If neither
    the embedder nor a precomputed vector is supplied, this raises —
    retrieval must not silently return empty on a mis-configured
    environment.

    On Ollama down + no precomputed vector → RetrievalPathwayError so
    the caller can decide to skip pathway B and keep the other three
    in the trigger's mix.

    Scope filter (RA-1, RETRIEVAL-DESIGN-AUDIT §3 arg 1): when
    `event_actors` and/or `event_entities` is provided, candidate
    Models are restricted to those whose scope overlaps the event.
    Matching is OR across the two dimensions — a Model that is scoped
    to an entity mentioned in the event is returned even if its
    scope_actors do not overlap event_actors, and vice versa. When
    both are None/empty the pre-audit behavior (no scope filter) is
    preserved for backward compatibility with callers that have not
    yet threaded scope through.
    """
    if k <= 0:
        raise ValidationError("k must be positive", k=k)
    notes: dict[str, Any] = {
        "seed_chars": len(seed_natural_text or ""),
        "k_requested": k,
        "vector_source": None,
        "scope_filter": None,
    }
    if not seed_natural_text and precomputed_vector is None:
        return PathwayResult(
            source_pathway="B",
            notes={**notes, "reason": "empty_seed"},
        )

    # Resolve the query vector.
    vec: list[float]
    if precomputed_vector is not None:
        vec = [float(x) for x in precomputed_vector]
        notes["vector_source"] = "precomputed"
    else:
        if embedder is None:
            raise RetrievalPathwayError(
                "pathway B requires either a precomputed_vector or an "
                "embedder; neither was supplied",
                seed_chars=len(seed_natural_text),
            )
        try:
            vec = await embedder.embed(seed_natural_text)
            notes["vector_source"] = "ollama"
        except OllamaError as e:
            raise RetrievalPathwayError(
                f"ollama embedding failed: {e}",
                cause=str(e),
            ) from e
    if len(vec) != EMBEDDING_DIM:
        raise ValidationError(
            f"pathway B vec dim {len(vec)} != {EMBEDDING_DIM}",
            got=len(vec),
            expected=EMBEDDING_DIM,
        )

    # Optional HNSW ef_search bump (RA-5, RETRIEVAL-DESIGN-AUDIT §3
    # arg 4). Applied per transaction — the SET LOCAL lands only
    # inside the caller's tx.
    if hnsw_ef_search is not None and hnsw_ef_search > 0:
        try:
            async with conn.transaction():
                await conn.execute(
                    f"SET LOCAL hnsw.ef_search = {int(hnsw_ef_search)}"
                )
            notes["hnsw_ef_search"] = int(hnsw_ef_search)
        except asyncpg.PostgresError:
            # Not fatal — just means we're not in a tx or pgvector
            # version doesn't honor the GUC. The savepoint keeps the
            # caller's transaction usable. Fall back to default.
            notes["hnsw_ef_search"] = None

    # RA-1 scope filter: restrict to Models whose scope overlaps the
    # event when either event_actors or event_entities is supplied.
    # Semantics: OR between the two dimensions. A Model matches if
    #   (scope_actors && event_actors) OR (scope_entities && event_entities).
    # Bind format depends on whether asyncpg has the pgvector binary
    # codec registered on this connection. The encoder accepts a
    # numpy array (or anything `Vector(...)` can wrap); the no-codec
    # path needs the stringified `[…]` literal that the `::vector`
    # cast can parse as text.
    if _conn_has_vector_codec(conn):
        import numpy as _np
        vec_param: Any = _np.asarray(
            [float(x) for x in vec], dtype="float32"
        )
    else:
        vec_param = "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"
    scope_clauses: list[str] = []
    scope_params: list[Any] = [tenant_id, vec_param, k]
    actor_list: list[UUID] = []
    entity_list: list[dict[str, Any]] = []
    if event_actors:
        for a in event_actors:
            if a is None:
                continue
            try:
                actor_list.append(UUID(str(a)))
            except (ValueError, TypeError):
                continue
    if event_entities:
        for e in event_entities:
            if not isinstance(e, dict):
                continue
            etype = e.get("type")
            eid = e.get("id")
            if etype is None or eid is None:
                continue
            entity_list.append({"type": str(etype), "id": str(eid)})
    if actor_list:
        scope_params.append(actor_list)
        scope_clauses.append(f"scope_actors && ${len(scope_params)}::uuid[]")
    if entity_list:
        for ent in entity_list:
            scope_params.append(_jsonb([ent]))
            scope_clauses.append(
                f"scope_entities @> ${len(scope_params)}::jsonb"
            )
    notes["scope_filter"] = {
        "event_actors_count": len(actor_list),
        "event_entities_count": len(entity_list),
        "applied": bool(scope_clauses),
    }

    scope_sql = ""
    if scope_clauses:
        scope_sql = "  AND (" + " OR ".join(scope_clauses) + ")\n"

    rows = await conn.fetch(
        f"""
        SELECT {_MODEL_SELECT_SQL}
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND embedding IS NOT NULL
        {scope_sql}ORDER BY embedding <=> $2::vector
        LIMIT $3
        """,
        *scope_params,
    )
    models = _hydrate_many(rows, _hydrate_model, notes, "models")

    # HNSW is approximate and Postgres applies the JSONB/actor scope
    # predicate around the vector order. For highly selective event
    # scopes, the indexed plan can occasionally return too few rows
    # even when scoped candidates exist. Exact-rank the scoped candidate
    # pool in Python as a production precision fallback.
    if scope_clauses and len(models) < min(k, 10):
        exact_rows = await conn.fetch(
            f"""
            WITH _params AS (
              SELECT $2::vector AS _query_vector, $3::int AS _k
            )
            SELECT {_MODEL_SELECT_SQL}
            FROM models, _params
            WHERE tenant_id = $1
              AND status = 'active'
              AND embedding IS NOT NULL
            {scope_sql}LIMIT LEAST(GREATEST($3::int * 20, 200), 2000)
            """,
            *scope_params,
        )
        exact_models = _hydrate_many(
            exact_rows, _hydrate_model, notes, "scope_exact_models"
        )
        exact_models.sort(
            key=lambda m: (
                _cosine_distance(vec, m.embedding),
                -m.activation,
                str(m.id),
            )
        )
        models = exact_models[:k]
        notes["scope_exact_fallback"] = {
            "hnsw_rows": len(rows),
            "candidate_rows": len(exact_models),
            "returned": len(models),
        }
    elif not scope_clauses and len(models) < k:
        exact_rows = await conn.fetch(
            f"""
            WITH _params AS (
              SELECT $2::vector AS _query_vector, $3::int AS _k
            )
            SELECT {_MODEL_SELECT_SQL}
            FROM models, _params
            WHERE tenant_id = $1
              AND status = 'active'
              AND embedding IS NOT NULL
            LIMIT LEAST(GREATEST($3::int * 20, 200), 5000)
            """,
            *scope_params,
        )
        exact_models = _hydrate_many(
            exact_rows, _hydrate_model, notes, "exact_models"
        )
        exact_models.sort(
            key=lambda m: (
                _cosine_distance(vec, m.embedding),
                -m.activation,
                str(m.id),
            )
        )
        models = exact_models[:k]
        notes["exact_fallback"] = {
            "hnsw_rows": len(rows),
            "candidate_rows": len(exact_models),
            "returned": len(models),
        }
    notes["models_returned"] = len(models)

    return PathwayResult(
        models=models,
        observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        source_pathway="B",
        notes=notes,
    )


# =====================================================================
# Pathway C — Temporal recency (Observations + Models in a time window)
# =====================================================================


async def pathway_c_temporal(
    seed_occurred_at: datetime,
    window: timedelta,
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    scope_actors: Sequence[UUID] | None = None,
    scope_entities: Sequence[dict[str, Any]] | None = None,
    max_observations: int = _TEMPORAL_MAX_OBSERVATIONS,
    max_models: int = 200,
    include_entity_mentions: bool = True,
) -> PathwayResult:
    """
    Return Observations in [seed-window, seed+window] (tenant-filtered,
    optionally actor/entity-filtered), plus active Models whose
    `created_at` or `last_retrieved_at` falls in the same window.

    The explicit [start, end] filter enables partition pruning on
    `observations` (partitioned monthly by occurred_at).

    `include_entity_mentions` (RA-5 fix for audit §4 arg 2): when
    True (default), the actor filter matches observations where the
    actor is EITHER the `author_id` OR present in
    `entities_mentioned` as `{type:"actor", id:"<uuid>"}`. When False,
    only `author_id` is matched (pre-fix behavior). Backward-compat
    for callers that opted out.
    """
    if window.total_seconds() <= 0:
        raise ValidationError("window must be > 0", window_seconds=window.total_seconds())

    start = seed_occurred_at - window
    end = seed_occurred_at + window
    notes: dict[str, Any] = {
        "window_seconds": window.total_seconds(),
        "seed_occurred_at": seed_occurred_at.isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "scope_actors_count": len(scope_actors or []),
        "scope_entities_count": len(scope_entities or []),
        "max_observations": int(max_observations),
        "max_models": int(max_models),
        "include_entity_mentions": include_entity_mentions,
    }
    entity_list: list[dict[str, str]] = []
    for ent in scope_entities or []:
        if not isinstance(ent, dict):
            continue
        etype = ent.get("type")
        eid = ent.get("id")
        if etype is None or eid is None:
            continue
        entity_list.append({"type": str(etype), "id": str(eid)})

    # Observations query — tenant + time-range; optional actor/entity filter.
    obs_sql = f"SELECT {_OBS_SELECT_SQL} FROM observations " \
              "WHERE tenant_id = $1 AND occurred_at >= $2 AND occurred_at <= $3"
    obs_params: list[Any] = [tenant_id, start, end]
    obs_scope_clauses: list[str] = []
    if scope_actors:
        actor_ids = list(scope_actors)
        obs_params.append(actor_ids)
        if include_entity_mentions:
            # Build a JSONB containment OR-chain so the GIN index on
            # entities_mentioned is exploitable. Actor entries are
            # canonical `{"type":"actor","id":"<uuid>"}`.
            mention_clauses: list[str] = []
            for aid in actor_ids:
                obs_params.append(_jsonb([{"type": "actor", "id": str(aid)}]))
                mention_clauses.append(
                    f"entities_mentioned @> ${len(obs_params)}::jsonb"
                )
            mention_sql = " OR ".join(mention_clauses)
            obs_scope_clauses.append(
                f"actor_id = ANY($4::uuid[]) OR ({mention_sql})"
            )
        else:
            obs_scope_clauses.append("actor_id = ANY($4::uuid[])")
    if entity_list:
        mention_clauses = []
        for ent in entity_list:
            obs_params.append(_jsonb([ent]))
            mention_clauses.append(
                f"entities_mentioned @> ${len(obs_params)}::jsonb"
            )
        obs_scope_clauses.append("(" + " OR ".join(mention_clauses) + ")")
    if obs_scope_clauses:
        obs_sql += " AND (" + " OR ".join(obs_scope_clauses) + ")"
    obs_sql += " ORDER BY occurred_at DESC LIMIT " + str(int(max_observations))
    obs_rows = await conn.fetch(obs_sql, *obs_params)
    observations = _hydrate_many(obs_rows, _hydrate_obs, notes, "observations")

    # Models in the window (active). Overlap is COALESCE(last_retrieved_at,
    # created_at) — if a Model has been reconsolidated inside the window
    # it is also relevant, otherwise fall back to birth time.
    model_sql = f"SELECT {_MODEL_SELECT_SQL} FROM models " \
                "WHERE tenant_id = $1 AND status = 'active' " \
                "  AND COALESCE(last_retrieved_at, created_at) >= $2 " \
                "  AND COALESCE(last_retrieved_at, created_at) <= $3"
    model_params: list[Any] = [tenant_id, start, end]
    model_scope_clauses: list[str] = []
    if scope_actors:
        model_params.append(list(scope_actors))
        model_scope_clauses.append(f"scope_actors && ${len(model_params)}::uuid[]")
    for ent in entity_list:
        model_params.append(_jsonb([ent]))
        model_scope_clauses.append(f"scope_entities @> ${len(model_params)}::jsonb")
    if model_scope_clauses:
        model_sql += " AND (" + " OR ".join(model_scope_clauses) + ")"
    model_sql += (
        " ORDER BY COALESCE(last_retrieved_at, created_at) DESC LIMIT "
        + str(max(1, int(max_models)))
    )
    model_rows = await conn.fetch(model_sql, *model_params)
    models = _hydrate_many(model_rows, _hydrate_model, notes, "models")

    notes["observations_returned"] = len(observations)
    notes["models_returned"] = len(models)

    return PathwayResult(
        models=models,
        observations=observations,
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        source_pathway="C",
        notes=notes,
    )


# =====================================================================
# Pathway D — Pattern (Models with claim_role='pattern' matching a
# signature, plus their pattern-instance Models)
# =====================================================================


async def pathway_d_pattern(
    seed_signature: dict[str, Any] | None,
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    limit: int = _PATTERN_MAX_INSTANCES,
) -> PathwayResult:
    """
    Match Pattern Models whose `proposition->'signature' @> $seed`,
    then fetch their pattern_instance Models.

    If `seed_signature` is None, fall back to "all active pattern
    Models", which is what trigger T4 does when the background
    worker proposes a new pattern candidate without a specific
    shape yet.
    """
    notes: dict[str, Any] = {
        "has_signature": seed_signature is not None,
        "limit": limit,
    }

    if seed_signature is None:
        pattern_sql = f"""
            SELECT {_MODEL_SELECT_SQL}
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND claim_role = 'pattern'
              AND abstraction_level = 'pattern'
            ORDER BY activation DESC, created_at DESC
            LIMIT $2
        """
        pattern_rows = await conn.fetch(pattern_sql, tenant_id, limit)
    else:
        pattern_rows = await conn.fetch(
            f"""
            SELECT {_MODEL_SELECT_SQL}
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND claim_role = 'pattern'
              AND abstraction_level = 'pattern'
              AND proposition -> 'signature' @> $2::jsonb
            ORDER BY activation DESC, created_at DESC
            LIMIT $3
            """,
            tenant_id,
            _jsonb(seed_signature),
            limit,
        )

    patterns = _hydrate_many(pattern_rows, _hydrate_model, notes, "patterns")
    notes["patterns_returned"] = len(patterns)

    # Fetch instances for each pattern. The typed `instance_of` edge is
    # canonical; proposition.pattern_id remains a legacy/fallback shape.
    instances: list[ModelRow] = []
    if patterns:
        pattern_ids_str = [str(p.id) for p in patterns]
        pattern_ids = [p.id for p in patterns]
        inst_rows = await conn.fetch(
            f"""
            WITH edge_instances AS (
              SELECT DISTINCT e.source_model_id AS model_id
              FROM model_edges e
              WHERE e.tenant_id = $1
                AND e.status = 'active'
                AND e.review_status IN ('accepted', 'candidate', 'needs_review')
                AND (e.expires_at IS NULL OR e.expires_at > now())
                AND e.edge_kind = 'instance_of'
                AND e.target_model_id = ANY($2::uuid[])
            ),
            json_instances AS (
              SELECT m.id AS model_id
              FROM models m
              WHERE m.tenant_id = $1
                AND m.status = 'active'
                AND m.claim_role = 'pattern'
                AND m.time_mode = 'past'
                AND (m.proposition ->> 'pattern_id') = ANY($3::text[])
            ),
            instance_ids AS (
              SELECT model_id FROM edge_instances
              UNION
              SELECT model_id FROM json_instances
            )
            SELECT {_MODEL_SELECT_SQL}
            FROM models m
            JOIN instance_ids i ON i.model_id = m.id
            WHERE m.tenant_id = $1
              AND m.status = 'active'
            ORDER BY m.activation DESC, m.created_at DESC
            LIMIT $4
            """,
            tenant_id,
            pattern_ids,
            pattern_ids_str,
            limit,
        )
        instances = _hydrate_many(inst_rows, _hydrate_model, notes, "instances")
    notes["instances_returned"] = len(instances)

    return PathwayResult(
        models=patterns + instances,
        observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        source_pathway="D",
        notes=notes,
    )


# =====================================================================
# Pathway G — Model graph traversal (edges + composition expansion)
# =====================================================================


async def pathway_g_model_edges(
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    seed_model_ids: Sequence[UUID] | None = None,
    seed_entity_ids: Sequence[dict[str, Any]] | None = None,
    scope_actors: Sequence[UUID] | None = None,
    edge_kinds: Sequence[str] = _EDGE_TRAVERSAL_KINDS,
    max_hops: int = _DEFAULT_EDGE_MAX_HOPS,
    limit: int = _EDGE_MAX_MODELS,
) -> PathwayResult:
    """Traverse the Model graph around known seed Models.

    This is the retrieval path for hidden/non-obvious links: a Model can be
    semantically distant yet relevant because it contradicts, blocks,
    explains, predicts, shares an underlying issue with the seed, or belongs
    to the same composite situation. Composition membership is graph structure
    too, even though it lives in `model_composition_members` instead of
    `model_edges`.
    """
    notes: dict[str, Any] = {
        "seed_model_ids": len(seed_model_ids or []),
        "seed_entity_ids": len(seed_entity_ids or []),
        "scope_actors": len(scope_actors or []),
        "edge_kinds": list(edge_kinds),
        "max_hops": max_hops,
        "limit": limit,
    }
    if max_hops < 0:
        raise ValidationError("max_hops must be >= 0", max_hops=max_hops)
    if limit <= 0:
        return PathwayResult(source_pathway="G", notes={**notes, "reason": "non_positive_limit"})

    seeds: set[UUID] = set(seed_model_ids or [])
    entity_types: list[str] = []
    entity_ids: list[UUID] = []
    for raw in seed_entity_ids or []:
        if not isinstance(raw, dict):
            continue
        try:
            entity_type = _canonical_seed_type(raw["type"]) or str(raw["type"])
            entity_types.append(entity_type)
            entity_ids.append(UUID(str(raw["id"])))
        except (KeyError, TypeError, ValueError):
            continue

    if entity_ids or scope_actors:
        scoped_seed_rows = await conn.fetch(
            f"""
            WITH seeded_entities AS (
              SELECT * FROM unnest($2::text[], $3::uuid[])
                AS e(entity_type, entity_id)
            ),
            scoped_models AS (
              SELECT mse.model_id
              FROM model_scope_entities mse
              JOIN seeded_entities se
                ON se.entity_type = mse.entity_type
               AND se.entity_id = mse.entity_id
              WHERE mse.tenant_id = $1
              UNION
              SELECT msa.model_id
              FROM model_scope_actors msa
              WHERE msa.tenant_id = $1
                AND msa.actor_id = ANY($4::uuid[])
            )
            SELECT m.id
            FROM models m
            JOIN scoped_models sm ON sm.model_id = m.id
            WHERE m.tenant_id = $1 AND m.status = 'active'
            ORDER BY m.activation DESC, m.created_at DESC
            LIMIT $5
            """,
            tenant_id,
            entity_types,
            entity_ids,
            list(scope_actors or []),
            min(limit, 50),
        )
        seeds.update(r["id"] for r in scoped_seed_rows)
        notes["scope_seed_models"] = len(scoped_seed_rows)

    if not seeds:
        return PathwayResult(source_pathway="G", notes={**notes, "reason": "empty_seed"})

    edge_kinds = [str(k) for k in edge_kinds]
    visited: set[UUID] = set(seeds)
    frontier: set[UUID] = set(seeds)
    rank_by_model: dict[UUID, tuple[int, int, str]] = {
        mid: (0, 0, "seed") for mid in seeds
    }
    edge_rows_seen = 0
    composition_rows_seen = 0

    for hop in range(1, max_hops + 1):
        if not frontier or len(visited) >= limit:
            break
        composition_rows = await conn.fetch(
            """
            SELECT composite_model_id, member_model_id, confidence, source
            FROM model_composition_members
            WHERE tenant_id = $1
              AND (
                composite_model_id = ANY($2::uuid[])
                OR member_model_id = ANY($2::uuid[])
              )
            ORDER BY confidence DESC, created_at DESC
            LIMIT $3
            """,
            tenant_id,
            list(frontier),
            limit * 4,
        )
        composition_rows_seen += len(composition_rows)

        next_candidates: list[tuple[UUID, int, str]] = []
        for pos, row in enumerate(composition_rows):
            composite = row["composite_model_id"]
            member = row["member_model_id"]
            if composite in frontier:
                next_candidates.append((member, pos, "composition_member"))
            if member in frontier:
                next_candidates.append((composite, pos, "composition_parent"))

        rows = await conn.fetch(
            """
            SELECT source_model_id, target_model_id, edge_kind, confidence,
                   weight, review_status
            FROM model_edges
            WHERE tenant_id = $1
              AND status = 'active'
              AND review_status IN ('accepted', 'candidate', 'needs_review')
              AND (expires_at IS NULL OR expires_at > now())
              AND edge_kind = ANY($2::text[])
              AND (source_model_id = ANY($3::uuid[])
                OR target_model_id = ANY($3::uuid[]))
            ORDER BY
              CASE edge_kind
                WHEN 'contradicts' THEN 0
                WHEN 'weakens' THEN 1
                WHEN 'blocks' THEN 2
                WHEN 'early_warning_for' THEN 3
                WHEN 'same_issue_as' THEN 4
                ELSE 10
              END,
              confidence DESC,
              created_at DESC
            LIMIT $4
            """,
            tenant_id,
            edge_kinds,
            list(frontier),
            limit * 4,
        )
        edge_rows_seen += len(rows)
        edge_pos_offset = len(next_candidates)
        for pos, row in enumerate(rows, start=edge_pos_offset):
            source = row["source_model_id"]
            target = row["target_model_id"]
            other = target if source in frontier else source
            next_candidates.append((other, pos, row["edge_kind"]))

        next_frontier: set[UUID] = set()
        for other, pos, relation_kind in next_candidates:
            if other in visited:
                continue
            visited.add(other)
            next_frontier.add(other)
            rank_by_model[other] = (hop, pos, relation_kind)
            if len(visited) >= limit:
                break
        frontier = next_frontier

    if not visited:
        return PathwayResult(source_pathway="G", notes={**notes, "reason": "no_reachable_models"})

    model_rows = await conn.fetch(
        f"""
        SELECT {_MODEL_SELECT_SQL}
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        list(visited),
    )
    models = _hydrate_many(model_rows, _hydrate_model, notes, "edge_models")
    models.sort(
        key=lambda m: (
            rank_by_model.get(m.id, (999, 999, ""))[0],
            rank_by_model.get(m.id, (999, 999, ""))[1],
            -m.activation,
            str(m.id),
        )
    )
    notes["edge_rows_seen"] = edge_rows_seen
    notes["composition_rows_seen"] = composition_rows_seen
    notes["models_returned"] = len(models)
    notes["hops_executed"] = max((rank_by_model.get(m.id, (0, 0, ""))[0] for m in models), default=0)

    return PathwayResult(
        models=models[:limit],
        observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        source_pathway="G",
        notes=notes,
    )


__all__ = [
    "PathwayResult",
    "PathwayName",
    "pathway_a_structural",
    "pathway_b_semantic",
    "pathway_c_temporal",
    "pathway_d_pattern",
    "pathway_g_model_edges",
    "RetrievalPathwayError",
]
