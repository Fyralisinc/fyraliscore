"""
services/retrieval/pathways.py — the four primary retrieval pathways.

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

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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

PathwayName = Literal["A", "B", "C", "D"]

_DEFAULT_K_SEMANTIC = 40
_DEFAULT_TEMPORAL_WINDOW_DAYS = 7
_DEFAULT_STRUCTURAL_MAX_HOPS = 2
_STRUCTURAL_MAX_MODELS = 200
_TEMPORAL_MAX_OBSERVATIONS = 300
_PATTERN_MAX_INSTANCES = 200


class RetrievalPathwayError(CompanyOSError):
    default_code = "retrieval_pathway_error"


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


# =====================================================================
# Pathway A — Structural proximity (graph walk over Acts edges)
# =====================================================================


_SEED_ENTITY_TYPES = frozenset(
    {"commitment", "goal", "decision", "actor", "customer_resource", "resource"}
)


async def pathway_a_structural(
    seed_entity_ids: Sequence[dict[str, Any]],
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    max_hops: int = _DEFAULT_STRUCTURAL_MAX_HOPS,
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
        t = raw.get("type")
        rid = raw.get("id")
        if t not in _SEED_ENTITY_TYPES:
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
    frontier_customers: set[UUID] = set(seeds["customer_resource"])
    frontier_actors: set[UUID] = set(seeds["actor"])

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
        frontier_customers = new_customers
        frontier_actors = set()  # never expand beyond hop 0 for actors

        notes["hops_executed"] = hop + 1

        # Early exit if frontier is empty.
        if not (frontier_commits or frontier_goals or frontier_customers):
            break

    # Fetch full rows for the touched entities (tenant-filtered).
    goals_out: list[GoalRow] = []
    if visited_goals:
        grs = await conn.fetch(
            "SELECT * FROM goals WHERE id = ANY($1::uuid[]) AND tenant_id = $2",
            list(visited_goals),
            tenant_id,
        )
        goals_out = [_hydrate_goal(r) for r in grs]

    commitments_out: list[CommitmentRow] = []
    if visited_commits:
        crs = await conn.fetch(
            "SELECT * FROM commitments WHERE id = ANY($1::uuid[]) AND tenant_id = $2",
            list(visited_commits),
            tenant_id,
        )
        commitments_out = [_hydrate_commitment(r) for r in crs]

    decisions_out: list[DecisionRow] = []
    if visited_decisions:
        drs = await conn.fetch(
            "SELECT * FROM decisions WHERE id = ANY($1::uuid[]) AND tenant_id = $2",
            list(visited_decisions),
            tenant_id,
        )
        decisions_out = [_hydrate_decision(r) for r in drs]

    resources_out: list[ResourceRow] = []
    touched_resource_ids = visited_customers | visited_resources
    if touched_resource_ids:
        rrs = await conn.fetch(
            "SELECT * FROM resources WHERE id = ANY($1::uuid[]) AND tenant_id = $2",
            list(touched_resource_ids),
            tenant_id,
        )
        resources_out = [_hydrate_resource(r) for r in rrs]

    # Scoped Model search — union of (scope_entities @> any touched
    # entity) and (scope_actors && visited actors).
    scope_entity_filters: list[dict[str, Any]] = []
    for cid in visited_commits:
        scope_entity_filters.append({"type": "commitment", "id": str(cid)})
    for gid in visited_goals:
        scope_entity_filters.append({"type": "goal", "id": str(gid)})
    for did in visited_decisions:
        scope_entity_filters.append({"type": "decision", "id": str(did)})
    for crid in visited_customers:
        scope_entity_filters.append({"type": "customer_resource", "id": str(crid)})
        # Models may use 'resource' instead of 'customer_resource' for the
        # scope entity type depending on how Think writes them. Surface
        # both shapes so retrieval catches both vocabularies.
        scope_entity_filters.append({"type": "resource", "id": str(crid)})
    for rid in visited_resources:
        scope_entity_filters.append({"type": "resource", "id": str(rid)})

    models_out: list[ModelRow] = []
    if scope_entity_filters or visited_actors:
        # Build: (scope_entities @> '[...]' matches ANY) OR scope_actors && {}
        # We generate one UNION query per batch to leverage the GIN
        # index on scope_entities and the GIN index on scope_actors.
        params: list[Any] = [tenant_id]
        clauses: list[str] = []
        if visited_actors:
            params.append(list(visited_actors))
            clauses.append(f"scope_actors && ${len(params)}::uuid[]")
        # Bundle the entity filters via JSONB containment of ANY element.
        # GIN @> is the indexable direction; scope_entities @> '[{...}]'
        # is true when any array element matches. We OR a CASE for each
        # filter via a @> ANY(ARRAY[...]) equivalent: Postgres doesn't
        # have native "@> ANY(array)" so we iterate in Python and build
        # the OR chain. At scale-limited Wave-3 sizes, N filters is <
        # a few hundred and the plan is still fast because of the GIN.
        for f in scope_entity_filters:
            params.append(_jsonb([f]))
            clauses.append(f"scope_entities @> ${len(params)}::jsonb")
        if clauses:
            where = " OR ".join(clauses)
            sql = f"""
                SELECT {_MODEL_SELECT_SQL} FROM models
                WHERE tenant_id = $1
                  AND status = 'active'
                  AND ({where})
                ORDER BY activation DESC, created_at DESC
                LIMIT {_STRUCTURAL_MAX_MODELS}
            """
            rows = await conn.fetch(sql, *params)
            seen_ids: set[UUID] = set()
            for r in rows:
                mid = r["id"]
                if mid in seen_ids:
                    continue
                seen_ids.add(mid)
                models_out.append(_hydrate_model(r))

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
    services.models.repo. PoolConnectionProxy.__getattr__ delegates
    `_con` to the wrapped Connection, so we identify by that id.
    """
    try:
        from services.models.repo import PGVECTOR_REGISTERED_POOL_IDS
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
            await conn.execute(
                f"SET LOCAL hnsw.ef_search = {int(hnsw_ef_search)}"
            )
            notes["hnsw_ef_search"] = int(hnsw_ef_search)
        except asyncpg.PostgresError:
            # Not fatal — just means we're not in a tx or pgvector
            # version doesn't honor the GUC. Fall back to default.
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
    models = [_hydrate_model(r) for r in rows]
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
    max_observations: int = _TEMPORAL_MAX_OBSERVATIONS,
    include_entity_mentions: bool = True,
) -> PathwayResult:
    """
    Return Observations in [seed-window, seed+window] (tenant-filtered,
    optionally actor-filtered), plus active Models whose `created_at`
    or `last_retrieved_at` falls in the same window.

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
        "include_entity_mentions": include_entity_mentions,
    }

    # Observations query — tenant + time-range; optional actor filter.
    obs_sql = f"SELECT {_OBS_SELECT_SQL} FROM observations " \
              "WHERE tenant_id = $1 AND occurred_at >= $2 AND occurred_at <= $3"
    obs_params: list[Any] = [tenant_id, start, end]
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
            obs_sql += f" AND (actor_id = ANY($4::uuid[]) OR ({mention_sql}))"
        else:
            obs_sql += " AND actor_id = ANY($4::uuid[])"
    obs_sql += " ORDER BY occurred_at DESC LIMIT " + str(int(max_observations))
    obs_rows = await conn.fetch(obs_sql, *obs_params)
    observations = [_hydrate_obs(r) for r in obs_rows]

    # Models in the window (active). Overlap is COALESCE(last_retrieved_at,
    # created_at) — if a Model has been reconsolidated inside the window
    # it is also relevant, otherwise fall back to birth time.
    model_sql = f"SELECT {_MODEL_SELECT_SQL} FROM models " \
                "WHERE tenant_id = $1 AND status = 'active' " \
                "  AND COALESCE(last_retrieved_at, created_at) >= $2 " \
                "  AND COALESCE(last_retrieved_at, created_at) <= $3"
    model_params: list[Any] = [tenant_id, start, end]
    if scope_actors:
        model_params.append(list(scope_actors))
        model_sql += " AND scope_actors && $4::uuid[]"
    model_sql += " ORDER BY COALESCE(last_retrieved_at, created_at) DESC LIMIT 200"
    model_rows = await conn.fetch(model_sql, *model_params)
    models = [_hydrate_model(r) for r in model_rows]

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
# Pathway D — Pattern (Models with proposition_kind='pattern' matching
# a signature, plus their pattern_instance Models)
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
              AND proposition_kind = 'pattern'
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
              AND proposition_kind = 'pattern'
              AND proposition -> 'signature' @> $2::jsonb
            ORDER BY activation DESC, created_at DESC
            LIMIT $3
            """,
            tenant_id,
            _jsonb(seed_signature),
            limit,
        )

    patterns = [_hydrate_model(r) for r in pattern_rows]
    notes["patterns_returned"] = len(patterns)

    # Fetch instances for each pattern. A pattern_instance Model has
    # proposition.kind='pattern_instance' AND proposition.pattern_id
    # = the pattern's id (as string).
    instances: list[ModelRow] = []
    if patterns:
        pattern_ids_str = [str(p.id) for p in patterns]
        inst_rows = await conn.fetch(
            f"""
            SELECT {_MODEL_SELECT_SQL}
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND proposition_kind = 'pattern_instance'
              AND (proposition ->> 'pattern_id') = ANY($2::text[])
            ORDER BY activation DESC, created_at DESC
            LIMIT $3
            """,
            tenant_id,
            pattern_ids_str,
            limit,
        )
        instances = [_hydrate_model(r) for r in inst_rows]
    notes["instances_returned"] = len(instances)

    return PathwayResult(
        models=patterns + instances,
        observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        source_pathway="D",
        notes=notes,
    )


__all__ = [
    "PathwayResult",
    "PathwayName",
    "pathway_a_structural",
    "pathway_b_semantic",
    "pathway_c_temporal",
    "pathway_d_pattern",
    "RetrievalPathwayError",
]
