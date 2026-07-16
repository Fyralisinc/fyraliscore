"""
services/domain/models/repo.py — Models repository.

Schema refs (SCHEMA-LOCK.md):
  - S2.1 `models` table
  - S2.2 indexes on `models`
  - Post-Wave-0 amendments A1-A5 (proposition_kind generated, first-class
    confirmed/contested/last_confirmed/confidence_at_assertion/
    resolved_at/resolution_outcome/activation_coefficient columns,
    CHECK constraints, `deprecated` archive_reason, model_status_notes
    sidecar, no `contesting_actor` column)

Public API per BUILD-PLAN §2 Prompt 1-C + Q3 resolution:

  ModelsRepo(pool, *, embedder=None, tenant_id=...)

  .insert(proposed: ModelCreate, *, conn=None) -> ModelRow
      Nine-step spec pipeline (§2 Process):
        1. Falsifier adequacy if confidence > 0.7
        2. Validate proposition JSON (kind-discriminated union)
        3. apply_calibration (identity in Wave 1)
        4. Clip confidence to [0.05, 0.95]
        5. Validate scope_actors exist
        6. Compute embedding from `natural` (if no vec supplied)
        7. INSERT (proposition_kind is the generated column — never in
           the column list; confidence_at_assertion is written once
           here and never UPDATEd afterwards)
        8. Emit state_change observation (cause_id=born_from_event_id)
        9. Return Model

  .retrieve(ids, *, conn=None) -> list[ModelRow]
      Reconsolidation side effect: last_retrieved_at=now(),
      retrieval_count+=1, activation = LEAST(1.0, activation+0.15).
      confidence NOT touched.

  .archive(model_id, reason, *, conn=None) -> ModelRow
      status='archived', archived_at=now(), archive_reason=reason.
      Emits state_change AND enqueues every active dependent Model
      into `model_reeval_queue` with a cause_kind derived from the
      archive reason (Q8 resolved by migration 0007).

  .search_by_embedding(vec, k, *, filters=None, conn=None)
      HNSW cosine. Excludes status!='active' via the partial index.

  .search_by_scope(*, scope_actors=[], scope_entities=[], conn=None)
      GIN lookups.

  .get_predictions_due(before_ts, *, conn=None)
      evaluate_at <= before_ts AND status='active'.

  .bulk_confidence_update(updates, *, conn=None)
      For the calibration updater. Clips; emits state_change per change.
      NEVER touches confidence_at_assertion.

Q3 translations (baked in here):
  - `confidence_at_assertion` written at INSERT, immutable afterwards —
     never appears in any UPDATE statement this repo runs.
  - `deprecated_at` has no column; callers asking for deprecation pass
    `archive_reason='deprecated'` to `.archive()`.
  - `contesting_actor` is NOT exposed — callers must join observations.
  - `proposition_kind` is a GENERATED column, never in INSERT list.

No mocks. Real Postgres. Embedder may be `None` if the caller supplies
`proposed.embedding` explicitly, or we have a fixture with a hand-built
vector.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Sequence
from uuid import UUID

import asyncpg
import structlog
from pgvector.asyncpg import register_vector

from lib.embeddings.ollama import (
    EMBEDDING_DIM,
    OllamaClient,
    OllamaDimensionMismatch,
    OllamaError,
)
from lib.shared.edge_registry import EDGE_REGISTRY, get_spec
from lib.shared.errors import CompanyOSError, FalsifierInadequateError, ValidationError
from lib.shared.ids import uuid7
from lib.shared.memory_grammar import derive_memory_grammar
from lib.shared.types import (
    ModelArchiveReason,
    ModelCreate,
    ModelRow,
    ModelStatus,
    PropositionKind,
)

from services.domain.models.calibration import apply_calibration
from services.domain.models.batch import ModelBatchPlan, PlannedModel, plan_model_batch
from services.domain.models.constructor import ConstructedModel, construct_model
from services.domain.models.edges_repo import EdgesRepo
from services.domain.models.events import (
    MODEL_EVENT_ARCHIVED,
    MODEL_EVENT_CREATED,
    MODEL_EVENT_UPDATED,
    emit_model_event,
    emit_model_events,
    model_semantic_snapshot,
)
from services.domain.models.falsifier import is_adequate_falsifier
from services.domain.models.read_shapes import (
    MODEL_ROW_SELECT_COLS,
    MODEL_ROW_SELECT_SQL,
    hydrate_model_row,
)
from services.domain.models.recommendations import validate_recommendation
from services.domain.observations.events import NewObservationEvent, schedule_notify
from services.domain.observations.state_change import (
    STATE_CHANGE_CHANNEL,
    STATE_CHANGE_TRUST_TIER,
    emit_state_change,
    render_state_change_text,
)
from services.platform.access_control.authority import (
    ObjectRef,
    record_derived_access_labels,
    record_provenance_edge,
)
from services.reasoning.sage.affordances.policy import derive_default_profile_from_model
from services.reasoning.topology import LatentTopologyService
from services.reasoning.topology.anchor import content_anchor
# NOTE: audit module is imported lazily inside the methods that use it.
# Importing services.reasoning.think.audit at module-load time triggers
# services/reasoning/think/__init__.py, which imports reason.py → retrieval →
# services.domain.models.repo (this module). The circular import is benign at
# call time but fatal at module-load. The lazy imports below break the
# cycle without restructuring the package surface.


# ---------------------------------------------------------------------
# Constants + helpers
# ---------------------------------------------------------------------


class ModelsRepoError(CompanyOSError):
    default_code = "models_repo_error"


_CONFIDENCE_MIN = 0.05
_CONFIDENCE_MAX = 0.95
_FALSIFIER_REQUIRED_ABOVE = 0.7
_BULK_INSERT_PARAM_LIMIT = 60_000
_BULK_INSERT_DEFAULT_CHUNK_SIZE = 1_000
_log = structlog.get_logger(__name__)
BulkTimingSink = Callable[[dict[str, Any]], None]


# Columns written on INSERT. `proposition_kind` is GENERATED and
# `created_at` has a DEFAULT; both are intentionally absent.
_INSERT_COLS = (
    "id",
    "tenant_id",
    "born_from_event_id",
    "proposition",
    "natural",
    "embedding",
    "scope_actors",
    "scope_entities",
    "scope_temporal",
    "confidence",
    "activation",
    "falsifier",
    "signal_readings",
    "reading_contestable",
    "supporting_event_ids",
    "supporting_model_ids",
    "evidential_weight",
    "status",
    "evaluate_at",
    "resolution_criteria",
    "contributing_models",
    "visible_to_subjects",
    "confidence_at_assertion",   # immutable after this insert
    "activation_coefficient",
    # NOTE: proposition_kind omitted — it's generated from proposition->>'kind'
    # NOTE: confirmed_count/contested_count default 0 — caller can't override
    # NOTE: last_confirmed_at/resolved_at/resolution_outcome start NULL
)

# Canonical read order — always select the same shape so Pydantic
# hydration never has to reorder. "natural" is quoted because it's a
# reserved keyword (Wave 0 migration quotes it too).
_SELECT_COLS = MODEL_ROW_SELECT_COLS
_SELECT_COLS_SQL = MODEL_ROW_SELECT_SQL

_BULK_MODEL_COPY_COLUMNS = [
    "id",
    "tenant_id",
    "born_from_event_id",
    "proposition",
    "natural",
    "embedding",
    "scope_actors",
    "scope_entities",
    "scope_temporal",
    "confidence",
    "activation",
    "falsifier",
    "signal_readings",
    "reading_contestable",
    "supporting_event_ids",
    "supporting_model_ids",
    "evidential_weight",
    "status",
    "created_at",
    "evaluate_at",
    "resolution_criteria",
    "contributing_models",
    "visible_to_subjects",
    "confidence_at_assertion",
    "activation_coefficient",
    "domain_tags",
    "topo_embedding",
    "topo_updated_at",
]

_BULK_MODEL_VALUE_CASTS = {
    "proposition": "jsonb",
    "embedding": "vector",
    "scope_actors": "uuid[]",
    "scope_entities": "jsonb",
    "scope_temporal": "jsonb",
    "falsifier": "jsonb",
    "signal_readings": "jsonb",
    "supporting_event_ids": "uuid[]",
    "supporting_model_ids": "uuid[]",
    "resolution_criteria": "jsonb",
    "contributing_models": "uuid[]",
    "domain_tags": "text[]",
    "topo_embedding": "vector",
}


# =====================================================================
# PUBLIC API: pgvector pool-shared codec registry
# =====================================================================
#
# `PGVECTOR_REGISTERED_POOL_IDS` is the process-wide set of asyncpg
# connection object ids that have had the pgvector codec registered
# via `pgvector.asyncpg.register_vector(conn)`. Any code that:
#
#   (a) shares an asyncpg pool with `ModelsRepo` (the gateway, the
#       Think worker, the synthesis harness, any test suite), AND
#   (b) wants retrieval Pathway B to bind seed vectors as numpy
#       arrays (the fast path) rather than as `'[…]'::vector` text
#       literals (the slow legacy path),
#
# MUST ensure every pooled connection has been added to this set
# before retrieval reads run on it. The recommended way is to call
# `register_pgvector_on_pool(pool)` once at startup, which hooks
# `register_vector` into the pool's `init` callback and also adds
# the connection's id to this set.
#
# Why a set of int ids and not a WeakSet:
#   asyncpg `PoolConnectionProxy` objects cannot be weak-referenced
#   (they have __slots__), and the set must survive across the
#   `Connection`/`PoolConnectionProxy` boundary. We track raw `id()`
#   values, accepting that the set may transiently retain ids past
#   connection eviction; the bounded clear at 1000 entries handles
#   long-running processes.
#
# Why pool-shared, not per-connection:
#   The codec lives on the asyncpg connection's codec map. asyncpg
#   pools reuse connections across acquisitions, so registering on
#   first use of a connection persists for the connection's lifetime
#   in that pool. Pathway B
#   (services/reasoning/retrieval/pathways.py:_conn_has_vector_codec) reads
#   this set to decide whether to bind a list of floats (fast,
#   binary) or a stringified `[…]` literal cast as `::vector` (slow,
#   text). If the set says "registered" but the connection's codec
#   was somehow not registered, asyncpg fails with a confusing
#   `could not convert string to float` error — see
#   tests/synthesis_harness/REPORT.md §8 for the full story.
#
# Treat this name as load-bearing. Any new pool that talks to the
# Models surface MUST go through `register_pgvector_on_pool` (or
# replicate its semantics — register the codec and add the
# connection id to this set).
# =====================================================================

PGVECTOR_REGISTERED_POOL_IDS: set[int] = set()

# Backwards-compat alias for callers that imported the old name. New
# code should use the public name. The alias is the same set object,
# so adding to either still tracks correctly.
_VECTOR_REGISTERED_IDS = PGVECTOR_REGISTERED_POOL_IDS


async def _ensure_vector_codec(conn: asyncpg.Connection) -> None:
    """Lazily register the pgvector codec on `conn` and remember it.

    Idempotent: a second call against the same connection is a
    no-op. Used by ModelsRepo's per-call paths that don't go through
    `register_pgvector_on_pool` (e.g. ad-hoc connections opened in
    one-off scripts).
    """
    key = id(conn)
    if key in PGVECTOR_REGISTERED_POOL_IDS:
        return
    try:
        await register_vector(conn)
    except Exception:
        # Duplicate registration is safe; swallow.
        pass
    PGVECTOR_REGISTERED_POOL_IDS.add(key)
    inner = getattr(conn, "_con", None)
    if inner is not None:
        PGVECTOR_REGISTERED_POOL_IDS.add(id(inner))
    # Bound the set so it doesn't grow unbounded in long-running procs.
    if len(PGVECTOR_REGISTERED_POOL_IDS) > 1000:
        PGVECTOR_REGISTERED_POOL_IDS.clear()


async def pgvector_pool_init(conn: asyncpg.Connection) -> None:
    """asyncpg pool `init` callback that installs the pgvector codec.

    Pass this as `init=pgvector_pool_init` to `asyncpg.create_pool(...)`.
    asyncpg invokes it on every connection the pool produces — both
    the initial `min_size` set and any later expansions up to
    `max_size` — so all connections are uniformly registered.

    Records the connection's id in `PGVECTOR_REGISTERED_POOL_IDS`
    (and the inner connection's id, if `conn` is a proxy) so Pathway
    B's `_conn_has_vector_codec` check returns True.

    Idempotent: a duplicate `register_vector` call against the same
    connection is a no-op at the Postgres level.
    """
    try:
        await register_vector(conn)
    except Exception:
        # Duplicate registration or pgvector extension missing in a
        # test sandbox — both safe to swallow.
        pass
    PGVECTOR_REGISTERED_POOL_IDS.add(id(conn))
    inner = getattr(conn, "_con", None)
    if inner is not None:
        PGVECTOR_REGISTERED_POOL_IDS.add(id(inner))


async def register_pgvector_on_pool(pool: asyncpg.Pool) -> None:
    """Register the pgvector codec on every CURRENT connection in a pool.

    Most callers should pass `init=pgvector_pool_init` to
    `asyncpg.create_pool(...)` instead — that's the only way to
    guarantee future-spawned connections also register. This helper
    exists for the case where the pool is already constructed and
    the caller cannot replace it.

    Walks idle connections by acquiring them serially, registering
    the codec, then releasing. Does NOT install an init callback,
    so connections created later (when the pool grows under load)
    will not be registered until they happen to be acquired by
    code that goes through `_ensure_vector_codec`. Use the init
    pattern instead when you can.
    """
    # Acquire min_size connections to ensure the initial set is
    # registered. Iteratively to avoid holding more than one at a time.
    seen: set[int] = set()
    for _ in range(getattr(pool, "_minsize", 1)):
        async with pool.acquire() as conn:
            if id(conn) in seen:
                break
            seen.add(id(conn))
            await pgvector_pool_init(conn)


def _jsonb(value: Any) -> str:
    """asyncpg needs a JSON string when the param is cast ::jsonb."""
    return json.dumps(value, sort_keys=True, default=str)


def _sql_ident(identifier: str) -> str:
    if (
        not identifier
        or identifier[0].isdigit()
        or not all(char.isalnum() or char == "_" for char in identifier)
    ):
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
    return f'"{identifier}"'


async def _table_uses_row_security(conn: asyncpg.Connection, table: str) -> bool:
    row = await conn.fetchrow(
        """
        SELECT relrowsecurity OR relforcerowsecurity AS enabled
        FROM pg_class
        WHERE oid = $1::regclass
        """,
        table,
    )
    return bool(row and row["enabled"])


async def _bulk_write_records(
    conn: asyncpg.Connection,
    table: str,
    *,
    records: Sequence[Sequence[Any]],
    columns: Sequence[str],
    casts: dict[str, str] | None = None,
    chunk_size: int = _BULK_INSERT_DEFAULT_CHUNK_SIZE,
) -> None:
    """Write many rows while respecting PostgreSQL RLS.

    PostgreSQL rejects COPY FROM on tables with row-level security for
    non-owner app roles. When RLS is enabled, use chunked multi-value
    INSERTs so the real product policies still apply.
    """
    if not records:
        return
    if not await _table_uses_row_security(conn, table):
        await conn.copy_records_to_table(table, records=records, columns=columns)
        return

    await _insert_records_values(
        conn,
        table,
        records=records,
        columns=columns,
        casts=casts or {},
        chunk_size=chunk_size,
    )


async def _insert_records_values(
    conn: asyncpg.Connection,
    table: str,
    *,
    records: Sequence[Sequence[Any]],
    columns: Sequence[str],
    casts: dict[str, str],
    chunk_size: int,
) -> None:
    if not records:
        return
    if not columns:
        raise ValueError("Bulk insert requires at least one column")

    table_sql = _sql_ident(table)
    columns_sql = ", ".join(_sql_ident(column) for column in columns)
    max_rows_by_params = max(1, _BULK_INSERT_PARAM_LIMIT // len(columns))
    effective_chunk_size = max(1, min(chunk_size, max_rows_by_params))

    for offset in range(0, len(records), effective_chunk_size):
        chunk = records[offset:offset + effective_chunk_size]
        params: list[Any] = []
        values_sql: list[str] = []
        param_idx = 1
        for row in chunk:
            if len(row) != len(columns):
                raise ValueError(
                    f"Bulk row for {table} has {len(row)} values; "
                    f"expected {len(columns)}"
                )
            placeholders: list[str] = []
            for column, value in zip(columns, row, strict=True):
                cast = casts.get(column)
                placeholder = f"${param_idx}"
                if cast:
                    placeholder = f"{placeholder}::{cast}"
                placeholders.append(placeholder)
                params.append(value)
                param_idx += 1
            values_sql.append(f"({', '.join(placeholders)})")

        await conn.execute(
            f"INSERT INTO {table_sql} ({columns_sql}) VALUES {', '.join(values_sql)}",
            *params,
        )


async def _upsert_model_semantic_terms(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    semantic_terms: Sequence[str],
) -> None:
    await conn.execute(
        """
        INSERT INTO model_semantic_terms (
          tenant_id, model_id, semantic_terms, updated_at
        ) VALUES ($1, $2, $3::text[], now())
        ON CONFLICT (tenant_id, model_id) DO UPDATE
        SET semantic_terms = EXCLUDED.semantic_terms,
            updated_at = now()
        """,
        tenant_id,
        model_id,
        list(semantic_terms or ()),
    )


async def _bulk_upsert_model_semantic_terms(
    conn: asyncpg.Connection,
    rows: Sequence[ModelRow],
) -> int:
    if not rows:
        return 0
    await conn.executemany(
        """
        INSERT INTO model_semantic_terms (
          tenant_id, model_id, semantic_terms, updated_at
        ) VALUES ($1, $2, $3::text[], now())
        ON CONFLICT (tenant_id, model_id) DO UPDATE
        SET semantic_terms = EXCLUDED.semantic_terms,
            updated_at = now()
        """,
        [
            (row.tenant_id, row.id, list(row.semantic_terms or []))
            for row in rows
        ],
    )
    return len(rows)


async def _bulk_upsert_default_affordance_profiles(
    conn: asyncpg.Connection,
    rows: Sequence[ModelRow],
) -> int:
    """Seed derived retrieval affordances for freshly inserted Models.

    The profile is utility-layer metadata and is inserted only when missing,
    so future reinforcement is preserved and canonical Model truth is not
    rewritten through this path.
    """
    if not rows:
        return 0
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.retrieval_affordance_profiles')"
    )
    if table_name is None:
        return 0

    params: list[tuple[Any, ...]] = []
    for row in rows:
        try:
            profile = derive_default_profile_from_model(row)
        except Exception:
            _log.warning(
                "models.affordance_default_derive_failed",
                model_id=str(row.id),
                tenant_id=str(row.tenant_id),
                exc_info=True,
            )
            continue
        params.append(
            (
                profile.model_id,
                profile.tenant_id,
                list(profile.answers_question_primitives),
                list(profile.supports_hypothesis_types),
                list(profile.weakens_hypothesis_types),
                list(profile.common_composition_types),
                list(profile.action_affordances),
                _jsonb(profile.activation_signatures),
                _jsonb(profile.projection_policy),
                float(profile.utility_score),
                profile.decay_after,
                profile.last_reinforced_at,
            )
        )
    if not params:
        return 0

    await conn.executemany(
        """
        INSERT INTO retrieval_affordance_profiles (
          model_id, tenant_id,
          answers_question_primitives, supports_hypothesis_types,
          weakens_hypothesis_types, common_composition_types,
          action_affordances, activation_signatures, projection_policy,
          utility_score, decay_after, last_reinforced_at,
          last_updated_at
        ) VALUES (
          $1, $2,
          $3::text[], $4::text[],
          $5::text[], $6::text[],
          $7::text[], $8::jsonb, $9::jsonb,
          $10, $11, $12,
          now()
        )
        ON CONFLICT (model_id) DO NOTHING
        """,
        params,
    )
    return len(params)


def _clip_confidence(value: float) -> float:
    if value < _CONFIDENCE_MIN:
        return _CONFIDENCE_MIN
    if value > _CONFIDENCE_MAX:
        return _CONFIDENCE_MAX
    return float(value)


def _scope_entity_uuid(entry: dict[str, Any]) -> UUID | None:
    try:
        return UUID(str(entry.get("id")))
    except (TypeError, ValueError, AttributeError):
        return None


async def _expand_scope_entities_via_customer_commitments(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    scope_entities: Sequence[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Expand a single customer/commitment scope across customer_commitments.

    A live customer memory often lands on the linked commitment ("Renew
    Globex contract") even when callers ask from the customer ("Globex").
    This linkage keeps customer and commitment memory discoverable without
    forcing the LLM to duplicate every scope perfectly.
    """
    if not scope_entities or len(scope_entities) != 1:
        return list(scope_entities or [])
    entry = dict(scope_entities[0])
    entity_id = _scope_entity_uuid(entry)
    if entity_id is None:
        return [entry]

    entity_type = str(entry.get("type") or "").casefold()
    expanded: list[dict[str, Any]] = [entry]

    if entity_type in {"", "customer", "resource", "relational"}:
        rows = await conn.fetch(
            """
            SELECT commitment_id
            FROM customer_commitments
            WHERE tenant_id = $1
              AND customer_resource_id = $2
            """,
            tenant_id,
            entity_id,
        )
        expanded.extend(
            {"type": "commitment", "id": str(r["commitment_id"])}
            for r in rows
        )
        identity = await conn.fetchval(
            """
            SELECT identity
            FROM resources
            WHERE tenant_id = $1
              AND id = $2
              AND kind = 'relational'
              AND archived_at IS NULL
            """,
            tenant_id,
            entity_id,
        )
        if identity:
            aliases = _customer_identity_aliases(str(identity))
            if aliases:
                like_patterns = [f"%{alias}%" for alias in aliases]
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
                expanded.extend(
                    {"type": "commitment", "id": str(r["id"])}
                    for r in rows
                )
        if entity_type == "resource":
            expanded.append({"type": "customer", "id": str(entity_id)})

    if entity_type in {"", "commitment"}:
        rows = await conn.fetch(
            """
            SELECT customer_resource_id
            FROM customer_commitments
            WHERE tenant_id = $1
              AND commitment_id = $2
            """,
            tenant_id,
            entity_id,
        )
        expanded.extend(
            {"type": "customer", "id": str(r["customer_resource_id"])}
            for r in rows
        )

    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for item in expanded:
        key = _jsonb(item)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _customer_identity_aliases(identity: str) -> list[str]:
    cleaned = " ".join(identity.split()).strip()
    if not cleaned:
        return []
    aliases = [cleaned]
    suffixes = (" inc", " corp", " llc", " ltd", " incorporated")
    folded = cleaned.casefold()
    for suffix in suffixes:
        if folded.endswith(suffix):
            aliases.append(cleaned[: -len(suffix)].strip())
            break
    return [a for a in aliases if a]


def _append_scope_entities_filter(
    where: list[str],
    params: list[Any],
    scope_entities: Sequence[dict[str, Any]] | None,
    expanded_scope_entities: Sequence[dict[str, Any]],
) -> None:
    if not scope_entities:
        return
    if len(scope_entities) != 1:
        params.append(_jsonb(list(scope_entities)))
        where.append(f"scope_entities @> ${len(params)}::jsonb")
        return
    clauses: list[str] = []
    for entry in expanded_scope_entities:
        params.append(_jsonb([entry]))
        clauses.append(f"scope_entities @> ${len(params)}::jsonb")
    where.append("(" + " OR ".join(clauses) + ")")


# S1 (migration 0031): cause_kind mapping moved to lib/shared/edge_registry.py
# inside the supports cascade callback. The registry owns this mapping
# now because cause_kind is a per-edge_kind concern, not a per-archive
# concern. See _supports_on_source_archive in edge_registry.py.

# Edge-kind → array column on `models` table. During the dual-write
# phase, every typed-edge mutation also updates the legacy array so
# pre-S1 consumers (cascade query in archive(), retrieval second-pass,
# debug UI) keep working unchanged. The drift detector verifies these
# stay in sync. Stage 2 (separate plan) cuts consumers to read edges
# directly; Stage 3 drops the array columns.
_EDGE_KIND_TO_ARRAY_COL: dict[str, str] = {
    "supports": "supporting_model_ids",
    "contributes_to_resolution": "contributing_models",
    # `instance_of` shares the legacy supporting_model_ids array — pre-S1
    # the pattern proposer appended the Pattern id to constituents'
    # supporting_model_ids. We preserve that exact behavior during dual-
    # write so retrieval expansion still surfaces the Pattern.
    "instance_of": "supporting_model_ids",
    # `superseded_by` has no legacy array column — supersession was
    # encoded as `archive_reason='superseded'` only. Its edge is purely
    # additive in S1.
}


# Singleton EdgesRepo. Lives at module scope so every method can route
# through it without threading a repo arg. EdgesRepo holds no state
# beyond an optional pool reference, which we don't use in conn-only
# callers — every public ModelsRepo method takes `conn` and forwards.
_EDGES = EdgesRepo()

# Singleton latent topology service. It is a best-effort pre-truth
# candidate generator, not accepted memory. It runs inside the insert
# transaction so candidates and any bounded Think trigger enqueue commit
# atomically with the Model that produced them.
_TOPOLOGY = LatentTopologyService()


async def _check_no_support_cycle(
    conn: asyncpg.Connection,
    *,
    new_model_id: UUID,
    new_supports: list[UUID],
) -> None:
    """
    Invariant M3: the supporting-evidence DAG must remain acyclic.

    Post-S1 (migration 0031): cycle scope is the registry's
    cycle_scope for `supports`, which is `{supports, instance_of}` —
    a Model cannot transitively support its own pattern via either
    edge. Delegates to EdgesRepo.check_no_cycle which runs a
    recursive CTE over `model_edges`.

    During dual-write, the typed `supports` edges may not yet exist
    for every Model (backfill is incremental). When that's the case,
    falling back to the legacy array-based check ensures we don't
    miss cycles formed against pre-S1 data. We run BOTH checks:

      1. Edge-based check (authoritative going forward).
      2. Legacy array-based check (catches pre-S1 cycles that
         haven't been backfilled yet).

    Self-support is explicitly rejected.
    """
    if not new_supports:
        return

    # Self-support.
    if new_model_id in new_supports:
        raise ValidationError(
            "supporting_model_ids cannot reference the model itself",
            model_id=str(new_model_id),
        )

    # Edge-based cycle check (registry-driven).
    # We need tenant_id to scope the query; fetch it from the proposed
    # model's existing row if it exists, or the targets' rows.
    tenant_id = await conn.fetchval(
        "SELECT tenant_id FROM models WHERE id = ANY($1::uuid[]) LIMIT 1",
        new_supports,
    )
    if tenant_id is not None:
        await _EDGES.check_no_cycle(
            conn,
            kind="supports",
            source=new_model_id,
            targets=new_supports,
            tenant_id=tenant_id,
        )

    # Legacy array-based cycle check, retained during dual-write to
    # catch cycles in pre-backfill data. Same recursive CTE shape as
    # the pre-S1 version. Drop in Stage 3 once arrays go away.
    row = await conn.fetchrow(
        """
        WITH RECURSIVE support_ancestors AS (
          SELECT unnest(supporting_model_ids) AS ancestor_id
            FROM models
            WHERE id = ANY($1::uuid[])
          UNION
          SELECT unnest(m.supporting_model_ids)
            FROM models m
            JOIN support_ancestors sa ON m.id = sa.ancestor_id
        )
        SELECT 1 FROM support_ancestors WHERE ancestor_id = $2 LIMIT 1
        """,
        new_supports,
        new_model_id,
    )
    if row is not None:
        raise ValidationError(
            "supporting_model_ids would create a cycle",
            new_model_id=str(new_model_id),
            new_supports=[str(s) for s in new_supports],
        )


# =====================================================================
# _set_model_relations — THE CHOKEPOINT for dual-write (S1)
# =====================================================================
#
# Every site that mutates a Model's relational state MUST go through
# this helper. It computes the diff between the current state and the
# desired state, writes both the typed `model_edges` rows AND the
# legacy array columns inside the same transaction, runs the
# generalized cycle check, and emits the cascade-prep work.
#
# Three call sites in S1:
#   1. ModelsRepo._insert_core — INSERT path: writes initial edges
#      from proposed.supporting_model_ids and proposed.contributing_models.
#   2. _apply_claim_op (services/reasoning/think/applier.py) — UPDATE path:
#      when claim_op.changes touches an array column, route through here.
#   3. promote_pattern_candidate (services/workers/precipitation/proposer.py)
#      — appends `instance_of` edges + back-links via supporting_model_ids.
#
# The drift detector verifies arrays stay in sync; if any future code
# bypasses this helper, the drift metric goes non-zero.
async def _set_model_relations(
    conn: asyncpg.Connection,
    *,
    model_id: UUID,
    tenant_id: UUID,
    detected_by: str,
    supports: list[UUID] | None = None,
    contributes_to: list[UUID] | None = None,
    instance_of: list[UUID] | None = None,
    superseded_by: UUID | None = None,
    created_by_event_id: UUID | None = None,
    update_arrays: bool = True,
) -> None:
    """Synchronize typed edges + legacy arrays for a single Model.

    Each named arg is the FULL desired list/value:
      - supports / contributes_to: replace the array with this list,
        diff against existing edges, INSERT/DELETE accordingly.
      - instance_of: list of pattern Models this Model is an instance
        of. Each gets an `instance_of` typed edge AND an append to
        supporting_model_ids (legacy back-link preserved).
      - superseded_by: a single replacement Model id; writes one
        `superseded_by` typed edge. No legacy array; supersession was
        previously implicit in archive_reason.
      - update_arrays: if False, only the edge rows are written. Used
        by the INSERT path because the INSERT statement itself sets
        the array columns (we just need to mirror to edges).

    None means "don't touch this kind"; pass [] to clear the kind
    explicitly. supports/contributes_to/instance_of are unioned
    against the supporting_model_ids array for the back-link
    semantics.
    """
    # Direction matters per edge_kind:
    #
    #   - `supports`: list elements are the SUPPORTERS of model_id
    #     (incoming). Edge direction: (supporter, model_id, 'supports').
    #     This matches the legacy supporting_model_ids array semantics
    #     ("A is in M's array iff A supports M").
    #
    #   - `contributes_to_resolution`: list elements are the
    #     CONTRIBUTORS to model_id's prediction (incoming). Edge:
    #     (contributor, model_id, 'contributes_to_resolution').
    #     Matches legacy contributing_models array semantics.
    #
    #   - `instance_of`: list elements are the PATTERNS this model is
    #     an instance of (outgoing). Edge: (model_id, pattern,
    #     'instance_of'). Note this is OUTGOING from the perspective
    #     of model_id — opposite direction from the two above. The
    #     legacy back-link (pattern id appended to model's
    #     supporting_model_ids array) is preserved by
    #     _sync_array_columns; only the typed edge has the
    #     semantically correct direction.
    if supports is not None:
        await _sync_incoming_kind(
            conn,
            kind="supports",
            model_id=model_id,
            tenant_id=tenant_id,
            new_sources=supports,
            detected_by=detected_by,
            created_by_event_id=created_by_event_id,
        )
    if contributes_to is not None:
        await _sync_incoming_kind(
            conn,
            kind="contributes_to_resolution",
            model_id=model_id,
            tenant_id=tenant_id,
            new_sources=contributes_to,
            detected_by=detected_by,
            created_by_event_id=created_by_event_id,
        )
    if instance_of is not None:
        await _sync_outgoing_kind(
            conn,
            kind="instance_of",
            model_id=model_id,
            tenant_id=tenant_id,
            new_targets=instance_of,
            detected_by=detected_by,
            created_by_event_id=created_by_event_id,
        )
    if superseded_by is not None:
        # Singleton edge; no array sync. Idempotent on UNIQUE.
        await _EDGES.link(
            conn,
            source=model_id,
            target=superseded_by,
            kind="superseded_by",
            tenant_id=tenant_id,
            detected_by=detected_by,
            created_by_event_id=created_by_event_id,
        )

    if update_arrays:
        await _sync_array_columns(
            conn,
            model_id=model_id,
            supports=supports,
            contributes_to=contributes_to,
            instance_of=instance_of,
        )


async def _sync_incoming_kind(
    conn: asyncpg.Connection,
    *,
    kind: str,
    model_id: UUID,
    tenant_id: UUID,
    new_sources: list[UUID],
    detected_by: str,
    created_by_event_id: UUID | None,
) -> None:
    """Diff incoming edges of `kind` to `model_id` against the
    desired source list, INSERT/DELETE to converge.

    Used for `supports` and `contributes_to_resolution`, where the
    legacy array on the model lists the OTHER endpoints (supporters /
    contributors) and the typed edge points FROM each of them TO
    model_id.

    Concretely: caller passes new_sources=[A, B] meaning A and B point
    at model_id via this kind. Typed edges written: (A, model_id,
    kind), (B, model_id, kind).
    """
    existing = await _EDGES.traverse_backward(
        conn,
        target=model_id,
        kinds=[kind],
        tenant_id=tenant_id,
        status="active",
    )
    existing_sources = {e["source_model_id"] for e in existing}
    desired_sources = set(new_sources)

    to_add = desired_sources - existing_sources
    to_remove = existing_sources - desired_sources

    for source in to_add:
        await _EDGES.link(
            conn,
            source=source,
            target=model_id,
            kind=kind,
            tenant_id=tenant_id,
            detected_by=detected_by,
            created_by_event_id=created_by_event_id,
        )
    for source in to_remove:
        await _EDGES.unlink(
            conn,
            source=source,
            target=model_id,
            kind=kind,
            tenant_id=tenant_id,
        )


async def _sync_outgoing_kind(
    conn: asyncpg.Connection,
    *,
    kind: str,
    model_id: UUID,
    tenant_id: UUID,
    new_targets: list[UUID],
    detected_by: str,
    created_by_event_id: UUID | None,
) -> None:
    """Diff outgoing edges of `kind` from `model_id` against the
    desired target list, INSERT/DELETE to converge.

    Used for `instance_of`, where the typed edge points FROM
    model_id TO each pattern. The legacy back-link (appending the
    pattern id to model_id's supporting_model_ids array) is handled
    separately by _sync_array_columns.

    Concretely: caller passes new_targets=[P] meaning model_id is an
    instance of P. Typed edge written: (model_id, P, 'instance_of').
    """
    existing = await _EDGES.traverse_forward(
        conn,
        source=model_id,
        kinds=[kind],
        tenant_id=tenant_id,
        status="active",
    )
    existing_targets = {e["target_model_id"] for e in existing}
    desired_targets = set(new_targets)

    to_add = desired_targets - existing_targets
    to_remove = existing_targets - desired_targets

    for target in to_add:
        await _EDGES.link(
            conn,
            source=model_id,
            target=target,
            kind=kind,
            tenant_id=tenant_id,
            detected_by=detected_by,
            created_by_event_id=created_by_event_id,
        )
    for target in to_remove:
        await _EDGES.unlink(
            conn,
            source=model_id,
            target=target,
            kind=kind,
            tenant_id=tenant_id,
        )


async def _sync_array_columns(
    conn: asyncpg.Connection,
    *,
    model_id: UUID,
    supports: list[UUID] | None,
    contributes_to: list[UUID] | None,
    instance_of: list[UUID] | None,
) -> None:
    """Update legacy array columns to mirror the desired edge state.

    `supporting_model_ids` is the union of `supports` and
    `instance_of` entries (matches pre-S1 pattern-promoter behavior
    of appending pattern ids to supporting_model_ids).

    `contributing_models` is `contributes_to` directly.

    None args leave the corresponding column untouched.
    """
    if supports is None and instance_of is None and contributes_to is None:
        return
    # Read the current arrays so we can compute the right merge for
    # supporting_model_ids when only one of (supports, instance_of) is
    # supplied.
    if supports is not None or instance_of is not None:
        current = await conn.fetchrow(
            "SELECT supporting_model_ids FROM models WHERE id = $1",
            model_id,
        )
        if current is None:
            # Model doesn't exist yet (called pre-INSERT). Skip; the
            # INSERT itself will set the array.
            return
        existing_supporting = list(current["supporting_model_ids"] or [])
        # Compute desired supporting_model_ids:
        #   - If `supports` was supplied, replace its contribution.
        #   - If `instance_of` was supplied, replace its contribution.
        #   - For the unspecified dimension, retain what's already there
        #     by reading current edges of the corresponding kind.
        if supports is None:
            sup_part = await _read_array_part(
                conn, model_id, "supports"
            )
        else:
            sup_part = list(supports)
        if instance_of is None:
            inst_part = await _read_array_part(
                conn, model_id, "instance_of"
            )
        else:
            inst_part = list(instance_of)
        # Stable order: deduplicate while preserving first occurrence.
        seen: set[UUID] = set()
        merged: list[UUID] = []
        for u in sup_part + inst_part:
            if u not in seen:
                seen.add(u)
                merged.append(u)
        if merged != existing_supporting:
            await conn.execute(
                "UPDATE models SET supporting_model_ids = $1::uuid[] WHERE id = $2",
                merged,
                model_id,
            )
    if contributes_to is not None:
        await conn.execute(
            "UPDATE models SET contributing_models = $1::uuid[] WHERE id = $2",
            list(contributes_to),
            model_id,
        )


async def _sync_model_scope_sidecars(
    conn: asyncpg.Connection,
    *,
    model_id: UUID,
    tenant_id: UUID,
    scope_actors: Sequence[UUID],
    scope_entities: Sequence[dict[str, Any]],
    source: str = "model_scope",
) -> None:
    """Mirror Model scope JSON/arrays into normalized retrieval sidecars."""
    await conn.execute(
        "DELETE FROM model_scope_actors WHERE tenant_id = $1 AND model_id = $2",
        tenant_id,
        model_id,
    )
    await conn.execute(
        "DELETE FROM model_scope_entities WHERE tenant_id = $1 AND model_id = $2",
        tenant_id,
        model_id,
    )
    actor_ids = []
    seen_actors: set[UUID] = set()
    for actor_id in scope_actors or []:
        if actor_id in seen_actors:
            continue
        seen_actors.add(actor_id)
        actor_ids.append(actor_id)
    if actor_ids:
        await conn.executemany(
            """
            INSERT INTO model_scope_actors
              (model_id, tenant_id, actor_id, source, confidence)
            VALUES ($1, $2, $3, $4, 1.0)
            ON CONFLICT (model_id, actor_id) DO UPDATE
              SET source = EXCLUDED.source,
                  confidence = EXCLUDED.confidence
            """,
            [(model_id, tenant_id, actor_id, source) for actor_id in actor_ids],
        )

    entity_rows: list[tuple[UUID, UUID, str, UUID, str]] = []
    seen_entities: set[tuple[str, UUID]] = set()
    for raw in scope_entities or []:
        if not isinstance(raw, dict):
            continue
        entity_type = raw.get("type")
        entity_id = raw.get("id")
        if not entity_type or entity_id is None:
            continue
        try:
            entity_uuid = entity_id if isinstance(entity_id, UUID) else UUID(str(entity_id))
        except (ValueError, TypeError):
            continue
        key = (str(entity_type), entity_uuid)
        if key in seen_entities:
            continue
        seen_entities.add(key)
        entity_rows.append((model_id, tenant_id, str(entity_type), entity_uuid, source))
    if entity_rows:
        await conn.executemany(
            """
            INSERT INTO model_scope_entities
              (model_id, tenant_id, entity_type, entity_id, source, confidence)
            VALUES ($1, $2, $3, $4, $5, 1.0)
            ON CONFLICT (model_id, entity_type, entity_id) DO UPDATE
              SET source = EXCLUDED.source,
                  confidence = EXCLUDED.confidence
            """,
            entity_rows,
        )


def _uuid_list_from_json(value: Any) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    if not isinstance(value, (list, tuple)):
        return out
    for raw in value:
        try:
            uid = raw if isinstance(raw, UUID) else UUID(str(raw))
        except (TypeError, ValueError):
            continue
        if uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return out


async def _sync_model_composition_members(
    conn: asyncpg.Connection,
    *,
    model_id: UUID,
    tenant_id: UUID,
    proposition: dict[str, Any],
    source: str = "model_proposition",
) -> None:
    """Mirror situation member_model_ids into a normalized sidecar."""
    await conn.execute(
        """
        DELETE FROM model_composition_members
        WHERE tenant_id = $1 AND composite_model_id = $2
        """,
        tenant_id,
        model_id,
    )
    grammar = derive_memory_grammar(proposition)
    if (
        not isinstance(proposition, dict)
        or grammar.claim_role != "situation"
    ):
        return

    member_ids = [
        uid
        for uid in _uuid_list_from_json(proposition.get("member_model_ids"))
        if uid != model_id
    ]
    if len(member_ids) < 2:
        return
    evidence_event_ids = _uuid_list_from_json(proposition.get("evidence_event_ids"))
    await conn.executemany(
        """
        INSERT INTO model_composition_members (
          composite_model_id, tenant_id, member_model_id,
          member_role, contribution, confidence, evidence_event_ids, source
        )
        VALUES ($1, $2, $3, 'member', NULL, 1.0, $4::uuid[], $5)
        ON CONFLICT (composite_model_id, member_model_id) DO UPDATE
          SET evidence_event_ids = EXCLUDED.evidence_event_ids,
              source = EXCLUDED.source
        """,
        [
            (model_id, tenant_id, member_id, evidence_event_ids, source)
            for member_id in member_ids
        ],
    )


async def _record_model_authority_provenance(
    conn: asyncpg.Connection,
    model: ModelRow,
) -> None:
    """Mirror model evidence arrays into the read-authority provenance graph."""
    source_refs: list[ObjectRef] = []
    seen_source_refs: set[tuple[str, UUID]] = set()
    for (
        source_kind,
        source_id,
        derivation_kind,
        source_column,
    ) in _model_authority_provenance_sources(model):
        source_key = (source_kind, source_id)
        if source_key not in seen_source_refs:
            seen_source_refs.add(source_key)
            source_refs.append(
                ObjectRef(
                    tenant_id=model.tenant_id,
                    object_kind=source_kind,
                    object_id=source_id,
                )
            )
        await record_provenance_edge(
            conn=conn,
            tenant_id=model.tenant_id,
            derived_kind="model",
            derived_id=model.id,
            source_kind=source_kind,
            source_id=source_id,
            derivation_kind=derivation_kind,
            metadata={"source_column": source_column},
        )
    await record_derived_access_labels(
        conn=conn,
        tenant_id=model.tenant_id,
        derived_kind="model",
        derived_id=model.id,
        source_refs=source_refs,
        source="model_provenance",
    )


def _model_authority_provenance_sources(
    model: ModelRow,
) -> tuple[tuple[str, UUID, str, str], ...]:
    rows: list[tuple[str, UUID, str, str]] = []
    rows.append(
        (
            "observation",
            model.born_from_event_id,
            "model_born_from_event",
            "born_from_event_id",
        )
    )
    for event_id in model.supporting_event_ids or []:
        rows.append(
            (
                "observation",
                event_id,
                "model_supporting_event",
                "supporting_event_ids",
            )
        )
    for source_model_id in model.supporting_model_ids or []:
        rows.append(
            (
                "model",
                source_model_id,
                "model_supporting_model",
                "supporting_model_ids",
            )
        )
    for source_model_id in model.contributing_models or []:
        rows.append(
            (
                "model",
                source_model_id,
                "model_contributing_model",
                "contributing_models",
            )
        )

    seen: set[tuple[str, UUID, str]] = set()
    deduped: list[tuple[str, UUID, str, str]] = []
    for source_kind, source_id, derivation_kind, source_column in rows:
        key = (source_kind, source_id, derivation_kind)
        if key in seen:
            continue
        seen.add(key)
        deduped.append((source_kind, source_id, derivation_kind, source_column))
    return tuple(deduped)


async def _bulk_record_model_authority_provenance(
    conn: asyncpg.Connection,
    models: Sequence[ModelRow],
) -> dict[str, int]:
    rows: list[tuple[UUID, UUID, str, UUID, str, str]] = []
    seen: set[tuple[UUID, UUID, str, UUID, str]] = set()
    for model in models:
        for (
            source_kind,
            source_id,
            derivation_kind,
            source_column,
        ) in _model_authority_provenance_sources(model):
            key = (
                model.tenant_id,
                model.id,
                source_kind,
                source_id,
                derivation_kind,
            )
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                (
                    model.tenant_id,
                    model.id,
                    source_kind,
                    source_id,
                    derivation_kind,
                    source_column,
                )
            )
    if not rows:
        return {
            "provenance_edges": 0,
            "access_label_calls": 0,
            "row_count": 0,
        }
    tenant_ids = [row[0] for row in rows]
    derived_ids = [row[1] for row in rows]
    source_kinds = [row[2] for row in rows]
    source_ids = [row[3] for row in rows]
    derivation_kinds = [row[4] for row in rows]
    source_columns = [row[5] for row in rows]
    await conn.execute(
        """
        INSERT INTO object_provenance_edges (
          tenant_id, derived_kind, derived_id,
          source_kind, source_id, derivation_kind, metadata
        )
        SELECT
          r.tenant_id,
          'model',
          r.derived_id,
          r.source_kind,
          r.source_id,
          r.derivation_kind,
          jsonb_build_object('source_column', r.source_column)
        FROM unnest(
          $1::uuid[],
          $2::uuid[],
          $3::text[],
          $4::uuid[],
          $5::text[],
          $6::text[]
        ) AS r(
          tenant_id,
          derived_id,
          source_kind,
          source_id,
          derivation_kind,
          source_column
        )
        ON CONFLICT (
          tenant_id, derived_kind, derived_id,
          source_kind, source_id, derivation_kind
        )
        DO UPDATE SET metadata = EXCLUDED.metadata
        """,
        tenant_ids,
        derived_ids,
        source_kinds,
        source_ids,
        derivation_kinds,
        source_columns,
    )
    for model in models:
        await record_derived_access_labels(
            conn=conn,
            tenant_id=model.tenant_id,
            derived_kind="model",
            derived_id=model.id,
            source_refs=[
                ObjectRef(
                    tenant_id=model.tenant_id,
                    object_kind=source_kind,
                    object_id=source_id,
                )
                for (
                    source_kind,
                    source_id,
                    _derivation_kind,
                    _source_column,
                ) in _model_authority_provenance_sources(model)
            ],
            source="model_provenance",
        )
    return {
        "provenance_edges": len(rows),
        "access_label_calls": len(models),
        "row_count": len(rows),
    }


async def _bulk_sync_model_scope_sidecars(
    conn: asyncpg.Connection,
    models: Sequence[ModelRow],
) -> dict[str, int]:
    actor_rows: list[tuple[UUID, UUID, UUID, str]] = []
    entity_rows: list[tuple[UUID, UUID, str, UUID, str]] = []
    seen_actor_rows: set[tuple[UUID, UUID]] = set()
    seen_entity_rows: set[tuple[UUID, str, UUID]] = set()
    for model in models:
        for actor_id in model.scope_actors or []:
            key = (model.id, actor_id)
            if key in seen_actor_rows:
                continue
            seen_actor_rows.add(key)
            actor_rows.append((model.id, model.tenant_id, actor_id, "model_scope"))
        for raw in model.scope_entities or []:
            if not isinstance(raw, dict):
                continue
            entity_type = raw.get("type")
            entity_id = raw.get("id")
            if not entity_type or entity_id is None:
                continue
            try:
                entity_uuid = entity_id if isinstance(entity_id, UUID) else UUID(str(entity_id))
            except (ValueError, TypeError):
                continue
            key = (model.id, str(entity_type), entity_uuid)
            if key in seen_entity_rows:
                continue
            seen_entity_rows.add(key)
            entity_rows.append((
                model.id,
                model.tenant_id,
                str(entity_type),
                entity_uuid,
                "model_scope",
            ))
    if actor_rows:
        await _bulk_write_records(
            conn,
            "model_scope_actors",
            records=actor_rows,
            columns=["model_id", "tenant_id", "actor_id", "source"],
        )
    if entity_rows:
        await _bulk_write_records(
            conn,
            "model_scope_entities",
            records=entity_rows,
            columns=["model_id", "tenant_id", "entity_type", "entity_id", "source"],
        )
    return {
        "actor_rows": len(actor_rows),
        "entity_rows": len(entity_rows),
        "row_count": len(actor_rows) + len(entity_rows),
    }


async def _bulk_sync_model_composition_members(
    conn: asyncpg.Connection,
    models: Sequence[ModelRow],
) -> int:
    rows: list[tuple[UUID, UUID, UUID, list[UUID], str]] = []
    seen_rows: set[tuple[UUID, UUID]] = set()
    for model in models:
        proposition = model.proposition if isinstance(model.proposition, dict) else {}
        grammar = derive_memory_grammar(proposition)
        if grammar.claim_role != "situation":
            continue
        member_ids = [
            member_id
            for member_id in _uuid_list_from_json(proposition.get("member_model_ids"))
            if member_id != model.id
        ]
        if len(member_ids) < 2:
            continue
        evidence_event_ids = _uuid_list_from_json(proposition.get("evidence_event_ids"))
        for member_id in member_ids:
            key = (model.id, member_id)
            if key in seen_rows:
                continue
            seen_rows.add(key)
            rows.append((
                model.id,
                model.tenant_id,
                member_id,
                evidence_event_ids,
                "model_proposition",
            ))
    if rows:
        await _bulk_write_records(
            conn,
            "model_composition_members",
            records=rows,
            columns=[
                "composite_model_id",
                "tenant_id",
                "member_model_id",
                "evidence_event_ids",
                "source",
            ],
            casts={"evidence_event_ids": "uuid[]"},
        )
    return len(rows)


async def _bulk_insert_model_relations(
    conn: asyncpg.Connection,
    models: Sequence[ModelRow],
) -> int:
    rows: list[tuple[Any, ...]] = []
    seen_edges: set[tuple[UUID, UUID, UUID, str]] = set()
    for model in models:
        for source in model.supporting_model_ids or []:
            if source == model.id:
                continue
            key = (model.tenant_id, source, model.id, "supports")
            if key in seen_edges:
                continue
            seen_edges.add(key)
            rows.append(_bulk_edge_row(
                tenant_id=model.tenant_id,
                source=source,
                target=model.id,
                kind="supports",
                created_by_event_id=model.born_from_event_id,
                evidence_model_id=source,
            ))
        for source in model.contributing_models or []:
            if source == model.id:
                continue
            key = (
                model.tenant_id,
                source,
                model.id,
                "contributes_to_resolution",
            )
            if key in seen_edges:
                continue
            seen_edges.add(key)
            rows.append(_bulk_edge_row(
                tenant_id=model.tenant_id,
                source=source,
                target=model.id,
                kind="contributes_to_resolution",
                created_by_event_id=model.born_from_event_id,
                evidence_model_id=source,
            ))
    if not rows:
        return 0
    await _bulk_write_records(
        conn,
        "model_edges",
        records=rows,
        columns=[
            "id",
            "tenant_id",
            "source_model_id",
            "target_model_id",
            "edge_kind",
            "weight",
            "metadata",
            "status",
            "detected_by",
            "created_by_event_id",
            "confidence",
            "evidence_event_ids",
            "evidence_model_ids",
            "explanation",
            "review_status",
            "last_confirmed_at",
            "confirmed_count",
        ],
        casts={
            "metadata": "jsonb",
            "evidence_event_ids": "uuid[]",
            "evidence_model_ids": "uuid[]",
        },
    )
    return len(rows)


def _bulk_edge_row(
    *,
    tenant_id: UUID,
    source: UUID,
    target: UUID,
    kind: str,
    created_by_event_id: UUID | None,
    evidence_model_id: UUID,
) -> tuple[Any, ...]:
    confirmed_at = datetime.now(timezone.utc)
    return (
        uuid7(),
        tenant_id,
        source,
        target,
        kind,
        None,
        _jsonb({}),
        "active",
        "llm_explicit",
        created_by_event_id,
        1.0,
        [created_by_event_id] if created_by_event_id is not None else [],
        [evidence_model_id],
        None,
        "accepted",
        confirmed_at,
        1,
    )


async def _bulk_emit_model_state_changes(
    conn: asyncpg.Connection,
    models: Sequence[ModelRow],
) -> int:
    rows: list[tuple[Any, ...]] = []
    events: list[NewObservationEvent] = []
    occurred_at = datetime.now(timezone.utc)
    for model in models:
        metadata = {
            "proposition_kind": model.proposition_kind,
            "claim_role": model.claim_role,
            "abstraction_level": model.abstraction_level,
            "confidence": model.confidence,
        }
        content = {
            "entity_id": str(model.id),
            "state_change_kind": "insert_model",
            "entity_kind": "model",
            "metadata": metadata,
        }
        obs_id = uuid7()
        rows.append((
            obs_id,
            model.tenant_id,
            occurred_at,
            "state_change",
            STATE_CHANGE_CHANNEL,
            _jsonb(content),
            render_state_change_text("insert_model", model.id, "model", metadata),
            False,
            STATE_CHANGE_TRUST_TIER,
            model.born_from_event_id,
            _jsonb([]),
        ))
        events.append(NewObservationEvent(
            id=obs_id,
            kind="state_change",
            tenant_id=model.tenant_id,
            source_channel=STATE_CHANGE_CHANNEL,
        ))
    if rows:
        await _bulk_write_records(
            conn,
            "observations",
            records=rows,
            columns=[
                "id",
                "tenant_id",
                "occurred_at",
                "kind",
                "source_channel",
                "content",
                "content_text",
                "embedding_pending",
                "trust_tier",
                "cause_id",
                "entities_mentioned",
            ],
            casts={
                "content": "jsonb",
                "entities_mentioned": "jsonb",
            },
        )
    for event in events:
        schedule_notify(event)
    return len(rows)


async def _bulk_emit_model_audits(
    conn: asyncpg.Connection,
    models: Sequence[ModelRow],
) -> int:
    from services.reasoning.think.audit import (
        CAUSE_CREATE,
        model_state_snapshot,
    )

    rows: list[tuple[Any, ...]] = []
    for model in models:
        snapshot = model_state_snapshot(model)
        rows.append((
            model.id,
            model.tenant_id,
            model.born_from_event_id,
            CAUSE_CREATE,
            _jsonb(snapshot),
            sorted(snapshot.keys()),
            [],
        ))
    if rows:
        await _bulk_write_records(
            conn,
            "audit_events",
            records=rows,
            columns=[
                "model_id",
                "tenant_id",
                "cause_id",
                "cause_type",
                "new_state",
                "changed_fields",
                "source_model_ids",
            ],
            casts={
                "new_state": "jsonb",
                "changed_fields": "text[]",
                "source_model_ids": "uuid[]",
            },
        )
    return len(rows)


async def _read_array_part(
    conn: asyncpg.Connection,
    model_id: UUID,
    kind: str,
) -> list[UUID]:
    """Read the OTHER endpoint of edges of `kind` involving model_id.

    Direction depends on kind:
      - `supports`: incoming. Other endpoint = source (the supporter).
      - `instance_of`: outgoing. Other endpoint = target (the pattern).

    Used by _sync_array_columns to retain the un-touched dimension
    when only one of (supports, instance_of) is being updated.
    """
    if kind == "supports":
        rows = await conn.fetch(
            """
            SELECT source_model_id AS other FROM model_edges
            WHERE target_model_id = $1
              AND edge_kind = $2
              AND status = 'active'
            """,
            model_id,
            kind,
        )
    else:
        # `instance_of` — outgoing
        rows = await conn.fetch(
            """
            SELECT target_model_id AS other FROM model_edges
            WHERE source_model_id = $1
              AND edge_kind = $2
              AND status = 'active'
            """,
            model_id,
            kind,
        )
    return [r["other"] for r in rows]


_AUTO_ACCEPT_MIN_CONFIDENCE = 0.55


async def _maybe_auto_accept(
    hydrated: "ModelRow", conn: asyncpg.Connection
) -> None:
    """Auto-act on `create_commitment` recommendations whose payload is
    structurally complete. The human-approval step is ceremonial when
    Think has already named the owner + contributing goal from the
    signal, so we run the accept handler server-side and let the
    Commitment land in the ledger without a CEO click.

    All failures are swallowed; the recommendation stays active and the
    user can act on it manually if anything goes wrong.
    """
    if hydrated.target_actor_id is None:
        return
    proposition = hydrated.proposition
    if not isinstance(proposition, dict):
        return
    target_ref = proposition.get("target_act_ref") or {}
    proposed_change = proposition.get("proposed_change") or {}
    if target_ref.get("type") != "commitment":
        return
    if proposed_change.get("operation") != "create":
        return
    payload = proposed_change.get("payload") or {}
    if not isinstance(payload, dict):
        return
    if not payload.get("title") or not payload.get("owner_id"):
        return
    if (hydrated.confidence or 0.0) < _AUTO_ACCEPT_MIN_CONFIDENCE:
        return

    try:
        from services.product.recommendations.handlers import act_on_recommendation

        await act_on_recommendation(
            recommendation_id=hydrated.id,
            actor_id=hydrated.target_actor_id,
            tenant_id=hydrated.tenant_id,
            notes="auto-accepted: low-risk create-commitment",
            conn=conn,
        )
    except Exception as exc:
        # Leave the recommendation active on any failure — Think log
        # surfaces the LLM payload, and the user can dismiss/accept
        # manually from Today.
        _log.warning(
            "models.recommendation_auto_accept_failed",
            recommendation_id=str(hydrated.id),
            tenant_id=str(hydrated.tenant_id),
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return


def _hydrate_row(record: asyncpg.Record) -> ModelRow:
    """asyncpg Record → ModelRow, tolerating JSONB str/bytes codecs
    and pgvector's numpy array return type."""
    return hydrate_model_row(record, wrap_errors=True)


# ---------------------------------------------------------------------
# ModelsRepo
# ---------------------------------------------------------------------


class ModelsRepo:
    def __init__(
        self,
        pool: asyncpg.Pool | None = None,
        *,
        embedder: OllamaClient | None = None,
        run_topology_on_insert: bool = True,
        bulk_timing_sink: BulkTimingSink | None = None,
    ) -> None:
        # Pool is optional when every call site supplies its own `conn`
        # (e.g. promote_pattern_candidate inside Think T4 pattern_review).
        # Methods that need a pool when conn is None raise a clear
        # error via `_require_pool()`.
        self._pool = pool
        self._embedder = embedder
        self._run_topology_on_insert = run_topology_on_insert
        self._bulk_timing_sink = bulk_timing_sink

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise ModelsRepoError(
                "ModelsRepo was constructed without a pool; "
                "callers in conn-only mode must pass conn= on every call"
            )
        return self._pool

    def _record_bulk_timing(self, event: dict[str, Any]) -> None:
        if self._bulk_timing_sink is None:
            return
        try:
            self._bulk_timing_sink(event)
        except Exception:
            _log.warning(
                "models.bulk_timing_sink_failed",
                phase=event.get("phase"),
                exc_info=True,
            )

    async def _time_bulk_phase(
        self,
        phase: str,
        *,
        model_count: int,
        op: Callable[[], Awaitable[Any]],
        stratum_index: int | None = None,
        metrics_from_result: Callable[[Any], dict[str, Any]] | None = None,
    ) -> Any:
        started = time.perf_counter()
        result: Any = None
        status = "ok"
        try:
            result = await op()
            return result
        except Exception:
            status = "error"
            raise
        finally:
            event: dict[str, Any] = {
                "phase": phase,
                "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
                "model_count": int(model_count),
                "status": status,
            }
            if stratum_index is not None:
                event["stratum_index"] = int(stratum_index)
            if status == "ok" and metrics_from_result is not None:
                try:
                    event.update(metrics_from_result(result) or {})
                except Exception:
                    _log.warning(
                        "models.bulk_timing_metrics_failed",
                        phase=phase,
                        exc_info=True,
                    )
            self._record_bulk_timing(event)

    # =================================================================
    # insert — the 9-step pipeline
    # =================================================================
    async def insert(
        self,
        proposed: ModelCreate,
        *,
        conn: asyncpg.Connection | None = None,
        apply_confidence_calibration: bool = True,
    ) -> ModelRow:
        """
        Insert a Model through the full §2 pipeline.

        Think validation already calibrates claim-op inserts so it can
        enforce falsifier adequacy against the final confidence. That
        caller passes `apply_confidence_calibration=False` to avoid
        discounting the same assertion twice; direct repo callers keep
        the default and get the central calibration step here.

        Raises:
          - FalsifierInadequateError (confidence > 0.7 without adequate falsifier)
          - ValidationError (proposition schema / scope actor missing /
            embedding shape wrong)
        """
        constructed = construct_model(proposed)
        return await self._insert_constructed(
            constructed,
            conn=conn,
            apply_confidence_calibration=apply_confidence_calibration,
        )

    async def _insert_constructed(
        self,
        constructed: ConstructedModel,
        *,
        conn: asyncpg.Connection | None,
        apply_confidence_calibration: bool,
    ) -> ModelRow:
        proposed = constructed.proposed
        # -- 1. Falsifier adequacy if confidence > 0.7 -----------------
        if proposed.confidence > _FALSIFIER_REQUIRED_ABOVE:
            ok, reason = is_adequate_falsifier(proposed.falsifier)
            if not ok:
                raise FalsifierInadequateError(
                    reason or "falsifier inadequate",
                    falsifier=proposed.falsifier,
                    confidence=proposed.confidence,
                )

        # -- 2. Use constructed canonical Model draft -------------------
        proposed = constructed.proposed
        prop_kind: PropositionKind = constructed.core.proposition["kind"]  # type: ignore[assignment]

        # confidence_at_assertion is the pre-calibration number. We
        # preserve it immutably (clipped into bounds to satisfy the
        # CHECK) so calibration learning has the raw "what Think
        # originally said" value even after Wave 4-C's real offset
        # lookup adjusts `confidence` on the way in.
        conf_at_assertion = _clip_confidence(proposed.confidence_at_assertion)

        # -- 3/4/5/6/7/8. Calibration, clip, INSERT, emit state_change
        # all happen in the transaction so calibration's DB read sees
        # any offsets written by a concurrent updater before we commit.
        if conn is not None:
            return await self._insert_core(
                conn,
                proposed,
                prop_kind,
                conf_at_assertion,
                apply_confidence_calibration=apply_confidence_calibration,
            )
        async with self._require_pool().acquire() as owned:
            async with owned.transaction():
                return await self._insert_core(
                    owned,
                    proposed,
                    prop_kind,
                    conf_at_assertion,
                    apply_confidence_calibration=apply_confidence_calibration,
                )

    async def insert_many(
        self,
        proposed: Sequence[ModelCreate],
        *,
        conn: asyncpg.Connection | None = None,
        apply_confidence_calibration: bool = True,
    ) -> list[ModelRow]:
        """Insert a dependency-safe batch of Models.

        The batch planner pre-constructs every Model, assigns stable ids,
        rejects intra-batch cycles before any write, and orders inserts so
        referenced Models exist before dependents. Each row still goes
        through the canonical ``insert`` pipeline, preserving sidecars,
        typed edges, audit events, state_change observations, topology,
        and recommendation behavior.

        Returned rows follow the caller's original input order.
        """
        if not proposed:
            return []

        plan = plan_model_batch(list(proposed))

        async def _run(c: asyncpg.Connection) -> list[ModelRow]:
            if not self._batch_requires_serial_insert(plan):
                return await self._insert_many_bulk(
                    plan,
                    conn=c,
                    apply_confidence_calibration=apply_confidence_calibration,
                )

            rows_by_id: dict[UUID, ModelRow] = {}
            for stratum in plan.strata:
                for planned in stratum:
                    row = await self._insert_constructed(
                        planned.constructed,
                        conn=c,
                        apply_confidence_calibration=apply_confidence_calibration,
                    )
                    rows_by_id[row.id] = row
            return [
                rows_by_id[planned.id]
                for planned in sorted(plan.models, key=lambda item: item.index)
            ]

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            async with owned.transaction():
                return await _run(owned)

    def _batch_requires_serial_insert(self, plan: ModelBatchPlan) -> bool:
        """Return True when batch rows need single-Model side effects."""
        for planned in plan.models:
            if planned.constructed.core.grammar.claim_role == "recommendation":
                return True
        return False

    async def _insert_many_bulk(
        self,
        plan: ModelBatchPlan,
        *,
        conn: asyncpg.Connection,
        apply_confidence_calibration: bool,
    ) -> list[ModelRow]:
        """Set-oriented insert path for ordinary Model batches.

        This keeps the insert-many contract honest while removing the
        per-row SQL chatter that made large Model batches unusably slow.
        """
        await _ensure_vector_codec(conn)
        await self._time_bulk_phase(
            "validate_batch_scope_actors",
            model_count=len(plan.models),
            op=lambda: self._validate_batch_scope_actors(conn, plan),
        )

        rows_by_id: dict[UUID, ModelRow] = {}
        for stratum_index, stratum in enumerate(plan.strata):
            prepared = await self._time_bulk_phase(
                "prepare_bulk_insert_stratum",
                stratum_index=stratum_index,
                model_count=len(stratum),
                op=lambda stratum=stratum: self._prepare_bulk_insert_stratum(
                    conn,
                    stratum,
                    apply_confidence_calibration=apply_confidence_calibration,
                ),
                metrics_from_result=lambda result: {
                    "row_count": len(result or ()),
                },
            )
            if not prepared:
                continue
            model_params = [item["params"] for item in prepared]
            await self._time_bulk_phase(
                "models_insert",
                stratum_index=stratum_index,
                model_count=len(prepared),
                op=lambda model_params=model_params: _bulk_write_records(
                    conn,
                    "models",
                    records=model_params,
                    columns=_BULK_MODEL_COPY_COLUMNS,
                    casts=_BULK_MODEL_VALUE_CASTS,
                ),
                metrics_from_result=lambda _result, count=len(prepared): {
                    "row_count": count,
                },
            )
            hydrated = [item["row"] for item in prepared]
            rows_by_id.update((row.id, row) for row in hydrated)

            await self._time_bulk_phase(
                "semantic_terms_upsert",
                stratum_index=stratum_index,
                model_count=len(hydrated),
                op=lambda hydrated=hydrated: _bulk_upsert_model_semantic_terms(
                    conn,
                    hydrated,
                ),
                metrics_from_result=lambda result: {"row_count": int(result or 0)},
            )
            await self._time_bulk_phase(
                "scope_sidecars_sync",
                stratum_index=stratum_index,
                model_count=len(hydrated),
                op=lambda hydrated=hydrated: _bulk_sync_model_scope_sidecars(
                    conn,
                    hydrated,
                ),
                metrics_from_result=lambda result: dict(result or {}),
            )
            await self._time_bulk_phase(
                "composition_members_sync",
                stratum_index=stratum_index,
                model_count=len(hydrated),
                op=lambda hydrated=hydrated: _bulk_sync_model_composition_members(
                    conn,
                    hydrated,
                ),
                metrics_from_result=lambda result: {"row_count": int(result or 0)},
            )
            await self._time_bulk_phase(
                "relations_insert",
                stratum_index=stratum_index,
                model_count=len(hydrated),
                op=lambda hydrated=hydrated: _bulk_insert_model_relations(
                    conn,
                    hydrated,
                ),
                metrics_from_result=lambda result: {"row_count": int(result or 0)},
            )
            await self._time_bulk_phase(
                "authority_provenance_record",
                stratum_index=stratum_index,
                model_count=len(hydrated),
                op=lambda hydrated=hydrated: _bulk_record_model_authority_provenance(
                    conn,
                    hydrated,
                ),
                metrics_from_result=lambda result: dict(result or {}),
            )
            await self._time_bulk_phase(
                "default_affordance_profiles_upsert",
                stratum_index=stratum_index,
                model_count=len(hydrated),
                op=lambda hydrated=hydrated: _bulk_upsert_default_affordance_profiles(
                    conn,
                    hydrated,
                ),
                metrics_from_result=lambda result: {"row_count": int(result or 0)},
            )
            await self._time_bulk_phase(
                "state_changes_emit",
                stratum_index=stratum_index,
                model_count=len(hydrated),
                op=lambda hydrated=hydrated: _bulk_emit_model_state_changes(
                    conn,
                    hydrated,
                ),
                metrics_from_result=lambda result: {"row_count": int(result or 0)},
            )
            await self._time_bulk_phase(
                "audits_emit",
                stratum_index=stratum_index,
                model_count=len(hydrated),
                op=lambda hydrated=hydrated: _bulk_emit_model_audits(
                    conn,
                    hydrated,
                ),
                metrics_from_result=lambda result: {"row_count": int(result or 0)},
            )
            await self._time_bulk_phase(
                "model_events_emit",
                stratum_index=stratum_index,
                model_count=len(hydrated),
                op=lambda hydrated=hydrated: emit_model_events(
                    conn,
                    models=hydrated,
                    event_type=MODEL_EVENT_CREATED,
                    changed_fields=model_semantic_snapshot(hydrated[0]).keys(),
                ),
                metrics_from_result=lambda _result, count=len(hydrated): {
                    "row_count": count,
                },
            )

            if self._run_topology_on_insert:
                async def _generate_topology() -> dict[str, int]:
                    generated = 0
                    failed = 0
                    for row in hydrated:
                        try:
                            async with conn.transaction():
                                await _TOPOLOGY.generate_for_model(conn, model=row)
                            generated += 1
                        except Exception:
                            failed += 1
                    return {
                        "attempted": len(hydrated),
                        "generated": generated,
                        "failed": failed,
                    }

                await self._time_bulk_phase(
                    "topology_generate",
                    stratum_index=stratum_index,
                    model_count=len(hydrated),
                    op=_generate_topology,
                    metrics_from_result=lambda result: dict(result or {}),
                )

        return [
            rows_by_id[planned.id]
            for planned in sorted(plan.models, key=lambda item: item.index)
        ]

    async def _validate_batch_scope_actors(
        self,
        conn: asyncpg.Connection,
        plan: ModelBatchPlan,
    ) -> None:
        actors_by_tenant: dict[UUID, set[UUID]] = {}
        for planned in plan.models:
            proposed = planned.constructed.proposed
            if proposed.scope_actors:
                actors_by_tenant.setdefault(proposed.tenant_id, set()).update(
                    proposed.scope_actors
                )
        for tenant_id, actor_ids in actors_by_tenant.items():
            existing = await conn.fetch(
                """
                SELECT id FROM actors
                WHERE tenant_id = $1 AND id = ANY($2::uuid[])
                """,
                tenant_id,
                list(actor_ids),
            )
            existing_ids = {row["id"] for row in existing}
            missing = sorted(actor_ids - existing_ids, key=str)
            if missing:
                raise ValidationError(
                    f"scope_actors reference {len(missing)} non-existent actor(s)",
                    missing=[str(actor_id) for actor_id in missing],
                )

    async def _prepare_bulk_insert_stratum(
        self,
        conn: asyncpg.Connection,
        stratum: Sequence[PlannedModel],
        *,
        apply_confidence_calibration: bool,
    ) -> list[dict[str, Any]]:
        prepared: list[dict[str, Any]] = []
        for planned in stratum:
            constructed = planned.constructed
            proposed = constructed.proposed

            if proposed.confidence > _FALSIFIER_REQUIRED_ABOVE:
                ok, reason = is_adequate_falsifier(proposed.falsifier)
                if not ok:
                    raise FalsifierInadequateError(
                        reason or "falsifier inadequate",
                        falsifier=proposed.falsifier,
                        confidence=proposed.confidence,
                    )

            prop_kind: PropositionKind = constructed.core.proposition["kind"]  # type: ignore[assignment]
            if proposed.supporting_model_ids:
                await _check_no_support_cycle(
                    conn,
                    new_model_id=planned.id,
                    new_supports=list(proposed.supporting_model_ids),
                )

            conf_at_assertion = _clip_confidence(proposed.confidence_at_assertion)
            if apply_confidence_calibration:
                calibrated_conf = await apply_calibration(
                    proposed.confidence,
                    proposed.scope_actors,
                    prop_kind,
                    tenant_id=proposed.tenant_id,
                    conn=conn,
                )
            else:
                calibrated_conf = proposed.confidence
            final_conf = _clip_confidence(calibrated_conf)

            embedding = await self._resolve_embedding(proposed)
            if len(embedding) != EMBEDDING_DIM:
                raise ValidationError(
                    f"embedding dim {len(embedding)} != {EMBEDDING_DIM}",
                    got=len(embedding),
                    expected=EMBEDDING_DIM,
                )
            topo_anchor: list[float] | None = None
            try:
                topo_anchor = content_anchor(embedding)
            except Exception:
                topo_anchor = None

            domain_tags = list(proposed.domain_tags or constructed.core.grammar.domain_tags)
            semantic_terms = list(proposed.semantic_terms or ())
            created_at = datetime.now(timezone.utc)
            row = ModelRow(
                id=planned.id,
                tenant_id=proposed.tenant_id,
                born_from_event_id=proposed.born_from_event_id,
                proposition=proposed.proposition,
                natural=proposed.natural,
                embedding=embedding,
                scope_actors=list(proposed.scope_actors),
                scope_entities=list(proposed.scope_entities),
                scope_temporal=dict(proposed.scope_temporal),
                confidence=final_conf,
                activation=1.0,
                falsifier=proposed.falsifier,
                signal_readings=list(proposed.signal_readings),
                reading_contestable=proposed.reading_contestable,
                supporting_event_ids=list(proposed.supporting_event_ids),
                supporting_model_ids=list(proposed.supporting_model_ids),
                evidential_weight=proposed.evidential_weight,
                status="active",
                archived_at=None,
                archive_reason=None,
                created_at=created_at,
                last_retrieved_at=None,
                retrieval_count=0,
                evaluate_at=proposed.evaluate_at,
                resolution_criteria=proposed.resolution_criteria,
                contributing_models=list(proposed.contributing_models),
                visible_to_subjects=proposed.visible_to_subjects,
                proposition_kind=str(prop_kind),
                claim_role=constructed.core.grammar.claim_role,
                abstraction_level=constructed.core.grammar.abstraction_level,
                time_mode=constructed.core.grammar.time_mode,
                modality=constructed.core.grammar.modality,
                polarity=constructed.core.grammar.polarity,
                domain_tags=domain_tags,
                semantic_terms=semantic_terms,
                memory_grammar_version="v1",
                confirmed_count=0,
                contested_count=0,
                last_confirmed_at=None,
                confidence_at_assertion=conf_at_assertion,
                resolved_at=None,
                resolution_outcome=None,
                activation_coefficient=proposed.activation_coefficient,
                target_actor_id=None,
                caused_act_change_id=None,
            )
            prepared.append({
                "id": planned.id,
                "row": row,
                "params": (
                    planned.id,
                    proposed.tenant_id,
                    proposed.born_from_event_id,
                    _jsonb(proposed.proposition),
                    proposed.natural,
                    embedding,
                    list(proposed.scope_actors),
                    _jsonb(proposed.scope_entities),
                    _jsonb(proposed.scope_temporal),
                    final_conf,
                    1.0,
                    _jsonb(proposed.falsifier) if proposed.falsifier is not None else None,
                    _jsonb(proposed.signal_readings),
                    proposed.reading_contestable,
                    list(proposed.supporting_event_ids),
                    list(proposed.supporting_model_ids),
                    proposed.evidential_weight,
                    "active",
                    created_at,
                    proposed.evaluate_at,
                    _jsonb(proposed.resolution_criteria) if proposed.resolution_criteria is not None else None,
                    list(proposed.contributing_models),
                    proposed.visible_to_subjects,
                    conf_at_assertion,
                    proposed.activation_coefficient,
                    domain_tags,
                    topo_anchor,
                    datetime.now(timezone.utc) if topo_anchor is not None else None,
                ),
            })
        return prepared

    async def _insert_core(
        self,
        conn: asyncpg.Connection,
        proposed: ModelCreate,
        prop_kind: PropositionKind,
        conf_at_assertion: float,
        *,
        apply_confidence_calibration: bool,
    ) -> ModelRow:
        await _ensure_vector_codec(conn)
        prepared = await self._prepare_insert_payload(
            conn,
            proposed,
            prop_kind,
            apply_confidence_calibration=apply_confidence_calibration,
        )
        hydrated = await self._insert_model_row(
            conn,
            proposed=proposed,
            model_id=prepared["model_id"],
            final_conf=prepared["final_conf"],
            conf_at_assertion=conf_at_assertion,
            embedding=prepared["embedding"],
            domain_tags=prepared["domain_tags"],
            semantic_terms=prepared["semantic_terms"],
        )
        await self._sync_insert_sidecars_and_relations(conn, proposed, hydrated)
        await _bulk_upsert_default_affordance_profiles(conn, [hydrated])
        await self._apply_insert_topology_effects(
            conn,
            hydrated=hydrated,
            embedding=prepared["embedding"],
        )
        await self._emit_insert_observability(conn, hydrated)
        await emit_model_event(
            conn,
            model=hydrated,
            event_type=MODEL_EVENT_CREATED,
            changed_fields=model_semantic_snapshot(hydrated).keys(),
            source_event_id=hydrated.born_from_event_id,
        )
        await self._publish_recommendation_insert(conn, hydrated)
        return hydrated

    async def _prepare_insert_payload(
        self,
        conn: asyncpg.Connection,
        proposed: ModelCreate,
        prop_kind: PropositionKind,
        *,
        apply_confidence_calibration: bool,
    ) -> dict[str, Any]:
        model_id = proposed.id or uuid7()
        await _check_no_support_cycle(
            conn,
            new_model_id=model_id,
            new_supports=list(proposed.supporting_model_ids or []),
        )
        grammar = derive_memory_grammar(
            proposed.proposition,
            natural=proposed.natural,
            scope_entities=proposed.scope_entities,
        )
        if grammar.claim_role == "recommendation":
            await validate_recommendation(
                proposed.proposition,
                tenant_id=proposed.tenant_id,
                conn=conn,
            )
        if apply_confidence_calibration:
            calibrated_conf = await apply_calibration(
                proposed.confidence,
                proposed.scope_actors,
                prop_kind,
                tenant_id=proposed.tenant_id,
                conn=conn,
            )
        else:
            calibrated_conf = proposed.confidence
        await self._validate_scope_actors(conn, proposed)
        embedding = await self._resolve_embedding(proposed)
        if len(embedding) != EMBEDDING_DIM:
            raise ValidationError(
                f"embedding dim {len(embedding)} != {EMBEDDING_DIM}",
                got=len(embedding),
                expected=EMBEDDING_DIM,
            )
        return {
            "model_id": model_id,
            "final_conf": _clip_confidence(calibrated_conf),
            "embedding": embedding,
            "domain_tags": list(proposed.domain_tags or grammar.domain_tags),
            "semantic_terms": list(proposed.semantic_terms or ()),
        }

    async def _validate_scope_actors(
        self,
        conn: asyncpg.Connection,
        proposed: ModelCreate,
    ) -> None:
        if not proposed.scope_actors:
            return
        existing = await conn.fetch(
            """
            SELECT id FROM actors
            WHERE tenant_id = $1 AND id = ANY($2::uuid[])
            """,
            proposed.tenant_id,
            list(proposed.scope_actors),
        )
        existing_ids = {r["id"] for r in existing}
        missing = [a for a in proposed.scope_actors if a not in existing_ids]
        if missing:
            raise ValidationError(
                f"scope_actors reference {len(missing)} non-existent actor(s)",
                missing=[str(m) for m in missing],
            )

    async def _insert_model_row(
        self,
        conn: asyncpg.Connection,
        *,
        proposed: ModelCreate,
        model_id: UUID,
        final_conf: float,
        conf_at_assertion: float,
        embedding: list[float],
        domain_tags: list[str],
        semantic_terms: list[str],
    ) -> ModelRow:
        row = await conn.fetchrow(
            f"""
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_actors, scope_entities, scope_temporal,
                confidence, activation, falsifier,
                signal_readings, reading_contestable,
                supporting_event_ids, supporting_model_ids, evidential_weight,
                status, evaluate_at, resolution_criteria,
                contributing_models, visible_to_subjects,
                confidence_at_assertion, activation_coefficient,
                domain_tags
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, $5, $6,
                $7::uuid[], $8::jsonb, $9::jsonb,
                $10, $11, $12::jsonb,
                $13::jsonb, $14,
                $15::uuid[], $16::uuid[], $17,
                $18, $19, $20::jsonb,
                $21::uuid[], $22,
                $23, $24,
                $25::text[]
            )
            RETURNING {_SELECT_COLS_SQL}
            """,
            model_id,
            proposed.tenant_id,
            proposed.born_from_event_id,
            _jsonb(proposed.proposition),
            proposed.natural,
            embedding,
            list(proposed.scope_actors),
            _jsonb(proposed.scope_entities),
            _jsonb(proposed.scope_temporal),
            final_conf,
            1.0,
            _jsonb(proposed.falsifier) if proposed.falsifier is not None else None,
            _jsonb(proposed.signal_readings),
            proposed.reading_contestable,
            list(proposed.supporting_event_ids),
            list(proposed.supporting_model_ids),
            proposed.evidential_weight,
            "active",
            proposed.evaluate_at,
            _jsonb(proposed.resolution_criteria) if proposed.resolution_criteria is not None else None,
            list(proposed.contributing_models),
            proposed.visible_to_subjects,
            conf_at_assertion,
            proposed.activation_coefficient,
            domain_tags,
        )
        assert row is not None
        hydrated = _hydrate_row(row)
        await _upsert_model_semantic_terms(
            conn,
            tenant_id=hydrated.tenant_id,
            model_id=hydrated.id,
            semantic_terms=semantic_terms,
        )
        return hydrated.model_copy(update={"semantic_terms": semantic_terms})

    async def _sync_insert_sidecars_and_relations(
        self,
        conn: asyncpg.Connection,
        proposed: ModelCreate,
        hydrated: ModelRow,
    ) -> None:
        await _sync_model_scope_sidecars(
            conn,
            model_id=hydrated.id,
            tenant_id=hydrated.tenant_id,
            scope_actors=hydrated.scope_actors,
            scope_entities=hydrated.scope_entities,
        )
        await _sync_model_composition_members(
            conn,
            model_id=hydrated.id,
            tenant_id=hydrated.tenant_id,
            proposition=hydrated.proposition,
        )
        if (
            list(proposed.supporting_model_ids)
            or list(proposed.contributing_models)
        ):
            await _set_model_relations(
                conn,
                model_id=hydrated.id,
                tenant_id=hydrated.tenant_id,
                detected_by="llm_explicit",
                supports=list(proposed.supporting_model_ids),
                contributes_to=list(proposed.contributing_models),
                created_by_event_id=hydrated.born_from_event_id,
                update_arrays=False,
            )
        await _record_model_authority_provenance(conn, hydrated)

    async def _apply_insert_topology_effects(
        self,
        conn: asyncpg.Connection,
        *,
        hydrated: ModelRow,
        embedding: list[float],
    ) -> None:
        if embedding:
            try:
                topo_anchor = content_anchor(embedding)
                await conn.execute(
                    """
                    UPDATE models
                       SET topo_embedding = $1::vector,
                           topo_updated_at = now()
                     WHERE id = $2 AND tenant_id = $3
                    """,
                    topo_anchor,
                    hydrated.id,
                    hydrated.tenant_id,
                )
            except Exception:
                # Best-effort: topo_embedding stays NULL and the
                # sweeper / a backfill will fill it later.
                pass

        if self._run_topology_on_insert:
            try:
                async with conn.transaction():
                    await _TOPOLOGY.generate_for_model(conn, model=hydrated)
            except Exception:
                # Topology is best-effort: the Model insert is canonical,
                # while candidate discovery can be retried by later sweeps.
                # The nested transaction keeps topology failures from
                # poisoning the surrounding model-insert transaction.
                pass

    async def _emit_insert_observability(
        self,
        conn: asyncpg.Connection,
        hydrated: ModelRow,
    ) -> None:
        await emit_state_change(
            conn,
            kind="insert_model",
            entity_id=hydrated.id,
            tenant_id=hydrated.tenant_id,
            cause_event_id=hydrated.born_from_event_id,
            entity_kind="model",
            metadata={
                "proposition_kind": hydrated.proposition_kind,
                "claim_role": hydrated.claim_role,
                "abstraction_level": hydrated.abstraction_level,
                "confidence": hydrated.confidence,
            },
        )

        from services.reasoning.think.audit import (  # noqa: WPS433 — see module top
            CAUSE_CREATE,
            emit_audit_event,
            model_state_snapshot,
        )
        snapshot = model_state_snapshot(hydrated)
        await emit_audit_event(
            conn,
            model_id=hydrated.id,
            tenant_id=hydrated.tenant_id,
            cause_type=CAUSE_CREATE,
            new_state=snapshot,
            previous_state=None,
            cause_id=hydrated.born_from_event_id,
            changed_fields=sorted(snapshot.keys()),
            detect_re_assert=False,  # creates have no prior to re-assert
        )

    async def _publish_recommendation_insert(
        self,
        conn: asyncpg.Connection,
        hydrated: ModelRow,
    ) -> None:
        if hydrated.claim_role == "recommendation" and hydrated.target_actor_id:
            from lib.shared.events import publish as publish_event

            await publish_event(
                "recommendation.event",
                tenant_id=hydrated.tenant_id,
                actor_id=hydrated.target_actor_id,
                event="created",
                recommendation_id=hydrated.id,
                summary={
                    "natural": hydrated.natural,
                    "confidence": hydrated.confidence,
                    "expected_impact": (
                        hydrated.proposition.get("expected_impact")
                        if isinstance(hydrated.proposition, dict) else None
                    ),
                },
            )
            await _maybe_auto_accept(hydrated, conn)

    async def _resolve_embedding(self, proposed: ModelCreate) -> list[float]:
        if proposed.embedding and len(proposed.embedding) == EMBEDDING_DIM:
            return [float(x) for x in proposed.embedding]
        # Fall back to Ollama if configured.
        if self._embedder is None:
            # If caller passed an embedding of wrong dim, surface clearly.
            if proposed.embedding:
                return [float(x) for x in proposed.embedding]
            raise ValidationError(
                "no embedding provided and no embedder configured",
                field="embedding",
            )
        try:
            vec = await self._embedder.embed(proposed.natural)
        except (OllamaError, OllamaDimensionMismatch) as e:
            raise ValidationError(
                f"embedding failed: {e}",
                field="natural",
            ) from e
        return vec

    # =================================================================
    # retrieve — reconsolidation side effect
    # =================================================================
    async def retrieve(
        self,
        ids: Sequence[UUID],
        *,
        conn: asyncpg.Connection | None = None,
    ) -> list[ModelRow]:
        """
        Fetch models by id AND bump activation/retrieval counters.

        Reconsolidation is best-effort under concurrency: rows that are
        already locked by another Think run are still returned for
        retrieval, but their activation/retrieval counters are skipped
        for this pass. These counters are heat signals, not correctness
        gates, so large signal drains must never serialize on them.

        confidence is NOT TOUCHED. Ever. Reconsolidation is read-only
        with respect to the epistemic value.
        """
        id_list = list(ids)
        if not id_list:
            return []

        async def _run(c: asyncpg.Connection) -> list[ModelRow]:
            await _ensure_vector_codec(c)
            await c.execute(
                """
                WITH target AS (
                    SELECT id
                    FROM models
                    WHERE id = ANY($1::uuid[])
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE models AS m
                SET last_retrieved_at = now(),
                    retrieval_count = retrieval_count + 1,
                    activation = LEAST(1.0, activation + 0.15)
                FROM target
                WHERE m.id = target.id
                """,
                id_list,
            )
            rows = await c.fetch(
                f"""
                SELECT {_SELECT_COLS_SQL}
                FROM models
                WHERE id = ANY($1::uuid[])
                """,
                id_list,
            )
            return [_hydrate_row(r) for r in rows]

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # =================================================================
    # get_by_id — no side effect
    # =================================================================
    async def get_by_id(
        self,
        model_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> ModelRow | None:
        async def _run(c: asyncpg.Connection) -> ModelRow | None:
            await _ensure_vector_codec(c)
            row = await c.fetchrow(
                f"SELECT {_SELECT_COLS_SQL} FROM models WHERE id = $1",
                model_id,
            )
            if row is None:
                return None
            return _hydrate_row(row)

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # =================================================================
    # correction visibility fence
    # =================================================================
    async def fence_for_correction(
        self,
        model_id: UUID,
        *,
        tenant_id: UUID,
        cause_event_id: UUID | None,
        cause_model_id: UUID,
        conn: asyncpg.Connection | None = None,
    ) -> ModelRow | None:
        """Fail closed while a dependent Model awaits correction re-evaluation.

        Returns the changed Model, or ``None`` when the Model is already hidden
        or no longer active. The tenant predicate is mandatory so a correction
        lineage can never fence a Model outside its own tenant.
        """

        async def _run(c: asyncpg.Connection) -> ModelRow | None:
            await _ensure_vector_codec(c)
            pre_row = await c.fetchrow(
                """
                SELECT status, visible_to_subjects
                FROM models
                WHERE id = $1 AND tenant_id = $2
                FOR UPDATE
                """,
                model_id,
                tenant_id,
            )
            if pre_row is None:
                raise ValidationError(
                    f"model {model_id} not found",
                    model_id=str(model_id),
                    tenant_id=str(tenant_id),
                )
            if (
                str(pre_row["status"]) != "active"
                or not bool(pre_row["visible_to_subjects"])
            ):
                return None

            row = await c.fetchrow(
                f"""
                UPDATE models
                SET visible_to_subjects = FALSE
                WHERE id = $1
                  AND tenant_id = $2
                  AND status = 'active'
                  AND visible_to_subjects = TRUE
                RETURNING {_SELECT_COLS_SQL}
                """,
                model_id,
                tenant_id,
            )
            if row is None:
                return None
            hydrated = _hydrate_row(row)
            previous_state = {"visible_to_subjects": True}
            new_state = {"visible_to_subjects": False}

            await emit_state_change(
                c,
                kind="fence_model_for_correction",
                entity_id=hydrated.id,
                tenant_id=hydrated.tenant_id,
                cause_event_id=cause_event_id,
                entity_kind="model",
                metadata={
                    "cause_model_id": str(cause_model_id),
                    "fence_reason": "grounding_corrected",
                },
            )

            from services.reasoning.think.audit import (  # noqa: WPS433
                CAUSE_FIELD_UPDATE,
                emit_audit_event,
            )

            await emit_audit_event(
                c,
                model_id=hydrated.id,
                tenant_id=hydrated.tenant_id,
                cause_type=CAUSE_FIELD_UPDATE,
                new_state=new_state,
                previous_state=previous_state,
                cause_id=cause_event_id,
                changed_fields=["visible_to_subjects"],
            )
            await emit_model_event(
                c,
                model=hydrated,
                event_type=MODEL_EVENT_UPDATED,
                changed_fields=["visible_to_subjects"],
                previous_snapshot=previous_state,
                source_event_id=cause_event_id,
            )
            return hydrated

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            async with owned.transaction():
                return await _run(owned)

    # =================================================================
    # archive
    # =================================================================
    async def archive(
        self,
        model_id: UUID,
        reason: ModelArchiveReason,
        *,
        cause_event_id: UUID | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> ModelRow:
        """
        Archive a Model and flag its dependents. Uses the spec's UPDATE
        pattern; reason must be one of the nine legal archive_reasons
        OR 'deprecated' (post-Wave-0 A3). NEVER touches
        confidence_at_assertion.
        """
        async def _run(c: asyncpg.Connection) -> ModelRow:
            await _ensure_vector_codec(c)
            # Fetch pre-archive state for the audit event. SELECT inside
            # the transaction; the subsequent UPDATE serialises with
            # other writers via the row lock acquired by UPDATE.
            pre_row = await c.fetchrow(
                f"SELECT {_SELECT_COLS_SQL} FROM models WHERE id = $1",
                model_id,
            )
            if pre_row is None:
                raise ValidationError(
                    f"model {model_id} not found",
                    model_id=str(model_id),
                )
            pre_hydrated = _hydrate_row(pre_row)

            row = await c.fetchrow(
                f"""
                UPDATE models
                SET status = 'archived',
                    archived_at = now(),
                    archive_reason = $2
                WHERE id = $1
                RETURNING {_SELECT_COLS_SQL}
                """,
                model_id,
                reason,
            )
            if row is None:
                raise ValidationError(
                    f"model {model_id} not found",
                    model_id=str(model_id),
                )
            hydrated = _hydrate_row(row)

            # S1 archive cascade: the registry's per-kind callbacks
            # decide how each edge cascades. Behavior preserved for
            # `supports` (cause_kind derived from archive_reason via
            # the same five-value mapping the pre-S1 code used —
            # owned by lib/shared/edge_registry.py). New cascades fire
            # for `instance_of` and `contributes_to_resolution`.
            #
            # Dual-write safety net: we ALSO run the legacy array-based
            # cascade for any dependent that has the archived Model in
            # its supporting_model_ids but doesn't yet have a typed
            # `supports` edge (pre-S1 data not yet backfilled). The
            # registry callback dedups via the model_reeval_queue
            # UNIQUE constraint, so running both is safe.
            #
            # 1. Edge-driven cascade. Walk forward edges (this Model
            #    as source) and backward edges (this Model as target)
            #    and dispatch to the appropriate registry callback.
            edge_cascade_count = 0
            forward_edges = await _EDGES.traverse_forward(
                c,
                source=model_id,
                kinds=list(EDGE_REGISTRY.keys()),
                tenant_id=hydrated.tenant_id,
                status="active",
            )
            for edge in forward_edges:
                spec = get_spec(edge["edge_kind"])
                if spec.on_source_archive is not None:
                    await spec.on_source_archive(
                        c,
                        model_id,                      # archived
                        edge["target_model_id"],       # other endpoint
                        edge,
                        reason,
                    )
                    edge_cascade_count += 1
            backward_edges = await _EDGES.traverse_backward(
                c,
                target=model_id,
                kinds=list(EDGE_REGISTRY.keys()),
                tenant_id=hydrated.tenant_id,
                status="active",
            )
            for edge in backward_edges:
                spec = get_spec(edge["edge_kind"])
                if spec.on_target_archive is not None:
                    await spec.on_target_archive(
                        c,
                        model_id,                      # archived
                        edge["source_model_id"],       # other endpoint
                        edge,
                        reason,
                    )
                    edge_cascade_count += 1

            # 2. Legacy array-based cascade safety net. Catches
            #    dependents whose typed `supports` edge hasn't been
            #    backfilled yet. Same SQL shape as pre-S1; same
            #    cause_kind derivation, now sourced from the registry.
            #    The model_reeval_queue UNIQUE NULLS NOT DISTINCT
            #    constraint dedups against the rows the edge cascade
            #    just inserted, so running both is safe.
            from lib.shared.edge_registry import legacy_supports_cause_kind
            legacy_cause_kind = legacy_supports_cause_kind(reason)
            deps = await c.fetch(
                """
                SELECT id FROM models
                WHERE $1 = ANY(supporting_model_ids) AND status = 'active'
                """,
                model_id,
            )
            dep_ids = [r["id"] for r in deps]
            from services.domain.triggers import enqueue_model_reeval

            for dep_id in dep_ids:
                await enqueue_model_reeval(
                    c,
                    tenant_id=hydrated.tenant_id,
                    model_id=dep_id,
                    cause_model_id=model_id,
                    cause_kind=legacy_cause_kind,
                )

            # 3. Mark every edge touching this Model inert (same
            #    transaction). Inert edges stay queryable for audit
            #    but don't appear in active-only retrieval.
            inerted = await _EDGES.mark_inert(
                c,
                model_id=model_id,
                tenant_id=hydrated.tenant_id,
                reason="endpoint_archived",
            )

            await emit_state_change(
                c,
                kind="archive_model",
                entity_id=hydrated.id,
                tenant_id=hydrated.tenant_id,
                cause_event_id=cause_event_id,
                entity_kind="model",
                metadata={
                    "archive_reason": reason,
                    "dependent_count": len(dep_ids),
                    "reeval_cause_kind": legacy_cause_kind,
                    "edge_cascades": edge_cascade_count,
                    "edges_marked_inert": len(inerted),
                },
            )

            # Audit event: partial snapshots of the fields that
            # changed. status/archive_reason are the legible diff.
            from services.reasoning.think.audit import (  # noqa: WPS433
                CAUSE_ARCHIVE,
                diff_changed_fields,
                emit_audit_event,
            )
            previous_state = {
                "status": pre_hydrated.status,
                "archive_reason": pre_hydrated.archive_reason,
            }
            new_state = {
                "status": hydrated.status,
                "archive_reason": hydrated.archive_reason,
            }
            await emit_audit_event(
                c,
                model_id=hydrated.id,
                tenant_id=hydrated.tenant_id,
                cause_type=CAUSE_ARCHIVE,
                new_state=new_state,
                previous_state=previous_state,
                cause_id=cause_event_id,
                changed_fields=diff_changed_fields(previous_state, new_state),
            )
            await emit_model_event(
                c,
                model=hydrated,
                event_type=MODEL_EVENT_ARCHIVED,
                changed_fields=["status", "archived_at", "archive_reason"],
                previous_snapshot=previous_state,
                source_event_id=cause_event_id,
            )
            return hydrated

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            async with owned.transaction():
                return await _run(owned)

    # =================================================================
    # search_by_embedding
    # =================================================================
    async def search_by_embedding(
        self,
        vec: Sequence[float],
        *,
        tenant_id: UUID,
        k: int = 20,
        scope_actors: Sequence[UUID] | None = None,
        scope_entities: Sequence[dict[str, Any]] | None = None,
        kind: PropositionKind | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> list[ModelRow]:
        """
        HNSW cosine search. Always filters status='active' so the
        partial index `models_embedding_idx` is used.
        """
        vec_list = [float(x) for x in vec]
        if len(vec_list) != EMBEDDING_DIM:
            raise ValidationError(
                f"search vec dim {len(vec_list)} != {EMBEDDING_DIM}"
            )

        async def _run(c: asyncpg.Connection) -> list[ModelRow]:
            await _ensure_vector_codec(c)
            params: list[Any] = [vec_list, tenant_id, k]
            where = ["status = 'active'", "tenant_id = $2"]
            if scope_actors:
                params.append(list(scope_actors))
                where.append(f"scope_actors && ${len(params)}::uuid[]")
            expanded_scope_entities = (
                await _expand_scope_entities_via_customer_commitments(
                    c,
                    tenant_id=tenant_id,
                    scope_entities=scope_entities,
                )
            )
            _append_scope_entities_filter(
                where,
                params,
                scope_entities,
                expanded_scope_entities,
            )
            if kind is not None:
                params.append(kind)
                where.append(f"proposition_kind = ${len(params)}")

            sql = f"""
                SELECT {_SELECT_COLS_SQL}
                FROM models
                WHERE {" AND ".join(where)}
                ORDER BY embedding <=> $1::vector
                LIMIT $3
            """
            rows = await c.fetch(sql, *params)
            if len(rows) < k:
                async with c.transaction():
                    await c.execute("SET LOCAL enable_indexscan = off")
                    await c.execute("SET LOCAL enable_bitmapscan = off")
                    exact_rows = await c.fetch(sql, *params)
                if len(exact_rows) > len(rows):
                    rows = exact_rows
            return [_hydrate_row(r) for r in rows]

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # =================================================================
    # search_by_scope — GIN on scope_actors / scope_entities
    # =================================================================
    async def search_by_scope(
        self,
        *,
        tenant_id: UUID,
        scope_actors: Sequence[UUID] | None = None,
        scope_entities: Sequence[dict[str, Any]] | None = None,
        status: ModelStatus | None = "active",
        limit: int = 100,
        conn: asyncpg.Connection | None = None,
    ) -> list[ModelRow]:
        async def _run(c: asyncpg.Connection) -> list[ModelRow]:
            await _ensure_vector_codec(c)
            params: list[Any] = [tenant_id]
            where = ["tenant_id = $1"]
            if status is not None:
                params.append(status)
                where.append(f"status = ${len(params)}")
            if scope_actors:
                params.append(list(scope_actors))
                where.append(f"scope_actors && ${len(params)}::uuid[]")
            expanded_scope_entities = (
                await _expand_scope_entities_via_customer_commitments(
                    c,
                    tenant_id=tenant_id,
                    scope_entities=scope_entities,
                )
            )
            _append_scope_entities_filter(
                where,
                params,
                scope_entities,
                expanded_scope_entities,
            )
            params.append(limit)
            sql = f"""
                SELECT {_SELECT_COLS_SQL}
                FROM models
                WHERE {" AND ".join(where)}
                ORDER BY created_at DESC, id DESC
                LIMIT ${len(params)}
            """
            rows = await c.fetch(sql, *params)
            return [_hydrate_row(r) for r in rows]

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # =================================================================
    # get_predictions_due
    # =================================================================
    async def get_predictions_due(
        self,
        before_ts: datetime,
        *,
        tenant_id: UUID,
        limit: int = 500,
        conn: asyncpg.Connection | None = None,
    ) -> list[ModelRow]:
        async def _run(c: asyncpg.Connection) -> list[ModelRow]:
            await _ensure_vector_codec(c)
            rows = await c.fetch(
                f"""
                SELECT {_SELECT_COLS_SQL}
                FROM models
                WHERE status = 'active'
                  AND tenant_id = $1
                  AND evaluate_at IS NOT NULL
                  AND evaluate_at <= $2
                ORDER BY evaluate_at ASC
                LIMIT $3
                """,
                tenant_id,
                before_ts,
                limit,
            )
            return [_hydrate_row(r) for r in rows]

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # =================================================================
    # bulk_confidence_update — used by Calibration updater (Wave 4-C)
    # =================================================================
    async def bulk_confidence_update(
        self,
        updates: dict[UUID, float],
        *,
        cause_event_id: UUID | None = None,
        audit_cause_override: str | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> list[ModelRow]:
        """
        Atomically update confidence for N Models and emit one
        state_change per changed row.

        IMPORTANT: this path deliberately never UPDATEs
        `confidence_at_assertion`. Q3 resolution: that column is the
        pre-calibration assertion, captured at INSERT and immutable
        afterwards. The DB has no trigger enforcing this; the
        application MUST keep the column out of every UPDATE statement.

        `audit_cause_override`: when set, used as the audit_events
        cause_type instead of the default `confidence_update`.
        Callers in the reconciler-substitution path (applier sees a
        recon decision of auto_merge or second_pass_merge that
        produced a confidence-only update) pass `reconciliation_merge`
        so the audit chain records the merge correctly.
        """
        if not updates:
            return []

        async def _run(c: asyncpg.Connection) -> list[ModelRow]:
            await _ensure_vector_codec(c)
            ids: list[UUID] = []
            vals: list[float] = []
            for mid, conf in updates.items():
                ids.append(mid)
                vals.append(_clip_confidence(float(conf)))

            # Fetch pre-update confidences for audit previous_state.
            pre_rows = await c.fetch(
                "SELECT id, confidence FROM models WHERE id = ANY($1::uuid[])",
                ids,
            )
            pre_conf: dict[UUID, float] = {
                r["id"]: float(r["confidence"]) for r in pre_rows
            }

            # UPDATE ... FROM (VALUES ...) AS u(id, conf).
            # We build a parameter list of (id, conf) pairs.
            # asyncpg doesn't support composite parameter arrays cleanly,
            # so we pass two parallel arrays and unnest them.
            rows = await c.fetch(
                f"""
                UPDATE models AS m
                SET confidence = u.new_conf
                FROM UNNEST($1::uuid[], $2::float8[]) AS u(u_id, new_conf)
                WHERE m.id = u.u_id
                RETURNING {_SELECT_COLS_SQL}
                """,
                ids,
                vals,
            )
            hydrated = [_hydrate_row(r) for r in rows]

            from services.reasoning.think.audit import (  # noqa: WPS433
                CAUSE_CONFIDENCE_UPDATE,
                emit_audit_event,
            )
            audit_cause = audit_cause_override or CAUSE_CONFIDENCE_UPDATE
            for row in hydrated:
                await emit_state_change(
                    c,
                    kind="bulk_confidence_update",
                    entity_id=row.id,
                    tenant_id=row.tenant_id,
                    cause_event_id=cause_event_id,
                    entity_kind="model",
                    metadata={"new_confidence": row.confidence},
                )
                old_conf = pre_conf.get(row.id)
                previous_state = (
                    {"confidence": old_conf}
                    if old_conf is not None
                    else None
                )
                new_state = {"confidence": float(row.confidence)}
                await emit_audit_event(
                    c,
                    model_id=row.id,
                    tenant_id=row.tenant_id,
                    cause_type=audit_cause,
                    new_state=new_state,
                    previous_state=previous_state,
                    cause_id=cause_event_id,
                    changed_fields=["confidence"],
                )
                await emit_model_event(
                    c,
                    model=row,
                    event_type=MODEL_EVENT_UPDATED,
                    changed_fields=["confidence"],
                    previous_snapshot=previous_state,
                    source_event_id=cause_event_id,
                )
            return hydrated

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            async with owned.transaction():
                return await _run(owned)


__all__ = ["ModelsRepo", "ModelsRepoError"]
