"""
services/models/repo.py — Models repository.

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
import math
from datetime import datetime
from typing import Any, Sequence
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
from lib.shared.db import RowHydrationError
from lib.shared.edge_registry import EDGE_REGISTRY, get_spec
from lib.shared.errors import CompanyOSError, FalsifierInadequateError, ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import (
    ModelArchiveReason,
    ModelCreate,
    ModelRow,
    ModelStatus,
    PropositionKind,
)

from services.models.calibration import apply_calibration
from services.models.edges_repo import EdgesRepo
from services.models.falsifier import is_adequate_falsifier
from services.models.propositions import validate_proposition
from services.models.recommendations import validate_recommendation
from services.observations.state_change import emit_state_change
from services.topology.topo_repo import TopoRepo
# NOTE: audit module is imported lazily inside the methods that use it.
# Importing services.think.audit at module-load time triggers
# services/think/__init__.py, which imports reason.py → retrieval →
# services.models.repo (this module). The circular import is benign at
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
_log = structlog.get_logger(__name__)


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
_SELECT_COLS = (
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
    # Recommendation-kind columns (migration 0022). target_actor_id is
    # GENERATED from the proposition JSONB; caused_act_change_id is
    # written by the recommendation act handler.
    "target_actor_id", "caused_act_change_id",
)
_SELECT_COLS_SQL = ", ".join(_SELECT_COLS)


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
#   (services/retrieval/pathways.py:_conn_has_vector_codec) reads
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


def _clip_confidence(value: float) -> float:
    if value < _CONFIDENCE_MIN:
        return _CONFIDENCE_MIN
    if value > _CONFIDENCE_MAX:
        return _CONFIDENCE_MAX
    return float(value)


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

# Singleton TopoRepo for the S2 topology layer. Shares the same
# pool-less / conn-only contract as _EDGES.
_TOPO = TopoRepo()


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
#   2. _apply_claim_op (services/think/applier.py) — UPDATE path:
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
        from services.recommendations.handlers import act_on_recommendation

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
        try:
            raw["embedding"] = [float(x) for x in emb]
        except TypeError:
            pass
    try:
        return ModelRow.model_validate(raw)
    except Exception as e:
        raise RowHydrationError(
            f"could not hydrate models row: {e}",
            row_keys=list(record.keys()),
        ) from e


# ---------------------------------------------------------------------
# ModelsRepo
# ---------------------------------------------------------------------


class ModelsRepo:
    def __init__(
        self,
        pool: asyncpg.Pool | None = None,
        *,
        embedder: OllamaClient | None = None,
    ) -> None:
        # Pool is optional when every call site supplies its own `conn`
        # (e.g. promote_pattern_candidate inside Think T4 pattern_review).
        # Methods that need a pool when conn is None raise a clear
        # error via `_require_pool()`.
        self._pool = pool
        self._embedder = embedder

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise ModelsRepoError(
                "ModelsRepo was constructed without a pool; "
                "callers in conn-only mode must pass conn= on every call"
            )
        return self._pool

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
        # -- 1. Falsifier adequacy if confidence > 0.7 -----------------
        if proposed.confidence > _FALSIFIER_REQUIRED_ABOVE:
            ok, reason = is_adequate_falsifier(proposed.falsifier)
            if not ok:
                raise FalsifierInadequateError(
                    reason or "falsifier inadequate",
                    falsifier=proposed.falsifier,
                    confidence=proposed.confidence,
                )

        # -- 2. Validate proposition JSON ------------------------------
        validated_prop = validate_proposition(proposed.proposition)
        prop_kind: PropositionKind = validated_prop.kind  # type: ignore[assignment]

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

        # -- 3. Invariant M3: supporting_model_ids acyclicity.
        # Per ARCHITECTURE-REVIEW-1 §C4: reject inserts whose
        # supporting_model_ids would create a cycle. Cheap recursive CTE
        # over an index on models.supporting_model_ids (GIN).
        model_id_preview = proposed.id or uuid7()
        await _check_no_support_cycle(
            conn,
            new_model_id=model_id_preview,
            new_supports=list(proposed.supporting_model_ids or []),
        )

        # -- 3b. Recommendation cross-field validation.
        # Pydantic enforces shape; here we check live DB state:
        # target entity exists in tenant, transition reachable.
        if prop_kind == "recommendation":
            await validate_recommendation(
                proposed.proposition,
                tenant_id=proposed.tenant_id,
                conn=conn,
            )

        # -- 4. Apply calibration (Wave 4-C: real DB lookup) -----------
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

        # -- 4. Clip confidence ----------------------------------------
        final_conf = _clip_confidence(calibrated_conf)

        # 5. scope_actors existence check.
        if proposed.scope_actors:
            existing = await conn.fetch(
                "SELECT id FROM actors WHERE id = ANY($1::uuid[])",
                list(proposed.scope_actors),
            )
            existing_ids = {r["id"] for r in existing}
            missing = [a for a in proposed.scope_actors if a not in existing_ids]
            if missing:
                raise ValidationError(
                    f"scope_actors reference {len(missing)} non-existent actor(s)",
                    missing=[str(m) for m in missing],
                )

        # 6. Compute embedding if not supplied.
        embedding = await self._resolve_embedding(proposed)
        if len(embedding) != EMBEDDING_DIM:
            raise ValidationError(
                f"embedding dim {len(embedding)} != {EMBEDDING_DIM}",
                got=len(embedding),
                expected=EMBEDDING_DIM,
            )

        model_id = model_id_preview  # pre-assigned in step 3 for cycle check

        # 7. INSERT. "natural" is a reserved keyword in SQL, so it must
        # be quoted in identifier contexts (Wave 0 migration does the
        # same — see SCHEMA-QUESTION Q0 / BUILD-LOG entry 0.1).
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
                confidence_at_assertion, activation_coefficient
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, $5, $6,
                $7::uuid[], $8::jsonb, $9::jsonb,
                $10, $11, $12::jsonb,
                $13::jsonb, $14,
                $15::uuid[], $16::uuid[], $17,
                $18, $19, $20::jsonb,
                $21::uuid[], $22,
                $23, $24
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
            1.0,  # activation starts at 1.0 (DB default; set explicit for clarity)
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
        )
        assert row is not None

        hydrated = _hydrate_row(row)

        # 7b. Dual-write typed edges to mirror the array columns just
        # written. Goes through the chokepoint helper so the drift
        # detector stays happy. update_arrays=False because the INSERT
        # above already set the array columns.
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

        # 7c. S2 topology layer: synchronously initialize this Model's
        # topo_embedding from its content (via content_anchor) so
        # Pathway F (S3) can find it the moment it commits. The
        # asynchronous topology_updater worker will refine the
        # position once neighbors exist; the initial enqueue here
        # ensures that happens.
        try:
            await _TOPO.set_initial_topo(
                conn,
                model_id=hydrated.id,
                content_embedding=embedding,
                tenant_id=hydrated.tenant_id,
                enqueue_propagation=True,
            )
        except Exception:
            # Topology is best-effort during S2 dual-write phase —
            # if anything in the topology layer fails, the Model
            # itself still inserts successfully. The drift between
            # topo_embedding NULL and "should have been set" will
            # be caught by the topology_updater on its next sweep
            # (it picks up Models with NULL topo_embedding too).
            pass

        # 8. Emit state_change in the same transaction.
        await emit_state_change(
            conn,
            kind="insert_model",
            entity_id=hydrated.id,
            tenant_id=hydrated.tenant_id,
            cause_event_id=hydrated.born_from_event_id,
            entity_kind="model",
            metadata={
                "proposition_kind": hydrated.proposition_kind,
                "confidence": hydrated.confidence,
            },
        )

        # 8b. Emit audit_events row (PR 1, Q5). Full snapshot as
        # new_state since this is the chain root for this Model.
        from services.think.audit import (  # noqa: WPS433 — see module top
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

        # Demo SSE: notify any open action-list streams for this actor
        # that a new recommendation has landed. No-op outside demo
        # mode (publish is a fan-out to in-process subscribers; if no
        # one is listening, nothing happens).
        if hydrated.proposition_kind == "recommendation" and hydrated.target_actor_id:
            from services.demo.sse import publish_recommendation_event

            await publish_recommendation_event(
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

            # Auto-accept low-risk create-commitment recommendations.
            # Self-reported new work ("I've started the backend rewrite")
            # produces a recommendation whose payload already names the
            # owner and the contributing goal — making the human-approval
            # step ceremonial. Auto-accept here so the new Commitment
            # appears in the ledger without an explicit click; failures
            # are swallowed so the recommendation stays in the queue and
            # the user can act on it manually.
            await _maybe_auto_accept(hydrated, conn)

        return hydrated

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

        Exactly mirrors spec §2 retrieval SQL:
            UPDATE models
            SET last_retrieved_at = now(),
                retrieval_count = retrieval_count + 1,
                activation = LEAST(1.0, activation + 0.15)
            WHERE id = ANY($retrieved_ids)
            RETURNING *;

        confidence is NOT TOUCHED. Ever. Reconsolidation is read-only
        with respect to the epistemic value.
        """
        id_list = list(ids)
        if not id_list:
            return []

        async def _run(c: asyncpg.Connection) -> list[ModelRow]:
            await _ensure_vector_codec(c)
            rows = await c.fetch(
                f"""
                UPDATE models
                SET last_retrieved_at = now(),
                    retrieval_count = retrieval_count + 1,
                    activation = LEAST(1.0, activation + 0.15)
                WHERE id = ANY($1::uuid[])
                RETURNING {_SELECT_COLS_SQL}
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
            for dep_id in dep_ids:
                await c.execute(
                    """
                    INSERT INTO model_reeval_queue
                      (id, tenant_id, model_id, cause_model_id, cause_kind)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT ON CONSTRAINT model_reeval_queue_dedup
                    DO NOTHING
                    """,
                    uuid7(),
                    hydrated.tenant_id,
                    dep_id,
                    model_id,
                    legacy_cause_kind,
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
            from services.think.audit import (  # noqa: WPS433
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

        params: list[Any] = [vec_list, tenant_id, k]
        where = ["status = 'active'", "tenant_id = $2"]
        if scope_actors:
            params.append(list(scope_actors))
            where.append(f"scope_actors && ${len(params)}::uuid[]")
        if scope_entities:
            params.append(_jsonb(list(scope_entities)))
            where.append(f"scope_entities @> ${len(params)}::jsonb")
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

        async def _run(c: asyncpg.Connection) -> list[ModelRow]:
            await _ensure_vector_codec(c)
            rows = await c.fetch(sql, *params)
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
        params: list[Any] = [tenant_id]
        where = ["tenant_id = $1"]
        if status is not None:
            params.append(status)
            where.append(f"status = ${len(params)}")
        if scope_actors:
            params.append(list(scope_actors))
            where.append(f"scope_actors && ${len(params)}::uuid[]")
        if scope_entities:
            params.append(_jsonb(list(scope_entities)))
            where.append(f"scope_entities @> ${len(params)}::jsonb")
        params.append(limit)
        sql = f"""
            SELECT {_SELECT_COLS_SQL}
            FROM models
            WHERE {" AND ".join(where)}
            ORDER BY created_at DESC, id DESC
            LIMIT ${len(params)}
        """

        async def _run(c: asyncpg.Connection) -> list[ModelRow]:
            await _ensure_vector_codec(c)
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

            from services.think.audit import (  # noqa: WPS433
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
            return hydrated

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            async with owned.transaction():
                return await _run(owned)


__all__ = ["ModelsRepo", "ModelsRepoError"]
