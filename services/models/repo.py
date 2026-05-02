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
from pgvector.asyncpg import register_vector

from lib.embeddings.ollama import (
    EMBEDDING_DIM,
    OllamaClient,
    OllamaDimensionMismatch,
    OllamaError,
)
from lib.shared.db import RowHydrationError
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
from services.models.falsifier import is_adequate_falsifier
from services.models.propositions import validate_proposition
from services.models.recommendations import validate_recommendation
from services.observations.state_change import emit_state_change


# ---------------------------------------------------------------------
# Constants + helpers
# ---------------------------------------------------------------------


class ModelsRepoError(CompanyOSError):
    default_code = "models_repo_error"


_CONFIDENCE_MIN = 0.05
_CONFIDENCE_MAX = 0.95
_FALSIFIER_REQUIRED_ABOVE = 0.7


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


# pgvector codec registration. We avoid WeakSet because asyncpg's
# PoolConnectionProxy cannot be weak-referenced. Track registered
# connection ids in a bounded LRU-like set; since asyncpg reuses
# connections across tests, a simple set that's cleared on overflow
# is fine for this purpose (double-registration is harmless at the
# Postgres level).
_VECTOR_REGISTERED_IDS: set[int] = set()


async def _ensure_vector_codec(conn: asyncpg.Connection) -> None:
    key = id(conn)
    if key in _VECTOR_REGISTERED_IDS:
        return
    try:
        await register_vector(conn)
    except Exception:
        # Duplicate registration is safe; swallow.
        pass
    _VECTOR_REGISTERED_IDS.add(key)
    # Bound the set so it doesn't grow unbounded in long-running procs.
    if len(_VECTOR_REGISTERED_IDS) > 1000:
        _VECTOR_REGISTERED_IDS.clear()


def _jsonb(value: Any) -> str:
    """asyncpg needs a JSON string when the param is cast ::jsonb."""
    return json.dumps(value, sort_keys=True, default=str)


def _clip_confidence(value: float) -> float:
    if value < _CONFIDENCE_MIN:
        return _CONFIDENCE_MIN
    if value > _CONFIDENCE_MAX:
        return _CONFIDENCE_MAX
    return float(value)


# Map archive_reason → cause_kind for model_reeval_queue enqueue.
# See BUILD-LOG.md "Wave 2→3 Q4/Q8 resolution" and SCHEMA-LOCK.md
# "Post-Wave-2 amendments" for the five-value cause_kind taxonomy.
_ARCHIVE_REASON_TO_CAUSE_KIND: dict[str, str] = {
    "deprecated": "supporting_deprecated",
    "superseded": "supporting_superseded",
    "falsifier_triggered": "falsifier_triggered_upstream",
    "contested_incorrect": "contested_cluster",
    "contested_reading_incorrect": "contested_cluster",
}


def _archive_reason_to_cause_kind(reason: str) -> str:
    # Any reason we don't specifically recognize is a generic
    # "supporting_archived" — the dependent still needs re-eval, we
    # just don't have a more specific classification.
    return _ARCHIVE_REASON_TO_CAUSE_KIND.get(reason, "supporting_archived")


async def _check_no_support_cycle(
    conn: asyncpg.Connection,
    *,
    new_model_id: UUID,
    new_supports: list[UUID],
) -> None:
    """
    Invariant M3 (ARCHITECTURE-REVIEW-1 §C4): the `supporting_model_ids`
    graph is a DAG. Reject the insert/update if adding edges
    (new_model_id → s) for s in new_supports would create a cycle.

    A cycle would form iff `new_model_id` is already (transitively) in
    the supporting-ancestor set of any `s`. That is, climbing from `s`
    up the `supporting_model_ids` edges, we eventually reach
    `new_model_id`.

    Self-support is explicitly rejected.

    Implementation: single recursive CTE. The hot path is the empty-supports
    case (no edges to check, function returns immediately).
    """
    if not new_supports:
        return

    # Self-support.
    if new_model_id in new_supports:
        raise ValidationError(
            "supporting_model_ids cannot reference the model itself",
            model_id=str(new_model_id),
        )

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
    ) -> ModelRow:
        """
        Insert a Model through the full §2 pipeline.

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
                conn, proposed, prop_kind, conf_at_assertion
            )
        async with self._require_pool().acquire() as owned:
            async with owned.transaction():
                return await self._insert_core(
                    owned, proposed, prop_kind, conf_at_assertion
                )

    async def _insert_core(
        self,
        conn: asyncpg.Connection,
        proposed: ModelCreate,
        prop_kind: PropositionKind,
        conf_at_assertion: float,
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
        calibrated_conf = await apply_calibration(
            proposed.confidence,
            proposed.scope_actors,
            prop_kind,
            tenant_id=proposed.tenant_id,
            conn=conn,
        )

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

            # Dependent flagging — Q8 resolved: real table
            # `model_reeval_queue` exists (migration 0007). Archive
            # cascades by enqueueing every active dependent with the
            # appropriate cause_kind derived from `reason`. Dedup is
            # enforced by the UNIQUE NULLS NOT DISTINCT constraint on
            # (tenant_id, model_id, cause_model_id, processed_at), so
            # re-archiving the same model is idempotent against the
            # unprocessed queue tail.
            deps = await c.fetch(
                """
                SELECT id FROM models
                WHERE $1 = ANY(supporting_model_ids) AND status = 'active'
                """,
                model_id,
            )
            dep_ids = [r["id"] for r in deps]

            cause_kind = _archive_reason_to_cause_kind(reason)
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
                    cause_kind,
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
                    "reeval_cause_kind": cause_kind,
                },
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
            return hydrated

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            async with owned.transaction():
                return await _run(owned)


__all__ = ["ModelsRepo", "ModelsRepoError"]
