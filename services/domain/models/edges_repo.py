"""
services/domain/models/edges_repo.py — repository for the unified
Model-to-Model edge primitive (migration 0031_model_edges.sql).

Single writer for `model_edges`. Every edge mutation in the codebase
goes through this repo; arrays on `models` (supporting_model_ids,
contributing_models) are kept in sync by services/domain/models/repo.py's
`_set_model_relations()` helper, which calls this repo. The drift
detector (services/workers/edge_drift) verifies parity continuously.

Public API:

  EdgesRepo(pool=None)

  .link(conn, *, source, target, kind, tenant_id, detected_by,
        weight=None, metadata=None, created_by_event_id=None,
        confidence=1.0, evidence_event_ids=None,
        evidence_model_ids=None, explanation=None,
        review_status='accepted', decay_after=None, expires_at=None)
      Insert one edge (or two for symmetric kinds). Idempotent on
      the (tenant, source, target, kind) UNIQUE; returns the
      inserted-or-existing edge id(s). Validates the kind is
      writable, weight rules, mutually-exclusive-with, and DAG
      cycle scope.

  .unlink(conn, *, source, target, kind, tenant_id)
      Hard DELETE. Used by the diff path inside
      `_set_model_relations` when the array column has shrunk.

  .traverse_forward(conn, *, source, kinds, tenant_id, status='active')
      "What does source relate to via these kinds?" — uses the
      partial index `model_edges_source_idx`.

  .traverse_backward(conn, *, target, kinds, tenant_id, status='active')
      "What relates to target via these kinds?" — uses
      `model_edges_target_idx`. THIS IS THE NEW CAPABILITY S1
      delivers: O(log n) reverse traversal for every kind.

  .mark_inert(conn, *, model_id, tenant_id, reason)
      Set status='inert' on every active edge where this Model is
      either source or target. Called from ModelsRepo.archive() in
      the same transaction.

  .check_no_cycle(conn, *, kind, source, targets, tenant_id)
      Generalized DAG check across the kind's `cycle_scope`. Replaces
      services/domain/models/repo.py:_check_no_support_cycle (which only
      handled `supports`). Recursive CTE over model_edges, scoped to
      the kinds in cycle_scope.

  .get_drift_sample(conn, *, tenant_id, sample_size=200)
      Samples Models and returns (model_id, array_kind,
      array_uuid_set, edge_uuid_set) tuples for drift detection.
      Used only by the drift detector worker.

Why a separate repo (not folded into ModelsRepo):
  Edges are a substrate-level concern with their own lifecycle and
  validation rules. ModelsRepo manages the 9-step model insert
  pipeline; EdgesRepo manages the graph layer. They cooperate via
  the `_set_model_relations` chokepoint helper, which is where the
  dual-write (array + edge) discipline is enforced.

Determinism note:
  Every method that takes a `conn` runs inside the caller's
  transaction; no method opens its own. This is required so dual-write
  with the array column updates is atomic — if the array UPDATE
  commits but the edge INSERT fails, the drift detector will catch
  it, but only after a window of inconsistency. We avoid that
  window by sharing the connection.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Sequence, get_args
from uuid import UUID

import asyncpg

from lib.shared.edge_registry import (
    EdgeKindSpec,
    EdgeRegistryError,
    cycle_scope_for,
)
from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import EdgeDetectedBy
from services.reasoning.relationships.ontology_runtime import (
    resolve_edge_kind_spec,
    validate_weight_for_spec,
)


# Legacy accepted-memory topology used to recompute positional graph
# embeddings after edge mutations. Topology is now the upstream latent
# relationship field, so edge writes no longer enqueue graph-geometry
# propagation. Keep this helper as a compatibility no-op for call sites
# that still converge model_edges.
async def _enqueue_topo_dirty(
    conn: asyncpg.Connection,
    *,
    model_id: UUID,
    tenant_id: UUID,
    cause_model_id: UUID | None = None,
    hop_depth: int = 0,
) -> None:
    del conn, model_id, tenant_id, cause_model_id, hop_depth
    return None


class EdgesRepoError(CompanyOSError):
    default_code = "edges_repo_error"


# Columns selected by traverse_* / get_by_id. Kept in declaration
# order matching the migration so consumers can map to dicts cleanly.
_SELECT_COLS_SQL = (
    "id, tenant_id, source_model_id, target_model_id, edge_kind, "
    "weight, metadata, status, detected_by, created_at, "
    "created_by_event_id, status_changed_at, status_reason, "
    "confidence, evidence_event_ids, evidence_model_ids, explanation, "
    "review_status, last_confirmed_at, confirmed_count, contested_count, "
    "decay_after, expires_at"
)

_ALLOWED_DETECTED_BY = set(get_args(EdgeDetectedBy))
_MISSING_CYCLE_SCOPE = object()


def _row_to_dict(row: asyncpg.Record) -> dict[str, Any]:
    """asyncpg Record → plain dict for cascade callbacks. Used so
    callbacks don't depend on Record being indexable by string and
    so tests can construct synthetic edges without a DB round-trip."""
    return {k: row[k] for k in row.keys()}


def _validate_confidence(confidence: float) -> float:
    try:
        c = float(confidence)
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            f"edge confidence must be numeric; got {confidence!r}",
            confidence=confidence,
        ) from exc
    if not (0.0 <= c <= 1.0):
        raise ValidationError(
            f"edge confidence out of range [0, 1]: {c}",
            confidence=c,
        )
    return c


def _unique_uuid_list(values: Sequence[UUID] | None) -> list[UUID]:
    if not values:
        return []
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in values:
        u = value if isinstance(value, UUID) else UUID(str(value))
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


class EdgesRepo:
    def __init__(self, pool: asyncpg.Pool | None = None) -> None:
        # Pool is optional: every public method accepts a `conn`
        # explicitly so the caller's transaction wraps the write.
        # The pool is only used by methods that don't take conn,
        # and those don't exist in v1.
        self._pool = pool

    # =================================================================
    # link — INSERT one or two edges (symmetric kinds get the mirror)
    # =================================================================
    async def link(
        self,
        conn: asyncpg.Connection,
        *,
        source: UUID,
        target: UUID,
        kind: str,
        tenant_id: UUID,
        detected_by: str,
        weight: float | None = None,
        metadata: dict[str, Any] | None = None,
        created_by_event_id: UUID | None = None,
        confidence: float = 1.0,
        evidence_event_ids: Sequence[UUID] | None = None,
        evidence_model_ids: Sequence[UUID] | None = None,
        explanation: str | None = None,
        review_status: str = "accepted",
        decay_after: datetime | None = None,
        expires_at: datetime | None = None,
    ) -> list[UUID]:
        """Insert an edge. Returns the edge id(s) (one for directed,
        two for symmetric).

        Idempotent on the (tenant, source, target, kind) UNIQUE: a
        duplicate INSERT silently no-ops via ON CONFLICT and returns
        the existing id.

        Raises:
          - EdgeRegistryError: unknown kind, reserved kind, weight
            rule violation, mutually-exclusive violation
          - ValidationError: self-edge attempt, missing tenant_id
        """
        # Registry validation (static kind or accepted ontology proposal).
        spec = await resolve_edge_kind_spec(conn, tenant_id=tenant_id, kind=kind)
        validate_weight_for_spec(spec, weight)
        confidence = _validate_confidence(confidence)
        event_evidence = _unique_uuid_list(evidence_event_ids)
        model_evidence = _unique_uuid_list(evidence_model_ids)
        if review_status not in {
            "accepted",
            "candidate",
            "needs_review",
            "disputed",
            "rejected",
            "retired",
        }:
            raise ValidationError(
                f"invalid edge review_status {review_status!r}",
                review_status=review_status,
            )
        if detected_by not in _ALLOWED_DETECTED_BY:
            raise ValidationError(
                f"invalid edge detected_by {detected_by!r}",
                detected_by=detected_by,
            )

        if source == target:
            raise ValidationError(
                f"self-edge not allowed: source == target == {source}",
                kind=kind,
            )

        # Mutually-exclusive-with: reject if any forbidden kind exists
        # between this pair already.
        await self._check_mutual_exclusion(
            conn,
            spec=spec,
            kind=kind,
            source=source,
            target=target,
            tenant_id=tenant_id,
        )
        if not spec.is_directed:
            await self._check_mutual_exclusion(
                conn,
                spec=spec,
                kind=kind,
                source=target,
                target=source,
                tenant_id=tenant_id,
            )

        # DAG cycle check across the kind's cycle_scope.
        scope = spec.cycle_scope
        if scope is not None:
            await self.check_no_cycle(
                conn,
                kind=kind,
                source=source,
                targets=[target],
                tenant_id=tenant_id,
                cycle_scope=scope,
            )

        # Insert one or two rows.
        ids = await self._insert_one(
            conn,
            source=source,
            target=target,
            kind=kind,
            tenant_id=tenant_id,
            detected_by=detected_by,
            weight=weight,
            metadata=metadata or {},
            created_by_event_id=created_by_event_id,
            confidence=confidence,
            evidence_event_ids=event_evidence,
            evidence_model_ids=model_evidence,
            explanation=explanation,
            review_status=review_status,
            decay_after=decay_after,
            expires_at=expires_at,
        )
        if not spec.is_directed:
            mirror_ids = await self._insert_one(
                conn,
                source=target,  # swap
                target=source,
                kind=kind,
                tenant_id=tenant_id,
                detected_by=detected_by,
                weight=weight,
                metadata=metadata or {},
                created_by_event_id=created_by_event_id,
                confidence=confidence,
                evidence_event_ids=event_evidence,
                evidence_model_ids=model_evidence,
                explanation=explanation,
                review_status=review_status,
                decay_after=decay_after,
                expires_at=expires_at,
            )
            ids.extend(mirror_ids)

        # Retired accepted-memory topology hook. Kept as a no-op so
        # edge writes no longer mutate graph-derived layout.
        await _enqueue_topo_dirty(
            conn,
            model_id=source,
            tenant_id=tenant_id,
            cause_model_id=target,
            hop_depth=0,
        )
        await _enqueue_topo_dirty(
            conn,
            model_id=target,
            tenant_id=tenant_id,
            cause_model_id=source,
            hop_depth=0,
        )
        return ids

    async def _check_mutual_exclusion(
        self,
        conn: asyncpg.Connection,
        *,
        spec: EdgeKindSpec,
        kind: str,
        source: UUID,
        target: UUID,
        tenant_id: UUID,
    ) -> None:
        """Reject active mutually-exclusive relationships for one ordered pair."""
        if not spec.mutually_exclusive_with:
            return
        existing_kind = await conn.fetchval(
            """
            SELECT edge_kind FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
              AND status = 'active'
              AND review_status != 'rejected'
              AND edge_kind = ANY($4::text[])
            LIMIT 1
            """,
            tenant_id,
            source,
            target,
            list(spec.mutually_exclusive_with),
        )
        if existing_kind is not None:
            raise EdgeRegistryError(
                f"edge_kind {kind!r} mutually exclusive with "
                f"{existing_kind!r} between {source} → {target}"
            )

    async def _insert_one(
        self,
        conn: asyncpg.Connection,
        *,
        source: UUID,
        target: UUID,
        kind: str,
        tenant_id: UUID,
        detected_by: str,
        weight: float | None,
        metadata: dict[str, Any],
        created_by_event_id: UUID | None,
        confidence: float,
        evidence_event_ids: Sequence[UUID],
        evidence_model_ids: Sequence[UUID],
        explanation: str | None,
        review_status: str,
        decay_after: datetime | None,
        expires_at: datetime | None,
    ) -> list[UUID]:
        """The actual SQL INSERT. Idempotent: ON CONFLICT returns
        the pre-existing id rather than raising. Wrapped in
        link() / mirror so the public API doesn't directly emit two
        SQL statements for symmetric kinds."""
        import json

        # We want INSERT ... RETURNING id, but ON CONFLICT DO NOTHING
        # returns nothing for the duplicate row. So we do a "INSERT
        # ... RETURNING id UNION ALL SELECT id ... WHERE NOT EXISTS"
        # trick via a CTE.
        new_id = uuid7()
        row = await conn.fetchrow(
            """
            WITH ins AS (
              INSERT INTO model_edges
                (id, tenant_id, source_model_id, target_model_id,
                 edge_kind, weight, metadata, status, detected_by,
                 created_by_event_id, confidence, evidence_event_ids,
                 evidence_model_ids, explanation, review_status,
                 decay_after, expires_at, last_confirmed_at, confirmed_count)
              VALUES (
                 $1, $2, $3, $4, $5, $6, $7::jsonb, 'active', $8, $9,
                 $10, $11::uuid[], $12::uuid[], $13, $14, $15, $16,
                 now(), 1
              )
              ON CONFLICT ON CONSTRAINT model_edges_unique DO UPDATE
                SET weight = COALESCE(EXCLUDED.weight, model_edges.weight),
                    metadata = model_edges.metadata || EXCLUDED.metadata,
                    confidence = GREATEST(model_edges.confidence, EXCLUDED.confidence),
                    evidence_event_ids = ARRAY(
                      SELECT DISTINCT x
                      FROM unnest(model_edges.evidence_event_ids || EXCLUDED.evidence_event_ids) AS t(x)
                    ),
                    evidence_model_ids = ARRAY(
                      SELECT DISTINCT x
                      FROM unnest(model_edges.evidence_model_ids || EXCLUDED.evidence_model_ids) AS t(x)
                    ),
                    explanation = COALESCE(EXCLUDED.explanation, model_edges.explanation),
                    review_status = CASE
                      WHEN model_edges.review_status IN ('rejected', 'retired')
                        THEN model_edges.review_status
                      WHEN model_edges.review_status = 'disputed'
                           OR EXCLUDED.review_status = 'disputed'
                        THEN 'disputed'
                      WHEN model_edges.review_status = 'accepted'
                           OR EXCLUDED.review_status = 'accepted'
                        THEN 'accepted'
                      WHEN model_edges.review_status = 'needs_review'
                           OR EXCLUDED.review_status = 'needs_review'
                        THEN 'needs_review'
                      ELSE 'candidate'
                    END,
                    decay_after = COALESCE(EXCLUDED.decay_after, model_edges.decay_after),
                    expires_at = COALESCE(EXCLUDED.expires_at, model_edges.expires_at),
                    last_confirmed_at = now(),
                    confirmed_count = model_edges.confirmed_count + 1
              RETURNING id
            )
            SELECT id FROM ins
            UNION ALL
            SELECT id FROM model_edges
              WHERE tenant_id = $2
                AND source_model_id = $3
                AND target_model_id = $4
                AND edge_kind = $5
                AND NOT EXISTS (SELECT 1 FROM ins)
            LIMIT 1
            """,
            new_id,
            tenant_id,
            source,
            target,
            kind,
            weight,
            json.dumps(metadata, sort_keys=True, default=str),
            detected_by,
            created_by_event_id,
            confidence,
            list(evidence_event_ids),
            list(evidence_model_ids),
            explanation,
            review_status,
            decay_after,
            expires_at,
        )
        if row is None:
            raise EdgesRepoError(
                f"edge insert returned no row for ({source}, {target}, {kind})"
            )
        return [row["id"]]

    # =================================================================
    # unlink — DELETE one edge (and its mirror if symmetric)
    # =================================================================
    async def unlink(
        self,
        conn: asyncpg.Connection,
        *,
        source: UUID,
        target: UUID,
        kind: str,
        tenant_id: UUID,
    ) -> int:
        """Hard DELETE. Returns rows-affected. For symmetric kinds,
        deletes both directions in one transactional call.

        Use case: the dual-write diff path inside
        _set_model_relations — when the array shrinks, the
        corresponding edges are unlinked.

        We DELETE rather than mark inert because unlink is for
        "this edge was wrong / no longer exists", not "the
        endpoint was archived". Inert is for archive; unlink is
        for revision.
        """
        # Note: we don't check writable here — unlinking a reserved
        # kind is fine (it just won't find anything since you couldn't
        # have written it). But invalid kind names should still error.
        spec = await resolve_edge_kind_spec(conn, tenant_id=tenant_id, kind=kind)

        if not spec.is_directed:
            count = await conn.fetchval(
                """
                WITH d AS (
                  DELETE FROM model_edges
                  WHERE tenant_id = $1
                    AND edge_kind = $2
                    AND ((source_model_id = $3 AND target_model_id = $4)
                       OR (source_model_id = $4 AND target_model_id = $3))
                  RETURNING 1
                )
                SELECT count(*) FROM d
                """,
                tenant_id,
                kind,
                source,
                target,
            )
        else:
            count = await conn.fetchval(
                """
                WITH d AS (
                  DELETE FROM model_edges
                  WHERE tenant_id = $1
                    AND edge_kind = $2
                    AND source_model_id = $3
                    AND target_model_id = $4
                  RETURNING 1
                )
                SELECT count(*) FROM d
                """,
                tenant_id,
                kind,
                source,
                target,
            )
        if count and int(count) > 0:
            # Retired accepted-memory topology hook.
            await _enqueue_topo_dirty(
                conn,
                model_id=source,
                tenant_id=tenant_id,
                cause_model_id=target,
                hop_depth=0,
            )
            await _enqueue_topo_dirty(
                conn,
                model_id=target,
                tenant_id=tenant_id,
                cause_model_id=source,
                hop_depth=0,
            )
        return int(count or 0)

    async def retire(
        self,
        conn: asyncpg.Connection,
        *,
        source: UUID,
        target: UUID,
        kind: str,
        tenant_id: UUID,
        reason: str,
    ) -> int:
        """Retire an edge without deleting audit history.

        Unlike unlink(), this marks the edge inert and sets
        review_status='retired'. Symmetric kinds retire both mirror rows.
        """
        spec = await resolve_edge_kind_spec(conn, tenant_id=tenant_id, kind=kind)
        if not spec.is_directed:
            rows = await conn.fetch(
                f"""
                UPDATE model_edges
                SET status = 'inert',
                    review_status = 'retired',
                    status_changed_at = now(),
                    status_reason = $5
                WHERE tenant_id = $1
                  AND edge_kind = $2
                  AND status = 'active'
                  AND ((source_model_id = $3 AND target_model_id = $4)
                    OR (source_model_id = $4 AND target_model_id = $3))
                RETURNING {_SELECT_COLS_SQL}
                """,
                tenant_id,
                kind,
                source,
                target,
                reason,
            )
        else:
            rows = await conn.fetch(
                f"""
                UPDATE model_edges
                SET status = 'inert',
                    review_status = 'retired',
                    status_changed_at = now(),
                    status_reason = $5
                WHERE tenant_id = $1
                  AND edge_kind = $2
                  AND source_model_id = $3
                  AND target_model_id = $4
                  AND status = 'active'
                RETURNING {_SELECT_COLS_SQL}
                """,
                tenant_id,
                kind,
                source,
                target,
                reason,
            )
        if rows:
            # Retired accepted-memory topology hook.
            await _enqueue_topo_dirty(
                conn,
                model_id=source,
                tenant_id=tenant_id,
                cause_model_id=target,
                hop_depth=0,
            )
            await _enqueue_topo_dirty(
                conn,
                model_id=target,
                tenant_id=tenant_id,
                cause_model_id=source,
                hop_depth=0,
            )
        return len(rows)

    # =================================================================
    # traverse_forward / traverse_backward
    # =================================================================
    async def traverse_forward(
        self,
        conn: asyncpg.Connection,
        *,
        source: UUID,
        kinds: Sequence[str],
        tenant_id: UUID,
        status: str = "active",
    ) -> list[dict[str, Any]]:
        """Forward edges from `source` matching any of `kinds`. Uses
        partial index `model_edges_source_idx` when status='active'."""
        rows = await conn.fetch(
            f"""
            SELECT {_SELECT_COLS_SQL}
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND edge_kind = ANY($3::text[])
              AND status = $4
            """,
            tenant_id,
            source,
            list(kinds),
            status,
        )
        return [_row_to_dict(r) for r in rows]

    async def traverse_backward(
        self,
        conn: asyncpg.Connection,
        *,
        target: UUID,
        kinds: Sequence[str],
        tenant_id: UUID,
        status: str = "active",
    ) -> list[dict[str, Any]]:
        """Reverse edges into `target` matching any of `kinds`. Uses
        partial index `model_edges_target_idx`. New capability in S1:
        constant-time reverse traversal for every edge_kind, not just
        `supports`."""
        rows = await conn.fetch(
            f"""
            SELECT {_SELECT_COLS_SQL}
            FROM model_edges
            WHERE tenant_id = $1
              AND target_model_id = $2
              AND edge_kind = ANY($3::text[])
              AND status = $4
            """,
            tenant_id,
            target,
            list(kinds),
            status,
        )
        return [_row_to_dict(r) for r in rows]

    # =================================================================
    # mark_inert — lifecycle on Model archive
    # =================================================================
    async def mark_inert(
        self,
        conn: asyncpg.Connection,
        *,
        model_id: UUID,
        tenant_id: UUID,
        reason: str,
    ) -> list[dict[str, Any]]:
        """Set status='inert' on every active edge where `model_id` is
        source or target. Called from ModelsRepo.archive() in the same
        transaction. Returns the rows that flipped (for cascade
        dispatch by the archive path)."""
        rows = await conn.fetch(
            f"""
            UPDATE model_edges
            SET status = 'inert',
                status_changed_at = now(),
                status_reason = $3
            WHERE tenant_id = $1
              AND status = 'active'
              AND (source_model_id = $2 OR target_model_id = $2)
            RETURNING {_SELECT_COLS_SQL}
            """,
            tenant_id,
            model_id,
            reason,
        )
        # S2 topology: every neighbor of the archived Model just lost
        # an active edge to it; enqueue each for topo recompute.
        # Iterate the now-inert edges and enqueue the OTHER endpoint
        # (the archived Model itself doesn't need a recompute — it's
        # being archived).
        for row in rows:
            # Retired accepted-memory topology hook.
            other = (
                row["target_model_id"]
                if row["source_model_id"] == model_id
                else row["source_model_id"]
            )
            await _enqueue_topo_dirty(
                conn,
                model_id=other,
                tenant_id=tenant_id,
                cause_model_id=model_id,
                hop_depth=0,
            )
        return [_row_to_dict(r) for r in rows]

    # =================================================================
    # check_no_cycle — generalized DAG check across cycle_scope
    # =================================================================
    async def check_no_cycle(
        self,
        conn: asyncpg.Connection,
        *,
        kind: str,
        source: UUID,
        targets: Iterable[UUID],
        tenant_id: UUID,
        cycle_scope: frozenset[str] | None | object = _MISSING_CYCLE_SCOPE,
    ) -> None:
        """Reject (source, t) edges for t in `targets` if any would
        create a cycle in the kind's cycle_scope. The scope is a SET
        of edge_kinds that participate jointly: e.g.
        `supports.cycle_scope = {'supports', 'instance_of'}` so a
        Model cannot transitively support its own pattern via either
        edge.

        Self-edge is always rejected (caller validates before this).

        Implementation: recursive CTE over `model_edges` filtered to
        the kinds in cycle_scope. If `source` appears anywhere in the
        ancestor closure of any target, there's a cycle.

        For the empty-scope case (kind not DAG-required), this is a
        no-op.
        """
        if cycle_scope is _MISSING_CYCLE_SCOPE:
            try:
                scope = cycle_scope_for(kind)
            except EdgeRegistryError:
                spec = await resolve_edge_kind_spec(
                    conn,
                    tenant_id=tenant_id,
                    kind=kind,
                )
                scope = spec.cycle_scope
        else:
            scope = cycle_scope
        if scope is None:
            return
        target_list = [t for t in targets if t != source]
        if not target_list:
            return

        # Cycle would form iff `source` is reachable from any target
        # by walking model_edges WHERE edge_kind ∈ scope, treating
        # the edge as source -> target. We climb backward (target
        # becomes the "current node", we look for outgoing edges
        # source_model_id = current_node). If at any depth we land on
        # `source`, the proposed (source, target) would close a
        # cycle.
        row = await conn.fetchrow(
            """
            WITH RECURSIVE descendants AS (
              -- Seed: the proposed targets.
              SELECT id::uuid AS node
              FROM unnest($1::uuid[]) AS t(id)
              UNION
              -- Step: from each known descendant, follow forward
              -- edges within the cycle scope to find further
              -- descendants. If we ever land on `source`, we'd be
              -- creating a cycle by adding (source, target).
              SELECT e.target_model_id
              FROM model_edges e
              JOIN descendants d ON e.source_model_id = d.node
              WHERE e.tenant_id = $3
                AND e.edge_kind = ANY($4::text[])
                AND e.status = 'active'
            )
            SELECT 1 FROM descendants WHERE node = $2 LIMIT 1
            """,
            target_list,
            source,
            tenant_id,
            list(scope),
        )
        if row is not None:
            raise ValidationError(
                f"edge {kind!r} ({source} → targets) would create a "
                f"cycle in scope {sorted(scope)!r}",
                source=str(source),
                targets=[str(t) for t in target_list],
                cycle_scope=sorted(scope),
            )

    # =================================================================
    # get_drift_sample — used by services/workers/edge_drift only
    # =================================================================
    async def get_drift_sample(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        sample_size: int = 200,
    ) -> list[dict[str, Any]]:
        """Return a sample of Models with both their array contents
        AND their corresponding edge counts, so the drift detector
        can flag mismatches.

        Each row: {model_id, supporting_array, supports_edges,
                   instance_of_targets, contributing_array,
                   contributes_edges}

        Where:
          - supporting_array = supporting_model_ids[] on the Model.
            This array carries TWO populations of ids:
              (a) actual supporters — the ids that have an incoming
                  `supports` edge (source=that_id, target=this_model).
              (b) pattern back-links — pattern ids this Model is an
                  instance of (legacy semantics). These are the
                  TARGETS of outgoing `instance_of` edges from
                  this_model.
            So the correct equivalence is:
              supporting_array  ==  supports_edges ∪ instance_of_targets
          - supports_edges = sources of incoming `supports` edges.
          - instance_of_targets = targets of outgoing `instance_of`
            edges from this Model.
          - contributing_array = contributing_models[].
          - contributes_edges = sources of incoming
            `contributes_to_resolution` edges.

        Discrepancy = symmetric difference between (array) and
        (edges union). Drift detector emits a metric on count.
        """
        rows = await conn.fetch(
            """
            WITH sampled AS (
              SELECT id, supporting_model_ids, contributing_models
              FROM models
              WHERE tenant_id = $1 AND status = 'active'
              ORDER BY random()
              LIMIT $2
            )
            SELECT
              s.id AS model_id,
              s.supporting_model_ids AS supporting_array,
              COALESCE(
                ARRAY(
                  SELECT source_model_id FROM model_edges
                  WHERE tenant_id = $1
                    AND target_model_id = s.id
                    AND edge_kind = 'supports'
                    AND status = 'active'
                ),
                '{}'::uuid[]
              ) AS supports_edges,
              COALESCE(
                ARRAY(
                  SELECT target_model_id FROM model_edges
                  WHERE tenant_id = $1
                    AND source_model_id = s.id
                    AND edge_kind = 'instance_of'
                    AND status = 'active'
                ),
                '{}'::uuid[]
              ) AS instance_of_targets,
              s.contributing_models AS contributing_array,
              COALESCE(
                ARRAY(
                  SELECT source_model_id FROM model_edges
                  WHERE tenant_id = $1
                    AND target_model_id = s.id
                    AND edge_kind = 'contributes_to_resolution'
                    AND status = 'active'
                ),
                '{}'::uuid[]
              ) AS contributes_edges
            FROM sampled s
            """,
            tenant_id,
            sample_size,
        )
        return [_row_to_dict(r) for r in rows]


__all__ = ["EdgesRepo", "EdgesRepoError"]
