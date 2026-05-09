"""
lib/shared/edge_registry.py — declarative registry for Model-to-Model
edge kinds.

Single source of truth for per-kind semantics:

  - direction (directed vs symmetric — the latter stored as 2 rows
    kept in sync by EdgesRepo)
  - DAG / cycle scope (a frozenset of edge_kinds that participate
    jointly in cycle prevention; e.g. `supports` and `instance_of`
    together cannot form a cycle, but `superseded_by` only checks
    against itself)
  - weight rules (required / allowed / forbidden per kind)
  - cascade callbacks invoked when either endpoint of an edge is
    archived (responsible for any model_reeval_queue inserts and
    side effects)
  - mutually_exclusive_with (registry-level invariant: e.g. you
    cannot have a `supports` edge and a future `contradicts` edge
    between the same ordered pair)
  - enabled_for_writes (False for kinds reserved in v1 but not yet
    populated; the repo refuses to insert them until flipped)

Why a registry, not per-table or per-column dispatch:
  Adding a new relationship type should be a ~10-line registry
  patch + a producer, not a schema migration + N consumer changes.
  The whole point of the model_edges unification (migration 0031)
  is to push every "what kind of relationship is this?" decision
  out of SQL and into one declarative module.

Cascade callback contract:
  async def cb(
      conn: asyncpg.Connection,
      archived_model_id: UUID,    # the endpoint that was archived
      other_endpoint_id: UUID,    # the other side of the edge
      edge: dict[str, Any],       # the edge row as a dict
      archive_reason: str,        # the ModelArchiveReason enum value
  ) -> None

  The callback is responsible for any model_reeval_queue inserts.
  It receives the FULL edge row so it can read weight, metadata,
  detected_by — useful for cascade strategies that vary by source
  (e.g., `cascade`-detected edges may be ignored to prevent
  infinite reverberation).

Why callbacks live in this module (not in services/models/edges_repo.py):
  Avoids a circular import. EdgesRepo imports the registry to look
  up specs; the registry's callbacks need to enqueue
  model_reeval_queue rows but can do so via raw SQL on `conn`
  without any repo dependency. Keeping callbacks here makes the
  registry self-contained.

See:
  - db/migrations/0031_model_edges.sql
  - services/models/edges_repo.py (the only writer)
  - services/think/deterministic.py (consumer of the cause_kinds we
    enqueue)
  - V1_PR_PROMPTS.md and the plan file for design.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7


# Type alias: signature every cascade callback must satisfy.
CascadeCallback = Callable[
    [asyncpg.Connection, UUID, UUID, dict[str, Any], str],
    Awaitable[None],
]


# ---------------------------------------------------------------------
# Spec dataclass
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class EdgeKindSpec:
    """Declarative specification for one edge_kind.

    Frozen so the registry is effectively immutable post-import.
    Adding a new edge_kind is a code change to this module + a
    producer — never a schema migration.
    """

    name: str

    # Direction. Symmetric edge_kinds (e.g. future `contradicts`)
    # are stored as TWO rows kept in sync by EdgesRepo.link(); every
    # consumer queries `WHERE source = X` without special-casing.
    is_directed: bool

    # Cycle invariant. None = not DAG-required (e.g. symmetric
    # `contradicts`, future `weakens`). Otherwise: the SET of
    # edge_kinds that participate jointly in the DAG check; cycles
    # within this scope are rejected at link time. Two important
    # patterns:
    #   - {"supports", "instance_of"}: a Model cannot transitively
    #     support its own pattern, nor instantiate something it
    #     also supports.
    #   - {"superseded_by"}: chains only; supersession is checked
    #     in isolation from the support graph.
    cycle_scope: frozenset[str] | None

    # Weight rules. Some kinds require a numeric strength (future
    # `contradicts(weight=0.4)` for "in tension"); some allow it
    # (`supports` may carry evidential weight); some forbid it
    # (`superseded_by` is binary by definition). Repo enforces.
    weight_required: bool
    weight_allowed: bool

    # Cascade callbacks. None = no cascade in this direction.
    on_source_archive: CascadeCallback | None
    on_target_archive: CascadeCallback | None

    # Mutually-exclusive-with: registry-level invariant. If a pair
    # (source, target) has any of these kinds, this kind cannot also
    # be added between them. Reserved for future use (e.g. you
    # cannot have BOTH `supports` and `contradicts` between the
    # same pair). Empty in v1.
    mutually_exclusive_with: frozenset[str] = field(default_factory=frozenset)

    # When False, the repo refuses to insert this kind. Reserved
    # names live in the registry so the validator can recognize
    # them; they cannot be written until a producer ships and this
    # flag is flipped.
    enabled_for_writes: bool = True


# ---------------------------------------------------------------------
# Cascade-callback implementations
# ---------------------------------------------------------------------


# Map archive_reason → cause_kind for the supports cascade. This is
# the pre-S1 mapping at services/models/repo.py:_ARCHIVE_REASON_TO_CAUSE_KIND
# moved here so the supports cascade callback owns it. Behavior is
# preserved exactly: the same five cause_kinds, the same default of
# 'supporting_archived' for unrecognized reasons.
_SUPPORTS_REASON_TO_CAUSE_KIND: dict[str, str] = {
    "deprecated": "supporting_deprecated",
    "superseded": "supporting_superseded",
    "falsifier_triggered": "falsifier_triggered_upstream",
    "contested_incorrect": "contested_cluster",
    "contested_reading_incorrect": "contested_cluster",
}


async def _enqueue_reeval(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    dependent_id: UUID,
    cause_model_id: UUID,
    cause_kind: str,
) -> None:
    """Insert a model_reeval_queue row, idempotent against the dedup
    UNIQUE constraint. Used by every cascade callback that wants to
    enqueue a re-eval; centralized so the SQL stays in one place.

    Note: model_reeval_queue's CHECK on cause_kind was dropped in
    migration 0031 because cause_kinds are now declarative (every
    edge_kind contributes its own cascade cause_kind via this
    callback). Validation is the registry's job.
    """
    await conn.execute(
        """
        INSERT INTO model_reeval_queue
          (id, tenant_id, model_id, cause_model_id, cause_kind)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT ON CONSTRAINT model_reeval_queue_dedup
        DO NOTHING
        """,
        uuid7(),
        tenant_id,
        dependent_id,
        cause_model_id,
        cause_kind,
    )


async def _supports_on_source_archive(
    conn: asyncpg.Connection,
    archived_model_id: UUID,
    other_endpoint_id: UUID,
    edge: dict[str, Any],
    archive_reason: str,
) -> None:
    """`supports` edge: source supports target. If source is
    archived, target is the dependent — it loses a support and needs
    re-eval. cause_kind derived from archive_reason via the legacy
    five-value mapping (preserves exact pre-S1 behavior)."""
    cause_kind = _SUPPORTS_REASON_TO_CAUSE_KIND.get(
        archive_reason, "supporting_archived"
    )
    tenant_id = edge.get("tenant_id")
    if tenant_id is None:
        # Defensive — every edge row has a tenant_id; if missing,
        # the row is malformed and we skip rather than crash the
        # archive transaction.
        return
    await _enqueue_reeval(
        conn,
        tenant_id=tenant_id,
        dependent_id=other_endpoint_id,
        cause_model_id=archived_model_id,
        cause_kind=cause_kind,
    )


async def _contributes_on_source_archive(
    conn: asyncpg.Connection,
    archived_model_id: UUID,
    other_endpoint_id: UUID,
    edge: dict[str, Any],
    archive_reason: str,
) -> None:
    """`contributes_to_resolution`: source's state resolves target
    prediction. If source is archived, target prediction loses a
    resolver and needs re-eval. cause_kind = 'contributor_archived'.

    Note: target is a prediction Model. Re-eval here means the
    deterministic T2 handler should reconsider the prediction's
    confidence given that one of its contributors no longer exists.
    """
    tenant_id = edge.get("tenant_id")
    if tenant_id is None:
        return
    await _enqueue_reeval(
        conn,
        tenant_id=tenant_id,
        dependent_id=other_endpoint_id,
        cause_model_id=archived_model_id,
        cause_kind="contributor_archived",
    )


async def _instance_of_on_source_archive(
    conn: asyncpg.Connection,
    archived_model_id: UUID,
    other_endpoint_id: UUID,
    edge: dict[str, Any],
    archive_reason: str,
) -> None:
    """`instance_of`: source is an instance of target pattern. If an
    instance is archived, the pattern's evidence base shrinks
    slightly — enqueue the pattern for re-eval with cause_kind
    'instance_archived'. Mild nudge."""
    tenant_id = edge.get("tenant_id")
    if tenant_id is None:
        return
    await _enqueue_reeval(
        conn,
        tenant_id=tenant_id,
        dependent_id=other_endpoint_id,  # the pattern
        cause_model_id=archived_model_id,  # the instance
        cause_kind="instance_archived",
    )


async def _instance_of_on_target_archive(
    conn: asyncpg.Connection,
    archived_model_id: UUID,
    other_endpoint_id: UUID,
    edge: dict[str, Any],
    archive_reason: str,
) -> None:
    """`instance_of`: if the pattern (target) is archived, every
    instance loses its categorization — enqueue the instance for
    re-eval with cause_kind 'pattern_archived'. Stronger nudge than
    instance_archived because the categorization is gone."""
    tenant_id = edge.get("tenant_id")
    if tenant_id is None:
        return
    await _enqueue_reeval(
        conn,
        tenant_id=tenant_id,
        dependent_id=other_endpoint_id,  # the instance
        cause_model_id=archived_model_id,  # the pattern
        cause_kind="pattern_archived",
    )


# `superseded_by` has no cascade in either direction:
#   - When source is archived (the superseded one): supports cascade
#     handles dependents via the supports edges from source. The
#     superseded_by edge itself just records the replacement; no
#     additional re-eval needed.
#   - When target is archived (the replacement): unusual scenario;
#     the supersession audit row stays, but no programmatic cascade.


# ---------------------------------------------------------------------
# THE REGISTRY
# ---------------------------------------------------------------------


# v1 enables writes for `supports`, `contributes_to_resolution`,
# `instance_of`, `superseded_by`. The reserved kinds (`contradicts`,
# `weakens`) live here so the validator recognizes them but the repo
# refuses to insert them until producers ship.
EDGE_REGISTRY: dict[str, EdgeKindSpec] = {
    "supports": EdgeKindSpec(
        name="supports",
        is_directed=True,
        cycle_scope=frozenset({"supports", "instance_of"}),
        weight_required=False,
        weight_allowed=True,
        on_source_archive=_supports_on_source_archive,
        on_target_archive=None,
    ),
    "contributes_to_resolution": EdgeKindSpec(
        name="contributes_to_resolution",
        is_directed=True,
        cycle_scope=frozenset({"contributes_to_resolution"}),
        weight_required=False,
        weight_allowed=False,
        on_source_archive=_contributes_on_source_archive,
        on_target_archive=None,
    ),
    "instance_of": EdgeKindSpec(
        name="instance_of",
        is_directed=True,
        cycle_scope=frozenset({"supports", "instance_of"}),
        weight_required=False,
        weight_allowed=False,
        on_source_archive=_instance_of_on_source_archive,
        on_target_archive=_instance_of_on_target_archive,
    ),
    "superseded_by": EdgeKindSpec(
        name="superseded_by",
        is_directed=True,
        cycle_scope=frozenset({"superseded_by"}),
        weight_required=False,
        weight_allowed=False,
        on_source_archive=None,
        on_target_archive=None,
    ),
    # Reserved — defined so the validator recognizes the name, but
    # repo refuses to write them until producers ship.
    "contradicts": EdgeKindSpec(
        name="contradicts",
        is_directed=False,
        cycle_scope=None,
        weight_required=True,
        weight_allowed=True,
        on_source_archive=None,  # stage-4 ships polarity-inverted callback
        on_target_archive=None,
        enabled_for_writes=False,
    ),
    "weakens": EdgeKindSpec(
        name="weakens",
        is_directed=True,
        cycle_scope=None,
        weight_required=True,
        weight_allowed=True,
        on_source_archive=None,
        on_target_archive=None,
        enabled_for_writes=False,
    ),
}


# ---------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------


class EdgeRegistryError(Exception):
    """Raised on registry-validation failures: unknown kind, write to
    a reserved kind, mutually-exclusive violation, weight rule
    violation. Distinct from ValidationError so callers can
    distinguish substrate-shape errors from content errors."""


def get_spec(kind: str) -> EdgeKindSpec:
    """Look up the spec for an edge_kind. Raises EdgeRegistryError
    on unknown kinds — never returns None, so callers don't have to
    handle the missing case."""
    spec = EDGE_REGISTRY.get(kind)
    if spec is None:
        raise EdgeRegistryError(
            f"unknown edge_kind {kind!r}; legal: {sorted(EDGE_REGISTRY.keys())}"
        )
    return spec


def assert_writable(kind: str) -> EdgeKindSpec:
    """Look up the spec AND assert the kind is currently writable.
    Used by EdgesRepo.link() to reject reserved kinds in v1."""
    spec = get_spec(kind)
    if not spec.enabled_for_writes:
        raise EdgeRegistryError(
            f"edge_kind {kind!r} is reserved (no producer in v1); "
            f"writes will be enabled when stage-4 (contradicts) or "
            f"a future weakens producer ships"
        )
    return spec


def validate_weight(kind: str, weight: float | None) -> None:
    """Enforce per-kind weight rules. None is treated as 'no
    weight'; numeric values must be in [0, 1]. Repo calls this
    before INSERT."""
    spec = get_spec(kind)
    if weight is None:
        if spec.weight_required:
            raise EdgeRegistryError(
                f"edge_kind {kind!r} requires a weight; got None"
            )
        return
    if not spec.weight_allowed:
        raise EdgeRegistryError(
            f"edge_kind {kind!r} forbids weight; got {weight}"
        )
    try:
        w = float(weight)
    except (TypeError, ValueError) as e:
        raise EdgeRegistryError(
            f"edge_kind {kind!r} weight must be numeric; got {weight!r}"
        ) from e
    if not (0.0 <= w <= 1.0):
        raise EdgeRegistryError(
            f"edge_kind {kind!r} weight out of range [0, 1]: {w}"
        )


def cycle_scope_for(kind: str) -> frozenset[str] | None:
    """Cycle-check scope for the kind, or None if not DAG-required.
    Used by EdgesRepo._check_no_cycle."""
    return get_spec(kind).cycle_scope


def is_symmetric(kind: str) -> bool:
    """True if the kind is symmetric (stored as 2 rows kept in sync
    by the repo helper). Used by link() to decide whether to insert
    one or two rows."""
    return not get_spec(kind).is_directed


def writable_kinds() -> frozenset[str]:
    """The set of edge_kinds the repo will currently accept on
    INSERT. Used by tests + the drift detector to know what to
    expect in dual-write."""
    return frozenset(
        name for name, spec in EDGE_REGISTRY.items() if spec.enabled_for_writes
    )


def legacy_supports_cause_kind(archive_reason: str) -> str:
    """Public accessor for the supports-cascade reason→cause_kind
    mapping. Used by the dual-write safety-net path in
    ModelsRepo.archive() when walking the legacy
    `supporting_model_ids` array (catches dependents whose typed
    `supports` edge hasn't been backfilled yet). Same five-value
    taxonomy the pre-S1 code used, with a default of
    'supporting_archived' for unrecognized reasons.
    """
    return _SUPPORTS_REASON_TO_CAUSE_KIND.get(
        archive_reason, "supporting_archived"
    )


__all__ = [
    "CascadeCallback",
    "EdgeKindSpec",
    "EDGE_REGISTRY",
    "EdgeRegistryError",
    "get_spec",
    "assert_writable",
    "validate_weight",
    "cycle_scope_for",
    "is_symmetric",
    "writable_kinds",
    "legacy_supports_cause_kind",
]
