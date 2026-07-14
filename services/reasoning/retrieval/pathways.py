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

Why this module does not own the pool: retrieval composes with the
caller's connection and transaction boundary. Inferential Think calls it
before the short mutation transaction; authoritative deterministic Think
may call it inside the legacy wide transaction so pre-commit state remains
visible to deterministic handlers.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Sequence
from uuid import UUID

import asyncpg

from lib.embeddings.ollama import EMBEDDING_DIM, OllamaClient, OllamaError
from lib.observability import counter, histogram
from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.types import (
    CommitmentRow,
    DecisionRow,
    GoalRow,
    ModelRow,
    ObservationRow,
    ResourceRow,
)
from services.domain.models.read_shapes import (
    MODEL_ROW_SELECT_COLS,
    MODEL_ROW_SELECT_SQL,
    hydrate_model_row,
)
from services.domain.models.semantic_terms import derive_query_semantic_terms
from services.reasoning.retrieval.read_fanout import ReadFanoutBudget


# ---------------------------------------------------------------------
# Constants + types
# ---------------------------------------------------------------------

PathwayName = Literal["A", "B", "C", "D", "G", "L"]

_DEFAULT_K_SEMANTIC = 40
_DEFAULT_TEMPORAL_WINDOW_DAYS = 7
_DEFAULT_STRUCTURAL_MAX_HOPS = 2
_STRUCTURAL_MAX_MODELS = 200
_STRUCTURAL_MODELS_PER_SCOPE_ENTITY = 32
_STRUCTURAL_MODELS_PER_SCOPE_ACTOR = 48
_STRUCTURAL_MAX_SCOPE_ENTITY_FILTERS = 64
_TEMPORAL_MAX_OBSERVATIONS = 300
_PATTERN_MAX_INSTANCES = 200
_TAG_RESCUE_LIMIT = 80
_SEMANTIC_TERMS_LIMIT = 80
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
_TAGIFY_RE = re.compile(r"[^a-z0-9_]+")
_REPRESENTATION_TAG_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "progress_signal",
        (
            "started",
            "picked up",
            "raised",
            "opened",
            "merged",
            "shipped",
            "completed",
            "pr",
            "pull request",
        ),
    ),
    ("review_loop", ("review", "feedback", "comment", "approval", "approved")),
    (
        "delivery_risk",
        ("risk", "blocked", "blocker", "stalled", "slip", "delay", "missing"),
    ),
    (
        "coordination_debt",
        ("handoff", "waiting", "unclear", "owner", "follow up", "follow-up"),
    ),
    ("deployment_activity", ("deploy", "release", "rollback", "staging", "production")),
    (
        "finance_flow",
        ("invoice", "bill", "payment", "vendor", "runway", "budget", "transaction"),
    ),
    (
        "operational_churn",
        ("alert", "latency", "error", "aws", "lambda", "incident", "disk", "5xx"),
    ),
    (
        "decision_pressure",
        ("decision", "revisited", "approved", "rejected", "exception"),
    ),
    (
        "contextual_recurrence",
        ("repeat", "repeated", "recurring", "cadence", "again", "same pattern"),
    ),
    ("source_code", ("github", "gitlab", "jira")),
    ("source_chat", ("slack", "telegram", "discord", "signal")),
    (
        "source_docs",
        ("notion", "drive", "gmail", "calendar", "fireflies", "miro", "figma"),
    ),
    (
        "source_finance",
        ("quickbooks", "ramp", "brex", "mercury", "deel", "carta", "gusto"),
    ),
    ("source_observability", ("aws", "grafana", "cloudwatch")),
    ("source_people", ("ashby", "hibob", "linkedin")),
)


class RetrievalPathwayError(CompanyOSError):
    default_code = "retrieval_pathway_error"


@dataclass(frozen=True)
class _SidecarFanoutRows:
    rows: list[asyncpg.Record]
    fanout_chunks: int = 0
    deferred_chunks: int = 0


@dataclass(frozen=True)
class _PathwayBExactCandidate:
    id: UUID
    activation: float
    embedding: list[float] | None


# Retrieval pathway Prometheus families (exposed by the worker /metrics).
_INNER_DURATION = histogram(
    "retrieval_pathway_inner_seconds",
    "Intra-pathway stage latency (graph walk, hydration, fallbacks).",
    ("stage",),
)
_PGVECTOR_DURATION = histogram(
    "retrieval_pgvector_query_seconds",
    "pgvector ANN / exact-rank query latency in pathway B.",
    ("strategy",),
)
_PGVECTOR_QUERIES = counter(
    "retrieval_pgvector_queries_total",
    "pgvector queries executed in pathway B (ann | exact_fallback).",
    ("strategy",),
)


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def _append_timing(
    notes: dict[str, Any],
    stage: str,
    started: float,
    **extra: Any,
) -> None:
    elapsed_ms = _elapsed_ms(started)
    timing = {
        "stage": stage,
        "elapsed_ms": elapsed_ms,
    }
    for key, value in extra.items():
        if value is not None:
            timing[key] = value
    notes.setdefault("timings", []).append(timing)
    # Prometheus twin — `stage` is a bounded literal set (graph_walk,
    # act_row_fetch, sidecar_*, jsonb_fallback_*, …).
    _INNER_DURATION.observe(elapsed_ms / 1000.0, stage=stage)


_MODEL_SELECT_COLS = MODEL_ROW_SELECT_COLS
_MODEL_SELECT_SQL = MODEL_ROW_SELECT_SQL

_OBS_SELECT_COLS = (
    "id",
    "tenant_id",
    "occurred_at",
    "ingested_at",
    "kind",
    "source_channel",
    "source_actor_ref",
    "actor_id",
    "content",
    "content_text",
    "embedding",
    "embedding_pending",
    "trust_tier",
    "external_id",
    "cause_id",
    "sequence_num",
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


@dataclass(frozen=True, slots=True)
class ModelCandidateHit:
    """Lightweight model candidate before full ModelRow hydration."""

    model_id: UUID
    activation: float = 0.0
    match_count: int = 0
    first_rank: int = 10_000


# ---------------------------------------------------------------------
# Row hydration helpers
#
# These duplicate the hydration in models/repo.py and observations/repo.py
# because those methods are not reusable from outside (they are private
# to the repo). Duplication is intentional and documented; a refactor
# to hoist into lib/shared is a Wave 5 nice-to-have.
# ---------------------------------------------------------------------


def _hydrate_model(record: asyncpg.Record) -> ModelRow:
    return hydrate_model_row(
        record,
        drop_internal_fields=True,
        null_invalid_embedding=True,
        use_vector_to_list=True,
    )


async def hydrate_active_models_by_ids(
    tenant_id: UUID,
    conn: asyncpg.Connection,
    model_ids: Sequence[UUID],
    *,
    notes: dict[str, Any] | None = None,
    bucket: str = "models",
) -> list[ModelRow]:
    ordered_ids = [model_id for model_id in model_ids if model_id is not None]
    if not ordered_ids:
        return []
    rows = await conn.fetch(
        f"""
        SELECT {_MODEL_SELECT_SQL}
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        ordered_ids,
    )
    hydration_notes = notes if notes is not None else {}
    models = _hydrate_many(rows, _hydrate_model, hydration_notes, bucket)
    by_id = {model.id: model for model in models}
    return [by_id[model_id] for model_id in ordered_ids if model_id in by_id]


def _candidate_hits_from_rows(
    rows: Sequence[asyncpg.Record],
    *,
    match_key: str,
    first_rank_key: str,
) -> list[ModelCandidateHit]:
    hits: list[ModelCandidateHit] = []
    for index, row in enumerate(rows, start=1):
        hits.append(
            ModelCandidateHit(
                model_id=row["model_id"],
                activation=float(row["activation"] or 0.0),
                match_count=int(row[match_key] or 0),
                first_rank=int(row[first_rank_key] or index),
            )
        )
    return hits


def _model_temporally_valid(model: ModelRow, *, now: datetime | None = None) -> bool:
    scope = model.scope_temporal or {}
    if not isinstance(scope, dict):
        return True
    raw_until = scope.get("valid_until")
    if raw_until in (None, ""):
        return True
    if isinstance(raw_until, datetime):
        valid_until = raw_until
    elif isinstance(raw_until, str):
        try:
            valid_until = datetime.fromisoformat(raw_until.replace("Z", "+00:00"))
        except ValueError:
            return True
    else:
        return True
    if valid_until.tzinfo is None:
        valid_until = valid_until.replace(tzinfo=timezone.utc)
    return valid_until > (now or datetime.now(timezone.utc))


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


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _record_get(row: Any, key: str) -> Any:
    try:
        return row[key]
    except (KeyError, TypeError, IndexError):
        return getattr(row, key, None)


def _tagify(value: Any) -> str:
    text = str(value or "").strip().casefold()
    return _TAGIFY_RE.sub("_", text).strip("_")


def _seed_representation_tags(
    seed_text: str | None,
    seed_signature: dict[str, Any] | None = None,
) -> list[str]:
    """Derive deep retrieval tags from trigger text/signature.

    These are intentionally not actor names or surface nouns. They are
    operating-shape hooks that line up with the tags Think now stores on
    models.
    """
    parts = [seed_text or ""]
    if isinstance(seed_signature, dict):
        parts.append(json.dumps(seed_signature, sort_keys=True, default=str))
    text = " ".join(parts).casefold()
    tags: list[str] = []
    seen: set[str] = set()
    for tag, needles in _REPRESENTATION_TAG_RULES:
        if any(needle in text for needle in needles):
            normalized = _tagify(tag)
            if normalized and normalized not in seen:
                seen.add(normalized)
                tags.append(normalized)
    if "pattern" in text or "recurrence" in text:
        for tag in ("discovered_pattern", "contextual_recurrence"):
            if tag not in seen:
                seen.add(tag)
                tags.append(tag)
    return tags


def _coverage_roles_from_seed_tags(tags: Sequence[str]) -> list[str]:
    roles: list[str] = []
    seen: set[str] = set()
    role_map = {
        "progress_signal": "workstream",
        "review_loop": "workstream",
        "delivery_risk": "state",
        "coordination_debt": "relationship",
        "deployment_activity": "state",
        "finance_flow": "state",
        "operational_churn": "state",
        "decision_pressure": "state",
        "contextual_recurrence": "discovered_pattern",
        "source_code": "source",
        "source_chat": "source",
        "source_docs": "source",
        "source_finance": "source",
        "source_observability": "source",
        "source_people": "source",
        "discovered_pattern": "discovered_pattern",
    }
    for tag in tags:
        for role in (role_map.get(tag), tag.removeprefix("coverage_")):
            if not role or role == tag or role in seen:
                continue
            seen.add(role)
            roles.append(role)
    return roles


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
    return 1.0 - (dot / ((na**0.5) * (nb**0.5)))


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
    expired = 0
    for r in records:
        try:
            item = hydrate_fn(r)
            if isinstance(item, ModelRow) and not _model_temporally_valid(item):
                expired += 1
                continue
            out.append(item)
        except Exception:
            skipped += 1
    if skipped:
        notes.setdefault("hydration_skipped", {})[bucket] = skipped
    if expired:
        notes.setdefault("expired_scope_temporal_skipped", {})[bucket] = expired
    return out


# =====================================================================
# Pathway A — Structural proximity (graph walk over Acts edges)
# =====================================================================


_SEED_ENTITY_TYPES = frozenset(
    {
        "commitment",
        "goal",
        "decision",
        "actor",
        "customer",
        "customer_resource",
        "resource",
    }
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


@dataclass(slots=True)
class _PathwayASeeds:
    seeds: dict[str, set[UUID]]
    direct_seed_entity_pairs: set[tuple[str, UUID]]
    accepted_count: int


@dataclass(slots=True)
class _PathwayAWalkResult:
    visited_commits: set[UUID]
    visited_goals: set[UUID]
    visited_decisions: set[UUID]
    visited_actors: set[UUID]
    visited_customers: set[UUID]
    visited_resources: set[UUID]
    hops_executed: int


@dataclass(slots=True)
class _PathwayAEntityRows:
    goals: list[GoalRow]
    commitments: list[CommitmentRow]
    decisions: list[DecisionRow]
    resources: list[ResourceRow]


@dataclass(slots=True)
class _PathwayAHopExpansion:
    commits: set[UUID] = field(default_factory=set)
    goals: set[UUID] = field(default_factory=set)
    decisions: set[UUID] = field(default_factory=set)
    customers: set[UUID] = field(default_factory=set)


def _parse_pathway_a_seeds(
    seed_entity_ids: Sequence[dict[str, Any]],
    notes: dict[str, Any],
) -> _PathwayASeeds:
    seeds: dict[str, set[UUID]] = {k: set() for k in _SEED_ENTITY_TYPES}
    for raw in seed_entity_ids:
        if not isinstance(raw, dict):
            continue
        seed_type = _canonical_seed_type(raw.get("type"))
        raw_id = raw.get("id")
        if seed_type is None or raw_id is None:
            continue
        try:
            seeds[seed_type].add(UUID(str(raw_id)))
        except (ValueError, TypeError):
            continue

    direct_pairs: set[tuple[str, UUID]] = set()
    for direct_type in ("commitment", "goal", "decision", "resource"):
        for direct_id in seeds[direct_type]:
            direct_pairs.add((direct_type, direct_id))
    for direct_id in seeds["customer_resource"]:
        direct_pairs.add(("customer", direct_id))
        direct_pairs.add(("customer_resource", direct_id))
        direct_pairs.add(("resource", direct_id))

    notes["seeds_by_type"] = {k: len(v) for k, v in seeds.items() if v}
    accepted_count = sum(len(v) for v in seeds.values())
    notes["seeds_accepted"] = accepted_count
    return _PathwayASeeds(
        seeds=seeds,
        direct_seed_entity_pairs=direct_pairs,
        accepted_count=accepted_count,
    )


def _chunked(values: Sequence[Any], size: int) -> list[list[Any]]:
    chunk_size = max(1, int(size))
    return [list(values[i : i + chunk_size]) for i in range(0, len(values), chunk_size)]


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


def _dedupe_scope_entity_filters(
    filters: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in filters:
        key = _jsonb(entry)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return deduped


async def _build_pathway_a_scope_entity_filters(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    visited_commits: set[UUID],
    visited_goals: set[UUID],
    visited_decisions: set[UUID],
    visited_customers: set[UUID],
    visited_resources: set[UUID],
    direct_seed_entity_pairs: set[tuple[str, UUID]],
) -> tuple[list[dict[str, Any]], int, int]:
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
            scope_entity_filters.append({"type": "customer_resource", "id": str(cid)})
            scope_entity_filters.append({"type": "resource", "id": str(cid)})

    scope_entity_filters = _dedupe_scope_entity_filters(scope_entity_filters)
    filters_before_cap = len(scope_entity_filters)
    capped, filters_dropped_by_cap = _cap_scope_entity_filters(
        scope_entity_filters,
        direct_seed_entity_pairs=direct_seed_entity_pairs,
    )
    return capped, filters_before_cap, filters_dropped_by_cap


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
    return ranked[: max(1, int(limit))]


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
    per_seed_cap = max(1, int(per_seed_limit))
    rows = await conn.fetch(
        f"""
        WITH seeds AS MATERIALIZED (
          SELECT seed.entity_type::text,
                 seed.entity_id::uuid,
                 seed.seed_order::int,
                 seed.seed_priority::int
          FROM unnest(
            $2::text[],
            $3::uuid[],
            $4::int[],
            $5::int[]
          ) AS seed(entity_type, entity_id, seed_order, seed_priority)
        ),
        ranked AS MATERIALIZED (
          SELECT seeds.seed_priority AS _seed_priority,
                 seeds.seed_order AS _seed_order,
                 (
                   row_number() OVER (
                     PARTITION BY seeds.seed_order
                     ORDER BY model_rows.activation DESC,
                              model_rows.created_at DESC,
                              model_rows.id
                   ) - 1
                 )::int AS _local_rank,
                 model_rows.*
          FROM seeds
          JOIN model_scope_entities mse
            ON mse.tenant_id = $1
           AND mse.entity_type = seeds.entity_type
           AND mse.entity_id = seeds.entity_id
          JOIN LATERAL (
            SELECT {_MODEL_SELECT_SQL}
            FROM models
            WHERE models.id = mse.model_id
              AND models.tenant_id = $1
              AND models.status = 'active'
            LIMIT 1
          ) model_rows ON TRUE
        )
        SELECT *
        FROM ranked
        WHERE _local_rank < $6
        ORDER BY _seed_priority ASC,
                 _local_rank ASC,
                 _seed_order ASC,
                 activation DESC,
                 id ASC
        LIMIT $7
        """,
        tenant_id,
        [str(entity_type) for entity_type in entity_types],
        list(entity_ids),
        [int(order) for order in entity_orders],
        [int(priority) for priority in entity_priorities],
        per_seed_cap,
        max(1, int(global_limit)),
    )
    return _rank_sidecar_records(rows, limit=global_limit)


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
    per_seed_cap = max(1, int(per_seed_limit))
    rows = await conn.fetch(
        f"""
        WITH seeds AS MATERIALIZED (
          SELECT seed.actor_id::uuid,
                 seed.seed_order::int
          FROM unnest($2::uuid[], $3::int[])
            AS seed(actor_id, seed_order)
        ),
        ranked AS MATERIALIZED (
          SELECT 1::int AS _seed_priority,
                 seeds.seed_order AS _seed_order,
                 (
                   row_number() OVER (
                     PARTITION BY seeds.seed_order
                     ORDER BY model_rows.activation DESC,
                              model_rows.created_at DESC,
                              model_rows.id
                   ) - 1
                 )::int AS _local_rank,
                 model_rows.*
          FROM seeds
          JOIN model_scope_actors msa
            ON msa.tenant_id = $1
           AND msa.actor_id = seeds.actor_id
          JOIN LATERAL (
            SELECT {_MODEL_SELECT_SQL}
            FROM models
            WHERE models.id = msa.model_id
              AND models.tenant_id = $1
              AND models.status = 'active'
            LIMIT 1
          ) model_rows ON TRUE
        )
        SELECT *
        FROM ranked
        WHERE _local_rank < $4
        ORDER BY _local_rank ASC,
                 _seed_order ASC,
                 activation DESC,
                 id ASC
        LIMIT $5
        """,
        tenant_id,
        list(actor_ids),
        [int(order) for order in actor_orders],
        per_seed_cap,
        max(1, int(global_limit)),
    )
    ranked = sorted(
        rows,
        key=lambda row: (
            int(_record_value(row, "_local_rank", 0) or 0),
            int(_record_value(row, "_seed_order", 0) or 0),
            -float(_record_value(row, "activation", 0.0) or 0.0),
            str(_record_value(row, "id", "") or ""),
        ),
    )
    return ranked[: max(1, int(global_limit))]


async def _fetch_pathway_a_entity_sidecar_rows_fanout(
    read_pool: asyncpg.Pool,
    *,
    fallback_conn: asyncpg.Connection | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
    tenant_id: UUID,
    entity_types: Sequence[str],
    entity_ids: Sequence[UUID],
    entity_orders: Sequence[int],
    entity_priorities: Sequence[int],
    per_seed_limit: int,
    global_limit: int,
    chunk_size: int,
) -> _SidecarFanoutRows:
    type_chunks = _chunked(entity_types, chunk_size)
    id_chunks = _chunked(entity_ids, chunk_size)
    order_chunks = _chunked(entity_orders, chunk_size)
    priority_chunks = _chunked(entity_priorities, chunk_size)
    chunk_args = list(zip(type_chunks, id_chunks, order_chunks, priority_chunks))

    async def fetch_chunk(
        chunk_index: int,
        types: list[str],
        ids: list[UUID],
        orders: list[int],
        priorities: list[int],
    ) -> tuple[str, int, list[asyncpg.Record]]:
        if read_fanout_budget is not None:
            async with read_fanout_budget.connection_if_available() as fanout_conn:
                if fanout_conn is None:
                    return ("deferred", chunk_index, [])
                return (
                    "fanout",
                    chunk_index,
                    await _fetch_pathway_a_entity_sidecar_rows(
                        fanout_conn,
                        tenant_id=tenant_id,
                        entity_types=types,
                        entity_ids=ids,
                        entity_orders=orders,
                        entity_priorities=priorities,
                        per_seed_limit=per_seed_limit,
                        global_limit=min(
                            global_limit,
                            per_seed_limit * max(1, len(ids)),
                        ),
                    ),
                )
        async with read_pool.acquire() as fanout_conn:
            return (
                "fanout",
                chunk_index,
                await _fetch_pathway_a_entity_sidecar_rows(
                    fanout_conn,
                    tenant_id=tenant_id,
                    entity_types=types,
                    entity_ids=ids,
                    entity_orders=orders,
                    entity_priorities=priorities,
                    per_seed_limit=per_seed_limit,
                    global_limit=min(global_limit, per_seed_limit * max(1, len(ids))),
                ),
            )

    chunk_results = await asyncio.gather(
        *[
            fetch_chunk(chunk_index, types, ids, orders, priorities)
            for chunk_index, (types, ids, orders, priorities) in enumerate(chunk_args)
        ]
    )

    rows: list[asyncpg.Record] = []
    deferred_indices: list[int] = []
    fanout_chunks = 0
    for status, chunk_index, chunk_rows in chunk_results:
        if status == "deferred":
            deferred_indices.append(chunk_index)
            continue
        fanout_chunks += 1
        rows.extend(chunk_rows)

    for chunk_index in deferred_indices:
        types, ids, orders, priorities = chunk_args[chunk_index]
        if fallback_conn is not None:
            rows.extend(
                await _fetch_pathway_a_entity_sidecar_rows(
                    fallback_conn,
                    tenant_id=tenant_id,
                    entity_types=types,
                    entity_ids=ids,
                    entity_orders=orders,
                    entity_priorities=priorities,
                    per_seed_limit=per_seed_limit,
                    global_limit=min(global_limit, per_seed_limit * max(1, len(ids))),
                )
            )
        elif read_fanout_budget is not None:
            async with read_fanout_budget.connection() as fanout_conn:
                rows.extend(
                    await _fetch_pathway_a_entity_sidecar_rows(
                        fanout_conn,
                        tenant_id=tenant_id,
                        entity_types=types,
                        entity_ids=ids,
                        entity_orders=orders,
                        entity_priorities=priorities,
                        per_seed_limit=per_seed_limit,
                        global_limit=min(
                            global_limit,
                            per_seed_limit * max(1, len(ids)),
                        ),
                    )
                )
                fanout_chunks += 1
        else:
            async with read_pool.acquire() as fanout_conn:
                rows.extend(
                    await _fetch_pathway_a_entity_sidecar_rows(
                        fanout_conn,
                        tenant_id=tenant_id,
                        entity_types=types,
                        entity_ids=ids,
                        entity_orders=orders,
                        entity_priorities=priorities,
                        per_seed_limit=per_seed_limit,
                        global_limit=min(
                            global_limit,
                            per_seed_limit * max(1, len(ids)),
                        ),
                    )
                )
                fanout_chunks += 1
    return _SidecarFanoutRows(
        rows=_rank_sidecar_records(
            rows,
            limit=global_limit,
        ),
        fanout_chunks=fanout_chunks,
        deferred_chunks=len(deferred_indices),
    )


async def _fetch_pathway_a_actor_sidecar_rows_fanout(
    read_pool: asyncpg.Pool,
    *,
    fallback_conn: asyncpg.Connection | None = None,
    read_fanout_budget: ReadFanoutBudget | None = None,
    tenant_id: UUID,
    actor_ids: Sequence[UUID],
    actor_orders: Sequence[int],
    per_seed_limit: int,
    global_limit: int,
    chunk_size: int,
) -> _SidecarFanoutRows:
    id_chunks = _chunked(actor_ids, chunk_size)
    order_chunks = _chunked(actor_orders, chunk_size)
    chunk_args = list(zip(id_chunks, order_chunks))

    async def fetch_chunk(
        chunk_index: int,
        ids: list[UUID],
        orders: list[int],
    ) -> tuple[str, int, list[asyncpg.Record]]:
        if read_fanout_budget is not None:
            async with read_fanout_budget.connection_if_available() as fanout_conn:
                if fanout_conn is None:
                    return ("deferred", chunk_index, [])
                return (
                    "fanout",
                    chunk_index,
                    await _fetch_pathway_a_actor_sidecar_rows(
                        fanout_conn,
                        tenant_id=tenant_id,
                        actor_ids=ids,
                        actor_orders=orders,
                        per_seed_limit=per_seed_limit,
                        global_limit=min(
                            global_limit,
                            per_seed_limit * max(1, len(ids)),
                        ),
                    ),
                )
        async with read_pool.acquire() as fanout_conn:
            return (
                "fanout",
                chunk_index,
                await _fetch_pathway_a_actor_sidecar_rows(
                    fanout_conn,
                    tenant_id=tenant_id,
                    actor_ids=ids,
                    actor_orders=orders,
                    per_seed_limit=per_seed_limit,
                    global_limit=min(global_limit, per_seed_limit * max(1, len(ids))),
                ),
            )

    chunk_results = await asyncio.gather(
        *[
            fetch_chunk(chunk_index, ids, orders)
            for chunk_index, (ids, orders) in enumerate(chunk_args)
        ]
    )

    rows: list[asyncpg.Record] = []
    deferred_indices: list[int] = []
    fanout_chunks = 0
    for status, chunk_index, chunk_rows in chunk_results:
        if status == "deferred":
            deferred_indices.append(chunk_index)
            continue
        fanout_chunks += 1
        rows.extend(chunk_rows)

    for chunk_index in deferred_indices:
        ids, orders = chunk_args[chunk_index]
        if fallback_conn is not None:
            rows.extend(
                await _fetch_pathway_a_actor_sidecar_rows(
                    fallback_conn,
                    tenant_id=tenant_id,
                    actor_ids=ids,
                    actor_orders=orders,
                    per_seed_limit=per_seed_limit,
                    global_limit=min(global_limit, per_seed_limit * max(1, len(ids))),
                )
            )
        elif read_fanout_budget is not None:
            async with read_fanout_budget.connection() as fanout_conn:
                rows.extend(
                    await _fetch_pathway_a_actor_sidecar_rows(
                        fanout_conn,
                        tenant_id=tenant_id,
                        actor_ids=ids,
                        actor_orders=orders,
                        per_seed_limit=per_seed_limit,
                        global_limit=min(
                            global_limit,
                            per_seed_limit * max(1, len(ids)),
                        ),
                    )
                )
                fanout_chunks += 1
        else:
            async with read_pool.acquire() as fanout_conn:
                rows.extend(
                    await _fetch_pathway_a_actor_sidecar_rows(
                        fanout_conn,
                        tenant_id=tenant_id,
                        actor_ids=ids,
                        actor_orders=orders,
                        per_seed_limit=per_seed_limit,
                        global_limit=min(
                            global_limit,
                            per_seed_limit * max(1, len(ids)),
                        ),
                    )
                )
                fanout_chunks += 1
    return _SidecarFanoutRows(
        rows=_rank_sidecar_records(
            rows,
            limit=global_limit,
        ),
        fanout_chunks=fanout_chunks,
        deferred_chunks=len(deferred_indices),
    )


async def _append_pathway_a_jsonb_fallback_models(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    scope_entity_filters: list[dict[str, Any]],
    visited_actors: set[UUID],
    entity_sidecar_rows: list[asyncpg.Record],
    actor_sidecar_rows: list[asyncpg.Record],
    seen_ids: set[UUID],
    notes: dict[str, Any],
    models_out: list[ModelRow],
) -> None:
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
    if not clauses:
        notes["scope_jsonb_fallback_used"] = False
        notes["scope_jsonb_rows"] = 0
        _append_timing(
            notes,
            "jsonb_fallback_query",
            time.perf_counter(),
            skipped=True,
            clauses=0,
        )
        return

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
            model = _hydrate_model(r)
            if not _model_temporally_valid(model):
                notes.setdefault("expired_scope_temporal_skipped", {}).setdefault(
                    "models", 0
                )
                notes["expired_scope_temporal_skipped"]["models"] += 1
                continue
            models_out.append(model)
        except Exception:
            notes.setdefault("hydration_skipped", {}).setdefault("models", 0)
            notes["hydration_skipped"]["models"] += 1
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


async def _fetch_pathway_a_scoped_models(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    scope_entity_filters: list[dict[str, Any]],
    visited_actors: set[UUID],
    direct_seed_entity_pairs: set[tuple[str, UUID]],
    notes: dict[str, Any],
    read_pool: asyncpg.Pool | None,
    read_fanout_enabled: bool,
    read_fanout_min_seeds: int,
    read_fanout_chunk_size: int,
    read_fanout_budget: ReadFanoutBudget | None = None,
) -> list[ModelRow]:
    models_out: list[ModelRow] = []
    if not scope_entity_filters and not visited_actors:
        return models_out

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
                0 if (entity_type, entity_id) in direct_seed_entity_pairs else 1
            )
        except (KeyError, TypeError, ValueError):
            continue

    seen_ids: set[UUID] = set()
    entity_sidecar_rows: list[asyncpg.Record] = []
    actor_sidecar_rows: list[asyncpg.Record] = []
    sidecar_read_fanout_budget = read_fanout_budget
    if (
        sidecar_read_fanout_budget is None
        and read_pool is not None
        and read_fanout_enabled
    ):
        sidecar_read_fanout_budget = ReadFanoutBudget.from_pool(read_pool)
    entity_fanout_chunks = 0
    entity_deferred_chunks = 0
    actor_fanout_chunks = 0
    actor_deferred_chunks = 0
    stage_started = time.perf_counter()
    if sidecar_entity_ids:
        if (
            read_pool is not None
            and read_fanout_enabled
            and len(sidecar_entity_ids) >= int(read_fanout_min_seeds)
        ):
            entity_fanout = await _fetch_pathway_a_entity_sidecar_rows_fanout(
                read_pool,
                fallback_conn=conn,
                read_fanout_budget=sidecar_read_fanout_budget,
                tenant_id=tenant_id,
                entity_types=sidecar_entity_types,
                entity_ids=sidecar_entity_ids,
                entity_orders=sidecar_entity_orders,
                entity_priorities=sidecar_entity_priorities,
                per_seed_limit=_STRUCTURAL_MODELS_PER_SCOPE_ENTITY,
                global_limit=_STRUCTURAL_MAX_MODELS,
                chunk_size=read_fanout_chunk_size,
            )
            entity_sidecar_rows = entity_fanout.rows
            entity_fanout_chunks = entity_fanout.fanout_chunks
            entity_deferred_chunks = entity_fanout.deferred_chunks
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
        fanout_used=entity_fanout_chunks > 0,
        fanout_chunks=entity_fanout_chunks,
        fanout_deferred_chunks=entity_deferred_chunks,
        fanout_budget_max=(
            sidecar_read_fanout_budget.max_concurrency
            if sidecar_read_fanout_budget is not None
            else None
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
            actor_fanout = await _fetch_pathway_a_actor_sidecar_rows_fanout(
                read_pool,
                fallback_conn=conn,
                read_fanout_budget=sidecar_read_fanout_budget,
                tenant_id=tenant_id,
                actor_ids=actor_ids,
                actor_orders=actor_orders,
                per_seed_limit=_STRUCTURAL_MODELS_PER_SCOPE_ACTOR,
                global_limit=_STRUCTURAL_MAX_MODELS,
                chunk_size=read_fanout_chunk_size,
            )
            actor_sidecar_rows = actor_fanout.rows
            actor_fanout_chunks = actor_fanout.fanout_chunks
            actor_deferred_chunks = actor_fanout.deferred_chunks
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
        fanout_used=actor_fanout_chunks > 0,
        fanout_chunks=actor_fanout_chunks,
        fanout_deferred_chunks=actor_deferred_chunks,
        fanout_budget_max=(
            sidecar_read_fanout_budget.max_concurrency
            if sidecar_read_fanout_budget is not None
            else None
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
            model = _hydrate_model(r)
            if not _model_temporally_valid(model):
                notes.setdefault("expired_scope_temporal_skipped", {}).setdefault(
                    "models", 0
                )
                notes["expired_scope_temporal_skipped"]["models"] += 1
                continue
            models_out.append(model)
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

    await _append_pathway_a_jsonb_fallback_models(
        conn,
        tenant_id=tenant_id,
        scope_entity_filters=scope_entity_filters,
        visited_actors=visited_actors,
        entity_sidecar_rows=entity_sidecar_rows,
        actor_sidecar_rows=actor_sidecar_rows,
        seen_ids=seen_ids,
        notes=notes,
        models_out=models_out,
    )
    return models_out


async def _expand_pathway_a_commit_frontier(
    conn: asyncpg.Connection,
    *,
    frontier_commits: set[UUID],
    visited_commits: set[UUID],
    visited_goals: set[UUID],
    visited_decisions: set[UUID],
    visited_customers: set[UUID],
    expansion: _PathwayAHopExpansion,
) -> None:
    if not frontier_commits:
        return
    commit_list = list(frontier_commits)
    goal_rows = await conn.fetch(
        """
        SELECT DISTINCT goal_id FROM contributes_to
        WHERE commitment_id = ANY($1::uuid[])
        """,
        commit_list,
    )
    for row in goal_rows:
        goal_id = row["goal_id"]
        if goal_id not in visited_goals:
            expansion.goals.add(goal_id)

    dep_rows = await conn.fetch(
        """
        SELECT dependency_commitment_id AS d, dependent_commitment_id AS t
        FROM depends_on
        WHERE dependent_commitment_id = ANY($1::uuid[])
           OR dependency_commitment_id = ANY($1::uuid[])
        """,
        commit_list,
    )
    for row in dep_rows:
        for commitment_id in (row["d"], row["t"]):
            if commitment_id is not None and commitment_id not in visited_commits:
                expansion.commits.add(commitment_id)

    decision_rows = await conn.fetch(
        """
        SELECT DISTINCT decision_id FROM constrained_by
        WHERE commitment_id = ANY($1::uuid[])
        """,
        commit_list,
    )
    for row in decision_rows:
        decision_id = row["decision_id"]
        if decision_id not in visited_decisions:
            expansion.decisions.add(decision_id)

    customer_rows = await conn.fetch(
        """
        SELECT DISTINCT customer_resource_id FROM customer_commitments
        WHERE commitment_id = ANY($1::uuid[])
        """,
        commit_list,
    )
    for row in customer_rows:
        customer_id = row["customer_resource_id"]
        if customer_id not in visited_customers:
            expansion.customers.add(customer_id)


async def _expand_pathway_a_goal_frontier(
    conn: asyncpg.Connection,
    *,
    frontier_goals: set[UUID],
    visited_commits: set[UUID],
    visited_goals: set[UUID],
    expansion: _PathwayAHopExpansion,
) -> None:
    if not frontier_goals:
        return
    goal_list = list(frontier_goals)
    parent_rows = await conn.fetch(
        """
        SELECT DISTINCT parent_goal_id FROM goals
        WHERE id = ANY($1::uuid[]) AND parent_goal_id IS NOT NULL
        """,
        goal_list,
    )
    for row in parent_rows:
        parent_id = row["parent_goal_id"]
        if parent_id is not None and parent_id not in visited_goals:
            expansion.goals.add(parent_id)
    child_rows = await conn.fetch(
        """
        SELECT DISTINCT id FROM goals
        WHERE parent_goal_id = ANY($1::uuid[])
        """,
        goal_list,
    )
    for row in child_rows:
        child_id = row["id"]
        if child_id not in visited_goals:
            expansion.goals.add(child_id)
    commit_from_goals = await conn.fetch(
        """
        SELECT DISTINCT commitment_id FROM contributes_to
        WHERE goal_id = ANY($1::uuid[])
        """,
        goal_list,
    )
    for row in commit_from_goals:
        commitment_id = row["commitment_id"]
        if commitment_id not in visited_commits:
            expansion.commits.add(commitment_id)


async def _expand_pathway_a_decision_frontier(
    conn: asyncpg.Connection,
    *,
    frontier_decisions: set[UUID],
    visited_commits: set[UUID],
    expansion: _PathwayAHopExpansion,
) -> None:
    if not frontier_decisions:
        return
    decision_list = list(frontier_decisions)
    commit_from_decisions = await conn.fetch(
        """
        SELECT DISTINCT commitment_id FROM constrained_by
        WHERE decision_id = ANY($1::uuid[])
        """,
        decision_list,
    )
    for row in commit_from_decisions:
        commitment_id = row["commitment_id"]
        if commitment_id not in visited_commits:
            expansion.commits.add(commitment_id)


async def _expand_pathway_a_customer_frontier(
    conn: asyncpg.Connection,
    *,
    frontier_customers: set[UUID],
    visited_commits: set[UUID],
    expansion: _PathwayAHopExpansion,
) -> None:
    if not frontier_customers:
        return
    customer_list = list(frontier_customers)
    customer_commits = await conn.fetch(
        """
        SELECT DISTINCT commitment_id FROM customer_commitments
        WHERE customer_resource_id = ANY($1::uuid[])
        """,
        customer_list,
    )
    for row in customer_commits:
        commitment_id = row["commitment_id"]
        if commitment_id not in visited_commits:
            expansion.commits.add(commitment_id)


async def _expand_pathway_a_actor_frontier(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    frontier_actors: set[UUID],
    visited_commits: set[UUID],
    expansion: _PathwayAHopExpansion,
) -> None:
    if not frontier_actors:
        return
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
    for row in owner_rows:
        commitment_id = row["id"]
        if commitment_id not in visited_commits:
            expansion.commits.add(commitment_id)
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
    for row in contributor_rows:
        commitment_id = row["commitment_id"]
        if commitment_id not in visited_commits:
            expansion.commits.add(commitment_id)


async def _walk_pathway_a_graph(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    seeds: dict[str, set[UUID]],
    max_hops: int,
    notes: dict[str, Any],
) -> _PathwayAWalkResult:
    visited_commits: set[UUID] = set(seeds["commitment"])
    visited_goals: set[UUID] = set(seeds["goal"])
    visited_decisions: set[UUID] = set(seeds["decision"])
    visited_actors: set[UUID] = set(seeds["actor"])
    visited_customers: set[UUID] = set(seeds["customer_resource"])
    visited_resources: set[UUID] = set(seeds["resource"])

    frontier_commits: set[UUID] = set(seeds["commitment"])
    frontier_goals: set[UUID] = set(seeds["goal"])
    frontier_decisions: set[UUID] = set(seeds["decision"])
    frontier_customers: set[UUID] = set(seeds["customer_resource"])
    frontier_actors: set[UUID] = set(seeds["actor"])

    stage_started = time.perf_counter()
    for hop in range(max_hops):
        expansion = _PathwayAHopExpansion()
        await _expand_pathway_a_commit_frontier(
            conn,
            frontier_commits=frontier_commits,
            visited_commits=visited_commits,
            visited_goals=visited_goals,
            visited_decisions=visited_decisions,
            visited_customers=visited_customers,
            expansion=expansion,
        )
        await _expand_pathway_a_goal_frontier(
            conn,
            frontier_goals=frontier_goals,
            visited_commits=visited_commits,
            visited_goals=visited_goals,
            expansion=expansion,
        )
        await _expand_pathway_a_decision_frontier(
            conn,
            frontier_decisions=frontier_decisions,
            visited_commits=visited_commits,
            expansion=expansion,
        )
        await _expand_pathway_a_customer_frontier(
            conn,
            frontier_customers=frontier_customers,
            visited_commits=visited_commits,
            expansion=expansion,
        )
        await _expand_pathway_a_actor_frontier(
            conn,
            tenant_id=tenant_id,
            frontier_actors=frontier_actors,
            visited_commits=visited_commits,
            expansion=expansion,
        )

        visited_commits.update(expansion.commits)
        visited_goals.update(expansion.goals)
        visited_decisions.update(expansion.decisions)
        visited_customers.update(expansion.customers)
        frontier_commits = expansion.commits
        frontier_goals = expansion.goals
        frontier_decisions = expansion.decisions
        frontier_customers = expansion.customers
        frontier_actors = set()
        notes["hops_executed"] = hop + 1

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
    return _PathwayAWalkResult(
        visited_commits=visited_commits,
        visited_goals=visited_goals,
        visited_decisions=visited_decisions,
        visited_actors=visited_actors,
        visited_customers=visited_customers,
        visited_resources=visited_resources,
        hops_executed=int(notes["hops_executed"]),
    )


async def _fetch_pathway_a_entity_rows(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    walk: _PathwayAWalkResult,
    notes: dict[str, Any],
) -> _PathwayAEntityRows:
    stage_started = time.perf_counter()
    goals_out: list[GoalRow] = []
    if walk.visited_goals:
        goal_rows = await conn.fetch(
            "SELECT * FROM goals WHERE id = ANY($1::uuid[]) AND tenant_id = $2",
            list(walk.visited_goals),
            tenant_id,
        )
        goals_out = _hydrate_many(goal_rows, _hydrate_goal, notes, "goals")

    commitments_out: list[CommitmentRow] = []
    if walk.visited_commits:
        commitment_rows = await conn.fetch(
            "SELECT * FROM commitments WHERE id = ANY($1::uuid[]) AND tenant_id = $2",
            list(walk.visited_commits),
            tenant_id,
        )
        commitments_out = _hydrate_many(
            commitment_rows, _hydrate_commitment, notes, "commitments"
        )

    decisions_out: list[DecisionRow] = []
    if walk.visited_decisions:
        decision_rows = await conn.fetch(
            "SELECT * FROM decisions WHERE id = ANY($1::uuid[]) AND tenant_id = $2",
            list(walk.visited_decisions),
            tenant_id,
        )
        decisions_out = _hydrate_many(
            decision_rows, _hydrate_decision, notes, "decisions"
        )

    resources_out: list[ResourceRow] = []
    touched_resource_ids = walk.visited_customers | walk.visited_resources
    if touched_resource_ids:
        resource_rows = await conn.fetch(
            "SELECT * FROM resources WHERE id = ANY($1::uuid[]) AND tenant_id = $2",
            list(touched_resource_ids),
            tenant_id,
        )
        resources_out = _hydrate_many(
            resource_rows, _hydrate_resource, notes, "resources"
        )
    _append_timing(
        notes,
        "act_row_fetch",
        stage_started,
        goals=len(goals_out),
        commitments=len(commitments_out),
        decisions=len(decisions_out),
        resources=len(resources_out),
    )
    return _PathwayAEntityRows(
        goals=goals_out,
        commitments=commitments_out,
        decisions=decisions_out,
        resources=resources_out,
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
    read_fanout_budget: ReadFanoutBudget | None = None,
) -> PathwayResult:
    """Walk structural Acts context from seed entities, then fetch scoped Models."""
    notes: dict[str, Any] = {
        "seeds_by_type": {},
        "hops_executed": 0,
        "entities_touched": {},
        "seeds_accepted": 0,
        "timings": [],
    }
    if not seed_entity_ids:
        return PathwayResult(
            source_pathway="A", notes={**notes, "reason": "empty_seed"}
        )
    if max_hops < 0:
        raise ValidationError("max_hops must be >= 0", max_hops=max_hops)

    parsed_seeds = _parse_pathway_a_seeds(seed_entity_ids, notes)
    seeds = parsed_seeds.seeds
    if parsed_seeds.accepted_count == 0:
        return PathwayResult(
            source_pathway="A", notes={**notes, "reason": "no_valid_seed"}
        )
    direct_seed_entity_pairs = parsed_seeds.direct_seed_entity_pairs

    walk = await _walk_pathway_a_graph(
        conn,
        tenant_id=tenant_id,
        seeds=seeds,
        max_hops=max_hops,
        notes=notes,
    )
    entity_rows = await _fetch_pathway_a_entity_rows(
        conn,
        tenant_id=tenant_id,
        walk=walk,
        notes=notes,
    )

    # Scoped Model search — union of (scope_entities @> any touched
    # entity) and (scope_actors && visited actors).
    stage_started = time.perf_counter()
    (
        scope_entity_filters,
        filters_before_cap,
        filters_dropped_by_cap,
    ) = await _build_pathway_a_scope_entity_filters(
        conn,
        tenant_id=tenant_id,
        visited_commits=walk.visited_commits,
        visited_goals=walk.visited_goals,
        visited_decisions=walk.visited_decisions,
        visited_customers=walk.visited_customers,
        visited_resources=walk.visited_resources,
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
        actors=len(walk.visited_actors),
    )

    models_out = await _fetch_pathway_a_scoped_models(
        conn,
        tenant_id=tenant_id,
        scope_entity_filters=scope_entity_filters,
        visited_actors=walk.visited_actors,
        direct_seed_entity_pairs=direct_seed_entity_pairs,
        notes=notes,
        read_pool=read_pool,
        read_fanout_enabled=read_fanout_enabled,
        read_fanout_min_seeds=read_fanout_min_seeds,
        read_fanout_chunk_size=read_fanout_chunk_size,
        read_fanout_budget=read_fanout_budget,
    )

    notes["entities_touched"] = {
        "commitments": len(walk.visited_commits),
        "goals": len(walk.visited_goals),
        "decisions": len(walk.visited_decisions),
        "actors": len(walk.visited_actors),
        "customers": len(walk.visited_customers),
        "resources": len(walk.visited_resources),
    }
    notes["model_scope_filters"] = len(scope_entity_filters)
    notes["models_returned"] = len(models_out)

    return PathwayResult(
        models=models_out,
        observations=[],
        acts={
            "goals": entity_rows.goals,
            "commitments": entity_rows.commitments,
            "decisions": entity_rows.decisions,
        },
        resources=entity_rows.resources,
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


@dataclass
class _PathwayBScope:
    sql: str
    params: list[Any]
    applied: bool


def _pathway_b_notes(seed_natural_text: str, k: int) -> dict[str, Any]:
    return {
        "seed_chars": len(seed_natural_text or ""),
        "k_requested": k,
        "vector_source": None,
        "scope_filter": None,
    }


async def _pathway_b_resolve_vector(
    seed_natural_text: str,
    *,
    embedder: OllamaClient | None,
    precomputed_vector: Sequence[float] | None,
    notes: dict[str, Any],
) -> list[float]:
    if precomputed_vector is not None:
        notes["vector_source"] = "precomputed"
        return [float(x) for x in precomputed_vector]
    if embedder is None:
        raise RetrievalPathwayError(
            "pathway B requires either a precomputed_vector or an "
            "embedder; neither was supplied",
            seed_chars=len(seed_natural_text),
        )
    try:
        vec = await embedder.embed(seed_natural_text)
        notes["vector_source"] = "ollama"
        return vec
    except OllamaError as e:
        raise RetrievalPathwayError(
            f"ollama embedding failed: {e}",
            cause=str(e),
        ) from e


def _pathway_b_validate_vector(vec: Sequence[float]) -> None:
    if len(vec) != EMBEDDING_DIM:
        raise ValidationError(
            f"pathway B vec dim {len(vec)} != {EMBEDDING_DIM}",
            got=len(vec),
            expected=EMBEDDING_DIM,
        )


async def _pathway_b_apply_hnsw_ef_search(
    conn: asyncpg.Connection,
    *,
    hnsw_ef_search: int | None,
    notes: dict[str, Any],
) -> None:
    if hnsw_ef_search is None or hnsw_ef_search <= 0:
        return
    try:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL hnsw.ef_search = {int(hnsw_ef_search)}")
        notes["hnsw_ef_search"] = int(hnsw_ef_search)
    except asyncpg.PostgresError:
        # Not fatal — just means we're not in a tx or pgvector
        # version doesn't honor the GUC. The savepoint keeps the
        # caller's transaction usable. Fall back to default.
        notes["hnsw_ef_search"] = None


async def _pathway_b_vector_param(
    conn: asyncpg.Connection,
    vec: Sequence[float],
) -> Any:
    # Ensure the pgvector codec is registered on THIS connection, then bind a
    # numpy array — the same pattern ModelsRepo.search_by_embedding uses.
    #
    # The previous `if _conn_has_vector_codec(conn): ndarray else: '[…]' string`
    # branch was unsafe: `_conn_has_vector_codec` keys on a process-wide id-set
    # (`PGVECTOR_REGISTERED_POOL_IDS`) that goes stale across the
    # PoolConnectionProxy/inner-Connection id boundary and for pools created
    # without the codec init. When it reported False while the codec was actually
    # live on the connection, binding the stringified `'[…]'::vector` literal
    # crashed asyncpg with "could not convert string to float" — which aborted
    # the retrieval and therefore every model write that depends on it (0 models
    # produced). Ensuring the codec + always binding an array removes the
    # state/bind mismatch entirely.
    from services.domain.models.repo import _ensure_vector_codec
    import numpy as _np

    await _ensure_vector_codec(conn)
    return _np.asarray([float(x) for x in vec], dtype="float32")


def _pathway_b_actor_scope(event_actors: Sequence[UUID] | None) -> list[UUID]:
    actors: list[UUID] = []
    for actor in event_actors or []:
        if actor is None:
            continue
        try:
            actors.append(UUID(str(actor)))
        except (ValueError, TypeError):
            continue
    return actors


def _pathway_b_entity_scope(
    event_entities: Sequence[dict[str, Any]] | None,
) -> list[dict[str, str]]:
    entities: list[dict[str, str]] = []
    for entity in event_entities or []:
        if not isinstance(entity, dict):
            continue
        etype = entity.get("type")
        eid = entity.get("id")
        if etype is None or eid is None:
            continue
        entities.append({"type": str(etype), "id": str(eid)})
    return entities


def _pathway_b_scope(
    *,
    tenant_id: UUID,
    vec_param: Any,
    k: int,
    event_actors: Sequence[UUID] | None,
    event_entities: Sequence[dict[str, Any]] | None,
    notes: dict[str, Any],
) -> _PathwayBScope:
    actor_list = _pathway_b_actor_scope(event_actors)
    entity_list = _pathway_b_entity_scope(event_entities)
    scope_clauses: list[str] = []
    scope_params: list[Any] = [tenant_id, vec_param, k]
    if actor_list:
        scope_params.append(actor_list)
        scope_clauses.append(f"scope_actors && ${len(scope_params)}::uuid[]")
    for ent in entity_list:
        scope_params.append(_jsonb([ent]))
        scope_clauses.append(f"scope_entities @> ${len(scope_params)}::jsonb")
    notes["scope_filter"] = {
        "event_actors_count": len(actor_list),
        "event_entities_count": len(entity_list),
        "applied": bool(scope_clauses),
    }
    scope_sql = ""
    if scope_clauses:
        scope_sql = "  AND (" + " OR ".join(scope_clauses) + ")\n"
    return _PathwayBScope(
        sql=scope_sql,
        params=scope_params,
        applied=bool(scope_clauses),
    )


async def _pathway_b_fetch_ann(
    conn: asyncpg.Connection,
    *,
    scope: _PathwayBScope,
    notes: dict[str, Any],
) -> list[asyncpg.Record]:
    ann_started = time.perf_counter()
    rows = await conn.fetch(
        f"""
        SELECT {_MODEL_SELECT_SQL}
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND embedding IS NOT NULL
        {scope.sql}ORDER BY embedding <=> $2::vector
        LIMIT $3
        """,
        *scope.params,
    )
    ann_elapsed = time.perf_counter() - ann_started
    _PGVECTOR_DURATION.observe(ann_elapsed, strategy="ann")
    _PGVECTOR_QUERIES.inc(strategy="ann")
    notes["ann_query_ms"] = int(ann_elapsed * 1000)
    return list(rows)


def _pathway_b_rank_exact(
    models: list[ModelRow],
    *,
    vec: Sequence[float],
    k: int,
) -> list[ModelRow]:
    models.sort(
        key=lambda m: (
            _cosine_distance(vec, m.embedding),
            -m.activation,
            str(m.id),
        )
    )
    return models[:k]


def _pathway_b_hydrate_exact_candidates(
    rows: Sequence[asyncpg.Record],
) -> list[_PathwayBExactCandidate]:
    candidates: list[_PathwayBExactCandidate] = []
    for row in rows:
        embedding = _vector_to_float_list(row.get("embedding"))
        if embedding is None:
            continue
        candidates.append(
            _PathwayBExactCandidate(
                id=row["id"],
                activation=float(row.get("activation") or 0.0),
                embedding=embedding,
            )
        )
    return candidates


def _pathway_b_rank_exact_candidates(
    candidates: list[_PathwayBExactCandidate],
    *,
    vec: Sequence[float],
    k: int,
) -> list[UUID]:
    candidates.sort(
        key=lambda c: (
            _cosine_distance(vec, c.embedding),
            -c.activation,
            str(c.id),
        )
    )
    return [c.id for c in candidates[:k]]


async def _pathway_b_fetch_ranked_models_by_id(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    ids: Sequence[UUID],
    notes: dict[str, Any],
    bucket: str,
) -> list[ModelRow]:
    if not ids:
        return []
    rows = await conn.fetch(
        f"""
        SELECT {_MODEL_SELECT_SQL}
        FROM models
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        list(ids),
    )
    models = _hydrate_many(rows, _hydrate_model, notes, bucket)
    by_id = {model.id: model for model in models}
    return [by_id[model_id] for model_id in ids if model_id in by_id]


async def _pathway_b_fetch_scope_exact_fallback(
    conn: asyncpg.Connection,
    *,
    vec: Sequence[float],
    scope: _PathwayBScope,
    k: int,
    ann_rows: Sequence[asyncpg.Record],
    notes: dict[str, Any],
) -> list[ModelRow]:
    exact_started = time.perf_counter()
    exact_rows = await conn.fetch(
        f"""
        WITH _params AS (
          SELECT $2::vector AS _query_vector, $3::int AS _k
        )
        SELECT id, activation, embedding
        FROM models, _params
        WHERE tenant_id = $1
          AND status = 'active'
          AND embedding IS NOT NULL
        {scope.sql}LIMIT LEAST(GREATEST($3::int * 20, 200), 2000)
        """,
        *scope.params,
    )
    _PGVECTOR_DURATION.observe(
        time.perf_counter() - exact_started, strategy="exact_fallback"
    )
    _PGVECTOR_QUERIES.inc(strategy="exact_fallback")
    candidates = _pathway_b_hydrate_exact_candidates(exact_rows)
    ranked_ids = _pathway_b_rank_exact_candidates(
        candidates,
        vec=vec,
        k=k,
    )
    models = await _pathway_b_fetch_ranked_models_by_id(
        conn,
        tenant_id=scope.params[0],
        ids=ranked_ids,
        notes=notes,
        bucket="scope_exact_models",
    )
    notes["scope_exact_fallback"] = {
        "hnsw_rows": len(ann_rows),
        "candidate_rows": len(candidates),
        "hydrated_rows": len(models),
        "returned": len(models),
    }
    return models


async def _pathway_b_fetch_exact_fallback(
    conn: asyncpg.Connection,
    *,
    vec: Sequence[float],
    scope: _PathwayBScope,
    k: int,
    ann_rows: Sequence[asyncpg.Record],
    notes: dict[str, Any],
) -> list[ModelRow]:
    exact_rows = await conn.fetch(
        """
        WITH _params AS (
          SELECT $2::vector AS _query_vector, $3::int AS _k
        )
        SELECT id, activation, embedding
        FROM models, _params
        WHERE tenant_id = $1
          AND status = 'active'
          AND embedding IS NOT NULL
        LIMIT LEAST(GREATEST($3::int * 20, 200), 5000)
        """,
        *scope.params,
    )
    candidates = _pathway_b_hydrate_exact_candidates(exact_rows)
    ranked_ids = _pathway_b_rank_exact_candidates(candidates, vec=vec, k=k)
    models = await _pathway_b_fetch_ranked_models_by_id(
        conn,
        tenant_id=scope.params[0],
        ids=ranked_ids,
        notes=notes,
        bucket="exact_models",
    )
    notes["exact_fallback"] = {
        "hnsw_rows": len(ann_rows),
        "candidate_rows": len(candidates),
        "hydrated_rows": len(models),
        "returned": len(models),
    }
    return models


async def _pathway_b_maybe_exact_fallback(
    conn: asyncpg.Connection,
    *,
    vec: Sequence[float],
    scope: _PathwayBScope,
    k: int,
    ann_rows: Sequence[asyncpg.Record],
    models: list[ModelRow],
    notes: dict[str, Any],
) -> list[ModelRow]:
    if scope.applied and len(models) < min(k, 10):
        return await _pathway_b_fetch_scope_exact_fallback(
            conn,
            vec=vec,
            scope=scope,
            k=k,
            ann_rows=ann_rows,
            notes=notes,
        )
    if not scope.applied and len(models) < k:
        return await _pathway_b_fetch_exact_fallback(
            conn,
            vec=vec,
            scope=scope,
            k=k,
            ann_rows=ann_rows,
            notes=notes,
        )
    return models


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
    notes = _pathway_b_notes(seed_natural_text, k)
    if not seed_natural_text and precomputed_vector is None:
        return PathwayResult(
            source_pathway="B",
            notes={**notes, "reason": "empty_seed"},
        )

    vec = await _pathway_b_resolve_vector(
        seed_natural_text,
        embedder=embedder,
        precomputed_vector=precomputed_vector,
        notes=notes,
    )
    _pathway_b_validate_vector(vec)
    await _pathway_b_apply_hnsw_ef_search(
        conn,
        hnsw_ef_search=hnsw_ef_search,
        notes=notes,
    )
    scope = _pathway_b_scope(
        tenant_id=tenant_id,
        vec_param=await _pathway_b_vector_param(conn, vec),
        k=k,
        event_actors=event_actors,
        event_entities=event_entities,
        notes=notes,
    )
    rows = await _pathway_b_fetch_ann(conn, scope=scope, notes=notes)
    models = _hydrate_many(rows, _hydrate_model, notes, "models")

    # HNSW is approximate and Postgres applies scope predicates around vector
    # order. Exact-rank a bounded candidate pool when ANN returns too few rows.
    models = await _pathway_b_maybe_exact_fallback(
        conn,
        vec=vec,
        scope=scope,
        k=k,
        ann_rows=rows,
        models=models,
        notes=notes,
    )
    notes["models_returned"] = len(models)

    return PathwayResult(
        models=models,
        observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        source_pathway="B",
        notes=notes,
    )


async def pathway_b_representation_tags(
    seed_natural_text: str | None,
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    seed_signature: dict[str, Any] | None = None,
    limit: int = _TAG_RESCUE_LIMIT,
    representation_feature_postings_available: bool | None = None,
    representation_postings_available: bool | None = None,
) -> PathwayResult:
    """Retrieve active models through representation tags and coverage roles."""
    seed_tags = _seed_representation_tags(seed_natural_text, seed_signature)
    coverage_roles = _coverage_roles_from_seed_tags(seed_tags)
    notes: dict[str, Any] = {
        "seed_tags": seed_tags,
        "coverage_roles": coverage_roles,
        "limit": int(limit),
    }
    if not seed_tags and not coverage_roles:
        return PathwayResult(source_pathway="B", notes={**notes, "reason": "no_tags"})

    if representation_feature_postings_available is None:
        feature_postings_table = await conn.fetchval(
            "SELECT to_regclass('public.model_representation_feature_postings')"
        )
        representation_feature_postings_available = feature_postings_table is not None
    if representation_feature_postings_available:
        per_tag_limit = max(32, min(240, max(1, int(limit)) * 6))
        notes["representation_postings_index"] = True
        notes["representation_feature_postings_index"] = True
        notes["representation_postings_table"] = "model_representation_feature_postings"
        notes["postings_per_tag_limit"] = per_tag_limit
        rows = await conn.fetch(
            f"""
            WITH query_tags AS MATERIALIZED (
              SELECT *
              FROM (
                SELECT 'domain'::text AS feature_type,
                       tag::text AS feature,
                       ord::int AS tag_ord
                FROM unnest($2::text[]) WITH ORDINALITY AS q(tag, ord)

                UNION ALL

                SELECT 'retrieval'::text AS feature_type,
                       tag::text AS feature,
                       ord::int AS tag_ord
                FROM unnest($2::text[]) WITH ORDINALITY AS q(tag, ord)

                UNION ALL

                SELECT 'coverage'::text AS feature_type,
                       tag::text AS feature,
                       (1000 + ord)::int AS tag_ord
                FROM unnest($3::text[]) WITH ORDINALITY AS q(tag, ord)
              ) raw
              WHERE nullif(feature, '') IS NOT NULL
            ),
            tag_hits AS MATERIALIZED (
              SELECT qt.feature_type,
                     qt.feature,
                     qt.tag_ord,
                     hit.model_id
              FROM query_tags qt
              CROSS JOIN LATERAL (
                SELECT post.model_id
                FROM model_representation_feature_postings post
                JOIN models m
                  ON m.id = post.model_id
                 AND m.tenant_id = $1
                 AND m.status = 'active'
                WHERE post.tenant_id = $1
                  AND post.status = 'active'
                  AND post.feature_type = qt.feature_type
                  AND post.feature = qt.feature
                ORDER BY m.activation DESC,
                         m.confirmed_count DESC,
                         m.created_at DESC,
                         post.model_id
                LIMIT $4
              ) hit
            ),
            scored AS MATERIALIZED (
              SELECT model_id,
                     count(DISTINCT feature_type)::int AS _tag_match_rank,
                     count(DISTINCT (feature_type, feature))::int AS _tag_match_count,
                     min(tag_ord)::int AS _first_tag_ord
              FROM tag_hits
              GROUP BY model_id
            )
            SELECT {_MODEL_SELECT_SQL},
                   scored._tag_match_rank
            FROM scored
            JOIN models
              ON models.id = scored.model_id
             AND models.tenant_id = $1
            WHERE status = 'active'
            ORDER BY scored._tag_match_rank DESC,
                     scored._tag_match_count DESC,
                     scored._first_tag_ord ASC,
                     activation DESC,
                     confirmed_count DESC,
                     created_at DESC
            LIMIT $5
            """,
            tenant_id,
            seed_tags,
            coverage_roles,
            per_tag_limit,
            max(1, int(limit)),
        )
    else:
        notes["representation_feature_postings_index"] = False
    if (
        not representation_feature_postings_available
        and representation_postings_available is None
    ):
        postings_table = await conn.fetchval(
            "SELECT to_regclass('public.model_representation_tag_postings')"
        )
        representation_postings_available = postings_table is not None
    if (
        not representation_feature_postings_available
        and representation_postings_available
    ):
        per_tag_limit = max(32, min(240, max(1, int(limit)) * 6))
        notes["representation_postings_index"] = True
        notes["representation_postings_table"] = "model_representation_tag_postings"
        notes["postings_per_tag_limit"] = per_tag_limit
        rows = await conn.fetch(
            f"""
            WITH query_tags AS MATERIALIZED (
              SELECT *
              FROM (
                SELECT 'domain'::text AS tag_type,
                       tag::text AS tag,
                       ord::int AS tag_ord
                FROM unnest($2::text[]) WITH ORDINALITY AS q(tag, ord)

                UNION ALL

                SELECT 'retrieval'::text AS tag_type,
                       tag::text AS tag,
                       ord::int AS tag_ord
                FROM unnest($2::text[]) WITH ORDINALITY AS q(tag, ord)

                UNION ALL

                SELECT 'coverage'::text AS tag_type,
                       tag::text AS tag,
                       (1000 + ord)::int AS tag_ord
                FROM unnest($3::text[]) WITH ORDINALITY AS q(tag, ord)
              ) raw
              WHERE nullif(tag, '') IS NOT NULL
            ),
            tag_hits AS MATERIALIZED (
              SELECT qt.tag_type,
                     qt.tag,
                     qt.tag_ord,
                     hit.model_id
              FROM query_tags qt
              CROSS JOIN LATERAL (
                SELECT post.model_id
                FROM model_representation_tag_postings post
                JOIN models m
                  ON m.id = post.model_id
                 AND m.tenant_id = $1
                 AND m.status = 'active'
                WHERE post.tenant_id = $1
                  AND post.status = 'active'
                  AND post.tag_type = qt.tag_type
                  AND post.tag = qt.tag
                ORDER BY m.activation DESC,
                         m.confirmed_count DESC,
                         m.created_at DESC,
                         post.model_id
                LIMIT $4
              ) hit
            ),
            scored AS MATERIALIZED (
              SELECT model_id,
                     count(DISTINCT tag_type)::int AS _tag_match_rank,
                     count(DISTINCT (tag_type, tag))::int AS _tag_match_count,
                     min(tag_ord)::int AS _first_tag_ord
              FROM tag_hits
              GROUP BY model_id
            )
            SELECT {_MODEL_SELECT_SQL},
                   scored._tag_match_rank
            FROM scored
            JOIN models
              ON models.id = scored.model_id
             AND models.tenant_id = $1
            WHERE status = 'active'
            ORDER BY scored._tag_match_rank DESC,
                     scored._tag_match_count DESC,
                     scored._first_tag_ord ASC,
                     activation DESC,
                     confirmed_count DESC,
                     created_at DESC
            LIMIT $5
            """,
            tenant_id,
            seed_tags,
            coverage_roles,
            per_tag_limit,
            max(1, int(limit)),
        )
    elif not representation_feature_postings_available:
        notes["representation_postings_index"] = False
        rows = await conn.fetch(
            f"""
            SELECT {_MODEL_SELECT_SQL},
                   (
                     CASE WHEN domain_tags && $2::text[] THEN 1 ELSE 0 END
                     + CASE WHEN coalesce(proposition->'retrieval_tags', '[]'::jsonb) ?| $2::text[] THEN 1 ELSE 0 END
                     + CASE WHEN coalesce(proposition->'coverage_roles', '[]'::jsonb) ?| $3::text[] THEN 1 ELSE 0 END
                   ) AS _tag_match_rank
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND (
                   domain_tags && $2::text[]
                   OR coalesce(proposition->'retrieval_tags', '[]'::jsonb) ?| $2::text[]
                   OR coalesce(proposition->'coverage_roles', '[]'::jsonb) ?| $3::text[]
              )
            ORDER BY _tag_match_rank DESC,
                     activation DESC,
                     confirmed_count DESC,
                     created_at DESC
            LIMIT $4
            """,
            tenant_id,
            seed_tags,
            coverage_roles,
            max(1, int(limit)),
        )
    models = _hydrate_many(rows, _hydrate_model, notes, "models")
    notes["models_returned"] = len(models)
    return PathwayResult(
        models=models,
        observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        source_pathway="B",
        notes=notes,
    )


async def pathway_b_representation_tag_candidates(
    seed_natural_text: str | None,
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    seed_signature: dict[str, Any] | None = None,
    limit: int = _TAG_RESCUE_LIMIT,
    representation_feature_postings_available: bool | None = None,
    representation_postings_available: bool | None = None,
) -> tuple[list[ModelCandidateHit], dict[str, Any]]:
    """Retrieve representation-tag candidates without hydrating ModelRows."""
    seed_tags = _seed_representation_tags(seed_natural_text, seed_signature)
    coverage_roles = _coverage_roles_from_seed_tags(seed_tags)
    notes: dict[str, Any] = {
        "seed_tags": seed_tags,
        "coverage_roles": coverage_roles,
        "limit": int(limit),
        "lightweight_candidates": True,
    }
    if not seed_tags and not coverage_roles:
        notes["reason"] = "no_tags"
        return [], notes

    if representation_feature_postings_available is None:
        feature_postings_table = await conn.fetchval(
            "SELECT to_regclass('public.model_representation_feature_postings')"
        )
        representation_feature_postings_available = feature_postings_table is not None
    if representation_feature_postings_available:
        per_tag_limit = max(32, min(240, max(1, int(limit)) * 6))
        notes["representation_postings_index"] = True
        notes["representation_feature_postings_index"] = True
        notes["representation_postings_table"] = "model_representation_feature_postings"
        notes["postings_per_tag_limit"] = per_tag_limit
        rows = await conn.fetch(
            """
            WITH query_tags AS MATERIALIZED (
              SELECT *
              FROM (
                SELECT 'domain'::text AS feature_type,
                       tag::text AS feature,
                       ord::int AS tag_ord
                FROM unnest($2::text[]) WITH ORDINALITY AS q(tag, ord)

                UNION ALL

                SELECT 'retrieval'::text AS feature_type,
                       tag::text AS feature,
                       ord::int AS tag_ord
                FROM unnest($2::text[]) WITH ORDINALITY AS q(tag, ord)

                UNION ALL

                SELECT 'coverage'::text AS feature_type,
                       tag::text AS feature,
                       (1000 + ord)::int AS tag_ord
                FROM unnest($3::text[]) WITH ORDINALITY AS q(tag, ord)
              ) raw
              WHERE nullif(feature, '') IS NOT NULL
            ),
            tag_hits AS MATERIALIZED (
              SELECT qt.feature_type,
                     qt.feature,
                     qt.tag_ord,
                     hit.model_id
              FROM query_tags qt
              CROSS JOIN LATERAL (
                SELECT post.model_id
                FROM model_representation_feature_postings post
                JOIN models m
                  ON m.id = post.model_id
                 AND m.tenant_id = $1
                 AND m.status = 'active'
                WHERE post.tenant_id = $1
                  AND post.status = 'active'
                  AND post.feature_type = qt.feature_type
                  AND post.feature = qt.feature
                ORDER BY m.activation DESC,
                         m.confirmed_count DESC,
                         m.created_at DESC,
                         post.model_id
                LIMIT $4
              ) hit
            ),
            scored AS MATERIALIZED (
              SELECT model_id,
                     count(DISTINCT feature_type)::int AS _tag_match_rank,
                     count(DISTINCT (feature_type, feature))::int AS _tag_match_count,
                     min(tag_ord)::int AS _first_tag_ord
              FROM tag_hits
              GROUP BY model_id
            )
            SELECT scored.model_id,
                   scored._tag_match_rank,
                   scored._tag_match_count,
                   scored._first_tag_ord,
                   m.activation
            FROM scored
            JOIN models m
              ON m.id = scored.model_id
             AND m.tenant_id = $1
             AND m.status = 'active'
            ORDER BY scored._tag_match_rank DESC,
                     scored._tag_match_count DESC,
                     scored._first_tag_ord ASC,
                     m.activation DESC,
                     m.confirmed_count DESC,
                     m.created_at DESC
            LIMIT $5
            """,
            tenant_id,
            seed_tags,
            coverage_roles,
            per_tag_limit,
            max(1, int(limit)),
        )
    else:
        notes["representation_feature_postings_index"] = False
    if (
        not representation_feature_postings_available
        and representation_postings_available is None
    ):
        postings_table = await conn.fetchval(
            "SELECT to_regclass('public.model_representation_tag_postings')"
        )
        representation_postings_available = postings_table is not None
    if (
        not representation_feature_postings_available
        and representation_postings_available
    ):
        per_tag_limit = max(32, min(240, max(1, int(limit)) * 6))
        notes["representation_postings_index"] = True
        notes["representation_postings_table"] = "model_representation_tag_postings"
        notes["postings_per_tag_limit"] = per_tag_limit
        rows = await conn.fetch(
            """
            WITH query_tags AS MATERIALIZED (
              SELECT *
              FROM (
                SELECT 'domain'::text AS tag_type,
                       tag::text AS tag,
                       ord::int AS tag_ord
                FROM unnest($2::text[]) WITH ORDINALITY AS q(tag, ord)

                UNION ALL

                SELECT 'retrieval'::text AS tag_type,
                       tag::text AS tag,
                       ord::int AS tag_ord
                FROM unnest($2::text[]) WITH ORDINALITY AS q(tag, ord)

                UNION ALL

                SELECT 'coverage'::text AS tag_type,
                       tag::text AS tag,
                       (1000 + ord)::int AS tag_ord
                FROM unnest($3::text[]) WITH ORDINALITY AS q(tag, ord)
              ) raw
              WHERE nullif(tag, '') IS NOT NULL
            ),
            tag_hits AS MATERIALIZED (
              SELECT qt.tag_type,
                     qt.tag,
                     qt.tag_ord,
                     hit.model_id
              FROM query_tags qt
              CROSS JOIN LATERAL (
                SELECT post.model_id
                FROM model_representation_tag_postings post
                JOIN models m
                  ON m.id = post.model_id
                 AND m.tenant_id = $1
                 AND m.status = 'active'
                WHERE post.tenant_id = $1
                  AND post.status = 'active'
                  AND post.tag_type = qt.tag_type
                  AND post.tag = qt.tag
                ORDER BY m.activation DESC,
                         m.confirmed_count DESC,
                         m.created_at DESC,
                         post.model_id
                LIMIT $4
              ) hit
            ),
            scored AS MATERIALIZED (
              SELECT model_id,
                     count(DISTINCT tag_type)::int AS _tag_match_rank,
                     count(DISTINCT (tag_type, tag))::int AS _tag_match_count,
                     min(tag_ord)::int AS _first_tag_ord
              FROM tag_hits
              GROUP BY model_id
            )
            SELECT scored.model_id,
                   scored._tag_match_rank,
                   scored._tag_match_count,
                   scored._first_tag_ord,
                   m.activation
            FROM scored
            JOIN models m
              ON m.id = scored.model_id
             AND m.tenant_id = $1
             AND m.status = 'active'
            ORDER BY scored._tag_match_rank DESC,
                     scored._tag_match_count DESC,
                     scored._first_tag_ord ASC,
                     m.activation DESC,
                     m.confirmed_count DESC,
                     m.created_at DESC
            LIMIT $5
            """,
            tenant_id,
            seed_tags,
            coverage_roles,
            per_tag_limit,
            max(1, int(limit)),
        )
    elif not representation_feature_postings_available:
        notes["representation_postings_index"] = False
        rows = await conn.fetch(
            """
            SELECT models.id AS model_id,
                   models.activation,
                   (
                     CASE WHEN domain_tags && $2::text[] THEN 1 ELSE 0 END
                     + CASE WHEN coalesce(proposition->'retrieval_tags', '[]'::jsonb) ?| $2::text[] THEN 1 ELSE 0 END
                     + CASE WHEN coalesce(proposition->'coverage_roles', '[]'::jsonb) ?| $3::text[] THEN 1 ELSE 0 END
                   ) AS _tag_match_rank,
                   1::int AS _tag_match_count,
                   1::int AS _first_tag_ord
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND (
                   domain_tags && $2::text[]
                   OR coalesce(proposition->'retrieval_tags', '[]'::jsonb) ?| $2::text[]
                   OR coalesce(proposition->'coverage_roles', '[]'::jsonb) ?| $3::text[]
              )
            ORDER BY _tag_match_rank DESC,
                     activation DESC,
                     confirmed_count DESC,
                     created_at DESC
            LIMIT $4
            """,
            tenant_id,
            seed_tags,
            coverage_roles,
            max(1, int(limit)),
        )
    hits = _candidate_hits_from_rows(
        rows,
        match_key="_tag_match_count",
        first_rank_key="_first_tag_ord",
    )
    notes["models_returned"] = len(hits)
    return hits, notes


# =====================================================================
# Pathway L — Lexical semantic-term overlap
# =====================================================================


async def pathway_l_semantic_terms(
    seed_natural_text: str | None,
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    seed_signature: dict[str, Any] | None = None,
    scope_actors: Sequence[UUID] | None = None,
    scope_entities: Sequence[dict[str, Any]] | None = None,
    limit: int = _SEMANTIC_TERMS_LIMIT,
    semantic_feature_postings_available: bool | None = None,
    semantic_postings_available: bool | None = None,
    semantic_postings_status_column: bool | None = None,
) -> PathwayResult:
    """Retrieve active Models through model-specific semantic terms."""
    query_terms = derive_query_semantic_terms(
        seed_natural_text,
        seed_signature=seed_signature,
    )
    notes: dict[str, Any] = {
        "query_terms": query_terms,
        "limit": int(limit),
    }
    if not query_terms:
        return PathwayResult(source_pathway="L", notes={**notes, "reason": "no_terms"})

    actor_list = _pathway_b_actor_scope(scope_actors)
    entity_list = _pathway_b_entity_scope(scope_entities)
    params: list[Any] = [tenant_id, query_terms, max(1, int(limit))]
    scope_clauses: list[str] = []
    if actor_list:
        params.append(actor_list)
        scope_clauses.append(f"scope_actors && ${len(params)}::uuid[]")
    for entity in entity_list:
        params.append(_jsonb([entity]))
        scope_clauses.append(f"scope_entities @> ${len(params)}::jsonb")
    notes["scope_filter"] = {
        "event_actors_count": len(actor_list),
        "event_entities_count": len(entity_list),
        "applied": bool(scope_clauses),
    }
    scope_sql = ""
    if scope_clauses:
        scope_sql = "  AND (" + " OR ".join(scope_clauses) + ")\n"

    if semantic_feature_postings_available is None:
        feature_postings_table = await conn.fetchval(
            "SELECT to_regclass('public.model_representation_feature_postings')"
        )
        semantic_feature_postings_available = feature_postings_table is not None
    if semantic_feature_postings_available:
        posting_limit = max(32, min(240, max(1, int(limit)) * 6))
        notes["postings_index"] = True
        notes["feature_postings_index"] = True
        notes["postings_table"] = "model_representation_feature_postings"
        notes["postings_status_filter"] = True
        notes["postings_per_term_limit"] = posting_limit
        params = [tenant_id, query_terms, max(1, int(limit)), posting_limit]
        lateral_scope_clauses = []
        if actor_list:
            params.append(actor_list)
            lateral_scope_clauses.append(f"m.scope_actors && ${len(params)}::uuid[]")
        for entity in entity_list:
            params.append(_jsonb([entity]))
            lateral_scope_clauses.append(f"m.scope_entities @> ${len(params)}::jsonb")
        lateral_scope_sql = ""
        if lateral_scope_clauses:
            lateral_scope_sql = (
                "                  AND (" + " OR ".join(lateral_scope_clauses) + ")\n"
            )
        rows = await conn.fetch(
            f"""
            WITH query_terms AS MATERIALIZED (
              SELECT term::text,
                     ord::int AS term_ord
              FROM unnest($2::text[]) WITH ORDINALITY AS q(term, ord)
            ),
            term_hits AS MATERIALIZED (
              SELECT qt.term,
                     qt.term_ord,
                     hit.model_id
              FROM query_terms qt
              CROSS JOIN LATERAL (
                SELECT post.model_id
                FROM model_representation_feature_postings post
                JOIN models m
                  ON m.id = post.model_id
                 AND m.tenant_id = $1
                 AND m.status = 'active'
                WHERE post.tenant_id = $1
                  AND post.status = 'active'
                  AND post.feature_type = 'lexical'
                  AND post.feature = qt.term
{lateral_scope_sql}                ORDER BY m.activation DESC,
                         m.confirmed_count DESC,
                         m.created_at DESC,
                         post.model_id
                LIMIT $4
              ) hit
            ),
            scored AS MATERIALIZED (
              SELECT model_id,
                     count(DISTINCT term)::int AS _semantic_term_overlap,
                     min(term_ord)::int AS _first_term_ord
              FROM term_hits
              GROUP BY model_id
            )
            SELECT {_MODEL_SELECT_SQL},
                   scored._semantic_term_overlap
            FROM scored
            JOIN models
              ON models.id = scored.model_id
             AND models.tenant_id = $1
            WHERE status = 'active'
            ORDER BY scored._semantic_term_overlap DESC,
                     scored._first_term_ord ASC,
                     activation DESC,
                     confirmed_count DESC,
                     created_at DESC
            LIMIT $3
            """,
            *params,
        )
    else:
        notes["feature_postings_index"] = False
    if not semantic_feature_postings_available and semantic_postings_available is None:
        postings_table = await conn.fetchval(
            "SELECT to_regclass('public.model_semantic_term_postings')"
        )
        semantic_postings_available = postings_table is not None
    if not semantic_feature_postings_available and semantic_postings_available:
        if semantic_postings_status_column is None:
            semantic_postings_status_column = bool(
                await conn.fetchval(
                    """
                    SELECT EXISTS (
                      SELECT 1
                      FROM information_schema.columns
                      WHERE table_schema = 'public'
                        AND table_name = 'model_semantic_term_postings'
                        AND column_name = 'status'
                    )
                    """
                )
            )
        postings_status_column = bool(semantic_postings_status_column)
        posting_limit = max(32, min(240, max(1, int(limit)) * 6))
        notes["postings_index"] = True
        notes["postings_table"] = "model_semantic_term_postings"
        notes["postings_status_filter"] = postings_status_column
        notes["postings_per_term_limit"] = posting_limit
        params = [tenant_id, query_terms, max(1, int(limit)), posting_limit]
        lateral_scope_clauses = []
        if actor_list:
            params.append(actor_list)
            lateral_scope_clauses.append(f"m.scope_actors && ${len(params)}::uuid[]")
        for entity in entity_list:
            params.append(_jsonb([entity]))
            lateral_scope_clauses.append(f"m.scope_entities @> ${len(params)}::jsonb")
        lateral_scope_sql = ""
        if lateral_scope_clauses:
            lateral_scope_sql = (
                "                  AND (" + " OR ".join(lateral_scope_clauses) + ")\n"
            )
        post_status_sql = (
            "                  AND post.status = 'active'\n"
            if postings_status_column
            else ""
        )
        rows = await conn.fetch(
            f"""
            WITH query_terms AS MATERIALIZED (
              SELECT term::text,
                     ord::int AS term_ord
              FROM unnest($2::text[]) WITH ORDINALITY AS q(term, ord)
            ),
            term_hits AS MATERIALIZED (
              SELECT qt.term,
                     qt.term_ord,
                     hit.model_id
              FROM query_terms qt
              CROSS JOIN LATERAL (
                SELECT post.model_id
                FROM model_semantic_term_postings post
                JOIN models m
                  ON m.id = post.model_id
                 AND m.tenant_id = $1
                 AND m.status = 'active'
                WHERE post.tenant_id = $1
{post_status_sql}
                  AND post.term = qt.term
{lateral_scope_sql}                ORDER BY m.activation DESC,
                         m.confirmed_count DESC,
                         m.created_at DESC,
                         post.model_id
                LIMIT $4
              ) hit
            ),
            scored AS MATERIALIZED (
              SELECT model_id,
                     count(DISTINCT term)::int AS _semantic_term_overlap,
                     min(term_ord)::int AS _first_term_ord
              FROM term_hits
              GROUP BY model_id
            )
            SELECT {_MODEL_SELECT_SQL},
                   scored._semantic_term_overlap
            FROM scored
            JOIN models
              ON models.id = scored.model_id
             AND models.tenant_id = $1
            WHERE status = 'active'
            ORDER BY scored._semantic_term_overlap DESC,
                     scored._first_term_ord ASC,
                     activation DESC,
                     confirmed_count DESC,
                     created_at DESC
            LIMIT $3
            """,
            *params,
        )
    elif not semantic_feature_postings_available:
        notes["postings_index"] = False
        rows = await conn.fetch(
            f"""
            SELECT {_MODEL_SELECT_SQL},
                   (
                     SELECT count(*)::int
                     FROM unnest(mst.semantic_terms) AS term
                     WHERE term = ANY($2::text[])
                   ) AS _semantic_term_overlap
            FROM models
            JOIN (
              SELECT model_id, semantic_terms
              FROM model_semantic_terms
              WHERE tenant_id = $1
            ) mst ON mst.model_id = models.id
            WHERE tenant_id = $1
              AND status = 'active'
              AND mst.semantic_terms && $2::text[]
            {scope_sql}
            ORDER BY _semantic_term_overlap DESC,
                     activation DESC,
                     confirmed_count DESC,
                     created_at DESC
            LIMIT $3
            """,
            *params,
        )
    models = _hydrate_many(rows, _hydrate_model, notes, "models")
    notes["models_returned"] = len(models)
    return PathwayResult(
        models=models,
        observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        source_pathway="L",
        notes=notes,
    )


async def pathway_l_semantic_term_candidates(
    seed_natural_text: str | None,
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    seed_signature: dict[str, Any] | None = None,
    scope_actors: Sequence[UUID] | None = None,
    scope_entities: Sequence[dict[str, Any]] | None = None,
    limit: int = _SEMANTIC_TERMS_LIMIT,
    semantic_feature_postings_available: bool | None = None,
    semantic_postings_available: bool | None = None,
    semantic_postings_status_column: bool | None = None,
) -> tuple[list[ModelCandidateHit], dict[str, Any]]:
    """Retrieve semantic-term candidates without hydrating ModelRows."""
    query_terms = derive_query_semantic_terms(
        seed_natural_text,
        seed_signature=seed_signature,
    )
    notes: dict[str, Any] = {
        "query_terms": query_terms,
        "limit": int(limit),
        "lightweight_candidates": True,
    }
    if not query_terms:
        notes["reason"] = "no_terms"
        return [], notes

    actor_list = _pathway_b_actor_scope(scope_actors)
    entity_list = _pathway_b_entity_scope(scope_entities)
    params: list[Any] = [tenant_id, query_terms, max(1, int(limit))]
    scope_clauses: list[str] = []
    if actor_list:
        params.append(actor_list)
        scope_clauses.append(f"scope_actors && ${len(params)}::uuid[]")
    for entity in entity_list:
        params.append(_jsonb([entity]))
        scope_clauses.append(f"scope_entities @> ${len(params)}::jsonb")
    notes["scope_filter"] = {
        "event_actors_count": len(actor_list),
        "event_entities_count": len(entity_list),
        "applied": bool(scope_clauses),
    }
    scope_sql = ""
    if scope_clauses:
        scope_sql = "  AND (" + " OR ".join(scope_clauses) + ")\n"

    if semantic_feature_postings_available is None:
        feature_postings_table = await conn.fetchval(
            "SELECT to_regclass('public.model_representation_feature_postings')"
        )
        semantic_feature_postings_available = feature_postings_table is not None
    if semantic_feature_postings_available:
        posting_limit = max(32, min(240, max(1, int(limit)) * 6))
        notes["postings_index"] = True
        notes["feature_postings_index"] = True
        notes["postings_table"] = "model_representation_feature_postings"
        notes["postings_status_filter"] = True
        notes["postings_per_term_limit"] = posting_limit
        params = [tenant_id, query_terms, max(1, int(limit)), posting_limit]
        lateral_scope_clauses = []
        if actor_list:
            params.append(actor_list)
            lateral_scope_clauses.append(f"m.scope_actors && ${len(params)}::uuid[]")
        for entity in entity_list:
            params.append(_jsonb([entity]))
            lateral_scope_clauses.append(f"m.scope_entities @> ${len(params)}::jsonb")
        lateral_scope_sql = ""
        if lateral_scope_clauses:
            lateral_scope_sql = (
                "                  AND (" + " OR ".join(lateral_scope_clauses) + ")\n"
            )
        rows = await conn.fetch(
            f"""
            WITH query_terms AS MATERIALIZED (
              SELECT term::text,
                     ord::int AS term_ord
              FROM unnest($2::text[]) WITH ORDINALITY AS q(term, ord)
            ),
            term_hits AS MATERIALIZED (
              SELECT qt.term,
                     qt.term_ord,
                     hit.model_id
              FROM query_terms qt
              CROSS JOIN LATERAL (
                SELECT post.model_id
                FROM model_representation_feature_postings post
                JOIN models m
                  ON m.id = post.model_id
                 AND m.tenant_id = $1
                 AND m.status = 'active'
                WHERE post.tenant_id = $1
                  AND post.status = 'active'
                  AND post.feature_type = 'lexical'
                  AND post.feature = qt.term
{lateral_scope_sql}                ORDER BY m.activation DESC,
                         m.confirmed_count DESC,
                         m.created_at DESC,
                         post.model_id
                LIMIT $4
              ) hit
            ),
            scored AS MATERIALIZED (
              SELECT model_id,
                     count(DISTINCT term)::int AS _semantic_term_overlap,
                     min(term_ord)::int AS _first_term_ord
              FROM term_hits
              GROUP BY model_id
            )
            SELECT scored.model_id,
                   scored._semantic_term_overlap,
                   scored._first_term_ord,
                   m.activation
            FROM scored
            JOIN models m
              ON m.id = scored.model_id
             AND m.tenant_id = $1
             AND m.status = 'active'
            ORDER BY scored._semantic_term_overlap DESC,
                     scored._first_term_ord ASC,
                     m.activation DESC,
                     m.confirmed_count DESC,
                     m.created_at DESC
            LIMIT $3
            """,
            *params,
        )
    else:
        notes["feature_postings_index"] = False
    if not semantic_feature_postings_available and semantic_postings_available is None:
        postings_table = await conn.fetchval(
            "SELECT to_regclass('public.model_semantic_term_postings')"
        )
        semantic_postings_available = postings_table is not None
    if not semantic_feature_postings_available and semantic_postings_available:
        if semantic_postings_status_column is None:
            semantic_postings_status_column = bool(
                await conn.fetchval(
                    """
                    SELECT EXISTS (
                      SELECT 1
                      FROM information_schema.columns
                      WHERE table_schema = 'public'
                        AND table_name = 'model_semantic_term_postings'
                        AND column_name = 'status'
                    )
                    """
                )
            )
        postings_status_column = bool(semantic_postings_status_column)
        posting_limit = max(32, min(240, max(1, int(limit)) * 6))
        notes["postings_index"] = True
        notes["postings_table"] = "model_semantic_term_postings"
        notes["postings_status_filter"] = postings_status_column
        notes["postings_per_term_limit"] = posting_limit
        params = [tenant_id, query_terms, max(1, int(limit)), posting_limit]
        lateral_scope_clauses = []
        if actor_list:
            params.append(actor_list)
            lateral_scope_clauses.append(f"m.scope_actors && ${len(params)}::uuid[]")
        for entity in entity_list:
            params.append(_jsonb([entity]))
            lateral_scope_clauses.append(f"m.scope_entities @> ${len(params)}::jsonb")
        lateral_scope_sql = ""
        if lateral_scope_clauses:
            lateral_scope_sql = (
                "                  AND (" + " OR ".join(lateral_scope_clauses) + ")\n"
            )
        post_status_sql = (
            "                  AND post.status = 'active'\n"
            if postings_status_column
            else ""
        )
        rows = await conn.fetch(
            f"""
            WITH query_terms AS MATERIALIZED (
              SELECT term::text,
                     ord::int AS term_ord
              FROM unnest($2::text[]) WITH ORDINALITY AS q(term, ord)
            ),
            term_hits AS MATERIALIZED (
              SELECT qt.term,
                     qt.term_ord,
                     hit.model_id
              FROM query_terms qt
              CROSS JOIN LATERAL (
                SELECT post.model_id
                FROM model_semantic_term_postings post
                JOIN models m
                  ON m.id = post.model_id
                 AND m.tenant_id = $1
                 AND m.status = 'active'
                WHERE post.tenant_id = $1
{post_status_sql}
                  AND post.term = qt.term
{lateral_scope_sql}                ORDER BY m.activation DESC,
                         m.confirmed_count DESC,
                         m.created_at DESC,
                         post.model_id
                LIMIT $4
              ) hit
            ),
            scored AS MATERIALIZED (
              SELECT model_id,
                     count(DISTINCT term)::int AS _semantic_term_overlap,
                     min(term_ord)::int AS _first_term_ord
              FROM term_hits
              GROUP BY model_id
            )
            SELECT scored.model_id,
                   scored._semantic_term_overlap,
                   scored._first_term_ord,
                   m.activation
            FROM scored
            JOIN models m
              ON m.id = scored.model_id
             AND m.tenant_id = $1
             AND m.status = 'active'
            ORDER BY scored._semantic_term_overlap DESC,
                     scored._first_term_ord ASC,
                     m.activation DESC,
                     m.confirmed_count DESC,
                     m.created_at DESC
            LIMIT $3
            """,
            *params,
        )
    elif not semantic_feature_postings_available:
        notes["postings_index"] = False
        rows = await conn.fetch(
            f"""
            SELECT models.id AS model_id,
                   models.activation,
                   (
                     SELECT count(*)::int
                     FROM unnest(mst.semantic_terms) AS term
                     WHERE term = ANY($2::text[])
                   ) AS _semantic_term_overlap,
                   1::int AS _first_term_ord
            FROM models
            JOIN (
              SELECT model_id, semantic_terms
              FROM model_semantic_terms
              WHERE tenant_id = $1
            ) mst ON mst.model_id = models.id
            WHERE tenant_id = $1
              AND status = 'active'
              AND mst.semantic_terms && $2::text[]
            {scope_sql}
            ORDER BY _semantic_term_overlap DESC,
                     activation DESC,
                     confirmed_count DESC,
                     created_at DESC
            LIMIT $3
            """,
            *params,
        )
    hits = _candidate_hits_from_rows(
        rows,
        match_key="_semantic_term_overlap",
        first_rank_key="_first_term_ord",
    )
    notes["models_returned"] = len(hits)
    return hits, notes


# =====================================================================
# Pathway C — Temporal recency (Observations + Models in a time window)
# =====================================================================


def _observation_record_matches_scope(
    row: asyncpg.Record,
    *,
    actor_ids: Sequence[UUID],
    entity_list: Sequence[dict[str, str]],
    include_entity_mentions: bool,
) -> bool:
    if not actor_ids and not entity_list:
        return True
    actor_id = _record_get(row, "actor_id")
    actor_values = {str(actor) for actor in actor_ids}
    if actor_id is not None and str(actor_id) in actor_values:
        return True
    mentions = [
        item
        for item in _json_list(_record_get(row, "entities_mentioned"))
        if isinstance(item, dict)
    ]
    mention_keys = {
        (str(item.get("type") or ""), str(item.get("id") or ""))
        for item in mentions
        if item.get("type") is not None and item.get("id") is not None
    }
    if include_entity_mentions:
        for actor in actor_values:
            if ("actor", actor) in mention_keys:
                return True
    for entity in entity_list:
        key = (str(entity.get("type") or ""), str(entity.get("id") or ""))
        if key in mention_keys:
            return True
    return False


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
    scope_filter_strategy: str = "indexed_or",
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
        raise ValidationError(
            "window must be > 0", window_seconds=window.total_seconds()
        )

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
        "temporal_scope_filter_strategy": scope_filter_strategy,
    }
    timings_ms: dict[str, int] = {}
    entity_list: list[dict[str, str]] = []
    for ent in scope_entities or []:
        if not isinstance(ent, dict):
            continue
        etype = ent.get("type")
        eid = ent.get("id")
        if etype is None or eid is None:
            continue
        entity_list.append({"type": str(etype), "id": str(eid)})

    actor_ids = list(scope_actors or [])
    has_scope_filter = bool(actor_ids or entity_list)
    use_time_prefilter = (
        str(scope_filter_strategy or "").strip().lower() == "time_prefilter"
        and has_scope_filter
    )
    obs_query_started = time.perf_counter()
    if use_time_prefilter:
        prefilter_limit = min(
            1000,
            max(
                int(max_observations),
                int(max_observations) * 12,
                (len(actor_ids) + len(entity_list)) * 48,
            ),
        )
        obs_sql = (
            f"SELECT {_OBS_SELECT_SQL} FROM observations "
            "WHERE tenant_id = $1 AND occurred_at >= $2 AND occurred_at <= $3 "
            "ORDER BY occurred_at DESC LIMIT " + str(prefilter_limit)
        )
        prefilter_rows = await conn.fetch(obs_sql, tenant_id, start, end)
        obs_rows = [
            row
            for row in prefilter_rows
            if _observation_record_matches_scope(
                row,
                actor_ids=actor_ids,
                entity_list=entity_list,
                include_entity_mentions=include_entity_mentions,
            )
        ][: int(max_observations)]
        notes["observations_prefilter_limit"] = prefilter_limit
        notes["observations_prefilter_rows"] = len(prefilter_rows)
        notes["observations_scope_filtered_in_python"] = True
    else:
        # Observations query — tenant + time-range; optional actor/entity filter.
        obs_sql = (
            f"SELECT {_OBS_SELECT_SQL} FROM observations "
            "WHERE tenant_id = $1 AND occurred_at >= $2 AND occurred_at <= $3"
        )
        obs_params: list[Any] = [tenant_id, start, end]
        obs_scope_clauses: list[str] = []
        if actor_ids:
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
        notes["observations_scope_filtered_in_python"] = False
    timings_ms["observations_query_ms"] = _elapsed_ms(obs_query_started)
    obs_hydrate_started = time.perf_counter()
    observations = _hydrate_many(obs_rows, _hydrate_obs, notes, "observations")
    timings_ms["observations_hydrate_ms"] = _elapsed_ms(obs_hydrate_started)

    # Models in the window (active). Overlap is COALESCE(last_retrieved_at,
    # created_at) — if a Model has been reconsolidated inside the window
    # it is also relevant, otherwise fall back to birth time.
    model_sql = (
        f"SELECT {_MODEL_SELECT_SQL} FROM models "
        "WHERE tenant_id = $1 AND status = 'active' "
        "  AND COALESCE(last_retrieved_at, created_at) >= $2 "
        "  AND COALESCE(last_retrieved_at, created_at) <= $3"
    )
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
    model_sql += " ORDER BY COALESCE(last_retrieved_at, created_at) DESC LIMIT " + str(
        max(1, int(max_models))
    )
    model_query_started = time.perf_counter()
    model_rows = await conn.fetch(model_sql, *model_params)
    timings_ms["models_query_ms"] = _elapsed_ms(model_query_started)
    model_hydrate_started = time.perf_counter()
    models = _hydrate_many(model_rows, _hydrate_model, notes, "models")
    timings_ms["models_hydrate_ms"] = _elapsed_ms(model_hydrate_started)

    notes["observations_returned"] = len(observations)
    notes["models_returned"] = len(models)
    notes["temporal_timings_ms"] = timings_ms

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
                AND e.review_status IN ('accepted', 'candidate', 'needs_review', 'disputed')
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


@dataclass
class _PathwayGScopedSeeds:
    entity_types: list[str]
    entity_ids: list[UUID]


@dataclass
class _PathwayGWalk:
    visited: set[UUID]
    rank_by_model: dict[UUID, tuple[int, int, str]]
    edge_rows_seen: int = 0
    composition_rows_seen: int = 0


def _pathway_g_notes(
    *,
    seed_model_ids: Sequence[UUID] | None,
    seed_entity_ids: Sequence[dict[str, Any]] | None,
    scope_actors: Sequence[UUID] | None,
    edge_kinds: Sequence[str],
    max_hops: int,
    limit: int,
) -> dict[str, Any]:
    return {
        "seed_model_ids": len(seed_model_ids or []),
        "seed_entity_ids": len(seed_entity_ids or []),
        "scope_actors": len(scope_actors or []),
        "edge_kinds": list(edge_kinds),
        "max_hops": max_hops,
        "limit": limit,
    }


def _pathway_g_scoped_seeds(
    seed_entity_ids: Sequence[dict[str, Any]] | None,
) -> _PathwayGScopedSeeds:
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
    return _PathwayGScopedSeeds(entity_types=entity_types, entity_ids=entity_ids)


async def _pathway_g_active_seed_candidates(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_ids: Sequence[UUID],
    limit: int,
) -> list[asyncpg.Record]:
    if not model_ids:
        return []
    return list(
        await conn.fetch(
            """
            SELECT id, activation, created_at
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND id = ANY($2::uuid[])
            ORDER BY activation DESC, created_at DESC
            LIMIT $3
            """,
            tenant_id,
            list(model_ids),
            limit,
        )
    )


async def _pathway_g_entity_seed_candidates(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    scoped_seeds: _PathwayGScopedSeeds,
    seed_model_limit: int,
) -> list[asyncpg.Record]:
    candidates: list[asyncpg.Record] = []
    for entity_type, entity_id in zip(
        scoped_seeds.entity_types,
        scoped_seeds.entity_ids,
        strict=False,
    ):
        rows = await conn.fetch(
            """
            SELECT model_id
            FROM model_scope_entities
            WHERE tenant_id = $1
              AND entity_type = $2
              AND entity_id = $3
            """,
            tenant_id,
            entity_type,
            entity_id,
        )
        candidates.extend(
            await _pathway_g_active_seed_candidates(
                conn,
                tenant_id=tenant_id,
                model_ids=[row["model_id"] for row in rows],
                limit=seed_model_limit,
            )
        )
    return candidates


async def _pathway_g_actor_seed_candidates(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    scope_actors: Sequence[UUID] | None,
    seed_model_limit: int,
) -> list[asyncpg.Record]:
    candidates: list[asyncpg.Record] = []
    for actor_id in scope_actors or []:
        rows = await conn.fetch(
            """
            SELECT model_id
            FROM model_scope_actors
            WHERE tenant_id = $1
              AND actor_id = $2
            """,
            tenant_id,
            actor_id,
        )
        candidates.extend(
            await _pathway_g_active_seed_candidates(
                conn,
                tenant_id=tenant_id,
                model_ids=[row["model_id"] for row in rows],
                limit=seed_model_limit,
            )
        )
    return candidates


def _pathway_g_rank_scoped_seed_ids(
    candidates: list[asyncpg.Record],
    *,
    limit: int,
) -> list[UUID]:
    candidates.sort(
        key=lambda row: (
            -float(row["activation"] or 0.0),
            -float(row["created_at"].timestamp() if row["created_at"] else 0.0),
            str(row["id"]),
        )
    )
    ranked: list[UUID] = []
    seen: set[UUID] = set()
    for row in candidates:
        model_id = row["id"]
        if model_id in seen:
            continue
        seen.add(model_id)
        ranked.append(model_id)
        if len(ranked) >= limit:
            break
    return ranked


async def _pathway_g_scoped_seed_ids(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    scoped_seeds: _PathwayGScopedSeeds,
    scope_actors: Sequence[UUID] | None,
    limit: int,
) -> list[UUID]:
    if not scoped_seeds.entity_ids and not scope_actors:
        return []
    seed_model_limit = min(limit, 50)
    candidates = await _pathway_g_entity_seed_candidates(
        conn,
        tenant_id=tenant_id,
        scoped_seeds=scoped_seeds,
        seed_model_limit=seed_model_limit,
    )
    candidates.extend(
        await _pathway_g_actor_seed_candidates(
            conn,
            tenant_id=tenant_id,
            scope_actors=scope_actors,
            seed_model_limit=seed_model_limit,
        )
    )
    return _pathway_g_rank_scoped_seed_ids(candidates, limit=seed_model_limit)


async def _pathway_g_composition_candidates(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    frontier: set[UUID],
    limit: int,
) -> tuple[list[tuple[UUID, int, str]], int]:
    rows = await conn.fetch(
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
    candidates: list[tuple[UUID, int, str]] = []
    for pos, row in enumerate(rows):
        composite = row["composite_model_id"]
        member = row["member_model_id"]
        if composite in frontier:
            candidates.append((member, pos, "composition_member"))
        if member in frontier:
            candidates.append((composite, pos, "composition_parent"))
    return candidates, len(rows)


async def _pathway_g_edge_candidates(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    edge_kinds: Sequence[str],
    frontier: set[UUID],
    limit: int,
    position_offset: int,
) -> list[tuple[UUID, int, str]]:
    rows = await conn.fetch(
        """
        SELECT source_model_id, target_model_id, edge_kind, confidence,
               weight, review_status
        FROM model_edges
        WHERE tenant_id = $1
          AND status = 'active'
          AND review_status IN ('accepted', 'candidate', 'needs_review', 'disputed')
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
        list(edge_kinds),
        list(frontier),
        limit * 4,
    )
    candidates: list[tuple[UUID, int, str]] = []
    for pos, row in enumerate(rows, start=position_offset):
        source = row["source_model_id"]
        target = row["target_model_id"]
        other = target if source in frontier else source
        candidates.append((other, pos, row["edge_kind"]))
    return candidates


def _pathway_g_next_frontier(
    *,
    candidates: list[tuple[UUID, int, str]],
    visited: set[UUID],
    rank_by_model: dict[UUID, tuple[int, int, str]],
    hop: int,
    limit: int,
) -> set[UUID]:
    next_frontier: set[UUID] = set()
    for other, pos, relation_kind in candidates:
        if other in visited:
            continue
        visited.add(other)
        next_frontier.add(other)
        rank_by_model[other] = (hop, pos, relation_kind)
        if len(visited) >= limit:
            break
    return next_frontier


async def _pathway_g_walk(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    seeds: set[UUID],
    edge_kinds: Sequence[str],
    max_hops: int,
    limit: int,
) -> _PathwayGWalk:
    walk = _PathwayGWalk(
        visited=set(seeds),
        rank_by_model={mid: (0, 0, "seed") for mid in seeds},
    )
    frontier: set[UUID] = set(seeds)
    for hop in range(1, max_hops + 1):
        if not frontier or len(walk.visited) >= limit:
            break
        candidates, composition_rows_seen = await _pathway_g_composition_candidates(
            conn,
            tenant_id=tenant_id,
            frontier=frontier,
            limit=limit,
        )
        walk.composition_rows_seen += composition_rows_seen
        edge_candidates = await _pathway_g_edge_candidates(
            conn,
            tenant_id=tenant_id,
            edge_kinds=edge_kinds,
            frontier=frontier,
            limit=limit,
            position_offset=len(candidates),
        )
        walk.edge_rows_seen += len(edge_candidates)
        candidates.extend(edge_candidates)
        frontier = _pathway_g_next_frontier(
            candidates=candidates,
            visited=walk.visited,
            rank_by_model=walk.rank_by_model,
            hop=hop,
            limit=limit,
        )
    return walk


async def _pathway_g_hydrate_models(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    walk: _PathwayGWalk,
    notes: dict[str, Any],
    limit: int,
) -> list[ModelRow]:
    model_rows = await conn.fetch(
        f"""
        SELECT {_MODEL_SELECT_SQL}
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        list(walk.visited),
    )
    models = _hydrate_many(model_rows, _hydrate_model, notes, "edge_models")
    models.sort(
        key=lambda m: (
            walk.rank_by_model.get(m.id, (999, 999, ""))[0],
            walk.rank_by_model.get(m.id, (999, 999, ""))[1],
            -m.activation,
            str(m.id),
        )
    )
    return models[:limit]


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
    notes = _pathway_g_notes(
        seed_model_ids=seed_model_ids,
        seed_entity_ids=seed_entity_ids,
        scope_actors=scope_actors,
        edge_kinds=edge_kinds,
        max_hops=max_hops,
        limit=limit,
    )
    if max_hops < 0:
        raise ValidationError("max_hops must be >= 0", max_hops=max_hops)
    if limit <= 0:
        return PathwayResult(
            source_pathway="G", notes={**notes, "reason": "non_positive_limit"}
        )

    seeds: set[UUID] = set(seed_model_ids or [])
    scoped_seed_ids = await _pathway_g_scoped_seed_ids(
        conn,
        tenant_id=tenant_id,
        scoped_seeds=_pathway_g_scoped_seeds(seed_entity_ids),
        scope_actors=scope_actors,
        limit=limit,
    )
    if scoped_seed_ids:
        seeds.update(scoped_seed_ids)
        notes["scope_seed_models"] = len(scoped_seed_ids)

    if not seeds:
        return PathwayResult(
            source_pathway="G", notes={**notes, "reason": "empty_seed"}
        )

    walk = await _pathway_g_walk(
        conn,
        tenant_id=tenant_id,
        seeds=seeds,
        edge_kinds=[str(k) for k in edge_kinds],
        max_hops=max_hops,
        limit=limit,
    )
    if not walk.visited:
        return PathwayResult(
            source_pathway="G", notes={**notes, "reason": "no_reachable_models"}
        )

    models = await _pathway_g_hydrate_models(
        conn,
        tenant_id=tenant_id,
        walk=walk,
        notes=notes,
        limit=limit,
    )
    notes["edge_rows_seen"] = walk.edge_rows_seen
    notes["composition_rows_seen"] = walk.composition_rows_seen
    notes["models_returned"] = len(models)
    notes["hops_executed"] = max(
        (walk.rank_by_model.get(m.id, (0, 0, ""))[0] for m in models), default=0
    )

    return PathwayResult(
        models=models,
        observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[],
        source_pathway="G",
        notes=notes,
    )


__all__ = [
    "ModelCandidateHit",
    "PathwayResult",
    "PathwayName",
    "hydrate_active_models_by_ids",
    "pathway_a_structural",
    "pathway_b_semantic",
    "pathway_b_representation_tags",
    "pathway_b_representation_tag_candidates",
    "pathway_l_semantic_terms",
    "pathway_l_semantic_term_candidates",
    "pathway_c_temporal",
    "pathway_d_pattern",
    "pathway_g_model_edges",
    "RetrievalPathwayError",
]
