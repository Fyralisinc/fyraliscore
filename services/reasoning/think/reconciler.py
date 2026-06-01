"""services/reasoning/think/reconciler.py — content-level Model dedup at insert time.

T5: explicit reconciliation as a first-class pipeline step. See
services/reasoning/think/RECONCILIATION_DESIGN.md for the design and
RECONCILIATION_README.md for the operator-facing surface.

The pipeline runs this between validate and apply:

  trigger → retrieve → reason → validate
          → reconcile_claim_op  ◀── this module
          → apply → cascade

For each `claim_op.insert` proposed by the LLM, the reconciler:

  1. Looks for existing active Models in the same tenant that match
     on FOUR signals: embedding cosine >= HUMAN_REVIEW_COSINE,
     overlapping scope, identical proposition_kind, and created
     within the recency window. If the LLM omitted an embedding, the
     reconciler uses the same deterministic fallback the applier uses
     before insert, so strict-schema live output can still deduplicate.
  2. Scores each candidate by combining cosine with graph-structural
     signals (shared evidence events, shared supporting_model_ids,
     shared falsifier semantics).
  3. Decides:
       * adjusted-score ≥ AUTO_MERGE_COSINE (or per-kind override)
                                     → 'auto_merge': convert the
         insert to a confidence update against the matched Model.
       * adjusted-score in [HUMAN_REVIEW, AUTO) → 'human_review':
         record the candidate in `reconciliation_events` for human
         review AND emit a `same_issue_as` relationship candidate so
         the duplicate suspicion is visible to T4 / adjudication.
         The original insert still proceeds.
       * no match in window             → 'no_match': pass through
         unchanged.
  4. Records the decision in `reconciliation_events`.

Per-kind overrides (see `_KIND_RULES`) tighten thresholds for
proposition kinds that paraphrase heavily (market_assessment,
concern) and forbid auto-merge for kinds that require human
confirmation (recommendation). `situation` and `pattern_instance`
get bespoke matching rules.

Reconciliation is opt-out via env `RECONCILE_ENABLED=false`. The
reconciler MUST never abort apply: any internal exception is
logged as `reconcile.error` and the original `claim_op` proceeds.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.memory_grammar import derive_memory_grammar
from services.domain.models.propositions import canonicalize_proposition

from .diff_schema import ClaimOp
from .text_embedding import deterministic_text_embedding, is_zero_embedding


_log = structlog.get_logger(__name__)


# =====================================================================
# Configuration
# =====================================================================
#
# Defaults are conservative starting points (see design §"Decision
# thresholds"). Empirical tuning will move these. Reading env on
# every call rather than at module import so an operator can flip
# the kill switch without restarting.


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "y", "t")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class ReconcilerConfig:
    enabled: bool
    auto_merge_cosine: float
    human_review_cosine: float
    recency_window_days: int
    log_no_match: bool

    @classmethod
    def from_env(cls) -> "ReconcilerConfig":
        return cls(
            enabled=_env_bool("RECONCILE_ENABLED", True),
            auto_merge_cosine=_env_float("RECONCILE_AUTO_MERGE_COSINE", 0.85),
            human_review_cosine=_env_float("RECONCILE_HUMAN_REVIEW_COSINE", 0.70),
            recency_window_days=_env_int("RECONCILE_RECENCY_WINDOW_DAYS", 30),
            log_no_match=_env_bool("RECONCILE_LOG_NO_MATCH", True),
        )


Decision = Literal["auto_merge", "human_review", "no_match", "skipped"]
DecisionReason = Literal[
    "exact_match",
    "high_cosine_auto_merge",
    "kind_specific_auto_merge",
    "graph_signal_boost",
    "near_duplicate_review",
    "same_issue_candidate_emitted",
    "no_match",
    "kind_blocked_auto_merge",
    "disabled",
    "inapplicable",
    "error",
]


@dataclass
class ReconcileResult:
    """Outcome of a single reconcile decision.

    `replacement_op` is set when `decision == "auto_merge"`: the
    caller should apply this op instead of the original insert.
    For all other decisions it is None and the caller proceeds
    with the original op.

    `decision_reason` and `signal_breakdown` are observability
    surfaces — they explain *why* the decision came out the way it
    did, including which graph-structural signals contributed.
    `same_issue_candidate_id` is set when the borderline branch
    emitted a `same_issue_as` RelationshipCandidate.
    """
    decision: Decision
    matched_model_id: UUID | None
    cosine_similarity: float | None
    replacement_op: ClaimOp | None
    event_id: UUID | None  # row id in reconciliation_events, if written
    decision_reason: DecisionReason = "no_match"
    signal_breakdown: dict[str, float] = field(default_factory=dict)
    same_issue_candidate_id: UUID | None = None


# =====================================================================
# Cosine similarity helper
# =====================================================================


def _cosine(a: list[float], b: list[float]) -> float:
    """Cosine similarity over two equal-length float lists.

    pgvector's `1 - (a <=> b)` gives the same value at the SQL layer;
    we duplicate the math in Python because the candidate vector
    we score against may have come from a different source than the
    in-DB vector and we want to compute the score outside the DB
    too (cleaner attribution, easier to test).

    Returns 0.0 for any zero-norm vector — the L2-normalized vectors
    we expect from `nomic-embed-text` should not produce this case
    in practice.
    """
    if len(a) != len(b):
        return 0.0
    dot = 0.0
    norm_a = 0.0
    norm_b = 0.0
    for x, y in zip(a, b):
        dot += x * y
        norm_a += x * x
        norm_b += y * y
    if norm_a <= 0.0 or norm_b <= 0.0:
        return 0.0
    import math
    return dot / (math.sqrt(norm_a) * math.sqrt(norm_b))


def _candidate_embedding(entry: dict[str, Any]) -> list[float]:
    """Return a vector usable against the `models.embedding` index.

    Live strict-schema providers are instructed not to emit embeddings. The
    applier would fill a deterministic lexical fallback right before insert;
    reconciliation needs the same fallback earlier or it cannot stop duplicate
    inserts.
    """
    raw = entry.get("embedding")
    if hasattr(raw, "tolist"):
        raw = raw.tolist()
    if isinstance(raw, (list, tuple)) and len(raw) == 768:
        try:
            vec = [float(x) for x in raw]
            if not is_zero_embedding(vec):
                return vec
        except (TypeError, ValueError):
            pass
    natural = str(
        entry.get("natural")
        or json.dumps(entry.get("proposition") or {}, sort_keys=True)
    )
    return deterministic_text_embedding(natural)


def _coerce_uuid(value: Any) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _normalize_uuid_list(value: Any) -> list[UUID]:
    out: list[UUID] = []
    if not isinstance(value, (list, tuple)):
        return out
    for item in value:
        mid = _coerce_uuid(item)
        if mid is not None and mid not in out:
            out.append(mid)
    return out


def _append_uuid_once(values: list[UUID], item: UUID) -> list[UUID]:
    return values if item in values else [*values, item]


def _normalize_signal_readings(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, (bytes, bytearray)):
        try:
            value = json.loads(value.decode())
        except (ValueError, UnicodeDecodeError):
            return []
    elif isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return []
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def _confirmation_reading(
    entry: dict[str, Any],
    source_event_id: UUID,
    observed_at: datetime,
) -> dict[str, Any]:
    reading: dict[str, Any] = {
        "kind": "confirm",
        "at": observed_at.isoformat(),
        "source_event_id": str(source_event_id),
        "confidence": float(entry.get("confidence", 0.5)),
        "natural": str(entry.get("natural") or "")[:500],
    }
    scope_actors = entry.get("scope_actors") or []
    if scope_actors:
        actor_id = _coerce_uuid(scope_actors[0])
        if actor_id is not None:
            reading["actor_id"] = str(actor_id)
    return reading


# =====================================================================
# Per-kind reconciliation rules
# =====================================================================
#
# Each rule may:
#   * override the default auto_merge / human_review thresholds for
#     its kind,
#   * forbid auto_merge entirely (never_auto_merge=True),
#   * supply custom match_score / qualifies functions that augment or
#     replace the default cosine + scope check.
#
# A rule returning False from `qualifies` means the candidate pair is
# not a duplicate for that kind even if cosine is high (e.g. two
# pattern_instances of different parent patterns).


@dataclass(frozen=True)
class KindRule:
    auto_merge_cosine: float | None = None
    human_review_cosine: float | None = None
    never_auto_merge: bool = False
    # Member-overlap shortcut for `situation`: when set, the rule
    # auto-merges if `>= auto_member_overlap` fraction of member ids
    # are shared, regardless of cosine.
    auto_member_overlap: float | None = None
    # 0.50-0.80 member overlap on situations should emit a
    # same_issue_as candidate even if cosine is below human_review.
    same_issue_member_overlap_floor: float | None = None
    # Recommendation: always require human confirmation.
    require_human_review: bool = False
    # If True, the rule disqualifies pairs whose parent pattern_id
    # does not match (used by pattern_instance).
    require_matching_pattern_id: bool = False


_KIND_RULES: dict[str, KindRule] = {
    # Market assessments paraphrase heavily ("AWS outage drives
    # multicloud demand" vs "AWS reliability concerns push customers
    # to multicloud"). Match on (entity OR market) within a 30-day
    # timeframe (already enforced by recency window + scope filter).
    # Lower the auto_merge bar to 0.75.
    "market_assessment": KindRule(
        auto_merge_cosine=0.75,
        human_review_cosine=0.65,
    ),
    # Concerns about the same workstream + activation period get
    # re-stated in different words. Lower auto_merge to 0.78.
    "concern": KindRule(
        auto_merge_cosine=0.78,
        human_review_cosine=0.65,
    ),
    # Recommendations are action-oriented and need a human in the
    # loop. NEVER auto_merge — even at high cosine, queue to
    # pending_reconciliation for review.
    "recommendation": KindRule(
        never_auto_merge=True,
        require_human_review=True,
    ),
    # Situation reconciliation is dominated by member overlap rather
    # than text similarity. ≥80% member overlap → auto_merge. 50-80%
    # → same_issue_as candidate emission.
    "situation": KindRule(
        auto_member_overlap=0.80,
        same_issue_member_overlap_floor=0.50,
    ),
    # Pattern instances are only duplicates if they share the parent
    # pattern_id.
    "pattern_instance": KindRule(
        require_matching_pattern_id=True,
    ),
}


def _kind_rule(prop_kind: str | None) -> KindRule:
    if prop_kind and prop_kind in _KIND_RULES:
        return _KIND_RULES[prop_kind]
    return KindRule()


def _semantic_rule_key(proposition: dict[str, Any] | None) -> str | None:
    if not isinstance(proposition, dict):
        return None
    legacy = proposition.get("legacy_kind")
    if isinstance(legacy, str) and legacy in _KIND_RULES:
        return legacy
    grammar = derive_memory_grammar(proposition)
    if grammar.claim_role in _KIND_RULES:
        return grammar.claim_role
    return proposition.get("kind") if isinstance(proposition.get("kind"), str) else None


# =====================================================================
# Graph-structural signal helpers
# =====================================================================


def _entry_supporting_event_ids(entry: dict[str, Any]) -> set[UUID]:
    ids: set[UUID] = set()
    for key in ("supporting_event_ids", "evidence_event_ids"):
        for v in entry.get(key) or []:
            uid = _coerce_uuid(v)
            if uid is not None:
                ids.add(uid)
    born = _coerce_uuid(entry.get("born_from_event_id"))
    if born is not None:
        ids.add(born)
    return ids


def _entry_supporting_model_ids(entry: dict[str, Any]) -> set[UUID]:
    ids: set[UUID] = set()
    for v in entry.get("supporting_model_ids") or []:
        uid = _coerce_uuid(v)
        if uid is not None:
            ids.add(uid)
    return ids


def _row_supporting_event_ids(row: dict[str, Any]) -> set[UUID]:
    return set(_normalize_uuid_list(row.get("supporting_event_ids")))


def _row_supporting_model_ids(row: dict[str, Any]) -> set[UUID]:
    return set(_normalize_uuid_list(row.get("supporting_model_ids")))


def _normalize_jsonish(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        try:
            return json.loads(value.decode())
        except (ValueError, UnicodeDecodeError):
            return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return None
    return value


def _falsifier_text(value: Any) -> str:
    obj = _normalize_jsonish(value)
    if not isinstance(obj, dict):
        return ""
    parts: list[str] = []
    for key in ("pattern", "condition", "natural", "description"):
        v = obj.get(key)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())
    return " ".join(parts)


def _falsifier_cosine(entry: dict[str, Any], row: dict[str, Any]) -> float:
    """Cosine of deterministic embeddings over the two falsifier blobs."""
    cand_text = _falsifier_text(entry.get("falsifier"))
    row_text = _falsifier_text(row.get("falsifier"))
    if not cand_text or not row_text:
        return 0.0
    cand_vec = deterministic_text_embedding(cand_text)
    row_vec = deterministic_text_embedding(row_text)
    return _cosine(cand_vec, row_vec)


def _member_model_ids(payload: Any) -> set[UUID]:
    """Pull `member_model_ids` from a situation proposition payload."""
    obj = _normalize_jsonish(payload)
    if not isinstance(obj, dict):
        return set()
    raw = obj.get("member_model_ids") or []
    out: set[UUID] = set()
    if isinstance(raw, (list, tuple)):
        for v in raw:
            uid = _coerce_uuid(v)
            if uid is not None:
                out.add(uid)
    return out


def _member_overlap_fraction(left: set[UUID], right: set[UUID]) -> float:
    if not left or not right:
        return 0.0
    smaller = min(len(left), len(right))
    if smaller == 0:
        return 0.0
    return len(left & right) / float(smaller)


def _pattern_id(payload: Any) -> UUID | None:
    obj = _normalize_jsonish(payload)
    if not isinstance(obj, dict):
        return None
    for key in ("pattern_id", "parent_pattern_id", "pattern"):
        uid = _coerce_uuid(obj.get(key))
        if uid is not None:
            return uid
    return None


def _compute_signal_breakdown(
    entry: dict[str, Any],
    row: dict[str, Any],
    base_cosine: float,
) -> tuple[float, dict[str, float]]:
    """Apply graph-structural boosts to the base cosine score.

    Returns (adjusted_score, breakdown) where breakdown maps the
    contributing signal names to their numeric boost / value.
    """
    breakdown: dict[str, float] = {"cosine": float(base_cosine)}
    boost = 0.0

    cand_events = _entry_supporting_event_ids(entry)
    row_events = _row_supporting_event_ids(row)
    shared_events = cand_events & row_events
    if shared_events:
        breakdown["shared_evidence_events"] = float(len(shared_events))
        boost += 0.10

    cand_models = _entry_supporting_model_ids(entry)
    row_models = _row_supporting_model_ids(row)
    shared_models = cand_models & row_models
    if len(shared_models) >= 2:
        breakdown["shared_supporting_models"] = float(len(shared_models))
        boost += 0.05

    fcos = _falsifier_cosine(entry, row)
    if fcos >= 0.80:
        breakdown["falsifier_cosine"] = float(fcos)
        boost += 0.05

    adjusted = min(1.0, float(base_cosine) + boost)
    if boost > 0:
        breakdown["graph_boost"] = float(boost)
    breakdown["adjusted_score"] = float(adjusted)
    return adjusted, breakdown


# =====================================================================
# Candidate search
# =====================================================================


async def _find_candidates(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    candidate_embedding: list[float],
    candidate_scope_actors: list[str],
    candidate_scope_entities: list[dict[str, Any]],
    proposition_kind: str | None,
    claim_role: str | None,
    recency_window_days: int,
    k: int = 5,
) -> list[dict[str, Any]]:
    """Query the `models` table for the top-k semantically nearest
    candidates within the same tenant + recency window + proposition
    kind, with at least one overlapping scope element.

    Returns a list of dicts with `id`, `embedding`, `scope_actors`,
    `scope_entities`, `confidence`, `proposition_kind`, `natural`,
    `created_at`. Cosine is computed in Python by the caller against
    `candidate_embedding`.
    """
    # We do scope filtering in SQL (cheaper to throw out non-overlap
    # candidates server-side) and let the caller compute cosine.
    # Recency is `created_at >= now() - interval`.
    where = [
        "status = 'active'",
        "tenant_id = $1",
        "created_at >= now() - ($2::int * interval '1 day')",
    ]
    params: list[Any] = [tenant_id, recency_window_days]

    if proposition_kind is not None:
        params.append(proposition_kind)
        where.append(f"proposition_kind = ${len(params)}")
    if claim_role is not None:
        params.append(claim_role)
        where.append(f"claim_role = ${len(params)}")

    # Scope predicate: at least one of the two dimensions overlaps.
    # We OR them so a Model that lists the candidate's scope_entities
    # but no overlapping scope_actors still qualifies, and vice
    # versa. Empty-set candidates fall through to "no scope filter"
    # — in that degenerate case the reconciler decides on
    # text + kind alone, which is intentional.
    scope_clauses: list[str] = []
    if candidate_scope_actors:
        params.append(candidate_scope_actors)
        scope_clauses.append(
            f"scope_actors && ${len(params)}::uuid[]"
        )
    if candidate_scope_entities:
        # Need at least one of the entity tuples to appear in the
        # existing Model's scope_entities. The simplest predicate is
        # `scope_entities @> $N::jsonb`, but @> requires the LEFT
        # side to contain *all* of the right-side. We want "any of"
        # — so OR an @> clause per candidate entity.
        for ent in candidate_scope_entities:
            params.append(json.dumps([ent]))
            scope_clauses.append(
                f"scope_entities @> ${len(params)}::jsonb"
            )
    if scope_clauses:
        where.append("(" + " OR ".join(scope_clauses) + ")")

    sql = f"""
        SELECT id, embedding, scope_actors, scope_entities,
               confidence, proposition_kind, "natural", created_at,
               supporting_event_ids, signal_readings, confirmed_count,
               supporting_model_ids, falsifier, proposition
        FROM models
        WHERE {' AND '.join(where)}
        ORDER BY embedding <=> $LIMITSEED::vector
        LIMIT {int(k)}
    """
    # ORDER BY needs the candidate vector. Push it on as the last
    # numbered param. We use a placeholder marker because the
    # embedding bind format depends on the connection's codec
    # state — see services/domain/models/PGVECTOR_REGISTRY.md.
    params.append(candidate_embedding)
    sql = sql.replace("$LIMITSEED", f"${len(params)}")

    rows = await conn.fetch(sql, *params)
    return [dict(r) for r in rows]


# =====================================================================
# Audit row
# =====================================================================


async def _record_event(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    decision: Decision,
    original_claim_op: ClaimOp,
    matched_model_id: UUID | None,
    cosine_similarity: float | None,
    proposition_kind: str | None,
    trigger_id: UUID,
    think_run_id: UUID | None,
) -> UUID:
    event_id = uuid7()
    await conn.execute(
        """
        INSERT INTO reconciliation_events (
            id, tenant_id, decision, original_claim_op,
            matched_model_id, cosine_similarity, proposition_kind,
            trigger_id, think_run_id
        ) VALUES (
            $1, $2, $3, $4::jsonb,
            $5, $6, $7, $8, $9
        )
        """,
        event_id,
        tenant_id,
        decision,
        json.dumps(original_claim_op.model_dump(mode="json"), default=str),
        matched_model_id,
        cosine_similarity,
        proposition_kind,
        trigger_id,
        think_run_id,
    )
    return event_id


# =====================================================================
# Public entry point
# =====================================================================


async def reconcile_claim_op(
    op: ClaimOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    trigger_id: UUID,
    think_run_id: UUID | None = None,
    config: ReconcilerConfig | None = None,
) -> ReconcileResult:
    """Decide what to do with a single claim_op.insert.

    Caller invariants:
      * Caller MUST have already passed `op` through the validator.
      * Caller MUST be inside the apply transaction so this
        decision is serialized with the eventual apply.
      * Caller is responsible for handling the result:
          - `decision="auto_merge"`: apply `replacement_op` instead
            of the original.
          - `decision="human_review"` or `"no_match"`: apply the
            original `op` unchanged.
          - `decision="skipped"`: reconciler was disabled or
            inapplicable; apply original.

    Reconciler-internal failures are caught and surfaced as
    `decision="skipped"` so apply never aborts on our account.
    """
    cfg = config or ReconcilerConfig.from_env()
    if not cfg.enabled:
        return ReconcileResult(
            decision="skipped",
            matched_model_id=None,
            cosine_similarity=None,
            replacement_op=None,
            event_id=None,
            decision_reason="disabled",
        )
    if op.op != "insert" or op.entry is None:
        return ReconcileResult(
            decision="skipped",
            matched_model_id=None,
            cosine_similarity=None,
            replacement_op=None,
            event_id=None,
            decision_reason="inapplicable",
        )

    try:
        async with conn.transaction():
            return await _reconcile_inner(
                op, conn,
                tenant_id=tenant_id,
                trigger_id=trigger_id,
                think_run_id=think_run_id,
                config=cfg,
            )
    except Exception as exc:  # noqa: BLE001
        # Reconciler must never abort apply. Log loudly and pass through.
        # The nested transaction above rolls back any failed DB work to a
        # savepoint so the main apply transaction remains usable.
        _log.warning(
            "reconcile.error",
            error=str(exc),
            error_type=type(exc).__name__,
            trigger_id=str(trigger_id),
        )
        return ReconcileResult(
            decision="skipped",
            matched_model_id=None,
            cosine_similarity=None,
            replacement_op=None,
            event_id=None,
            decision_reason="error",
            signal_breakdown={"error": 1.0},
        )


async def _reconcile_inner(
    op: ClaimOp,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    trigger_id: UUID,
    think_run_id: UUID | None,
    config: ReconcilerConfig,
) -> ReconcileResult:
    entry = op.entry or {}
    candidate_embedding = _candidate_embedding(entry)

    proposition = entry.get("proposition") or {}
    if isinstance(proposition, dict):
        try:
            proposition = canonicalize_proposition(proposition)
        except Exception:
            pass
    prop_kind = proposition.get("kind") if isinstance(proposition, dict) else None
    grammar = derive_memory_grammar(proposition if isinstance(proposition, dict) else {})
    rule_key = _semantic_rule_key(proposition if isinstance(proposition, dict) else None)
    kind_rule = _kind_rule(rule_key)

    # Per-kind threshold overrides; otherwise use global defaults.
    auto_merge_threshold = (
        kind_rule.auto_merge_cosine
        if kind_rule.auto_merge_cosine is not None
        else config.auto_merge_cosine
    )
    human_review_threshold = (
        kind_rule.human_review_cosine
        if kind_rule.human_review_cosine is not None
        else config.human_review_cosine
    )

    # scope_actors come in as either UUID strings or UUID objects.
    raw_actors = entry.get("scope_actors") or []
    candidate_scope_actors = [str(a) for a in raw_actors]
    candidate_scope_entities = [
        e for e in (entry.get("scope_entities") or []) if isinstance(e, dict)
    ]

    rows = await _find_candidates(
        conn,
        tenant_id=tenant_id,
        candidate_embedding=candidate_embedding,
        candidate_scope_actors=candidate_scope_actors,
        candidate_scope_entities=candidate_scope_entities,
        proposition_kind=prop_kind,
        claim_role=grammar.claim_role,
        recency_window_days=config.recency_window_days,
    )

    # Score each row and pick the best, applying graph-structural
    # boosts on top of the raw cosine. We track both the raw cosine
    # (for audit trail / `cosine_similarity` on the result) and the
    # adjusted score (used for threshold decisions).
    best_row: dict[str, Any] | None = None
    best_cosine: float = -1.0
    best_adjusted: float = -1.0
    best_breakdown: dict[str, float] = {}
    candidate_member_ids = _member_model_ids(entry.get("proposition"))
    candidate_pattern_id = _pattern_id(entry.get("proposition"))
    best_member_overlap: float = 0.0

    for r in rows:
        existing_emb = r.get("embedding")
        # Embedding may come back as numpy array (codec-registered)
        # or list. Normalize.
        if existing_emb is None:
            continue
        if hasattr(existing_emb, "tolist"):
            existing_emb = existing_emb.tolist()
        if not isinstance(existing_emb, list):
            continue

        # pattern_instance: require matching parent pattern_id.
        if kind_rule.require_matching_pattern_id:
            row_pattern_id = _pattern_id(r.get("proposition"))
            if (
                candidate_pattern_id is None
                or row_pattern_id is None
                or candidate_pattern_id != row_pattern_id
            ):
                continue

        cos = _cosine(candidate_embedding, list(existing_emb))
        adjusted, breakdown = _compute_signal_breakdown(entry, r, cos)

        # situation: factor member-overlap into the adjusted score.
        member_overlap = 0.0
        if grammar.claim_role == "situation" and candidate_member_ids:
            row_member_ids = _member_model_ids(r.get("proposition"))
            member_overlap = _member_overlap_fraction(
                candidate_member_ids, row_member_ids,
            )
            if member_overlap > 0:
                breakdown["situation_member_overlap"] = float(member_overlap)
                # Promote overlap into the adjusted score so the
                # decision branch can short-circuit. We cap at 1.0.
                adjusted = max(adjusted, min(1.0, member_overlap))
                breakdown["adjusted_score"] = float(adjusted)

        if adjusted > best_adjusted:
            best_adjusted = adjusted
            best_cosine = cos
            best_row = r
            best_breakdown = breakdown
            best_member_overlap = member_overlap

    if best_row is None or best_adjusted < human_review_threshold:
        # Situation may still cross the same_issue floor on member
        # overlap alone even when adjusted score is sub-threshold.
        situation_floor = kind_rule.same_issue_member_overlap_floor
        if (
            best_row is not None
            and situation_floor is not None
            and best_member_overlap >= situation_floor
        ):
            # Fall through to the borderline branch below — overlap
            # alone qualifies for same_issue_as emission.
            pass
        else:
            event_id: UUID | None = None
            if config.log_no_match:
                event_id = await _record_event(
                    conn,
                    tenant_id=tenant_id,
                    decision="no_match",
                    original_claim_op=op,
                    matched_model_id=None,
                    cosine_similarity=(
                        best_cosine if best_cosine >= 0.0 else None
                    ),
                    proposition_kind=prop_kind,
                    trigger_id=trigger_id,
                    think_run_id=think_run_id,
                )
            _emit_metric("no_match")
            _log.info(
                "reconcile.decision",
                decision="no_match",
                cosine=best_cosine if best_cosine >= 0.0 else None,
                trigger_id=str(trigger_id),
            )
            return ReconcileResult(
                decision="no_match",
                matched_model_id=None,
                cosine_similarity=(
                    best_cosine if best_cosine >= 0.0 else None
                ),
                replacement_op=None,
                event_id=event_id,
                decision_reason="no_match",
                signal_breakdown=best_breakdown,
            )

    matched_id: UUID = best_row["id"]
    used_threshold = auto_merge_threshold

    # ---- Situation member-overlap shortcut -------------------------
    member_overlap_auto = (
        grammar.claim_role == "situation"
        and kind_rule.auto_member_overlap is not None
        and best_member_overlap >= kind_rule.auto_member_overlap
    )

    # ---- Auto-merge branch -----------------------------------------
    can_auto_merge = (
        not kind_rule.never_auto_merge
        and not kind_rule.require_human_review
        and (best_adjusted >= used_threshold or member_overlap_auto)
    )
    if can_auto_merge:
        replacement = _build_auto_merge_replacement(
            entry, best_row, matched_id,
        )
        event_id = await _record_event(
            conn,
            tenant_id=tenant_id,
            decision="auto_merge",
            original_claim_op=op,
            matched_model_id=matched_id,
            cosine_similarity=best_cosine,
            proposition_kind=prop_kind,
            trigger_id=trigger_id,
            think_run_id=think_run_id,
        )
        _emit_metric("auto_merge")
        _log.info(
            "reconcile.decision",
            decision="auto_merge",
            cosine=best_cosine,
            adjusted=best_adjusted,
            matched_model_id=str(matched_id),
            trigger_id=str(trigger_id),
        )
        # Distinguish "graph signals pushed us over" vs "kind-specific
        # threshold did" vs "default high-cosine path".
        if member_overlap_auto:
            decision_reason: DecisionReason = "kind_specific_auto_merge"
        elif (
            best_breakdown.get("graph_boost", 0.0) > 0.0
            and best_cosine < used_threshold
        ):
            decision_reason = "graph_signal_boost"
        elif (
            kind_rule.auto_merge_cosine is not None
            and best_cosine < config.auto_merge_cosine
        ):
            decision_reason = "kind_specific_auto_merge"
        elif best_cosine >= 0.999:
            decision_reason = "exact_match"
        else:
            decision_reason = "high_cosine_auto_merge"
        return ReconcileResult(
            decision="auto_merge",
            matched_model_id=matched_id,
            cosine_similarity=best_cosine,
            replacement_op=replacement,
            event_id=event_id,
            decision_reason=decision_reason,
            signal_breakdown=best_breakdown,
        )

    # ---- Borderline / human-review branch --------------------------
    # Log to the human-review queue, proceed with the original insert.
    # NEW: also emit a `same_issue_as` RelationshipCandidate so the
    # duplicate suspicion is visible to T4 / adjudication.
    event_id = await _record_event(
        conn,
        tenant_id=tenant_id,
        decision="human_review",
        original_claim_op=op,
        matched_model_id=matched_id,
        cosine_similarity=best_cosine,
        proposition_kind=prop_kind,
        trigger_id=trigger_id,
        think_run_id=think_run_id,
    )
    _emit_metric("human_review")

    same_issue_candidate_id = await _emit_same_issue_candidate(
        conn,
        tenant_id=tenant_id,
        matched_model_id=matched_id,
        cosine=best_cosine,
        adjusted=best_adjusted,
        breakdown=best_breakdown,
        prop_kind=prop_kind,
        candidate_entry=entry,
        kind_rule=kind_rule,
    )

    # Which reason wins?
    if kind_rule.never_auto_merge or kind_rule.require_human_review:
        decision_reason = "kind_blocked_auto_merge"
    elif same_issue_candidate_id is not None:
        decision_reason = "same_issue_candidate_emitted"
    else:
        decision_reason = "near_duplicate_review"

    _log.info(
        "reconcile.decision",
        decision="human_review",
        cosine=best_cosine,
        adjusted=best_adjusted,
        matched_model_id=str(matched_id),
        trigger_id=str(trigger_id),
        decision_reason=decision_reason,
        same_issue_candidate_id=(
            str(same_issue_candidate_id)
            if same_issue_candidate_id is not None
            else None
        ),
    )
    return ReconcileResult(
        decision="human_review",
        matched_model_id=matched_id,
        cosine_similarity=best_cosine,
        replacement_op=None,
        event_id=event_id,
        decision_reason=decision_reason,
        signal_breakdown=best_breakdown,
        same_issue_candidate_id=same_issue_candidate_id,
    )


def _build_auto_merge_replacement(
    entry: dict[str, Any],
    best_row: dict[str, Any],
    matched_id: UUID,
) -> ClaimOp:
    """Convert an insert into a confidence-update against the matched
    Model. We choose the *higher* of the two confidences as the new
    value — the reconciler treats the new claim as a confirming
    signal and lets the underlying Model rise toward the more
    confident reading. Going lower is reserved for explicit
    contestation."""
    candidate_conf = float(entry.get("confidence", 0.5))
    existing_conf = float(best_row["confidence"])
    new_conf = max(candidate_conf, existing_conf)
    now = datetime.now(timezone.utc)
    source_event_id = _coerce_uuid(entry.get("born_from_event_id"))
    existing_events = _normalize_uuid_list(
        best_row.get("supporting_event_ids"),
    )
    existing_readings = _normalize_signal_readings(
        best_row.get("signal_readings"),
    )
    changes: dict[str, Any] = {
        "confidence": new_conf,
        "last_confirmed_at": now,
        "confirmed_count": int(best_row.get("confirmed_count") or 0) + 1,
    }
    if source_event_id is not None:
        changes["supporting_event_ids"] = _append_uuid_once(
            existing_events,
            source_event_id,
        )
        changes["signal_readings"] = [
            *existing_readings,
            _confirmation_reading(entry, source_event_id, now),
        ]
    situation_merge = _build_situation_merge_payload(
        entry=entry,
        best_row=best_row,
        source_event_id=source_event_id,
    )
    if situation_merge is not None:
        changes["__situation_merge"] = situation_merge
    return ClaimOp(
        op="update",
        model_id=matched_id,
        changes=changes,
    )


def _build_situation_merge_payload(
    *,
    entry: dict[str, Any],
    best_row: dict[str, Any],
    source_event_id: UUID | None,
) -> dict[str, Any] | None:
    """Return internal applier payload for evolving an existing situation.

    A situation auto-merge should absorb the new composite structure, not just
    bump confidence. The public LLM diff surface still only sees normal
    claim_ops; this private payload is emitted by the reconciler and consumed
    by the applier under the reconciliation_merge audit path.
    """
    candidate_prop = _normalize_jsonish(entry.get("proposition"))
    existing_prop = _normalize_jsonish(best_row.get("proposition"))
    if not isinstance(candidate_prop, dict) or not isinstance(existing_prop, dict):
        return None
    candidate_grammar = derive_memory_grammar(candidate_prop)
    existing_grammar = derive_memory_grammar(existing_prop)
    if (
        candidate_grammar.claim_role != "situation"
        or existing_grammar.claim_role != "situation"
    ):
        return None

    merged = dict(existing_prop)
    old_members = _member_model_ids(existing_prop)
    candidate_members = _member_model_ids(candidate_prop)
    if not candidate_members:
        return None

    member_ids = _merge_uuid_lists(
        existing_prop.get("member_model_ids"),
        candidate_prop.get("member_model_ids"),
    )
    if len(member_ids) < 2:
        return None
    merged["member_model_ids"] = member_ids

    event_ids = _merge_uuid_lists(
        existing_prop.get("evidence_event_ids"),
        candidate_prop.get("evidence_event_ids"),
        [str(source_event_id)] if source_event_id is not None else [],
    )
    if event_ids:
        merged["evidence_event_ids"] = event_ids

    for key in ("affected_decisions", "affected_customers", "affected_teams"):
        merged_values = _merge_string_lists(
            existing_prop.get(key),
            candidate_prop.get(key),
        )
        if merged_values:
            merged[key] = merged_values

    for key in (
        "summary",
        "relationship_summary",
        "shared_mechanism",
        "judgment_change",
        "open_falsifier",
    ):
        candidate_value = candidate_prop.get(key)
        existing_value = merged.get(key)
        if (
            isinstance(candidate_value, str)
            and candidate_value.strip()
            and (
                not isinstance(existing_value, str)
                or len(candidate_value) > len(existing_value)
            )
        ):
            merged[key] = candidate_value

    candidate_status = candidate_prop.get("status")
    if candidate_status in {"forming", "active", "contested", "resolved"}:
        existing_status = merged.get("status")
        if existing_status in {None, "", "forming"} or candidate_status in {
            "active",
            "contested",
            "resolved",
        }:
            merged["status"] = candidate_status

    candidate_tags = set(candidate_grammar.domain_tags)
    candidate_tags.update(str(tag) for tag in (entry.get("domain_tags") or []))
    candidate_tags.update(
        str(tag)
        for tag in candidate_prop.get("domain_tags", [])
        if isinstance(tag, str)
    )
    return {
        "proposition": merged,
        "added_member_model_ids": [
            str(uid) for uid in (candidate_members - old_members)
        ],
        "candidate_domain_tags": sorted(tag for tag in candidate_tags if tag),
        "candidate_natural": str(entry.get("natural") or "")[:1000],
    }


def _merge_uuid_lists(*values: Any) -> list[str]:
    out: list[str] = []
    seen: set[UUID] = set()
    for value in values:
        if not isinstance(value, (list, tuple)):
            continue
        for raw in value:
            uid = _coerce_uuid(raw)
            if uid is None or uid in seen:
                continue
            seen.add(uid)
            out.append(str(uid))
    return out


def _merge_string_lists(*values: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, (list, tuple)):
            continue
        for raw in value:
            text = str(raw).strip()
            if not text:
                continue
            key = text.casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append(text)
    return out


async def _emit_same_issue_candidate(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    matched_model_id: UUID,
    cosine: float,
    adjusted: float,
    breakdown: dict[str, float],
    prop_kind: str | None,
    candidate_entry: dict[str, Any],
    kind_rule: KindRule,
) -> UUID | None:
    """Emit a `same_issue_as` RelationshipCandidate for a near-duplicate.

    The new model's id is not yet known at reconcile time (apply
    happens AFTER reconcile decides), so we use the
    `born_from_event_id` as a stable placeholder for the new model
    via the candidate's `evidence_event_ids` field, and we pass
    `placeholder_model_id` (a uuid7 we mint here) as `source_model_id`.
    The placeholder is wired up in metadata so adjudication can
    resolve it after the apply step writes the real model id.

    Returns the candidate id on success, or None on failure (we
    intentionally swallow failures: this is best-effort enrichment
    and must NEVER break the reconcile path).
    """
    try:
        # P5 owns candidates.py — we only CALL into it.
        from services.reasoning.judgment.scoring import JudgmentScores, clamp_score
        from services.reasoning.relationships.candidates import make_edge_candidate
        from services.reasoning.relationships.repo import RelationshipCandidatesRepo
    except Exception as exc:  # noqa: BLE001
        _log.debug(
            "reconcile.same_issue_emit_import_error",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        return None

    placeholder_source_id = uuid7()  # stand-in for the soon-to-be-inserted model
    leverage = clamp_score(cosine)
    scores = JudgmentScores(
        impact=0.35,
        urgency=0.25,
        actionability=0.30,
        authority_required=0.25,
        uncertainty=clamp_score(1.0 - cosine),
        novelty=0.20,
        reversibility=0.55,
        confidence=clamp_score(cosine),
    )
    metadata: dict[str, Any] = {
        "origin": "reconciler_near_duplicate",
        # The relationship_candidates.basis CHECK constraint does not
        # accept 'paraphrase_suspect', so we encode the operator-
        # visible basis in metadata while keeping the column value
        # within the allowed set.
        "operator_basis": "paraphrase_suspect",
        "cosine": float(cosine),
        "adjusted_score": float(adjusted),
        "signal_breakdown": {k: float(v) for k, v in breakdown.items()},
        "proposition_kind": prop_kind,
        "judgment_leverage_proxy": float(leverage),
        "placeholder_source_model_id": str(placeholder_source_id),
        "kind_rule": {
            "never_auto_merge": kind_rule.never_auto_merge,
            "require_human_review": kind_rule.require_human_review,
            "auto_merge_cosine": kind_rule.auto_merge_cosine,
        },
    }
    # `born_from_event_id` is the strongest hook back to the new model
    # once the apply step writes it.
    born_event = _coerce_uuid(candidate_entry.get("born_from_event_id"))
    evidence_event_ids: tuple[UUID, ...] = (
        (born_event,) if born_event is not None else ()
    )

    try:
        candidate = make_edge_candidate(
            tenant_id=tenant_id,
            source_model_id=placeholder_source_id,
            target_model_id=matched_model_id,
            edge_kind="same_issue_as",
            basis="inferred",  # CHECK-constrained; real basis in metadata.
            explanation=(
                f"reconciler near-duplicate (cosine={cosine:.2f}, "
                f"adjusted={adjusted:.2f}) — paraphrase_suspect"
            ),
            scores=scores,
            evidence_event_ids=evidence_event_ids,
            metadata=metadata,
            source="think.reconciler",
            review_status="needs_review",
        )
        repo = RelationshipCandidatesRepo()
        await repo.insert(conn, candidate)
        return candidate.id
    except Exception as exc:  # noqa: BLE001
        _log.debug(
            "reconcile.same_issue_emit_failed",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        return None


# =====================================================================
# Metrics
# =====================================================================


def _emit_metric(decision: str) -> None:
    """Bump `METRICS.reconcile_decisions_total{decision}`. Local import
    avoids a circular: observability imports from this module's
    sibling `cascade`, which would import the reconciler at module
    load time."""
    try:
        from .observability import METRICS
        METRICS.inc_reconcile_decision(decision)
    except Exception:  # noqa: BLE001
        # Metrics must never crash the reconciler.
        pass


__all__ = [
    "Decision",
    "DecisionReason",
    "KindRule",
    "ReconcileResult",
    "ReconcilerConfig",
    "reconcile_claim_op",
]
