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
from .representation_contract import contextual_frames_compatible
from .reconciler_situation_merge import build_situation_merge_payload
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


@dataclass(frozen=True)
class _ReconcileContext:
    entry: dict[str, Any]
    candidate_embedding: list[float]
    proposition: dict[str, Any]
    prop_kind: str | None
    grammar: Any
    kind_rule: "KindRule"
    auto_merge_threshold: float
    human_review_threshold: float
    candidate_scope_actors: list[str]
    candidate_scope_entities: list[dict[str, Any]]


@dataclass(frozen=True)
class _BestCandidate:
    row: dict[str, Any] | None
    cosine: float
    adjusted: float
    breakdown: dict[str, float]
    member_overlap: float


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
    proposition = entry.get("proposition") if isinstance(entry, dict) else {}
    reading: dict[str, Any] = {
        "kind": "confirm",
        "at": observed_at.isoformat(),
        "source_event_id": str(source_event_id),
        "confidence": float(entry.get("confidence", 0.5)),
        "natural": str(entry.get("natural") or "")[:500],
    }
    if isinstance(proposition, dict):
        if isinstance(proposition.get("contextual_frame"), dict):
            reading["contextual_frame"] = proposition["contextual_frame"]
        if isinstance(proposition.get("retrieval_tags"), list):
            reading["retrieval_tags"] = proposition["retrieval_tags"][:24]
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
               supporting_model_ids, falsifier, proposition, domain_tags
        FROM accepted_current_models
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
    context = _build_reconcile_context(entry, config)
    rows = await _load_reconcile_candidates(conn, tenant_id, context, config)
    best = _select_best_candidate(rows, context)

    if _should_record_no_match(best, context):
        return await _record_no_match_result(
            conn,
            op=op,
            tenant_id=tenant_id,
            trigger_id=trigger_id,
            think_run_id=think_run_id,
            config=config,
            context=context,
            best=best,
        )
    if _can_auto_merge(best, context):
        return await _record_auto_merge_result(
            conn,
            op=op,
            tenant_id=tenant_id,
            trigger_id=trigger_id,
            think_run_id=think_run_id,
            config=config,
            context=context,
            best=best,
        )
    return await _record_human_review_result(
        conn,
        op=op,
        tenant_id=tenant_id,
        trigger_id=trigger_id,
        think_run_id=think_run_id,
        context=context,
        best=best,
    )


def _build_reconcile_context(
    entry: dict[str, Any],
    config: ReconcilerConfig,
) -> _ReconcileContext:
    candidate_embedding = _candidate_embedding(entry)
    proposition = _canonical_proposition(entry.get("proposition") or {})
    prop_kind = proposition.get("kind") if isinstance(proposition, dict) else None
    grammar = derive_memory_grammar(proposition if isinstance(proposition, dict) else {})
    rule_key = _semantic_rule_key(proposition if isinstance(proposition, dict) else None)
    kind_rule = _kind_rule(rule_key)
    raw_actors = entry.get("scope_actors") or []
    return _ReconcileContext(
        entry=entry,
        candidate_embedding=candidate_embedding,
        proposition=proposition if isinstance(proposition, dict) else {},
        prop_kind=prop_kind,
        grammar=grammar,
        kind_rule=kind_rule,
        auto_merge_threshold=_auto_merge_threshold(kind_rule, config),
        human_review_threshold=_human_review_threshold(kind_rule, config),
        candidate_scope_actors=[str(a) for a in raw_actors],
        candidate_scope_entities=[
            e for e in (entry.get("scope_entities") or []) if isinstance(e, dict)
        ],
    )


def _canonical_proposition(proposition: Any) -> dict[str, Any]:
    if not isinstance(proposition, dict):
        return {}
    try:
        canonical = canonicalize_proposition(proposition)
    except Exception:
        return proposition
    return canonical if isinstance(canonical, dict) else proposition


def _auto_merge_threshold(
    kind_rule: "KindRule",
    config: ReconcilerConfig,
) -> float:
    if kind_rule.auto_merge_cosine is not None:
        return kind_rule.auto_merge_cosine
    return config.auto_merge_cosine


def _human_review_threshold(
    kind_rule: "KindRule",
    config: ReconcilerConfig,
) -> float:
    if kind_rule.human_review_cosine is not None:
        return kind_rule.human_review_cosine
    return config.human_review_cosine


async def _load_reconcile_candidates(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    context: _ReconcileContext,
    config: ReconcilerConfig,
) -> list[dict[str, Any]]:
    return await _find_candidates(
        conn,
        tenant_id=tenant_id,
        candidate_embedding=context.candidate_embedding,
        candidate_scope_actors=context.candidate_scope_actors,
        candidate_scope_entities=context.candidate_scope_entities,
        proposition_kind=context.prop_kind,
        claim_role=context.grammar.claim_role,
        recency_window_days=config.recency_window_days,
    )


def _select_best_candidate(
    rows: list[dict[str, Any]],
    context: _ReconcileContext,
) -> _BestCandidate:
    best = _BestCandidate(
        row=None,
        cosine=-1.0,
        adjusted=-1.0,
        breakdown={},
        member_overlap=0.0,
    )
    for row in rows:
        scored = _score_candidate_row(row, context)
        if scored is not None and scored.adjusted > best.adjusted:
            best = scored
    return best


def _score_candidate_row(
    row: dict[str, Any],
    context: _ReconcileContext,
) -> _BestCandidate | None:
    existing_embedding = _embedding_list(row.get("embedding"))
    if existing_embedding is None:
        return None
    if not _pattern_instance_matches(row, context):
        return None
    compatible, compatibility = contextual_frames_compatible(context.entry, row)
    if not compatible:
        return None

    cosine = _cosine(context.candidate_embedding, existing_embedding)
    adjusted, breakdown = _compute_signal_breakdown(context.entry, row, cosine)
    if compatibility.get("compared"):
        breakdown = dict(breakdown)
        breakdown["contextual_frame_compared"] = 1.0
        if any(
            value
            for key, value in compatibility.items()
            if key.endswith("_overlap") and value
        ):
            breakdown["contextual_frame_overlap"] = 1.0
    adjusted, breakdown, member_overlap = _apply_situation_member_overlap(
        row,
        context,
        adjusted,
        breakdown,
    )
    return _BestCandidate(
        row=row,
        cosine=cosine,
        adjusted=adjusted,
        breakdown=breakdown,
        member_overlap=member_overlap,
    )


def _embedding_list(raw: Any) -> list[float] | None:
    if raw is None:
        return None
    if hasattr(raw, "tolist"):
        raw = raw.tolist()
    if not isinstance(raw, list):
        return None
    return list(raw)


def _pattern_instance_matches(
    row: dict[str, Any],
    context: _ReconcileContext,
) -> bool:
    if not context.kind_rule.require_matching_pattern_id:
        return True
    candidate_pattern_id = _pattern_id(context.entry.get("proposition"))
    row_pattern_id = _pattern_id(row.get("proposition"))
    return (
        candidate_pattern_id is not None
        and row_pattern_id is not None
        and candidate_pattern_id == row_pattern_id
    )


def _apply_situation_member_overlap(
    row: dict[str, Any],
    context: _ReconcileContext,
    adjusted: float,
    breakdown: dict[str, float],
) -> tuple[float, dict[str, float], float]:
    candidate_member_ids = _member_model_ids(context.entry.get("proposition"))
    if context.grammar.claim_role != "situation" or not candidate_member_ids:
        return adjusted, breakdown, 0.0

    member_overlap = _member_overlap_fraction(
        candidate_member_ids,
        _member_model_ids(row.get("proposition")),
    )
    if member_overlap <= 0:
        return adjusted, breakdown, member_overlap
    breakdown = dict(breakdown)
    breakdown["situation_member_overlap"] = float(member_overlap)
    adjusted = max(adjusted, min(1.0, member_overlap))
    breakdown["adjusted_score"] = float(adjusted)
    return adjusted, breakdown, member_overlap


def _should_record_no_match(
    best: _BestCandidate,
    context: _ReconcileContext,
) -> bool:
    if best.row is None:
        return True
    if best.adjusted >= context.human_review_threshold:
        return False
    situation_floor = context.kind_rule.same_issue_member_overlap_floor
    return situation_floor is None or best.member_overlap < situation_floor


async def _record_no_match_result(
    conn: asyncpg.Connection,
    *,
    op: ClaimOp,
    tenant_id: UUID,
    trigger_id: UUID,
    think_run_id: UUID | None,
    config: ReconcilerConfig,
    context: _ReconcileContext,
    best: _BestCandidate,
) -> ReconcileResult:
    event_id: UUID | None = None
    if config.log_no_match:
        event_id = await _record_event(
            conn,
            tenant_id=tenant_id,
            decision="no_match",
            original_claim_op=op,
            matched_model_id=None,
            cosine_similarity=_result_cosine(best),
            proposition_kind=context.prop_kind,
            trigger_id=trigger_id,
            think_run_id=think_run_id,
        )
    _emit_metric("no_match")
    _log.info(
        "reconcile.decision",
        decision="no_match",
        cosine=_result_cosine(best),
        trigger_id=str(trigger_id),
    )
    return ReconcileResult(
        decision="no_match",
        matched_model_id=None,
        cosine_similarity=_result_cosine(best),
        replacement_op=None,
        event_id=event_id,
        decision_reason="no_match",
        signal_breakdown=best.breakdown,
    )


def _result_cosine(best: _BestCandidate) -> float | None:
    return best.cosine if best.cosine >= 0.0 else None


def _can_auto_merge(
    best: _BestCandidate,
    context: _ReconcileContext,
) -> bool:
    if best.row is None:
        return False
    if context.kind_rule.never_auto_merge or context.kind_rule.require_human_review:
        return False
    return best.adjusted >= context.auto_merge_threshold or _member_overlap_auto(
        best,
        context,
    )


def _member_overlap_auto(
    best: _BestCandidate,
    context: _ReconcileContext,
) -> bool:
    return (
        context.grammar.claim_role == "situation"
        and context.kind_rule.auto_member_overlap is not None
        and best.member_overlap >= context.kind_rule.auto_member_overlap
    )


async def _record_auto_merge_result(
    conn: asyncpg.Connection,
    *,
    op: ClaimOp,
    tenant_id: UUID,
    trigger_id: UUID,
    think_run_id: UUID | None,
    config: ReconcilerConfig,
    context: _ReconcileContext,
    best: _BestCandidate,
) -> ReconcileResult:
    assert best.row is not None
    matched_id: UUID = best.row["id"]
    replacement = _build_auto_merge_replacement(
        context.entry,
        best.row,
        matched_id,
    )
    event_id = await _record_event(
        conn,
        tenant_id=tenant_id,
        decision="auto_merge",
        original_claim_op=op,
        matched_model_id=matched_id,
        cosine_similarity=best.cosine,
        proposition_kind=context.prop_kind,
        trigger_id=trigger_id,
        think_run_id=think_run_id,
    )
    _emit_metric("auto_merge")
    _log.info(
        "reconcile.decision",
        decision="auto_merge",
        cosine=best.cosine,
        adjusted=best.adjusted,
        matched_model_id=str(matched_id),
        trigger_id=str(trigger_id),
    )
    return ReconcileResult(
        decision="auto_merge",
        matched_model_id=matched_id,
        cosine_similarity=best.cosine,
        replacement_op=replacement,
        event_id=event_id,
        decision_reason=_auto_merge_decision_reason(best, context, config),
        signal_breakdown=best.breakdown,
    )


def _auto_merge_decision_reason(
    best: _BestCandidate,
    context: _ReconcileContext,
    config: ReconcilerConfig,
) -> DecisionReason:
    if _member_overlap_auto(best, context):
        return "kind_specific_auto_merge"
    if (
        best.breakdown.get("graph_boost", 0.0) > 0.0
        and best.cosine < context.auto_merge_threshold
    ):
        return "graph_signal_boost"
    if (
        context.kind_rule.auto_merge_cosine is not None
        and best.cosine < config.auto_merge_cosine
    ):
        return "kind_specific_auto_merge"
    if best.cosine >= 0.999:
        return "exact_match"
    return "high_cosine_auto_merge"


async def _record_human_review_result(
    conn: asyncpg.Connection,
    *,
    op: ClaimOp,
    tenant_id: UUID,
    trigger_id: UUID,
    think_run_id: UUID | None,
    context: _ReconcileContext,
    best: _BestCandidate,
) -> ReconcileResult:
    assert best.row is not None
    matched_id: UUID = best.row["id"]
    event_id = await _record_event(
        conn,
        tenant_id=tenant_id,
        decision="human_review",
        original_claim_op=op,
        matched_model_id=matched_id,
        cosine_similarity=best.cosine,
        proposition_kind=context.prop_kind,
        trigger_id=trigger_id,
        think_run_id=think_run_id,
    )
    _emit_metric("human_review")

    same_issue_candidate_id = await _emit_same_issue_candidate(
        conn,
        tenant_id=tenant_id,
        matched_model_id=matched_id,
        cosine=best.cosine,
        adjusted=best.adjusted,
        breakdown=best.breakdown,
        prop_kind=context.prop_kind,
        candidate_entry=context.entry,
        kind_rule=context.kind_rule,
    )

    decision_reason = _human_review_decision_reason(
        context.kind_rule,
        same_issue_candidate_id,
    )
    _log.info(
        "reconcile.decision",
        decision="human_review",
        cosine=best.cosine,
        adjusted=best.adjusted,
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
        cosine_similarity=best.cosine,
        replacement_op=None,
        event_id=event_id,
        decision_reason=decision_reason,
        signal_breakdown=best.breakdown,
        same_issue_candidate_id=same_issue_candidate_id,
    )


def _human_review_decision_reason(
    kind_rule: "KindRule",
    same_issue_candidate_id: UUID | None,
) -> DecisionReason:
    if kind_rule.never_auto_merge or kind_rule.require_human_review:
        return "kind_blocked_auto_merge"
    if same_issue_candidate_id is not None:
        return "same_issue_candidate_emitted"
    return "near_duplicate_review"


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
    merged_domain_tags = _merged_auto_merge_domain_tags(entry, best_row)
    if merged_domain_tags is not None:
        changes["domain_tags"] = merged_domain_tags
    merged_proposition = _merged_auto_merge_proposition(entry, best_row)
    if merged_proposition is not None:
        changes["proposition"] = merged_proposition
    situation_merge = build_situation_merge_payload(
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


def _merged_auto_merge_domain_tags(
    entry: dict[str, Any],
    best_row: dict[str, Any],
) -> list[str] | None:
    existing = _string_sequence(best_row.get("domain_tags"))
    merged = _merge_string_sequence(
        existing,
        _entry_representation_tags(entry, include_coverage=True),
    )
    return merged if merged != existing else None


def _merged_auto_merge_proposition(
    entry: dict[str, Any],
    best_row: dict[str, Any],
) -> dict[str, Any] | None:
    existing = _normalize_jsonish(best_row.get("proposition"))
    if not isinstance(existing, dict):
        return None
    candidate = entry.get("proposition")
    if not isinstance(candidate, dict):
        return None

    merged = dict(existing)
    changed = False
    for key in ("retrieval_tags", "coverage_roles", "domain_tags"):
        values = _merge_string_sequence(
            existing.get(key),
            candidate.get(key),
            entry.get("domain_tags") if key == "domain_tags" else None,
        )
        if values and values != _string_sequence(existing.get(key)):
            merged[key] = values
            changed = True
    return merged if changed else None


def _entry_representation_tags(
    entry: dict[str, Any],
    *,
    include_coverage: bool = False,
) -> list[str]:
    prop = entry.get("proposition")
    tags: list[Any] = [entry.get("domain_tags")]
    if isinstance(prop, dict):
        tags.extend([prop.get("domain_tags"), prop.get("retrieval_tags")])
        if include_coverage:
            tags.append(prop.get("coverage_roles"))
    return _merge_string_sequence(*tags)


def _string_sequence(value: Any) -> list[str]:
    if value is None:
        return []
    values = value if isinstance(value, (list, tuple, set)) else (value,)
    out: list[str] = []
    for raw in values:
        if raw is None:
            continue
        tag = str(raw).strip()
        if tag:
            out.append(tag)
    return out


def _merge_string_sequence(*groups: Any, limit: int = 64) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for tag in _string_sequence(group):
            key = tag.casefold()
            if key in seen:
                continue
            seen.add(key)
            merged.append(tag)
            if len(merged) >= limit:
                return merged
    return merged


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
