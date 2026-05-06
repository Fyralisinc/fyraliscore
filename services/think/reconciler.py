"""services/think/reconciler.py — content-level Model dedup at insert time.

T5: explicit reconciliation as a first-class pipeline step. See
services/think/RECONCILIATION_DESIGN.md for the design and
RECONCILIATION_README.md for the operator-facing surface.

The pipeline runs this between validate and apply:

  trigger → retrieve → reason → validate
          → reconcile_claim_op  ◀── this module
          → apply → cascade

For each `claim_op.insert` proposed by the LLM, the reconciler:

  1. Looks for existing active Models in the same tenant that match
     on FOUR signals: embedding cosine ≥ HUMAN_REVIEW_COSINE,
     overlapping scope, identical proposition_kind, and created
     within the recency window.
  2. Decides:
       * cosine ≥ AUTO_MERGE_COSINE     → 'auto_merge': convert
         the insert to a confidence update against the matched Model.
       * cosine in [HUMAN_REVIEW, AUTO) → 'human_review': record
         the candidate in `pending_reconciliation` queue, proceed
         with the original insert anyway. The queue is reviewed
         out-of-band; auto-merge does NOT happen for borderline
         cases.
       * no match in window             → 'no_match': pass through
         unchanged.
  3. Records the decision in `reconciliation_events`.

Reconciliation is opt-out via env `RECONCILE_ENABLED=false`. The
reconciler MUST never abort apply: any internal exception is
logged as `reconcile.error` and the original `claim_op` proceeds.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7

from .diff_schema import ClaimOp


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


@dataclass
class ReconcileResult:
    """Outcome of a single reconcile decision.

    `replacement_op` is set when `decision == "auto_merge"`: the
    caller should apply this op instead of the original insert.
    For all other decisions it is None and the caller proceeds
    with the original op.
    """
    decision: Decision
    matched_model_id: UUID | None
    cosine_similarity: float | None
    replacement_op: ClaimOp | None
    event_id: UUID | None  # row id in reconciliation_events, if written


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
               confidence, proposition_kind, "natural", created_at
        FROM models
        WHERE {' AND '.join(where)}
        ORDER BY embedding <=> $LIMITSEED::vector
        LIMIT {int(k)}
    """
    # ORDER BY needs the candidate vector. Push it on as the last
    # numbered param. We use a placeholder marker because the
    # embedding bind format depends on the connection's codec
    # state — see services/models/PGVECTOR_REGISTRY.md.
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
        )
    if op.op != "insert" or op.entry is None:
        return ReconcileResult(
            decision="skipped",
            matched_model_id=None,
            cosine_similarity=None,
            replacement_op=None,
            event_id=None,
        )

    try:
        return await _reconcile_inner(
            op, conn,
            tenant_id=tenant_id,
            trigger_id=trigger_id,
            think_run_id=think_run_id,
            config=cfg,
        )
    except Exception as exc:  # noqa: BLE001
        # Reconciler must never abort apply. Log loudly and pass through.
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
    candidate_embedding = entry.get("embedding")
    if not isinstance(candidate_embedding, list) or not candidate_embedding:
        # Inserts without an embedding (e.g. think.applier's zero-vector
        # placeholder) cannot be reconciled by cosine. Skip cleanly.
        return ReconcileResult(
            decision="skipped",
            matched_model_id=None,
            cosine_similarity=None,
            replacement_op=None,
            event_id=None,
        )
    # Skip placeholder zero-vectors — they would match every other
    # zero-vector in the system, which would be a catastrophic false
    # positive. The applier writes zero-vectors when the LLM omitted
    # an embedding; those Models are unreconcilable until backfilled.
    if not any(abs(float(x)) > 1e-9 for x in candidate_embedding):
        return ReconcileResult(
            decision="skipped",
            matched_model_id=None,
            cosine_similarity=None,
            replacement_op=None,
            event_id=None,
        )

    proposition = entry.get("proposition") or {}
    prop_kind = (
        proposition.get("kind") if isinstance(proposition, dict) else None
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
        recency_window_days=config.recency_window_days,
    )

    # Score each row and pick the best.
    best_row: dict[str, Any] | None = None
    best_cosine: float = -1.0
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
        cos = _cosine(candidate_embedding, list(existing_emb))
        if cos > best_cosine:
            best_cosine = cos
            best_row = r

    if best_row is None or best_cosine < config.human_review_cosine:
        # No qualifying match. Optionally write a no_match audit row
        # for tuning data.
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
            cosine_similarity=best_cosine if best_cosine >= 0.0 else None,
            replacement_op=None,
            event_id=event_id,
        )

    matched_id: UUID = best_row["id"]

    if best_cosine >= config.auto_merge_cosine:
        # Auto-merge: convert insert into a confidence update against
        # the matched Model. We choose the *higher* of the two
        # confidences as the new value — the reconciler treats the
        # new claim as a confirming signal and lets the underlying
        # Model rise toward the more confident reading. Going lower
        # is reserved for explicit contestation.
        candidate_conf = float(entry.get("confidence", 0.5))
        existing_conf = float(best_row["confidence"])
        new_conf = max(candidate_conf, existing_conf)
        replacement = ClaimOp(
            op="update",
            model_id=matched_id,
            changes={"confidence": new_conf},
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
            matched_model_id=str(matched_id),
            trigger_id=str(trigger_id),
        )
        return ReconcileResult(
            decision="auto_merge",
            matched_model_id=matched_id,
            cosine_similarity=best_cosine,
            replacement_op=replacement,
            event_id=event_id,
        )

    # Borderline: log to the human-review queue, proceed with the
    # original insert. We do NOT auto-merge here.
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
    _log.info(
        "reconcile.decision",
        decision="human_review",
        cosine=best_cosine,
        matched_model_id=str(matched_id),
        trigger_id=str(trigger_id),
    )
    return ReconcileResult(
        decision="human_review",
        matched_model_id=matched_id,
        cosine_similarity=best_cosine,
        replacement_op=None,
        event_id=event_id,
    )


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
    "ReconcileResult",
    "ReconcilerConfig",
    "reconcile_claim_op",
]
