"""services/calibration/hit_rate.py — per-class 30d hit-rate.

Track BC. The CEO view's card surface wants a calibration anchor next
to each card so the reader can ask "how often has this *class* of
claim turned out true in the last 30 days?" — distinct from the model
confidence on the specific claim.

Resolution signals in the substrate
-----------------------------------
Two reliable signals already exist:

  * `calibration_stats(outcome BOOLEAN)` — populated by the weekly
    Calibration updater from `models.resolved_at` /
    `models.resolution_outcome` rows. A "hit" is `outcome = TRUE`. The
    `proposition_kind` column gives us the natural axis for the
    `belief_movement` claim class.
  * `commitments` — a commitment is "resolved" when it enters the
    `doneverified` state. A "hit" for `delivery_estimate` is a
    commitment that finished on time (`terminal_at <= due_date`).

Two other classes (`renewal_risk`, `expansion_likelihood`) target
customer-resource trajectories. We do not have a reliable, queryable
resolution signal for them in the substrate today (no per-renewal
outcome rows), so this module returns None for those — honest absence
rather than fabricated calibration.

Threshold: < 5 resolved samples → None. Calibration on a base of 1-2
points is noise, not signal.
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

import asyncpg


log = logging.getLogger(__name__)


MIN_SAMPLES_FOR_CALIBRATION = 5

SUPPORTED_CLAIM_CLASSES = (
    "delivery_estimate",
    "belief_movement",
    # The two classes below are *recognised* by `classify_card` but
    # `hit_rate_for_class` returns None for them because the substrate
    # doesn't yet expose a reliable resolution signal. Listed here so
    # callers can introspect the surface.
    "renewal_risk",
    "expansion_likelihood",
)


# ---------------------------------------------------------------------
# classify_card — focus dict → claim class
# ---------------------------------------------------------------------


def classify_card(card_focus: dict[str, Any] | None) -> str | None:
    """Derive a claim class from a card-focus dict.

    Recognised classes (first match wins):

      * `renewal_risk`         — customer Resource with revenue_at_risk
                                 in focus.
      * `expansion_likelihood` — customer Resource with positive
                                 trajectory (`health == "healthy"` and
                                 utilization_state in {"committed",
                                 "available"}).
      * `delivery_estimate`    — Commitment with a due date in focus.
      * `belief_movement`      — Model with `prior_confidence !=
                                 confidence` in focus.

    Returns None when no rule fires; the card then carries
    `calibration: None`, which is the honest answer for unclassifiable
    cards.
    """
    if not isinstance(card_focus, dict):
        return None

    # ----- renewal_risk
    resource = card_focus.get("resource")
    if isinstance(resource, dict):
        rev_str = resource.get("revenue_at_risk")
        rev_num = resource.get("revenue_at_risk_usd")
        # Treat *any* non-None revenue_at_risk as renewal_risk — the
        # parser at stake-derivation time filters non-parsing strings,
        # but the classifier is intentionally permissive here.
        if rev_str is not None or rev_num is not None:
            return "renewal_risk"

    # ----- expansion_likelihood
    if isinstance(resource, dict):
        # No revenue at risk, but a customer-shaped resource with
        # healthy/positive trajectory.
        health = (resource.get("health") or "").lower() if isinstance(
            resource.get("health"), str
        ) else None
        kind = resource.get("kind")
        utilization = (
            resource.get("utilization_state")
            if isinstance(resource.get("utilization_state"), str)
            else None
        )
        if (
            kind in ("customer", "relational")
            and health == "healthy"
            and (utilization in (None, "available", "committed"))
        ):
            return "expansion_likelihood"

    # ----- delivery_estimate
    commitment = card_focus.get("commitment")
    if isinstance(commitment, dict):
        if commitment.get("due_at") is not None:
            return "delivery_estimate"
        if commitment.get("days_to_due") is not None:
            return "delivery_estimate"

    # ----- belief_movement
    model = card_focus.get("model")
    if isinstance(model, dict):
        prior = model.get("prior_confidence")
        current = model.get("confidence")
        if (
            isinstance(prior, (int, float))
            and isinstance(current, (int, float))
            and float(prior) != float(current)
        ):
            return "belief_movement"

    return None


# ---------------------------------------------------------------------
# hit_rate_for_class — SQL query
# ---------------------------------------------------------------------


async def hit_rate_for_class(
    tenant_id: UUID,
    claim_class: str,
    *,
    db: asyncpg.Pool | asyncpg.Connection,
    window_days: int = 30,
) -> dict[str, Any] | None:
    """Return a 30-day hit-rate for `claim_class` on `tenant_id`.

    Shape: `{"class": claim_class, "hit_rate_30d": float in [0,1],
    "n_samples": int}` or None.

    None is returned when:
      * `claim_class` is unrecognised
      * the substrate doesn't expose a reliable resolution signal for
        this class
      * fewer than `MIN_SAMPLES_FOR_CALIBRATION` resolved samples were
        found in the window

    Never fabricates. The caller must treat None as "we don't know".
    """
    if claim_class not in SUPPORTED_CLAIM_CLASSES:
        return None
    if window_days <= 0:
        return None

    try:
        if claim_class == "delivery_estimate":
            result = await _hit_rate_delivery_estimate(
                tenant_id, db=db, window_days=window_days
            )
        elif claim_class == "belief_movement":
            result = await _hit_rate_belief_movement(
                tenant_id, db=db, window_days=window_days
            )
        else:
            # renewal_risk / expansion_likelihood — no resolution signal yet.
            return None
    except Exception as exc:
        log.warning(
            "calibration.hit_rate_query_failed",
            extra={
                "tenant_id": str(tenant_id),
                "claim_class": claim_class,
                "error": str(exc),
            },
        )
        return None

    if result is None:
        return None
    hits, n = result
    if n < MIN_SAMPLES_FOR_CALIBRATION:
        return None
    rate = hits / n if n > 0 else 0.0
    # Defensive clamp; the query already produces a value in [0, n].
    if rate < 0.0:
        rate = 0.0
    elif rate > 1.0:
        rate = 1.0
    return {
        "class": claim_class,
        "hit_rate_30d": float(rate),
        "n_samples": int(n),
    }


# ---------------------------------------------------------------------
# Per-class queries
# ---------------------------------------------------------------------


async def _hit_rate_delivery_estimate(
    tenant_id: UUID,
    *,
    db: asyncpg.Pool | asyncpg.Connection,
    window_days: int,
) -> tuple[int, int] | None:
    """Hit-rate for `delivery_estimate`.

    Universe: commitments that reached `doneverified` in the last
    `window_days`, with both a `due_date` and a `terminal_at` set.
    Hit: `terminal_at <= due_date` (delivered on time).
    """
    sql = """
        SELECT
          COUNT(*) FILTER (WHERE terminal_at <= due_date) AS hits,
          COUNT(*) AS total
        FROM commitments
        WHERE tenant_id = $1
          AND state = 'doneverified'
          AND terminal_at IS NOT NULL
          AND due_date IS NOT NULL
          AND terminal_at >= now() - make_interval(days => $2)
    """
    row = await _fetchrow(db, sql, tenant_id, window_days)
    if row is None:
        return None
    return int(row["hits"] or 0), int(row["total"] or 0)


async def _hit_rate_belief_movement(
    tenant_id: UUID,
    *,
    db: asyncpg.Pool | asyncpg.Connection,
    window_days: int,
) -> tuple[int, int] | None:
    """Hit-rate for `belief_movement`.

    Universe: Models whose `outcome` is non-null in `calibration_stats`
    over the window. Hit: `outcome = TRUE` (the prediction resolved
    true).

    This taps the table the Calibration updater already maintains
    weekly. It includes all proposition_kinds; over time we may want
    to scope to predictions only, but at Tier 1 the all-kinds rate is
    the right anchor.
    """
    sql = """
        SELECT
          COUNT(*) FILTER (WHERE outcome IS TRUE) AS hits,
          COUNT(*) FILTER (WHERE outcome IS NOT NULL) AS total
        FROM calibration_stats
        WHERE tenant_id = $1
          AND resolved_at >= now() - make_interval(days => $2)
    """
    row = await _fetchrow(db, sql, tenant_id, window_days)
    if row is None:
        return None
    return int(row["hits"] or 0), int(row["total"] or 0)


# ---------------------------------------------------------------------
# Plumbing — accept either a pool or a connection
# ---------------------------------------------------------------------


async def _fetchrow(
    db: asyncpg.Pool | asyncpg.Connection,
    sql: str,
    *args: Any,
):
    """Tolerant fetchrow that works against a pool or a held connection."""
    if hasattr(db, "acquire"):
        async with db.acquire() as conn:
            return await conn.fetchrow(sql, *args)
    return await db.fetchrow(sql, *args)


__all__ = [
    "classify_card",
    "hit_rate_for_class",
    "SUPPORTED_CLAIM_CLASSES",
    "MIN_SAMPLES_FOR_CALIBRATION",
]
