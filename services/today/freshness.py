"""services/greeting/freshness.py — Track BC card-payload enrichment.

Compute `truth_freshness_seconds` per card so the CEO view's later Map
surface can render "still true as of …" rather than "filed Xd ago".

Definition: seconds since the most recent *supporting event* attached
to the card's focus. The supporting event is the maximum of:

  - any `state_changed_at` on a focus Model,
  - the `at` of any `state_change` in the focus,
  - the `t` field on any evidence ref in the focus.

If no supporting event is findable, return None — the absence of an
anchor is meaningful and should not be papered over with `0`.

A small helper, `build_card_focus_dict_from_snapshot`, is also provided
here as the canonical bridge between a `SubstrateSnapshot` (what
GRT-snapshot composes) and the dict shape these helpers consume. It is
used by the scheduler so all per-card enrichments work off the same
view.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any


log = logging.getLogger(__name__)


def _coerce_datetime(value: Any) -> datetime | None:
    """Tolerantly coerce a value to a UTC-aware datetime.

    Accepts: datetime (naive treated as UTC), ISO-8601 strings (with or
    without trailing Z), None. Returns None on any other input or on
    parse failure.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # datetime.fromisoformat accepts "+HH:MM" but not the trailing
        # "Z" suffix until Python 3.11 — and even there, "Z" support is
        # limited. Normalise.
        if s.endswith("Z") or s.endswith("z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


def _collect_event_timestamps(card_focus: dict[str, Any]) -> list[datetime]:
    """Walk the card focus dict pulling every event timestamp we know
    how to read. Caller picks the maximum."""
    out: list[datetime] = []

    # 1) focus Model — single dict OR list of dicts under `models`.
    model_dicts: list[dict[str, Any]] = []
    if isinstance(card_focus.get("model"), dict):
        model_dicts.append(card_focus["model"])
    models_list = card_focus.get("models")
    if isinstance(models_list, list):
        for m in models_list:
            if isinstance(m, dict):
                model_dicts.append(m)
    for m in model_dicts:
        dt = _coerce_datetime(m.get("state_changed_at"))
        if dt is not None:
            out.append(dt)
        # Also accept `last_state_change_at` for compatibility with the
        # snapshot-side `ModelRef` field name.
        dt = _coerce_datetime(m.get("last_state_change_at"))
        if dt is not None:
            out.append(dt)

    # 2) state_changes in the focus — list of dicts; each may use `at`
    #    (rendering shape) or `occurred_at` (snapshot shape).
    state_changes = card_focus.get("state_changes")
    if isinstance(state_changes, list):
        for sc in state_changes:
            if not isinstance(sc, dict):
                continue
            dt = _coerce_datetime(sc.get("at"))
            if dt is not None:
                out.append(dt)
            dt = _coerce_datetime(sc.get("occurred_at"))
            if dt is not None:
                out.append(dt)

    # 3) evidence refs — list of dicts; each uses `t`.
    evidence = card_focus.get("evidence")
    if isinstance(evidence, list):
        for ev in evidence:
            if not isinstance(ev, dict):
                continue
            dt = _coerce_datetime(ev.get("t"))
            if dt is not None:
                out.append(dt)

    return out


def truth_freshness_seconds(
    card_focus: dict[str, Any] | None,
    now: datetime,
) -> int | None:
    """Return seconds since the most-recent supporting event in `card_focus`.

    Returns None when no supporting event can be located. Clamps
    negatives to 0 (a future-dated event would otherwise yield a
    nonsensical negative freshness).
    """
    if not isinstance(card_focus, dict):
        return None
    now_utc = (
        now.astimezone(timezone.utc) if now.tzinfo else now.replace(tzinfo=timezone.utc)
    )
    timestamps = _collect_event_timestamps(card_focus)
    if not timestamps:
        return None
    most_recent = max(timestamps)
    delta = (now_utc - most_recent).total_seconds()
    if delta < 0:
        return 0
    return int(delta)


# ---------------------------------------------------------------------
# SubstrateSnapshot -> card-focus dict adapter
# ---------------------------------------------------------------------


def build_card_focus_dict_from_snapshot(snapshot: Any) -> dict[str, Any]:
    """Convert a `SubstrateSnapshot` (GRT-side dataclass) into the
    permissive `card_focus` dict that `derive_stake`,
    `truth_freshness_seconds`, and `classify_card` all consume.

    The snapshot's pinned position-0 entries are surfaced under
    `model` / `commitment` / `resource`. The `candidate` field
    (`conversation_context.recent_queries[0]['card_candidate']`)
    carries the explicit "this is the card subject" hint produced by
    `SnapshotComposer._card_candidates`. We always emit the candidate
    so downstream code can know which pinned entity to trust.

    This is intentionally permissive — fields are present when
    available, absent when not. Helpers downstream test by `get`.
    """
    focus: dict[str, Any] = {}

    # ---- candidate hint (the composer pins it here)
    candidate: dict[str, Any] = {}
    conv = getattr(snapshot, "conversation_context", None)
    if conv is not None:
        rq = getattr(conv, "recent_queries", None) or []
        if rq and isinstance(rq[0], dict):
            candidate = rq[0].get("card_candidate") or {}
    if candidate:
        focus["candidate"] = candidate

    # ---- pinned position-0 Model
    top_models = list(getattr(snapshot, "top_models", []) or [])
    if top_models:
        m = top_models[0]
        focus["model"] = {
            "id": str(getattr(m, "id", "")),
            "natural": getattr(m, "natural", None),
            "confidence": float(getattr(m, "confidence", 0.0)),
            "prior_confidence": (
                float(getattr(m, "confidence_at_assertion", 0.0))
                if getattr(m, "confidence_at_assertion", None) is not None
                else None
            ),
            "state_changed_at": getattr(m, "last_state_change_at", None),
            "proposition_kind": getattr(m, "proposition_kind", None),
        }
        focus["models"] = [focus["model"]]

    # ---- pinned position-0 Commitment
    commits = list(getattr(snapshot, "active_commitments", []) or [])
    if commits:
        c = commits[0]
        is_cp = bool(getattr(c, "is_critical_path", False))
        # The rendering adapter computes pressure as "high" iff
        # is_critical_path — preserve that contract.
        pressure = "high" if is_cp else None
        focus["commitment"] = {
            "id": str(getattr(c, "id", "")),
            "title": getattr(c, "title", None),
            "state": getattr(c, "state", None),
            "due_at": getattr(c, "due_date", None),
            "days_to_due": getattr(c, "days_to_due", None),
            "is_critical_path": is_cp,
            "pressure": pressure,
            "last_state_change_at": getattr(c, "last_state_change_at", None),
        }

    # ---- pinned position-0 customer Resource
    resources = list(getattr(snapshot, "customer_resources", []) or [])
    if resources:
        r = resources[0]
        rev_usd = getattr(r, "revenue_at_risk_usd", None)
        # Pre-format the way the rendering contract demands ("$487K"
        # style) so downstream code that expects rendering's
        # `revenue_at_risk` string sees the same shape we'd render.
        rev_str: str | None
        if rev_usd is None:
            rev_str = None
        else:
            try:
                rev_str = f"${float(rev_usd):,.0f}"
            except (ValueError, TypeError):
                rev_str = None
        focus["resource"] = {
            "id": str(getattr(r, "id", "")),
            "kind": getattr(r, "kind", None),
            "name": getattr(r, "identity", None),
            "health": getattr(r, "health", None)
                or getattr(r, "utilization_state", None),
            "revenue_at_risk": rev_str,
            "revenue_at_risk_usd": (
                float(rev_usd) if rev_usd is not None else None
            ),
            "last_updated_at": getattr(r, "last_updated_at", None),
        }

    # ---- recent state changes (snapshot-shape; helper coerces both)
    state_changes = list(getattr(snapshot, "recent_state_changes", []) or [])
    if state_changes:
        focus["state_changes"] = [
            {
                "at": getattr(sc, "occurred_at", None),
                "occurred_at": getattr(sc, "occurred_at", None),
                "kind": getattr(sc, "kind", None),
                "entity_kind": getattr(sc, "entity_kind", None),
            }
            for sc in state_changes
        ]

    return focus


__all__ = [
    "truth_freshness_seconds",
    "build_card_focus_dict_from_snapshot",
]
