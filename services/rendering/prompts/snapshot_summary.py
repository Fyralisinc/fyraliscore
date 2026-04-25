"""services/rendering/prompts/snapshot_summary.py

Turn a SubstrateSnapshot into a terse, structured summary string the
prompt can include in its user message. This is NOT JSON dump — it is
a grounded operator-voice summary that signals to the model which
facts are load-bearing.

The function is defensive: it never raises on missing fields; it
simply omits sections that are empty.
"""
from __future__ import annotations

from ..contracts import (
    CommitmentRef,
    ConversationContext,
    FounderContext,
    ModelRef,
    ResourceRef,
    StateChange,
    SubstrateSnapshot,
)


def _fmt_dt(dt) -> str:
    if dt is None:
        return "unknown"
    try:
        return dt.strftime("%a %d %b %H:%M")
    except Exception:
        return str(dt)


def _fmt_model(m: ModelRef) -> str:
    parts = [f"{m.id} claim={m.claim!r}"]
    if m.prior_confidence is not None:
        parts.append(f"conf={m.prior_confidence:.2f} \u2192 {m.confidence:.2f}")
    else:
        parts.append(f"conf={m.confidence:.2f}")
    if m.state_changed_at is not None:
        parts.append(f"changed_at={_fmt_dt(m.state_changed_at)}")
    if m.falsifier:
        parts.append(f"falsifier={m.falsifier!r}")
    return " ".join(parts)


def _fmt_commitment(c: CommitmentRef) -> str:
    parts = [f"{c.id} {c.label!r}", f"state={c.state}"]
    if c.owner_name:
        parts.append(f"owner={c.owner_name}")
    if c.due_at is not None:
        parts.append(f"due={_fmt_dt(c.due_at)}")
    if c.pressure:
        parts.append(f"pressure={c.pressure}")
    return " ".join(parts)


def _fmt_resource(r: ResourceRef) -> str:
    parts = [f"{r.id} {r.kind}={r.name}", f"health={r.health}"]
    if r.revenue_at_risk:
        parts.append(f"revenue_at_risk={r.revenue_at_risk}")
    return " ".join(parts)


def _fmt_state_change(sc: StateChange) -> str:
    base = f"{sc.subject_kind}:{sc.subject_id} {sc.from_state} \u2192 {sc.to_state}"
    base += f" at {_fmt_dt(sc.at)}"
    if sc.reason:
        base += f" reason={sc.reason!r}"
    return base


def summarize_snapshot(
    snap: SubstrateSnapshot,
    *,
    founder: FounderContext | None = None,
) -> str:
    """Return a compact multi-line summary ready for a prompt body."""
    lines: list[str] = [f"time_of_day_bucket: {snap.time_of_day_bucket}"]
    lines.append(f"signals_watched_count: {snap.signals_watched_count}")
    if founder:
        lines.append(
            f"founder: name={founder.display_name} role={founder.role} "
            f"rhythms={founder.observed_rhythms or 'n/a'}"
        )
    lines.append(f"captured_at: {_fmt_dt(snap.captured_at)}")

    if snap.top_models:
        lines.append("top_models:")
        for m in snap.top_models:
            lines.append(f"  - {_fmt_model(m)}")
    else:
        lines.append("top_models: []")

    if snap.active_commitments:
        lines.append("active_commitments:")
        for c in snap.active_commitments:
            lines.append(f"  - {_fmt_commitment(c)}")
    else:
        lines.append("active_commitments: []")

    if snap.customer_resources:
        lines.append("customer_resources:")
        for r in snap.customer_resources:
            lines.append(f"  - {_fmt_resource(r)}")
    else:
        lines.append("customer_resources: []")

    if snap.recent_state_changes:
        lines.append("recent_state_changes:")
        for sc in snap.recent_state_changes:
            lines.append(f"  - {_fmt_state_change(sc)}")
    else:
        lines.append("recent_state_changes: []")

    if snap.anomalies:
        lines.append("anomalies:")
        for a in snap.anomalies:
            lines.append(f"  - {a.id} kind={a.kind} severity={a.severity} desc={a.description!r}")
    else:
        lines.append("anomalies: []")

    cc: ConversationContext = snap.conversation_context
    lines.append(
        "conversation_context: "
        f"was_here_recently={cc.was_here_recently} "
        f"last_visit_at={_fmt_dt(cc.last_visit_at) if cc.last_visit_at else 'n/a'} "
        f"last_queries={cc.last_queries or []}"
    )

    return "\n".join(lines)


__all__ = ["summarize_snapshot"]
