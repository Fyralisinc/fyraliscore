"""Evidence evaluator for Resolution Threads.

This is the backend watcher primitive. Connector-specific pollers can
feed evidence explicitly through the API; this evaluator also inspects
the linked Decision Delta evidence and marks watched signals as seen
when the expected source/proof appears.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from services.resolution_threads import repo


@dataclass
class EvaluationResult:
    thread_id: UUID
    signals_seen: int
    signals_checked: int
    matched: list[dict[str, Any]]


async def evaluate_thread(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    thread_id: UUID,
    actor_id: UUID | None = None,
) -> tuple[repo.ResolutionThread, EvaluationResult]:
    thread = await repo.get_thread(
        conn,
        tenant_id=tenant_id,
        thread_id=thread_id,
    )
    if thread is None:
        raise repo.ResolutionThreadNotFoundError(
            f"resolution thread {thread_id} not found",
            thread_id=str(thread_id),
        )

    evidence = await _load_evidence(conn, tenant_id=tenant_id, thread=thread)
    matched: list[dict[str, Any]] = []
    seen = 0
    checked = 0

    for signal in thread.watched_signals:
        if signal.status in {"seen", "contradicted"}:
            continue
        checked += 1
        evidence_hit = _match_signal(signal, evidence)
        if evidence_hit is None:
            if signal.status == "watching":
                await repo.update_signal_status(
                    conn,
                    tenant_id=tenant_id,
                    thread_id=thread.id,
                    signal_id=signal.id,
                    status="missing",
                    actor_id=actor_id,
                )
            continue

        seen += 1
        matched.append(evidence_hit)
        await repo.update_signal_status(
            conn,
            tenant_id=tenant_id,
            thread_id=thread.id,
            signal_id=signal.id,
            status="seen",
            actor_id=actor_id,
            matched_evidence=evidence_hit,
            observed_at=_parse_observed_at(evidence_hit),
        )

    refreshed = await repo.get_thread(
        conn,
        tenant_id=tenant_id,
        thread_id=thread_id,
    )
    assert refreshed is not None
    await repo.append_event(
        conn,
        tenant_id=tenant_id,
        thread_id=thread_id,
        event_type="evaluated",
        actor_id=actor_id,
        payload={
            "signals_checked": checked,
            "signals_seen": seen,
            "matched": matched,
        },
    )
    return refreshed, EvaluationResult(
        thread_id=thread_id,
        signals_seen=seen,
        signals_checked=checked,
        matched=matched,
    )


async def observe_signal(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    thread_id: UUID,
    signal_id: UUID,
    status: str,
    evidence: dict[str, Any] | None = None,
    actor_id: UUID | None = None,
) -> repo.ResolutionThread:
    """Manual/connector evidence ingress for one watched signal."""
    if status not in {"seen", "contradicted", "missing", "watching"}:
        status = "seen"
    return await repo.update_signal_status(
        conn,
        tenant_id=tenant_id,
        thread_id=thread_id,
        signal_id=signal_id,
        status=status,
        actor_id=actor_id,
        matched_evidence=evidence,
        observed_at=_parse_observed_at(evidence or {}),
    )


async def _load_evidence(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    thread: repo.ResolutionThread,
) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    if thread.source_decision_delta_id is not None:
        rows = await conn.fetch(
            """
            SELECT e.id, e.source, e.title, e.ts, e.trust_tier, e.excerpt, e.weight
            FROM decision_delta_evidence e
            JOIN decision_deltas d ON d.id = e.delta_id
            WHERE d.tenant_id = $1 AND e.delta_id = $2
            ORDER BY e.ordinal ASC, e.ts ASC
            """,
            tenant_id,
            thread.source_decision_delta_id,
        )
        for row in rows:
            evidence.append({
                "id": str(row["id"]),
                "source": row["source"],
                "title": row["title"],
                "occurredAt": row["ts"].isoformat() if row["ts"] else None,
                "trustTier": row["trust_tier"],
                "excerpt": row["excerpt"],
                "weight": float(row["weight"]) if row["weight"] is not None else None,
            })

    # Keep the hook open for future connector-normalized evidence.
    # The table exists today as part of the substrate and often carries
    # source/title-ish payloads in proposition or metadata JSON.
    if thread.target_node_id is not None:
        rows = await conn.fetch(
            """
            SELECT id, kind, proposition, updated_at
            FROM models
            WHERE tenant_id = $1 AND id = $2
            """,
            tenant_id,
            thread.target_node_id,
        )
        for row in rows:
            evidence.append({
                "id": str(row["id"]),
                "source": row["kind"],
                "title": str(row["proposition"] or ""),
                "occurredAt": row["updated_at"].isoformat() if row["updated_at"] else None,
                "excerpt": str(row["proposition"] or ""),
            })
    return evidence


def _match_signal(
    signal: repo.ResolutionWatchedSignal,
    evidence: list[dict[str, Any]],
) -> dict[str, Any] | None:
    for item in evidence:
        if not _source_matches(signal.source_type, item):
            continue
        haystack = " ".join(
            str(item.get(k) or "")
            for k in ("source", "title", "excerpt", "trustTier")
        ).lower()
        keywords = _keywords(signal.label + " " + signal.expected)
        if not keywords:
            return item
        if any(k in haystack for k in keywords):
            return item
    return None


def _source_matches(source_type: str, item: dict[str, Any]) -> bool:
    expected = _source_tokens(source_type)
    if not expected:
        return True
    haystack = " ".join(
        str(item.get(k) or "")
        for k in ("source", "title", "excerpt")
    ).lower()
    return any(token in haystack for token in expected)


def _source_tokens(value: str) -> set[str]:
    return {
        token
        for token in re.split(r"[^a-z0-9]+", value.lower())
        if len(token) >= 3 and token not in {"and", "the", "with"}
    }


def _keywords(value: str) -> set[str]:
    stop = {
        "the", "and", "for", "with", "this", "that", "will", "appears",
        "before", "after", "week", "call", "sent", "proof", "watching",
    }
    return {
        token
        for token in re.split(r"[^a-z0-9]+", value.lower())
        if len(token) >= 4 and token not in stop
    }


def _parse_observed_at(evidence: dict[str, Any]) -> datetime | None:
    raw = evidence.get("occurredAt") or evidence.get("ts") or evidence.get("observedAt")
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
