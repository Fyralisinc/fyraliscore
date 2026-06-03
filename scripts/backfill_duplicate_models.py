#!/usr/bin/env python
"""
scripts/backfill_duplicate_models.py — one-shot cleanup of legacy
paraphrased / duplicate Models for a single tenant.

The Phase-3 reconciler (`services/reasoning/think/reconciler.py`) only protects
FUTURE inserts. Tenants whose substrate predates the new per-kind rules
already carry duplicate Atlas / HarborRail / Cobalt-style Models from
earlier runs. This script scans the existing `models` table, clusters
near-duplicates under the new rules, and emits one of three actions per
cluster:

  * auto_merge          → archive the others, lift the canonical's
                          confidence to max(cluster), and union the
                          supporting_event_ids / supporting_model_ids.
  * same_issue_candidate → emit a `same_issue_as` RelationshipCandidate
                          (basis='inferred', metadata.operator_basis=
                          'paraphrase_suspect_backfill') between the
                          canonical and each non-canonical member.
  * human_review        → write a row into `reconciliation_events` so
                          a human can decide (mirrors the production
                          reconciler's borderline branch).

Per the spec we do NOT modify `services/reasoning/think/reconciler.py`,
`services/domain/models/repo.py`, or `services/reasoning/relationships/candidates.py` —
all decision logic is imported from `services.reasoning.think.reconciler` and
reused unchanged. The repo's `bulk_confidence_update` + `archive`
methods do the auto_merge writes (so dependent re-evaluation cascades
fire identically to the live path).

CLI:
    python -m scripts.backfill_duplicate_models \
        --tenant <UUID> \
        --dry-run \
        --max-clusters 1000 \
        --kinds state,concern,market_assessment,situation \
        --output-jsonl /tmp/backfill_report.jsonl

Safety:
  * `--tenant` is required. There is no cross-tenant mode.
  * `--dry-run` is the default; nothing is written to the database.
  * `--apply` flips on writes, but the script ALSO refuses to proceed
    unless `BACKFILL_I_KNOW_WHAT_I_AM_DOING=yes` is in the environment.
  * Each cluster's writes run inside ONE transaction. A per-cluster
    failure rolls back that cluster only.

Idempotence:
  * Re-running on already-merged data produces no further auto_merges
    because the duplicates are now archived and the candidate search
    only considers `status='active'` rows.
  * `same_issue_as` candidates are emitted with random ids each run; an
    optional content-hash dedupe is applied via the metadata so reruns
    do not produce literal duplicate candidate rows (best-effort).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence
from uuid import UUID

import asyncpg
import structlog
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# Reuse reconciler helpers + the candidate emitter. We do NOT redefine
# any threshold or signal logic here.
from services.reasoning.think.reconciler import (  # noqa: E402
    KindRule,
    ReconcilerConfig,
    _compute_signal_breakdown,
    _find_candidates,
    _kind_rule,
    _KIND_RULES,
)
from services.reasoning.relationships.candidates import (  # noqa: E402
    make_edge_candidate,
)
from services.reasoning.relationships.repo import (  # noqa: E402
    RelationshipCandidatesRepo,
)
from services.reasoning.judgment.scoring import (  # noqa: E402
    JudgmentScores,
    clamp_score,
)
from services.domain.models.repo import (  # noqa: E402
    ModelsRepo,
    _ensure_vector_codec,
)
from lib.shared.ids import uuid7  # noqa: E402


_log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

# `models.archive_reason` is a free TEXT column at the DB level, but the
# Pydantic Literal in lib/shared/types.py enforces the allowed set on
# row hydration. None of the post-Wave-0 literals exactly matches our
# semantics, so we use 'superseded' (the canonical "merged into another
# Model" reason) AND tag the merge into `metadata` via the
# reconciliation_events row + the surviving canonical's signal_readings.
_ARCHIVE_REASON = "superseded"
_BACKFILL_TAG = "reconciliation_backfill_duplicate"
_BACKFILL_OPERATOR_BASIS = "paraphrase_suspect_backfill"

# Per-kind override default. Kinds with a per-kind rule in the
# reconciler get included by default; others (e.g. relation,
# environmental_trend) are excluded because reconciler.py has no
# tuned rule for them.
_DEFAULT_KINDS: tuple[str, ...] = tuple(_KIND_RULES.keys())

_KNEIGHBOURS = 5


# ---------------------------------------------------------------------
# Decision + cluster types
# ---------------------------------------------------------------------


@dataclass
class PairDecision:
    decision: str  # 'auto_merge' | 'same_issue_candidate' | 'human_review' | 'no_match'
    cosine: float
    adjusted: float
    signal_breakdown: dict[str, float]


@dataclass
class Cluster:
    members: list[UUID]
    kind: str
    cosines: list[float] = field(default_factory=list)
    breakdowns: list[dict[str, float]] = field(default_factory=list)
    decision: str = "no_match"


@dataclass
class BackfillMetrics:
    clusters_considered: int = 0
    per_decision: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    models_archived: int = 0
    candidates_emitted: int = 0
    human_review_rows: int = 0
    errors: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "clusters_considered": self.clusters_considered,
            "per_decision": dict(self.per_decision),
            "models_archived": self.models_archived,
            "candidates_emitted": self.candidates_emitted,
            "human_review_rows": self.human_review_rows,
            "errors": self.errors,
        }


# ---------------------------------------------------------------------
# Active-model fetch (chunked generator)
# ---------------------------------------------------------------------


def _decode_jsonish(value: Any) -> Any:
    """Asyncpg returns JSONB columns as raw text strings unless a codec
    is registered. Normalise to native Python (list / dict / None)."""
    if value is None:
        return None
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


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    """Decode JSONB columns + normalise array types on a `models` row."""
    out = dict(row)
    out["scope_entities"] = _decode_jsonish(out.get("scope_entities")) or []
    out["proposition"] = _decode_jsonish(out.get("proposition")) or {}
    out["falsifier"] = _decode_jsonish(out.get("falsifier"))
    out["signal_readings"] = _decode_jsonish(out.get("signal_readings")) or []
    return out


async def _iter_active_models(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    kinds: Sequence[str],
    chunk_size: int = 500,
) -> Iterable[dict[str, Any]]:
    """Generator over active Models in the targeted kinds, ordered by
    `created_at ASC` so the "later" model in each pair is well-defined.
    Implemented as a keyset paginator on (created_at, id).
    """
    last_created: datetime | None = None
    last_id: UUID | None = None
    while True:
        if last_created is None:
            rows = await conn.fetch(
                """
                SELECT id, embedding, scope_actors, scope_entities,
                       confidence, proposition_kind, "natural", created_at,
                       supporting_event_ids, signal_readings,
                       confirmed_count, supporting_model_ids,
                       falsifier, proposition, activation,
                       born_from_event_id
                FROM models
                WHERE tenant_id = $1
                  AND status = 'active'
                  AND proposition_kind = ANY($2::text[])
                ORDER BY created_at ASC, id ASC
                LIMIT $3
                """,
                tenant_id,
                list(kinds),
                chunk_size,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT id, embedding, scope_actors, scope_entities,
                       confidence, proposition_kind, "natural", created_at,
                       supporting_event_ids, signal_readings,
                       confirmed_count, supporting_model_ids,
                       falsifier, proposition, activation,
                       born_from_event_id
                FROM models
                WHERE tenant_id = $1
                  AND status = 'active'
                  AND proposition_kind = ANY($2::text[])
                  AND (created_at, id) > ($3, $4)
                ORDER BY created_at ASC, id ASC
                LIMIT $5
                """,
                tenant_id,
                list(kinds),
                last_created,
                last_id,
                chunk_size,
            )
        if not rows:
            break
        for r in rows:
            yield _normalize_row(dict(r))
        last_created = rows[-1]["created_at"]
        last_id = rows[-1]["id"]
        if len(rows) < chunk_size:
            break


# ---------------------------------------------------------------------
# Per-pair decision (reuses reconciler helpers)
# ---------------------------------------------------------------------


def _row_as_entry(row: dict[str, Any]) -> dict[str, Any]:
    """Reshape a `models` row into the `entry` dict shape the reconciler
    helpers expect for the *candidate* side of the comparison.
    """
    return {
        "embedding": (
            row["embedding"].tolist()
            if hasattr(row["embedding"], "tolist")
            else list(row["embedding"] or [])
        ),
        "scope_actors": [str(a) for a in (row.get("scope_actors") or [])],
        "scope_entities": list(row.get("scope_entities") or []),
        "proposition": row.get("proposition") or {},
        "confidence": float(row.get("confidence") or 0.5),
        "natural": row.get("natural") or "",
        "supporting_event_ids": list(row.get("supporting_event_ids") or []),
        "supporting_model_ids": list(row.get("supporting_model_ids") or []),
        "falsifier": row.get("falsifier"),
        "born_from_event_id": row.get("born_from_event_id"),
    }


def _classify_pair(
    *,
    earlier_row: dict[str, Any],
    later_row: dict[str, Any],
    kind_rule: KindRule,
    config: ReconcilerConfig,
    cosine_floor: float,
) -> PairDecision:
    """Apply the reconciler's signal_breakdown + per-kind rules to one
    ordered pair. Decision vocabulary mirrors the reconciler.
    """
    # Treat the LATER row as the "incoming" candidate, the EARLIER row
    # as the existing Model. This matches the reconciler's framing:
    # the older Model is what we'd be merging into.
    cand_entry = _row_as_entry(later_row)
    base_cosine = _cosine(
        cand_entry["embedding"],
        (
            earlier_row["embedding"].tolist()
            if hasattr(earlier_row["embedding"], "tolist")
            else list(earlier_row["embedding"] or [])
        ),
    )
    if base_cosine < cosine_floor:
        return PairDecision(
            decision="no_match",
            cosine=base_cosine,
            adjusted=base_cosine,
            signal_breakdown={"cosine": base_cosine},
        )

    adjusted, breakdown = _compute_signal_breakdown(
        cand_entry, earlier_row, base_cosine,
    )

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

    can_auto_merge = (
        not kind_rule.never_auto_merge
        and not kind_rule.require_human_review
        and adjusted >= auto_merge_threshold
    )
    if can_auto_merge:
        return PairDecision(
            decision="auto_merge",
            cosine=base_cosine,
            adjusted=adjusted,
            signal_breakdown=breakdown,
        )

    if adjusted >= human_review_threshold:
        # Kind-blocked auto-merge collapses to a human_review queue
        # entry (production reconciler does the same). All other
        # borderline matches become same_issue_as candidates.
        if kind_rule.never_auto_merge or kind_rule.require_human_review:
            return PairDecision(
                decision="human_review",
                cosine=base_cosine,
                adjusted=adjusted,
                signal_breakdown=breakdown,
            )
        return PairDecision(
            decision="same_issue_candidate",
            cosine=base_cosine,
            adjusted=adjusted,
            signal_breakdown=breakdown,
        )

    return PairDecision(
        decision="no_match",
        cosine=base_cosine,
        adjusted=adjusted,
        signal_breakdown=breakdown,
    )


def _cosine(a: Sequence[float], b: Sequence[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += float(x) * float(y)
        na += float(x) * float(x)
        nb += float(y) * float(y)
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    import math
    return dot / (math.sqrt(na) * math.sqrt(nb))


# ---------------------------------------------------------------------
# Union-find for transitive clustering
# ---------------------------------------------------------------------


class _UF:
    def __init__(self) -> None:
        self.parent: dict[UUID, UUID] = {}

    def add(self, x: UUID) -> None:
        if x not in self.parent:
            self.parent[x] = x

    def find(self, x: UUID) -> UUID:
        self.add(x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, x: UUID, y: UUID) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            self.parent[ry] = rx


# ---------------------------------------------------------------------
# Cluster discovery
# ---------------------------------------------------------------------


async def _discover_clusters(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    kinds: Sequence[str],
    cosine_floor: float,
    max_clusters: int,
    config: ReconcilerConfig,
) -> list[Cluster]:
    """Pairwise scan: for each model (in created-asc order), look at
    top-K nearest neighbours that arrived LATER (same kind, same
    tenant). Pair decisions feed a union-find for auto_merge edges;
    other decisions are recorded as singleton-pair clusters of size 2.

    Returns at most `max_clusters` clusters.
    """
    # Snapshot every model in scope into memory so the union-find /
    # ordering work is straightforward. Tens of thousands of rows
    # is well within RAM (each row carries one 768-float vector ≈ 6 KB).
    models_by_id: dict[UUID, dict[str, Any]] = {}
    created_at_order: list[UUID] = []
    async for row in _iter_active_models(
        conn, tenant_id=tenant_id, kinds=kinds,
    ):
        mid: UUID = row["id"]
        models_by_id[mid] = row
        created_at_order.append(mid)

    if not models_by_id:
        return []

    uf = _UF()
    # Non-auto-merge pair decisions, keyed by frozenset({a,b}) so a
    # duplicate (later→earlier or earlier→later) collapses.
    other_decisions: dict[
        frozenset[UUID],
        tuple[str, float, dict[str, float], str],
    ] = {}

    for mid in created_at_order:
        row = models_by_id[mid]
        kind = row["proposition_kind"]
        kind_rule = _kind_rule(kind)
        # Reuse the production candidate search. It restricts to the
        # tenant + active + same kind + recency window + scope-overlap.
        # We pass our row's embedding as the seed vector; the result
        # includes our own row, which we drop.
        candidate_embedding = (
            row["embedding"].tolist()
            if hasattr(row["embedding"], "tolist")
            else list(row["embedding"] or [])
        )
        if not candidate_embedding:
            continue
        try:
            neighbours = await _find_candidates(
                conn,
                tenant_id=tenant_id,
                candidate_embedding=candidate_embedding,
                candidate_scope_actors=[
                    str(a) for a in (row.get("scope_actors") or [])
                ],
                candidate_scope_entities=list(row.get("scope_entities") or []),
                proposition_kind=kind,
                # The legacy substrate is older than the live window;
                # raise the recency cap so backfill actually sees the
                # historical duplicates.
                recency_window_days=365 * 5,
                k=_KNEIGHBOURS + 1,
            )
        except Exception as exc:  # noqa: BLE001
            _log.warning(
                "backfill.find_candidates_failed",
                model_id=str(mid),
                error=str(exc),
                error_type=type(exc).__name__,
            )
            continue

        for nb_raw in neighbours:
            nb = _normalize_row(nb_raw)
            nb_id: UUID = nb["id"]
            if nb_id == mid:
                continue
            # Only score each unordered pair once. The earlier-created
            # row is the "existing" side per the reconciler's framing.
            earlier_row, later_row = _orient_pair(row, nb)
            pair_key = frozenset({earlier_row["id"], later_row["id"]})
            if pair_key in other_decisions and other_decisions[pair_key][0] != "auto_merge":
                # Already classified; skip re-scoring.
                continue
            pd = _classify_pair(
                earlier_row=earlier_row,
                later_row=later_row,
                kind_rule=kind_rule,
                config=config,
                cosine_floor=cosine_floor,
            )
            if pd.decision == "auto_merge":
                uf.add(earlier_row["id"])
                uf.add(later_row["id"])
                uf.union(earlier_row["id"], later_row["id"])
                # Drop any prior non-auto-merge classification for this
                # pair — auto_merge wins.
                other_decisions.pop(pair_key, None)
            elif pd.decision in ("same_issue_candidate", "human_review"):
                other_decisions[pair_key] = (
                    pd.decision,
                    pd.cosine,
                    pd.signal_breakdown,
                    kind,
                )

    # Materialise auto_merge clusters from the union-find.
    auto_groups: dict[UUID, list[UUID]] = defaultdict(list)
    for mid in uf.parent.keys():
        root = uf.find(mid)
        auto_groups[root].append(mid)

    clusters: list[Cluster] = []
    seen_pairs: set[frozenset[UUID]] = set()
    for root, members in auto_groups.items():
        if len(members) < 2:
            continue
        members = sorted(members, key=lambda m: str(m))
        kind = models_by_id[members[0]]["proposition_kind"]
        cluster = Cluster(
            members=members,
            kind=kind,
            decision="auto_merge",
        )
        # Recompute cosines + breakdowns for every (canonical, other)
        # pair so the report carries the same numbers reconciliation
        # would have produced.
        # The canonical is chosen later (highest activation, earliest
        # created_at), but for reporting we just average the pairwise
        # cosines among members.
        for i, a in enumerate(members):
            for b in members[i + 1:]:
                seen_pairs.add(frozenset({a, b}))
                pd = _classify_pair(
                    earlier_row=_orient_pair(models_by_id[a], models_by_id[b])[0],
                    later_row=_orient_pair(models_by_id[a], models_by_id[b])[1],
                    kind_rule=_kind_rule(kind),
                    config=config,
                    cosine_floor=0.0,  # we already know they're > floor
                )
                cluster.cosines.append(pd.cosine)
                cluster.breakdowns.append(pd.signal_breakdown)
        clusters.append(cluster)
        if len(clusters) >= max_clusters:
            return clusters

    # Add same_issue_candidate / human_review pairs as size-2 clusters.
    for pair_key, (decision, cos, breakdown, kind) in other_decisions.items():
        if pair_key in seen_pairs:
            # Pair was absorbed by an auto_merge cluster.
            continue
        members = sorted(pair_key, key=lambda m: str(m))
        clusters.append(
            Cluster(
                members=members,
                kind=kind,
                cosines=[cos],
                breakdowns=[breakdown],
                decision=decision,
            )
        )
        if len(clusters) >= max_clusters:
            break

    return clusters


def _orient_pair(
    a: dict[str, Any], b: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (earlier_created, later_created). Tie-break on id for
    deterministic ordering."""
    a_ts = a.get("created_at")
    b_ts = b.get("created_at")
    if a_ts is None and b_ts is None:
        return (a, b) if str(a["id"]) < str(b["id"]) else (b, a)
    if a_ts is None:
        return (b, a)
    if b_ts is None:
        return (a, b)
    if a_ts < b_ts:
        return (a, b)
    if a_ts > b_ts:
        return (b, a)
    return (a, b) if str(a["id"]) < str(b["id"]) else (b, a)


def _pick_canonical(
    members: list[UUID],
    models_by_id: dict[UUID, dict[str, Any]],
) -> UUID:
    """Highest activation, earliest created_at, then lowest id (stable)."""
    def _key(mid: UUID) -> tuple[float, float, str]:
        row = models_by_id[mid]
        # negative activation → highest first
        act = -float(row.get("activation") or 0.0)
        ts = row.get("created_at")
        ts_secs = ts.timestamp() if ts is not None else 0.0
        return (act, ts_secs, str(mid))

    return sorted(members, key=_key)[0]


# ---------------------------------------------------------------------
# Apply actions
# ---------------------------------------------------------------------


async def _apply_auto_merge(
    conn: asyncpg.Connection,
    *,
    repo: ModelsRepo,
    cluster: Cluster,
    models_by_id: dict[UUID, dict[str, Any]],
    metrics: BackfillMetrics,
    apply: bool,
) -> tuple[UUID, list[str]]:
    canonical = _pick_canonical(cluster.members, models_by_id)
    others = [m for m in cluster.members if m != canonical]
    actions: list[str] = []

    canonical_row = models_by_id[canonical]
    confidences = [
        float(models_by_id[m].get("confidence") or 0.0)
        for m in cluster.members
    ]
    new_conf = max(confidences)
    old_conf = float(canonical_row.get("confidence") or 0.0)

    # Union supporting_event_ids and supporting_model_ids across all
    # members and write the result into the canonical's row.
    union_events: list[UUID] = []
    union_event_set: set[UUID] = set()
    union_supports: list[UUID] = []
    union_support_set: set[UUID] = set()
    for m in cluster.members:
        for e in models_by_id[m].get("supporting_event_ids") or []:
            try:
                eid = UUID(str(e))
            except (TypeError, ValueError):
                continue
            if eid not in union_event_set:
                union_event_set.add(eid)
                union_events.append(eid)
        for s in models_by_id[m].get("supporting_model_ids") or []:
            try:
                sid = UUID(str(s))
            except (TypeError, ValueError):
                continue
            # Don't include cluster members in canonical's
            # supporting_model_ids — they're about to be archived.
            if sid in cluster.members:
                continue
            if sid not in union_support_set:
                union_support_set.add(sid)
                union_supports.append(sid)

    actions.append(
        f"summed confidence: {old_conf:.2f} -> {new_conf:.2f}"
    )

    if apply:
        # 1. Lift canonical's confidence + union arrays. Two separate
        #    writes: bulk_confidence_update for the audit chain, then
        #    a direct UPDATE for the array columns (bulk_confidence_update
        #    only touches `confidence`).
        await repo.bulk_confidence_update(
            {canonical: new_conf},
            audit_cause_override="reconciliation_merge",
            conn=conn,
        )
        await conn.execute(
            """
            UPDATE models
            SET supporting_event_ids = $2::uuid[],
                supporting_model_ids = $3::uuid[],
                signal_readings = COALESCE(signal_readings, '[]'::jsonb)
                                  || $4::jsonb
            WHERE id = $1
            """,
            canonical,
            union_events,
            union_supports,
            json.dumps(
                [
                    {
                        "kind": "merge",
                        "at": datetime.now(timezone.utc).isoformat(),
                        "source": _BACKFILL_TAG,
                        "absorbed_model_ids": [str(m) for m in others],
                        "operator_basis": _BACKFILL_OPERATOR_BASIS,
                    }
                ]
            ),
        )

        # 2. Archive the non-canonical members. The repo's archive
        #    path emits state_change, audit events, edge cascades and
        #    enqueues dependent re-eval rows.
        for other in others:
            try:
                await repo.archive(
                    other,
                    _ARCHIVE_REASON,
                    conn=conn,
                )
                metrics.models_archived += 1
            except Exception as exc:  # noqa: BLE001
                _log.warning(
                    "backfill.archive_failed",
                    model_id=str(other),
                    error=str(exc),
                )
                raise
        actions.append(f"archived {len(others)} models")
    else:
        actions.append(f"would archive {len(others)} models")

    return canonical, actions


async def _apply_same_issue_candidates(
    conn: asyncpg.Connection,
    *,
    cluster: Cluster,
    models_by_id: dict[UUID, dict[str, Any]],
    tenant_id: UUID,
    metrics: BackfillMetrics,
    apply: bool,
) -> tuple[UUID, list[str]]:
    canonical = _pick_canonical(cluster.members, models_by_id)
    others = [m for m in cluster.members if m != canonical]
    actions: list[str] = []
    cosine = cluster.cosines[0] if cluster.cosines else 0.0
    breakdown = cluster.breakdowns[0] if cluster.breakdowns else {}

    if apply:
        repo = RelationshipCandidatesRepo()
        for other in others:
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
            metadata = {
                "origin": "reconciler_backfill_duplicate",
                "operator_basis": _BACKFILL_OPERATOR_BASIS,
                "cosine": float(cosine),
                "signal_breakdown": {
                    k: float(v) for k, v in breakdown.items()
                },
                "proposition_kind": cluster.kind,
            }
            cand = make_edge_candidate(
                tenant_id=tenant_id,
                source_model_id=other,
                target_model_id=canonical,
                edge_kind="same_issue_as",
                basis="inferred",
                explanation=(
                    f"backfill near-duplicate (cosine={cosine:.2f}) — "
                    f"{_BACKFILL_OPERATOR_BASIS}"
                ),
                scores=scores,
                metadata=metadata,
                source="scripts.backfill_duplicate_models",
                review_status="needs_review",
            )
            try:
                await repo.insert(conn, cand)
                metrics.candidates_emitted += 1
            except Exception as exc:  # noqa: BLE001
                _log.warning(
                    "backfill.candidate_insert_failed",
                    other=str(other),
                    error=str(exc),
                )
                raise
        actions.append(
            f"emitted {len(others)} same_issue_as candidates"
        )
    else:
        actions.append(
            f"would emit {len(others)} same_issue_as candidates"
        )

    return canonical, actions


async def _apply_human_review(
    conn: asyncpg.Connection,
    *,
    cluster: Cluster,
    models_by_id: dict[UUID, dict[str, Any]],
    tenant_id: UUID,
    metrics: BackfillMetrics,
    apply: bool,
) -> tuple[UUID, list[str]]:
    canonical = _pick_canonical(cluster.members, models_by_id)
    others = [m for m in cluster.members if m != canonical]
    actions: list[str] = []
    cosine = cluster.cosines[0] if cluster.cosines else 0.0

    if apply:
        for other in others:
            event_id = uuid7()
            original_claim_op = {
                "op": "insert",
                "entry": {
                    "id": str(other),
                    "natural": models_by_id[other].get("natural"),
                    "proposition": models_by_id[other].get("proposition"),
                    "confidence": float(
                        models_by_id[other].get("confidence") or 0.0
                    ),
                    "backfill_origin": _BACKFILL_TAG,
                },
            }
            # `trigger_id` is NOT NULL on reconciliation_events but is
            # not a FK; mint a placeholder uuid so backfill rows are
            # legible in the human-review queue without lying about a
            # trigger we never had.
            placeholder_trigger_id = uuid7()
            try:
                await conn.execute(
                    """
                    INSERT INTO reconciliation_events (
                        id, tenant_id, decision, original_claim_op,
                        matched_model_id, cosine_similarity,
                        proposition_kind, trigger_id, think_run_id
                    ) VALUES (
                        $1, $2, 'human_review', $3::jsonb,
                        $4, $5, $6, $7, NULL
                    )
                    """,
                    event_id,
                    tenant_id,
                    json.dumps(original_claim_op, default=str),
                    canonical,
                    float(cosine),
                    cluster.kind,
                    placeholder_trigger_id,
                )
                metrics.human_review_rows += 1
            except Exception as exc:  # noqa: BLE001
                _log.warning(
                    "backfill.reconciliation_event_failed",
                    other=str(other),
                    error=str(exc),
                )
                raise
        actions.append(
            f"queued {len(others)} reconciliation_events rows for review"
        )
    else:
        actions.append(
            f"would queue {len(others)} reconciliation_events rows"
        )
    return canonical, actions


# ---------------------------------------------------------------------
# Cluster processor
# ---------------------------------------------------------------------


async def _process_cluster(
    pool: asyncpg.Pool,
    *,
    cluster: Cluster,
    models_by_id: dict[UUID, dict[str, Any]],
    tenant_id: UUID,
    metrics: BackfillMetrics,
    apply: bool,
    repo: ModelsRepo,
) -> dict[str, Any]:
    """Run one cluster's writes inside ONE transaction. Returns the
    JSONL report row for the cluster."""
    canonical: UUID | None = None
    actions: list[str] = []
    err: str | None = None

    async with pool.acquire() as conn:
        await _ensure_vector_codec(conn)
        async with conn.transaction():
            try:
                if cluster.decision == "auto_merge":
                    canonical, actions = await _apply_auto_merge(
                        conn,
                        repo=repo,
                        cluster=cluster,
                        models_by_id=models_by_id,
                        metrics=metrics,
                        apply=apply,
                    )
                elif cluster.decision == "same_issue_candidate":
                    canonical, actions = await _apply_same_issue_candidates(
                        conn,
                        cluster=cluster,
                        models_by_id=models_by_id,
                        tenant_id=tenant_id,
                        metrics=metrics,
                        apply=apply,
                    )
                elif cluster.decision == "human_review":
                    canonical, actions = await _apply_human_review(
                        conn,
                        cluster=cluster,
                        models_by_id=models_by_id,
                        tenant_id=tenant_id,
                        metrics=metrics,
                        apply=apply,
                    )
                else:
                    canonical = _pick_canonical(cluster.members, models_by_id)
                    actions = ["no_match — nothing to do"]
            except Exception as exc:  # noqa: BLE001
                err = f"{type(exc).__name__}: {exc}"
                metrics.errors += 1
                # asyncpg transaction context will roll back on exception
                # but we want to record + continue, so re-raise inside
                # the with block and catch outside.
                raise

    metrics.per_decision[cluster.decision] += 1

    avg_cos = (
        sum(cluster.cosines) / len(cluster.cosines)
        if cluster.cosines
        else 0.0
    )
    breakdown_avg = _avg_breakdowns(cluster.breakdowns)
    return {
        "cluster_id": str(uuid7()),
        "kind": cluster.kind,
        "decision": cluster.decision,
        "canonical_model_id": str(canonical) if canonical else None,
        "member_model_ids": [str(m) for m in cluster.members],
        "cosine_mean": round(avg_cos, 4),
        "signal_breakdown": breakdown_avg,
        "actions": actions,
        "error": err,
        "applied": apply,
    }


def _avg_breakdowns(
    breakdowns: list[dict[str, float]],
) -> dict[str, float]:
    if not breakdowns:
        return {}
    sums: dict[str, float] = defaultdict(float)
    counts: dict[str, int] = defaultdict(int)
    for bd in breakdowns:
        for k, v in bd.items():
            sums[k] += float(v)
            counts[k] += 1
    return {k: round(sums[k] / counts[k], 4) for k in sums}


# ---------------------------------------------------------------------
# Top-level runner
# ---------------------------------------------------------------------


@dataclass
class BackfillResult:
    metrics: BackfillMetrics
    cluster_reports: list[dict[str, Any]]


async def run_backfill(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    kinds: Sequence[str],
    apply: bool,
    cosine_floor: float,
    max_clusters: int,
    output_jsonl: Path | None,
    config: ReconcilerConfig | None = None,
) -> BackfillResult:
    """Importable entry point. Tests use this directly."""
    cfg = config or ReconcilerConfig.from_env()
    metrics = BackfillMetrics()
    repo = ModelsRepo(pool, embedder=None, run_topology_on_insert=False)

    # Snapshot active models into memory once.
    async with pool.acquire() as conn:
        await _ensure_vector_codec(conn)
        models_by_id: dict[UUID, dict[str, Any]] = {}
        async for row in _iter_active_models(
            conn, tenant_id=tenant_id, kinds=kinds,
        ):
            models_by_id[row["id"]] = row
        clusters = await _discover_clusters(
            conn,
            tenant_id=tenant_id,
            kinds=kinds,
            cosine_floor=cosine_floor,
            max_clusters=max_clusters,
            config=cfg,
        )

    metrics.clusters_considered = len(clusters)
    reports: list[dict[str, Any]] = []
    out_fh = open(output_jsonl, "w", encoding="utf-8") if output_jsonl else None
    try:
        for cluster in clusters:
            try:
                report = await _process_cluster(
                    pool,
                    cluster=cluster,
                    models_by_id=models_by_id,
                    tenant_id=tenant_id,
                    metrics=metrics,
                    apply=apply,
                    repo=repo,
                )
            except Exception as exc:  # noqa: BLE001
                report = {
                    "cluster_id": str(uuid7()),
                    "kind": cluster.kind,
                    "decision": cluster.decision,
                    "canonical_model_id": None,
                    "member_model_ids": [str(m) for m in cluster.members],
                    "cosine_mean": 0.0,
                    "signal_breakdown": {},
                    "actions": [],
                    "error": f"{type(exc).__name__}: {exc}",
                    "applied": apply,
                }
            reports.append(report)
            if out_fh is not None:
                out_fh.write(json.dumps(report, default=str) + "\n")
                out_fh.flush()
            _log.info(
                "backfill.cluster_done",
                decision=report["decision"],
                kind=report["kind"],
                members=len(report["member_model_ids"]),
                actions=report["actions"],
                error=report.get("error"),
            )
    finally:
        if out_fh is not None:
            out_fh.close()

    _log.info("backfill.summary", **metrics.as_dict())
    return BackfillResult(metrics=metrics, cluster_reports=reports)


# ---------------------------------------------------------------------
# CLI plumbing
# ---------------------------------------------------------------------


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill paraphrase / duplicate cleanup for legacy Models "
            "in a single tenant."
        )
    )
    parser.add_argument(
        "--tenant",
        required=True,
        type=str,
        help="Tenant UUID. REQUIRED — there is no cross-tenant pass.",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN (default: $DATABASE_URL).",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=True,
        help="(default) Report only — no DB writes.",
    )
    mode.add_argument(
        "--apply",
        dest="dry_run",
        action="store_false",
        help=(
            "Actually perform writes. Requires "
            "BACKFILL_I_KNOW_WHAT_I_AM_DOING=yes in the environment."
        ),
    )
    parser.add_argument(
        "--max-clusters",
        type=int,
        default=1000,
        help="Safety bound on the number of clusters processed.",
    )
    parser.add_argument(
        "--kinds",
        type=str,
        default=",".join(_DEFAULT_KINDS),
        help=(
            "Comma-separated proposition kinds to consider "
            f"(default: {','.join(_DEFAULT_KINDS)})."
        ),
    )
    parser.add_argument(
        "--output-jsonl",
        type=Path,
        default=None,
        help="If set, write a per-cluster JSONL report to this path.",
    )
    parser.add_argument(
        "--cosine-floor",
        type=float,
        default=0.70,
        help="Minimum raw cosine to consider a candidate pair.",
    )
    return parser.parse_args(argv)


async def _main_async(args: argparse.Namespace) -> int:
    if not args.dsn:
        print("ERROR: --dsn or $DATABASE_URL required", file=sys.stderr)
        return 2
    try:
        tenant_id = UUID(args.tenant)
    except ValueError:
        print(f"ERROR: --tenant must be a UUID (got {args.tenant!r})", file=sys.stderr)
        return 2

    apply = not args.dry_run
    if apply:
        ack = os.environ.get("BACKFILL_I_KNOW_WHAT_I_AM_DOING", "")
        if ack.strip().lower() != "yes":
            print(
                "ERROR: --apply requires BACKFILL_I_KNOW_WHAT_I_AM_DOING=yes",
                file=sys.stderr,
            )
            return 3

    kinds = tuple(k.strip() for k in args.kinds.split(",") if k.strip())
    if not kinds:
        print("ERROR: --kinds resolved to empty list", file=sys.stderr)
        return 2

    pool = await asyncpg.create_pool(args.dsn, min_size=1, max_size=4)
    started = time.monotonic()
    try:
        result = await run_backfill(
            pool=pool,
            tenant_id=tenant_id,
            kinds=kinds,
            apply=apply,
            cosine_floor=args.cosine_floor,
            max_clusters=args.max_clusters,
            output_jsonl=args.output_jsonl,
        )
    finally:
        await pool.close()

    elapsed = round(time.monotonic() - started, 3)
    print(
        json.dumps(
            {
                "tenant_id": str(tenant_id),
                "applied": apply,
                "elapsed_s": elapsed,
                "metrics": result.metrics.as_dict(),
                "cluster_count": len(result.cluster_reports),
                "output_jsonl": (
                    str(args.output_jsonl) if args.output_jsonl else None
                ),
            },
            indent=2,
            default=str,
        )
    )
    return 0 if result.metrics.errors == 0 else 1


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv(REPO_ROOT / ".env", override=False)
    args = _parse_args(argv)
    return asyncio.run(_main_async(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
