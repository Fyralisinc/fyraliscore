"""services/greeting/snapshot.py — Phase 2.

Compose the structured `SubstrateSnapshot` that Agent-RND consumes to
render greeting / cards / query grid. Shape is CONTRACTS §2.3.

Scope (per COMPANY-OS-UI-BUILD-PLAN §3 Phase 2):

* `compose_greeting_snapshot`          — top-of-home prose input.
* `compose_card_snapshot(card_kind)`   — top-3 candidates for each of
                                         observation / decision / question.
* `compose_query_grid_snapshot`        — 2 situation-tied + 4 evergreen.

Sources:
  - Models        (services/models/repo.py — high-conf recent state changes)
  - Commitments   (services/acts/commitments.py — active, deadline pressure)
  - Resources     (services/resources/repo.py — customer health)
  - state_changes (services/observations/state_change.py — recency signal)
  - Anomalies     (services/think/anomaly_integration.py → think_anomalies_raw)
  - conversation  (most recent CEO asks — wired from services/query/ when live;
                   empty until then)

Everything here is read-only. We never mutate substrate state; the
worker only composes snapshots.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import UUID

import asyncpg


# =====================================================================
# Public types (CONTRACTS §2.3)
# =====================================================================


@dataclass(frozen=True)
class ModelRef:
    id: UUID
    natural: str
    confidence: float
    confidence_at_assertion: float
    proposition_kind: str | None
    status: str
    last_state_change_at: datetime | None
    scope_actors: list[UUID] = field(default_factory=list)
    scope_entities: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class CommitmentRef:
    id: UUID
    title: str
    state: str
    owner_id: UUID | None
    due_date: datetime | None
    priority: int
    is_critical_path: bool
    days_to_due: int | None
    last_state_change_at: datetime | None


@dataclass(frozen=True)
class ResourceRef:
    id: UUID
    kind: str
    identity: str
    utilization_state: str
    health: str | None
    last_updated_at: datetime | None
    revenue_at_risk_usd: float | None


@dataclass(frozen=True)
class StateChange:
    observation_id: UUID
    entity_id: UUID
    entity_kind: str | None
    kind: str
    occurred_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AnomalyRef:
    id: UUID
    kind: str
    region: dict[str, Any]
    significance: float
    published_at: datetime


@dataclass(frozen=True)
class ConversationContext:
    """Recent CEO asks. Filled from services/query/ once Agent-QRY lands;
    empty-list-by-default until then so the rendering prompt always has
    a well-typed field to read.
    """
    recent_queries: list[dict[str, Any]] = field(default_factory=list)
    last_interaction_at: datetime | None = None


@dataclass(frozen=True)
class FounderContext:
    tenant_id: UUID
    role: str                    # 'ceo' for dogfood
    display_name: str
    timezone_name: str           # e.g. 'Asia/Kathmandu' — UI-side conversion
    observed_rhythms: dict[str, Any] = field(default_factory=dict)


TimeOfDayBucket = Literal[
    "early_morning", "morning", "afternoon", "evening", "late"
]


@dataclass(frozen=True)
class SubstrateSnapshot:
    tenant_id: UUID
    captured_at: datetime
    top_models: list[ModelRef]
    active_commitments: list[CommitmentRef]
    customer_resources: list[ResourceRef]
    recent_state_changes: list[StateChange]
    anomalies: list[AnomalyRef]
    conversation_context: ConversationContext
    time_of_day_bucket: TimeOfDayBucket

    def to_json(self) -> dict[str, Any]:
        """JSON-safe dict for logging / prompt assembly."""
        return _serialise(asdict(self))


@dataclass(frozen=True)
class QueryGridSnapshot:
    tenant_id: UUID
    captured_at: datetime
    situation_queries: list[dict[str, Any]]   # 2, tied to hot observations
    evergreen_queries: list[dict[str, Any]]   # 4, standing patterns
    time_of_day_bucket: TimeOfDayBucket


# =====================================================================
# Composer
# =====================================================================


# Pre-approved icon names (CONTRACTS §4). Used by the evergreen
# standing-query list so that pre-Agent-RND we can still ship a valid
# query_grid payload.
_EVERGREEN_QUERIES: list[dict[str, Any]] = [
    {
        "id": "evergreen:why_calibration",
        "icon": "calibration",
        "label": "why calibration shifted",
        "tag": "evergreen",
        "hot": False,
        "query_template": "Why did calibration shift this week?",
    },
    {
        "id": "evergreen:customer_drift",
        "icon": "customer",
        "label": "customers drifting",
        "tag": "evergreen",
        "hot": False,
        "query_template": "Which customers have drifted from their commitments?",
    },
    {
        "id": "evergreen:timeline",
        "icon": "timeline",
        "label": "what changed yesterday",
        "tag": "evergreen",
        "hot": False,
        "query_template": "What changed in the substrate yesterday?",
    },
    {
        "id": "evergreen:dependency",
        "icon": "dependency",
        "label": "blocked dependencies",
        "tag": "evergreen",
        "hot": False,
        "query_template": "Which commitments are blocked on upstream work?",
    },
    {
        "id": "evergreen:pattern",
        "icon": "pattern",
        "label": "recurring patterns",
        "tag": "evergreen",
        "hot": False,
        "query_template": "What patterns recurred in the last 30 days?",
    },
    {
        "id": "evergreen:brief",
        "icon": "brief",
        "label": "week brief",
        "tag": "evergreen",
        "hot": False,
        "query_template": "Give me the week brief.",
    },
]


class SnapshotComposer:
    """Reads structured state out of the substrate. No writes.

    Every method accepts either an owned pool (constructor default) or
    an explicit `conn` for transactional callers. Methods are kept
    narrow — each returns a ready-to-serialise structure — so the
    scheduler can compose them cheaply.
    """

    # Tunables. Concrete values match COMPANY-OS-UI-BUILD-PLAN §3.
    RECENT_WINDOW_HOURS = 24
    COMMITMENT_DEADLINE_WINDOW_DAYS = 7
    TOP_MODELS = 10
    TOP_CARDS = 3
    TOP_ANOMALIES = 3
    MIN_CONFIDENCE_FOR_TOP = 0.7
    SITUATION_QUERY_COUNT = 2
    EVERGREEN_QUERY_COUNT = 4

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    # -----------------------------------------------------------------
    # Greeting
    # -----------------------------------------------------------------
    async def compose_greeting_snapshot(
        self,
        tenant_id: UUID,
        *,
        now: datetime | None = None,
        conversation_context: ConversationContext | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> SubstrateSnapshot:
        now = _normalise_now(now)
        bucket = _time_of_day_bucket(now)

        async def _run(c: asyncpg.Connection) -> SubstrateSnapshot:
            models = await self._top_models(c, tenant_id, now)
            commits = await self._active_commitments(c, tenant_id, now)
            resources = await self._unhealthy_customer_resources(c, tenant_id)
            changes = await self._recent_state_changes(c, tenant_id, now)
            anomalies = await self._recent_anomalies(c, tenant_id, now)
            return SubstrateSnapshot(
                tenant_id=tenant_id,
                captured_at=now,
                top_models=models,
                active_commitments=commits,
                customer_resources=resources,
                recent_state_changes=changes,
                anomalies=anomalies,
                conversation_context=conversation_context or ConversationContext(),
                time_of_day_bucket=bucket,
            )

        if conn is not None:
            return await _run(conn)
        async with self._pool.acquire() as owned:
            return await _run(owned)

    # -----------------------------------------------------------------
    # Cards — observation / decision / question
    # -----------------------------------------------------------------
    async def compose_card_snapshot(
        self,
        tenant_id: UUID,
        card_kind: Literal["observation", "decision", "question"],
        *,
        now: datetime | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> list[SubstrateSnapshot]:
        """Return up to `TOP_CARDS` snapshots per card kind, each
        centred on one candidate entity.

        Candidates:
          observation — high-significance anomalies (24h window) joined
                        to the Models / Resources in their `region`.
          decision    — active Commitments in 'blocked' or on
                        critical-path with deadline pressure.
          question    — Models whose `confidence_at_assertion - confidence`
                        gap is large and unresolved (unexplained drift).

        Each candidate snapshot reuses the greeting-style surface but
        with the candidate entity pinned at position-0 of the relevant
        list. The rendering service uses that hint to pick reasoning /
        evidence / verbs.
        """
        now = _normalise_now(now)
        bucket = _time_of_day_bucket(now)

        async def _run(c: asyncpg.Connection) -> list[SubstrateSnapshot]:
            models = await self._top_models(c, tenant_id, now)
            commits = await self._active_commitments(c, tenant_id, now)
            resources = await self._unhealthy_customer_resources(c, tenant_id)
            changes = await self._recent_state_changes(c, tenant_id, now)
            anomalies = await self._recent_anomalies(c, tenant_id, now)

            candidates: list[dict[str, Any]] = await self._card_candidates(
                c, tenant_id, card_kind, now,
                models=models,
                commitments=commits,
                anomalies=anomalies,
            )
            out: list[SubstrateSnapshot] = []
            for cand in candidates[: self.TOP_CARDS]:
                snap_models = _pin(models, cand, "model")
                snap_commits = _pin(commits, cand, "commitment")
                snap_resources = _pin(resources, cand, "resource")
                snap_anomalies = _pin(anomalies, cand, "anomaly")
                out.append(
                    SubstrateSnapshot(
                        tenant_id=tenant_id,
                        captured_at=now,
                        top_models=snap_models,
                        active_commitments=snap_commits,
                        customer_resources=snap_resources,
                        recent_state_changes=changes,
                        anomalies=snap_anomalies,
                        conversation_context=ConversationContext(
                            recent_queries=[{"card_candidate": cand}],
                        ),
                        time_of_day_bucket=bucket,
                    )
                )
            return out

        if conn is not None:
            return await _run(conn)
        async with self._pool.acquire() as owned:
            return await _run(owned)

    # -----------------------------------------------------------------
    # Query grid
    # -----------------------------------------------------------------
    async def compose_query_grid_snapshot(
        self,
        tenant_id: UUID,
        *,
        now: datetime | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> QueryGridSnapshot:
        now = _normalise_now(now)
        bucket = _time_of_day_bucket(now)

        async def _run(c: asyncpg.Connection) -> QueryGridSnapshot:
            situation = await self._situation_queries(c, tenant_id, now)
            evergreen = _EVERGREEN_QUERIES[: self.EVERGREEN_QUERY_COUNT]
            return QueryGridSnapshot(
                tenant_id=tenant_id,
                captured_at=now,
                situation_queries=situation[: self.SITUATION_QUERY_COUNT],
                evergreen_queries=list(evergreen),
                time_of_day_bucket=bucket,
            )

        if conn is not None:
            return await _run(conn)
        async with self._pool.acquire() as owned:
            return await _run(owned)

    # =================================================================
    # Private — individual substrate reads
    # =================================================================

    async def _top_models(
        self,
        c: asyncpg.Connection,
        tenant_id: UUID,
        now: datetime,
    ) -> list[ModelRef]:
        """High-confidence recent Models that changed state in 24h.

        Strategy: pick active Models whose `last_retrieved_at` is within
        24h OR whose `created_at` is within 24h, and whose confidence is
        >= MIN_CONFIDENCE_FOR_TOP. Falls back to top by confidence
        recency if no fresh rows exist (so a quiet day still renders).
        """
        cutoff = now - timedelta(hours=self.RECENT_WINDOW_HOURS)
        rows = await c.fetch(
            """
            SELECT id, "natural" AS natural, confidence,
                   confidence_at_assertion, proposition_kind, status,
                   created_at, last_retrieved_at,
                   scope_actors, scope_entities
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND confidence >= $2
              AND (
                created_at >= $3
                OR COALESCE(last_retrieved_at, created_at) >= $3
              )
            ORDER BY confidence DESC,
                     COALESCE(last_retrieved_at, created_at) DESC
            LIMIT $4
            """,
            tenant_id,
            self.MIN_CONFIDENCE_FOR_TOP,
            cutoff,
            self.TOP_MODELS,
        )
        if not rows:
            # Fallback: anything active, highest confidence, last 7d.
            rows = await c.fetch(
                """
                SELECT id, "natural" AS natural, confidence,
                       confidence_at_assertion, proposition_kind, status,
                       created_at, last_retrieved_at,
                       scope_actors, scope_entities
                FROM models
                WHERE tenant_id = $1
                  AND status = 'active'
                ORDER BY confidence DESC, created_at DESC
                LIMIT $2
                """,
                tenant_id,
                self.TOP_MODELS,
            )
        out: list[ModelRef] = []
        for r in rows:
            scope_entities = _coerce_jsonb(r["scope_entities"]) or []
            out.append(
                ModelRef(
                    id=r["id"],
                    natural=r["natural"],
                    confidence=float(r["confidence"]),
                    confidence_at_assertion=float(r["confidence_at_assertion"]),
                    proposition_kind=r["proposition_kind"],
                    status=r["status"],
                    last_state_change_at=r["last_retrieved_at"] or r["created_at"],
                    scope_actors=list(r["scope_actors"] or []),
                    scope_entities=scope_entities if isinstance(scope_entities, list) else [],
                )
            )
        return out

    async def _active_commitments(
        self,
        c: asyncpg.Connection,
        tenant_id: UUID,
        now: datetime,
    ) -> list[CommitmentRef]:
        """Active / blocked / paused commitments with deadline pressure
        in the next `COMMITMENT_DEADLINE_WINDOW_DAYS`. Critical-path
        edges bubble to the top.
        """
        horizon = now + timedelta(days=self.COMMITMENT_DEADLINE_WINDOW_DAYS)
        rows = await c.fetch(
            """
            SELECT com.id, com.title, com.state, com.owner_id, com.due_date,
                   com.priority, com.last_state_change_at,
                   EXISTS (
                     SELECT 1 FROM contributes_to ct
                     WHERE ct.commitment_id = com.id
                       AND ct.is_critical_path = TRUE
                   ) AS is_critical_path
            FROM commitments com
            WHERE com.tenant_id = $1
              AND com.state IN ('active', 'blocked', 'paused', 'proposed')
              AND (com.due_date IS NULL OR com.due_date <= $2)
            ORDER BY is_critical_path DESC,
                     (com.state = 'blocked') DESC,
                     com.priority ASC,
                     com.due_date ASC NULLS LAST,
                     com.last_state_change_at DESC
            LIMIT 20
            """,
            tenant_id,
            horizon,
        )
        out: list[CommitmentRef] = []
        for r in rows:
            due = r["due_date"]
            days_to_due: int | None = None
            if due is not None:
                delta = due - now
                days_to_due = int(delta.total_seconds() // 86400)
            out.append(
                CommitmentRef(
                    id=r["id"],
                    title=r["title"],
                    state=r["state"],
                    owner_id=r["owner_id"],
                    due_date=due,
                    priority=int(r["priority"] or 5),
                    is_critical_path=bool(r["is_critical_path"]),
                    days_to_due=days_to_due,
                    last_state_change_at=r["last_state_change_at"],
                )
            )
        return out

    async def _unhealthy_customer_resources(
        self,
        c: asyncpg.Connection,
        tenant_id: UUID,
    ) -> list[ResourceRef]:
        """Customer Resources first (`kind='relational'` in dogfood) —
        surface any that are NOT 'available'. Health lives in
        `current_value->>'health'` when set; otherwise we treat the
        utilization_state as the health signal.
        """
        rows = await c.fetch(
            """
            SELECT r.id, r.kind, r.identity, r.utilization_state,
                   r.current_value, r.last_updated_at,
                   cc.revenue_at_risk_usd
            FROM resources r
            LEFT JOIN customer_commitments cc
              ON cc.customer_resource_id = r.id
            WHERE r.tenant_id = $1
              AND r.archived_at IS NULL
              AND (
                r.kind = 'relational'
                OR r.utilization_state IN ('depleted', 'expired', 'committed')
              )
            ORDER BY
              CASE r.utilization_state
                WHEN 'depleted' THEN 0
                WHEN 'expired' THEN 1
                WHEN 'committed' THEN 2
                ELSE 3
              END,
              r.last_updated_at DESC
            LIMIT 20
            """,
            tenant_id,
        )
        out: list[ResourceRef] = []
        seen: set[UUID] = set()
        for r in rows:
            if r["id"] in seen:
                continue
            seen.add(r["id"])
            cv = _coerce_jsonb(r["current_value"]) or {}
            health = None
            if isinstance(cv, dict):
                hv = cv.get("health")
                if isinstance(hv, str):
                    health = hv
            rev = r["revenue_at_risk_usd"]
            rev_f = float(rev) if rev is not None else None
            out.append(
                ResourceRef(
                    id=r["id"],
                    kind=r["kind"],
                    identity=r["identity"],
                    utilization_state=r["utilization_state"],
                    health=health,
                    last_updated_at=r["last_updated_at"],
                    revenue_at_risk_usd=rev_f,
                )
            )
        return out

    async def _recent_state_changes(
        self,
        c: asyncpg.Connection,
        tenant_id: UUID,
        now: datetime,
    ) -> list[StateChange]:
        """Recent state_change Observations — these drive the 'something
        changed' sense in the greeting."""
        cutoff = now - timedelta(hours=self.RECENT_WINDOW_HOURS)
        rows = await c.fetch(
            """
            SELECT id, content, occurred_at
            FROM observations
            WHERE tenant_id = $1
              AND kind = 'state_change'
              AND occurred_at >= $2
            ORDER BY occurred_at DESC
            LIMIT 50
            """,
            tenant_id,
            cutoff,
        )
        out: list[StateChange] = []
        for r in rows:
            content = _coerce_jsonb(r["content"]) or {}
            if not isinstance(content, dict):
                continue
            entity_id_raw = content.get("entity_id")
            if entity_id_raw is None:
                continue
            try:
                entity_id = UUID(str(entity_id_raw))
            except (ValueError, TypeError):
                continue
            out.append(
                StateChange(
                    observation_id=r["id"],
                    entity_id=entity_id,
                    entity_kind=content.get("entity_kind"),
                    kind=str(content.get("kind") or ""),
                    occurred_at=r["occurred_at"],
                    metadata=content.get("metadata") or {},
                )
            )
        return out

    async def _recent_anomalies(
        self,
        c: asyncpg.Connection,
        tenant_id: UUID,
        now: datetime,
    ) -> list[AnomalyRef]:
        cutoff = now - timedelta(hours=self.RECENT_WINDOW_HOURS)
        rows = await c.fetch(
            """
            SELECT id, kind, region, significance, published_at
            FROM think_anomalies_raw
            WHERE tenant_id = $1
              AND published_at >= $2
            ORDER BY significance DESC, published_at DESC
            LIMIT $3
            """,
            tenant_id,
            cutoff,
            self.TOP_ANOMALIES,
        )
        out: list[AnomalyRef] = []
        for r in rows:
            region = _coerce_jsonb(r["region"]) or {}
            out.append(
                AnomalyRef(
                    id=r["id"],
                    kind=r["kind"],
                    region=region if isinstance(region, dict) else {},
                    significance=float(r["significance"]),
                    published_at=r["published_at"],
                )
            )
        return out

    # -----------------------------------------------------------------
    # Card candidate selection
    # -----------------------------------------------------------------
    async def _card_candidates(
        self,
        c: asyncpg.Connection,
        tenant_id: UUID,
        card_kind: str,
        now: datetime,
        *,
        models: list[ModelRef],
        commitments: list[CommitmentRef],
        anomalies: list[AnomalyRef],
    ) -> list[dict[str, Any]]:
        """Pick the top-3 candidates for `card_kind`. Each candidate
        is a small dict carrying the entity id + kind hint so the
        caller can pin it in the snapshot.
        """
        if card_kind == "observation":
            # Anomalies are the most direct observation seed.
            out = [
                {
                    "kind": "anomaly",
                    "id": str(a.id),
                    "subject_kind": a.kind,
                    "significance": a.significance,
                }
                for a in anomalies
            ]
            # Back-fill with high-confidence Models that recently changed,
            # with a lightweight token-overlap diversity filter so the UI
            # doesn't stack three cards on the same subject (Week 7-8
            # CONCERN fix — an Acme-heavy Think output used to fill all
            # three observation slots with near-duplicate cards).
            selected_keys: set[str] = set()
            for m in models:
                if len(out) >= self.TOP_CARDS:
                    break
                tokens = _topic_tokens(m.natural)
                if _shares_majority_with_any(tokens, selected_keys):
                    continue
                selected_keys.update(tokens)
                out.append(
                    {
                        "kind": "model",
                        "id": str(m.id),
                        "natural": m.natural,
                        "confidence": m.confidence,
                    }
                )
            return out[: self.TOP_CARDS]

        if card_kind == "decision":
            # Blocked / critical-path Commitments are decision candidates.
            ranked = sorted(
                commitments,
                key=lambda com: (
                    0 if com.is_critical_path else 1,
                    0 if com.state == "blocked" else 1,
                    com.priority,
                    com.days_to_due if com.days_to_due is not None else 99,
                ),
            )
            return [
                {
                    "kind": "commitment",
                    "id": str(com.id),
                    "state": com.state,
                    "days_to_due": com.days_to_due,
                    "is_critical_path": com.is_critical_path,
                }
                for com in ranked[: self.TOP_CARDS]
            ]

        if card_kind == "question":
            # Question candidates: Models with large post-calibration
            # confidence drift (contested/unresolved).
            scored: list[tuple[float, ModelRef]] = []
            for m in models:
                drift = abs(m.confidence_at_assertion - m.confidence)
                if drift > 0.1:
                    scored.append((drift, m))
            scored.sort(key=lambda t: t[0], reverse=True)
            out = [
                {
                    "kind": "model",
                    "id": str(m.id),
                    "natural": m.natural,
                    "drift": d,
                }
                for d, m in scored[: self.TOP_CARDS]
            ]
            # Back-fill with anomalies to guarantee TOP_CARDS candidates.
            for a in anomalies:
                if len(out) >= self.TOP_CARDS:
                    break
                out.append(
                    {
                        "kind": "anomaly",
                        "id": str(a.id),
                        "subject_kind": a.kind,
                    }
                )
            return out[: self.TOP_CARDS]

        return []

    # -----------------------------------------------------------------
    # Situation-tied queries
    # -----------------------------------------------------------------
    async def _situation_queries(
        self,
        c: asyncpg.Connection,
        tenant_id: UUID,
        now: datetime,
    ) -> list[dict[str, Any]]:
        """Two queries tied to today's hot situations. Built from the
        top anomaly + the blocked-critical-path commitment (if any).

        These are the chips the design doc calls 'hot' — their tag is
        'urgent' or 'relevant' depending on significance.
        """
        cutoff = now - timedelta(hours=self.RECENT_WINDOW_HOURS)
        out: list[dict[str, Any]] = []

        # Hot anomaly → "why X happened"
        row = await c.fetchrow(
            """
            SELECT id, kind, region, significance
            FROM think_anomalies_raw
            WHERE tenant_id = $1 AND published_at >= $2
            ORDER BY significance DESC
            LIMIT 1
            """,
            tenant_id,
            cutoff,
        )
        if row is not None:
            region = _coerce_jsonb(row["region"]) or {}
            subject = ""
            if isinstance(region, dict):
                subject = (
                    region.get("model_id")
                    or region.get("commitment_id")
                    or region.get("resource_id")
                    or ""
                )
            tag = "urgent" if float(row["significance"]) >= 0.7 else "relevant"
            out.append(
                {
                    "id": f"situation:anomaly:{row['id']}",
                    "icon": "why",
                    "label": f"why {row['kind']}",
                    "tag": tag,
                    "hot": True,
                    "query_template": (
                        f"Why was an anomaly of kind '{row['kind']}' "
                        f"flagged for {subject or 'the substrate'}?"
                    ),
                }
            )

        # Blocked critical-path commitment → "what's holding X"
        c_row = await c.fetchrow(
            """
            SELECT com.id, com.title
            FROM commitments com
            WHERE com.tenant_id = $1
              AND com.state = 'blocked'
              AND EXISTS (
                SELECT 1 FROM contributes_to ct
                WHERE ct.commitment_id = com.id
                  AND ct.is_critical_path = TRUE
              )
            ORDER BY com.last_state_change_at DESC
            LIMIT 1
            """,
            tenant_id,
        )
        if c_row is not None:
            out.append(
                {
                    "id": f"situation:blocked:{c_row['id']}",
                    "icon": "dependency",
                    "label": f"what's holding {_short_title(c_row['title'])}",
                    "tag": "urgent",
                    "hot": True,
                    "query_template": (
                        f"What is blocking commitment '{c_row['title']}'?"
                    ),
                }
            )

        return out


# =====================================================================
# helpers
# =====================================================================


def _time_of_day_bucket(ts: datetime) -> TimeOfDayBucket:
    """Bucket a UTC timestamp into the five tenant-local morning /
    afternoon / evening / late windows used by scheduler triggers and
    rendering prompts. Dogfood tenant is Asia/Kathmandu (UTC+05:45);
    callers can pass `now` already local-converted if that matters.
    The time-of-day logic uses the hour-of-day of the passed timestamp
    as-is — the scheduler is responsible for picking the right
    reference clock.
    """
    hour = ts.hour
    if 5 <= hour < 9:
        return "early_morning"
    if 9 <= hour < 12:
        return "morning"
    if 12 <= hour < 17:
        return "afternoon"
    if 17 <= hour < 22:
        return "evening"
    return "late"


def _normalise_now(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _coerce_jsonb(value: Any) -> Any:
    """asyncpg may return JSONB as str or already-parsed dict/list."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return value


def _serialise(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _serialise(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialise(v) for v in obj]
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.astimezone(timezone.utc).isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return obj


# Stopword list for the topic-token diversity filter. Keep it small
# and conservative so real subjects (people / customers / products)
# survive. Week 7-8 CONCERN fix — observation-card diversity.
_TOPIC_STOPWORDS: frozenset[str] = frozenset({
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
    "has", "have", "in", "is", "it", "its", "of", "on", "or", "our",
    "that", "the", "this", "to", "was", "were", "will", "with",
    "about", "been", "being", "but", "if", "into", "no", "not", "so",
    "such", "their", "them", "they", "we", "what", "when", "which",
    "who", "you", "your", "still", "just", "all", "any", "some", "more",
})


def _topic_tokens(text: str) -> set[str]:
    """Lightweight subject-token extraction for the observation-card
    diversity filter. Lowercases, strips punctuation, drops tokens in
    `_TOPIC_STOPWORDS` and tokens shorter than 4 chars.

    Not a retrieval tool — just "is this card about the same subject
    as another card I've already selected?"
    """
    tokens: set[str] = set()
    for raw in re.split(r"[^A-Za-z0-9']+", text or ""):
        tok = raw.lower().strip()
        if len(tok) < 4 or tok in _TOPIC_STOPWORDS:
            continue
        tokens.add(tok)
    return tokens


def _shares_majority_with_any(
    tokens: set[str],
    selected: set[str],
    *,
    min_overlap: int = 2,
) -> bool:
    """True when `tokens` share >= `min_overlap` content-tokens with
    `selected`. Used to suppress observation-card duplicates.

    We use an absolute overlap count rather than a Jaccard-style ratio
    because Think-produced Model propositions vary in length (short
    "Acme renewal at risk" vs long "Acme renewal confidence dropped
    after two contracted deliverables slipped"), and a ratio either
    punishes long captures or lets a short near-duplicate slip through.
    Two shared content-tokens is a decent cue for "same subject" on the
    simulation corpus while still preserving independent topics.
    """
    if not tokens:
        return False
    overlap = len(tokens & selected)
    return overlap >= min_overlap


def _pin(
    items: list,
    candidate: dict[str, Any],
    target_kind: str,
) -> list:
    """If `candidate.kind == target_kind`, move the matching entry to
    the head of `items`. Otherwise return items unchanged. Used to
    hint the rendering layer which entity the card is centred on.
    """
    if candidate.get("kind") != target_kind:
        return list(items)
    try:
        cand_id = UUID(str(candidate.get("id")))
    except (ValueError, TypeError):
        return list(items)
    head = [x for x in items if getattr(x, "id", None) == cand_id]
    tail = [x for x in items if getattr(x, "id", None) != cand_id]
    return head + tail


def _short_title(title: str, limit: int = 36) -> str:
    s = title.strip()
    return s if len(s) <= limit else s[: limit - 1] + "…"


__all__ = [
    "ModelRef",
    "CommitmentRef",
    "ResourceRef",
    "StateChange",
    "AnomalyRef",
    "ConversationContext",
    "FounderContext",
    "SubstrateSnapshot",
    "QueryGridSnapshot",
    "SnapshotComposer",
    "TimeOfDayBucket",
]
