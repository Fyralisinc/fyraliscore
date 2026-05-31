"""services/sage/evidence_projection.py — Phase 8 Node-to-Evidence Projection.

Implements Stage F ("Node-to-Evidence Projection") of the SAGE-inspired
self-evolution architecture. See:

  * fyralis-sage-synthesis-self-evolution.md §7.7 (output shape, impact)
  * fyralis-sage-synthesis-self-evolution.md Phase 8 (projection policy +
    acceptance criteria)

Goal
----
Convert a list of selected `models` rows (the activated Nodes for the
current question) into a small, ranked set of `EvidenceCandidate`s that
the synthesis prompt can actually afford. The projector preserves both
support AND counterevidence, demotes redundant items to summary-only,
and applies a hard token budget so prompt assembly is bounded.

Design notes
------------
* v1 is deterministic, no LLM. The ranking heuristics live in this
  file; nothing in the SQL is fancier than `SELECT ... WHERE id = ANY`.
* All DB I/O is performed via `pool` or an optional caller-supplied
  `conn`. Same convention as `services/sage/inquiry_traces/repo.py`.
* The output dataclasses are frozen/slotted so they can be passed
  across boundaries and re-hashed for cache keys.
* The `signal_readings` JSONB array is treated as a list of
  `{"kind": "...", "weight": float, "event_id": uuid, ...}` rows. Only
  rows that name an `event_id` can become evidence; everything else is
  used only to explain confidence movement.
* The model table column used as the human-readable one-line summary
  is `content_text` (see `db/migrations/0001_foundation.sql` S1.1).
  The Phase 8 spec calls this the "embedding_text" / one-line summary;
  the projector stores it on the candidate via the
  `EvidenceCandidate.token_estimate` calculation but does NOT inline it
  on the returned dataclass — call sites that need the actual text re-
  fetch by id (this keeps the projection result cheaply serializable
  and lets the caller pick the right rendering for include_level).
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import UUID

import asyncpg
import structlog


_log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------

IncludeLevel = Literal[
    "raw_excerpt", "evidence_card", "summary_only", "ref_only",
]

EvidenceReason = Literal[
    "decisive_support",
    "decisive_counterevidence",
    "freshest_confirmation",
    "falsification_relevant",
    "explains_confidence",
    "minimal_provenance",
    "redundant_summarized",
]


# Token estimates per include level. These are deliberately coarse;
# the synthesis writer applies the real tokenizer downstream. Kept as a
# module-level dict so tests can pin the exact numeric budget math.
TOKEN_ESTIMATE: dict[str, int] = {
    "raw_excerpt": 400,
    "evidence_card": 120,
    "summary_only": 40,
    "ref_only": 8,
}


# Question primitives that warrant raw-excerpt promotion for
# counterevidence. The list is intentionally small — every primitive
# here implies the user needs to see the literal text, not a summary.
_RAW_EXCERPT_QUESTION_PRIMITIVES: frozenset[str] = frozenset(
    {"CONTRADICTION", "FALSIFICATION"},
)


@dataclass(frozen=True, slots=True)
class EvidenceCandidate:
    """Single projected piece of evidence for one source Node/Model.

    `evidence_id` may be an observation id or a model id (supporting
    Model). `evidence_kind` disambiguates. `score` is the ranking
    score this projector assigned in [0, 1] — higher means more
    decisive — and is used for both ordering and tie-breaks when the
    token budget forces demotion.
    """

    node_id: UUID
    evidence_id: UUID
    evidence_kind: Literal["observation", "model"]
    reason: EvidenceReason
    include_level: IncludeLevel
    token_estimate: int
    trust_tier: str | None
    occurred_at: datetime | None
    score: float


@dataclass(frozen=True, slots=True)
class ProjectionBudget:
    """Caps applied during projection.

    `fresh_window_days` defines what counts as a "fresh" observation
    for the freshness_share metric and for the freshest_confirmation
    ranking step.
    """

    max_evidence_per_node: int = 5
    max_raw_excerpts_total: int = 8
    max_total_tokens: int = 24_000
    fresh_window_days: int = 30


@dataclass(frozen=True, slots=True)
class ProjectionResult:
    """The outcome of `EvidenceProjector.project`.

    `omitted` lists `(evidence_id, reason)` pairs that the projector
    saw but dropped — useful for the omitted_evidence trace table.
    `coverage` carries three diagnostic shares used by the
    self-evolution loop to detect biased projections.
    """

    projected: tuple[EvidenceCandidate, ...]
    omitted: tuple[tuple[UUID, str], ...]
    coverage: dict[str, float]


# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------


def _coerce_jsonb_obj(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _coerce_jsonb_list(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [v for v in parsed if isinstance(v, dict)]
    return []


def _coerce_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        try:
            return UUID(value)
        except (ValueError, TypeError):
            return None
    return None


# Trust-tier ranking. Higher is "more authoritative". Unknown tiers map
# to 0.0 so they cannot dominate a real tier in ranking. Tiers below
# follow the convention used by the ingestion + retrieval stack.
_TIER_RANK: dict[str, float] = {
    "authoritative": 1.0,
    "validated": 0.85,
    "high": 0.85,
    "verified": 0.8,
    "trusted": 0.75,
    "medium": 0.5,
    "default": 0.4,
    "low": 0.25,
    "unverified": 0.1,
    "anecdotal": 0.05,
}


def _tier_score(tier: str | None) -> float:
    if not tier:
        return 0.0
    return _TIER_RANK.get(tier.lower(), 0.3)


# ---------------------------------------------------------------------
# Internal working types (per-model, pre-budget)
# ---------------------------------------------------------------------


@dataclass(slots=True)
class _ObsRow:
    id: UUID
    occurred_at: datetime
    trust_tier: str | None
    kind: str | None
    source_channel: str | None
    content_text: str | None


@dataclass(slots=True)
class _ModelRow:
    id: UUID
    confidence: float
    falsifier: dict[str, Any]
    signal_readings: list[dict[str, Any]]
    supporting_event_ids: list[UUID]
    supporting_model_ids: list[UUID]
    proposition: dict[str, Any]


@dataclass(slots=True)
class _Pick:
    """Mutable candidate prior to budget enforcement."""

    node_id: UUID
    evidence_id: UUID
    evidence_kind: Literal["observation", "model"]
    reason: EvidenceReason
    include_level: IncludeLevel
    trust_tier: str | None
    occurred_at: datetime | None
    score: float
    counterevidence: bool = False
    fresh: bool = False
    falsification_relevant: bool = False


# ---------------------------------------------------------------------
# Projector
# ---------------------------------------------------------------------


class EvidenceProjector:
    """Project a set of selected Models into ranked evidence candidates.

    Construction is dependency-free; `project()` accepts the asyncpg
    pool (or a borrowed `conn`) at call time so this projector can be
    instantiated once per process and shared across requests.
    """

    def __init__(self, *, budget: ProjectionBudget | None = None) -> None:
        self._budget = budget or ProjectionBudget()

    # ----- public API -------------------------------------------------

    async def project(
        self,
        *,
        pool: asyncpg.Pool | None,
        tenant_id: UUID,
        selected_model_ids: list[UUID],
        question_primitive: str,
        conn: asyncpg.Connection | None = None,
    ) -> ProjectionResult:
        if not selected_model_ids:
            return ProjectionResult(
                projected=(),
                omitted=(),
                coverage={
                    "counterevidence_share": 0.0,
                    "freshness_share": 0.0,
                    "falsification_share": 0.0,
                },
            )

        if conn is None:
            if pool is None:
                raise ValueError(
                    "EvidenceProjector.project requires pool= or conn="
                )
            async with pool.acquire() as acquired:
                return await self._project_with_conn(
                    acquired,
                    tenant_id=tenant_id,
                    selected_model_ids=selected_model_ids,
                    question_primitive=question_primitive,
                )
        return await self._project_with_conn(
            conn,
            tenant_id=tenant_id,
            selected_model_ids=selected_model_ids,
            question_primitive=question_primitive,
        )

    # ----- internals --------------------------------------------------

    async def _project_with_conn(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        selected_model_ids: list[UUID],
        question_primitive: str,
    ) -> ProjectionResult:
        models = await self._load_models(
            conn, tenant_id=tenant_id, model_ids=selected_model_ids,
        )
        # Gather every observation id any model references — both via
        # supporting_event_ids AND via signal_readings.event_id — so we
        # can do a single batched fetch.
        all_obs_ids: set[UUID] = set()
        for m in models.values():
            for oid in m.supporting_event_ids:
                all_obs_ids.add(oid)
            for sr in m.signal_readings:
                oid = _coerce_uuid(sr.get("event_id"))
                if oid is not None:
                    all_obs_ids.add(oid)

        obs_map = await self._load_observations(
            conn, tenant_id=tenant_id, observation_ids=sorted(all_obs_ids),
        )

        # Per-model pick lists.
        all_picks: list[_Pick] = []
        omitted: list[tuple[UUID, str]] = []
        # Cap counterevidence per model at 2 per spec.
        for mid in selected_model_ids:
            model = models.get(mid)
            if model is None:
                omitted.append((mid, "model_not_found"))
                continue
            picks, model_omitted = self._rank_for_model(
                model=model,
                obs_map=obs_map,
                question_primitive=question_primitive,
            )
            all_picks.extend(picks)
            omitted.extend(model_omitted)

        # Apply token budget — may demote raw_excerpt / evidence_card
        # entries to ref_only.
        projected, budget_omitted = self._apply_token_budget(
            all_picks, question_primitive=question_primitive,
        )
        omitted.extend(budget_omitted)

        coverage = self._compute_coverage(projected, all_picks)

        candidates = tuple(
            EvidenceCandidate(
                node_id=p.node_id,
                evidence_id=p.evidence_id,
                evidence_kind=p.evidence_kind,
                reason=p.reason,
                include_level=p.include_level,
                token_estimate=TOKEN_ESTIMATE[p.include_level],
                trust_tier=p.trust_tier,
                occurred_at=p.occurred_at,
                score=p.score,
            )
            for p in projected
        )

        _log.info(
            "evidence_projection.complete",
            tenant_id=str(tenant_id),
            question_primitive=question_primitive,
            selected_models=len(selected_model_ids),
            projected_count=len(candidates),
            omitted_count=len(omitted),
            counterevidence_share=coverage["counterevidence_share"],
            freshness_share=coverage["freshness_share"],
            falsification_share=coverage["falsification_share"],
        )

        return ProjectionResult(
            projected=candidates,
            omitted=tuple(omitted),
            coverage=coverage,
        )

    # ----- SQL --------------------------------------------------------

    async def _load_models(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        model_ids: list[UUID],
    ) -> dict[UUID, _ModelRow]:
        rows = await conn.fetch(
            """
            SELECT id,
                   confidence,
                   falsifier,
                   signal_readings,
                   supporting_event_ids,
                   supporting_model_ids,
                   proposition
              FROM models
             WHERE tenant_id = $1
               AND id = ANY($2::uuid[])
            """,
            tenant_id,
            list(model_ids),
        )
        out: dict[UUID, _ModelRow] = {}
        for r in rows:
            out[r["id"]] = _ModelRow(
                id=r["id"],
                confidence=float(r["confidence"]),
                falsifier=_coerce_jsonb_obj(r["falsifier"]),
                signal_readings=_coerce_jsonb_list(r["signal_readings"]),
                supporting_event_ids=list(r["supporting_event_ids"] or []),
                supporting_model_ids=list(r["supporting_model_ids"] or []),
                proposition=_coerce_jsonb_obj(r["proposition"]),
            )
        return out

    async def _load_observations(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        observation_ids: list[UUID],
    ) -> dict[UUID, _ObsRow]:
        if not observation_ids:
            return {}
        rows = await conn.fetch(
            """
            SELECT id,
                   occurred_at,
                   trust_tier,
                   kind,
                   source_channel,
                   content_text
              FROM observations
             WHERE tenant_id = $1
               AND id = ANY($2::uuid[])
            """,
            tenant_id,
            list(observation_ids),
        )
        out: dict[UUID, _ObsRow] = {}
        for r in rows:
            out[r["id"]] = _ObsRow(
                id=r["id"],
                occurred_at=r["occurred_at"],
                trust_tier=r["trust_tier"],
                kind=r["kind"],
                source_channel=r["source_channel"],
                content_text=r["content_text"],
            )
        return out

    # ----- per-model ranking -----------------------------------------

    def _rank_for_model(
        self,
        *,
        model: _ModelRow,
        obs_map: dict[UUID, _ObsRow],
        question_primitive: str,
    ) -> tuple[list[_Pick], list[tuple[UUID, str]]]:
        picks: list[_Pick] = []
        omitted: list[tuple[UUID, str]] = []
        seen_evidence: set[UUID] = set()
        now = datetime.now(timezone.utc)
        fresh_cutoff = now - timedelta(days=self._budget.fresh_window_days)

        # ---- 1. decisive_counterevidence (cap 2 per model) ----------
        counter_picks: list[_Pick] = []
        for sr in model.signal_readings:
            kind = (sr.get("kind") or "").lower()
            weight = sr.get("weight")
            try:
                weight_f = float(weight) if weight is not None else 0.0
            except (TypeError, ValueError):
                weight_f = 0.0
            is_counter = (kind == "contradiction") or (weight_f < 0)
            if not is_counter:
                continue
            oid = _coerce_uuid(sr.get("event_id"))
            if oid is None or oid in seen_evidence:
                continue
            obs = obs_map.get(oid)
            if obs is None:
                omitted.append((oid, "counterevidence_obs_missing"))
                continue
            # Score: stronger magnitude = higher score. Bounded to 1.0.
            magnitude = min(1.0, abs(weight_f) if weight_f else 0.6)
            score = 0.75 + 0.25 * magnitude
            level: IncludeLevel = (
                "raw_excerpt"
                if question_primitive in _RAW_EXCERPT_QUESTION_PRIMITIVES
                else "evidence_card"
            )
            counter_picks.append(
                _Pick(
                    node_id=model.id,
                    evidence_id=oid,
                    evidence_kind="observation",
                    reason="decisive_counterevidence",
                    include_level=level,
                    trust_tier=obs.trust_tier,
                    occurred_at=obs.occurred_at,
                    score=min(1.0, score),
                    counterevidence=True,
                    fresh=obs.occurred_at >= fresh_cutoff,
                )
            )
        counter_picks.sort(key=lambda p: p.score, reverse=True)
        for p in counter_picks[:2]:
            picks.append(p)
            seen_evidence.add(p.evidence_id)
        for p in counter_picks[2:]:
            omitted.append((p.evidence_id, "counterevidence_cap_reached"))

        # ---- 2a. falsification_relevant (BEFORE decisive_support) ---
        # Run the falsifier-pattern match before decisive_support so a
        # supporting obs that matches the falsifier pattern is picked
        # with reason='falsification_relevant', not consumed silently
        # by decisive_support. Otherwise, with N<=2 supporting events,
        # decisive_support would grab everything and falsification_
        # relevant would never fire.
        falsifier_kind = (model.falsifier.get("kind") or "").lower()
        if falsifier_kind in {"observation_pattern", "prediction_deadline"}:
            pattern = (model.falsifier.get("pattern") or "").lower()
            for oid in model.supporting_event_ids:
                if oid in seen_evidence:
                    continue
                obs = obs_map.get(oid)
                if obs is None:
                    continue
                if not pattern:
                    chosen = obs
                else:
                    blob = " ".join(
                        s for s in (
                            obs.kind, obs.source_channel, obs.content_text,
                        ) if s
                    ).lower()
                    if pattern not in blob:
                        continue
                    chosen = obs
                picks.append(
                    _Pick(
                        node_id=model.id,
                        evidence_id=chosen.id,
                        evidence_kind="observation",
                        reason="falsification_relevant",
                        include_level="evidence_card",
                        trust_tier=chosen.trust_tier,
                        occurred_at=chosen.occurred_at,
                        score=0.7,
                        fresh=chosen.occurred_at >= fresh_cutoff,
                        falsification_relevant=True,
                    )
                )
                seen_evidence.add(chosen.id)
                break  # one falsification pick per model

        # ---- 2b. decisive_support (top-2 recent + highest tier) -----
        support_candidates: list[_Pick] = []
        for oid in model.supporting_event_ids:
            if oid in seen_evidence:
                continue
            obs = obs_map.get(oid)
            if obs is None:
                omitted.append((oid, "supporting_obs_missing"))
                continue
            tier = _tier_score(obs.trust_tier)
            age_days = max(
                0.0, (now - obs.occurred_at).total_seconds() / 86400.0
            )
            recency = 1.0 / (1.0 + age_days / 14.0)  # half-life ~14d
            score = 0.5 + 0.3 * tier + 0.2 * recency
            level = (
                "evidence_card"
                if question_primitive in _RAW_EXCERPT_QUESTION_PRIMITIVES
                or question_primitive == "DEPENDENCY"
                else "summary_only"
            )
            support_candidates.append(
                _Pick(
                    node_id=model.id,
                    evidence_id=oid,
                    evidence_kind="observation",
                    reason="decisive_support",
                    include_level=level,
                    trust_tier=obs.trust_tier,
                    occurred_at=obs.occurred_at,
                    score=min(1.0, score),
                    fresh=obs.occurred_at >= fresh_cutoff,
                )
            )
        # Sort: tier first, then recency.
        support_candidates.sort(
            key=lambda p: (
                _tier_score(p.trust_tier),
                p.occurred_at or datetime.min.replace(tzinfo=timezone.utc),
            ),
            reverse=True,
        )
        for p in support_candidates[:2]:
            picks.append(p)
            seen_evidence.add(p.evidence_id)

        # ---- 3. freshest_confirmation -------------------------------
        # Pick the single freshest supporting observation that we have
        # NOT already selected.
        freshest: _ObsRow | None = None
        for oid in model.supporting_event_ids:
            if oid in seen_evidence:
                continue
            obs = obs_map.get(oid)
            if obs is None:
                continue
            if freshest is None or obs.occurred_at > freshest.occurred_at:
                freshest = obs
        if freshest is not None:
            picks.append(
                _Pick(
                    node_id=model.id,
                    evidence_id=freshest.id,
                    evidence_kind="observation",
                    reason="freshest_confirmation",
                    include_level="summary_only",
                    trust_tier=freshest.trust_tier,
                    occurred_at=freshest.occurred_at,
                    score=0.55,
                    fresh=freshest.occurred_at >= fresh_cutoff,
                )
            )
            seen_evidence.add(freshest.id)

        # ---- 4. falsification_relevant ------------------------------
        # Moved to stage 2a above (runs before decisive_support so the
        # falsifier-matching observation isn't silently consumed under
        # reason='decisive_support').

        # ---- 5. explains_confidence --------------------------------
        # Observation referenced by the signal_reading with the largest
        # |weight| (already-seen ids are skipped).
        best_sr: dict[str, Any] | None = None
        best_mag = 0.0
        for sr in model.signal_readings:
            try:
                w = abs(float(sr.get("weight") or 0.0))
            except (TypeError, ValueError):
                w = 0.0
            if w <= best_mag:
                continue
            oid = _coerce_uuid(sr.get("event_id"))
            if oid is None or oid in seen_evidence:
                continue
            if oid not in obs_map:
                continue
            best_mag = w
            best_sr = sr
        if best_sr is not None:
            oid = _coerce_uuid(best_sr.get("event_id"))
            assert oid is not None
            obs = obs_map[oid]
            picks.append(
                _Pick(
                    node_id=model.id,
                    evidence_id=oid,
                    evidence_kind="observation",
                    reason="explains_confidence",
                    include_level="summary_only",
                    trust_tier=obs.trust_tier,
                    occurred_at=obs.occurred_at,
                    score=min(1.0, 0.4 + 0.4 * best_mag),
                    fresh=obs.occurred_at >= fresh_cutoff,
                )
            )
            seen_evidence.add(oid)

        # ---- per-node cap ------------------------------------------
        cap = self._budget.max_evidence_per_node
        if len(picks) > cap:
            # Keep all counterevidence (per acceptance: always at least
            # one piece of counterevidence when any exists), then fill
            # remaining slots by score.
            picks.sort(
                key=lambda p: (p.counterevidence, p.score),
                reverse=True,
            )
            kept = picks[:cap]
            dropped = picks[cap:]
            for p in dropped:
                omitted.append((p.evidence_id, "per_node_cap"))
            picks = kept

        return picks, omitted

    # ----- token budget enforcement ----------------------------------

    def _apply_token_budget(
        self,
        picks: list[_Pick],
        *,
        question_primitive: str,
    ) -> tuple[list[_Pick], list[tuple[UUID, str]]]:
        """Demote include_level to stay under the token budget.

        Algorithm:
          1. Sort all picks descending by (counterevidence, score).
          2. Walk the list, accumulating token cost; if a pick would
             push us over budget, demote it one step
             (raw_excerpt -> evidence_card -> summary_only -> ref_only).
          3. Also enforce `max_raw_excerpts_total`.
          4. We always project AT LEAST one counterevidence pick if any
             counterevidence exists — even if that forces other picks
             to demote.
        """
        omitted: list[tuple[UUID, str]] = []
        if not picks:
            return [], omitted

        # Pre-sort: counterevidence first (so they get budget first),
        # then by score desc, then by recency.
        picks_sorted = sorted(
            picks,
            key=lambda p: (
                p.counterevidence,
                p.score,
                p.occurred_at or datetime.min.replace(tzinfo=timezone.utc),
            ),
            reverse=True,
        )

        # Enforce max_raw_excerpts_total up front.
        raw_seen = 0
        for p in picks_sorted:
            if p.include_level == "raw_excerpt":
                raw_seen += 1
                if raw_seen > self._budget.max_raw_excerpts_total:
                    p.include_level = "evidence_card"

        # Enforce token budget by demoting.
        demote_chain: dict[IncludeLevel, IncludeLevel | None] = {
            "raw_excerpt": "evidence_card",
            "evidence_card": "summary_only",
            "summary_only": "ref_only",
            "ref_only": None,
        }

        running = 0
        final: list[_Pick] = []
        for p in picks_sorted:
            level = p.include_level
            cost = TOKEN_ESTIMATE[level]
            # Demote as long as we'd blow the budget and a demotion is
            # available.
            while (
                running + cost > self._budget.max_total_tokens
                and demote_chain[level] is not None
            ):
                level = demote_chain[level]  # type: ignore[assignment]
                cost = TOKEN_ESTIMATE[level]
            if running + cost > self._budget.max_total_tokens:
                # Even ref_only would overflow — drop the pick entirely.
                omitted.append((p.evidence_id, "token_budget_overflow"))
                continue
            p.include_level = level
            running += cost
            final.append(p)

        # Final guard: if any counterevidence exists in the original
        # input but none survives in `final`, force the highest-scoring
        # counterevidence back in at ref_only.
        had_counter = any(p.counterevidence for p in picks)
        has_counter_final = any(p.counterevidence for p in final)
        if had_counter and not has_counter_final:
            best_counter = max(
                (p for p in picks if p.counterevidence),
                key=lambda p: p.score,
            )
            best_counter.include_level = "ref_only"
            final.append(best_counter)
            # Remove from omitted if present.
            omitted = [
                e for e in omitted if e[0] != best_counter.evidence_id
            ]

        return final, omitted

    # ----- coverage --------------------------------------------------

    def _compute_coverage(
        self,
        projected: list[_Pick],
        all_picks: list[_Pick],
    ) -> dict[str, float]:
        total = len(projected)
        if total == 0:
            return {
                "counterevidence_share": 0.0,
                "freshness_share": 0.0,
                "falsification_share": 0.0,
            }
        counter = sum(1 for p in projected if p.counterevidence)
        fresh = sum(1 for p in projected if p.fresh)
        falsif = sum(
            1 for p in projected if p.falsification_relevant
        )
        return {
            "counterevidence_share": counter / total,
            "freshness_share": fresh / total,
            "falsification_share": falsif / total,
        }


__all__ = [
    "EvidenceCandidate",
    "EvidenceProjector",
    "EvidenceReason",
    "IncludeLevel",
    "ProjectionBudget",
    "ProjectionResult",
    "TOKEN_ESTIMATE",
]
