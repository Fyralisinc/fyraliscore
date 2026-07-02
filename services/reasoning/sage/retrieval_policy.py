"""Adaptive SAGE retrieval policy primitives.

SAGE's retrieval policy is a small, deterministic controller over the
retrieval layer. It does not retrieve rows itself and it does not write
canonical truth. It compresses signal shape into an auditable plan:
which retrieval paths should run, which should be cheap probes, which can
be skipped, and why.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Literal, Sequence
from uuid import UUID

from services.reasoning.sage.company_profile.types import CompanyLearningProfile


PrimaryPathway = Literal["projection_context", "A", "B", "L", "C", "D", "G"]
InquiryActionPath = Literal[
    "structural",
    "focused_index",
    "semantic_terms",
    "semantic",
    "temporal",
    "pattern",
    "model_edge",
    "sage_reader",
]
PolicyMode = Literal["required", "preferred", "probe", "skip"]

_PRIMARY_PATHWAYS: frozenset[str] = frozenset(
    {"projection_context", "A", "B", "L", "C", "D", "G"}
)
_VAGUE_TERMS: frozenset[str] = frozenset(
    {
        "thing",
        "stuff",
        "issue",
        "problem",
        "something",
        "maybe",
        "seems",
        "looks",
        "unclear",
        "unknown",
    }
)
_COUNTEREVIDENCE_PRIMITIVES: frozenset[str] = frozenset(
    {"COUNTEREVIDENCE", "CONTRADICTION", "FALSIFICATION"}
)
_STRUCTURAL_FIRST_PRIMITIVES: frozenset[str] = frozenset(
    {"OWNERSHIP", "COMMITMENT", "CONSTRAINT", "DEPENDENCY"}
)
_EXPANSIVE_DISCOVERY_ACTIONS: frozenset[str] = frozenset({"semantic", "pattern"})


@dataclass(frozen=True, slots=True)
class SageSignalSignature:
    """Compact, auditable representation of a retrieval situation."""

    signal_type: str
    subkind: str | None = None
    question_primitive: str | None = None
    entity_count: int = 0
    actor_count: int = 0
    explicit_model_count: int = 0
    has_text: bool = False
    has_vector: bool = False
    has_temporal_anchor: bool = False
    has_projection_opportunity: bool = False
    lexical_specificity: float = 0.0
    vague_language: bool = False


@dataclass(frozen=True, slots=True)
class SagePathwayDecision:
    """One policy decision for a retriever or inquiry action path."""

    path: str
    mode: PolicyMode
    stage: int = 1
    budget: int | None = None
    weight_multiplier: float = 1.0
    reason: str = ""
    exploration: bool = False

    @property
    def allowed(self) -> bool:
        return self.mode != "skip"


@dataclass(frozen=True, slots=True)
class SageRouteOutcome:
    """One observed retrieval route outcome before compression."""

    path: str
    admitted: bool = True
    skipped: bool = False
    elapsed_ms: int = 0
    returned_models: int = 0
    returned_observations: int = 0
    selected_evidence: int = 0
    budget: int = 0
    quality_credit: float = 0.0
    cost_units: float = 0.0


@dataclass(frozen=True, slots=True)
class SageRouteUtility:
    """Compressed SAGE utility memory for one route under one signal shape."""

    signature_hash: str
    path: str
    signal_type: str = ""
    subkind: str | None = None
    question_primitive: str | None = None
    attempts: int = 0
    wins: int = 0
    skips: int = 0
    returned_models: int = 0
    returned_observations: int = 0
    selected_evidence: int = 0
    elapsed_ms_total: int = 0
    latency_ms_p95: float = 0.0
    budget_total: int = 0
    total_cost: float = 0.0
    total_quality_credit: float = 0.0
    utility_score: float = 0.0
    confidence: float = 0.0
    match_score: float = 1.0

    @property
    def avg_latency_ms(self) -> float:
        return float(self.elapsed_ms_total) / max(1, int(self.attempts))

    @property
    def avg_budget(self) -> float:
        return float(self.budget_total) / max(1, int(self.attempts))

    @property
    def win_rate(self) -> float:
        return float(self.wins) / max(1, int(self.attempts))

    @property
    def selected_rate(self) -> float:
        return float(self.selected_evidence) / max(1, int(self.attempts))

    def notes(self) -> dict[str, Any]:
        return {
            "signature_hash": self.signature_hash,
            "path": self.path,
            "signal_type": self.signal_type,
            "subkind": self.subkind,
            "question_primitive": self.question_primitive,
            "attempts": self.attempts,
            "wins": self.wins,
            "skips": self.skips,
            "returned_models": self.returned_models,
            "returned_observations": self.returned_observations,
            "selected_evidence": self.selected_evidence,
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "latency_ms_p95": round(float(self.latency_ms_p95), 2),
            "avg_budget": round(self.avg_budget, 2),
            "utility_score": round(float(self.utility_score), 4),
            "confidence": round(float(self.confidence), 4),
            "match_score": round(float(self.match_score), 4),
        }


@dataclass(frozen=True, slots=True)
class SageRetrievalStage:
    """A parallel execution stage in the retrieval DAG."""

    stage: int
    paths: tuple[str, ...]
    parallel: bool = True
    deadline_ms: int | None = None
    run_if: str = "always"
    cancel_if_sufficient: bool = False


@dataclass(frozen=True, slots=True)
class SageRetrievalPolicy:
    """SAGE's non-canonical plan for steering retrieval."""

    signature: SageSignalSignature
    decisions: tuple[SagePathwayDecision, ...]
    stages: tuple[SageRetrievalStage, ...]
    confidence: float
    reasons: tuple[str, ...] = ()
    shadow: bool = False
    exploration_rate: float = 0.0
    profile_effects: tuple[dict[str, Any], ...] = ()

    def decision_for(self, path: str) -> SagePathwayDecision | None:
        for decision in self.decisions:
            if decision.path == path:
                return decision
        return None

    def allows(self, path: str) -> bool:
        decision = self.decision_for(path)
        return decision is None or decision.allowed or self.shadow

    def budget_for(self, path: str, default: int) -> int:
        decision = self.decision_for(path)
        if decision is None or decision.budget is None:
            return default
        return max(1, int(decision.budget))

    def apply_primary_weights(self, weights: dict[str, float]) -> dict[str, float]:
        """Return trigger weights after policy skip/probe decisions."""

        adjusted: dict[str, float] = {}
        for path, weight in weights.items():
            decision = self.decision_for(path)
            if decision is not None and decision.mode == "skip" and not self.shadow:
                continue
            multiplier = decision.weight_multiplier if decision is not None else 1.0
            value = max(0.0, float(weight) * float(multiplier))
            if value > 0:
                adjusted[path] = value
        total = sum(adjusted.values())
        if total <= 0:
            return dict(weights)
        return {path: value / total for path, value in adjusted.items()}

    def notes(self) -> dict[str, Any]:
        return {
            "enabled": True,
            "shadow": self.shadow,
            "confidence": round(float(self.confidence), 4),
            "exploration_rate": round(float(self.exploration_rate), 4),
            "signature": asdict(self.signature),
            "reasons": list(self.reasons),
            "decisions": [asdict(decision) for decision in self.decisions],
            "stages": [asdict(stage) for stage in self.stages],
            "profile_effects": [dict(effect) for effect in self.profile_effects],
        }


@dataclass(frozen=True, slots=True)
class SageRetrievalObservation:
    """Post-run telemetry SAGE can learn from later."""

    policy: SageRetrievalPolicy
    paths_run: tuple[str, ...]
    paths_skipped: tuple[str, ...]
    models: int = 0
    observations: int = 0
    elapsed_ms_by_stage: dict[str, int] = field(default_factory=dict)
    route_utilities: tuple[SageRouteUtility, ...] = ()

    def notes(self) -> dict[str, Any]:
        return {
            "paths_run": list(self.paths_run),
            "paths_skipped": list(self.paths_skipped),
            "models": self.models,
            "observations": self.observations,
            "elapsed_ms_by_stage": dict(self.elapsed_ms_by_stage),
            "route_utilities": [utility.notes() for utility in self.route_utilities],
        }


def build_signal_signature(
    *,
    trigger: Any,
    effective_seed_entities: list[dict[str, Any]] | None = None,
    effective_scope_actors: list[UUID] | None = None,
    question_primitive: str | None = None,
    projection_enabled: bool = True,
) -> SageSignalSignature:
    """Build the shared policy signature from trigger and derived scope."""

    text = str(getattr(trigger, "seed_natural_text", None) or "")
    model_ids = []
    model_id = getattr(trigger, "model_id", None)
    if model_id is not None:
        model_ids.append(model_id)
    for member_id in getattr(trigger, "member_model_ids", ()) or ():
        if member_id not in model_ids:
            model_ids.append(member_id)
    entities = effective_seed_entities if effective_seed_entities is not None else (
        getattr(trigger, "seed_entity_ids", None) or []
    )
    actors = effective_scope_actors if effective_scope_actors is not None else (
        getattr(trigger, "scope_actors", None) or []
    )
    specificity = _lexical_specificity(text, getattr(trigger, "seed_signature", None))
    return SageSignalSignature(
        signal_type=str(getattr(trigger, "kind", "") or ""),
        subkind=getattr(trigger, "subkind", None),
        question_primitive=(
            str(question_primitive).upper() if question_primitive else None
        ),
        entity_count=len(entities),
        actor_count=len(actors),
        explicit_model_count=len(model_ids),
        has_text=bool(text.strip()),
        has_vector=getattr(trigger, "precomputed_seed_vector", None) is not None,
        has_temporal_anchor=getattr(trigger, "seed_occurred_at", None) is not None,
        has_projection_opportunity=bool(projection_enabled and (entities or model_ids)),
        lexical_specificity=specificity,
        vague_language=_has_vague_language(text),
    )


def plan_primary_retrieval(
    *,
    trigger: Any,
    weights: dict[str, float],
    effective_seed_entities: list[dict[str, Any]],
    effective_scope_actors: list[UUID],
    projection_enabled: bool,
    semantic_terms_enabled: bool,
    semantic_k: int,
    shadow: bool = False,
    exploration_rate: float = 0.0,
    route_utilities: Sequence[SageRouteUtility] | None = None,
    company_profile: CompanyLearningProfile | None = None,
) -> SageRetrievalPolicy:
    """Plan the low-level primary retrievers for a trigger.

    This v1 planner is deliberately deterministic and conservative. It only
    downshifts dense semantic retrieval when cheaper, more explainable signals
    are likely to dominate, while preserving a tiny deterministic exploration
    probe so SAGE can discover drift.
    """

    signature = build_signal_signature(
        trigger=trigger,
        effective_seed_entities=effective_seed_entities,
        effective_scope_actors=effective_scope_actors,
        projection_enabled=projection_enabled,
    )
    decisions: list[SagePathwayDecision] = []
    reasons: list[str] = []
    b_decision: SagePathwayDecision | None = None

    if projection_enabled:
        mode: PolicyMode = (
            "preferred" if signature.has_projection_opportunity else "probe"
        )
        decisions.append(
            SagePathwayDecision(
                "projection_context",
                mode,
                stage=1,
                reason=(
                    "fresh compact state should be checked first"
                    if signature.has_projection_opportunity
                    else "cheap projection probe"
                ),
            )
        )

    for path in ("L", "A", "G", "C", "D"):
        if path not in weights:
            continue
        stage = 1 if path in {"L", "A", "G"} else 2
        mode: PolicyMode = "preferred"
        if path in {"A", "G"} and signature.explicit_model_count:
            mode = "required"
        if path == "L" and not semantic_terms_enabled:
            mode = "probe"
        if path == "C" and not signature.has_temporal_anchor:
            mode = "probe"
        decisions.append(
            SagePathwayDecision(
                path,
                mode,
                stage=stage,
                reason=_primary_reason(path, signature),
            )
        )

    if "B" in weights:
        b_decision = _plan_dense_semantic(
            signature=signature,
            semantic_k=semantic_k,
            semantic_terms_enabled=semantic_terms_enabled,
            exploration_rate=exploration_rate,
        )
        decisions.append(b_decision)
        if b_decision.mode == "skip":
            reasons.append(b_decision.reason)

    if signature.explicit_model_count:
        reasons.append("explicit_model_anchor_prefers_graph_and_sparse_context")
    if signature.vague_language:
        if b_decision is not None and b_decision.mode == "skip":
            reasons.append("vague_signal_with_cheap_anchors_skips_dense_semantic")
        else:
            reasons.append("vague_language_keeps_dense_semantic_available")
    if signature.lexical_specificity >= 0.58:
        reasons.append("specific_terms_raise_sparse_retrieval_priority")

    decisions, route_reasons = _apply_route_utilities_to_decisions(
        signature=signature,
        decisions=decisions,
        route_utilities=route_utilities or (),
        shadow=shadow,
        exploration_rate=exploration_rate,
    )
    reasons.extend(route_reasons)
    decisions, profile_reasons, profile_effects = _apply_company_profile_to_decisions(
        signature=signature,
        decisions=decisions,
        company_profile=company_profile,
        shadow=shadow,
        actor_refs=effective_scope_actors,
        source_keys=_source_keys_from_trigger(trigger),
    )
    reasons.extend(profile_reasons)
    stage_one, stage_two = _stage_paths_for_decisions(decisions, shadow=shadow)
    stages = _build_stages(stage_one, stage_two)
    confidence = _policy_confidence(signature, decisions)
    return SageRetrievalPolicy(
        signature=signature,
        decisions=tuple(decisions),
        stages=stages,
        confidence=confidence,
        reasons=tuple(dict.fromkeys(reasons)),
        shadow=shadow,
        exploration_rate=exploration_rate,
        profile_effects=tuple(profile_effects),
    )


def adapt_inquiry_actions(
    *,
    question_primitive: str,
    actions: list[Any],
    signal_type: str | None = None,
    subkind: str | None = None,
    route_utilities: Sequence[SageRouteUtility] | None = None,
    company_profile: CompanyLearningProfile | None = None,
    shadow: bool = False,
    semantic_budget_floor: int = 8,
) -> tuple[list[Any], list[dict[str, Any]]]:
    """Annotate and lightly adapt inquiry actions using SAGE policy.

    The action dataclass lives in platform execution, so this helper treats
    actions duck-typed and returns replacement instances by calling
    ``type(action)(...)``. It keeps counterevidence/falsification retrieval
    broad, but downshifts semantic actions when structural/focused evidence
    should lead.
    """

    primitive = (question_primitive or "").upper()
    signature = SageSignalSignature(
        signal_type=str(signal_type or ""),
        subkind=subkind,
        question_primitive=primitive or None,
    )
    if not actions:
        return [], []
    has_structural_first = any(
        str(getattr(action, "path", ""))
        in {"focused_index", "structural", "model_edge"}
        for action in actions
    )
    adapted: list[Any] = []
    notes: list[dict[str, Any]] = []
    for action in actions:
        path = str(getattr(action, "path", ""))
        filters = dict(getattr(action, "filters", {}) or {})
        budget = int(getattr(action, "budget", 25) or 25)
        stage = 1
        mode: PolicyMode = "preferred"
        reason = "default_inquiry_action"
        if path in {"focused_index", "structural"}:
            mode = "required" if primitive in _STRUCTURAL_FIRST_PRIMITIVES else "preferred"
            reason = "cheap_structural_or_focused_probe"
        elif path == "model_edge":
            stage = 1 if primitive in _STRUCTURAL_FIRST_PRIMITIVES else 2
            reason = "typed_model_graph_for_question"
        elif path == "semantic_terms":
            stage = 1
            reason = "cheap_semantic_terms_before_dense_fallback"
        elif path == "temporal":
            stage = 1 if primitive in _COUNTEREVIDENCE_PRIMITIVES else 2
            reason = "freshness_or_counterevidence_check"
        elif path in _EXPANSIVE_DISCOVERY_ACTIONS:
            stage = 2
            reason = "broad_discovery_after_cheap_context"
            if (
                path == "semantic"
                and primitive in _STRUCTURAL_FIRST_PRIMITIVES
                and has_structural_first
            ):
                mode = "probe"
                reduced = max(semantic_budget_floor, int(round(budget * 0.6)))
                if not shadow:
                    budget = reduced
                reason = "semantic_probe_after_structural_first_actions"
        utility = _best_route_utility(signature, route_utilities or (), path)
        utility_note: dict[str, Any] | None = None
        if utility is not None:
            utility_note = _route_utility_note(utility)
            mode, stage, budget, reason = _apply_route_utility_to_action(
                path=path,
                mode=mode,
                stage=stage,
                budget=budget,
                reason=reason,
                utility=utility,
                shadow=shadow,
                semantic_budget_floor=semantic_budget_floor,
            )
        profile_effect = _profile_effect_for_path(
            company_profile,
            path,
            signal_type=signature.signal_type,
            question_primitive=signature.question_primitive,
        )
        if profile_effect is not None:
            mode, stage, budget, reason = _apply_profile_effect_to_action(
                path=path,
                mode=mode,
                stage=stage,
                budget=budget,
                reason=reason,
                profile_effect=profile_effect,
                shadow=shadow,
                semantic_budget_floor=semantic_budget_floor,
            )
        filters.setdefault("_sage_policy_stage", stage)
        filters["_sage_policy_mode"] = mode
        filters["_sage_policy_reason"] = reason
        if utility_note is not None:
            filters["_sage_route_utility_score"] = utility_note["utility_score"]
            filters["_sage_route_utility_confidence"] = utility_note["confidence"]
            filters["_sage_route_utility_match"] = utility_note["match_score"]
            if mode == "skip" and not shadow:
                filters["_sage_route_utility_skip"] = True
        adapted.append(
            type(action)(
                getattr(action, "question_id"),
                getattr(action, "path"),
                getattr(action, "target"),
                query=getattr(action, "query", None),
                filters=filters,
                budget=budget,
            )
        )
        notes.append(
            {
                "path": path,
                "target": getattr(action, "target", None),
                "mode": mode,
                "stage": stage,
                "budget": budget,
                "reason": reason,
                **({"route_utility": utility_note} if utility_note else {}),
                **({"company_profile": profile_effect} if profile_effect else {}),
            }
        )
    return adapted, notes


def summarize_primary_observation(
    *,
    policy: SageRetrievalPolicy,
    notes: dict[str, Any],
    models: int,
    observations: int,
) -> SageRetrievalObservation:
    timings = {
        str(item.get("stage")): int(item.get("elapsed_ms") or 0)
        for item in notes.get("pathway_timings", []) or []
        if isinstance(item, dict) and item.get("stage")
    }
    skipped = []
    for item in notes.get("pathways_skipped", []) or []:
        if isinstance(item, dict) and item.get("pathway"):
            skipped.append(str(item["pathway"]))
    for decision in policy.decisions:
        if decision.mode == "skip" and decision.path in _PRIMARY_PATHWAYS:
            skipped.append(decision.path)
    outcomes = primary_route_outcomes_from_notes(
        policy=policy,
        notes=notes,
        total_models=models,
        total_observations=observations,
    )
    route_utilities = route_utilities_from_outcomes(policy.signature, outcomes)
    return SageRetrievalObservation(
        policy=policy,
        paths_run=tuple(str(path) for path in notes.get("pathways_run", []) or []),
        paths_skipped=tuple(dict.fromkeys(skipped)),
        models=models,
        observations=observations,
        elapsed_ms_by_stage=timings,
        route_utilities=route_utilities,
    )


def signature_hash(signature: SageSignalSignature) -> str:
    """Stable hash key for route utility memory."""

    payload = {
        "signal_type": signature.signal_type,
        "subkind": signature.subkind,
        "question_primitive": signature.question_primitive,
        "entity_count_bucket": _small_count_bucket(signature.entity_count),
        "actor_count_bucket": _small_count_bucket(signature.actor_count),
        "explicit_model_count_bucket": _small_count_bucket(
            signature.explicit_model_count
        ),
        "has_text": signature.has_text,
        "has_vector": signature.has_vector,
        "has_temporal_anchor": signature.has_temporal_anchor,
        "has_projection_opportunity": signature.has_projection_opportunity,
        "lexical_specificity_bucket": round(
            min(1.0, max(0.0, signature.lexical_specificity)) * 10
        )
        / 10,
        "vague_language": signature.vague_language,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:24]


def route_utilities_from_outcomes(
    signature: SageSignalSignature,
    outcomes: Iterable[SageRouteOutcome],
) -> tuple[SageRouteUtility, ...]:
    """Compress route outcomes into one utility record per path."""

    grouped: dict[str, list[SageRouteOutcome]] = {}
    for outcome in outcomes:
        if not outcome.path:
            continue
        grouped.setdefault(str(outcome.path), []).append(outcome)
    out: list[SageRouteUtility] = []
    sig_hash = signature_hash(signature)
    for path, bucket in grouped.items():
        attempts = sum(1 for item in bucket if item.admitted and not item.skipped)
        skips = sum(1 for item in bucket if item.skipped or not item.admitted)
        wins = sum(1 for item in bucket if _route_outcome_is_win(item))
        elapsed_values = sorted(
            max(0, int(item.elapsed_ms))
            for item in bucket
            if item.admitted and not item.skipped
        )
        elapsed_total = sum(elapsed_values)
        returned_models = sum(max(0, int(item.returned_models)) for item in bucket)
        returned_observations = sum(
            max(0, int(item.returned_observations)) for item in bucket
        )
        selected_evidence = sum(max(0, int(item.selected_evidence)) for item in bucket)
        budget_total = sum(max(0, int(item.budget)) for item in bucket)
        cost_total = sum(max(0.0, float(item.cost_units)) for item in bucket)
        quality_credit = sum(float(item.quality_credit) for item in bucket)
        latency_p95 = _percentile(elapsed_values, 0.95)
        utility_score = _score_route_utility(
            attempts=attempts,
            wins=wins,
            skips=skips,
            returned_models=returned_models,
            returned_observations=returned_observations,
            selected_evidence=selected_evidence,
            elapsed_ms_total=elapsed_total,
            latency_ms_p95=latency_p95,
            budget_total=budget_total,
            total_cost=cost_total,
            total_quality_credit=quality_credit,
        )
        confidence = _route_utility_confidence(attempts=attempts, wins=wins)
        out.append(
            SageRouteUtility(
                signature_hash=sig_hash,
                path=path,
                signal_type=signature.signal_type,
                subkind=signature.subkind,
                question_primitive=signature.question_primitive,
                attempts=attempts,
                wins=wins,
                skips=skips,
                returned_models=returned_models,
                returned_observations=returned_observations,
                selected_evidence=selected_evidence,
                elapsed_ms_total=elapsed_total,
                latency_ms_p95=latency_p95,
                budget_total=budget_total,
                total_cost=cost_total,
                total_quality_credit=quality_credit,
                utility_score=utility_score,
                confidence=confidence,
                match_score=1.0,
            )
        )
    return tuple(sorted(out, key=lambda item: (-item.utility_score, item.path)))


def primary_route_outcomes_from_notes(
    *,
    policy: SageRetrievalPolicy,
    notes: dict[str, Any],
    total_models: int,
    total_observations: int,
) -> tuple[SageRouteOutcome, ...]:
    """Build lightweight primary-route outcomes from pathway telemetry."""

    outcomes: list[SageRouteOutcome] = []
    timing_by_path: dict[str, dict[str, Any]] = {}
    for item in notes.get("pathway_timings", []) or []:
        if not isinstance(item, dict):
            continue
        raw_stage = str(item.get("stage") or "")
        path = _primary_path_from_stage(raw_stage)
        if path:
            timing_by_path[path] = item
    skipped_paths = set()
    for item in notes.get("pathways_skipped", []) or []:
        if isinstance(item, dict) and item.get("pathway"):
            skipped_paths.add(str(item["pathway"]))
    for decision in policy.decisions:
        if decision.path not in _PRIMARY_PATHWAYS:
            continue
        timing = timing_by_path.get(decision.path, {})
        returned_models = _safe_int(timing.get("models"))
        returned_observations = _safe_int(timing.get("observations"))
        skipped = (
            decision.mode == "skip"
            or decision.path in skipped_paths
            or bool(timing.get("skipped"))
        )
        if decision.path == "projection_context" and not timing:
            skipped = skipped or decision.mode == "skip"
        quality_credit = 0.0
        if not skipped and (returned_models or returned_observations):
            quality_credit = min(2.0, (returned_models + returned_observations) / 24.0)
        outcomes.append(
            SageRouteOutcome(
                path=decision.path,
                admitted=not skipped,
                skipped=skipped,
                elapsed_ms=_safe_int(timing.get("elapsed_ms")),
                returned_models=returned_models,
                returned_observations=returned_observations,
                selected_evidence=0,
                budget=int(decision.budget or 0),
                quality_credit=quality_credit,
                cost_units=_primary_route_cost_units(
                    decision.path,
                    elapsed_ms=_safe_int(timing.get("elapsed_ms")),
                    budget=int(decision.budget or 0),
                ),
            )
        )
    if total_models == 0 and total_observations == 0:
        outcomes = [
            SageRouteOutcome(
                path=outcome.path,
                admitted=outcome.admitted,
                skipped=outcome.skipped,
                elapsed_ms=outcome.elapsed_ms,
                returned_models=outcome.returned_models,
                returned_observations=outcome.returned_observations,
                selected_evidence=outcome.selected_evidence,
                budget=outcome.budget,
                quality_credit=min(0.0, outcome.quality_credit),
                cost_units=outcome.cost_units + 0.05,
            )
            for outcome in outcomes
        ]
    return tuple(outcomes)


def _route_outcome_is_win(outcome: SageRouteOutcome) -> bool:
    if float(outcome.quality_credit) < 0:
        return False
    if float(outcome.quality_credit) > 0:
        return True
    return outcome.selected_evidence > 0


def _plan_dense_semantic(
    *,
    signature: SageSignalSignature,
    semantic_k: int,
    semantic_terms_enabled: bool,
    exploration_rate: float,
) -> SagePathwayDecision:
    exploration = _deterministic_exploration(signature, exploration_rate)
    cheap_anchors_available = (
        signature.explicit_model_count > 0
        or signature.entity_count > 0
        or signature.actor_count > 0
        or signature.has_projection_opportunity
    )
    if not signature.has_text and not signature.has_vector:
        return SagePathwayDecision(
            "B",
            "preferred",
            stage=2,
            budget=semantic_k,
            reason="legacy semantic path retained; pathway may self-skip without seed",
        )
    if signature.vague_language or signature.lexical_specificity < 0.36:
        if semantic_terms_enabled and cheap_anchors_available:
            return SagePathwayDecision(
                "B",
                "probe",
                stage=2,
                budget=max(4, min(semantic_k, 6 if not exploration else 4)),
                reason=(
                    "cheap_anchors_bound_dense_semantic_for_vague_signal"
                    if not exploration
                    else "cheap_anchors_allow_tiny_dense_semantic_exploration_probe"
                ),
                exploration=exploration,
            )
        if semantic_terms_enabled:
            return SagePathwayDecision(
                "B",
                "probe",
                stage=2,
                budget=max(4, min(semantic_k, 10)),
                weight_multiplier=0.35,
                reason="dense semantic fallback for vague or low-term signal",
                exploration=exploration,
            )
        return SagePathwayDecision(
            "B",
            "preferred",
            stage=1,
            budget=semantic_k,
            reason="dense semantic useful for vague or low-specificity signals",
        )
    semantic_terms_or_graph_strong = semantic_terms_enabled and (
        signature.explicit_model_count > 0
        or signature.entity_count > 0
        or signature.lexical_specificity >= 0.36
    )
    if semantic_terms_or_graph_strong:
        return SagePathwayDecision(
            "B",
            "probe",
            stage=2,
            budget=max(4, min(semantic_k, 6)),
            reason=(
                "semantic_terms_primary_dense_semantic_bounded_probe"
                if not exploration
                else "semantic_terms_primary_dense_semantic_exploration_probe"
            ),
            exploration=exploration,
        )
    return SagePathwayDecision(
        "B",
        "probe" if semantic_terms_enabled else "preferred",
        stage=2,
        budget=max(4, min(semantic_k, 10)) if semantic_terms_enabled else semantic_k,
        weight_multiplier=0.35 if semantic_terms_enabled else 1.0,
        reason=(
            "dense semantic fallback for uncovered signal"
            if semantic_terms_enabled
            else "semantic expansion retained for uncovered signal"
        ),
    )


def _build_stages(
    stage_one: list[str],
    stage_two: list[str],
) -> tuple[SageRetrievalStage, ...]:
    stages: list[SageRetrievalStage] = []
    if stage_one:
        stages.append(
            SageRetrievalStage(
                1,
                tuple(dict.fromkeys(stage_one)),
                deadline_ms=250,
                run_if="always",
            )
        )
    if stage_two:
        stages.append(
            SageRetrievalStage(
                2,
                tuple(dict.fromkeys(stage_two)),
                deadline_ms=700,
                run_if="coverage_gap_or_low_confidence",
                cancel_if_sufficient=True,
            )
        )
    return tuple(stages)


def _stage_paths_for_decisions(
    decisions: Sequence[SagePathwayDecision],
    *,
    shadow: bool,
) -> tuple[list[str], list[str]]:
    stage_one: list[str] = []
    stage_two: list[str] = []
    for decision in decisions:
        if not decision.allowed and not shadow:
            continue
        if decision.stage <= 1:
            stage_one.append(decision.path)
        else:
            stage_two.append(decision.path)
    return stage_one, stage_two


def _apply_route_utilities_to_decisions(
    *,
    signature: SageSignalSignature,
    decisions: list[SagePathwayDecision],
    route_utilities: Sequence[SageRouteUtility],
    shadow: bool,
    exploration_rate: float,
) -> tuple[list[SagePathwayDecision], list[str]]:
    reasons: list[str] = []
    if not route_utilities:
        return decisions, reasons
    adjusted: list[SagePathwayDecision] = []
    for decision in decisions:
        utility = _best_route_utility(signature, route_utilities, decision.path)
        if utility is None:
            adjusted.append(decision)
            continue
        new_decision, reason = _apply_route_utility_to_primary_decision(
            decision,
            utility=utility,
            shadow=shadow,
            exploration_rate=exploration_rate,
        )
        adjusted.append(new_decision)
        if reason:
            reasons.append(reason)
    return adjusted, list(dict.fromkeys(reasons))


def _apply_company_profile_to_decisions(
    *,
    signature: SageSignalSignature,
    decisions: list[SagePathwayDecision],
    company_profile: CompanyLearningProfile | None,
    shadow: bool,
    actor_refs: Sequence[UUID] = (),
    source_keys: Sequence[str] = (),
) -> tuple[list[SagePathwayDecision], list[str], list[dict[str, Any]]]:
    if company_profile is None:
        return decisions, [], []
    adjusted: list[SagePathwayDecision] = []
    reasons: list[str] = []
    effects: list[dict[str, Any]] = []
    latent_pattern_effect = _latent_pattern_profile_effect(company_profile)
    salience_effect = _source_actor_salience_profile_effect(
        company_profile,
        actor_refs=actor_refs,
        source_keys=source_keys,
    )
    for decision in decisions:
        effect = _profile_effect_for_path(
            company_profile,
            decision.path,
            signal_type=signature.signal_type,
            question_primitive=signature.question_primitive,
        )
        if effect is None and decision.path == "D":
            effect = latent_pattern_effect
        if effect is None and salience_effect is not None:
            effect = salience_effect
        if effect is None:
            adjusted.append(decision)
            continue
        new_decision, reason = _apply_profile_effect_to_primary_decision(
            decision,
            profile_effect=effect,
            shadow=shadow,
        )
        adjusted.append(new_decision)
        effects.append(effect)
        if reason:
            reasons.append(reason)
    return adjusted, list(dict.fromkeys(reasons)), effects


def _profile_effect_for_path(
    company_profile: CompanyLearningProfile | None,
    path: str,
    *,
    signal_type: str | None = None,
    question_primitive: str | None = None,
) -> dict[str, Any] | None:
    if company_profile is None:
        return None
    prior = company_profile.best_prior(kind="route", key=path)
    if prior is not None and prior.confidence >= 0.20:
        return {
            "kind": prior.kind,
            "key": prior.key,
            "score": round(float(prior.effective_score), 4),
            "confidence": round(float(prior.confidence), 4),
            "sample_count": int(prior.sample_count),
            "source": prior.metadata.get("source", "company_profile"),
            "canonical_write": False,
            "authority_effect": "none",
        }
    negative = _negative_memory_profile_effect_for_path(
        company_profile,
        path,
        signal_type=signal_type,
        question_primitive=question_primitive,
    )
    if negative is not None:
        return negative
    return None


def _negative_memory_profile_effect_for_path(
    company_profile: CompanyLearningProfile,
    path: str,
    *,
    signal_type: str | None,
    question_primitive: str | None,
) -> dict[str, Any] | None:
    aliases = _path_aliases(path)
    candidates = []
    for prior in company_profile.priors_for_kind("negative_memory"):
        prior_path = str(prior.metadata.get("path") or prior.key or "").strip()
        if prior_path and prior_path not in aliases:
            continue
        prior_signal = str(prior.metadata.get("signal_type") or "").strip()
        if prior_signal and signal_type and prior_signal != signal_type:
            continue
        prior_primitive = str(prior.metadata.get("question_primitive") or "").upper()
        if prior_primitive and question_primitive and prior_primitive != question_primitive:
            continue
        if prior.confidence >= 0.20:
            candidates.append(prior)
    if not candidates:
        return None
    prior = min(candidates, key=lambda item: item.effective_score)
    return {
        "kind": prior.kind,
        "key": prior.key,
        "score": round(float(prior.effective_score), 4),
        "confidence": round(float(prior.confidence), 4),
        "sample_count": int(prior.sample_count),
        "path": prior.metadata.get("path") or path,
        "memory_type": prior.metadata.get("memory_type"),
        "source": prior.metadata.get("source", "company_profile"),
        "canonical_write": False,
        "authority_effect": "none",
    }


def _source_actor_salience_profile_effect(
    company_profile: CompanyLearningProfile,
    *,
    actor_refs: Sequence[UUID],
    source_keys: Sequence[str],
) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    source_key_set = {str(key).strip() for key in source_keys if str(key).strip()}
    for prior in company_profile.priors_for_kind("source_reliability"):
        if prior.confidence < 0.20:
            continue
        if source_key_set and prior.key not in source_key_set:
            continue
        if not source_key_set:
            continue
        candidates.append(_salience_effect_from_prior(prior))

    actor_key_set = {str(actor_id) for actor_id in actor_refs}
    for prior in company_profile.priors_for_kind("actor_reliability"):
        if prior.confidence < 0.20:
            continue
        actor_id = str(prior.metadata.get("actor_id") or "").strip()
        if actor_id and actor_id in actor_key_set:
            candidates.append(_salience_effect_from_prior(prior))

    if not candidates:
        return None
    return max(candidates, key=lambda effect: abs(float(effect["score"])))


def _salience_effect_from_prior(prior: Any) -> dict[str, Any]:
    return {
        "kind": prior.kind,
        "key": prior.key,
        "score": round(float(prior.effective_score), 4),
        "confidence": round(float(prior.confidence), 4),
        "sample_count": int(prior.sample_count),
        "source": prior.metadata.get("source", "company_profile"),
        "canonical_write": False,
        "salience_only": True,
        "authority_effect": "none",
    }


def _latent_pattern_profile_effect(
    company_profile: CompanyLearningProfile,
) -> dict[str, Any] | None:
    latent_priors = company_profile.priors_for_kind("latent_pattern")
    if not latent_priors:
        return None
    best = max(latent_priors, key=lambda prior: prior.effective_score)
    if best.effective_score < 0.28 or best.confidence < 0.20:
        return None
    return {
        "kind": "latent_pattern",
        "key": best.key,
        "score": round(float(best.effective_score), 4),
        "confidence": round(float(best.confidence), 4),
        "sample_count": int(best.sample_count),
        "source": best.metadata.get("source", "company_profile"),
        "canonical_write": False,
        "authority_effect": "none",
    }


def _apply_profile_effect_to_primary_decision(
    decision: SagePathwayDecision,
    *,
    profile_effect: dict[str, Any],
    shadow: bool,
) -> tuple[SagePathwayDecision, str | None]:
    score = float(profile_effect.get("score") or 0.0)
    confidence = float(profile_effect.get("confidence") or 0.0)
    if confidence < 0.20:
        return decision, None
    if profile_effect.get("salience_only") is True:
        return _apply_salience_effect_to_primary_decision(
            decision,
            profile_effect=profile_effect,
        )
    reason_suffix = (
        f"company_profile(kind={profile_effect.get('kind')},"
        f"score={score:.3f},confidence={confidence:.3f})"
    )
    if score >= 0.22:
        mode: PolicyMode = "preferred" if decision.mode != "required" else "required"
        stage = min(decision.stage, 1 if score >= 0.52 else decision.stage)
        multiplier = max(
            float(decision.weight_multiplier),
            1.0 + min(0.35, max(0.0, score) * 0.28),
        )
        return (
            SagePathwayDecision(
                path=decision.path,
                mode=mode,
                stage=stage,
                budget=decision.budget,
                weight_multiplier=multiplier,
                reason=f"{decision.reason}; positive_{reason_suffix}",
                exploration=decision.exploration,
            ),
            "positive_company_profile_promoted_path",
        )
    if score <= -0.22 and decision.mode != "required":
        if shadow:
            mode = "probe"
            multiplier = min(float(decision.weight_multiplier), 0.16)
        else:
            mode = "skip" if score <= -0.42 else "probe"
            multiplier = 0.0 if mode == "skip" else 0.25
        return (
            SagePathwayDecision(
                path=decision.path,
                mode=mode,
                stage=max(decision.stage, 2),
                budget=max(1, min(int(decision.budget or 6), 6)),
                weight_multiplier=multiplier,
                reason=f"negative_{reason_suffix}",
                exploration=decision.exploration,
            ),
            "negative_company_profile_suppressed_path",
        )
    return decision, None


def _apply_salience_effect_to_primary_decision(
    decision: SagePathwayDecision,
    *,
    profile_effect: dict[str, Any],
) -> tuple[SagePathwayDecision, str | None]:
    score = float(profile_effect.get("score") or 0.0)
    confidence = float(profile_effect.get("confidence") or 0.0)
    reason_suffix = (
        f"company_profile_salience(kind={profile_effect.get('kind')},"
        f"score={score:.3f},confidence={confidence:.3f},authority=none)"
    )
    if score >= 0.22:
        multiplier = max(
            float(decision.weight_multiplier),
            1.0 + min(0.18, score * 0.16),
        )
        return (
            SagePathwayDecision(
                path=decision.path,
                mode=decision.mode,
                stage=decision.stage,
                budget=decision.budget,
                weight_multiplier=multiplier,
                reason=f"{decision.reason}; positive_{reason_suffix}",
                exploration=decision.exploration,
            ),
            "source_actor_reliability_raised_salience",
        )
    if score <= -0.22:
        multiplier = min(float(decision.weight_multiplier), max(0.45, 1.0 + score * 0.35))
        mode = decision.mode
        if mode == "preferred":
            mode = "probe"
        return (
            SagePathwayDecision(
                path=decision.path,
                mode=mode,
                stage=max(decision.stage, 2 if mode == "probe" else decision.stage),
                budget=decision.budget,
                weight_multiplier=multiplier,
                reason=f"{decision.reason}; negative_{reason_suffix}",
                exploration=decision.exploration,
            ),
            "source_actor_reliability_lowered_salience",
        )
    return decision, None


def _apply_profile_effect_to_action(
    *,
    path: str,
    mode: PolicyMode,
    stage: int,
    budget: int,
    reason: str,
    profile_effect: dict[str, Any],
    shadow: bool,
    semantic_budget_floor: int,
) -> tuple[PolicyMode, int, int, str]:
    score = float(profile_effect.get("score") or 0.0)
    confidence = float(profile_effect.get("confidence") or 0.0)
    if confidence < 0.20:
        return mode, stage, budget, reason
    if profile_effect.get("salience_only") is True:
        reason_suffix = (
            f"company_profile_salience(path={path},score={score:.3f},"
            f"confidence={confidence:.3f},authority=none)"
        )
        if score >= 0.22:
            return mode, stage, budget, f"{reason}; positive_{reason_suffix}"
        if score <= -0.22 and mode != "required":
            return "probe", max(stage, 2), budget, f"{reason}; negative_{reason_suffix}"
        return mode, stage, budget, reason
    reason_suffix = (
        f"company_profile(path={path},score={score:.3f},"
        f"confidence={confidence:.3f})"
    )
    if score >= 0.22:
        if mode != "required":
            mode = "preferred"
        stage = min(stage, 1 if score >= 0.52 else stage)
        return mode, stage, budget, f"{reason}; positive_{reason_suffix}"
    if score <= -0.22 and mode != "required":
        if shadow:
            mode = "probe"
            budget = max(semantic_budget_floor, min(budget, 6))
        else:
            mode = "skip" if score <= -0.42 else "probe"
            budget = max(1, min(budget, 6))
        return mode, max(stage, 2), budget, f"negative_{reason_suffix}"
    return mode, stage, budget, reason


def _apply_route_utility_to_primary_decision(
    decision: SagePathwayDecision,
    *,
    utility: SageRouteUtility,
    shadow: bool,
    exploration_rate: float,
) -> tuple[SagePathwayDecision, str | None]:
    if not _route_utility_is_confident(utility):
        return decision, "route_utility_seen_but_not_confident"
    score = float(utility.utility_score)
    confidence = float(utility.confidence)
    reason_suffix = (
        f"route_utility(path={utility.path},score={score:.3f},"
        f"confidence={confidence:.3f})"
    )
    if score >= 0.55:
        budget = decision.budget
        if utility.avg_budget > 0:
            budget = max(int(budget or 0), int(round(utility.avg_budget)))
        mode: PolicyMode = "preferred"
        if decision.mode == "required":
            mode = "required"
        elif decision.mode == "skip" and score < 0.82:
            mode = "probe"
        multiplier = max(
            0.35 if mode == "probe" else 1.0,
            float(decision.weight_multiplier),
        )
        return (
            SagePathwayDecision(
                path=decision.path,
                mode=mode,
                stage=min(decision.stage, 1 if score >= 0.82 else decision.stage),
                budget=budget,
                weight_multiplier=multiplier,
                reason=f"{decision.reason}; positive_{reason_suffix}",
                exploration=decision.exploration,
            ),
            "positive_route_utility_promoted_path",
        )
    if decision.mode == "required":
        return decision, None
    if score <= -0.30 and not _deterministic_exploration_for_key(
        utility.signature_hash + utility.path,
        exploration_rate,
    ):
        mode: PolicyMode = "skip"
        multiplier = 0.0
        if shadow:
            mode = "probe"
            multiplier = min(float(decision.weight_multiplier), 0.12)
        return (
            SagePathwayDecision(
                path=decision.path,
                mode=mode,
                stage=max(decision.stage, 2),
                budget=max(1, min(int(decision.budget or 6), 6)),
                weight_multiplier=multiplier,
                reason=f"negative_{reason_suffix}",
                exploration=decision.exploration,
            ),
            "negative_route_utility_suppressed_path",
        )
    if score <= 0.08 and decision.mode == "preferred":
        return (
            SagePathwayDecision(
                path=decision.path,
                mode="probe",
                stage=max(decision.stage, 2),
                budget=max(1, min(int(decision.budget or 8), 8)),
                weight_multiplier=min(float(decision.weight_multiplier), 0.35),
                reason=f"{decision.reason}; weak_{reason_suffix}",
                exploration=decision.exploration,
            ),
            "weak_route_utility_downshifted_path",
        )
    return decision, None


def _apply_route_utility_to_action(
    *,
    path: str,
    mode: PolicyMode,
    stage: int,
    budget: int,
    reason: str,
    utility: SageRouteUtility,
    shadow: bool,
    semantic_budget_floor: int,
) -> tuple[PolicyMode, int, int, str]:
    if not _route_utility_is_confident(utility):
        return mode, stage, budget, reason
    score = float(utility.utility_score)
    reason_suffix = (
        f"route_utility(path={path},score={score:.3f},"
        f"confidence={float(utility.confidence):.3f})"
    )
    if score >= 0.55:
        stage = 1 if score >= 0.82 else stage
        if utility.avg_budget > 0:
            budget = max(budget, int(round(utility.avg_budget)))
        if mode != "required":
            mode = "preferred"
        return mode, stage, budget, f"{reason}; positive_{reason_suffix}"
    if mode == "required":
        return mode, stage, budget, reason
    if score <= -0.30:
        if shadow:
            mode = "probe"
            budget = max(semantic_budget_floor, min(budget, 6))
        else:
            mode = "skip"
            budget = max(1, min(budget, 6))
        return mode, max(stage, 2), budget, f"negative_{reason_suffix}"
    if score <= 0.08 and mode == "preferred":
        return (
            "probe",
            max(stage, 2),
            max(semantic_budget_floor, min(budget, 8)),
            f"{reason}; weak_{reason_suffix}",
        )
    return mode, stage, budget, reason


def _best_route_utility(
    signature: SageSignalSignature,
    route_utilities: Sequence[SageRouteUtility],
    path: str,
) -> SageRouteUtility | None:
    best: SageRouteUtility | None = None
    best_score = 0.0
    for utility in route_utilities:
        if utility.path != path:
            continue
        match_score = _route_signature_match_score(signature, utility)
        if match_score < 0.46:
            continue
        rank_score = match_score * max(0.05, float(utility.confidence))
        if best is None or rank_score > best_score:
            best_score = rank_score
            best = SageRouteUtility(
                signature_hash=utility.signature_hash,
                path=utility.path,
                signal_type=utility.signal_type,
                subkind=utility.subkind,
                question_primitive=utility.question_primitive,
                attempts=utility.attempts,
                wins=utility.wins,
                skips=utility.skips,
                returned_models=utility.returned_models,
                returned_observations=utility.returned_observations,
                selected_evidence=utility.selected_evidence,
                elapsed_ms_total=utility.elapsed_ms_total,
                latency_ms_p95=utility.latency_ms_p95,
                budget_total=utility.budget_total,
                total_cost=utility.total_cost,
                total_quality_credit=utility.total_quality_credit,
                utility_score=utility.utility_score,
                confidence=utility.confidence,
                match_score=match_score,
            )
    return best


def _route_signature_match_score(
    signature: SageSignalSignature,
    utility: SageRouteUtility,
) -> float:
    if utility.signal_type and utility.signal_type != signature.signal_type:
        return 0.0
    score = 0.35 if utility.signal_type else 0.12
    if utility.subkind:
        if utility.subkind != signature.subkind:
            return 0.0
        score += 0.12
    else:
        score += 0.06
    if utility.question_primitive:
        if utility.question_primitive != signature.question_primitive:
            return 0.0
        score += 0.25
    elif signature.question_primitive is None:
        score += 0.20
    else:
        score += 0.08
    if utility.signature_hash == signature_hash(signature):
        score += 0.20
    if utility.attempts >= 8:
        score += 0.04
    if utility.selected_evidence > 0 or utility.wins > 0:
        score += 0.04
    return max(0.0, min(1.0, score))


def _route_utility_is_confident(utility: SageRouteUtility) -> bool:
    return int(utility.attempts) >= 3 and float(utility.confidence) >= 0.24


def _route_utility_note(utility: SageRouteUtility) -> dict[str, Any]:
    return {
        "path": utility.path,
        "attempts": int(utility.attempts),
        "wins": int(utility.wins),
        "utility_score": round(float(utility.utility_score), 4),
        "confidence": round(float(utility.confidence), 4),
        "match_score": round(float(utility.match_score), 4),
    }


def _path_aliases(path: str) -> set[str]:
    aliases = {
        "A": {"A", "structural", "focused_index", "scope"},
        "B": {"B", "semantic", "dense_semantic", "hybrid_semantic"},
        "L": {"L", "semantic_terms", "sparse_terms", "lexical"},
        "C": {"C", "temporal", "freshness"},
        "D": {"D", "pattern", "pattern_models", "precipitation"},
        "G": {"G", "model_edge", "graph", "typed_edges"},
        "projection_context": {"projection_context", "projection"},
    }
    return aliases.get(path, {path})


def _source_keys_from_trigger(trigger: Any) -> tuple[str, ...]:
    seed_signature = getattr(trigger, "seed_signature", None)
    keys: set[str] = set()
    if isinstance(seed_signature, dict):
        _collect_source_keys(seed_signature, keys)
    for attr in ("source_channel", "source_kind", "source", "channel"):
        value = getattr(trigger, attr, None)
        if value:
            keys.add(str(value).strip())
    return tuple(sorted(key for key in keys if key))


def _collect_source_keys(value: Any, out: set[str]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            lowered = str(key).lower()
            if lowered in {
                "source",
                "source_key",
                "source_kind",
                "source_channel",
                "channel",
            } and item:
                out.add(str(item).strip())
            elif isinstance(item, (dict, list, tuple)):
                _collect_source_keys(item, out)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _collect_source_keys(item, out)


def _primary_reason(path: str, signature: SageSignalSignature) -> str:
    if path == "A":
        return "structural scope anchors company entities and acts"
    if path == "G":
        return (
            "explicit model anchor makes typed edge traversal high value"
            if signature.explicit_model_count
            else "model graph can bridge related memory"
        )
    if path == "L":
        return "sparse terms preserve aliases, acronyms, and exact company language"
    if path == "C":
        return "temporal anchor allows freshness and counterevidence checks"
    if path == "D":
        return "pattern/background trigger benefits from pattern models"
    return "default primary pathway"


def _policy_confidence(
    signature: SageSignalSignature,
    decisions: list[SagePathwayDecision],
) -> float:
    confidence = 0.36
    if signature.explicit_model_count:
        confidence += 0.18
    if signature.entity_count:
        confidence += 0.10
    if signature.lexical_specificity >= 0.58:
        confidence += 0.14
    if signature.has_projection_opportunity:
        confidence += 0.08
    if signature.vague_language:
        confidence -= 0.10
    if any(decision.exploration for decision in decisions):
        confidence -= 0.04
    return max(0.05, min(0.92, confidence))


def _lexical_specificity(text: str, seed_signature: Any) -> float:
    words = _word_tokens(text)
    if not words:
        return 0.0
    long_or_marked = sum(
        1
        for word in words
        if (
            len(word) >= 7
            or any(ch.isdigit() for ch in word)
            or "_" in word
            or "-" in word
        )
    )
    titleish = len(re.findall(r"\b[A-Z][A-Za-z0-9_-]{2,}\b", text or ""))
    signature_boost = 0.0
    if isinstance(seed_signature, dict) and seed_signature:
        signature_boost = min(0.18, 0.04 * len(seed_signature))
    return max(
        0.0,
        min(
            1.0,
            (long_or_marked / max(1, len(words)))
            + min(0.22, titleish * 0.03)
            + signature_boost,
        ),
    )


def _has_vague_language(text: str) -> bool:
    tokens = {token.lower() for token in _word_tokens(text)}
    return bool(tokens & _VAGUE_TERMS)


def _word_tokens(text: str) -> list[str]:
    return re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]{1,}", text or "")


def _deterministic_exploration(
    signature: SageSignalSignature,
    exploration_rate: float,
) -> bool:
    rate = max(0.0, min(1.0, float(exploration_rate)))
    if rate <= 0:
        return False
    key = "|".join(
        [
            signature.signal_type,
            signature.subkind or "",
            str(signature.question_primitive or ""),
            str(signature.entity_count),
            str(signature.actor_count),
            str(signature.explicit_model_count),
            f"{signature.lexical_specificity:.3f}",
        ]
    )
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    bucket = int(digest[:8], 16) / 0xFFFFFFFF
    return bucket < rate


def _deterministic_exploration_for_key(key: str, exploration_rate: float) -> bool:
    rate = max(0.0, min(1.0, float(exploration_rate)))
    if rate <= 0:
        return False
    digest = hashlib.sha256(str(key).encode("utf-8")).hexdigest()
    bucket = int(digest[:8], 16) / 0xFFFFFFFF
    return bucket < rate


def _small_count_bucket(value: int) -> int:
    count = max(0, int(value or 0))
    if count <= 0:
        return 0
    if count <= 2:
        return count
    if count <= 5:
        return 5
    if count <= 12:
        return 12
    return 32


def _percentile(values: Sequence[int], q: float) -> float:
    if not values:
        return 0.0
    bounded_q = max(0.0, min(1.0, float(q)))
    index = min(len(values) - 1, int(math.ceil(bounded_q * len(values))) - 1)
    return float(values[index])


def _route_utility_confidence(*, attempts: int, wins: int) -> float:
    if attempts <= 0:
        return 0.0
    evidence = math.log1p(max(0, attempts)) / math.log(17)
    balanced = 0.82 if wins > 0 else 0.58
    return max(0.0, min(1.0, evidence * balanced))


def _score_route_utility(
    *,
    attempts: int,
    wins: int,
    skips: int,
    returned_models: int,
    returned_observations: int,
    selected_evidence: int,
    elapsed_ms_total: int,
    latency_ms_p95: float,
    budget_total: int,
    total_cost: float,
    total_quality_credit: float,
) -> float:
    if attempts <= 0:
        return -0.08 * max(1, skips)
    win_rate = wins / max(1, attempts)
    selected_rate = min(1.0, selected_evidence / max(1, attempts))
    result_rate = min(1.0, (returned_models + returned_observations) / max(1, attempts * 8))
    avg_latency = elapsed_ms_total / max(1, attempts)
    latency_penalty = min(0.55, math.log1p(max(avg_latency, latency_ms_p95)) / 14.0)
    budget_penalty = min(0.25, budget_total / max(1, attempts * 160))
    cost_penalty = min(0.30, total_cost / max(1.0, attempts * 2.5))
    skip_penalty = min(0.20, skips / max(1, attempts + skips))
    quality = min(1.0, max(-1.0, total_quality_credit / max(1, attempts)))
    score = (
        0.46 * win_rate
        + 0.36 * selected_rate
        + 0.18 * result_rate
        + 0.24 * quality
        - latency_penalty
        - budget_penalty
        - cost_penalty
        - skip_penalty
    )
    return round(max(-1.0, min(1.5, score)), 4)


def _primary_path_from_stage(stage: str) -> str | None:
    if stage == "projection_context":
        return "projection_context"
    if stage.startswith("pathway_"):
        path = stage.removeprefix("pathway_")
        if path in _PRIMARY_PATHWAYS:
            return path
    return None


def _primary_route_cost_units(path: str, *, elapsed_ms: int, budget: int) -> float:
    latency = max(0, int(elapsed_ms)) / 1000.0
    budget_cost = max(0, int(budget)) / 100.0
    if path == "B":
        return 0.28 + latency + budget_cost
    if path in {"C", "D"}:
        return 0.10 + latency + budget_cost
    if path in {"A", "G"}:
        return 0.08 + latency + budget_cost
    return 0.03 + latency + budget_cost


def _safe_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


__all__ = [
    "PrimaryPathway",
    "InquiryActionPath",
    "PolicyMode",
    "SagePathwayDecision",
    "SageRouteOutcome",
    "SageRouteUtility",
    "SageRetrievalObservation",
    "SageRetrievalPolicy",
    "SageRetrievalStage",
    "SageSignalSignature",
    "adapt_inquiry_actions",
    "build_signal_signature",
    "plan_primary_retrieval",
    "primary_route_outcomes_from_notes",
    "route_utilities_from_outcomes",
    "signature_hash",
    "summarize_primary_observation",
]
