"""Build compact SAGE company learning profiles from existing surfaces."""
from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import UUID

from services.reasoning.sage.company_profile.types import (
    CompanyLearningProfile,
    LearningPrior,
)
from services.reasoning.sage.patterns.types import (
    PatternScoutCandidate,
    StructuralSignature,
)


def build_company_learning_profile(
    *,
    tenant_id: UUID,
    route_utilities: Iterable[Any] = (),
    question_policy_stats: Iterable[Any] = (),
    negative_memories: Iterable[Any] = (),
    shortcuts: Iterable[Any] = (),
    affordance_profiles: Iterable[Any] = (),
    structural_feature_rows: Iterable[Any] = (),
    structural_signatures: Iterable[StructuralSignature] = (),
    latent_pattern_candidates: Iterable[PatternScoutCandidate] = (),
    residuals: Iterable[Any] = (),
    recent_drift_signals: Iterable[Any] = (),
    source_reliability_stats: Iterable[Any] = (),
    actor_reliability_stats: Iterable[Any] = (),
    max_priors_per_kind: int = 12,
    built_at: datetime | None = None,
) -> CompanyLearningProfile:
    """Assemble tenant-specific adaptive priors without creating truth."""

    priors: list[LearningPrior] = []
    priors.extend(_route_priors(route_utilities))
    priors.extend(_question_priors(question_policy_stats))
    priors.extend(_negative_memory_priors(negative_memories))
    priors.extend(_shortcut_priors(shortcuts))
    priors.extend(_affordance_priors(affordance_profiles))
    priors.extend(_structural_feature_priors(structural_feature_rows))
    priors.extend(_structural_priors(structural_signatures))
    priors.extend(_latent_pattern_priors(latent_pattern_candidates))
    priors.extend(_residual_priors(residuals))
    priors.extend(_recent_drift_priors(recent_drift_signals))
    priors.extend(_source_reliability_priors(source_reliability_stats))
    priors.extend(_actor_reliability_priors(actor_reliability_stats))

    bounded = _bound_by_kind(priors, max_per_kind=max_priors_per_kind)
    sample_count = sum(max(0, int(prior.sample_count)) for prior in bounded)
    confidence = _profile_confidence(sample_count, bounded)
    notes = _profile_notes(bounded)
    return CompanyLearningProfile(
        tenant_id=tenant_id,
        built_at=built_at or datetime.now(timezone.utc),
        priors=tuple(bounded),
        sample_count=sample_count,
        confidence=confidence,
        notes=notes,
    )


def _route_priors(route_utilities: Iterable[Any]) -> list[LearningPrior]:
    out: list[LearningPrior] = []
    for utility in route_utilities:
        path = _text(_get(utility, "path"))
        if not path:
            continue
        attempts = _int(_get(utility, "attempts"))
        out.append(
            LearningPrior(
                kind="route",
                key=path,
                score=_float(_get(utility, "utility_score")),
                confidence=_float(_get(utility, "confidence")),
                sample_count=attempts,
                evidence_refs=(_text(_get(utility, "signature_hash")),),
                metadata={
                    "wins": _int(_get(utility, "wins")),
                    "selected_evidence": _int(_get(utility, "selected_evidence")),
                    "source": "sage_retrieval_route_utilities",
                    "canonical_write": False,
                    "authority_effect": "none",
                    "explanation_provenance": "tenant_scoped_aggregate",
                },
            )
        )
    return out


def _question_priors(question_policy_stats: Iterable[Any]) -> list[LearningPrior]:
    out: list[LearningPrior] = []
    for stat in question_policy_stats:
        primitive = _text(
            _get(stat, "primitive")
            or _get(stat, "question_primitive")
            or _get(stat, "key")
        ).upper()
        if not primitive:
            continue
        attempts = _int(_get(stat, "attempts"))
        successes = _int(_get(stat, "successes"))
        utility = _float(_get(stat, "utility_score"))
        success_rate = successes / max(1, attempts)
        score = max(-1.0, min(1.0, 0.62 * utility + 0.38 * (success_rate - 0.35)))
        out.append(
            LearningPrior(
                kind="question",
                key=primitive,
                score=round(score, 4),
                confidence=_sample_confidence(attempts),
                sample_count=attempts,
                metadata={
                    "successes": successes,
                    "success_rate": round(success_rate, 4),
                    "source": "sage_question_policy_stats",
                    "canonical_write": False,
                    "authority_effect": "none",
                    "explanation_provenance": "tenant_scoped_aggregate",
                },
            )
        )
    return out


def _negative_memory_priors(negative_memories: Iterable[Any]) -> list[LearningPrior]:
    out: list[LearningPrior] = []
    for memory in negative_memories:
        path = _text(_get(memory, "path") or _get(memory, "route"))
        key = (
            _text(_get(memory, "signature_hash"))
            or path
            or _text(_get(memory, "reason"))
            or _text(_get(memory, "memory_key"))
        )
        if not key:
            continue
        count = max(1, _int(_get(memory, "count") or _get(memory, "suppressed_count") or 1))
        confidence = max(_sample_confidence(count), _float(_get(memory, "confidence")))
        out.append(
            LearningPrior(
                kind="negative_memory",
                key=key,
                score=-min(1.0, 0.25 + count * 0.12),
                confidence=confidence,
                sample_count=count,
                metadata={
                    "reason": _text(_get(memory, "reason")),
                    "memory_type": _text(_get(memory, "memory_type")),
                    "path": path,
                    "signal_type": _text(_get(memory, "signal_type")),
                    "question_primitive": _text(_get(memory, "question_primitive")).upper(),
                    "source": "negative_memory",
                    "canonical_write": False,
                    "authority_effect": "none",
                    "explanation_provenance": "tenant_scoped_aggregate",
                },
            )
        )
    return out


def _shortcut_priors(shortcuts: Iterable[Any]) -> list[LearningPrior]:
    out: list[LearningPrior] = []
    for shortcut in shortcuts:
        key = (
            _text(_get(shortcut, "target_model_id"))
            or _text(_get(shortcut, "shortcut_key"))
            or _text(_get(shortcut, "signature_hash"))
        )
        if not key:
            continue
        hits = max(1, _int(_get(shortcut, "hits") or _get(shortcut, "support_count") or 1))
        score = _float(_get(shortcut, "utility_score") or _get(shortcut, "weight") or 0.45)
        out.append(
            LearningPrior(
                kind="shortcut",
                key=key,
                score=max(0.0, min(1.0, score)),
                confidence=_sample_confidence(hits),
                sample_count=hits,
                metadata={
                    "source": "discovery_shortcuts",
                    "canonical_write": False,
                    "authority_effect": "none",
                    "explanation_provenance": "tenant_scoped_aggregate",
                },
            )
        )
    return out


def _affordance_priors(affordance_profiles: Iterable[Any]) -> list[LearningPrior]:
    out: list[LearningPrior] = []
    for profile in affordance_profiles:
        key = _text(_get(profile, "model_id") or _get(profile, "target_model_id"))
        if not key:
            continue
        samples = max(1, _int(_get(profile, "reinforcements") or _get(profile, "attempts") or 1))
        score = _float(_get(profile, "utility_score") or _get(profile, "affordance_score"))
        out.append(
            LearningPrior(
                kind="affordance",
                key=key,
                score=max(-1.0, min(1.0, score)),
                confidence=_sample_confidence(samples),
                sample_count=samples,
                metadata={
                    "source": "retrieval_affordance_profiles",
                    "canonical_write": False,
                    "authority_effect": "none",
                    "explanation_provenance": "tenant_scoped_aggregate",
                },
            )
        )
    return out


def _structural_priors(
    structural_signatures: Iterable[StructuralSignature],
) -> list[LearningPrior]:
    signatures = list(structural_signatures)
    counts = Counter(facet for signature in signatures for facet in signature.shape_facets)
    out: list[LearningPrior] = []
    for facet, count in counts.items():
        if count < 2:
            continue
        out.append(
            LearningPrior(
                kind="structural",
                key=facet,
                score=min(1.0, count / max(2.0, len(signatures) * 0.5)),
                confidence=_sample_confidence(count),
                sample_count=count,
                metadata={
                    "source": "structural_signatures",
                    "canonical_write": False,
                    "authority_effect": "none",
                    "explanation_provenance": "tenant_scoped_aggregate",
                },
            )
        )
    return out


def _structural_feature_priors(structural_feature_rows: Iterable[Any]) -> list[LearningPrior]:
    out: list[LearningPrior] = []
    for row in structural_feature_rows:
        model_id = _text(_get(row, "model_id"))
        if not model_id:
            continue
        bridge_score = _float(_get(row, "bridge_score"))
        hub_score = _float(_get(row, "hub_score"))
        degree_total = _int(_get(row, "degree_total"))
        samples = max(1, degree_total)
        if bridge_score > 0:
            out.append(
                LearningPrior(
                    kind="structural_feature",
                    key=f"bridge:{model_id}",
                    score=max(0.0, min(1.0, bridge_score)),
                    confidence=_sample_confidence(samples),
                    sample_count=samples,
                    evidence_refs=(model_id,),
                    metadata={
                        "source": "model_structural_features",
                        "canonical_write": False,
                        "authority_effect": "none",
                        "explanation_provenance": "tenant_scoped_aggregate",
                    },
                )
            )
        if hub_score > 0:
            out.append(
                LearningPrior(
                    kind="structural_feature",
                    key=f"hub:{model_id}",
                    score=max(0.0, min(1.0, hub_score)),
                    confidence=_sample_confidence(samples),
                    sample_count=samples,
                    evidence_refs=(model_id,),
                    metadata={
                        "source": "model_structural_features",
                        "canonical_write": False,
                        "authority_effect": "none",
                        "explanation_provenance": "tenant_scoped_aggregate",
                    },
                )
            )
    return out


def _latent_pattern_priors(
    latent_pattern_candidates: Iterable[PatternScoutCandidate],
) -> list[LearningPrior]:
    out: list[LearningPrior] = []
    for candidate in latent_pattern_candidates:
        decay, decay_reasons = _latent_pattern_decay(candidate)
        metadata = dict(candidate.metadata)
        metadata.update(
            {
                "shared_facets": list(candidate.shared_facets),
                "surface_domain_count": candidate.surface_domain_count,
                "counterexample_count": candidate.counterexample_count,
                "canonical_write": False,
                "authority_effect": "none",
                "explanation_provenance": "tenant_scoped_aggregate",
                "source": "sage_global_scout",
            }
        )
        if decay_reasons:
            metadata["decay_reasons"] = list(decay_reasons)
        out.append(
            LearningPrior(
                kind="latent_pattern",
                key=candidate.candidate_hash,
                score=candidate.promotion_readiness_score,
                confidence=candidate.confidence,
                sample_count=candidate.support_count,
                evidence_refs=candidate.support_source_refs,
                decay=decay,
                metadata=metadata,
            )
        )
    return out


def _latent_pattern_decay(candidate: PatternScoutCandidate) -> tuple[float, tuple[str, ...]]:
    factors = [1.0]
    reasons: list[str] = []
    if candidate.counterexample_count > 0:
        factor = max(0.25, 1.0 - 0.16 * candidate.counterexample_count)
        factors.append(factor)
        reasons.append(f"counterexamples:{candidate.counterexample_count}")

    drift_score = max(
        _float(candidate.metadata.get("semantic_drift_score")),
        _float(candidate.metadata.get("drift_score")),
        _float(candidate.metadata.get("contradiction_score")),
    )
    if drift_score > 0:
        factor = max(0.20, 1.0 - 0.75 * min(1.0, drift_score))
        factors.append(factor)
        reasons.append(f"semantic_drift:{drift_score:.3f}")

    last_seen_at = _datetime(candidate.metadata.get("last_seen_at"))
    if last_seen_at is not None:
        now = datetime.now(timezone.utc)
        if last_seen_at.tzinfo is None:
            last_seen_at = last_seen_at.replace(tzinfo=timezone.utc)
        age_days = max(0.0, (now - last_seen_at).total_seconds() / 86400.0)
        if age_days >= 14.0:
            half_life_days = max(
                7.0,
                _float(candidate.metadata.get("decay_half_life_days")) or 90.0,
            )
            factor = max(0.18, 0.5 ** (age_days / half_life_days))
            factors.append(factor)
            reasons.append(f"stale_support_days:{age_days:.1f}")

    return round(max(0.0, min(1.0, min(factors))), 4), tuple(reasons)


def _residual_priors(residuals: Iterable[Any]) -> list[LearningPrior]:
    grouped: dict[str, list[Any]] = defaultdict(list)
    for residual in residuals:
        kind = _text(_get(residual, "residual_kind"))
        status = _text(_get(residual, "status"))
        if kind and (not status or status == "open"):
            grouped[kind].append(residual)
    out: list[LearningPrior] = []
    for kind, bucket in grouped.items():
        out.append(
            LearningPrior(
                kind="residual",
                key=kind,
                score=min(1.0, 0.30 + len(bucket) * 0.08),
                confidence=_sample_confidence(len(bucket)),
                sample_count=len(bucket),
                evidence_refs=tuple(
                    _text(_get(item, "id")) for item in bucket if _get(item, "id")
                ),
                metadata={
                    "source": "model_residual_evidence",
                    "canonical_write": False,
                    "authority_effect": "none",
                    "explanation_provenance": "tenant_scoped_aggregate",
                },
            )
        )
    return out


def _recent_drift_priors(drift_signals: Iterable[Any]) -> list[LearningPrior]:
    grouped: dict[str, list[Any]] = defaultdict(list)
    for signal in drift_signals:
        drift_kind = (
            _text(_get(signal, "drift_kind"))
            or _text(_get(signal, "residual_kind"))
            or _text(_get(signal, "kind"))
        )
        if drift_kind:
            grouped[drift_kind].append(signal)
    out: list[LearningPrior] = []
    for drift_kind, bucket in grouped.items():
        evidence_refs = tuple(
            _text(_get(item, "id"))
            or _text(_get(item, "model_id"))
            or _text(_get(item, "source_observation_id"))
            for item in bucket[:5]
            if _get(item, "id")
            or _get(item, "model_id")
            or _get(item, "source_observation_id")
        )
        out.append(
            LearningPrior(
                kind="drift",
                key=drift_kind,
                score=min(1.0, 0.34 + len(bucket) * 0.10),
                confidence=_sample_confidence(len(bucket)),
                sample_count=len(bucket),
                evidence_refs=evidence_refs,
                metadata={
                    "source": "model_residual_evidence",
                    "canonical_write": False,
                    "authority_effect": "none",
                    "explanation_provenance": "tenant_scoped_aggregate",
                },
            )
        )
    return out


def _source_reliability_priors(source_stats: Iterable[Any]) -> list[LearningPrior]:
    out: list[LearningPrior] = []
    for stat in source_stats:
        source_key = (
            _text(_get(stat, "source_key"))
            or _text(_get(stat, "source"))
            or _text(_get(stat, "source_channel"))
            or _text(_get(stat, "key"))
        )
        if not source_key:
            continue
        attempts = max(1, _int(_get(stat, "attempts") or _get(stat, "sample_count") or 1))
        successes = _int(_get(stat, "successes") or _get(stat, "wins"))
        total_credit = _float(_get(stat, "total_credit") or _get(stat, "credit"))
        avg_activation = _float(_get(stat, "avg_activation"))
        success_rate = successes / max(1, attempts)
        credit_rate = total_credit / max(1, attempts)
        score = max(
            -1.0,
            min(
                1.0,
                0.55 * (success_rate - 0.30)
                + 0.35 * credit_rate
                + 0.10 * (avg_activation - 0.35),
            ),
        )
        out.append(
            LearningPrior(
                kind="source_reliability",
                key=source_key,
                score=round(score, 4),
                confidence=_sample_confidence(attempts),
                sample_count=attempts,
                metadata={
                    "successes": successes,
                    "success_rate": round(success_rate, 4),
                    "avg_activation": round(avg_activation, 4),
                    "source": _text(_get(stat, "provenance_source"))
                    or "sage_reader_decision_attributions",
                    "canonical_write": False,
                    "authority_effect": "none",
                    "salience_only": True,
                    "explanation_provenance": "tenant_scoped_aggregate",
                },
            )
        )
    return out


def _actor_reliability_priors(actor_stats: Iterable[Any]) -> list[LearningPrior]:
    out: list[LearningPrior] = []
    for stat in actor_stats:
        actor_key = (
            _text(_get(stat, "actor_key"))
            or _text(_get(stat, "actor_id"))
            or _text(_get(stat, "key"))
        )
        if not actor_key:
            continue
        proposition_kind = _text(_get(stat, "proposition_kind")) or "all"
        attempts = max(1, _int(_get(stat, "attempts") or _get(stat, "sample_count") or 1))
        successes = _int(_get(stat, "successes") or _get(stat, "true_outcomes"))
        success_rate = successes / max(1, attempts)
        avg_asserted = _float(_get(stat, "avg_asserted_confidence"))
        calibration_gap = abs(success_rate - avg_asserted) if avg_asserted > 0 else 0.0
        score = max(-1.0, min(1.0, success_rate - 0.50 - calibration_gap * 0.35))
        out.append(
            LearningPrior(
                kind="actor_reliability",
                key=f"{actor_key}:{proposition_kind}",
                score=round(score, 4),
                confidence=_sample_confidence(attempts),
                sample_count=attempts,
                evidence_refs=tuple(
                    _text(ref)
                    for ref in (_get(stat, "evidence_refs") or ())
                    if _text(ref)
                ),
                metadata={
                    "actor_id": actor_key,
                    "proposition_kind": proposition_kind,
                    "successes": successes,
                    "success_rate": round(success_rate, 4),
                    "avg_asserted_confidence": round(avg_asserted, 4),
                    "calibration_gap": round(calibration_gap, 4),
                    "source": _text(_get(stat, "provenance_source"))
                    or "calibration_stats",
                    "canonical_write": False,
                    "authority_effect": "none",
                    "salience_only": True,
                    "explanation_provenance": "tenant_scoped_aggregate",
                },
            )
        )
    return out


def _bound_by_kind(
    priors: list[LearningPrior],
    *,
    max_per_kind: int,
) -> list[LearningPrior]:
    grouped: dict[str, list[LearningPrior]] = defaultdict(list)
    for prior in priors:
        grouped[prior.kind].append(prior)
    out: list[LearningPrior] = []
    for kind in sorted(grouped):
        out.extend(
            sorted(
                grouped[kind],
                key=lambda prior: (-abs(prior.effective_score), -prior.confidence, prior.key),
            )[: max(1, int(max_per_kind))]
        )
    return out


def _profile_confidence(sample_count: int, priors: list[LearningPrior]) -> float:
    if not priors:
        return 0.0
    sample_factor = min(1.0, math.log1p(max(0, sample_count)) / math.log(65))
    prior_factor = sum(prior.confidence for prior in priors) / max(1, len(priors))
    return round(max(0.0, min(1.0, 0.55 * sample_factor + 0.45 * prior_factor)), 4)


def _profile_notes(priors: list[LearningPrior]) -> tuple[str, ...]:
    kinds = sorted({prior.kind for prior in priors})
    if not kinds:
        return ("empty_company_learning_profile",)
    notes = [f"profile_priors:{','.join(kinds)}", "canonical_write:false"]
    if any(prior.kind == "latent_pattern" and prior.decay < 0.999 for prior in priors):
        notes.append("latent_pattern_decay:applied")
    return tuple(notes)


def _sample_confidence(sample_count: int) -> float:
    return round(max(0.0, min(1.0, math.log1p(max(0, sample_count)) / math.log(17))), 4)


def _get(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        return value.get(key)
    return getattr(value, key, None)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


__all__ = ["build_company_learning_profile"]
