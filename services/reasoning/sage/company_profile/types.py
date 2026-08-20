"""Tenant-scoped SAGE company learning profile types."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID


_REF_KEY_MARKERS = (
    "evidence_ref",
    "evidence_refs",
    "evidence_id",
    "evidence_ids",
    "source_ref",
    "source_refs",
    "support_source_ref",
    "support_source_refs",
    "supporting_observation_id",
    "supporting_observation_ids",
    "supporting_residual_id",
    "supporting_residual_ids",
    "observation_id",
    "observation_ids",
    "residual_id",
    "residual_ids",
)


@dataclass(frozen=True, slots=True)
class LearningPrior:
    """One compact company-specific policy prior.

    A prior is optimization memory. It may steer retrieval or questions, but it
    is not canonical company truth.
    """

    kind: str
    key: str
    score: float
    confidence: float
    sample_count: int = 0
    evidence_refs: tuple[str, ...] = ()
    decay: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def effective_score(self) -> float:
        return round(
            max(-1.0, min(1.0, float(self.score) * float(self.confidence) * self.decay)),
            4,
        )

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        evidence_refs = tuple(data.pop("evidence_refs", ()) or ())
        metadata = dict(data.get("metadata") or {})
        allow_refs = metadata.get("explanation_safe_evidence_refs") is True
        data["metadata"] = _sanitize_policy_metadata(metadata, allow_refs=allow_refs)
        if allow_refs:
            data["evidence_refs"] = list(evidence_refs)
        else:
            data["evidence_ref_count"] = len(evidence_refs)
            if evidence_refs:
                data["evidence_refs_redacted"] = True
        data["effective_score"] = self.effective_score
        return data


@dataclass(frozen=True, slots=True)
class CompanyLearningProfile:
    """Compact SAGE digest for one tenant/company."""

    tenant_id: UUID
    built_at: datetime
    priors: tuple[LearningPrior, ...]
    sample_count: int
    confidence: float
    notes: tuple[str, ...] = ()

    def priors_for_kind(self, kind: str) -> tuple[LearningPrior, ...]:
        return tuple(prior for prior in self.priors if prior.kind == kind)

    def best_prior(self, *, kind: str, key: str) -> LearningPrior | None:
        matches = [
            prior
            for prior in self.priors
            if prior.kind == kind and prior.key == key and prior.confidence > 0
        ]
        if not matches:
            return None
        return max(matches, key=lambda prior: abs(prior.effective_score))

    def to_policy_notes(self, *, max_priors: int = 24) -> dict[str, Any]:
        priors = sorted(
            self.priors,
            key=lambda prior: (-abs(prior.effective_score), prior.kind, prior.key),
        )[: max(0, int(max_priors))]
        return {
            "enabled": True,
            "tenant_id": str(self.tenant_id),
            "built_at": self.built_at.isoformat(),
            "sample_count": self.sample_count,
            "confidence": round(float(self.confidence), 4),
            "notes": list(self.notes),
            "priors": [prior.to_dict() for prior in priors],
            "canonical_write": False,
            "authority_effect": "none",
        }


def _sanitize_policy_metadata(value: Any, *, allow_refs: bool) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if _is_reference_metadata_key(key_text) and not allow_refs:
                out[f"{key_text}_count"] = _metadata_ref_count(item)
                out[f"{key_text}_redacted"] = True
                continue
            out[key_text] = _sanitize_policy_metadata(item, allow_refs=allow_refs)
        out.setdefault("canonical_write", False)
        out.setdefault("authority_effect", "none")
        return out
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_policy_metadata(item, allow_refs=allow_refs) for item in value]
    return value


def _is_reference_metadata_key(key: str) -> bool:
    normalized = key.lower()
    if normalized == "explanation_safe_evidence_refs":
        return False
    return any(marker in normalized for marker in _REF_KEY_MARKERS)


def _metadata_ref_count(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, dict):
        return len(value)
    if isinstance(value, (str, bytes)):
        return 1 if value else 0
    try:
        return len(value)
    except TypeError:
        return 1


__all__ = [
    "CompanyLearningProfile",
    "LearningPrior",
]
