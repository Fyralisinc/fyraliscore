"""Model formation contract candidates.

Formation candidates are not beliefs. They are deterministic obligations for
Think to decide whether a belief should be formed, updated, deferred, rejected,
or marked already covered by existing Models.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Literal
from uuid import UUID


FormationResolution = Literal[
    "formed",
    "updated",
    "deferred",
    "rejected",
    "already_covered",
]

_MAX_TEXT_CHARS = 260


@dataclass(frozen=True)
class FormationSpec:
    id: str
    domain: str
    facet: str
    question_template: str
    keywords: tuple[str, ...]
    min_evidence: int = 2
    priority: float = 0.5


@dataclass(frozen=True)
class FormationEvidence:
    id: UUID
    kind: Literal["observation", "model"]
    text: str
    occurred_at: datetime | None = None
    confidence: float | None = None

    def prompt_text(self) -> str:
        text = _normalize_space(self.text)
        if len(text) <= _MAX_TEXT_CHARS:
            return text
        return text[: _MAX_TEXT_CHARS - 3].rstrip() + "..."


@dataclass(frozen=True)
class FormationCandidate:
    candidate_id: str
    spec_id: str
    domain: str
    facet: str
    subject_type: str
    subject_id: UUID
    question: str
    rationale: str
    salience: float
    evidence: tuple[FormationEvidence, ...] = field(default_factory=tuple)
    existing_model_ids: tuple[UUID, ...] = field(default_factory=tuple)
    allowed_resolutions: tuple[FormationResolution, ...] = (
        "formed",
        "updated",
        "deferred",
        "rejected",
        "already_covered",
    )

    @property
    def evidence_observation_ids(self) -> tuple[UUID, ...]:
        return tuple(e.id for e in self.evidence if e.kind == "observation")

    @property
    def evidence_model_ids(self) -> tuple[UUID, ...]:
        return tuple(e.id for e in self.evidence if e.kind == "model")

    def to_prompt_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "formation_type": self.spec_id,
            "subject": {"type": self.subject_type, "id": str(self.subject_id)},
            "question": self.question,
            "rationale": self.rationale,
            "salience": round(self.salience, 3),
            "evidence_observation_ids": [
                str(eid) for eid in self.evidence_observation_ids
            ],
            "evidence_model_ids": [str(eid) for eid in self.evidence_model_ids],
            "existing_model_ids": [str(mid) for mid in self.existing_model_ids],
            "allowed_resolutions": list(self.allowed_resolutions),
            "evidence_snippets": [
                {
                    "kind": evidence.kind,
                    "id": str(evidence.id),
                    "text": evidence.prompt_text(),
                }
                for evidence in self.evidence[:4]
            ],
        }


EMPLOYEE_FORMATION_SPECS: tuple[FormationSpec, ...] = (
    FormationSpec(
        id="employee.capability",
        domain="employee",
        facet="capability",
        question_template=(
            "What capability has actor {actor_id} repeatedly demonstrated?"
        ),
        keywords=(
            "shipped",
            "delivered",
            "resolved",
            "handled",
            "owned",
            "built",
            "merged",
            "closed",
            "diagnosed",
            "unblocked",
            "stabilized",
            "led",
            "reviewed",
        ),
        priority=0.92,
    ),
    FormationSpec(
        id="employee.support_need",
        domain="employee",
        facet="support_need",
        question_template=(
            "What support condition would make actor {actor_id} more effective?"
        ),
        keywords=(
            "needs",
            "need",
            "blocked",
            "unclear",
            "ambiguity",
            "clarity",
            "owner",
            "ownership",
            "decision",
            "context",
            "requirements",
            "waiting",
            "depends",
        ),
        priority=0.9,
    ),
    FormationSpec(
        id="employee.work_style",
        domain="employee",
        facet="work_style",
        question_template=(
            "What work-style or preference pattern is visible for actor {actor_id}?"
        ),
        keywords=(
            "prefers",
            "preference",
            "likes",
            "async",
            "written",
            "docs",
            "documentation",
            "deep work",
            "focus",
            "brief",
            "detailed",
            "pair",
            "solo",
        ),
        priority=0.82,
    ),
    FormationSpec(
        id="employee.load_risk",
        domain="employee",
        facet="load_risk",
        question_template=(
            "Is actor {actor_id} showing a repeated load, bandwidth, or fragility risk?"
        ),
        keywords=(
            "overloaded",
            "over capacity",
            "bandwidth",
            "juggling",
            "stretched",
            "burnout",
            "too much",
            "pager",
            "on-call",
            "fatigue",
            "swamped",
        ),
        priority=0.86,
    ),
    FormationSpec(
        id="employee.collaboration_pattern",
        domain="employee",
        facet="collaboration_pattern",
        question_template=(
            "What collaboration pattern is emerging around actor {actor_id}?"
        ),
        keywords=(
            "collaborates",
            "coordinates",
            "mentor",
            "mentors",
            "handoff",
            "hands off",
            "depends on",
            "reviews",
            "pairs",
            "escalates",
            "aligns",
        ),
        priority=0.78,
    ),
    FormationSpec(
        id="employee.commitment_pattern",
        domain="employee",
        facet="commitment_pattern",
        question_template=(
            "What commitment-taking or delivery pattern is visible for actor {actor_id}?"
        ),
        keywords=(
            "started",
            "working on",
            "committed",
            "promised",
            "due",
            "delayed",
            "missed",
            "completed",
            "done",
            "delivered",
            "blocked",
        ),
        priority=0.76,
    ),
)


def build_model_formation_candidates(
    trigger: Any,
    bundle: Any,
    *,
    max_candidates: int = 8,
    specs: Iterable[FormationSpec] = EMPLOYEE_FORMATION_SPECS,
) -> tuple[FormationCandidate, ...]:
    """Return deterministic formation obligations for the retrieved context."""
    del trigger  # Reserved for trigger-kind-specific specs.
    actor_evidence = _actor_evidence(bundle)
    candidates: list[FormationCandidate] = []
    for actor_id, evidence in actor_evidence.items():
        if len(evidence) < 2:
            continue
        existing_model_ids = tuple(
            sorted(
                {
                    item.id
                    for item in evidence
                    if item.kind == "model"
                },
                key=str,
            )
        )
        for spec in specs:
            matching = tuple(_matching_evidence(evidence, spec))
            if len(matching) < spec.min_evidence:
                continue
            candidates.append(
                _candidate_for_spec(
                    spec,
                    actor_id=actor_id,
                    evidence=matching,
                    existing_model_ids=existing_model_ids,
                )
            )
    candidates.sort(
        key=lambda candidate: (
            -candidate.salience,
            candidate.spec_id,
            str(candidate.subject_id),
            candidate.candidate_id,
        )
    )
    return tuple(candidates[: max(0, int(max_candidates))])


def formation_candidate_ids(trigger: Any, bundle: Any) -> frozenset[str]:
    return frozenset(
        candidate.candidate_id
        for candidate in build_model_formation_candidates(trigger, bundle)
    )


def _candidate_for_spec(
    spec: FormationSpec,
    *,
    actor_id: UUID,
    evidence: tuple[FormationEvidence, ...],
    existing_model_ids: tuple[UUID, ...],
) -> FormationCandidate:
    evidence_ids = tuple(sorted({str(item.id) for item in evidence}))
    digest = hashlib.sha256(
        json.dumps(
            {
                "spec_id": spec.id,
                "actor_id": str(actor_id),
                "evidence_ids": evidence_ids,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    matched_count = len(evidence)
    model_count = sum(1 for item in evidence if item.kind == "model")
    salience = min(1.0, spec.priority + (matched_count - spec.min_evidence) * 0.03)
    if model_count:
        salience = min(1.0, salience + 0.03)
    return FormationCandidate(
        candidate_id=f"formation:{spec.id}:{actor_id}:{digest}",
        spec_id=spec.id,
        domain=spec.domain,
        facet=spec.facet,
        subject_type="actor",
        subject_id=actor_id,
        question=spec.question_template.format(actor_id=actor_id),
        rationale=(
            f"{matched_count} retrieved evidence items repeatedly touch "
            f"{spec.domain}.{spec.facet} for this actor."
        ),
        salience=salience,
        evidence=tuple(sorted(evidence, key=lambda item: (item.kind, str(item.id)))),
        existing_model_ids=existing_model_ids,
    )


def _actor_evidence(bundle: Any) -> dict[UUID, tuple[FormationEvidence, ...]]:
    grouped: dict[UUID, dict[UUID, FormationEvidence]] = {}
    for observation in getattr(bundle, "observations", None) or []:
        actor_id = getattr(observation, "actor_id", None)
        evidence_id = getattr(observation, "id", None)
        if not isinstance(actor_id, UUID) or not isinstance(evidence_id, UUID):
            continue
        text = str(getattr(observation, "content_text", "") or "").strip()
        if not text:
            continue
        _add_evidence(
            grouped.setdefault(actor_id, {}),
            FormationEvidence(
                id=evidence_id,
                kind="observation",
                text=text,
                occurred_at=getattr(observation, "occurred_at", None),
            ),
        )
    for model in getattr(bundle, "models", None) or []:
        scope_actors = getattr(model, "scope_actors", None) or []
        model_id = getattr(model, "id", None)
        if not isinstance(model_id, UUID):
            continue
        text = str(getattr(model, "natural", "") or "").strip()
        if not text:
            proposition = getattr(model, "proposition", None)
            text = json.dumps(proposition, sort_keys=True, default=str)
        confidence = getattr(model, "confidence", None)
        for actor_id in scope_actors:
            if isinstance(actor_id, UUID):
                _add_evidence(
                    grouped.setdefault(actor_id, {}),
                    FormationEvidence(
                        id=model_id,
                        kind="model",
                        text=text,
                        occurred_at=getattr(model, "created_at", None),
                        confidence=float(confidence) if confidence is not None else None,
                    ),
                )
    return {
        actor_id: tuple(items.values())
        for actor_id, items in grouped.items()
    }


def _matching_evidence(
    evidence: Iterable[FormationEvidence],
    spec: FormationSpec,
) -> list[FormationEvidence]:
    return [
        item
        for item in evidence
        if _text_matches_keywords(item.text, spec.keywords)
    ]


def _add_evidence(
    bucket: dict[UUID, FormationEvidence],
    evidence: FormationEvidence,
) -> None:
    existing = bucket.get(evidence.id)
    if existing is None:
        bucket[evidence.id] = evidence
        return
    if evidence.text and evidence.text not in existing.text:
        bucket[evidence.id] = FormationEvidence(
            id=existing.id,
            kind=existing.kind,
            text=f"{existing.text}\n{evidence.text}",
            occurred_at=existing.occurred_at or evidence.occurred_at,
            confidence=(
                existing.confidence
                if existing.confidence is not None
                else evidence.confidence
            ),
        )


def _text_matches_keywords(text: str, keywords: tuple[str, ...]) -> bool:
    lowered = _normalize_space(text).casefold()
    return any(_keyword_matches(lowered, keyword) for keyword in keywords)


def _keyword_matches(text: str, keyword: str) -> bool:
    needle = _normalize_space(keyword).casefold()
    if " " in needle or "-" in needle:
        return needle in text
    return bool(re.search(rf"\b{re.escape(needle)}\b", text))


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


__all__ = [
    "EMPLOYEE_FORMATION_SPECS",
    "FormationCandidate",
    "FormationEvidence",
    "FormationResolution",
    "FormationSpec",
    "build_model_formation_candidates",
    "formation_candidate_ids",
]
