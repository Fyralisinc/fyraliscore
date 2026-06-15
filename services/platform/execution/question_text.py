"""Question text anchoring and repair helpers for inquiry retrieval."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from services.reasoning.retrieval.primary import TriggerContext

from .routing import trigger_text


@dataclass(frozen=True, slots=True)
class QuestionAnchors:
    subject: str
    claim: str
    focus: str
    constraint: str | None = None


def claim_from_text(text: str, *, fallback: str) -> str:
    clean = re.sub(r"\s+", " ", text or "").strip()
    if not clean:
        return fallback
    if len(clean) <= 140:
        return clean
    return clean[:137].rstrip() + "..."


def question_anchors(trigger: TriggerContext) -> QuestionAnchors:
    text = trigger_text(trigger)
    claim = claim_from_text(text, fallback="this signal")
    entity_labels = question_entity_labels(trigger)
    subject = question_subject(text, entity_labels)
    focus = question_focus_phrase(text, subject=subject)
    constraint = question_constraint_phrase(text)
    return QuestionAnchors(
        subject=subject,
        claim=claim,
        focus=focus,
        constraint=constraint,
    )


def question_entity_labels(trigger: TriggerContext) -> tuple[str, ...]:
    labels: list[str] = []
    for raw_entity in trigger.seed_entity_ids[:8]:
        if not isinstance(raw_entity, dict):
            continue
        label = entity_label_from_seed(raw_entity)
        if not label:
            continue
        if label.casefold() in {existing.casefold() for existing in labels}:
            continue
        labels.append(label)
    return tuple(labels[:4])


def entity_label_from_seed(entity: dict[str, Any]) -> str | None:
    for key in ("label", "name", "title", "natural", "slug", "id"):
        value = entity.get(key)
        if value is None:
            continue
        label = clean_question_anchor(str(value))
        if not label or looks_like_machine_identifier(label):
            continue
        return label
    return None


def question_subject(text: str, entity_labels: tuple[str, ...]) -> str:
    if entity_labels:
        return clean_question_anchor(", ".join(entity_labels[:3])) or "this signal"
    spans = capitalized_anchor_spans(text)
    if spans:
        return clean_question_anchor(", ".join(spans[:3])) or "this signal"
    return "this signal"


def capitalized_anchor_spans(text: str) -> tuple[str, ...]:
    spans: list[str] = []
    pattern = re.compile(
        r"\b(?:[A-Z][A-Za-z0-9]*(?:[-_][A-Za-z0-9]+)*"
        r"(?:\s+[A-Z][A-Za-z0-9]*(?:[-_][A-Za-z0-9]+)*){0,2})\b"
    )
    stop = {
        "Board",
        "Company",
        "Customer",
        "Customers",
        "Data",
        "Goal",
        "Issue",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    }
    for match in pattern.finditer(text or ""):
        span = clean_question_anchor(match.group(0))
        if not span or span in stop or looks_like_machine_identifier(span):
            continue
        if span.casefold() not in {existing.casefold() for existing in spans}:
            spans.append(span)
        if len(spans) >= 4:
            break
    return tuple(spans)


def question_constraint_phrase(text: str) -> str | None:
    clean = " ".join((text or "").split())
    if not clean:
        return None
    patterns = (
        r"\bblocked by\s+([^.;,]+)",
        r"\bconstrained by\s+([^.;,]+)",
        r"\bdepends on\s+([^.;,]+)",
        r"\bwaiting on\s+([^.;,]+)",
        r"\bdue to\s+([^.;,]+)",
        r"\bbecause\s+([^.;,]+)",
    )
    for pattern in patterns:
        match = re.search(pattern, clean, flags=re.IGNORECASE)
        if not match:
            continue
        phrase = re.split(r"\s+(?:and|but|while|which|that)\s+", match.group(1))[0]
        phrase = clean_question_anchor(phrase)
        if phrase:
            return truncate_text(phrase, 90)
    return None


def question_focus_phrase(text: str, *, subject: str) -> str:
    clean = " ".join((text or "").split())
    if not clean:
        return subject
    clean = re.sub(r"^\[[^\]]+\]\s*", "", clean).strip()
    candidates: list[tuple[float, str]] = []

    before_context, sep, after_context = clean.partition("Company context:")
    preface_focus = focus_from_preface(before_context)
    if preface_focus:
        candidates.append((1.2, preface_focus))
    candidate_text = after_context if sep else clean
    for index, sentence in enumerate(focus_sentences(candidate_text)):
        if looks_like_company_overview(sentence):
            continue
        score = focus_sentence_score(sentence) - index * 0.05
        if score <= 0.0:
            continue
        candidates.append((score, sentence))

    if not candidates:
        return truncate_text(subject, 120)
    _, best = max(candidates, key=lambda item: (item[0], len(item[1])))
    return truncate_text(best, 140)


def focus_from_preface(text: str) -> str | None:
    clean = clean_question_anchor(text)
    if not clean:
        return None
    match = re.search(r"\brelates to\s+(.+)$", clean, flags=re.IGNORECASE)
    if match:
        return clean_question_anchor(match.group(1))
    return truncate_text(clean, 120)


def focus_sentences(text: str) -> tuple[str, ...]:
    out: list[str] = []
    for raw in re.split(r"(?<=[.!?])\s+", text or ""):
        sentence = clean_question_anchor(raw)
        if sentence:
            out.append(sentence)
    return tuple(out)


def looks_like_company_overview(sentence: str) -> bool:
    lower = sentence.casefold()
    return (
        "post-product-market fit" in lower
        or "months runway" in lower
        or "people in " in lower
        or "series " in lower
    )


def focus_sentence_score(sentence: str) -> float:
    lower = sentence.casefold()
    keywords = (
        "approve",
        "approval",
        "asked",
        "at risk",
        "blocker",
        "blocked",
        "capacity",
        "cannot",
        "conflict",
        "constrained",
        "delayed",
        "dependency",
        "edge case",
        "expansion",
        "falsifier",
        "gap",
        "incident",
        "owner",
        "procurement",
        "redline",
        "repeats",
        "review",
        "risk",
        "saml",
        "security",
        "stage",
        "stale",
        "terms",
        "visible",
    )
    score = sum(1.0 for keyword in keywords if keyword in lower)
    if "$" in sentence or "arr" in lower:
        score += 1.0
    if len(sentence) >= 40:
        score += 0.3
    return score


def specific_question(primitive: str, anchors: QuestionAnchors) -> str:
    subject = anchors.subject or "this signal"
    focus = safe_question_focus(anchors.focus or anchors.claim, subject)
    constraint = anchors.constraint

    if primitive == "DEPENDENCY":
        if constraint:
            question = (
                f"Is {constraint} the dependency that puts {subject} "
                "on the critical path?"
            )
        else:
            question = f"Is {focus} the critical-path issue for {subject}?"
    elif primitive == "COMMITMENT":
        question = (
            f"Which active promise, deadline, or expected outcome does {focus} "
            f"put at risk for {subject}?"
        )
    elif primitive == "CONSTRAINT":
        if constraint:
            question = (
                f"What resource, policy, or capacity constraint behind {constraint} "
                f"is blocking {subject}?"
            )
        else:
            question = (
                "What resource, policy, or capacity constraint is driving "
                f"{focus} for {subject}?"
            )
    elif primitive == "COUNTEREVIDENCE":
        counter_focus = counterevidence_focus(
            anchors.claim,
            fallback=focus,
            subject=subject,
        )
        question = (
            f"What evidence would weaken the interpretation that {counter_focus}?"
        )
    elif primitive == "OWNERSHIP":
        if constraint:
            question = (
                f"Who owns resolving {constraint} for {subject}, and who owns "
                "the affected commitment?"
            )
        else:
            question = f"Who owns the next action on {focus} for {subject}?"
    elif primitive == "GOAL_IMPACT":
        question = (
            "Which customer goal, revenue path, or scarce resource does "
            f"{focus} threaten for {subject}?"
        )
    elif primitive == "RECURRENCE":
        question = (
            f"Has {focus} appeared before for {subject}, or is this a one-off signal?"
        )
    else:
        question = f"What does {subject} require us to verify next?"
    return truncate_text(" ".join(question.split()), 240)


def counterevidence_focus(
    claim: str,
    *,
    fallback: str,
    subject: str,
) -> str:
    clean = clean_question_anchor(claim)
    subject_parts = [
        part.strip().casefold()
        for part in re.split(r"[,/]| and ", subject or "")
        if part.strip()
    ]
    if clean and any(part in clean.casefold() for part in subject_parts):
        return truncate_text(clean, 150)
    return fallback


def safe_question_focus(value: str, subject: str) -> str:
    focus = clean_question_anchor(value)
    if is_specific_focus_phrase(focus):
        return truncate_text(focus, 100)
    keyword_focus = domain_keyword_focus(focus)
    if keyword_focus:
        return truncate_text(keyword_focus, 100)
    before_verb = re.split(
        r"\b(should|is|are|was|were|has|have|will|may|might|must|needs?|requires?|involves?|creates?|reports?|shows?|indicates?|threatens?)\b",
        focus,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    before_verb = clean_question_anchor(before_verb)
    if len(before_verb) >= 8:
        return truncate_text(before_verb, 100)
    return truncate_text(subject or "this signal", 100)


def clean_question_anchor(value: str) -> str:
    cleaned = re.sub(r"[_]+", " ", value or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" \t\r\n'\"`.,;:()[]{}")
    return cleaned


def looks_like_machine_identifier(value: str) -> bool:
    clean = value.strip()
    if not clean:
        return True
    if re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        clean,
        flags=re.IGNORECASE,
    ):
        return True
    if re.fullmatch(r"[0-9a-f]{16,}", clean, flags=re.IGNORECASE):
        return True
    return False


GENERIC_DELTA_SLOT_PATTERNS = (
    "blocker",
    "constraint",
    "critical path",
    "critical path status",
    "dependency",
    "goal impact",
    "issue type",
    "owner",
    "ownership",
    "recurrence",
    "status",
    "which existing model should change",
    "what evidence would weaken this interpretation",
    "who owns the next action",
    "whether this signal is already captured",
    "what evidence supports no model update",
    "which prior model is now stale",
    "which existing beliefs should be separated or combined",
    "whether this appeared before",
    "this appeared before",
    "appeared before",
)


def clean_question_focus_phrase(value: str) -> str:
    phrase = clean_question_anchor(value)
    phrase = re.sub(
        r"^(commitment|constraint|counterevidence|dependency|goal[_\s-]*impact|ownership|recurrence)\s*[:/-]\s*",
        "",
        phrase,
        flags=re.IGNORECASE,
    )
    phrase = re.sub(
        r"^(whether|if|what|which|who|how|is|are|does|do|has|have|should|would|can|could)\s+",
        "",
        phrase,
        flags=re.IGNORECASE,
    )
    phrase = re.sub(
        r"^(the|a|an)\s+(evidence|source|question|signal)\s+(that|for|about)\s+",
        "",
        phrase,
        flags=re.IGNORECASE,
    )
    phrase = re.sub(
        r"\b(actually|explicitly|currently)\b", "", phrase, flags=re.IGNORECASE
    )
    phrase = re.sub(r"\bpattern\s+frequency\b", "pattern", phrase, flags=re.IGNORECASE)
    phrase = re.sub(r"\bpattern\s+pattern\b", "pattern", phrase, flags=re.IGNORECASE)
    phrase = re.sub(r"\s+", " ", phrase).strip(" .,:;?") or "the signal"
    return phrase


def is_specific_focus_phrase(phrase: str) -> bool:
    lower = phrase.casefold()
    if len(phrase) < 8 or lower in {"the signal", "this signal", "this interpretation"}:
        return False
    if any(pattern in lower for pattern in GENERIC_DELTA_SLOT_PATTERNS):
        words = set(re.findall(r"[a-z0-9_-]+", lower))
        domain_words = {
            "audit",
            "customer",
            "data",
            "export",
            "incident",
            "mapping",
            "permission",
            "procurement",
            "renewal",
            "saml",
            "security",
            "soc2",
            "trail",
        }
        if not words.intersection(domain_words):
            return False
    sentence_verbs = (
        " should ",
        " is ",
        " are ",
        " was ",
        " were ",
        " has ",
        " have ",
        " will ",
        " may ",
        " might ",
        " must ",
    )
    if len(phrase) > 72 and any(marker in f" {lower} " for marker in sentence_verbs):
        return False
    if phrase.count(" ") > 13:
        return False
    return True


def fallback_focus_from_delta_claim(
    claim: str,
    trigger: TriggerContext,
) -> str:
    clean = clean_question_anchor(claim)
    quoted = re.findall(r"'([^']{8,90})'|\"([^\"]{8,90})\"", clean)
    for left, right in quoted:
        phrase = clean_question_focus_phrase(left or right)
        if is_specific_focus_phrase(phrase):
            return truncate_text(phrase, 120)

    before_verb = re.split(
        r"\b(should|is|are|was|were|has|have|will|may|might|must|needs?|requires?|involves?|creates?|reports?|shows?|indicates?)\b",
        clean,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    before_verb = clean_question_focus_phrase(before_verb)
    keyword_phrase = domain_keyword_focus(clean)
    if is_specific_focus_phrase(before_verb):
        if keyword_phrase and keyword_phrase.casefold() not in before_verb.casefold():
            return truncate_text(f"{before_verb} {keyword_phrase}", 120)
        return truncate_text(before_verb, 120)
    if keyword_phrase:
        return truncate_text(keyword_phrase, 120)

    anchors = question_anchors(trigger)
    if anchors.focus:
        return truncate_text(anchors.focus, 120)
    return "the signal"


def domain_keyword_focus(text: str) -> str:
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]*", text)
    if not tokens:
        return ""
    keywords = {
        "blocker",
        "blockers",
        "capacity",
        "commitment",
        "constraint",
        "dependency",
        "evidence",
        "incident",
        "onboarding",
        "permission",
        "policy",
        "procurement",
        "renewal",
        "replay",
        "risk",
        "saml",
        "timeline",
    }
    for idx, token in enumerate(tokens):
        if token.casefold() not in keywords:
            continue
        start = max(0, idx - 2)
        end = min(len(tokens), idx + 4)
        phrase = clean_question_focus_phrase(" ".join(tokens[start:end]))
        if len(phrase) >= 8:
            return phrase
    return ""


def truncate_text(text: str, limit: int) -> str:
    clean = " ".join((text or "").split())
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 3)].rstrip() + "..."
