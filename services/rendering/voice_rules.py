"""services/rendering/voice_rules.py — post-LLM voice enforcement.

Phase 1 of the Agent-RND build plan. Rule-based validators run after the
LLM produces text and before it ships to callers. Reject-severity
violations trigger a single retry with an explicit correction prompt;
flag-severity violations log but pass through.

Design reference: company-os-design.md §8 (voice principles).

The rules catch cheaply what we do NOT want the model to do:
  - Exclamation marks.
  - Marketing / performative language ("exciting", "insights",
    "leverage" (unless architectural), "unpack", "dive into",
    "quick heads up").
  - Emoji (any pictograph).
  - Sentences over 35 words (§8 "economic").
  - Card bodies with no concrete reference (no name / number / date)
    — violates §8 "specific to this company".
  - Hedge preamble ("I just wanted to", "I'd like to highlight", "FYI").

Rules are pure string functions. They see the raw text the LLM
returned (HTML is stripped before rule execution for the word-level
rules; structural rules read the HTML directly).
"""
from __future__ import annotations

import enum
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Iterable


# ---------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------


class Severity(str, enum.Enum):
    REJECT = "reject"
    FLAG = "flag"


@dataclass(frozen=True)
class Violation:
    rule: str
    severity: Severity
    message: str
    offending_text: str | None = None

    def to_dict(self) -> dict:
        return {
            "rule": self.rule,
            "severity": self.severity.value,
            "message": self.message,
            "offending_text": self.offending_text,
        }


class VoiceRule:
    """Base class. Subclasses implement `check(text, context) -> list[Violation]`."""

    name: str = "voice_rule"
    severity: Severity = Severity.REJECT

    def check(self, text: str, context: "RuleContext | None" = None) -> list[Violation]:
        raise NotImplementedError


@dataclass
class RuleContext:
    """Per-render context the rules may consult.

    `kind` is one of the rendering types (greeting, card_observation,
    card_decision, card_question, query_grid_item, conversation_turn,
    close_line). Some rules only fire for certain kinds — e.g.,
    RequiresSpecificity fires for card bodies only.
    """

    kind: str
    extras: dict[str, object] = field(default_factory=dict)


# ---------------------------------------------------------------------
# HTML → plain text helper
# ---------------------------------------------------------------------


class _HTMLToText(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._buf: list[str] = []

    def handle_data(self, data: str) -> None:
        self._buf.append(data)

    def text(self) -> str:
        return "".join(self._buf)


def strip_html(html: str) -> str:
    """Extract visible text from an HTML fragment. Never raises."""
    try:
        p = _HTMLToText()
        p.feed(html)
        p.close()
        return p.text()
    except Exception:
        # Defensive: if the HTML is malformed the caller still wants
        # the rule to run on something.
        return re.sub(r"<[^>]+>", " ", html)


# ---------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------


class NoExclamationMark(VoiceRule):
    """§8: 'Never performative. No exclamation points.'"""

    name = "no_exclamation_mark"
    severity = Severity.REJECT

    def check(self, text: str, context: RuleContext | None = None) -> list[Violation]:
        plain = strip_html(text)
        if "!" in plain:
            return [
                Violation(
                    rule=self.name,
                    severity=self.severity,
                    message="Exclamation mark found. Voice is serious; remove it.",
                    offending_text="!",
                )
            ]
        return []


# Marketing phrases — exact-word list from the build plan. Word
# boundaries applied so e.g. 'unpackaged' isn't flagged.
_MARKETING_PHRASES: tuple[str, ...] = (
    "exciting",
    "amazing",
    "insights",  # the forbidden word
    "unpack",
    "dive into",
    "quick heads up",
    "heads up",
    "just a quick",
    "super excited",
    "thrilled to",
    "game changer",
    "game-changer",
    "game changing",
    "game-changing",
    "synergy",
    "move the needle",
)

# 'leverage' is allowed in architectural sense (noun/verb for
# structural use), so we only flag it when it occurs in marketing
# patterns like 'leverage our' or 'leverage the power of'.
_LEVERAGE_MARKETING = re.compile(
    r"\bleverage\s+(?:our|your|the\s+power|the\s+strength|ai|synerg)",
    re.IGNORECASE,
)


class NoMarketingLanguage(VoiceRule):
    """§8: 'Never performative. No manufactured warmth, no exciting news.'"""

    name = "no_marketing_language"
    severity = Severity.REJECT

    _patterns: tuple[re.Pattern[str], ...] = tuple(
        re.compile(rf"\b{re.escape(p)}\b", re.IGNORECASE) for p in _MARKETING_PHRASES
    )

    def check(self, text: str, context: RuleContext | None = None) -> list[Violation]:
        plain = strip_html(text)
        violations: list[Violation] = []
        for pat in self._patterns:
            m = pat.search(plain)
            if m:
                violations.append(
                    Violation(
                        rule=self.name,
                        severity=self.severity,
                        message=(
                            f"Marketing phrase {m.group(0)!r} found. "
                            "Voice is operational, not promotional."
                        ),
                        offending_text=m.group(0),
                    )
                )
        m = _LEVERAGE_MARKETING.search(plain)
        if m:
            violations.append(
                Violation(
                    rule=self.name,
                    severity=self.severity,
                    message=(
                        "'leverage' used in marketing sense. Allowed only in "
                        "architectural use (e.g., 'leverages the Models spine')."
                    ),
                    offending_text=m.group(0),
                )
            )
        return violations


# Emoji ranges — broad pictographic coverage without blocking normal punctuation.
_EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001F5FF"   # symbols & pictographs
    "\U0001F600-\U0001F64F"   # emoticons
    "\U0001F680-\U0001F6FF"   # transport & map
    "\U0001F700-\U0001F77F"   # alchemical
    "\U0001F780-\U0001F7FF"   # geometric shapes ext
    "\U0001F800-\U0001F8FF"   # supplemental arrows
    "\U0001F900-\U0001F9FF"   # supplemental symbols & pictographs
    "\U0001FA00-\U0001FA6F"   # chess / other
    "\U0001FA70-\U0001FAFF"   # symbols & pictographs ext-A
    "\U00002600-\U000026FF"   # misc symbols
    "\U00002700-\U000027BF"   # dingbats
    "\U0001F1E6-\U0001F1FF"   # flags
    "]",
    flags=re.UNICODE,
)


class NoEmoji(VoiceRule):
    """§8: 'no emoji, no mascot'."""

    name = "no_emoji"
    severity = Severity.REJECT

    def check(self, text: str, context: RuleContext | None = None) -> list[Violation]:
        plain = strip_html(text)
        m = _EMOJI_RE.search(plain)
        if m:
            return [
                Violation(
                    rule=self.name,
                    severity=self.severity,
                    message="Emoji found. Voice is text-only.",
                    offending_text=m.group(0),
                )
            ]
        return []


# Simple sentence splitter: split on . ! ? followed by whitespace/end.
# Good enough for prose-quality checks; not aiming at academic parsing.
_SENTENCE_SPLIT = re.compile(r"(?<=[\.!?])\s+(?=[A-Z0-9\"'\(])")


def _sentences(plain: str) -> list[str]:
    plain = plain.strip()
    if not plain:
        return []
    parts = _SENTENCE_SPLIT.split(plain)
    return [p.strip() for p in parts if p.strip()]


class SentenceLengthLimit(VoiceRule):
    """§8 'Economic. Fewer words. Every sentence earns its length.'

    Flag sentences over 35 words — does not reject. Voice degrades
    gracefully for a single long sentence; degrades badly for a
    marketing-run-on.
    """

    name = "sentence_length_limit"
    severity = Severity.FLAG
    max_words: int = 35

    def check(self, text: str, context: RuleContext | None = None) -> list[Violation]:
        plain = strip_html(text)
        violations: list[Violation] = []
        for s in _sentences(plain):
            word_count = len(re.findall(r"\b[\w'\-]+\b", s))
            if word_count > self.max_words:
                violations.append(
                    Violation(
                        rule=self.name,
                        severity=self.severity,
                        message=(
                            f"Sentence is {word_count} words (> {self.max_words}). "
                            "Tighten."
                        ),
                        offending_text=s[:200],
                    )
                )
        return violations


# Regexes used by RequiresSpecificity.
_NUMBER_RE = re.compile(r"\b\d[\d,.\-–]*\b")
_MONEY_RE = re.compile(r"\$\s*\d|\bUSD\b|\bEUR\b|\bGBP\b|[€£¥]")
_DATE_RE = re.compile(
    r"\b("
    r"Mon|Tue|Wed|Thu|Fri|Sat|Sun|"
    r"Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|"
    r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|"
    r"January|February|March|April|June|July|August|September|October|"
    r"November|December|yesterday|today|tomorrow|this\s+week|next\s+week"
    r")\b",
    re.IGNORECASE,
)
# Proper-noun heuristic: a capitalized word appearing mid-sentence
# (preceded by a lowercase alphanumeric word and a space). This avoids
# false positives on sentence openers ("Things are behind" should NOT
# count as a concrete reference; "Acme" / "Monica" mid-sentence does).
_PROPER_NOUN_RE = re.compile(r"[a-z0-9][\s,]+([A-Z][a-z]{2,})")
_CITE_RE = re.compile(r"\b[a-z]-\d{2,}\b|\bm-\d{2,}\b|\bc-\d{2,}\b|\bobs-\d{2,}\b")


class RequiresSpecificity(VoiceRule):
    """§8: 'Specific to this company. References actual people, actual
    commitments, actual Models. Never generic.'

    For card bodies, require at least one concrete reference: a
    number, a money amount, a date, a cite code, or a capitalized
    name. Generic 'engineering is behind' language fails this.
    """

    name = "requires_specificity"
    severity = Severity.REJECT
    # Fires only for these kinds.
    _applies_to: frozenset[str] = frozenset({
        "card_observation",
        "card_decision",
        "card_question",
    })

    def check(self, text: str, context: RuleContext | None = None) -> list[Violation]:
        if context is None or context.kind not in self._applies_to:
            return []
        plain = strip_html(text)
        has_number = bool(_NUMBER_RE.search(plain))
        has_money = bool(_MONEY_RE.search(plain))
        has_date = bool(_DATE_RE.search(plain))
        has_cite = bool(_CITE_RE.search(plain))
        has_proper_noun = bool(_PROPER_NOUN_RE.search(" " + plain))
        if not (has_number or has_money or has_date or has_cite or has_proper_noun):
            return [
                Violation(
                    rule=self.name,
                    severity=self.severity,
                    message=(
                        "Card body lacks a concrete reference (name, number, "
                        "date, or cite). Voice must be specific to this "
                        "company, not generic."
                    ),
                    offending_text=plain[:200],
                )
            ]
        return []


# Hedge preambles — phrases that signal a rambling opener.
_HEDGE_PHRASES: tuple[str, ...] = (
    "I just wanted to",
    "I just want to",
    "I'd like to highlight",
    "I would like to highlight",
    "I just wanted to flag",
    "Just a quick note",
    "FYI,",
    "FYI:",
    "For your information",
    "I thought I'd",
    "Wanted to touch base",
    "Hope this finds you well",
    "Reaching out to",
    "As per our last",
    "Circling back",
)


class NoHedgePadding(VoiceRule):
    """§8: 'Direct. No preamble. No "I'd like to highlight that..."'."""

    name = "no_hedge_padding"
    severity = Severity.REJECT

    _patterns: tuple[re.Pattern[str], ...] = tuple(
        re.compile(rf"\b{re.escape(p)}", re.IGNORECASE) for p in _HEDGE_PHRASES
    )

    def check(self, text: str, context: RuleContext | None = None) -> list[Violation]:
        plain = strip_html(text)
        violations: list[Violation] = []
        for pat in self._patterns:
            m = pat.search(plain)
            if m:
                violations.append(
                    Violation(
                        rule=self.name,
                        severity=self.severity,
                        message=(
                            f"Hedge preamble {m.group(0)!r} found. "
                            "Say the thing directly; no preamble."
                        ),
                        offending_text=m.group(0),
                    )
                )
        return violations


# ---------------------------------------------------------------------
# Ordered rule list. Order matters only for deterministic violation
# reporting; the reject/flag distinction does the work.
# ---------------------------------------------------------------------


RULES: tuple[VoiceRule, ...] = (
    NoExclamationMark(),
    NoMarketingLanguage(),
    NoEmoji(),
    SentenceLengthLimit(),
    RequiresSpecificity(),
    NoHedgePadding(),
)


def check_all(
    text: str,
    context: RuleContext | None = None,
    rules: Iterable[VoiceRule] | None = None,
) -> list[Violation]:
    """Run every rule and aggregate the violations.

    Caller decides what to do with reject vs flag severities.
    """
    active = tuple(rules) if rules is not None else RULES
    out: list[Violation] = []
    for rule in active:
        out.extend(rule.check(text, context))
    return out


def has_rejections(violations: list[Violation]) -> bool:
    return any(v.severity is Severity.REJECT for v in violations)


def format_corrections(violations: list[Violation]) -> str:
    """Turn violations into a compact correction prompt for the retry.

    Voice: the correction instructions themselves are in the same
    register — terse, direct, specific.
    """
    if not violations:
        return ""
    lines = ["Your prior output failed voice checks. Fix these specifically:"]
    # Deduplicate by (rule, offending_text) so the retry prompt stays short.
    seen: set[tuple[str, str | None]] = set()
    for v in violations:
        key = (v.rule, v.offending_text)
        if key in seen:
            continue
        seen.add(key)
        if v.offending_text:
            lines.append(f"- {v.rule}: {v.message} Offending: {v.offending_text!r}")
        else:
            lines.append(f"- {v.rule}: {v.message}")
    lines.append(
        "Rewrite. Keep the same structure and span classes. "
        "Remove the violations. Do not hedge."
    )
    return "\n".join(lines)


__all__ = [
    "RULES",
    "NoEmoji",
    "NoExclamationMark",
    "NoHedgePadding",
    "NoMarketingLanguage",
    "RequiresSpecificity",
    "RuleContext",
    "SentenceLengthLimit",
    "Severity",
    "Violation",
    "VoiceRule",
    "check_all",
    "format_corrections",
    "has_rejections",
    "strip_html",
]
