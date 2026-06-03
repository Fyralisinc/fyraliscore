"""services/reasoning/think/quality_gate.py — model-quality scoring + verdict gate.

Final gate between reconciliation and insertion. Scores proposed Models on
three NEW dimensions that are NOT covered by `validator.py` (which already
handles proposition shape, falsifier adequacy, scope actor validation) or
`reconciler.py` (which already handles dedupe via cosine similarity).

The three dimensions:

    - atomicity   — did the splitter (if it ran) successfully produce a
                    single-claim op?
    - durability  — is the claim about something likely to remain true for
                    long enough to be worth remembering as a Model, or is
                    it an ephemeral observation/sentiment that should live
                    as evidence instead?
    - kind_fit    — does the declared proposition.kind actually match the
                    shape of the claim text? (e.g. kind=state but text says
                    "we should..." is a recommendation, not a state)

All scoring is pure-Python heuristic; no LLM calls.

Integration order in services/reasoning/think/applier.py:_apply_diff():

    all_ops = [...from splitter...]
    for op in all_ops:
        rec_result = reconcile_claim_op(op, conn, ...)
        if rec_result.replacement_op:  # auto_merge converted to update
            op = rec_result.replacement_op
            # quality gate doesn't run on confidence-update ops
        else:
            verdict = score_quality(op, QualityContext(reconcile_result=rec_result, ...))
            op, side_ops = apply_verdict(op, verdict)
            if op is None:
                continue  # rejected or downgraded
        _apply_claim_op(op, conn, ...)
        for side in side_ops:
            _apply_claim_op(side, conn, ...)
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import UUID

from lib.shared.memory_grammar import derive_memory_grammar
from services.reasoning.think.diff_schema import ClaimOp

# Reconciler ReconcileResult is imported as a forward type only. We avoid
# the runtime import to keep quality_gate independently testable without
# pulling in the reconciler's DB-bound dependencies.
try:  # pragma: no cover - only used for type hints
    from services.reasoning.think.reconciler import ReconcileResult  # type: ignore
except Exception:  # pragma: no cover - defensive
    ReconcileResult = Any  # type: ignore


# =====================================================================
# Splitter compatibility — `is_compound` fallback.
# =====================================================================


try:
    from services.reasoning.think.splitter import is_compound as _splitter_is_compound  # type: ignore
except Exception:  # splitter module not yet in this worktree
    _splitter_is_compound = None  # type: ignore[assignment]


_COMPOUND_CONNECTORS = re.compile(
    r"\b(?:and also|as well as|;|,\s*and\b|,\s*but\b|,\s*so\b|"
    r"\bin addition\b|\bfurthermore\b|\bmoreover\b)",
    re.IGNORECASE,
)
_BULLET_MARKERS = re.compile(r"(^|\n)\s*(?:[-*•]|\d+[.)])\s+")
_MULTI_SENTENCE = re.compile(r"[.!?]\s+[A-Z]")


def _is_compound_text(text: str) -> bool:
    """Fallback compound-claim detector used when splitter.is_compound is
    not available. Conservative: returns True only on strong signals.
    """
    if not text:
        return False
    if _BULLET_MARKERS.search(text):
        return True
    # Multi-sentence text is usually compound.
    if _MULTI_SENTENCE.search(text):
        return True
    # Common connectors joining two independent clauses.
    if _COMPOUND_CONNECTORS.search(text):
        # Also require >1 verb-y / clause-y signal — count "and" + comma.
        return text.count(",") >= 1 or text.lower().count(" and ") >= 1
    return False


def is_compound(text_or_entry: str | dict) -> bool:
    """Public wrapper — defers to splitter.is_compound if present.

    The Phase-2 splitter signature is `is_compound(entry: dict) -> (bool, [reasons])`,
    so we adapt accordingly. When called with a plain string we wrap it
    into a minimal entry shape so the splitter's text extractor sees it.
    """
    # Pull a text view for the local bullet/sentence fallback regardless
    # of whether splitter fires — splitter is conjunction-based and won't
    # catch newline-bullet or multi-sentence packing.
    if isinstance(text_or_entry, dict):
        prop = text_or_entry.get("proposition") or {}
        text = prop.get("assertion") or prop.get("summary") or ""
        text = text if isinstance(text, str) else ""
    else:
        text = text_or_entry

    if _is_compound_text(text):
        return True

    if _splitter_is_compound is not None:
        try:
            entry: dict
            if isinstance(text_or_entry, str):
                entry = {"proposition": {"kind": "state", "assertion": text_or_entry}}
            else:
                entry = text_or_entry
            result = _splitter_is_compound(entry)
            if isinstance(result, tuple):
                return bool(result[0])
            return bool(result)
        except Exception:
            pass
    return False


# =====================================================================
# Public dataclasses
# =====================================================================


Decision = Literal["accept", "reject", "downgrade_to_evidence", "needs_review"]
DowngradeTarget = Literal["evidence", "concern_only", "pattern_instance"]


@dataclass
class QualityContext:
    """Per-op context the gate needs.

    The integrator should pass whatever it already has — `reconcile_result`
    is informational only (the gate currently does not consult it directly,
    but having it on the dataclass keeps a stable signature when future
    rules want to e.g. soften scoring on near-duplicate candidates).
    """

    tenant_id: UUID
    reconcile_result: "ReconcileResult | None" = None
    trigger_kind: str | None = None  # T1/T2/T3/T4 etc


@dataclass
class QualityVerdict:
    decision: Decision
    atomicity_score: float
    durability_score: float
    kind_fit_score: float
    overall_score: float
    rejection_reasons: list[str] = field(default_factory=list)
    downgrade_target: DowngradeTarget | None = None


# =====================================================================
# Heuristic vocabularies
# =====================================================================


# "Structural" properties — process / system / contract / relationship /
# capability. Used by the durability heuristic.
_STRUCTURAL_TERMS = re.compile(
    r"\b("
    r"process|workflow|policy|procedure|playbook|system|platform|"
    r"contract|agreement|sla|relationship|partnership|account|"
    r"capability|capacity|infrastructure|architecture|"
    r"team\s+structure|org\s+chart|reporting\s+line|"
    r"pricing|tier|plan|integration|api|sdk"
    r")\b",
    re.IGNORECASE,
)

# Timeframe phrases that point at least ~1 week ahead.
_FAR_TIMEFRAME = re.compile(
    r"\b("
    r"next\s+(?:week|month|quarter|year|q[1-4])|"
    r"in\s+(?:\d+|a|one|two|three|four|several|many)\s+"
    r"(?:weeks?|months?|quarters?|years?)|"
    r"by\s+(?:end\s+of\s+)?(?:q[1-4]|\d{4}|next\s+\w+)|"
    r"over\s+the\s+next\s+(?:few\s+)?(?:weeks?|months?|quarters?)"
    r")\b",
    re.IGNORECASE,
)

# Ephemeral / point-in-time references that argue against durability.
_EPHEMERAL_TERMS = re.compile(
    r"\b("
    r"yesterday|this\s+morning|this\s+afternoon|this\s+evening|"
    r"earlier\s+today|just\s+now|a\s+moment\s+ago|"
    r"yesterday'?s\s+(?:call|meeting|sync|standup|email|message)|"
    r"today'?s\s+(?:call|meeting|sync|standup|email|message)|"
    r"the\s+last\s+(?:call|meeting|sync|standup|email|message)"
    r")\b",
    re.IGNORECASE,
)

# Pure-sentiment markers without observable consequence (we look for an
# absence of an "observable consequence" clause separately).
_SENTIMENT_TERMS = re.compile(
    r"\b("
    r"frustrated|annoyed|upset|happy|sad|excited|anxious|worried(?!\s+that\b)|"
    r"feels?\s+(?:stuck|lost|good|bad|great|terrible|frustrated|fine)|"
    r"feeling\s+(?:stuck|lost|good|bad|great|terrible|frustrated|fine)|"
    r"is\s+frustrated|is\s+annoyed|is\s+upset|is\s+excited"
    r")\b",
    re.IGNORECASE,
)
_OBSERVABLE_CONSEQUENCE = re.compile(
    r"\b("
    r"because|so that|leading to|results? in|caused?\s+by|drove|"
    r"missed\s+(?:deadline|target|sla)|churned|escalated|delayed|"
    r"blocked|stopped|paused|cancelled|signed|renewed|expanded"
    r")\b",
    re.IGNORECASE,
)

# Falsifier types that require external evidence vs. internal opinion.
_EXTERNAL_FALSIFIER = re.compile(
    r"\b("
    r"observe|observed|measurement|metric|threshold|"
    r"customer|user|partner|vendor|invoice|contract|"
    r"report|dashboard|signal|event|log|email|message|"
    r"renewal|churn|payment|delivery|deadline"
    r")\b",
    re.IGNORECASE,
)
_INTERNAL_OPINION_FALSIFIER = re.compile(
    r"\b(opinion|think|believe|feel|gut|sense\s+that|hunch)\b",
    re.IGNORECASE,
)

# Action verb markers — used both by the "state vs action" downgrade and
# by the recommendation kind_fit check.
_ACTION_VERBS = re.compile(
    r"\b("
    r"we\s+should|we\s+need\s+to|we\s+must|we\s+have\s+to|"
    r"\w+\s+should|\w+\s+needs?\s+to|\w+\s+must|"
    r"let'?s|consider(?:ing)?\s+\w+ing|"
    r"propose|recommend|suggest|"
    r"build|ship|launch|deploy|hire|fire|stop|start|pause|kill|cut|"
    r"reduce|increase|scale|move|migrate|switch|adopt|sunset|retire"
    r")\b",
    re.IGNORECASE,
)

# Future-tense / conditional indicators for prediction kind.
_FUTURE_MARKERS = re.compile(
    r"\b("
    r"will|won'?t|going\s+to|gonna|"
    r"expect(?:s|ed)?\s+to|likely\s+to|projected\s+to|forecast|"
    r"if\s+\w+|when\s+\w+|by\s+(?:next|end\s+of|q[1-4]|\d{4})|"
    r"next\s+(?:week|month|quarter|year|q[1-4])|"
    r"might|may\s+\w+|could\s+\w+|should\s+\w+"
    r")\b",
    re.IGNORECASE,
)

# Concern / worry / risk markers.
_CONCERN_MARKERS = re.compile(
    r"\b("
    r"concern(?:ed|ing)?|worry|worried|risk|risky|fragile|"
    r"uncertain|unclear|ambiguous|exposed|vulnerable|threat|"
    r"could\s+go\s+wrong|might\s+fail|may\s+break|"
    r"don'?t\s+(?:know|trust)|not\s+sure"
    r")\b",
    re.IGNORECASE,
)

# Modal / non-factual markers that disqualify a "state" kind.
_NON_FACTUAL_MARKERS = re.compile(
    r"\b(should|will|won'?t|might|may|could|would|"
    r"need(?:s)?\s+to|have\s+to|must|going\s+to)\b",
    re.IGNORECASE,
)


# =====================================================================
# Text extraction helpers
# =====================================================================


def _entry_text(op: ClaimOp) -> str:
    """Best-effort flatten of the claim's free-text content. We only score
    `op=='insert'` ops; for other op kinds the caller should not invoke
    score_quality, but we defensively return an empty string."""
    entry = op.entry or {}
    prop = entry.get("proposition") or {}
    parts: list[str] = []
    for key in (
        "assertion",
        "summary",
        "situation",
        "relationship_summary",
        "hypothesis_text",
        "nature",
        "observed_tendency",
        "matched_context",
        "shared_mechanism",
        "judgment_change",
        "open_falsifier",
    ):
        v = prop.get(key)
        if isinstance(v, str):
            parts.append(v)
        elif isinstance(v, dict):
            # nested structured assertions — concatenate string values
            parts.extend(str(x) for x in v.values() if isinstance(x, str))
    for key in ("expected", "resolution", "subject", "object", "about", "assessment"):
        v = prop.get(key)
        if isinstance(v, str):
            parts.append(v)
    # falsifier text often lives at the entry level, not inside proposition
    for key in ("falsifier", "falsifier_description"):
        v = entry.get(key)
        if isinstance(v, str):
            parts.append(v)
    return " ".join(p for p in parts if p).strip()


def _proposition_kind(op: ClaimOp) -> str | None:
    entry = op.entry or {}
    prop = entry.get("proposition") or {}
    k = prop.get("kind")
    return k if isinstance(k, str) else None


def _falsifier_text(op: ClaimOp) -> str:
    entry = op.entry or {}
    parts: list[str] = []
    for key in ("falsifier", "falsifier_description", "open_falsifier"):
        v = entry.get(key)
        if isinstance(v, str):
            parts.append(v)
    prop = entry.get("proposition") or {}
    v = prop.get("open_falsifier")
    if isinstance(v, str):
        parts.append(v)
    return " ".join(parts)


def _evaluate_at(op: ClaimOp) -> Any:
    entry = op.entry or {}
    return entry.get("evaluate_at") or (entry.get("proposition") or {}).get(
        "evaluate_at"
    )


# =====================================================================
# Dimension scoring
# =====================================================================


def _score_atomicity(op: ClaimOp) -> tuple[float, list[str]]:
    """1.0 unless the claim text still looks compound after splitter ran."""
    reasons: list[str] = []
    entry = op.entry or {}
    text = _entry_text(op)
    if not text:
        return 1.0, reasons
    # Prefer the entry-shaped call so the Phase-2 splitter has access to
    # scope.entities (multi_entity reason) in addition to the text.
    compound = is_compound(entry) if entry else is_compound(text)
    if compound:
        reasons.append(
            "atomicity: claim text still looks compound after splitter "
            "(multi-conjunction / multi-kind / multi-entity signal)"
        )
        return 0.4, reasons
    return 1.0, reasons


def _score_durability(op: ClaimOp) -> tuple[float, list[str]]:
    """Heuristic durability score in [0, 1]."""
    reasons: list[str] = []
    text = _entry_text(op)
    if not text:
        # No text to assess. Lean conservative — not durable.
        reasons.append("durability: no scorable text in claim")
        return 0.3, reasons

    score = 0.4  # base
    pos_signals: list[str] = []
    neg_signals: list[str] = []

    if _STRUCTURAL_TERMS.search(text):
        score += 0.3
        pos_signals.append("structural-property reference")
    if _FAR_TIMEFRAME.search(text):
        score += 0.2
        pos_signals.append("forward timeframe ≥ 1 week")
    # external-evidence falsifier
    fals = _falsifier_text(op)
    if fals and _EXTERNAL_FALSIFIER.search(fals) and not _INTERNAL_OPINION_FALSIFIER.search(fals):
        score += 0.2
        pos_signals.append("falsifier cites external evidence")
    conf = (op.entry or {}).get("confidence_at_assertion")
    if isinstance(conf, (int, float)) and conf >= 0.6:
        score += 0.1
        pos_signals.append(f"confidence_at_assertion={conf:.2f} ≥ 0.6")

    # Negative signals.
    if _EPHEMERAL_TERMS.search(text):
        score -= 0.3
        neg_signals.append("references a single ephemeral event")
    if _SENTIMENT_TERMS.search(text) and not _OBSERVABLE_CONSEQUENCE.search(text):
        score -= 0.3
        neg_signals.append("pure sentiment without observable consequence")

    kind = _proposition_kind(op)
    if kind == "state" and _ACTION_VERBS.search(text):
        score -= 0.2
        neg_signals.append("kind=state but text describes an action")

    # Clamp.
    score = max(0.0, min(1.0, score))

    if score < 0.5:
        reason = (
            f"durability: low ({score:.2f}); "
            + ("; ".join(neg_signals) or "no positive signals matched")
        )
        reasons.append(reason)
    return score, reasons


def _score_kind_fit(op: ClaimOp) -> tuple[float, list[str]]:
    """Check declared proposition.kind matches the claim shape."""
    reasons: list[str] = []
    entry = op.entry or {}
    prop = entry.get("proposition") or {}
    kind = prop.get("kind")
    grammar = derive_memory_grammar(
        prop if isinstance(prop, dict) else {},
        natural=str(entry.get("natural") or ""),
        scope_entities=entry.get("scope_entities") or [],
    )
    text = _entry_text(op)

    if not isinstance(kind, str) or not kind:
        reasons.append("kind_fit: proposition.kind missing")
        return 0.0, reasons

    if kind in {"state", "belief"} and grammar.claim_role == "fact":
        # Present-tense factual claim. If text uses should/will/might it
        # is really an action / prediction.
        if text and _NON_FACTUAL_MARKERS.search(text):
            reasons.append(
                "kind_fit: factual belief uses non-factual markers "
                "(should/will/might/need to)"
            )
            return 0.4, reasons
        return 1.0, reasons

    if kind == "prediction":
        if text and not _FUTURE_MARKERS.search(text):
            # also check for evaluate_at as a structural future indicator
            if not _evaluate_at(op):
                reasons.append(
                    "kind_fit: kind=prediction but no future/conditional "
                    "marker and no evaluate_at set"
                )
                return 0.4, reasons
        return 1.0, reasons

    if kind == "concern" or grammar.claim_role == "concern":
        if text and not _CONCERN_MARKERS.search(text):
            reasons.append(
                "kind_fit: concern claim reads as pure factual "
                "(no worry/risk/uncertainty marker)"
            )
            return 0.5, reasons
        return 1.0, reasons

    if kind == "recommendation" or grammar.claim_role == "recommendation":
        # Recommendations have a structured `proposed_change.operation`,
        # but the surfaced text should also contain an action verb.
        prop_change = prop.get("proposed_change") or {}
        has_op = isinstance(prop_change, dict) and bool(prop_change.get("operation"))
        if text and not _ACTION_VERBS.search(text) and not has_op:
            reasons.append(
                "kind_fit: recommendation claim is descriptive "
                "(no action verb) and proposed_change.operation missing"
            )
            return 0.3, reasons
        return 1.0, reasons

    if kind == "pattern_instance" or (
        grammar.claim_role == "pattern" and grammar.time_mode == "past"
    ):
        pid = prop.get("pattern_id")
        if not isinstance(pid, str) or not pid.strip():
            reasons.append(
                "kind_fit: kind=pattern_instance but pattern_id is missing"
            )
            return 0.0, reasons
        return 1.0, reasons

    if kind == "situation" or grammar.claim_role == "situation":
        members = prop.get("member_model_ids")
        # A splitter may emit a pending marker — e.g. entry["pending_split"]
        # or proposition["_pending_members"] — recognise either.
        pending = bool(
            entry.get("pending_split")
            or entry.get("members_pending")
            or prop.get("_pending_members")
            or prop.get("members_pending")
        )
        if isinstance(members, list) and len(members) > 0:
            return 1.0, reasons
        if pending:
            reasons.append(
                "kind_fit: situation has no member_model_ids yet "
                "(pending splitter marker present)"
            )
            return 0.5, reasons
        reasons.append(
            "kind_fit: situation has empty member_model_ids and no "
            "pending splitter marker"
        )
        return 0.0, reasons

    # All other kinds — no strict shape check.
    return 1.0, reasons


# =====================================================================
# Public entry points
# =====================================================================


_WEIGHT_ATOMICITY = 0.4
_WEIGHT_DURABILITY = 0.3
_WEIGHT_KIND_FIT = 0.3


def score_quality(op: ClaimOp, context: QualityContext) -> QualityVerdict:
    """Score a ClaimOp and return a verdict.

    Only insert ops are scored; updates/archives short-circuit to accept.
    """
    if op.op != "insert":
        return QualityVerdict(
            decision="accept",
            atomicity_score=1.0,
            durability_score=1.0,
            kind_fit_score=1.0,
            overall_score=1.0,
            rejection_reasons=[],
            downgrade_target=None,
        )

    atomicity, a_reasons = _score_atomicity(op)
    durability, d_reasons = _score_durability(op)
    kind_fit, k_reasons = _score_kind_fit(op)

    overall = (
        _WEIGHT_ATOMICITY * atomicity
        + _WEIGHT_DURABILITY * durability
        + _WEIGHT_KIND_FIT * kind_fit
    )

    individual = (atomicity, durability, kind_fit)

    reasons = list(a_reasons) + list(d_reasons) + list(k_reasons)

    # Decision rules — order matters. Durability-specific downgrade
    # comes BEFORE hard reject so that low-durability-but-coherent
    # claims (e.g. one-off observations) flow to the evidence path
    # instead of being silently dropped. Atomicity guard prevents
    # downgrading compound junk.
    if durability < 0.3 and kind_fit >= 0.5 and atomicity >= 0.3:
        downgrade_reasons = list(reasons)
        downgrade_reasons.append(
            "downgrade: durability < 0.3 → emit as evidence attachment "
            "instead of a Model (integrator: evidence path not yet wired; "
            "side-op currently no-op)"
        )
        return QualityVerdict(
            decision="downgrade_to_evidence",
            atomicity_score=atomicity,
            durability_score=durability,
            kind_fit_score=kind_fit,
            overall_score=overall,
            rejection_reasons=downgrade_reasons,
            downgrade_target="evidence",
        )

    # Hard reject — failed downgrade conditions AND overall is too low
    # OR any single dimension is catastrophically bad.
    if overall < 0.45 or any(s < 0.2 for s in individual):
        return QualityVerdict(
            decision="reject",
            atomicity_score=atomicity,
            durability_score=durability,
            kind_fit_score=kind_fit,
            overall_score=overall,
            rejection_reasons=reasons or [f"overall_score={overall:.2f} below 0.45"],
            downgrade_target=None,
        )

    if 0.45 <= overall < 0.6:
        return QualityVerdict(
            decision="needs_review",
            atomicity_score=atomicity,
            durability_score=durability,
            kind_fit_score=kind_fit,
            overall_score=overall,
            rejection_reasons=reasons
            or [f"overall_score={overall:.2f} in review band [0.45, 0.6)"],
            downgrade_target=None,
        )

    # Accept path requires all three individual scores >= 0.3.
    if overall >= 0.6 and all(s >= 0.3 for s in individual):
        return QualityVerdict(
            decision="accept",
            atomicity_score=atomicity,
            durability_score=durability,
            kind_fit_score=kind_fit,
            overall_score=overall,
            rejection_reasons=[],
            downgrade_target=None,
        )

    # Fallback (shouldn't reach in practice): treat as needs_review with
    # an explanatory note rather than silently dropping.
    return QualityVerdict(
        decision="needs_review",
        atomicity_score=atomicity,
        durability_score=durability,
        kind_fit_score=kind_fit,
        overall_score=overall,
        rejection_reasons=reasons
        or [
            f"fallback: overall_score={overall:.2f} did not match any decision rule"
        ],
        downgrade_target=None,
    )


def apply_verdict(
    op: ClaimOp, verdict: QualityVerdict
) -> tuple[ClaimOp | None, list[ClaimOp]]:
    """Translate a verdict into (insert_op_or_None, side_ops).

    - accept                 → (op, [])
    - reject                 → (None, []) — caller drops the op
    - downgrade_to_evidence  → (None, []) — applier attaches the evidence
                                to an existing Model anchor when possible;
                                rejection_reasons carries the rationale
    - needs_review           → (op, []) — caller inspects verdict.decision
                                to queue review
    """
    decision = verdict.decision
    if decision == "accept":
        return op, []
    if decision == "reject":
        return None, []
    if decision == "downgrade_to_evidence":
        # Evidence emission is the applier's responsibility. The gate
        # returns no side-op because the applier needs DB state to select
        # the existing Model anchor.
        return None, []
    if decision == "needs_review":
        return op, []
    # Future decisions: be permissive — let it through with no side-ops.
    return op, []


__all__ = [
    "Decision",
    "DowngradeTarget",
    "QualityContext",
    "QualityVerdict",
    "apply_verdict",
    "is_compound",
    "score_quality",
]
