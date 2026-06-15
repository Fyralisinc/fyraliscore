"""Language predicates used by adaptive inquiry routing and relevance."""

from __future__ import annotations

import re


def signal_has_material_update_intent(lower: str) -> bool:
    scrubbed = scrub_negated_signal_language(lower)
    return (
        has_risk_language(scrubbed)
        or has_commitment_language(scrubbed)
        or has_act_affecting_language(scrubbed)
        or mentions_recurrence(scrubbed)
    )


def has_broad_signal_language(lower: str) -> bool:
    scrubbed = scrub_negated_signal_language(lower)
    broad_terms = bool(
        re.search(
            r"\b(all|portfolio|company-wide|team-wide|board|exec|"
            r"customers|renewals|fleet|global)\b",
            scrubbed,
        )
    )
    every_scope = bool(
        re.search(
            r"\bevery\s+(?:customer|account|renewal|team|segment|"
            r"pipeline|portfolio|region|department|business)\b",
            scrubbed,
        )
    )
    broad_across = bool(
        re.search(
            r"\bacross\s+(?:all\s+|the\s+)?(?:enterprise\s+)?"
            r"(?:customers|accounts|renewals|pipeline|portfolio|teams|"
            r"company|org|organization|business|segments)\b",
            scrubbed,
        )
    )
    return broad_terms or every_scope or broad_across


def scrub_negated_signal_language(lower: str) -> str:
    text = re.sub(
        r"\b(no|not|without)\b[^.;\n]{0,90}\b("
        r"blocker|blocked|blocking|risk|owner|decision|commitment|"
        r"commitments|customer|customers|delivery|deliver|launch|"
        r"incident|escalation|renewal|renewals"
        r")\w*",
        " ",
        lower,
    )
    text = re.sub(
        r"\bnot related to\b[^.;\n]{0,120}",
        " ",
        text,
    )
    return text


def has_risk_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(blocked|blocker|cannot|can't|unable|risk|churn|escalat|incident|"
            r"outage|breach|failed|failure|delay|slip|overdue|urgent|critical)\b",
            lower,
        )
    )


def has_dependency_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(depends?|dependency|critical path|binding constraint|"
            r"blocked by|tied to|requires?|reversed?|exception|policy|"
            r"approval depends|review depends)\b",
            lower,
        )
    )


def has_constraint_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(constraint|constrained|capacity|quota|scarce|limited|"
            r"bottleneck|blocked by|shortage|policy exception|approval|"
            r"resourc(?:e|ing)|sandbox|license|budget|rate limit)\b",
            lower,
        )
    )


def has_revenue_impact_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(revenue|renewal|churn|invoice|finance|pricing|arr|"
            r"sponsor|commercial|forecast|expansion)\b",
            lower,
        )
    )


def has_commitment_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(promised|committed|commitment|deadline|due|deliver|ship|launch|"
            r"go-live|owner|approved|decision|agreed)\b",
            lower,
        )
    )


def has_act_affecting_language(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(promised|committed|commitment|deadline|due|deliver|ship|launch|"
            r"go-live|owner|goal)\b",
            lower,
        )
    )


def mentions_recurrence(lower: str) -> bool:
    return bool(
        re.search(
            r"\b(recur(?:s|red|ring)?|repeated|again|several|multiple|pattern|systemic|"
            r"another|also|same issue|broader)\b",
            lower,
        )
    )


__all__ = [
    "has_act_affecting_language",
    "has_broad_signal_language",
    "has_commitment_language",
    "has_constraint_language",
    "has_dependency_language",
    "has_revenue_impact_language",
    "has_risk_language",
    "mentions_recurrence",
    "scrub_negated_signal_language",
    "signal_has_material_update_intent",
]
