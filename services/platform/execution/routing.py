"""Deterministic signal routing gate.

The first implementation is intentionally cheap and conservative. It
records shadow decisions for every new ingested observation while the
existing T1 Think enqueue remains unchanged.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any
from uuid import UUID

import asyncpg

from .contracts import DecisionStatus, RoutingDecision, SignalEnvelope, SignalRoute


_TRUE = {"1", "true", "yes", "on", "y", "t"}

_HIGH_TRUST = {
    "authoritative",
    "authoritative_external",
    "attested_agent",
}
_LOW_TRUST = {
    "inferential",
    "inferential_external",
    "unvetted",
}

_RISK_RE = re.compile(
    r"\b("
    r"blocked|blocker|blocking|cannot|can't|unable|stalled|stuck|"
    r"dependency|critical path|required|risk|risky|churn|escalat(?:e|ed|ion)|"
    r"outage|incident|breach|failed|failure|slip(?:ped)?|delay(?:ed)?|"
    r"overdue|unresolved|urgent"
    r")\b",
    re.I,
)

_COMMITMENT_RE = re.compile(
    r"\b("
    r"promised|committed|commitment|go-live|launch|deadline|due|deliver|"
    r"ship|started|working on|picked up|kicking off|done|merged|closed"
    r")\b",
    re.I,
)

_DECISION_RE = re.compile(
    r"\b("
    r"decision|decided|revisit|approved|rejected|aligned|agreed|"
    r"owner|ownership|who owns|unclear"
    r")\b",
    re.I,
)

_HUMAN_VALIDATION_RE = re.compile(
    r"\b("
    r"offline alignment|offline decision|no recorded decision|unrecorded|"
    r"backchannel|verbal agreement|informal agreement|unclear owner|"
    r"who owns|did we agree|sensitive intent"
    r")\b",
    re.I,
)

_SENSITIVE_RE = re.compile(
    r"\b(password|secret|token|api key|ssn|social security|private key)\b",
    re.I,
)

_LIGHTWEIGHT = {
    "ack",
    "got it",
    "hi",
    "hello",
    "lol",
    "ok",
    "okay",
    "thanks",
    "thank you",
    "+1",
}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUE


def should_record_routing_decisions() -> bool:
    """Return True when the routing gate should persist decisions."""
    return _env_bool("EXECUTION_ROUTING_SHADOW", True) or _env_bool(
        "EXECUTION_ROUTING_ENABLED",
        False,
    )


def routing_decision_status_from_env() -> DecisionStatus:
    """Map rollout flags to the persisted decision status."""
    enabled = _env_bool("EXECUTION_ROUTING_ENABLED", False)
    shadow = _env_bool("EXECUTION_ROUTING_SHADOW", True)
    if enabled and not shadow:
        return "enforced"
    return "shadow"


def decide_route(signal: SignalEnvelope) -> RoutingDecision:
    """Return the route this signal deserves.

    This gate does not call an LLM or run broad retrieval. It is meant
    to be stable enough for route-matrix tests and cheap enough for the
    hot ingestion path.
    """
    text = " ".join(
        [
            signal.summary or "",
            json.dumps(signal.content or {}, default=str),
            signal.signal_type or "",
            signal.observation_kind or "",
            signal.source_channel or "",
        ]
    )
    lowered_summary = (signal.summary or "").strip().casefold()
    has_entities = bool(signal.explicit_entities)
    trust = signal.trust_tier or "unknown"
    source = signal.source_channel or "unknown"
    kind = signal.observation_kind or "signal"

    risk_hit = bool(_RISK_RE.search(text))
    commitment_hit = bool(_COMMITMENT_RE.search(text))
    decision_hit = bool(_DECISION_RE.search(text))
    human_hit = bool(_HUMAN_VALIDATION_RE.search(text))
    sensitive = "sensitive" if _SENSITIVE_RE.search(text) else "normal"

    score_breakdown = {
        "trust": _trust_score(trust),
        "entity_overlap": 0.24 if has_entities else 0.0,
        "signal_kind": _kind_score(kind),
        "risk_language": 0.28 if risk_hit else 0.0,
        "commitment_language": 0.16 if commitment_hit else 0.0,
        "decision_language": 0.12 if decision_hit else 0.0,
        "source_importance": _source_score(source),
        "sensitivity_penalty": -0.08 if sensitive != "normal" else 0.0,
    }
    score = max(0.0, min(1.0, sum(score_breakdown.values())))
    risk_level = _risk_level(score, risk_hit=risk_hit, human_hit=human_hit)

    if _is_user_query(signal):
        route: SignalRoute = "FAST_PATH"
        reason = "explicit user/query signal should serve a read-only fast path"
    elif kind == "anomaly_flagged" or source == "internal:anomaly":
        route = "BACKGROUND_PATH"
        reason = "anomaly signals should run through background analysis first"
    elif human_hit and (has_entities or risk_hit or decision_hit):
        route = "HUMAN_VALIDATION_PATH"
        reason = "signal appears to require a human-resolvable missing fact"
    elif kind in {"state_change", "prediction_resolution"} and trust in _HIGH_TRUST:
        route = "DETERMINISTIC_UPDATE"
        reason = "authoritative state signal is a candidate for mechanical update"
    elif _is_low_value_chatter(lowered_summary, has_entities=has_entities):
        route = "IGNORE_OR_ARCHIVE"
        reason = "low-value chatter with no durable entity or risk signal"
    elif trust in _LOW_TRUST and score < 0.5:
        route = "IGNORE_OR_ARCHIVE"
        reason = "low-trust signal lacks enough materiality for inquiry"
    elif score >= 0.45:
        route = "DEEP_INQUIRY_PATH"
        reason = "signal has enough entity, trust, risk, or commitment weight"
    else:
        route = "IGNORE_OR_ARCHIVE"
        reason = "signal is below the materiality threshold for inquiry"

    return RoutingDecision(
        tenant_id=signal.tenant_id,
        signal_ref_type=signal.signal_ref_type,
        signal_ref_id=signal.signal_id,
        route=route,
        score=round(score, 4),
        score_breakdown={k: round(v, 4) for k, v in score_breakdown.items()},
        estimated_cost=_estimated_cost(route),
        reason=reason,
        risk_level=risk_level,
        sensitivity=sensitive,
    )


async def record_routing_decision(
    conn: asyncpg.Connection,
    decision: RoutingDecision,
    *,
    skip_if_missing: bool = True,
) -> None:
    """Persist a routing decision.

    `skip_if_missing` protects rolling deployments where new app code
    may briefly run before the migration has landed. The existence
    probe is safe inside the ingestion transaction and avoids raising
    `UndefinedTableError`, which would poison that transaction.
    """
    if skip_if_missing:
        table_name = await conn.fetchval(
            "SELECT to_regclass('public.signal_routing_decisions')"
        )
        if table_name is None:
            return

    await conn.execute(
        """
        INSERT INTO signal_routing_decisions (
          id, tenant_id, signal_ref_type, signal_ref_id,
          route, decision_status, score, score_breakdown,
          estimated_cost, risk_level, sensitivity, reason,
          enqueued_trigger_id
        ) VALUES (
          $1, $2, $3, $4,
          $5, $6, $7, $8::jsonb,
          $9::jsonb, $10, $11, $12,
          $13
        )
        """,
        decision.id,
        decision.tenant_id,
        decision.signal_ref_type,
        decision.signal_ref_id,
        decision.route,
        decision.decision_status,
        float(decision.score),
        json.dumps(decision.score_breakdown, default=str),
        json.dumps(decision.estimated_cost, default=str),
        decision.risk_level,
        decision.sensitivity,
        decision.reason,
        decision.enqueued_trigger_id,
    )


def _trust_score(trust: str) -> float:
    if trust == "authoritative":
        return 0.28
    if trust == "authoritative_external":
        return 0.24
    if trust == "attested_agent":
        return 0.2
    if trust == "reputable":
        return 0.14
    if trust in {"inferential", "inferential_external"}:
        return 0.08
    if trust == "unvetted":
        return 0.02
    return 0.05


def _kind_score(kind: str) -> float:
    if kind in {"state_change", "prediction_resolution"}:
        return 0.18
    if kind == "anomaly_flagged":
        return 0.22
    if kind == "contestation":
        return 0.2
    return 0.06


def _source_score(source: str) -> float:
    if source.startswith(("linear:", "github:", "stripe:", "calendar:")):
        return 0.12
    if source.startswith(("internal:", "ui:")):
        return 0.1
    if source.startswith(("email:", "slack:", "discord:")):
        return 0.05
    if source.startswith(("regulatory:", "market:", "analyst:")):
        return 0.08
    if source.startswith(("social:", "news:")):
        return 0.03
    return 0.02


def _risk_level(score: float, *, risk_hit: bool, human_hit: bool) -> str:
    if human_hit or score >= 0.75:
        return "high"
    if risk_hit or score >= 0.45:
        return "medium"
    return "low"


def _estimated_cost(route: SignalRoute) -> dict[str, Any]:
    if route == "IGNORE_OR_ARCHIVE":
        return {"class": "none", "llm_calls": 0, "expected_latency_ms": 0}
    if route == "DETERMINISTIC_UPDATE":
        return {"class": "db_only", "llm_calls": 0, "expected_latency_ms": 500}
    if route == "FAST_PATH":
        return {
            "class": "fast_retrieval",
            "llm_calls": "0-1 small",
            "expected_latency_ms": 2000,
        }
    if route == "BACKGROUND_PATH":
        return {"class": "async", "llm_calls": "budgeted", "expected_latency_ms": None}
    if route == "HUMAN_VALIDATION_PATH":
        return {
            "class": "human",
            "llm_calls": "0-1 small",
            "expected_latency_ms": None,
        }
    return {
        "class": "deep_inquiry",
        "llm_calls": "0-3 small + 1 frontier",
        "expected_latency_ms": 30000,
    }


def _is_user_query(signal: SignalEnvelope) -> bool:
    return signal.signal_ref_type == "query" or (
        signal.trigger_type or ""
    ).upper() == "USER_QUERY" or (signal.source_channel or "").startswith("ui:query")


def _is_low_value_chatter(text: str, *, has_entities: bool) -> bool:
    if has_entities:
        return False
    compact = re.sub(r"\s+", " ", text or "").strip()
    if not compact:
        return True
    if compact in _LIGHTWEIGHT:
        return True
    return len(compact) <= 18 and compact.rstrip(".!?") in _LIGHTWEIGHT


__all__ = [
    "decide_route",
    "record_routing_decision",
    "routing_decision_status_from_env",
    "should_record_routing_decisions",
]
