"""Provider-free structured-call adapter for CF2 mechanical evaluation.

The provider sees only the runtime prompt and requested output schema.  It does
not import fixture gold, sealed populations, or evaluator oracles.  Built-in
handlers are deliberately conservative; fixture-specific semantic decisions
must be supplied explicitly as request-only handlers by the future CF2 runner.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
import time
from typing import Any, Callable, Mapping

from lib.llm.provider import LLMConfig, LLMProvider


class UnsupportedCF2StructuredCall(RuntimeError):
    """Raised when CF2 has no explicit safe response for a requested schema."""


@dataclass(frozen=True, slots=True)
class CF2StructuredRequest:
    schema_name: str
    system: str
    user: str
    schema: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class CF2ProviderCall:
    ordinal: int
    schema_name: str
    input_tokens: int
    output_tokens: int
    elapsed_ms: float


CF2ResponseHandler = Callable[[CF2StructuredRequest], Mapping[str, Any]]


_ENVELOPE = re.compile(
    r"^(?P<surface>[A-Z][A-Za-z0-9-]+(?: [A-Za-z0-9-]+){1,3})"
    r"(?:,\s*update\s+\d+:|\s+(?:is|are|was|were)\b)",
)
_UUID = (
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)


def _token_estimate(*values: str) -> int:
    return max(1, (sum(len(value) for value in values) + 3) // 4)


def _schema_object(schema_hint: str) -> dict[str, Any]:
    try:
        value = json.loads(schema_hint)
    except json.JSONDecodeError as exc:
        raise UnsupportedCF2StructuredCall("requested schema hint is not JSON") from exc
    if not isinstance(value, dict):
        raise UnsupportedCF2StructuredCall("requested schema hint is not an object")
    return value


def _schema_name(schema: Mapping[str, Any]) -> str:
    value = schema.get("title")
    if isinstance(value, str) and value:
        return value
    properties = set((schema.get("properties") or {}).keys())
    definitions = set((schema.get("$defs") or {}).keys())
    if properties == {"ready"}:
        return "_DiscoveryPreflightResult"
    if properties == {"mentions"}:
        return "LearnedMentionBatch"
    if {"r", "d", "q"} <= properties:
        return "LLMCompactQuestionPlan"
    if {"rationale", "belief_deltas", "questions"} <= properties:
        return "LLMInquiryQuestionPlan"
    if {"decisions", "reasoning_trace"} <= properties:
        if "BatchMemoryCandidateDecision" in definitions:
            return "BatchMemoryDecisionSet"
        if "RelationshipCandidateDecision" in definitions:
            return "RelationshipCandidateDecisionSet"
    if {
        "candidate_id", "canonical_ref", "confidence", "reasoning",
        "decision_source", "resolution_scope",
    } <= properties:
        return "EntityResolution"
    if {"trigger_ref", "tenant_id", "claim_ops"} <= properties:
        return "RawDiff" if "edge_ops" in properties else "RawDiffClaimsOnly"
    raise UnsupportedCF2StructuredCall(
        "requested schema has no supported structural fingerprint"
    )


def _runtime_signals(user: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(user)
    except json.JSONDecodeError:
        return []
    signals = payload.get("signals") if isinstance(payload, dict) else None
    return [dict(item) for item in signals or () if isinstance(item, dict)]


def _entity_type(surface: str) -> str:
    suffix = surface.rsplit(" ", 1)[-1].casefold()
    return {
        "release": "project",
        "migration": "project",
        "handoff": "project",
        "renewal": "commitment",
        "approval": "decision",
        "ticket": "issue",
    }.get(suffix, "other")


def _mention_batch(request: CF2StructuredRequest) -> Mapping[str, Any]:
    mentions = []
    for signal in _runtime_signals(request.user):
        text = str(signal.get("content_text") or "")
        match = _ENVELOPE.search(text)
        if match is None:
            continue
        surface = match.group("surface")
        mentions.append({
            "signal_id": str(signal.get("signal_id") or ""),
            "surface": surface,
            "span_start": match.start("surface"),
            "span_end": match.end("surface"),
            "entity_type": _entity_type(surface),
            "confidence": 0.99,
            "abstain": False,
        })
    return {"mentions": mentions}


def _uuid_for(label: str, text: str) -> str | None:
    match = re.search(
        rf"[\"']?{re.escape(label)}[\"']?\s*[:=]\s*[\"']?(?P<id>{_UUID})",
        text,
        re.IGNORECASE,
    )
    return match.group("id") if match else None


def _empty_raw_diff(request: CF2StructuredRequest) -> Mapping[str, Any]:
    combined = f"{request.system}\n{request.user}"
    tenant_id = _uuid_for("tenant_id", combined)
    trigger_ref = _uuid_for("trigger_ref", combined) or _uuid_for(
        "trigger_id", combined
    )
    if tenant_id is None or trigger_ref is None:
        raise UnsupportedCF2StructuredCall(
            "RawDiff requires runtime tenant_id and trigger_ref/trigger_id"
        )
    return {
        "trigger_ref": trigger_ref,
        "tenant_id": tenant_id,
        "claim_ops": [],
        "reasoning_trace": "CF2 provider-free conservative no-op",
    }


_SAFE_DEFAULTS: dict[str, CF2ResponseHandler] = {
    "_DiscoveryPreflightResult": lambda _request: {"ready": True},
    "LearnedMentionBatch": _mention_batch,
    "LLMCompactQuestionPlan": lambda _request: {"r": "CF2 no inquiry", "d": [], "q": []},
    "LLMInquiryQuestionPlan": lambda _request: {
        "rationale": "CF2 no inquiry", "belief_deltas": [], "questions": [],
    },
    "BatchMemoryDecisionSet": lambda _request: {
        "decisions": [], "reasoning_trace": "CF2 conservative no-op",
    },
    "RelationshipCandidateDecisionSet": lambda _request: {
        "decisions": [], "reasoning_trace": "CF2 conservative no-op",
    },
    "EntityResolution": lambda _request: {
        "candidate_id": None,
        "canonical_ref": None,
        "confidence": 0.0,
        "reasoning": (
            "CF2 provider-free unresolved: no authoritative identity evidence"
        ),
        "decision_source": "cf2_provider_free_conservative_unresolved",
        "resolution_scope": "request_local_unresolved",
    },
    "RawDiff": _empty_raw_diff,
    "RawDiffClaimsOnly": _empty_raw_diff,
}


class CF2ProviderFreeLLM(LLMProvider):
    """Deterministic LLMProvider-compatible dispatcher for actual Think calls."""

    def __init__(
        self,
        *,
        handlers: Mapping[str, CF2ResponseHandler] | None = None,
        model: str = "cf2-provider-free-v1",
    ) -> None:
        super().__init__(LLMConfig(
            provider="cf2_provider_free", api_key="", model=model,
            timeout_s=5.0, max_retries=0,
        ))
        self._handlers = {**_SAFE_DEFAULTS, **dict(handlers or {})}
        self.calls: list[CF2ProviderCall] = []

    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        del temperature, max_tokens
        started = time.perf_counter()
        schema = _schema_object(schema_hint)
        schema_name = _schema_name(schema)
        handler = self._handlers.get(schema_name)
        if handler is None:
            raise UnsupportedCF2StructuredCall(
                f"unsupported CF2 structured schema: {schema_name}"
            )
        request = CF2StructuredRequest(schema_name, system, user, schema)
        raw = json.dumps(dict(handler(request)), sort_keys=True)
        input_tokens = _token_estimate(system, user, schema_hint)
        output_tokens = _token_estimate(raw)
        self._record_usage(
            input_tokens, output_tokens, usage_exactness="estimated"
        )
        self.calls.append(CF2ProviderCall(
            ordinal=len(self.calls) + 1,
            schema_name=schema_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            elapsed_ms=(time.perf_counter() - started) * 1000,
        ))
        return raw

    def telemetry(self) -> dict[str, Any]:
        return {
            "provider": self.config.provider,
            "model": self.config.model,
            "call_count": len(self.calls),
            "input_tokens": sum(call.input_tokens for call in self.calls),
            "output_tokens": sum(call.output_tokens for call in self.calls),
            "elapsed_ms": sum(call.elapsed_ms for call in self.calls),
            "calls": [
                {
                    "ordinal": call.ordinal,
                    "schema_name": call.schema_name,
                    "input_tokens": call.input_tokens,
                    "output_tokens": call.output_tokens,
                    "elapsed_ms": call.elapsed_ms,
                }
                for call in self.calls
            ],
            "proof_boundary": (
                "Estimated local tokens and in-process time; no provider cost or "
                "semantic-quality claim."
            ),
        }


__all__ = [
    "CF2ProviderCall",
    "CF2ProviderFreeLLM",
    "CF2ResponseHandler",
    "CF2StructuredRequest",
    "UnsupportedCF2StructuredCall",
]
