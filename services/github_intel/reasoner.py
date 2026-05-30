"""services/github_intel/reasoner.py — optional LLM causal reasoning.

Flag-gated (github_intel.llm_enabled). Reserved for non-obvious events; the rule
fast-path in fsm.py handles the bulk with no LLM call. Returns a reasoning dict
shaped like fsm.rule_reasoning, or None on any failure (callers fall back to the
rule path — a signal is never dropped).
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from services.github_intel.fsm import GithubEvent

# Process-cached provider so we don't rebuild per call. build_provider() raises
# without a configured key — that's caught by the caller's try/except.
_PROVIDER: Any | None = None


def _provider() -> Any:
    global _PROVIDER
    if _PROVIDER is None:
        from lib.llm.provider import LLMConfig, build_provider
        _PROVIDER = build_provider(LLMConfig.from_env())
    return _PROVIDER


class _AffectedEntity(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: str
    ref: str
    relation: str


class CausalExplanation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cause: str = Field(description="What action triggered this, in one sentence")
    effect: str = Field(description="What this action results in / changes")
    explanation: str = Field(description="Why this matters, 1-2 sentences")
    affected_entities: list[_AffectedEntity] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)


_SYSTEM = (
    "You are a GitHub repository intelligence engine. Given a GitHub action, the "
    "prior/next state of the affected entity, and the code blast radius (files and "
    "symbols that depend on the changed code), explain the CAUSE, the EFFECT (the "
    "state change it produces), and WHY it matters. Be precise and concrete; cite "
    "specific files/symbols from the blast radius when relevant."
)


async def llm_causal(
    ev: GithubEvent,
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    blast_radius: dict[str, Any],
    content: dict[str, Any],
) -> dict[str, Any] | None:
    try:
        provider = _provider()
    except Exception:  # noqa: BLE001 — no/invalid LLM config: caller uses rule path
        return None

    dep_files = (blast_radius or {}).get("dependent_files", [])[:10]
    dep_syms = (blast_radius or {}).get("dependent_symbols", [])[:10]
    user = (
        f"event_type={ev.event_type} action={ev.action} repo={ev.repo} "
        f"entity={ev.entity_kind} ref={ev.entity_ref} author={ev.author}\n"
        f"state_before={before}\nstate_after={after}\n"
        f"changed_files={(blast_radius or {}).get('changed_files', [])}\n"
        f"dependent_files={dep_files}\ndependent_symbols={dep_syms}\n"
        f"title={content.get('pr_title') or content.get('issue_title')}\n"
    )
    try:
        out: CausalExplanation = await provider.structured(
            system=_SYSTEM, user=user, schema=CausalExplanation,
            temperature=0.1, max_tokens=600,
        )
    except Exception:  # noqa: BLE001 — transient/parse failure: caller uses rule path
        return None

    label = "none"
    for k in ("lifecycle", "status", "head_sha"):
        if k in before or k in after:
            label = f"{before.get(k)}->{after.get(k)}"
            break
    return {
        "cause": out.cause, "effect": out.effect, "explanation": out.explanation,
        "state_change": label, "confidence": out.confidence, "reasoning_path": "llm",
    }
