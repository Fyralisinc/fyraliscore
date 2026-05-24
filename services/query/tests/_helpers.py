"""Test doubles shared across services/query tests."""
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

from lib.shared.types import ModelRow
from services.retrieval.assembler import AccessContext, ContextBundle
from services.retrieval.primary import RetrievalResult, TriggerContext

from services.query.adapters import (
    InMemoryCacheAdapter,
    RenderingAdapter,
    RenderRequest,
    RenderResponse,
)
from services.query.classifier import QueryClassifier
from services.query.strategies.base import StrategyContext, StrategyResult


# ---------------------------------------------------------------------
# ScriptedClassifier — returns a predetermined category per query
# ---------------------------------------------------------------------


class ScriptedClassifier:
    """Classifier double that returns a pre-scripted category per
    query. Bypasses the LLM entirely; used by handler tests."""

    def __init__(self, category: str = "arbitrary") -> None:
        self.category = category
        self.calls: list[tuple[str, bool]] = []

    async def classify(
        self,
        tenant_id: UUID,
        query: str,
        *,
        has_card_context: bool = False,
    ):
        from services.query.classifier import ClassificationResult
        self.calls.append((query, has_card_context))
        return ClassificationResult(
            category=self.category,  # type: ignore[arg-type]
            source="test",
            confidence=1.0,
            trace={},
        )


# ---------------------------------------------------------------------
# ScriptedProvider — drop-in replacement for the classifier's LLMProvider
# ---------------------------------------------------------------------


class ScriptedLLMProvider:
    """A tiny LLMProvider double. `answer_category` is returned as the
    `category` field in the parsed output."""

    def __init__(self, answer_category: str = "arbitrary") -> None:
        self.answer_category = answer_category
        self.calls: list[dict[str, Any]] = []
        # LLMProvider exposes .config; QueryClassifier reads it.
        from lib.llm.provider import LLMConfig
        self.config = LLMConfig(
            provider="deepseek",
            api_key="test",
            model="deepseek-chat",
            timeout_s=5.0,
        )

    async def structured(
        self, *, system: str, user: str, schema, temperature: float, max_tokens: int,
    ):
        self.calls.append(
            {"system": system, "user": user, "schema": schema.__name__}
        )
        return schema(category=self.answer_category, reason="test")


# ---------------------------------------------------------------------
# FakeRenderingAdapter — predictable response
# ---------------------------------------------------------------------


class FakeRenderingAdapter:
    def __init__(
        self,
        *,
        html: str = "<p>rendered</p>",
        model_name: str = "fake",
        cost: Decimal = Decimal("0.001"),
        latency_ms: float = 0.0,
    ) -> None:
        self.html = html
        self.model_name = model_name
        self.cost = cost
        self.latency_ms = latency_ms
        self.calls: list[RenderRequest] = []

    async def render_conversation_turn(
        self, req: RenderRequest
    ) -> RenderResponse:
        import asyncio
        self.calls.append(req)
        if self.latency_ms > 0:
            await asyncio.sleep(self.latency_ms / 1000.0)
        return RenderResponse(
            response_html=self.html,
            rendering_model_used=self.model_name,
            cost_usd=self.cost,
        )


# ---------------------------------------------------------------------
# FakeStrategy — bypasses retrieval for handler integration tests
# ---------------------------------------------------------------------


@dataclass
class _FakeRetrievalResult:
    """Minimal RetrievalResult stand-in — our tests read .notes only."""
    trigger: Any = None
    observations: list = field(default_factory=list)
    models: list = field(default_factory=list)
    acts: dict = field(default_factory=lambda: {"goals": [], "commitments": [], "decisions": []})
    resources: list = field(default_factory=list)
    pathway_results: list = field(default_factory=list)
    notes: dict = field(default_factory=lambda: {"pathways_run": ["A", "B"]})
    model_scores: dict = field(default_factory=dict)


def _empty_bundle() -> ContextBundle:
    return ContextBundle(
        observations=[],
        models=[],
        acts_summary={"goals": [], "commitments": [], "decisions": []},
        resources_summary=[],
        bridge_context=None,
        access_redactions=0,
        notes={"budgets": {}, "access_redactions": 0,
               "retrieval_trigger_kind": "T1",
               "budget_overflow": {},
               "access_redaction_reasons": {},
               "mmr": {"used": False}},
    )


class FakeStrategy:
    """Replaces a real strategy: uses the category's parse method but
    returns an empty retrieval result + empty bundle."""

    category = "arbitrary"

    def __init__(self, category: str = "arbitrary") -> None:
        self.category = category

    def parse(self, query, *, conversation_history=None, card_context=None):
        from services.query.strategies.base import ParsedQuery
        return ParsedQuery(raw_query=query, category=self.category)

    def build_trigger(self, parsed, tenant_id, *, now):
        from services.retrieval.primary import TriggerContext
        return TriggerContext(kind="T1", tenant_id=tenant_id)

    async def gather(self, parsed, ctx):
        return StrategyResult(
            parsed=parsed,
            retrieval_result=_FakeRetrievalResult(),
            context_bundle=_empty_bundle(),
            notes={"strategy": self.category},
        )


# ---------------------------------------------------------------------
# FakeConnProvider — yields a no-op connection with a no-op transaction
# ---------------------------------------------------------------------


class _FakeTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def transaction(self):
        return _FakeTx()


class _FakeConnProvider:
    @asynccontextmanager
    async def _cm(self):
        yield _FakeConn()

    def __call__(self):
        return self._cm()


def fake_conn_provider() -> _FakeConnProvider:
    return _FakeConnProvider()


__all__ = [
    "ScriptedClassifier",
    "ScriptedLLMProvider",
    "FakeRenderingAdapter",
    "FakeStrategy",
    "fake_conn_provider",
]
