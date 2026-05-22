"""
services/query/core.py — QueryHandler orchestration.

Flow per CONTRACTS §2.2:

  1. Classify the query (classifier.QueryClassifier).
  2. Dispatch to the per-category strategy (strategies/*).
  3. Strategy runs primary_retrieve + assemble_context against the
     caller's DB connection.
  4. Serialize the context bundle + strategy notes into the rendering
     adapter's RenderRequest.
  5. Rendering adapter returns HTML; we wrap it in AnswerQueryResponse.

Follow-ups: if `conversation_history` is non-empty, every strategy
gets a chance to fold prior turns into its parser (see strategies/*.parse
— they already read `conversation_history`). The rendering adapter also
receives `conversation_history` so the renderer can maintain voice
continuity.

Card context pass-through: if `context_card_id` is set, the handler
fetches a lightweight `CardContext` (by calling the card-context
resolver) and routes it into the strategy + rendering layer. Until
Agent-GRT ships its card store, the resolver is a pluggable seam so
tests can pass a fake.

Out-of-scope for this module:
  - Voice enforcement (Agent-RND owns)
  - Greeting / cards cache (Agent-GRT owns)
  - UI state (Agent-UI owns)
"""
from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable, Literal, Optional, Protocol
from uuid import UUID, uuid4

import asyncpg

from lib.shared.errors import ValidationError
from services.retrieval.assembler import AccessContext, ContextBundle
from services.retrieval.primary import RetrievalResult

from .adapters import (
    CacheAdapter,
    RenderRequest,
    RenderResponse,
    RenderingAdapter,
    build_cache_adapter,
    build_rendering_adapter,
)
from .classifier import (
    ClassificationResult,
    QueryCategory,
    QueryClassifier,
)
from .strategies import get_strategy
from .strategies.base import StrategyContext, StrategyResult

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Public request / response / turn models
# ---------------------------------------------------------------------


@dataclass
class Turn:
    """One round of conversation.

    Queries carry enough context that a follow-up can reference prior
    subjects ("why?" following "what about Acme?" still points at
    Acme). Mirrors the TurnAction shape in CONTRACTS §1.3 enough that
    the API layer can project it verbatim.
    """
    turn_id: UUID
    query: str
    response_html: str
    category: QueryCategory
    created_at: datetime
    saved: bool = False
    done: bool = False


@dataclass
class CardContext:
    """Minimal card-context payload passed from UI → API → handler
    when a CEO taps a card verb. Kept narrow so we don't couple to
    Agent-GRT's eventual card schema."""
    card_id: UUID
    subject: Optional[str] = None
    recipient: Optional[str] = None
    kind: Optional[str] = None
    raw: dict[str, Any] = field(default_factory=dict)


CardContextResolver = Callable[[UUID, UUID], Awaitable[Optional[CardContext]]]
"""Signature: (tenant_id, card_id) -> Optional[CardContext]."""


async def _noop_resolver(tenant_id: UUID, card_id: UUID) -> Optional[CardContext]:
    """Default resolver — returns None. Agent-GRT's card store will
    supply a real resolver once ready."""
    return None


@dataclass
class AnswerQueryRequest:
    """CONTRACTS §2.2."""
    tenant_id: UUID
    query: str
    context_card_id: Optional[UUID] = None
    conversation_history: list[Turn] = field(default_factory=list)
    # Optional caller-provided card context (bypasses resolver — used by
    # tests and by the API layer when the UI passes card_context inline).
    inline_card_context: Optional[CardContext] = None
    # Optional — used by prefetch so responses can be cached under a
    # stable key. When set, the handler writes to
    # `query_prefetch:<query_id>` after rendering.
    query_id: Optional[str] = None


@dataclass
class RetrievalTrace:
    """Observability payload returned alongside the response."""
    category: QueryCategory
    classifier_source: str
    classifier_confidence: float
    strategy: str
    pathways_run: list[str]
    models_returned: int
    observations_returned: int
    acts_returned: dict[str, int]
    latency_ms_total: int
    latency_ms_classify: int
    latency_ms_retrieve: int
    latency_ms_render: int
    notes: dict[str, Any] = field(default_factory=dict)


@dataclass
class AnswerQueryResponse:
    """CONTRACTS §2.2. Includes `turn_id`, the rendered HTML, and a
    retrieval trace. The API layer projects this into the
    CONTRACTS §1.2 shape (adds `query_echo`, `verbs`, `computed_at`,
    `latency_ms`)."""
    turn_id: UUID
    query_echo: str
    response_html: str
    category: QueryCategory
    retrieval_trace: RetrievalTrace
    rendering_cost_usd: Decimal
    rendering_model_used: str
    computed_at: datetime
    latency_ms: int


# ---------------------------------------------------------------------
# QueryHandler
# ---------------------------------------------------------------------


class QueryHandler:
    """
    Orchestrates classify → retrieve → render for an Ask query.

    Construction args:
      - `conn_provider`: async callable returning an asyncpg.Connection
        (or a context manager). API layer wires this to its request pool.
      - `classifier`: optional injection; defaults to a module-level
        QueryClassifier using deepseek-chat.
      - `rendering_adapter`: defaults to the factory-built adapter
        (mock unless env configured).
      - `cache_adapter`: defaults to the factory-built cache adapter
        (in-memory unless Postgres configured).
      - `card_resolver`: async callable (tenant_id, card_id) -> CardContext | None.
    """

    def __init__(
        self,
        *,
        conn_provider: Callable[[], Any],
        classifier: Optional[QueryClassifier] = None,
        rendering_adapter: Optional[RenderingAdapter] = None,
        cache_adapter: Optional[CacheAdapter] = None,
        card_resolver: Optional[CardContextResolver] = None,
        access_context_builder: Optional[
            Callable[[UUID], AccessContext]
        ] = None,
        embedder: Optional[Any] = None,
    ) -> None:
        self._conn_provider = conn_provider
        self._classifier = classifier or QueryClassifier()
        self._rendering = rendering_adapter or build_rendering_adapter()
        self._cache = cache_adapter or build_cache_adapter()
        self._card_resolver = card_resolver or _noop_resolver
        self._access_builder = access_context_builder or (
            lambda tid: AccessContext(tenant_id=tid)
        )
        self._embedder = embedder

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------
    async def answer_query(
        self, request: AnswerQueryRequest
    ) -> AnswerQueryResponse:
        if not request.query or not request.query.strip():
            raise ValidationError(
                "query must be non-empty",
                query=request.query,
            )

        t_overall = time.perf_counter()

        # --- Resolve card context ---
        card_ctx: Optional[CardContext] = request.inline_card_context
        if card_ctx is None and request.context_card_id is not None:
            try:
                card_ctx = await self._card_resolver(
                    request.tenant_id, request.context_card_id
                )
            except Exception:  # noqa: BLE001
                log.exception("card_resolver_failed")
                card_ctx = None

        # --- Classify ---
        t_classify = time.perf_counter()
        classification = await self._classifier.classify(
            request.tenant_id,
            request.query,
            has_card_context=card_ctx is not None,
        )
        latency_classify_ms = int((time.perf_counter() - t_classify) * 1000)

        # --- Dispatch to strategy ---
        strategy = get_strategy(classification.category)
        parsed = strategy.parse(
            request.query,
            conversation_history=request.conversation_history,
            card_context=card_ctx,
        )

        # --- Retrieval (runs in a fresh transaction per query) ---
        t_retrieve = time.perf_counter()
        async with self._conn_provider() as conn:
            async with conn.transaction():
                access = self._access_builder(request.tenant_id)
                ctx = StrategyContext(
                    tenant_id=request.tenant_id,
                    conn=conn,
                    access_context=access,
                    conversation_history=request.conversation_history,
                    card_context=card_ctx,
                    now=datetime.now(timezone.utc),
                    embedder=self._embedder,
                )
                result: StrategyResult = await strategy.gather(parsed, ctx)
        latency_retrieve_ms = int((time.perf_counter() - t_retrieve) * 1000)

        # --- Render ---
        t_render = time.perf_counter()
        render_req = RenderRequest(
            tenant_id=request.tenant_id,
            query=request.query,
            category=classification.category,
            conversation_history=[_turn_to_dict(t) for t in request.conversation_history],
            card_context=_card_context_to_dict(card_ctx),
            context_bundle=_bundle_to_dict(result.context_bundle),
            strategy_notes=result.notes,
            retrieval_trace={
                "pathways_run": result.retrieval_result.notes.get("pathways_run", []),
                "classifier": {
                    "category": classification.category,
                    "source": classification.source,
                    "confidence": classification.confidence,
                },
            },
        )
        render_resp: RenderResponse = await self._rendering.render_conversation_turn(
            render_req
        )
        latency_render_ms = int((time.perf_counter() - t_render) * 1000)

        latency_total_ms = int((time.perf_counter() - t_overall) * 1000)

        turn_id = uuid4()

        # --- Cache under query_prefetch key when asked ---
        if request.query_id:
            await self._cache.set(
                request.tenant_id,
                f"query_prefetch:{request.query_id}",
                _serializable_response(
                    turn_id=turn_id,
                    request=request,
                    classification=classification,
                    result=result,
                    render_resp=render_resp,
                    latency_total_ms=latency_total_ms,
                    latency_retrieve_ms=latency_retrieve_ms,
                    latency_render_ms=latency_render_ms,
                    latency_classify_ms=latency_classify_ms,
                ),
                reason="prefetched",
            )

        trace = RetrievalTrace(
            category=classification.category,
            classifier_source=classification.source,
            classifier_confidence=classification.confidence,
            strategy=result.notes.get("strategy", classification.category),
            pathways_run=list(
                result.retrieval_result.notes.get("pathways_run", [])
            ),
            models_returned=len(result.context_bundle.models),
            observations_returned=len(result.context_bundle.observations),
            acts_returned={
                k: len(v) for k, v in result.context_bundle.acts_summary.items()
            },
            latency_ms_total=latency_total_ms,
            latency_ms_classify=latency_classify_ms,
            latency_ms_retrieve=latency_retrieve_ms,
            latency_ms_render=latency_render_ms,
            notes=result.notes,
        )

        return AnswerQueryResponse(
            turn_id=turn_id,
            query_echo=request.query,
            response_html=render_resp.response_html,
            category=classification.category,
            retrieval_trace=trace,
            rendering_cost_usd=render_resp.cost_usd,
            rendering_model_used=render_resp.rendering_model_used,
            computed_at=datetime.now(timezone.utc),
            latency_ms=latency_total_ms,
        )

    # ------------------------------------------------------------------
    # Prefetched fast-path
    # ------------------------------------------------------------------
    async def try_serve_from_prefetch(
        self,
        tenant_id: UUID,
        query_id: str,
    ) -> Optional[AnswerQueryResponse]:
        """If prefetch cached a response for this query_id, rehydrate
        and return it (without rerunning retrieval or rendering). Used
        by the API layer when the UI taps a pre-loaded chip."""
        row = await self._cache.get(
            tenant_id, f"query_prefetch:{query_id}"
        )
        if row is None:
            return None
        try:
            return _response_from_serializable(row["content"])
        except Exception:  # noqa: BLE001
            log.exception("prefetch_deserialize_failed")
            return None


# ---------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------


def _turn_to_dict(t: Turn) -> dict[str, Any]:
    return {
        "turn_id": str(t.turn_id),
        "query": t.query,
        "response_html": t.response_html,
        "category": t.category,
        "created_at": t.created_at.isoformat(),
        "saved": t.saved,
        "done": t.done,
    }


def _card_context_to_dict(c: Optional[CardContext]) -> Optional[dict[str, Any]]:
    if c is None:
        return None
    return {
        "card_id": str(c.card_id),
        "subject": c.subject,
        "recipient": c.recipient,
        "kind": c.kind,
        "raw": dict(c.raw or {}),
    }


def _bundle_to_dict(b: ContextBundle) -> dict[str, Any]:
    """Serialize a ContextBundle for the rendering adapter. Keeps only
    fields the renderer is known to use; the full row shapes live in
    Pydantic and round-trip via `model_dump`."""
    return {
        "observations": [_model_dump(o) for o in b.observations],
        "models": [_model_dump(m) for m in b.models],
        "acts_summary": {
            k: [_model_dump(x) for x in v]
            for k, v in b.acts_summary.items()
        },
        "resources_summary": [_model_dump(r) for r in b.resources_summary],
        "bridge_context": b.bridge_context,
        "access_redactions": b.access_redactions,
        "notes": b.notes,
    }


def _model_dump(obj: Any) -> dict[str, Any]:
    """Dump a Pydantic row (or dict, or dataclass) to a JSON-safe
    dict, stringifying UUIDs / datetimes. Uses Pydantic when present,
    falls back to asdict."""
    if hasattr(obj, "model_dump"):
        d = obj.model_dump(mode="json")
    elif hasattr(obj, "__dataclass_fields__"):
        d = asdict(obj)
    elif isinstance(obj, dict):
        d = dict(obj)
    else:
        return {"_repr": repr(obj)}
    # Coerce UUIDs / datetimes to strings for JSON.
    return _jsonify(d)


def _jsonify(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _jsonify(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_jsonify(v) for v in obj]
    if isinstance(obj, (UUID, datetime)):
        return str(obj)
    if isinstance(obj, Decimal):
        return str(obj)
    return obj


def _serializable_response(
    *,
    turn_id: UUID,
    request: AnswerQueryRequest,
    classification: ClassificationResult,
    result: StrategyResult,
    render_resp: RenderResponse,
    latency_total_ms: int,
    latency_retrieve_ms: int,
    latency_render_ms: int,
    latency_classify_ms: int,
) -> dict[str, Any]:
    return {
        "turn_id": str(turn_id),
        "query_echo": request.query,
        "response_html": render_resp.response_html,
        "category": classification.category,
        "rendering_cost_usd": str(render_resp.cost_usd),
        "rendering_model_used": render_resp.rendering_model_used,
        "classifier_source": classification.source,
        "classifier_confidence": classification.confidence,
        "strategy": result.notes.get("strategy", classification.category),
        "pathways_run": list(
            result.retrieval_result.notes.get("pathways_run", [])
        ),
        "models_returned": len(result.context_bundle.models),
        "observations_returned": len(result.context_bundle.observations),
        "acts_returned": {
            k: len(v) for k, v in result.context_bundle.acts_summary.items()
        },
        "strategy_notes": _jsonify(result.notes),
        "latency_ms_total": latency_total_ms,
        "latency_ms_retrieve": latency_retrieve_ms,
        "latency_ms_render": latency_render_ms,
        "latency_ms_classify": latency_classify_ms,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }


def _response_from_serializable(d: dict[str, Any]) -> AnswerQueryResponse:
    """Inverse of `_serializable_response`. Used by the prefetch hit
    path. Rebuilds only the public fields — internal pydantic rows
    aren't reconstructed (the UI doesn't need them)."""
    category: QueryCategory = d.get("category", "arbitrary")  # type: ignore[assignment]
    trace = RetrievalTrace(
        category=category,
        classifier_source=d.get("classifier_source", "cache"),
        classifier_confidence=float(d.get("classifier_confidence", 0.0)),
        strategy=d.get("strategy", category),
        pathways_run=list(d.get("pathways_run", [])),
        models_returned=int(d.get("models_returned", 0)),
        observations_returned=int(d.get("observations_returned", 0)),
        acts_returned=dict(d.get("acts_returned", {})),
        latency_ms_total=int(d.get("latency_ms_total", 0)),
        latency_ms_classify=int(d.get("latency_ms_classify", 0)),
        latency_ms_retrieve=int(d.get("latency_ms_retrieve", 0)),
        latency_ms_render=int(d.get("latency_ms_render", 0)),
        notes=d.get("strategy_notes", {}),
    )
    return AnswerQueryResponse(
        turn_id=UUID(d["turn_id"]),
        query_echo=d["query_echo"],
        response_html=d["response_html"],
        category=category,
        retrieval_trace=trace,
        rendering_cost_usd=Decimal(str(d.get("rendering_cost_usd", "0"))),
        rendering_model_used=d.get("rendering_model_used", "unknown"),
        computed_at=datetime.fromisoformat(d["computed_at"]),
        latency_ms=int(d.get("latency_ms_total", 0)),
    )


__all__ = [
    "Turn",
    "CardContext",
    "CardContextResolver",
    "AnswerQueryRequest",
    "AnswerQueryResponse",
    "RetrievalTrace",
    "QueryHandler",
]
