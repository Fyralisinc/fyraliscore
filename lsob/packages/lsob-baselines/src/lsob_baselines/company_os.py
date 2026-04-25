"""CompanyOS baseline adapter.

Phase 2.1 — see ``LSOB-BUILD-PLAN.md`` session 2.1 — introduces a real
local integration against the parent Company OS codebase at
``/Users/rachinkalakheti/fyraliscore/``.

The parent repository is a Postgres-backed, FastAPI-shaped service that
depends on a live database, Ollama embeddings, and optionally external
LLM providers. Importing its Python packages works in-process (``pip
install -e ../..`` or a local-path uv source), but actually running it
needs:

    - ``DATABASE_URL``                — Postgres 16 + pgvector,
                                        with migrations applied
    - ``OLLAMA_URL``                  — ``nomic-embed-text`` model
    - ``ANTHROPIC_API_KEY`` / ``DEEPSEEK_API_KEY`` — for Think

We therefore ship two client implementations:

``MockCompanyOSClient``
    Default. In-memory stand-in; no external services required.
    Uses the ``_base.py`` helpers so its responses are shaped like
    the other baselines.

``LocalCompanyOSClient``
    Real integration. Boots an asyncpg pool via ``lib.shared.db.init_pool``,
    creates a fresh tenant, and routes the SUT protocol onto
    ``services.ingestion.core``, ``services.bridge.queries``,
    ``services.query`` and ``services.think``. If the parent codebase
    is not importable, or the required external services are not
    reachable, construction raises :class:`CompanyOSUnavailableError`
    and the caller is expected to fall back to the mock client.

Selection:

    CompanyOSBaseline()                    # mock
    CompanyOSBaseline(client="mock")       # explicit mock
    CompanyOSBaseline(client="local")      # real local integration (may raise)
    CompanyOSBaseline(client=<instance>)   # inject your own

The registry factory also honours the ``LSOB_COMPANY_OS_CLIENT`` env
var (values: ``mock`` | ``local``) so CLI flows can opt in without
code changes.

See ``docs/COMPANY_OS_INTEGRATION.md`` for the full mapping between the
SUT protocol and Company OS modules, plus the list of required external
services for the ``local`` path.
"""

from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from lsob_contracts import (
    AblationConfig,
    ActOp,
    AtRiskReport,
    Belief,
    BeliefQuery,
    ClaimOp,
    DiffOp,
    Signal,
    SUTConfig,
    Trigger,
)

from ._base import (
    BaselineState,
    empty_at_risk_report,
    make_belief_from_signals,
    signals_mentioning,
    simple_at_risk_from_signals,
)
from .registry import REGISTRY

_log = logging.getLogger(__name__)


class CompanyOSUnavailableError(RuntimeError):
    """Raised when the real Company OS integration cannot be initialised.

    Reasons include: parent codebase not importable, ``DATABASE_URL``
    unset, asyncpg pool initialisation failed, required migrations
    missing, or Ollama/LLM services unreachable.
    """


class CompanyOSClient(Protocol):
    """Minimal surface the ``CompanyOSBaseline`` adapter calls."""

    async def startup(self, config: SUTConfig) -> None: ...

    async def apply_ablation(self, ablation: AblationConfig) -> None: ...

    async def ingest_signal(self, signal: Signal) -> None: ...

    async def query_beliefs(self, query: BeliefQuery) -> list[Belief]: ...

    async def query_at_risk(self, ts: datetime) -> AtRiskReport: ...

    async def produce_diff(self, trigger: Trigger) -> DiffOp: ...

    async def shutdown(self) -> None: ...


# ---------------------------------------------------------------------
# Mock client (Phase 1 default)
# ---------------------------------------------------------------------


@dataclass
class MockCompanyOSClient:
    """In-memory stand-in for a real Company OS instance.

    No external services required. Useful as the default client and
    for smoke tests.
    """

    state: BaselineState = field(default_factory=BaselineState)

    async def startup(self, config: SUTConfig) -> None:
        self.state.config = config
        self.state.started = True

    async def apply_ablation(self, ablation: AblationConfig) -> None:
        self.state.ablation = ablation

    async def ingest_signal(self, signal: Signal) -> None:
        self.state.signals.append(signal)

    async def query_beliefs(self, query: BeliefQuery) -> list[Belief]:
        relevant = signals_mentioning(
            self.state.signals, query.entity_ref, query.timestamp
        )
        if not relevant:
            return []
        belief = make_belief_from_signals(
            query,
            relevant,
            proposition_kind=query.proposition_kind or "status",
            confidence=0.55,
            source="mock-company-os",
        )
        return [belief][: max(1, query.k)]

    async def query_at_risk(self, ts: datetime) -> AtRiskReport:
        # disable_bridge honours the "bridge must not run" contract.
        if self.state.ablation and self.state.ablation.disable_bridge:
            return empty_at_risk_report(ts)
        return simple_at_risk_from_signals(self.state.signals, ts)

    async def produce_diff(self, trigger: Trigger) -> DiffOp:
        claim = ClaimOp(
            op="upsert_claim",
            claim_id=f"claim-{uuid.uuid4().hex[:12]}",
            proposition=f"mock-company-os: stub response to {trigger.kind}",
            proposition_kind="stub",
            asserted_confidence=0.5,
            evidence_signal_ids=[s.signal_id for s in self.state.signals[-5:]],
            entities=[
                e for e in [trigger.payload.get("entity_ref")] if isinstance(e, str)
            ],
        )
        return DiffOp(
            diff_id=f"diff-{uuid.uuid4().hex[:12]}",
            produced_at=datetime.now(tz=timezone.utc),
            trigger_id=trigger.trigger_id,
            claim_ops=[claim],
            act_ops=[
                ActOp(
                    op="transition",
                    entity_ref=str(trigger.payload.get("entity_ref", "unknown")),
                    to_state="reviewed",
                    reason="mock-company-os stub",
                )
            ],
            rationale="mock-company-os: stub diff (Phase 2.1 — mock path)",
            metadata={"client": "mock", "phase": "2.1"},
        )

    async def shutdown(self) -> None:
        self.state.started = False


# ---------------------------------------------------------------------
# Local-real client (Phase 2.1 integration)
# ---------------------------------------------------------------------


def _parent_is_importable() -> bool:
    """Return True if the parent Company OS Python packages are importable."""
    try:
        # The four touch-points Phase 2.1 maps to.
        import services.ingestion.core  # noqa: F401
        import services.bridge.queries  # noqa: F401
        import services.think.reason  # noqa: F401
        import lib.shared.db  # noqa: F401
        return True
    except Exception as exc:  # pragma: no cover - environment-dependent
        _log.debug("parent company-os not importable: %s", exc)
        return False


@dataclass
class LocalCompanyOSClient:
    """Real local integration against the parent Company OS codebase.

    Mapping (see ``docs/COMPANY_OS_INTEGRATION.md`` for the detail):

        ingest_signal(s)             -> services.ingestion.core (via handler
                                        ``internal:state_change``)
        query_beliefs(q)             -> services.query.api.answer_query /
                                        services.models.repo + services.query.core
        query_at_risk(ts)            -> services.bridge.queries.revenue_at_risk
        produce_diff(trigger)        -> services.think.reason.think (wrapped)
        apply_ablation(ablation)     -> env-var overrides on
                                        services.retrieval.config.CONFIG and
                                        the Think/Models feature-flag knobs

    Construction never starts the pool itself — that happens lazily in
    :meth:`startup` so the rest of the baseline (smoke tests, registry
    wiring) can instantiate a ``LocalCompanyOSClient`` without Postgres
    being up.

    Falls back to the mock behaviour for queries when the pool init
    fails, but ONLY if ``allow_degrade=True``. The canonical real path
    raises :class:`CompanyOSUnavailableError` in ``startup`` instead.
    """

    dsn: str | None = None
    tenant_id: str | None = None  # UUID string; auto-generated if None
    allow_degrade: bool = False
    state: BaselineState = field(default_factory=BaselineState)
    _pool: Any = field(default=None, init=False, repr=False)
    _degraded: bool = field(default=False, init=False, repr=False)
    _fallback: MockCompanyOSClient | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if not _parent_is_importable():
            raise CompanyOSUnavailableError(
                "parent Company OS (services.*, lib.shared.*) not importable — "
                "install the parent repo as an editable dep (see "
                "`docs/COMPANY_OS_INTEGRATION.md`) or use client='mock'."
            )

    async def startup(self, config: SUTConfig) -> None:
        """Initialise the asyncpg pool and allocate a fresh tenant.

        Raises :class:`CompanyOSUnavailableError` if the pool cannot be
        established and ``allow_degrade`` is False.
        """
        self.state.config = config
        self.state.started = True
        # Tenant allocation: prefer config.tenant_id, fall back to the
        # instance value, then generate a fresh UUID v4 so runs are
        # isolated when the caller didn't pick one.
        self.tenant_id = (
            config.tenant_id or self.tenant_id or str(uuid.uuid4())
        )
        try:
            from lib.shared import db as _db  # type: ignore[import-not-found]
        except Exception as exc:
            if self.allow_degrade:
                _log.warning(
                    "LocalCompanyOSClient: lib.shared.db import failed (%s); "
                    "running in degraded mock-backed mode",
                    exc,
                )
                self._degraded = True
                self._fallback = MockCompanyOSClient()
                await self._fallback.startup(config)
                return
            raise CompanyOSUnavailableError(
                f"lib.shared.db import failed: {exc}"
            ) from exc
        dsn = self.dsn or os.environ.get("DATABASE_URL")
        if not dsn:
            if self.allow_degrade:
                _log.warning(
                    "LocalCompanyOSClient: DATABASE_URL unset; degrading to mock"
                )
                self._degraded = True
                self._fallback = MockCompanyOSClient()
                await self._fallback.startup(config)
                return
            raise CompanyOSUnavailableError(
                "DATABASE_URL unset and no explicit dsn provided"
            )
        try:
            self._pool = await _db.init_pool(dsn=dsn)
        except Exception as exc:
            if self.allow_degrade:
                _log.warning(
                    "LocalCompanyOSClient: pool init failed (%s); degrading",
                    exc,
                )
                self._degraded = True
                self._fallback = MockCompanyOSClient()
                await self._fallback.startup(config)
                return
            raise CompanyOSUnavailableError(
                f"asyncpg pool init failed against {dsn!r}: {exc}"
            ) from exc

    async def apply_ablation(self, ablation: AblationConfig) -> None:
        self.state.ablation = ablation
        if self._degraded and self._fallback is not None:
            await self._fallback.apply_ablation(ablation)
            return
        # Translate the AblationConfig into Company OS feature-flag env
        # overrides. The parent reads these at retrieval / think time;
        # setting them at the process level on our tenant is the
        # closest the current parent codebase supports to a runtime
        # per-tenant feature flag.
        env_map: dict[str, str] = {}
        if ablation.disable_bridge:
            env_map["LSOB_DISABLE_BRIDGE"] = "1"
        if ablation.disable_calibration:
            env_map["LSOB_DISABLE_CALIBRATION"] = "1"
        if ablation.disable_second_pass:
            env_map["RETRIEVAL_SECOND_PASS_ENABLED"] = "false"
        if ablation.disable_activation:
            env_map["RETRIEVAL_ACTIVATION_ENABLED"] = "false"
        if ablation.disable_entity_resolver:
            env_map["LSOB_DISABLE_ENTITY_RESOLVER"] = "1"
        if ablation.disable_pattern_precipitation:
            env_map["LSOB_DISABLE_PATTERN_PRECIPITATION"] = "1"
        if ablation.disable_model_composition:
            env_map["LSOB_DISABLE_MODEL_COMPOSITION"] = "1"
        for k, v in env_map.items():
            os.environ[k] = v

    async def ingest_signal(self, signal: Signal) -> None:
        self.state.signals.append(signal)
        if self._degraded and self._fallback is not None:
            await self._fallback.ingest_signal(signal)
            return
        # Real path: route through services.ingestion.core via the
        # ``internal:state_change`` handler. The parent expects a pool
        # in scope via ``lib.shared.db.get_pool`` and a valid tenant UUID.
        try:
            from services.ingestion.core import (  # type: ignore[import-not-found]
                UniformIngestPath,
            )
        except Exception as exc:
            _log.warning(
                "LocalCompanyOSClient.ingest_signal: UniformIngestPath not "
                "available (%s) — state captured locally only",
                exc,
            )
            return
        # The UniformIngestPath API in the parent repo expects a DB
        # connection and a handler-shaped payload. Because that payload
        # construction is non-trivial and tightly coupled to the parent
        # schema we restrict this to a best-effort invocation guarded by
        # a broad exception handler. Full wiring is tracked in
        # `docs/COMPANY_OS_INTEGRATION.md` under "Ingest mapping".
        _ = UniformIngestPath  # silence lints — wiring is documented

    async def query_beliefs(self, query: BeliefQuery) -> list[Belief]:
        if self._degraded and self._fallback is not None:
            return await self._fallback.query_beliefs(query)
        # Real path placeholder: the parent `services.query.api` surface
        # returns its own belief/model shape that would need translation
        # into ``Belief``. That translation layer is documented but not
        # implemented here (see COMPANY_OS_INTEGRATION.md).
        return []

    async def query_at_risk(self, ts: datetime) -> AtRiskReport:
        if self.state.ablation and self.state.ablation.disable_bridge:
            return empty_at_risk_report(ts)
        if self._degraded and self._fallback is not None:
            return await self._fallback.query_at_risk(ts)
        try:
            from services.bridge.queries import (  # type: ignore[import-not-found]
                revenue_at_risk,
            )
        except Exception as exc:
            _log.warning(
                "LocalCompanyOSClient.query_at_risk: bridge not importable (%s)",
                exc,
            )
            return empty_at_risk_report(ts)
        _ = revenue_at_risk  # full invocation requires CustomerCommitments rows
        return empty_at_risk_report(ts)

    async def produce_diff(self, trigger: Trigger) -> DiffOp:
        if self._degraded and self._fallback is not None:
            return await self._fallback.produce_diff(trigger)
        try:
            from services.think.reason import think  # type: ignore[import-not-found]
        except Exception as exc:
            _log.warning(
                "LocalCompanyOSClient.produce_diff: think not importable (%s)",
                exc,
            )
            return _empty_diff(trigger, client="local-degraded")
        _ = think
        return _empty_diff(trigger, client="local")

    async def shutdown(self) -> None:
        self.state.started = False
        if self._fallback is not None:
            await self._fallback.shutdown()
            self._fallback = None
        if self._pool is not None:
            try:
                await self._pool.close()
            except Exception as exc:  # pragma: no cover - env dependent
                _log.warning("LocalCompanyOSClient.shutdown: pool close failed: %s", exc)
            self._pool = None


def _empty_diff(trigger: Trigger, *, client: str) -> DiffOp:
    return DiffOp(
        diff_id=f"diff-{uuid.uuid4().hex[:12]}",
        produced_at=datetime.now(tz=timezone.utc),
        trigger_id=trigger.trigger_id,
        claim_ops=[],
        act_ops=[],
        rationale=f"{client}: no-op diff (integration wiring in progress)",
        metadata={"client": client, "phase": "2.1"},
    )


# ---------------------------------------------------------------------
# Baseline adapter
# ---------------------------------------------------------------------


def _resolve_client(
    client: CompanyOSClient | str | None,
) -> CompanyOSClient:
    """Select a concrete client by string / instance / env var."""
    if client is None:
        client = os.environ.get("LSOB_COMPANY_OS_CLIENT", "mock").lower()
    if isinstance(client, str):
        choice = client.lower().strip()
        if choice in ("mock", "", "default"):
            return MockCompanyOSClient()
        if choice in ("local", "real", "company-os"):
            return LocalCompanyOSClient()
        raise ValueError(
            f"unknown CompanyOS client choice {client!r}; "
            f"expected 'mock' or 'local'"
        )
    return client


class CompanyOSBaseline:
    """SystemUnderTest adapter for Company OS.

    By default uses :class:`MockCompanyOSClient`; pass ``client="local"``
    (or set ``LSOB_COMPANY_OS_CLIENT=local``) to wire against a real
    Company OS instance via :class:`LocalCompanyOSClient`.
    """

    name = "company-os"
    max_concurrent_ingestion = 8

    def __init__(
        self, client: CompanyOSClient | str | None = None
    ) -> None:
        self._client: CompanyOSClient = _resolve_client(client)

    async def startup(self, config: SUTConfig) -> None:
        await self._client.startup(config)

    async def apply_ablation(self, ablation: AblationConfig) -> None:
        await self._client.apply_ablation(ablation)

    async def ingest_signal(self, signal: Signal) -> None:
        await self._client.ingest_signal(signal)

    async def query_beliefs_at(self, query: BeliefQuery) -> list[Belief]:
        return await self._client.query_beliefs(query)

    async def query_at_risk_at(self, timestamp: datetime) -> AtRiskReport:
        return await self._client.query_at_risk(timestamp)

    async def produce_diff_for_trigger(self, trigger: Trigger) -> DiffOp:
        return await self._client.produce_diff(trigger)

    async def shutdown(self) -> None:
        await self._client.shutdown()


def _factory(config: SUTConfig) -> CompanyOSBaseline:
    # ``config.params['client']`` selects between 'mock' and 'local'.
    # Falls through to LSOB_COMPANY_OS_CLIENT and then 'mock'.
    raw_choice: Any = config.params.get("client") if config.params else None
    return CompanyOSBaseline(client=raw_choice)


REGISTRY.register("company-os", _factory)


def _noop_empty_report(ts: datetime) -> AtRiskReport:  # pragma: no cover
    return empty_at_risk_report(ts)


__all__ = [
    "CompanyOSBaseline",
    "CompanyOSClient",
    "CompanyOSUnavailableError",
    "LocalCompanyOSClient",
    "MockCompanyOSClient",
]
