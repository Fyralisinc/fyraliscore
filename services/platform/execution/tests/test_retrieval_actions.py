from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
import asyncpg

from services.platform.execution import inquiry, retrieval_actions
from services.platform.execution.config import InquiryConfig
from services.platform.execution.types import RetrievalAction
from services.reasoning.retrieval.pathways import ModelCandidateHit, PathwayResult
from services.reasoning.retrieval.primary import TriggerContext


def _trigger(text: str = "Generic customer dependency status") -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_entity_ids=[],
        scope_actors=[],
        seed_natural_text=text,
        seed_occurred_at=datetime(2026, 6, 17, 8, 0, tzinfo=timezone.utc),
    )


class _ExplodingConn:
    async def fetchval(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("generic hybrid lookup should not touch the database")

    async def fetch(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("generic hybrid lookup should not touch the database")


class _LookupTransaction:
    def __init__(self, conn: "_SparseTimeoutConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_LookupTransaction":
        self._conn.transaction_enter_count += 1
        return self

    async def __aexit__(self, *_exc: object) -> bool:
        self._conn.transaction_exit_count += 1
        return False


class _SparseTimeoutConn:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.transaction_enter_count = 0
        self.transaction_exit_count = 0

    def transaction(self) -> _LookupTransaction:
        return _LookupTransaction(self)

    async def execute(self, query: str) -> None:
        self.executed.append(query)

    async def fetchval(self, query: str, *args: object) -> object:
        self.fetchval_calls.append((query, args))
        if "model_sparse_terms" in query:
            return "model_sparse_terms"
        if "model_search_documents" in query:
            raise AssertionError("LIKE fallback should not run after sparse timeout")
        return None

    async def fetch(self, query: str, *args: object) -> object:
        self.fetch_calls.append((query, args))
        raise asyncpg.QueryCanceledError("statement timeout")


class _RecordingLookupConn:
    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        self.rows = rows or []
        self.executed: list[str] = []
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.transaction_enter_count = 0
        self.transaction_exit_count = 0

    def transaction(self) -> _LookupTransaction:
        return _LookupTransaction(self)  # type: ignore[arg-type]

    async def execute(self, query: str) -> None:
        self.executed.append(query)

    async def fetchval(self, query: str, *args: object) -> object:
        self.fetchval_calls.append((query, args))
        if "model_sparse_terms" in query:
            return "model_sparse_terms"
        if "model_answerability_index" in query:
            return "model_answerability_index"
        return None

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        self.fetch_calls.append((query, args))
        return self.rows


class _AcquireConn:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    async def __aenter__(self) -> object:
        return self._conn

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        tb: object,
    ) -> None:
        return None


class _ReadPool:
    def __init__(self, conn: object, *, max_size: int = 4) -> None:
        self._conn = conn
        self._max_size = max_size
        self.acquires = 0

    def get_max_size(self) -> int:
        return self._max_size

    def acquire(self) -> _AcquireConn:
        self.acquires += 1
        return _AcquireConn(self._conn)


def test_inquiry_private_aliases_point_to_retrieval_actions_module() -> None:
    assert (
        inquiry._execute_focused_index_action
        is retrieval_actions.execute_focused_index_action
    )
    assert (
        inquiry._execute_semantic_hybrid_action
        is retrieval_actions.execute_semantic_hybrid_action
    )
    assert inquiry._cap_pathway_models is retrieval_actions.cap_pathway_models
    assert (
        inquiry._fetch_bounded_lookup_rows
        is retrieval_actions.fetch_bounded_lookup_rows
    )
    assert (
        inquiry._merge_hybrid_semantic_lexical_models
        is retrieval_actions.merge_hybrid_semantic_lexical_models
    )


def test_focused_seed_entity_pairs_expands_customer_resource_aliases() -> None:
    entity_id = uuid4()

    pairs = retrieval_actions.focused_seed_entity_pairs(
        [
            {"type": "customer", "id": str(entity_id)},
            {"type": "resource", "id": str(entity_id)},
            {"type": "commitment", "id": str(uuid4())},
            {"type": "bad", "id": "not-a-uuid"},
        ]
    )

    pair_set = {(kind, UUID(str(raw_id))) for kind, raw_id in pairs}
    assert {
        ("customer", entity_id),
        ("customer_resource", entity_id),
        ("resource", entity_id),
    } <= pair_set
    assert len([pair for pair in pairs if pair[1] == entity_id]) == 3


def test_merge_hybrid_semantic_lexical_models_prefers_cross_signal_hits() -> None:
    semantic_first = SimpleNamespace(id=uuid4())
    cross_signal = SimpleNamespace(id=uuid4())
    lexical_only = SimpleNamespace(id=uuid4())

    merged = retrieval_actions.merge_hybrid_semantic_lexical_models(
        [semantic_first, cross_signal],
        [(lexical_only, 3), (cross_signal, 2)],
        limit=2,
    )

    assert [model.id for model in merged] == [cross_signal.id, lexical_only.id]


@pytest.mark.asyncio
async def test_hybrid_lexical_scan_skips_generic_lookup_terms() -> None:
    hits = await retrieval_actions.hybrid_lexical_model_scan(
        _trigger(),
        _ExplodingConn(),  # type: ignore[arg-type]
        terms=["owner responsible assigned dependency evidence blocker customer"],
        limit=8,
        per_term_limit=4,
    )

    assert hits == []


@pytest.mark.asyncio
async def test_hybrid_lexical_scan_skips_like_after_sparse_timeout() -> None:
    conn = _SparseTimeoutConn()

    hits = await retrieval_actions.hybrid_lexical_model_scan(
        _trigger("Does the security review renewal risk have counterevidence?"),
        conn,  # type: ignore[arg-type]
        terms=["security review", "renewal risk", "counterevidence"],
        limit=8,
        per_term_limit=4,
    )

    assert hits == []
    assert len(conn.fetch_calls) == 1
    assert conn.transaction_enter_count == 1
    assert any("model_sparse_terms" in query for query, _args in conn.fetchval_calls)
    assert not any(
        "model_search_documents" in query for query, _args in conn.fetchval_calls
    )


@pytest.mark.asyncio
async def test_hybrid_sparse_model_scan_uses_supplied_active_sparse_model_count() -> None:
    conn = _RecordingLookupConn()

    hits = await retrieval_actions.hybrid_sparse_model_scan(
        _trigger("Does SOC2-RISK-77 block the launch?"),
        conn,  # type: ignore[arg-type]
        terms=["soc2-risk-77", "vendor escrow"],
        limit=8,
        per_term_limit=4,
        active_sparse_model_count=123,
    )

    assert hits == []
    assert conn.fetchval_calls == []
    assert len(conn.fetch_calls) == 1
    query, args = conn.fetch_calls[0]
    assert "SELECT DISTINCT model_id" not in query
    assert "$7::int" in query
    assert args[-1] == 123


@pytest.mark.asyncio
async def test_cached_active_sparse_model_count_uses_models_not_sparse_distinct() -> None:
    conn = _RecordingLookupConn(rows=[{"active_model_count": 42}])
    cache: dict[UUID, int] = {}
    tenant_id = uuid4()

    count = await retrieval_actions._cached_active_sparse_model_count(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        active_sparse_model_counts=cache,
    )

    assert count == 42
    assert cache[tenant_id] == 42
    assert len(conn.fetch_calls) == 1
    query, args = conn.fetch_calls[0]
    assert "FROM models" in query
    assert "COUNT(DISTINCT MODEL_ID)" not in query.upper()
    assert "FROM model_sparse_terms" not in query
    assert args == (tenant_id,)


@pytest.mark.asyncio
async def test_hybrid_sparse_model_scan_counts_models_not_sparse_distinct() -> None:
    conn = _RecordingLookupConn()

    hits = await retrieval_actions.hybrid_sparse_model_scan(
        _trigger("Does SOC2-RISK-77 block the launch?"),
        conn,  # type: ignore[arg-type]
        terms=["soc2-risk-77", "vendor escrow"],
        limit=8,
        per_term_limit=4,
    )

    assert hits == []
    assert len(conn.fetch_calls) == 1
    query, _args = conn.fetch_calls[0]
    assert "active_models AS MATERIALIZED" in query
    assert "FROM models" in query
    assert "COUNT(DISTINCT MODEL_ID)" not in query.upper()
    assert "active_sparse_models" not in query


@pytest.mark.asyncio
async def test_focused_scope_sparse_scan_bounds_scope_candidates_first() -> None:
    conn = _RecordingLookupConn()
    tenant_id = uuid4()
    entity_id = uuid4()

    hits = await retrieval_actions.focused_scope_sparse_scan(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        terms=["security review", "renewal risk"],
        seed_pairs=[("customer", entity_id)],
        limit=8,
    )

    assert hits == []
    assert len(conn.fetch_calls) == 1
    query, args = conn.fetch_calls[0]
    assert "scope_candidates AS MATERIALIZED" in query
    assert "scope_pool AS MATERIALIZED" in query
    assert "CROSS JOIN LATERAL" in query
    assert "WITH scoped_ids AS MATERIALIZED" in query
    assert "FROM model_scope_entities mse" in query
    assert "JOIN models m" in query
    assert "EXISTS (" not in query
    assert "ORDER BY m.activation DESC" in query
    assert "LIMIT $6" in query
    assert "JOIN scope_overlap sc" not in query
    assert "JOIN scope_pool sp" in query
    assert "AND mst.model_id = sp.model_id" in query
    assert "LIMIT $7" in query
    assert args[-2] == 96
    assert args[-1] == 240


@pytest.mark.asyncio
async def test_focused_scope_sparse_scan_globally_caps_batch_shaped_scope_pool() -> None:
    conn = _RecordingLookupConn()
    tenant_id = uuid4()
    seed_pairs = [
        (kind, uuid4())
        for kind in ("customer", "customer_resource", "resource", "commitment")
        for _index in range(8)
    ]

    hits = await retrieval_actions.focused_scope_sparse_scan(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        terms=[
            "atlas retail renewal blocker",
            "security review evidence",
            "support noise counterevidence",
            "owner followup",
        ],
        seed_pairs=seed_pairs,
        limit=48,
    )

    assert hits == []
    query, args = conn.fetch_calls[0]
    assert "scope_pool AS MATERIALIZED" in query
    assert "JOIN scope_pool sp" in query
    assert "JOIN scope_overlap sc" not in query
    assert args[-2] == 240
    assert args[-1] == 960


@pytest.mark.asyncio
async def test_focused_direct_scope_scan_bounds_scope_candidates() -> None:
    conn = _RecordingLookupConn()
    tenant_id = uuid4()
    entity_id = uuid4()

    hits = await retrieval_actions.focused_direct_scope_scan(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        seed_pairs=[("customer", entity_id)],
        limit=8,
    )

    assert hits == []
    assert len(conn.fetch_calls) == 1
    query, args = conn.fetch_calls[0]
    assert "scope_candidates AS MATERIALIZED" in query
    assert "CROSS JOIN LATERAL" in query
    assert "WITH scoped_ids AS MATERIALIZED" in query
    assert "FROM model_scope_entities mse" in query
    assert "JOIN models m" in query
    assert "EXISTS (" not in query
    assert "ORDER BY m.activation DESC" in query
    assert "LIMIT $5" in query
    assert args[-1] == 128


@pytest.mark.asyncio
async def test_focused_answerability_scan_bounds_scope_overlap() -> None:
    conn = _RecordingLookupConn()
    tenant_id = uuid4()
    entity_id = uuid4()

    hits = await retrieval_actions.focused_answerability_index_scan(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        primitives=("DEPENDENCY",),
        terms=["soc2 evidence"],
        seed_pairs=[("customer", entity_id)],
        limit=8,
    )

    assert hits == []
    assert len(conn.fetch_calls) == 1
    query, args = conn.fetch_calls[0]
    assert "scope_hits AS MATERIALIZED" in query
    assert "CROSS JOIN LATERAL" in query
    assert "WITH scoped_ids AS MATERIALIZED" in query
    assert "token_stats AS MATERIALIZED" in query
    assert "LIMIT $9" in query
    assert "stats.term_df <= $10" in query
    assert "FROM model_scope_entities mse" in query
    assert "JOIN models m" in query
    assert "EXISTS (" not in query
    assert "ORDER BY m.activation DESC" in query
    assert "LIMIT $8" in query
    assert args[-4] == 32
    assert args[-3] == 80
    assert args[-2] == 513
    assert args[-1] == 512


@pytest.mark.asyncio
async def test_semantic_retrieval_session_reuses_in_flight_duplicate_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dense_model = SimpleNamespace(id=uuid4(), activation=0.9)
    calls: list[str] = []

    async def fake_pathway_b_semantic(
        query_text: str,
        *_args: object,
        **_kwargs: object,
    ) -> PathwayResult:
        calls.append(query_text)
        await asyncio.sleep(0)
        return PathwayResult(
            models=[dense_model],
            source_pathway="B",
            notes={"semantic_fixture": "dense_only"},
        )

    async def fake_semantic_term_rescue(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[list[ModelCandidateHit], dict[str, object]]:
        return [], {"enabled": True, "used": False}

    async def fake_representation_tag_rescue(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[list[ModelCandidateHit], dict[str, object]]:
        return [], {"enabled": True, "used": False}

    async def fake_lexical_rescue(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[list[tuple[SimpleNamespace, int]], dict[str, object]]:
        return [], {"enabled": True, "used": False, "lexical_count": 0}

    monkeypatch.setattr(
        retrieval_actions,
        "pathway_b_semantic",
        fake_pathway_b_semantic,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_semantic_term_candidate_rescue",
        fake_semantic_term_rescue,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_representation_tag_candidate_rescue",
        fake_representation_tag_rescue,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_semantic_hybrid_lexical_rescue",
        fake_lexical_rescue,
    )

    trigger = _trigger("same semantic launch blocker")
    cfg = InquiryConfig(semantic_hybrid_lexical_enabled=True)
    session = retrieval_actions.SemanticRetrievalSession()
    action_one = RetrievalAction(
        "Q1",
        "semantic",
        "constraint_evidence",
        query="same semantic launch blocker",
        budget=5,
    )
    action_two = RetrievalAction(
        "Q2",
        "semantic",
        "constraint_evidence",
        query="same semantic launch blocker",
        budget=5,
    )

    result_one, result_two = await asyncio.gather(
        session.execute_action(
            action_one,
            trigger,
            object(),  # type: ignore[arg-type]
            None,
            cfg,
            model_limit=5,
        ),
        session.execute_action(
            action_two,
            trigger,
            object(),  # type: ignore[arg-type]
            None,
            cfg,
            model_limit=5,
        ),
    )

    assert calls == ["same semantic launch blocker"]
    notes = [
        result_one.notes["semantic_substrate"],
        result_two.notes["semantic_substrate"],
    ]
    assert sorted(note["cache_hit"] for note in notes) == [False, True]
    assert sorted(note["cache_wait"] for note in notes) == [False, True]
    assert [model.id for model in result_one.models] == [dense_model.id]
    assert [model.id for model in result_two.models] == [dense_model.id]

    result_three = await session.execute_action(
        RetrievalAction(
            "Q3",
            "semantic",
            "constraint_evidence",
            query="same semantic launch blocker",
            budget=5,
        ),
        trigger,
        object(),  # type: ignore[arg-type]
        None,
        cfg,
        model_limit=5,
    )

    assert calls == ["same semantic launch blocker"]
    assert result_three.notes["semantic_substrate"]["cache_hit"] is True
    assert result_three.notes["semantic_substrate"]["cache_wait"] is False


@pytest.mark.asyncio
async def test_semantic_retrieval_session_keeps_seed_entity_scopes_separate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[dict[str, str]]] = []

    async def fake_pathway_b_semantic(
        _query_text: str,
        *_args: object,
        **kwargs: object,
    ) -> PathwayResult:
        calls.append(list(kwargs.get("event_entities") or []))
        return PathwayResult(
            models=[SimpleNamespace(id=uuid4(), activation=0.7)],
            source_pathway="B",
        )

    async def fake_semantic_term_rescue(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[list[ModelCandidateHit], dict[str, object]]:
        return [], {"enabled": True, "used": False}

    async def fake_representation_tag_rescue(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[list[ModelCandidateHit], dict[str, object]]:
        return [], {"enabled": True, "used": False}

    async def fake_lexical_rescue(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[list[tuple[SimpleNamespace, int]], dict[str, object]]:
        return [], {"enabled": False, "used": False, "lexical_count": 0}

    monkeypatch.setattr(
        retrieval_actions,
        "pathway_b_semantic",
        fake_pathway_b_semantic,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_semantic_term_candidate_rescue",
        fake_semantic_term_rescue,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_representation_tag_candidate_rescue",
        fake_representation_tag_rescue,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_semantic_hybrid_lexical_rescue",
        fake_lexical_rescue,
    )

    trigger = _trigger("same semantic launch blocker")
    session = retrieval_actions.SemanticRetrievalSession()
    cfg = InquiryConfig(semantic_hybrid_lexical_enabled=False)
    scope_one = [{"type": "customer", "id": str(uuid4())}]
    scope_two = [{"type": "customer", "id": str(uuid4())}]

    for question_id, scope in (("Q1", scope_one), ("Q2", scope_two)):
        await session.execute_action(
            RetrievalAction(
                question_id,
                "semantic",
                "constraint_evidence",
                query="same semantic launch blocker",
                budget=5,
                filters={"seed_entities": scope},
            ),
            trigger,
            object(),  # type: ignore[arg-type]
            None,
            cfg,
            model_limit=5,
        )

    assert calls == [scope_one, scope_two]


@pytest.mark.asyncio
async def test_semantic_retrieval_session_caches_schema_capability_probes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Conn:
        def __init__(self) -> None:
            self.fetchval_queries: list[str] = []

        async def fetchval(self, query: str, *_args: object) -> object:
            self.fetchval_queries.append(query)
            if "information_schema.columns" in query:
                return True
            return "public.table"

    async def fake_pathway_b_semantic(
        *_args: object,
        **_kwargs: object,
    ) -> PathwayResult:
        return PathwayResult(
            models=[SimpleNamespace(id=uuid4(), activation=0.9)],
            source_pathway="B",
        )

    semantic_capabilities: list[tuple[object, object, object]] = []
    representation_capabilities: list[tuple[object, object]] = []

    async def fake_pathway_l_semantic_term_candidates(
        *_args: object,
        **kwargs: object,
    ) -> tuple[list[ModelCandidateHit], dict[str, object]]:
        semantic_capabilities.append(
            (
                kwargs.get("semantic_feature_postings_available"),
                kwargs.get("semantic_postings_available"),
                kwargs.get("semantic_postings_status_column"),
            )
        )
        return [], {"source_pathway": "L"}

    async def fake_pathway_b_representation_tag_candidates(
        *_args: object,
        **kwargs: object,
    ) -> tuple[list[ModelCandidateHit], dict[str, object]]:
        representation_capabilities.append(
            (
                kwargs.get("representation_feature_postings_available"),
                kwargs.get("representation_postings_available"),
            )
        )
        return [], {"source_pathway": "B"}

    monkeypatch.setattr(
        retrieval_actions,
        "pathway_b_semantic",
        fake_pathway_b_semantic,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "pathway_l_semantic_term_candidates",
        fake_pathway_l_semantic_term_candidates,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "pathway_b_representation_tag_candidates",
        fake_pathway_b_representation_tag_candidates,
    )

    conn = Conn()
    session = retrieval_actions.SemanticRetrievalSession()
    trigger = _trigger("same launch blocker")
    cfg = InquiryConfig(semantic_hybrid_lexical_enabled=False)

    for question_id, query in (("Q1", "alpha launch blocker"), ("Q2", "beta owner")):
        await session.execute_action(
            RetrievalAction(
                question_id,
                "semantic",
                "constraint_evidence",
                query=query,
                budget=5,
            ),
            trigger,
            conn,  # type: ignore[arg-type]
            None,
            cfg,
            model_limit=5,
        )

    assert len(conn.fetchval_queries) == 1
    assert (
        sum(
            "model_representation_feature_postings')" in q
            for q in conn.fetchval_queries
        )
        == 1
    )
    assert sum("model_semantic_term_postings')" in q for q in conn.fetchval_queries) == 0
    assert sum("information_schema.columns" in q for q in conn.fetchval_queries) == 0
    assert (
        sum("model_representation_tag_postings')" in q for q in conn.fetchval_queries)
        == 0
    )
    assert semantic_capabilities == [(True, True, True), (True, True, True)]
    assert representation_capabilities == [(True, True), (True, True)]


@pytest.mark.asyncio
async def test_semantic_hybrid_merge_preserves_lexical_only_exact_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dense_model = SimpleNamespace(id=uuid4(), activation=0.9)
    lexical_only_model = SimpleNamespace(id=uuid4(), activation=0.1)

    async def fake_pathway_b_semantic(
        *_args: object, **_kwargs: object
    ) -> PathwayResult:
        return PathwayResult(
            models=[dense_model],
            source_pathway="B",
            notes={"semantic_fixture": "dense_only"},
        )

    async def fake_hybrid_lexical_model_scan(
        _trigger: TriggerContext,
        _conn: object,
        *,
        terms: list[str] | tuple[str, ...],
        limit: int,
        per_term_limit: int,
        active_sparse_model_count: int | None = None,
    ) -> list[tuple[SimpleNamespace, int]]:
        assert terms == ["renewal_anchor_42"]
        assert limit >= 1
        assert per_term_limit >= 1
        assert active_sparse_model_count is None
        return [(lexical_only_model, 1)]

    async def fake_empty_candidate_rescue(
        *_args: object, **_kwargs: object
    ) -> tuple[list[ModelCandidateHit], dict[str, object]]:
        return [], {"enabled": True, "used": False}

    monkeypatch.setattr(
        retrieval_actions,
        "pathway_b_semantic",
        fake_pathway_b_semantic,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_hybrid_lexical_terms",
        lambda *_args, **_kwargs: ["renewal_anchor_42"],
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_hybrid_lookup_terms",
        lambda terms, **_kwargs: list(terms),
    )
    monkeypatch.setattr(
        retrieval_actions,
        "hybrid_lexical_model_scan",
        fake_hybrid_lexical_model_scan,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_semantic_term_candidate_rescue",
        fake_empty_candidate_rescue,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_representation_tag_candidate_rescue",
        fake_empty_candidate_rescue,
    )

    result = await retrieval_actions.execute_semantic_hybrid_action(
        RetrievalAction(
            "Q1",
            "semantic",
            "constraint_evidence",
            query="renewal_anchor_42 launch blocker",
            budget=2,
        ),
        _trigger("renewal_anchor_42 launch blocker"),
        object(),  # type: ignore[arg-type]
        None,
        InquiryConfig(
            semantic_hybrid_lexical_enabled=True,
            semantic_hybrid_lexical_max_candidates=4,
            semantic_hybrid_lexical_terms=4,
            semantic_hybrid_lexical_per_term_limit=2,
        ),
        model_limit=2,
    )

    result_ids = [model.id for model in result.models]
    assert dense_model.id in result_ids
    assert lexical_only_model.id in result_ids
    assert result.notes["semantic_hybrid_lexical"]["lexical_only_selected"] == 1


@pytest.mark.asyncio
async def test_candidate_merge_hydrates_only_selected_rescue_winners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dense_model = SimpleNamespace(id=uuid4(), activation=1.0)
    semantic_candidate = ModelCandidateHit(model_id=uuid4(), activation=0.9)
    representation_candidate = ModelCandidateHit(model_id=uuid4(), activation=0.8)
    hydrated_ids: list[object] = []

    async def fake_hydrate_active_models_by_ids(
        _tenant_id: object,
        _conn: object,
        model_ids: list[object],
        **_kwargs: object,
    ) -> list[SimpleNamespace]:
        hydrated_ids.extend(model_ids)
        return [
            SimpleNamespace(id=semantic_candidate.model_id, activation=0.9)
            for model_id in model_ids
            if model_id == semantic_candidate.model_id
        ]

    monkeypatch.setattr(
        retrieval_actions,
        "hydrate_active_models_by_ids",
        fake_hydrate_active_models_by_ids,
    )

    notes: dict[str, object] = {}
    models = await retrieval_actions.merge_semantic_substrate_candidate_models(
        uuid4(),
        object(),  # type: ignore[arg-type]
        [dense_model],
        [semantic_candidate],
        [representation_candidate],
        [],
        limit=2,
        notes=notes,
    )

    assert hydrated_ids == [semantic_candidate.model_id]
    assert {model.id for model in models} == {dense_model.id, semantic_candidate.model_id}
    assert notes["candidate_ids_considered"] == 3
    assert notes["candidate_ids_hydrated"] == 1


@pytest.mark.asyncio
async def test_semantic_hybrid_fans_out_dense_and_rescues_with_read_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dense_model = SimpleNamespace(id=uuid4(), activation=0.9)
    semantic_term_model = SimpleNamespace(id=uuid4(), activation=0.8)
    representation_model = SimpleNamespace(id=uuid4(), activation=0.7)
    lexical_model = SimpleNamespace(id=uuid4(), activation=0.6)
    started: list[str] = []
    all_started = asyncio.Event()

    async def mark_started(name: str) -> None:
        started.append(name)
        if len(started) == 4:
            all_started.set()
        await asyncio.wait_for(all_started.wait(), timeout=0.5)

    async def fake_pathway_b_semantic(
        *_args: object,
        **_kwargs: object,
    ) -> PathwayResult:
        await mark_started("dense")
        return PathwayResult(models=[dense_model], source_pathway="B")

    async def fake_semantic_term_rescue(
        _trigger_arg: TriggerContext,
        conn: object,
        **kwargs: object,
    ) -> tuple[list[ModelCandidateHit], dict[str, object]]:
        assert "active_sparse_model_counts" not in kwargs

        async def run(_read_conn: object) -> list[ModelCandidateHit]:
            await mark_started("semantic_terms")
            return [
                ModelCandidateHit(
                    model_id=semantic_term_model.id,
                    activation=semantic_term_model.activation,
                    match_count=2,
                )
            ]

        hits = await retrieval_actions._run_with_optional_pool(
            conn,  # type: ignore[arg-type]
            kwargs["read_pool"],  # type: ignore[arg-type]
            run,
            read_fanout_budget=kwargs["read_fanout_budget"],  # type: ignore[arg-type]
        )
        return (
            hits,
            {"enabled": True, "used": True},
        )

    async def fake_representation_tag_rescue(
        _trigger_arg: TriggerContext,
        conn: object,
        **kwargs: object,
    ) -> tuple[list[ModelCandidateHit], dict[str, object]]:
        async def run(_read_conn: object) -> list[ModelCandidateHit]:
            await mark_started("representation_tags")
            return [
                ModelCandidateHit(
                    model_id=representation_model.id,
                    activation=representation_model.activation,
                    match_count=1,
                )
            ]

        hits = await retrieval_actions._run_with_optional_pool(
            conn,  # type: ignore[arg-type]
            kwargs["read_pool"],  # type: ignore[arg-type]
            run,
            read_fanout_budget=kwargs["read_fanout_budget"],  # type: ignore[arg-type]
        )
        return (
            hits,
            {"enabled": True, "used": True},
        )

    async def fake_lexical_rescue(
        _trigger_arg: TriggerContext,
        conn: object,
        _cfg: InquiryConfig,
        **kwargs: object,
    ) -> tuple[list[tuple[SimpleNamespace, int]], dict[str, object]]:
        async def run(_read_conn: object) -> list[tuple[SimpleNamespace, int]]:
            await mark_started("lexical")
            return [(lexical_model, 2)]

        lexical_hits = await retrieval_actions._run_with_optional_pool(
            conn,  # type: ignore[arg-type]
            kwargs["read_pool"],  # type: ignore[arg-type]
            run,
            read_fanout_budget=kwargs["read_fanout_budget"],  # type: ignore[arg-type]
        )
        return (
            lexical_hits,
            {"enabled": True, "used": True, "lexical_count": 1},
        )

    async def fake_hydrate_active_models_by_ids(
        _tenant_id: object,
        _conn: object,
        model_ids: list[object],
        **_kwargs: object,
    ) -> list[SimpleNamespace]:
        by_id = {
            semantic_term_model.id: semantic_term_model,
            representation_model.id: representation_model,
        }
        return [by_id[model_id] for model_id in model_ids if model_id in by_id]

    monkeypatch.setattr(
        retrieval_actions,
        "pathway_b_semantic",
        fake_pathway_b_semantic,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_semantic_term_candidate_rescue",
        fake_semantic_term_rescue,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_representation_tag_candidate_rescue",
        fake_representation_tag_rescue,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_semantic_hybrid_lexical_rescue",
        fake_lexical_rescue,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "hydrate_active_models_by_ids",
        fake_hydrate_active_models_by_ids,
    )

    result = await retrieval_actions.execute_semantic_hybrid_action(
        RetrievalAction(
            "Q1",
            "semantic",
            "constraint_evidence",
            query="parallel semantic substrate",
            budget=4,
        ),
        _trigger("parallel semantic substrate"),
        object(),  # type: ignore[arg-type]
        None,
        InquiryConfig(semantic_hybrid_lexical_enabled=True),
        model_limit=4,
        read_pool=_ReadPool(object()),  # type: ignore[arg-type]
    )

    assert set(started) == {
        "dense",
        "semantic_terms",
        "representation_tags",
        "lexical",
    }
    assert result.notes["semantic_substrate"]["fanout_parallel"] is True
    assert result.notes["semantic_substrate"]["read_fanout_budget"] == {
        "max_concurrency": 4,
        "peak_in_use": 4,
        "acquired": 4,
        "denied": 0,
    }
    assert len(result.models) == 4


@pytest.mark.asyncio
async def test_focused_index_fans_out_scans_with_read_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model_a = uuid4()
    model_b = uuid4()
    model_c = uuid4()
    started: list[str] = []
    all_started = asyncio.Event()
    read_pool = _ReadPool(object())

    async def mark_started(name: str) -> None:
        started.append(name)
        if len(started) == 3:
            all_started.set()
        await asyncio.wait_for(all_started.wait(), timeout=0.5)

    async def fake_answerability(*_args: object, **_kwargs: object) -> list[retrieval_actions.FocusedIndexHit]:
        await mark_started("answerability")
        return [
            retrieval_actions.FocusedIndexHit(
                model_a,
                0.50,
                "answerability_index",
                match_count=4,
                scope_overlap=0,
            )
        ]

    async def fake_scope_sparse(*_args: object, **_kwargs: object) -> list[retrieval_actions.FocusedIndexHit]:
        await mark_started("scope_sparse")
        return [
            retrieval_actions.FocusedIndexHit(
                model_a,
                0.70,
                "scope_sparse",
                match_count=2,
                scope_overlap=2,
            ),
            retrieval_actions.FocusedIndexHit(
                model_b,
                0.90,
                "scope_sparse",
                match_count=3,
                scope_overlap=1,
            ),
        ]

    async def fake_direct_scope(*_args: object, **_kwargs: object) -> list[retrieval_actions.FocusedIndexHit]:
        await mark_started("direct_scope")
        return [
            retrieval_actions.FocusedIndexHit(
                model_c,
                0.60,
                "direct_scope",
                scope_overlap=3,
            )
        ]

    class FakeModelsRepo:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def retrieve(self, ids: list[UUID], **_kwargs: object) -> list[SimpleNamespace]:
            return [SimpleNamespace(id=model_id, activation=0.5) for model_id in ids]

    monkeypatch.setattr(
        retrieval_actions,
        "focused_answerability_index_scan",
        fake_answerability,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "focused_scope_sparse_scan",
        fake_scope_sparse,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "focused_direct_scope_scan",
        fake_direct_scope,
    )
    monkeypatch.setattr(retrieval_actions, "ModelsRepo", FakeModelsRepo)

    result = await retrieval_actions.execute_focused_index_action(
        RetrievalAction(
            "Q1",
            "focused_index",
            "question_answerability_scope",
            query="Which dependency mentions SOC2-RISK-77?",
            filters={"primitive": "DEPENDENCY", "terms": ["soc2-risk-77"]},
            budget=8,
        ),
        _trigger("Does SOC2-RISK-77 block the launch?"),
        object(),  # type: ignore[arg-type]
        InquiryConfig(),
        model_limit=8,
        read_pool=read_pool,  # type: ignore[arg-type]
    )

    assert result is not None
    assert set(started) == {"answerability", "scope_sparse", "direct_scope"}
    assert read_pool.acquires == 3
    assert result.notes["fanout_parallel"] is True
    assert [model.id for model in result.models] == [model_a, model_b, model_c]
    assert result.notes["top_hits"][0] == {
        "model_id": str(model_a),
        "score": 1.2,
        "sources": ["answerability_index", "scope_sparse"],
        "match_count": 4,
        "scope_overlap": 2,
    }


@pytest.mark.asyncio
async def test_focused_index_stays_sequential_without_read_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model_id = uuid4()
    calls: list[str] = []

    async def fake_answerability(*_args: object, **_kwargs: object) -> list[retrieval_actions.FocusedIndexHit]:
        calls.append("answerability")
        return [
            retrieval_actions.FocusedIndexHit(
                model_id,
                0.50,
                "answerability_index",
                match_count=2,
            )
        ]

    async def fake_scope_sparse(*_args: object, **_kwargs: object) -> list[retrieval_actions.FocusedIndexHit]:
        calls.append("scope_sparse")
        return []

    async def fake_direct_scope(*_args: object, **_kwargs: object) -> list[retrieval_actions.FocusedIndexHit]:
        calls.append("direct_scope")
        return []

    class FakeModelsRepo:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def retrieve(self, ids: list[UUID], **_kwargs: object) -> list[SimpleNamespace]:
            return [SimpleNamespace(id=value, activation=0.5) for value in ids]

    monkeypatch.setattr(
        retrieval_actions,
        "focused_answerability_index_scan",
        fake_answerability,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "focused_scope_sparse_scan",
        fake_scope_sparse,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "focused_direct_scope_scan",
        fake_direct_scope,
    )
    monkeypatch.setattr(retrieval_actions, "ModelsRepo", FakeModelsRepo)

    result = await retrieval_actions.execute_focused_index_action(
        RetrievalAction(
            "Q1",
            "focused_index",
            "question_answerability_scope",
            query="Which dependency mentions SOC2-RISK-77?",
            filters={"primitive": "DEPENDENCY", "terms": ["soc2-risk-77"]},
            budget=8,
        ),
        _trigger("Does SOC2-RISK-77 block the launch?"),
        object(),  # type: ignore[arg-type]
        InquiryConfig(),
        model_limit=8,
    )

    assert result is not None
    assert calls == ["answerability", "scope_sparse", "direct_scope"]
    assert result.notes["fanout_parallel"] is False
    assert [model.id for model in result.models] == [model_id]


@pytest.mark.asyncio
async def test_focused_index_fanout_propagates_query_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_id = uuid4()
    model_id = uuid4()
    terms = ["soc2-risk-77", "vendor escrow"]
    read_conn = object()
    main_conn = object()
    trigger = _trigger("Does SOC2-RISK-77 block the launch?")
    recorded: dict[str, dict[str, object]] = {}

    async def fake_answerability(
        read_conn_arg: object,
        **kwargs: object,
    ) -> list[retrieval_actions.FocusedIndexHit]:
        recorded["answerability"] = {"conn": read_conn_arg, **kwargs}
        return []

    async def fake_scope_sparse(
        read_conn_arg: object,
        **kwargs: object,
    ) -> list[retrieval_actions.FocusedIndexHit]:
        recorded["scope_sparse"] = {"conn": read_conn_arg, **kwargs}
        return [
            retrieval_actions.FocusedIndexHit(
                model_id,
                0.75,
                "scope_sparse",
                match_count=2,
                scope_overlap=3,
            )
        ]

    async def fake_direct_scope(
        read_conn_arg: object,
        **kwargs: object,
    ) -> list[retrieval_actions.FocusedIndexHit]:
        recorded["direct_scope"] = {"conn": read_conn_arg, **kwargs}
        return []

    class FakeModelsRepo:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def retrieve(
            self,
            ids: list[UUID],
            **kwargs: object,
        ) -> list[SimpleNamespace]:
            assert ids == [model_id]
            assert kwargs["conn"] is main_conn
            return [SimpleNamespace(id=model_id, activation=0.5)]

    monkeypatch.setattr(
        retrieval_actions,
        "focused_answerability_index_scan",
        fake_answerability,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "focused_scope_sparse_scan",
        fake_scope_sparse,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "focused_direct_scope_scan",
        fake_direct_scope,
    )
    monkeypatch.setattr(retrieval_actions, "ModelsRepo", FakeModelsRepo)

    result = await retrieval_actions.execute_focused_index_action(
        RetrievalAction(
            "Q1",
            "focused_index",
            "question_answerability_scope",
            query="Which dependency mentions SOC2-RISK-77?",
            filters={
                "primitive": "DEPENDENCY",
                "terms": terms,
                "seed_entities": [{"type": "customer", "id": str(seed_id)}],
            },
            budget=8,
        ),
        trigger,
        main_conn,  # type: ignore[arg-type]
        InquiryConfig(focused_index_scope_candidates=2),
        model_limit=5,
        read_pool=_ReadPool(read_conn),  # type: ignore[arg-type]
    )

    expected_seed_pairs = {
        ("customer", seed_id),
        ("customer_resource", seed_id),
        ("resource", seed_id),
    }
    for call in recorded.values():
        assert call["conn"] is read_conn
        assert call["tenant_id"] == trigger.tenant_id
        assert set(call["seed_pairs"]) == expected_seed_pairs

    assert recorded["answerability"]["primitives"] == ("DEPENDENCY", "COMMITMENT")
    assert recorded["answerability"]["terms"] == terms
    assert recorded["answerability"]["limit"] == 5
    assert recorded["scope_sparse"]["terms"] == terms
    assert recorded["scope_sparse"]["limit"] == 5
    assert recorded["direct_scope"]["limit"] == 2
    assert result is not None
    assert [model.id for model in result.models] == [model_id]
    assert result.notes["fanout_parallel"] is True
    assert result.notes["answerability_hits"] == 0
    assert result.notes["scoped_sparse_hits"] == 1
    assert result.notes["direct_scope_hits"] == 0
    assert result.notes["seed_scope_pairs"] == 3
    assert result.notes["top_hits"][0]["sources"] == ["scope_sparse"]
