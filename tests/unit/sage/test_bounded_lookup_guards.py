from __future__ import annotations

from typing import Any
from uuid import uuid4

import asyncpg
import pytest

from services.platform.execution import inquiry as inquiry_mod
from services.reasoning.sage import reader as reader_mod


class _FakeTransaction:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeTransaction:
        self._conn.transaction_enter_count += 1
        return self

    async def __aexit__(self, *_exc: object) -> bool:
        self._conn.transaction_exit_count += 1
        return False


class _FakeConn:
    def __init__(
        self,
        *,
        rows: list[dict[str, Any]] | None = None,
        error: BaseException | None = None,
        outer_transaction: bool = False,
        statement_timeout: str = "0",
    ) -> None:
        self.rows = rows or []
        self.error = error
        self.outer_transaction = outer_transaction
        self.statement_timeout = statement_timeout
        self.executed: list[str] = []
        self.fetchval_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.transaction_enter_count = 0
        self.transaction_exit_count = 0

    def is_in_transaction(self) -> bool:
        return self.outer_transaction

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)

    async def execute(self, query: str) -> None:
        self.executed.append(query)

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.fetchval_calls.append((query, args))
        if "current_setting('statement_timeout')" in query:
            return self.statement_timeout
        if "set_config('statement_timeout'" in query:
            self.statement_timeout = str(args[0])
            return self.statement_timeout
        return None

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((query, args))
        if self.error is not None:
            raise self.error
        return self.rows


class _ExplodingConn:
    async def fetchval(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("generic reader lookup should not touch the database")

    async def fetch(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("generic reader lookup should not touch the database")


class _SparseTimeoutConn(_FakeConn):
    async def fetchval(self, query: str, *args: Any) -> Any:
        self.fetchval_calls.append((query, args))
        if "model_sparse_terms" in query:
            return "model_sparse_terms"
        if "model_search_documents" in query:
            raise AssertionError("LIKE fallback should not run after sparse timeout")
        return await super().fetchval(query, *args)


class _OperationalRolePostingsConn(_FakeConn):
    async def fetchval(self, query: str, *args: Any) -> Any:
        self.fetchval_calls.append((query, args))
        if "model_operational_role_postings" in query:
            return "model_operational_role_postings"
        return await super().fetchval(query, *args)


class _AnswerabilityIndexConn(_FakeConn):
    async def fetchval(self, query: str, *args: Any) -> Any:
        self.fetchval_calls.append((query, args))
        if "model_answerability_index" in query:
            return "model_answerability_index"
        return await super().fetchval(query, *args)


class _BeliefAddressLookupConn(_FakeConn):
    def __init__(self, fetch_outcomes: list[Any]) -> None:
        super().__init__()
        self.fetch_outcomes = list(fetch_outcomes)

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.fetchval_calls.append((query, args))
        if "model_belief_addresses" in query:
            return "model_belief_addresses"
        if "model_answerability_index" in query:
            return "model_answerability_index"
        if "model_search_documents" in query:
            return "model_search_documents"
        return None

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((query, args))
        outcome = self.fetch_outcomes.pop(0) if self.fetch_outcomes else []
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def test_sage_sparse_lookup_terms_skip_generic_words_but_keep_identifiers() -> None:
    terms = reader_mod._sparse_lookup_terms(
        [
            "owner responsible assigned dependency evidence blocker customer launch",
            "customer-95 Borealis renewal",
        ],
        max_terms=8,
    )

    assert "owner" not in terms
    assert "dependency" not in terms
    assert "launch" not in terms
    assert "customer-95" in terms
    assert "borealis" in terms
    assert "renewal" in terms


@pytest.mark.asyncio
async def test_sage_search_document_lookup_skips_generic_terms_without_db() -> None:
    rows = await reader_mod._fetch_search_document_matches(
        _ExplodingConn(),  # type: ignore[arg-type]
        tenant_id=uuid4(),
        terms=["owner responsible assigned dependency evidence blocker customer"],
        limit=8,
        microquery_enabled=True,
    )

    assert rows == []


@pytest.mark.asyncio
async def test_sage_search_document_lookup_skips_like_after_sparse_timeout() -> None:
    conn = _SparseTimeoutConn(error=asyncpg.QueryCanceledError("statement timeout"))

    rows = await reader_mod._fetch_search_document_matches(
        conn,  # type: ignore[arg-type]
        tenant_id=uuid4(),
        terms=["security review renewal risk counterevidence"],
        limit=8,
        microquery_enabled=True,
    )

    assert rows == []
    assert len(conn.fetch_calls) == 1
    assert any("model_sparse_terms" in query for query, _args in conn.fetchval_calls)
    assert not any(
        "model_search_documents" in query for query, _args in conn.fetchval_calls
    )


@pytest.mark.asyncio
async def test_sage_operational_role_lookup_uses_bounded_postings() -> None:
    conn = _OperationalRolePostingsConn(
        rows=[
            {
                "id": uuid4(),
                "natural": "SSD option adds 300 dollars",
                "matched_roles": ["delta"],
                "role_match_count": 1,
                "lexical_match_count": 4,
            }
        ]
    )

    rows = await reader_mod._fetch_operational_role_matches(
        conn,  # type: ignore[arg-type]
        tenant_id=uuid4(),
        seed_roles=["delta"],
        terms=["ssd", "dollars"],
        limit=8,
        per_role_limit=32,
    )

    assert rows[0]["matched_roles"] == ["delta"]
    assert len(conn.fetch_calls) == 1
    query = conn.fetch_calls[0][0]
    assert "model_operational_role_postings" in query
    role_lateral = query.split("CROSS JOIN LATERAL (", 1)[1].split(") hit", 1)[0]
    assert "model_search_documents msd" in role_lateral
    assert "LEFT JOIN LATERAL" in role_lateral
    assert "coalesce(lexical.lexical_match_count, 0) > 0" in role_lateral
    assert role_lateral.index("model_search_documents msd") < role_lateral.index("LIMIT $5")
    assert conn.executed == ["SET LOCAL statement_timeout = 1500"]
    assert any(
        "model_operational_role_postings" in query
        for query, _args in conn.fetchval_calls
    )


@pytest.mark.asyncio
async def test_sage_operational_role_lookup_degrades_on_postings_timeout() -> None:
    conn = _OperationalRolePostingsConn(
        error=asyncpg.QueryCanceledError("statement timeout")
    )

    rows = await reader_mod._fetch_operational_role_matches(
        conn,  # type: ignore[arg-type]
        tenant_id=uuid4(),
        seed_roles=["delta"],
        terms=["ssd", "dollars"],
        limit=8,
        per_role_limit=32,
    )

    assert rows == []
    assert len(conn.fetch_calls) == 1
    assert "model_operational_role_postings" in conn.fetch_calls[0][0]
    assert "model_search_documents msd" in conn.fetch_calls[0][0]


@pytest.mark.asyncio
async def test_sage_answerability_lookup_uses_dynamic_df_guard() -> None:
    conn = _AnswerabilityIndexConn(
        rows=[
            {
                "id": uuid4(),
                "natural": "Kestrel invoice handoff is ownerless.",
                "primitive_match_count": 1,
                "lexical_match_count": 3,
                "matched_primitives": ["OWNERSHIP"],
                "lexical_terms_present": True,
            }
        ]
    )

    rows = await reader_mod._fetch_answerability_index_matches(
        conn,  # type: ignore[arg-type]
        tenant_id=uuid4(),
        primitive_values=["OWNERSHIP"],
        terms=["kestrel invoice handoff"],
        limit=8,
    )

    assert rows[0]["lexical_terms_present"] is True
    assert len(conn.fetch_calls) == 1
    query, args = conn.fetch_calls[0]
    assert "token_stats AS MATERIALIZED" in query
    assert "LIMIT $6" in query
    assert "stats.term_df <= $7" in query
    assert "GROUP BY group_ord, primitive" in query
    assert args[-3] == 32
    assert args[-2] == 513
    assert args[-1] == 512


@pytest.mark.asyncio
async def test_sage_belief_address_lookup_stops_after_answerability_timeout() -> None:
    conn = _BeliefAddressLookupConn(
        [asyncpg.QueryCanceledError("statement timeout")]
    )

    rows = await reader_mod._fetch_belief_address_matches(
        conn,  # type: ignore[arg-type]
        tenant_id=uuid4(),
        primitives=["DEPENDENCY"],
        terms=["kestrel invoice handoff"],
        limit=8,
    )

    assert rows == []
    assert len(conn.fetch_calls) == 1
    assert "model_answerability_index" in conn.fetch_calls[0][0]
    assert sum(
        1 for query, _args in conn.fetchval_calls if "model_belief_addresses" in query
    ) == 1
    assert not any(
        "model_search_documents" in query for query, _args in conn.fetchval_calls
    )


@pytest.mark.asyncio
async def test_sage_belief_address_lookup_stops_after_fts_timeout() -> None:
    conn = _BeliefAddressLookupConn(
        [
            [],
            asyncpg.QueryCanceledError("statement timeout"),
        ]
    )

    rows = await reader_mod._fetch_belief_address_matches(
        conn,  # type: ignore[arg-type]
        tenant_id=uuid4(),
        primitives=["DEPENDENCY"],
        terms=["kestrel invoice handoff"],
        limit=8,
    )

    assert rows == []
    assert len(conn.fetch_calls) == 2
    assert "model_answerability_index" in conn.fetch_calls[0][0]
    assert "model_belief_addresses mba, query" in conn.fetch_calls[1][0]
    assert not any(
        "model_search_documents" in query for query, _args in conn.fetchval_calls
    )


@pytest.mark.asyncio
async def test_sage_belief_address_lookup_stops_after_search_document_timeout() -> None:
    conn = _BeliefAddressLookupConn(
        [
            [],
            [],
            asyncpg.QueryCanceledError("statement timeout"),
        ]
    )

    rows = await reader_mod._fetch_belief_address_matches(
        conn,  # type: ignore[arg-type]
        tenant_id=uuid4(),
        primitives=["DEPENDENCY"],
        terms=["kestrel invoice handoff"],
        limit=8,
    )

    assert rows == []
    assert len(conn.fetch_calls) == 3
    assert "model_search_documents msd" in conn.fetch_calls[2][0]
    assert not any("belief_address_like" in str(args) for _query, args in conn.fetch_calls)


@pytest.mark.asyncio
async def test_sage_sparse_lookup_counts_models_not_sparse_distinct() -> None:
    conn = _SparseTimeoutConn()

    rows = await reader_mod._fetch_sparse_term_matches(
        conn,  # type: ignore[arg-type]
        tenant_id=uuid4(),
        terms=["soc2-risk-77", "vendor escrow"],
        limit=8,
        max_terms=4,
        per_term_limit=4,
    )

    assert rows == []
    assert len(conn.fetch_calls) == 1
    query, _args = conn.fetch_calls[0]
    assert "active_models AS MATERIALIZED" in query
    assert "FROM accepted_current_models accepted" in query
    assert "JOIN models legacy" in query
    assert "COUNT(DISTINCT MODEL_ID)" not in query.upper()
    assert "active_sparse_models" not in query


@pytest.mark.asyncio
async def test_sage_operational_role_lookup_legacy_fallback_is_bounded() -> None:
    conn = _FakeConn(
        rows=[
            {
                "id": uuid4(),
                "natural": "SSD option adds 300 dollars",
                "matched_roles": ["delta"],
                "role_match_count": 1,
                "lexical_match_count": 4,
            }
        ]
    )

    rows = await reader_mod._fetch_operational_role_matches(
        conn,  # type: ignore[arg-type]
        tenant_id=uuid4(),
        seed_roles=["delta"],
        terms=["ssd", "dollars"],
        limit=8,
        per_role_limit=32,
    )

    assert rows[0]["lexical_match_count"] == 4
    assert len(conn.fetch_calls) == 1
    assert "model_search_documents msd" in conn.fetch_calls[0][0]
    assert conn.executed == ["SET LOCAL statement_timeout = 1500"]


@pytest.mark.asyncio
async def test_sage_bounded_lookup_returns_rows_under_statement_timeout() -> None:
    conn = _FakeConn(rows=[{"id": "model-a"}])

    rows = await reader_mod._fetch_bounded_lookup_rows(
        conn,  # type: ignore[arg-type]
        "SELECT $1",
        "needle",
        label="unit",
    )

    assert rows == [{"id": "model-a"}]
    assert conn.executed == ["SET LOCAL statement_timeout = 1500"]
    assert conn.fetch_calls == [("SELECT $1", ("needle",))]
    assert conn.transaction_enter_count == 1
    assert conn.transaction_exit_count == 1


@pytest.mark.asyncio
async def test_sage_bounded_lookup_degrades_on_statement_timeout() -> None:
    conn = _FakeConn(error=asyncpg.QueryCanceledError("statement timeout"))

    rows = await reader_mod._fetch_bounded_lookup_rows(
        conn,  # type: ignore[arg-type]
        "SELECT pg_sleep(10)",
        label="unit_timeout",
    )

    assert rows == []
    assert getattr(rows, "timed_out") is True
    assert conn.executed == ["SET LOCAL statement_timeout = 1500"]
    assert len(conn.fetch_calls) == 1
    assert conn.transaction_enter_count == 1
    assert conn.transaction_exit_count == 1


@pytest.mark.asyncio
async def test_sage_bounded_lookup_restores_outer_transaction_timeout() -> None:
    conn = _FakeConn(
        error=asyncpg.QueryCanceledError("statement timeout"),
        outer_transaction=True,
        statement_timeout="7s",
    )

    rows = await reader_mod._fetch_bounded_lookup_rows(
        conn,  # type: ignore[arg-type]
        "SELECT pg_sleep(10)",
        label="outer_timeout",
    )

    assert rows == []
    assert getattr(rows, "timed_out") is True
    assert conn.executed == []
    assert conn.statement_timeout == "7s"
    assert conn.fetchval_calls == [
        ("SELECT current_setting('statement_timeout')", ()),
        ("SELECT set_config('statement_timeout', $1, true)", ("1500",)),
        ("SELECT set_config('statement_timeout', $1, true)", ("7s",)),
    ]
    assert conn.transaction_enter_count == 1
    assert conn.transaction_exit_count == 1


@pytest.mark.asyncio
async def test_inquiry_bounded_lookup_returns_rows_under_statement_timeout() -> None:
    conn = _FakeConn(rows=[{"id": "model-b"}])

    rows = await inquiry_mod._fetch_bounded_lookup_rows(
        conn,  # type: ignore[arg-type]
        "SELECT $1",
        "needle",
        label="unit",
    )

    assert rows == [{"id": "model-b"}]
    assert conn.executed == ["SET LOCAL statement_timeout = 1500"]
    assert conn.fetch_calls == [("SELECT $1", ("needle",))]
    assert conn.transaction_enter_count == 1
    assert conn.transaction_exit_count == 1


@pytest.mark.asyncio
async def test_inquiry_bounded_lookup_degrades_on_statement_timeout() -> None:
    conn = _FakeConn(error=asyncpg.QueryCanceledError("statement timeout"))

    rows = await inquiry_mod._fetch_bounded_lookup_rows(
        conn,  # type: ignore[arg-type]
        "SELECT pg_sleep(10)",
        label="unit_timeout",
    )

    assert rows == []
    assert getattr(rows, "timed_out") is True
    assert conn.executed == ["SET LOCAL statement_timeout = 1500"]
    assert len(conn.fetch_calls) == 1
    assert conn.transaction_enter_count == 1
    assert conn.transaction_exit_count == 1


@pytest.mark.asyncio
async def test_inquiry_bounded_lookup_restores_outer_transaction_timeout() -> None:
    conn = _FakeConn(
        error=asyncpg.QueryCanceledError("statement timeout"),
        outer_transaction=True,
        statement_timeout="9s",
    )

    rows = await inquiry_mod._fetch_bounded_lookup_rows(
        conn,  # type: ignore[arg-type]
        "SELECT pg_sleep(10)",
        label="outer_timeout",
    )

    assert rows == []
    assert getattr(rows, "timed_out") is True
    assert conn.executed == []
    assert conn.statement_timeout == "9s"
    assert conn.fetchval_calls == [
        ("SELECT current_setting('statement_timeout')", ()),
        ("SELECT set_config('statement_timeout', $1, true)", ("1500",)),
        ("SELECT set_config('statement_timeout', $1, true)", ("9s",)),
    ]
    assert conn.transaction_enter_count == 1
    assert conn.transaction_exit_count == 1
