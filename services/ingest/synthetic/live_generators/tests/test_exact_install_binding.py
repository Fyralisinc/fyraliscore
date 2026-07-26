"""Exact, fail-closed installation binding for synthetic live generators."""
from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI

from services.ingest.synthetic.live_generators.aws_poll import AwsPollGenerator
from services.ingest.synthetic.live_generators.carta_poll import CartaPollGenerator
from services.ingest.synthetic.live_generators.gmail_pubsub import GmailPubSubGenerator
from services.ingest.synthetic.live_generators.linkedin_poll import (
    LinkedinPollGenerator,
)
from services.ingest.synthetic.live_generators.signal_gateway import (
    SignalGatewayGenerator,
)
from services.ingest.synthetic.live_generators.telegram_gateway import (
    TelegramGatewayGenerator,
)


class _Pool:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((query, args))
        return self.rows


def _binding_case(
    provider: str,
    rows: list[dict[str, Any]],
    tenant_id: UUID,
) -> tuple[Any, Any, _Pool]:
    pool = _Pool(rows)
    if provider == "telegram":
        generator = TelegramGatewayGenerator(pool=pool)
        call = generator._installation_id(tenant_id, 4242)
    elif provider == "signal":
        generator = SignalGatewayGenerator(pool=pool)
        call = generator._installation_id(tenant_id, 4343)
    elif provider == "aws":
        generator = AwsPollGenerator(pool=pool)
        call = generator._resolve_install(
            tenant_id,
            "123456789012",
            "eu-west-1",
        )
    elif provider == "carta":
        generator = CartaPollGenerator(pool=pool)
        call = generator._resolve_install(tenant_id, "firm-123")
    elif provider == "linkedin":
        generator = LinkedinPollGenerator(pool=pool)
        call = generator._resolve_install(
            tenant_id,
            "urn:li:organization:123",
        )
    else:  # pragma: no cover - parametrization is closed over known providers
        raise AssertionError(provider)
    return generator, call, pool


_SUCCESS_ROWS = {
    "telegram": [{"id": "telegram-install"}],
    "signal": [{"id": "signal-install"}],
    "aws": [{
        "id": "aws-install",
        "account_id": "123456789012",
        "region": "eu-west-1",
    }],
    "carta": [{"id": "carta-install", "firm_id": "firm-123"}],
    "linkedin": [{
        "id": "linkedin-install",
        "organization_urn": "urn:li:organization:123",
    }],
}

_SELECTOR_SQL = {
    "telegram": ("JOIN telegram_dialogs", "td.dialog_id = $2"),
    "signal": ("JOIN signal_threads", "st.thread_id = $2"),
    "aws": ("account_id = $2", "region = $3"),
    "carta": ("firm_id = $2",),
    "linkedin": ("organization_urn = $2",),
}


@pytest.mark.parametrize("provider", sorted(_SUCCESS_ROWS))
async def test_install_binding_uses_the_live_target_selector(provider: str) -> None:
    tenant_id = uuid4()
    _, call, pool = _binding_case(
        provider,
        _SUCCESS_ROWS[provider],
        tenant_id,
    )

    await call

    query, args = pool.calls[0]
    assert "LIMIT 1" not in query
    assert "ORDER BY created_at" not in query
    assert "tenant_id = $1" in query
    assert args[0] == tenant_id
    for fragment in _SELECTOR_SQL[provider]:
        assert fragment in query


@pytest.mark.parametrize("provider", sorted(_SUCCESS_ROWS))
@pytest.mark.parametrize("match_count", [0, 2])
async def test_install_binding_rejects_missing_or_ambiguous_matches(
    provider: str,
    match_count: int,
) -> None:
    rows = _SUCCESS_ROWS[provider] * match_count
    _, call, _ = _binding_case(provider, rows, uuid4())

    with pytest.raises(ValueError, match=rf"matches={match_count}"):
        await call


class _AsyncContext:
    def __init__(self, value: Any) -> None:
        self._value = value

    async def __aenter__(self) -> Any:
        return self._value

    async def __aexit__(self, *args: Any) -> None:
        return None


class _GmailConnection:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, query: str, *args: Any) -> str:
        return "SELECT 1"

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((query, args))
        return self.rows

    def transaction(self) -> _AsyncContext:
        return _AsyncContext(None)


class _GmailPool:
    def __init__(self, conn: _GmailConnection) -> None:
        self._conn = conn

    def acquire(self) -> _AsyncContext:
        return _AsyncContext(self._conn)


def _gmail_generator(
    conn: _GmailConnection,
    *,
    tenant_id: UUID | None = None,
) -> GmailPubSubGenerator:
    email = "exact@example.com"
    tenant_ids = {email: tenant_id} if tenant_id is not None else None
    return GmailPubSubGenerator(
        app=FastAPI(),
        pool=_GmailPool(conn),
        mailboxes={email: object()},
        tenant_ids_by_email=tenant_ids,
    )


async def test_gmail_fetches_all_mailbox_bindings_and_rejects_ambiguity() -> None:
    conn = _GmailConnection([
        {
            "tenant_id": uuid4(),
            "gmail_installation_id": uuid4(),
            "subscription_name": "subscription-1",
        },
        {
            "tenant_id": uuid4(),
            "gmail_installation_id": uuid4(),
            "subscription_name": "subscription-2",
        },
    ])
    generator = _gmail_generator(conn)

    with pytest.raises(ValueError, match="ambiguous.*matches=2"):
        await generator._find_existing_binding(
            conn,
            email="exact@example.com",
            tenant_id=None,
        )

    query, _ = conn.fetch_calls[0]
    assert "LIMIT 1" not in query
    assert "teardown_at IS NULL" in query


async def test_gmail_explicit_tenant_binding_rejects_a_missing_watch() -> None:
    tenant_id = uuid4()
    conn = _GmailConnection([])
    generator = _gmail_generator(conn, tenant_id=tenant_id)

    with pytest.raises(ValueError, match=r"matches=0"):
        await generator._seed_db()

    query, args = conn.fetch_calls[0]
    assert "w.tenant_id = $2" in query
    assert "LIMIT 1" not in query
    assert args == ("exact@example.com", tenant_id)
