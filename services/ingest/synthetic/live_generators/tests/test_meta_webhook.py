"""Standalone production-router coverage for Meta live generators."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from fastapi import FastAPI

from services.app.gateway import facebook_pages_router, whatsapp_router
from services.app.gateway.facebook_pages_router import build_facebook_pages_router
from services.app.gateway.whatsapp_router import build_whatsapp_router
from services.ingest.synthetic.live_generators.meta_webhook import (
    FacebookPagesWebhookGenerator,
    WhatsAppWebhookGenerator,
)


class _AsyncContext:
    def __init__(self, value: Any) -> None:
        self._value = value

    async def __aenter__(self) -> Any:
        return self._value

    async def __aexit__(self, *args: Any) -> None:
        return None


class _Connection:
    def __init__(self, router_row: dict[str, Any] | None) -> None:
        self._router_row = router_row
        self.fetchrow_calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.fetchrow_calls.append((query, args))
        return self._router_row


class _Pool:
    def __init__(
        self,
        *,
        exact_rows: list[dict[str, Any]],
        router_row: dict[str, Any] | None,
    ) -> None:
        self.exact_rows = exact_rows
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.conn = _Connection(router_row)

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((query, args))
        return self.exact_rows

    def acquire(self) -> _AsyncContext:
        return _AsyncContext(self.conn)


class _SecretStore:
    def __init__(self, values: dict[str, str]) -> None:
        self._values = values

    async def get(self, ref: str, *, tenant_id: Any) -> bytes:
        return self._values[ref].encode("utf-8")


def _app(pool: _Pool, source: str) -> FastAPI:
    app = FastAPI()
    app.state.deps = SimpleNamespace(
        pool=pool,
        actor_repo=object(),
        alias_repo=object(),
        embedder=None,
    )
    if source == "whatsapp":
        app.include_router(build_whatsapp_router())
    else:
        app.include_router(build_facebook_pages_router())
    return app


def _whatsapp_install() -> dict[str, Any]:
    return {
        "id": uuid4(),
        "tenant_id": uuid4(),
        "phone_number_id": "phone-exact",
        "waba_id": "waba-exact",
        "display_phone_number": "+15551234567",
        "app_secret": None,
        "app_secret_ref": "secret-ref-wa",
        "enabled": True,
    }


def _facebook_install() -> dict[str, Any]:
    return {
        "id": uuid4(),
        "tenant_id": uuid4(),
        "page_id": "page-exact",
        "page_name": "Exact Page",
        "app_secret_ref": "secret-ref-fb",
        "enabled": True,
    }


@pytest.fixture(autouse=True)
def _signed_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WHATSAPP_ALLOW_UNSIGNED", raising=False)
    monkeypatch.delenv("FACEBOOK_PAGES_ALLOW_UNSIGNED", raising=False)


async def test_whatsapp_generator_uses_real_signed_router_and_mints_fresh_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install = _whatsapp_install()
    pool = _Pool(exact_rows=[install], router_row=install)
    captured: list[dict[str, Any]] = []

    async def fake_ingest(
        deps: Any,
        tenant_id: Any,
        channel: str,
        item_payload: dict[str, Any],
        headers: dict[str, str],
    ) -> dict[str, Any]:
        captured.append(item_payload)
        return {
            "channel": channel,
            "observation_id": str(uuid4()),
            "deduped": False,
        }

    monkeypatch.setattr(whatsapp_router, "_ingest_item", fake_ingest)
    target = SimpleNamespace(
        tenant_id=install["tenant_id"],
        whatsapp_phone_number_id=install["phone_number_id"],
    )
    async with WhatsAppWebhookGenerator(
        app=_app(pool, "whatsapp"),
        pool=pool,
        secret_store=_SecretStore({"secret-ref-wa": "wa-app-secret"}),
    ) as generator:
        first = await generator.simulate_message(
            target=target,
            content="first",
        )
        second = await generator.simulate_message(
            target=target,
            content="second",
        )
        tampered = await generator.simulate_message(
            target=target,
            tamper_signature=True,
        )

    assert first.http_status == second.http_status == 200
    assert first.response_body["tenant_id"] == str(install["tenant_id"])
    assert first.message_id != second.message_id
    assert captured[0]["message"]["text"]["body"] == "first"
    assert captured[1]["message"]["text"]["body"] == "second"
    assert tampered.http_status == 401
    assert len(captured) == 2


async def test_facebook_generator_uses_real_signed_router_and_mints_fresh_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install = _facebook_install()
    pool = _Pool(exact_rows=[install], router_row=install)
    captured: list[dict[str, Any]] = []

    async def fake_ingest(
        deps: Any,
        tenant_id: Any,
        item_payload: dict[str, Any],
        headers: dict[str, str],
    ) -> dict[str, Any]:
        captured.append(item_payload)
        return {
            "channel": "facebook_pages:message",
            "observation_id": str(uuid4()),
            "deduped": False,
        }

    monkeypatch.setattr(facebook_pages_router, "_ingest_item", fake_ingest)
    target = SimpleNamespace(
        tenant_id=install["tenant_id"],
        facebook_page_id=install["page_id"],
    )
    async with FacebookPagesWebhookGenerator(
        app=_app(pool, "facebook_pages"),
        pool=pool,
        secret_store=_SecretStore({"secret-ref-fb": "fb-app-secret"}),
    ) as generator:
        first = await generator.simulate_message(
            target=target,
            content="first",
        )
        second = await generator.simulate_message(
            target=target,
            content="second",
        )
        tampered = await generator.simulate_message(
            target=target,
            tamper_signature=True,
        )

    assert first.http_status == second.http_status == 200
    assert first.response_body["tenant_id"] == str(install["tenant_id"])
    assert first.message_id != second.message_id
    assert captured[0]["message"]["text"] == "first"
    assert captured[1]["message"]["text"] == "second"
    assert tampered.http_status == 401
    assert len(captured) == 2


@pytest.mark.parametrize("match_count", [0, 2])
@pytest.mark.parametrize("source", ["whatsapp", "facebook_pages"])
async def test_meta_generators_reject_missing_or_ambiguous_exact_scope(
    source: str,
    match_count: int,
) -> None:
    install = (
        _whatsapp_install()
        if source == "whatsapp"
        else _facebook_install()
    )
    pool = _Pool(
        exact_rows=[install] * match_count,
        router_row=install,
    )
    app = _app(pool, source)
    if source == "whatsapp":
        generator: Any = WhatsAppWebhookGenerator(app=app, pool=pool)
        call = generator._resolve_install(
            install["tenant_id"],
            install["phone_number_id"],
        )
        scope_fragment = "phone_number_id = $2"
        scope = install["phone_number_id"]
    else:
        generator = FacebookPagesWebhookGenerator(app=app, pool=pool)
        call = generator._resolve_install(
            install["tenant_id"],
            install["page_id"],
        )
        scope_fragment = "page_id = $2"
        scope = install["page_id"]

    with pytest.raises(ValueError, match=rf"matches={match_count}"):
        await call

    query, args = pool.fetch_calls[0]
    assert "tenant_id = $1" in query
    assert scope_fragment in query
    assert "LIMIT 1" not in query
    assert args == (install["tenant_id"], scope)


@pytest.mark.parametrize("source", ["whatsapp", "facebook_pages"])
async def test_meta_generators_wire_router_kafka_cutover_dependencies(
    source: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install = (
        _whatsapp_install()
        if source == "whatsapp"
        else _facebook_install()
    )
    if source == "facebook_pages":
        install["app_secret_ref"] = None
        monkeypatch.setenv("FACEBOOK_APP_SECRET", "meta-app-secret")
    secret_ref = install["app_secret_ref"]
    secret_values = (
        {secret_ref: "meta-app-secret"}
        if isinstance(secret_ref, str)
        else {}
    )
    pool = _Pool(exact_rows=[install], router_row=install)
    app = _app(pool, source)
    producer = object()
    s3 = object()

    class _Flags:
        async def kafka_path_enabled(self, tenant_id: Any) -> bool:
            return True

    async def fake_publish(*args: Any, **kwargs: Any) -> bool:
        return True

    if source == "whatsapp":
        monkeypatch.setattr(
            whatsapp_router,
            "_publish_items_kafka",
            fake_publish,
        )
        generator_class: Any = WhatsAppWebhookGenerator
        target = SimpleNamespace(
            tenant_id=install["tenant_id"],
            whatsapp_phone_number_id=install["phone_number_id"],
        )
    else:
        monkeypatch.setattr(
            facebook_pages_router,
            "_publish_items_kafka",
            fake_publish,
        )
        generator_class = FacebookPagesWebhookGenerator
        target = SimpleNamespace(
            tenant_id=install["tenant_id"],
            facebook_page_id=install["page_id"],
        )

    async with generator_class(
        app=app,
        pool=pool,
        secret_store=_SecretStore(secret_values),
        app_secret="meta-app-secret",
        kafka_producer=producer,
        s3_raw_client=s3,
        tenant_flags=_Flags(),
    ) as generator:
        result = await generator.simulate_message(target=target)

    assert result.http_status == 202
    assert result.response_body["path"] == "kafka"
    assert app.state.kafka_producer is producer
    assert app.state.s3_raw_client is s3
