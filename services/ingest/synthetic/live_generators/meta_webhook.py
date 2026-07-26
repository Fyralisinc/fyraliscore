"""Production-path synthetic live webhooks for Meta messaging sources."""
from __future__ import annotations

import json
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg
import httpx
from fastapi import FastAPI

from lib.shared.secrets import build_secret_store
from services.ingest.integrations.whatsapp.signature import sign_payload


_LIVE_BASE_S = 1_781_000_000
_LIVE_BASE_MS = _LIVE_BASE_S * 1000


@dataclass
class MetaWebhookResult:
    source: str
    provider_scope: str
    message_id: str
    http_status: int
    external_hint: str
    response_body: dict[str, Any] = field(default_factory=dict)
    tenant_id: UUID | None = None
    was_tamper: bool = False


class _MetaWebhookGenerator:
    def __init__(
        self,
        *,
        app: FastAPI,
        pool: asyncpg.Pool,
        secret_store: Any = None,
        app_secret: str | None = None,
        kafka_producer: Any = None,
        s3_raw_client: Any = None,
        tenant_flags: Any = None,
    ) -> None:
        self._app = app
        self._pool = pool
        self._configured_secret_store = secret_store
        self._configured_app_secret = app_secret
        self._seq = 0
        self._exit_stack = AsyncExitStack()
        self._client: httpx.AsyncClient | None = None

        if secret_store is not None:
            app.state.secret_store = secret_store
        for name, value in (
            ("kafka_producer", kafka_producer),
            ("s3_raw_client", s3_raw_client),
            ("tenant_flags", tenant_flags),
        ):
            if value is not None:
                setattr(app.state, name, value)

    async def _enter(self, base_url: str) -> None:
        self._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self._app),
            base_url=base_url,
        )
        await self._exit_stack.enter_async_context(self._client)

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._exit_stack.aclose()

    def _secret_store(self) -> Any:
        state = self._app.state
        runtime = getattr(state, "integration_runtime", None)
        store = (
            getattr(runtime, "secret_store", None)
            if runtime is not None
            else None
        )
        if store is None:
            store = self._configured_secret_store
        if store is None:
            store = getattr(state, "secret_store", None)
        if store is None:
            store = build_secret_store(self._pool)
            state.secret_store = store
        return store

    async def _post(
        self,
        *,
        path: str,
        payload: dict[str, Any],
        app_secret: str,
        tamper_signature: bool,
    ) -> tuple[int, dict[str, Any]]:
        if self._client is None:
            raise RuntimeError("generator must be entered as an async context")
        raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        signature = sign_payload(app_secret, raw)
        if tamper_signature:
            signature = sign_payload(f"{app_secret}-tampered", raw)
        response = await self._client.post(
            path,
            content=raw,
            headers={
                "Content-Type": "application/json",
                "X-Hub-Signature-256": signature,
            },
        )
        try:
            body = response.json()
        except Exception:  # noqa: BLE001
            body = {"raw": response.text[:500]}
        return response.status_code, body

    @staticmethod
    def _assert_accepted(
        *,
        source: str,
        tenant_id: UUID,
        status: int,
        body: dict[str, Any],
        tamper_signature: bool,
    ) -> None:
        if tamper_signature:
            return
        if (
            status not in {200, 202}
            or body.get("status") != "accepted"
            or body.get("tenant_id") != str(tenant_id)
        ):
            raise RuntimeError(
                f"{source} synthetic webhook was not accepted by the exact "
                f"tenant binding: status={status}, body={body!r}",
            )


class WhatsAppWebhookGenerator(_MetaWebhookGenerator):
    """Send fresh WhatsApp messages through the real dedicated router."""

    async def __aenter__(self) -> "WhatsAppWebhookGenerator":
        await self._enter("http://synthetic-whatsapp")
        return self

    async def _resolve_install(
        self,
        tenant_id: UUID,
        phone_number_id: str,
    ) -> dict[str, Any]:
        rows = await self._pool.fetch(
            """
            SELECT id, tenant_id, phone_number_id, waba_id,
                   display_phone_number, app_secret, app_secret_ref, enabled
              FROM whatsapp_installations
             WHERE tenant_id = $1
               AND phone_number_id = $2
               AND enabled = true
            """,
            tenant_id,
            phone_number_id,
        )
        if len(rows) != 1:
            raise ValueError(
                "whatsapp target must resolve to exactly one enabled "
                "installation: "
                f"tenant_id={tenant_id}, phone_number_id={phone_number_id!r}, "
                f"matches={len(rows)}",
            )
        return dict(rows[0])

    async def simulate_message(
        self,
        *,
        target: Any,
        content: str = "hello from synthetic WhatsApp",
        tamper_signature: bool = False,
    ) -> MetaWebhookResult:
        from services.app.gateway.whatsapp_router import (
            _resolve_install_secret,
        )

        tenant_id = target.tenant_id
        phone_number_id = getattr(
            target,
            "whatsapp_phone_number_id",
            None,
        )
        if not isinstance(phone_number_id, str) or not phone_number_id:
            raise ValueError(
                "whatsapp target is missing whatsapp_phone_number_id",
            )
        install = await self._resolve_install(tenant_id, phone_number_id)
        app_secret = await _resolve_install_secret(
            self._secret_store(),
            install,
            ref_field="app_secret_ref",
            legacy_field="app_secret",
            label="app_secret",
        )
        app_secret = app_secret or self._configured_app_secret
        if not app_secret:
            raise ValueError(
                "whatsapp target installation has no resolvable app secret",
            )

        self._seq += 1
        message_id = f"wamid.synthetic.{install['id']}.{self._seq}"
        sender_id = f"1555000{self._seq:04d}"
        payload = {
            "object": "whatsapp_business_account",
            "entry": [{
                "id": str(install.get("waba_id") or phone_number_id),
                "changes": [{
                    "field": "messages",
                    "value": {
                        "messaging_product": "whatsapp",
                        "metadata": {
                            "display_phone_number": (
                                install.get("display_phone_number")
                                or "+15550000000"
                            ),
                            "phone_number_id": phone_number_id,
                        },
                        "contacts": [{
                            "wa_id": sender_id,
                            "profile": {"name": "Synthetic WhatsApp User"},
                        }],
                        "messages": [{
                            "id": message_id,
                            "from": sender_id,
                            "timestamp": str(_LIVE_BASE_S + self._seq),
                            "type": "text",
                            "text": {"body": content},
                        }],
                    },
                }],
            }],
        }
        status, body = await self._post(
            path="/integrations/whatsapp/webhook",
            payload=payload,
            app_secret=app_secret,
            tamper_signature=tamper_signature,
        )
        self._assert_accepted(
            source="whatsapp",
            tenant_id=tenant_id,
            status=status,
            body=body,
            tamper_signature=tamper_signature,
        )
        return MetaWebhookResult(
            source="whatsapp",
            provider_scope=phone_number_id,
            message_id=message_id,
            http_status=status,
            external_hint=f"whatsapp:{phone_number_id}:{message_id}",
            response_body=body,
            tenant_id=tenant_id,
            was_tamper=tamper_signature,
        )


class FacebookPagesWebhookGenerator(_MetaWebhookGenerator):
    """Send fresh Messenger messages through the real Facebook Pages router."""

    async def __aenter__(self) -> "FacebookPagesWebhookGenerator":
        await self._enter("http://synthetic-facebook-pages")
        return self

    async def _resolve_install(
        self,
        tenant_id: UUID,
        page_id: str,
    ) -> dict[str, Any]:
        rows = await self._pool.fetch(
            """
            SELECT id, tenant_id, page_id, page_name, app_secret_ref, enabled
              FROM facebook_page_installations
             WHERE tenant_id = $1
               AND page_id = $2
               AND enabled = true
            """,
            tenant_id,
            page_id,
        )
        if len(rows) != 1:
            raise ValueError(
                "facebook_pages target must resolve to exactly one enabled "
                "installation: "
                f"tenant_id={tenant_id}, page_id={page_id!r}, "
                f"matches={len(rows)}",
            )
        return dict(rows[0])

    async def simulate_message(
        self,
        *,
        target: Any,
        content: str = "hello from synthetic Messenger",
        tamper_signature: bool = False,
    ) -> MetaWebhookResult:
        from services.app.gateway.facebook_pages_router import _resolve_secret

        tenant_id = target.tenant_id
        page_id = getattr(target, "facebook_page_id", None)
        if not isinstance(page_id, str) or not page_id:
            raise ValueError("facebook_pages target is missing facebook_page_id")
        install = await self._resolve_install(tenant_id, page_id)
        app_secret = await _resolve_secret(
            self._secret_store(),
            install,
            ref_field="app_secret_ref",
            label="app_secret",
        )
        app_secret = app_secret or self._configured_app_secret
        if not app_secret:
            raise ValueError(
                "facebook_pages target installation has no resolvable app secret",
            )

        self._seq += 1
        message_id = f"m.synthetic.{install['id']}.{self._seq}"
        sender_id = f"psid-synthetic-{self._seq}"
        payload = {
            "object": "page",
            "entry": [{
                "id": page_id,
                "time": _LIVE_BASE_S + self._seq,
                "messaging": [{
                    "sender": {"id": sender_id},
                    "recipient": {"id": page_id},
                    "timestamp": _LIVE_BASE_MS + self._seq * 1000,
                    "message": {
                        "mid": message_id,
                        "text": content,
                    },
                }],
            }],
        }
        status, body = await self._post(
            path="/integrations/facebook_pages/webhook",
            payload=payload,
            app_secret=app_secret,
            tamper_signature=tamper_signature,
        )
        self._assert_accepted(
            source="facebook_pages",
            tenant_id=tenant_id,
            status=status,
            body=body,
            tamper_signature=tamper_signature,
        )
        return MetaWebhookResult(
            source="facebook_pages",
            provider_scope=page_id,
            message_id=message_id,
            http_status=status,
            external_hint=f"facebook_pages:{page_id}:{message_id}",
            response_body=body,
            tenant_id=tenant_id,
            was_tamper=tamper_signature,
        )


__all__ = [
    "FacebookPagesWebhookGenerator",
    "MetaWebhookResult",
    "WhatsAppWebhookGenerator",
]
