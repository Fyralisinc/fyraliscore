"""Facebook Page / Messenger webhook ingress."""

from __future__ import annotations

import hmac
import json
import os
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from lib.shared.env import is_prod
from lib.shared.errors import (
    CompanyOSError,
    SecretNotFoundError,
    SecretStoreError,
    ValidationError,
)
from lib.shared.secrets import build_secret_store
from services.app.gateway.deps import get_gateway_deps
from services.ingest.ingestion.core import ingest
from services.ingest.ingestion.kafka.flush_batcher import coalesced_flush
from services.ingest.ingestion.shadow_write import (
    CUTOVER_FLUSH_TIMEOUT_SEC,
    shadow_write_raw,
)
from services.ingest.integrations.meta_signature import verify_signature

log = structlog.get_logger("facebook_pages.webhook")

_WEBHOOK_PATH = "/integrations/facebook_pages/webhook"
_CHANNEL = "facebook_pages:message"


def _deps_or_503(request: Request) -> Any:
    try:
        return get_gateway_deps(request)
    except RuntimeError:
        return None


def _secret_store_for_request(request: Request, pool: Any) -> Any:
    state = request.app.state
    runtime = getattr(state, "integration_runtime", None)
    store = getattr(runtime, "secret_store", None) if runtime is not None else None
    if store is None:
        store = getattr(state, "secret_store", None)
    if store is None:
        store = build_secret_store(pool)
        state.secret_store = store
    return store


def _dev_env_secret(name: str) -> str | None:
    if is_prod():
        return None
    return os.environ.get(name)


def _unsigned_webhooks_allowed() -> bool:
    return (not is_prod()) and os.environ.get("FACEBOOK_PAGES_ALLOW_UNSIGNED") == "1"


async def _resolve_secret(
    secret_store: Any,
    install: dict[str, Any],
    *,
    ref_field: str,
    label: str,
) -> str | None:
    ref = install.get(ref_field)
    if not ref:
        return None
    try:
        raw = await secret_store.get(str(ref), tenant_id=install["tenant_id"])
    except (SecretNotFoundError, SecretStoreError, ValueError) as exc:
        raise SecretStoreError(f"{label} unavailable", reason=label) from exc
    return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)


async def _verify_token_matches_installation(
    pool: Any,
    secret_store: Any,
    presented_token: str,
) -> bool:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, tenant_id, verify_token_ref
              FROM facebook_page_installations
             WHERE enabled = true
            """
        )
    for row in rows:
        install = dict(row)
        try:
            token = await _resolve_secret(
                secret_store,
                install,
                ref_field="verify_token_ref",
                label="verify_token",
            )
        except SecretStoreError:
            continue
        if token is not None and hmac.compare_digest(token, presented_token):
            return True
    return False


def _first_page_id(payload: dict[str, Any]) -> str | None:
    for entry in payload.get("entry") or []:
        if isinstance(entry, dict):
            page_id = entry.get("id")
            if isinstance(page_id, str) and page_id:
                return page_id
    return None


async def _lookup_installation(pool: Any, page_id: str) -> dict[str, Any] | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, tenant_id, page_id, page_name, app_secret_ref,
                   verify_token_ref, enabled
              FROM facebook_page_installations
             WHERE page_id = $1
            """,
            page_id,
        )
    return dict(row) if row is not None else None


def _iter_messaging_items(payload: dict[str, Any]):
    for entry in payload.get("entry") or []:
        if not isinstance(entry, dict):
            continue
        page_id = entry.get("id")
        for messaging in entry.get("messaging") or []:
            if isinstance(messaging, dict):
                yield page_id, messaging


async def _ingest_item(
    deps: Any,
    tenant_id: UUID,
    item_payload: dict[str, Any],
    headers: dict[str, str],
) -> dict[str, Any] | None:
    try:
        result = await ingest(
            _CHANNEL,
            item_payload,
            pool=deps.pool,
            tenant_id=tenant_id,
            actor_repo=deps.actor_repo,
            alias_repo=deps.alias_repo,
            embedder=deps.embedder,
            request_headers=headers,
        )
    except (ValidationError, CompanyOSError) as exc:
        log.warning(
            "facebook_pages.item_rejected",
            code=getattr(exc, "code", "error"),
            message=getattr(exc, "message", str(exc))[:200],
        )
        return None
    return {
        "channel": _CHANNEL,
        "observation_id": str(result.observation.id),
        "deduped": result.deduped,
    }


def _dataplane_runtime(request: Request) -> tuple[Any, Any, Any]:
    state = request.app.state
    ir = getattr(state, "integration_runtime", None)

    def attr(name: str) -> Any:
        if ir is not None:
            value = getattr(ir, name, None)
            if value is not None:
                return value
        return getattr(state, name, None)

    return attr("kafka_producer"), attr("s3_raw_client"), attr("tenant_flags")


async def _publish_items_kafka(
    items: list[dict[str, Any]],
    *,
    tenant_id: UUID,
    page_id: str,
    kafka_producer: Any,
    s3_client: Any,
) -> bool:
    try:
        for item in items:
            raw_body = json.dumps(item, separators=(",", ":")).encode("utf-8")
            await shadow_write_raw(
                tenant_id=tenant_id,
                source="facebook_pages",  # type: ignore[arg-type]
                ingress_kind="webhook",
                raw_body=raw_body,
                s3_client=s3_client,
                kafka_producer=kafka_producer,
                ingress_metadata={"event_type": "message", "page_id": page_id},
            )
        remaining = await coalesced_flush(
            kafka_producer,
            timeout_seconds=CUTOVER_FLUSH_TIMEOUT_SEC,
        )
        if remaining:
            log.warning("facebook_pages.kafka_flush_incomplete", remaining=remaining)
            return False
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "facebook_pages.kafka_publish_failed",
            error_type=type(exc).__name__,
            error_message=str(exc)[:200],
        )
        return False


def _message_item(
    *,
    page_id: str,
    page_name: str | None,
    messaging: dict[str, Any],
) -> dict[str, Any] | None:
    message = messaging.get("message")
    postback = messaging.get("postback")
    if not isinstance(message, dict) and not isinstance(postback, dict):
        return None
    return {
        "source": "webhook",
        "page_id": page_id,
        "page_name": page_name,
        "messaging": messaging,
        "sender": messaging.get("sender"),
        "recipient": messaging.get("recipient"),
        "timestamp": messaging.get("timestamp"),
        "message": message if isinstance(message, dict) else None,
        "postback": postback if isinstance(postback, dict) else None,
    }


def build_facebook_pages_router() -> APIRouter:
    router = APIRouter(tags=["facebook_pages"])

    @router.get(_WEBHOOK_PATH, include_in_schema=False)
    async def verify_webhook(
        request: Request,
        hub_mode: str | None = Query(None, alias="hub.mode"),
        hub_verify_token: str | None = Query(None, alias="hub.verify_token"),
        hub_challenge: str | None = Query(None, alias="hub.challenge"),
    ) -> Any:
        ok = bool(hub_mode == "subscribe" and hub_verify_token)
        env_token = _dev_env_secret("FACEBOOK_WEBHOOK_VERIFY_TOKEN")
        matched = bool(
            ok
            and env_token is not None
            and hub_verify_token is not None
            and hmac.compare_digest(hub_verify_token, env_token)
        )
        if ok and not matched:
            deps = _deps_or_503(request)
            if deps is not None and deps.pool is not None:
                try:
                    matched = await _verify_token_matches_installation(
                        deps.pool,
                        _secret_store_for_request(request, deps.pool),
                        hub_verify_token,
                    )
                except SecretStoreError:
                    return PlainTextResponse(
                        "secret store unavailable",
                        status_code=503,
                    )
        if matched and hub_challenge is not None:
            return PlainTextResponse(hub_challenge, status_code=200)
        return PlainTextResponse("verification failed", status_code=403)

    @router.post(_WEBHOOK_PATH)
    async def receive_webhook(request: Request) -> JSONResponse:
        raw = await request.body()
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return JSONResponse({"status": "bad_json"}, status_code=400)
        if not isinstance(payload, dict):
            return JSONResponse({"status": "bad_payload"}, status_code=400)

        deps = _deps_or_503(request)
        if deps is None or deps.pool is None:
            return JSONResponse({"status": "deps_unavailable"}, status_code=503)

        page_id = _first_page_id(payload)
        if not page_id:
            return JSONResponse({"status": "no_page_id"}, status_code=400)

        install = await _lookup_installation(deps.pool, page_id)
        if install is None or not install.get("enabled"):
            log.warning("facebook_pages.unknown_installation", page_id=page_id)
            return JSONResponse(
                {"status": "ignored", "reason": "unknown_or_disabled_installation"},
                status_code=200,
            )

        if not _unsigned_webhooks_allowed():
            try:
                app_secret = await _resolve_secret(
                    _secret_store_for_request(request, deps.pool),
                    install,
                    ref_field="app_secret_ref",
                    label="app_secret",
                )
            except SecretStoreError:
                return JSONResponse(
                    {"status": "app_secret_unavailable"}, status_code=503
                )
            app_secret = app_secret or _dev_env_secret("FACEBOOK_APP_SECRET")
            if not app_secret:
                return JSONResponse(
                    {"status": "no_app_secret_configured"}, status_code=503
                )
            if not verify_signature(
                app_secret,
                raw,
                request.headers.get("X-Hub-Signature-256"),
            ):
                return JSONResponse({"status": "signature_invalid"}, status_code=401)

        tenant_id: UUID = install["tenant_id"]
        headers = {"x-facebook-page-id": page_id}
        items: list[dict[str, Any]] = []
        for entry_page_id, messaging in _iter_messaging_items(payload):
            item = _message_item(
                page_id=str(entry_page_id or page_id),
                page_name=install.get("page_name"),
                messaging=messaging,
            )
            if item is not None:
                items.append(item)

        kafka_producer, s3_client, tenant_flags = _dataplane_runtime(request)
        use_kafka = False
        if (
            items
            and kafka_producer is not None
            and s3_client is not None
            and tenant_flags is not None
        ):
            try:
                use_kafka = await tenant_flags.kafka_path_enabled(tenant_id)
            except Exception:  # noqa: BLE001
                use_kafka = False

        if use_kafka and await _publish_items_kafka(
            items,
            tenant_id=tenant_id,
            page_id=page_id,
            kafka_producer=kafka_producer,
            s3_client=s3_client,
        ):
            return JSONResponse(
                {
                    "status": "accepted",
                    "path": "kafka",
                    "tenant_id": str(tenant_id),
                    "messages": len(items),
                },
                status_code=202,
            )

        results: list[dict[str, Any]] = []
        for item in items:
            result = await _ingest_item(deps, tenant_id, item, headers)
            if result:
                results.append(result)
        return JSONResponse(
            {
                "status": "accepted",
                "path": "inline",
                "tenant_id": str(tenant_id),
                "messages": len(items),
                "ingested": len(results),
                "results": results,
            },
            status_code=200,
        )

    return router


__all__ = ["build_facebook_pages_router"]
