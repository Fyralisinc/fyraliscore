"""Instagram Messaging webhook ingress.

Routes:
  GET  /integrations/instagram/webhook  Meta subscribe handshake
  POST /integrations/instagram/webhook  HMAC verify, route tenant, fan out records
"""
from __future__ import annotations

import asyncio
import hmac
import json
import os
from datetime import datetime, timezone
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
from lib.shared.secrets import build_secret_store, load_app_secret_text_from_env
from services.app.gateway.deps import get_gateway_deps
from services.ingest.ingestion.core import ingest
from services.ingest.ingestion.kafka.flush_batcher import coalesced_flush
from services.ingest.ingestion.shadow_write import (
    CUTOVER_FLUSH_TIMEOUT_SEC,
    shadow_write_raw,
)
from services.ingest.integrations.instagram.records import (
    first_business_account_id,
    iter_webhook_records,
)
from services.ingest.integrations.instagram.signature import verify_signature
from services.ingest.integrations.instagram.onboarding import (
    record_webhook_contact,
    schedule_conversation_discovery,
)
from services.ingest.integrations.instagram.client import InstagramClient


log = structlog.get_logger("instagram.webhook")

_WEBHOOK_PATH = "/integrations/instagram/webhook"


class _InstagramSecretResolutionError(RuntimeError):
    def __init__(self, *, label: str, original: BaseException) -> None:
        super().__init__(f"{label} could not be resolved")
        self.label = label
        self.original = original


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


def _deployment_secret(name: str) -> str | None:
    """Resolve app-level Meta secrets without ever persisting them per tenant."""
    value = load_app_secret_text_from_env(name)
    return value.strip() if value else None


def _alias_discovery_max_candidates() -> int:
    try:
        return max(
            1,
            min(32, int(os.environ.get("INSTAGRAM_WEBHOOK_ALIAS_DISCOVERY_MAX_CANDIDATES", "8"))),
        )
    except ValueError:
        return 8


def _unsigned_webhooks_allowed() -> bool:
    return (not is_prod()) and os.environ.get("INSTAGRAM_ALLOW_UNSIGNED") == "1"


def _decode_secret_bytes(value: bytes, *, label: str) -> str:
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _InstagramSecretResolutionError(label=label, original=exc) from exc


async def _resolve_route_secret(
    secret_store: Any,
    route: dict[str, Any],
    *,
    ref_field: str,
    label: str,
) -> str | None:
    ref = route.get(ref_field)
    if not ref:
        return None
    try:
        raw = await secret_store.get(str(ref), tenant_id=route["tenant_id"])
    except (SecretNotFoundError, SecretStoreError, ValueError) as exc:
        raise _InstagramSecretResolutionError(label=label, original=exc) from exc
    return _decode_secret_bytes(raw, label=label)


async def _verify_token_matches_route(
    pool: Any,
    secret_store: Any,
    presented_token: str,
) -> bool:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, tenant_id, verify_token_ref
              FROM instagram_webhook_routes
             WHERE enabled = TRUE
            """
        )
    for row in rows:
        route = dict(row)
        try:
            token = await _resolve_route_secret(
                secret_store,
                route,
                ref_field="verify_token_ref",
                label="verify_token",
            )
        except _InstagramSecretResolutionError as exc:
            log.warning(
                "instagram.verify_token_ref_unresolvable",
                route_id=str(route.get("id")),
                error_type=type(exc.original).__name__,
            )
            continue
        if token is not None and hmac.compare_digest(token, presented_token):
            return True
    return False


async def _lookup_route(pool: Any, ig_business_account_id: str) -> dict[str, Any] | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT r.id, r.tenant_id, r.instagram_installation_id,
                   r.ig_business_account_id, r.page_id, r.app_secret_ref,
                   r.verify_token_ref, r.webhook_delivery_account_id, r.enabled
              FROM instagram_webhook_routes r
              JOIN instagram_installations i ON i.id = r.instagram_installation_id
             WHERE (
                    r.ig_business_account_id = $1
                 OR r.webhook_delivery_account_id = $1
                 OR r.page_id = $1
             )
               AND r.enabled = TRUE
               AND i.disabled_at IS NULL
               AND i.connection_status = 'active'
             ORDER BY CASE
                        WHEN r.ig_business_account_id = $1 THEN 0
                        WHEN r.webhook_delivery_account_id = $1 THEN 1
                        ELSE 2
                      END
             LIMIT 1
            """,
            ig_business_account_id,
        )
    return dict(row) if row is not None else None


async def _delivery_alias_matches(
    route: dict[str, Any],
    *,
    delivery_account_id: str,
    secret_store: Any,
) -> bool:
    client = InstagramClient(
        base_url=str(route["base_url"]),
        secret_store=secret_store,
        secret_ref=str(route["access_token_ref"]),
        tenant_id=route["tenant_id"],
    )
    try:
        account = await asyncio.wait_for(
            client.validate_account(delivery_account_id),
            timeout=3.0,
        )
    except Exception:  # The signed payload is still acknowledged below.
        return False
    finally:
        await client.aclose()
    return str(account.get("id") or "").strip() == str(route["ig_business_account_id"])


async def _resolve_delivery_alias_route(
    pool: Any,
    *,
    delivery_account_id: str,
    secret_store: Any,
) -> dict[str, Any] | None:
    """Bind Meta's delivery id after proving it resolves to an installation.

    New OAuth exchanges persist ``user_id`` directly. This bounded fallback is
    for legacy exchanges and is reached only after a valid app signature.
    """
    app_id = os.environ.get("INSTAGRAM_APP_ID", "").strip()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT r.id, r.tenant_id, r.instagram_installation_id,
                   r.ig_business_account_id, r.page_id, r.app_secret_ref,
                   r.verify_token_ref, r.webhook_delivery_account_id, r.enabled,
                   i.base_url, i.access_token_ref
              FROM instagram_webhook_routes r
              JOIN instagram_installations i ON i.id = r.instagram_installation_id
             WHERE r.enabled = TRUE
               AND r.webhook_delivery_account_id IS NULL
               AND i.disabled_at IS NULL
               AND i.connection_status = 'active'
               AND i.access_token_ref IS NOT NULL
               AND (r.app_id = $1 OR r.app_id IS NULL)
             ORDER BY r.updated_at DESC
             LIMIT $2
            """,
            app_id,
            _alias_discovery_max_candidates(),
        )
    candidates = [dict(row) for row in rows]
    matches = await asyncio.gather(
        *(
            _delivery_alias_matches(
                route,
                delivery_account_id=delivery_account_id,
                secret_store=secret_store,
            )
            for route in candidates
        ),
        return_exceptions=True,
    )
    for route, matched in zip(candidates, matches, strict=True):
        if matched is not True:
            continue
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE instagram_webhook_routes
                       SET webhook_delivery_account_id = $1, updated_at = now()
                     WHERE id = $2
                       AND (webhook_delivery_account_id IS NULL
                            OR webhook_delivery_account_id = $1)
                    """,
                    delivery_account_id,
                    route["id"],
                )
        except Exception:  # A concurrent delivery may have bound the alias.
            return await _lookup_route(pool, delivery_account_id)
        route["webhook_delivery_account_id"] = delivery_account_id
        log.info("instagram.webhook_delivery_alias_bound", route_id=str(route["id"]))
        return route
    return None


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


async def _ingest_item(
    deps: Any,
    tenant_id: UUID,
    item_payload: dict[str, Any],
    headers: dict[str, str],
) -> dict[str, Any] | None:
    try:
        result = await ingest(
            "instagram:message",
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
            "instagram.item_rejected",
            code=getattr(exc, "code", "error"),
            message=getattr(exc, "message", str(exc))[:200],
        )
        return None
    return {
        "channel": "instagram:message",
        "observation_id": str(result.observation.id),
        "deduped": result.deduped,
    }


async def _publish_items_kafka(
    items: list[dict[str, Any]],
    *,
    tenant_id: UUID,
    kafka_producer: Any,
    s3_client: Any,
) -> bool:
    try:
        for item in items:
            raw_body = json.dumps(item, separators=(",", ":")).encode("utf-8")
            await shadow_write_raw(
                tenant_id=tenant_id,
                source="instagram",  # type: ignore[arg-type]
                ingress_kind="webhook",
                raw_body=raw_body,
                s3_client=s3_client,
                kafka_producer=kafka_producer,
                ingress_metadata={"event_type": str(item.get("event_type") or "unknown")},
            )
        remaining = await coalesced_flush(
            kafka_producer,
            timeout_seconds=CUTOVER_FLUSH_TIMEOUT_SEC,
        )
        if remaining:
            log.warning("instagram.kafka_flush_incomplete", remaining=remaining)
            return False
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "instagram.kafka_publish_failed",
            error_type=type(exc).__name__,
            error_message=str(exc)[:200],
        )
        return False


def _parse_occurred_at(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


async def _upsert_webhook_conversations(
    pool: Any,
    *,
    tenant_id: UUID,
    installation_id: UUID,
    items: list[dict[str, Any]],
) -> None:
    if not items:
        return
    try:
        discovered_new_thread = False
        for item in items:
            if item.get("_fyralis_record_type") != "message":
                continue
            discovered_new_thread = (
                await record_webhook_contact(
                    pool,
                    tenant_id=tenant_id,
                    installation_id=installation_id,
                    ig_business_account_id=str(item.get("ig_business_account_id") or ""),
                    customer_id=str(item.get("customer_id") or "").strip() or None,
                    occurred_at=_parse_occurred_at(item.get("occurred_at")),
                    inbound=item.get("direction") != "outbound",
                )
                or discovered_new_thread
            )
        if discovered_new_thread:
            await schedule_conversation_discovery(
                pool,
                tenant_id=tenant_id,
                installation_id=installation_id,
            )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "instagram.conversation_upsert_failed",
            error_type=type(exc).__name__,
        )


def build_instagram_router() -> APIRouter:
    router = APIRouter(tags=["instagram"])

    @router.get(_WEBHOOK_PATH, include_in_schema=False)
    async def verify_webhook(
        request: Request,
        hub_mode: str | None = Query(None, alias="hub.mode"),
        hub_verify_token: str | None = Query(None, alias="hub.verify_token"),
        hub_challenge: str | None = Query(None, alias="hub.challenge"),
    ) -> Any:
        env_token = _deployment_secret("INSTAGRAM_VERIFY_TOKEN") or _dev_env_secret("INSTAGRAM_VERIFY_TOKEN")
        ok = bool(hub_mode == "subscribe" and hub_verify_token)
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
                    secret_store = _secret_store_for_request(request, deps.pool)
                    matched = await _verify_token_matches_route(
                        deps.pool,
                        secret_store,
                        hub_verify_token,
                    )
                except SecretStoreError:
                    return PlainTextResponse("secret store unavailable", status_code=503)
        if matched and hub_challenge is not None:
            log.info("instagram.webhook_verified")
            return PlainTextResponse(hub_challenge, status_code=200)
        log.warning("instagram.webhook_verify_failed", mode=hub_mode)
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

        route_key = first_business_account_id(payload)
        if not route_key:
            return JSONResponse({"status": "no_instagram_route_key"}, status_code=400)

        if not _unsigned_webhooks_allowed():
            app_secret = _deployment_secret("INSTAGRAM_APP_SECRET") or _dev_env_secret("INSTAGRAM_APP_SECRET")
            if not app_secret:
                # Temporary compatibility for rows created by the original
                # implementation. New installs never write app secrets here.
                route = await _lookup_route(deps.pool, route_key)
                if route is not None:
                    try:
                        secret_store = _secret_store_for_request(request, deps.pool)
                        app_secret = await _resolve_route_secret(
                            secret_store,
                            route,
                            ref_field="app_secret_ref",
                            label="app_secret",
                        )
                    except (SecretStoreError, _InstagramSecretResolutionError):
                        app_secret = None
            if not app_secret:
                return JSONResponse({"status": "no_app_secret_configured"}, status_code=503)
            sig = request.headers.get("X-Hub-Signature-256")
            if not verify_signature(app_secret, raw, sig):
                log.warning("instagram.signature_invalid", route_key=route_key)
                return JSONResponse({"status": "signature_invalid"}, status_code=401)

        route = await _lookup_route(deps.pool, route_key)
        if route is None:
            try:
                secret_store = _secret_store_for_request(request, deps.pool)
                route = await _resolve_delivery_alias_route(
                    deps.pool,
                    delivery_account_id=route_key,
                    secret_store=secret_store,
                )
            except SecretStoreError:
                route = None
        if route is None or not route.get("enabled"):
            log.info("instagram.unknown_or_disabled_installation", route_key=route_key)
            return JSONResponse(
                {"status": "ignored", "reason": "unknown_or_disabled_installation"},
                status_code=200,
            )

        tenant_id: UUID = route["tenant_id"]
        business_id = str(route["ig_business_account_id"])
        items = iter_webhook_records(
            payload,
            default_ig_business_account_id=business_id,
            page_id=route.get("page_id"),
        )
        await _upsert_webhook_conversations(
            deps.pool,
            tenant_id=tenant_id,
            installation_id=route["instagram_installation_id"],
            items=items,
        )
        headers = {"x-instagram-business-account-id": business_id}

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
            kafka_producer=kafka_producer,
            s3_client=s3_client,
        ):
            return JSONResponse(
                {
                    "status": "accepted",
                    "path": "kafka",
                    "tenant_id": str(tenant_id),
                    "items": len(items),
                },
                status_code=202,
            )

        results: list[dict[str, Any]] = []
        for item in items:
            result = await _ingest_item(deps, tenant_id, item, headers)
            if result is not None:
                results.append(result)
        return JSONResponse(
            {
                "status": "accepted",
                "path": "inline",
                "tenant_id": str(tenant_id),
                "items": len(items),
                "ingested": len(results),
                "results": results,
            },
            status_code=200,
        )

    return router


__all__ = ["build_instagram_router"]
