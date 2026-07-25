"""services/app/gateway/whatsapp_router.py — WhatsApp Cloud API LIVE ingestion.

A dedicated webhook ingress for WhatsApp (the generic /webhooks/{provider} router
is POST-only and single-payload; WhatsApp needs a GET verify-token handshake and
multi-item fan-out, since one delivery batches many messages + statuses). It still
runs the REAL inline `ingest()` core, so each inbound message lands as a genuine
`observations` row with dedup + a Think (T1) trigger — identical to every other
live source, just a WhatsApp-shaped front door.

Routes:
  GET  /integrations/whatsapp/webhook   Meta subscribe handshake (echo hub.challenge)
  POST /integrations/whatsapp/webhook   verify X-Hub-Signature-256 → resolve tenant
                                        by phone_number_id → fan out → ingest() each
  POST /debug/whatsapp/register         dev: upsert a whatsapp_installations row (creds)
  GET  /debug/whatsapp                  dev: live viewer (polls /recent)
  GET  /debug/whatsapp/recent           dev: recent whatsapp observations as JSON

Signature: Meta signs each POST body with HMAC-SHA256(app_secret). We resolve the
phone_number_id from the (still-unverified) body, load that install's app_secret,
then verify over the raw bytes — the phone_number_id is not a secret, so this is
safe (an attacker can't forge the HMAC without the app secret). Set
WHATSAPP_ALLOW_UNSIGNED=1 to bypass verification for local debugging only.
"""
from __future__ import annotations

import json
import hmac
import os
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

from lib.shared.env import is_prod
from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.errors import SecretNotFoundError, SecretStoreError
from lib.shared.secrets import build_secret_store
from services.app.gateway.deps import get_gateway_deps
from services.app.gateway.html_responses import trusted_static_html_response
from services.ingest.ingestion.core import ingest
from services.ingest.ingestion.kafka.flush_batcher import coalesced_flush
from services.ingest.ingestion.shadow_write import (
    CUTOVER_FLUSH_TIMEOUT_SEC,
    shadow_write_raw,
)
from services.ingest.integrations.whatsapp.signature import verify_signature
from services.ingest.source_contract import dedicated_ingress_definition


log = structlog.get_logger("whatsapp.webhook")

_INGRESS = dedicated_ingress_definition("whatsapp_webhook")
_WEBHOOK_PATH = _INGRESS.route_path


# --------------------------------------------------------------------------- #
#  helpers                                                                     #
# --------------------------------------------------------------------------- #
def _deps_or_503(request: Request) -> Any:
    try:
        return get_gateway_deps(request)
    except RuntimeError:
        return None


class _WhatsAppSecretResolutionError(RuntimeError):
    def __init__(self, *, label: str, original: BaseException) -> None:
        super().__init__(f"{label} could not be resolved")
        self.label = label
        self.original = original


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
    return (not is_prod()) and os.environ.get("WHATSAPP_ALLOW_UNSIGNED") == "1"


def _decode_secret_bytes(value: bytes, *, label: str) -> str:
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _WhatsAppSecretResolutionError(label=label, original=exc) from exc


async def _resolve_install_secret(
    secret_store: Any,
    install: dict[str, Any],
    *,
    ref_field: str,
    legacy_field: str,
    label: str,
) -> str | None:
    ref = install.get(ref_field)
    if ref:
        try:
            raw = await secret_store.get(str(ref), tenant_id=install["tenant_id"])
        except (SecretNotFoundError, SecretStoreError, ValueError) as exc:
            raise _WhatsAppSecretResolutionError(label=label, original=exc) from exc
        return _decode_secret_bytes(raw, label=label)
    legacy_value = install.get(legacy_field)
    if legacy_value and is_prod():
        raise _WhatsAppSecretResolutionError(
            label=label,
            original=RuntimeError("legacy_plaintext_secret_disabled"),
        )
    return str(legacy_value) if legacy_value else None


async def _verify_token_matches_installation(
    pool: Any,
    secret_store: Any,
    presented_token: str,
) -> bool:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, tenant_id, verify_token, verify_token_ref
              FROM whatsapp_installations
             WHERE enabled = true
            """
        )
    for row in rows:
        install = dict(row)
        try:
            token = await _resolve_install_secret(
                secret_store,
                install,
                ref_field="verify_token_ref",
                legacy_field="verify_token",
                label="verify_token",
            )
        except _WhatsAppSecretResolutionError as exc:
            log.warning(
                "whatsapp.verify_token_ref_unresolvable",
                installation_id=str(install.get("id")),
                error_type=type(exc.original).__name__,
            )
            continue
        if token is not None and hmac.compare_digest(token, presented_token):
            return True
    return False


async def _store_optional_secret(
    secret_store: Any,
    *,
    tenant_id: UUID,
    phone_number_id: str,
    body: dict[str, Any],
    key: str,
) -> str | None:
    value = body.get(key)
    if value is None or value == "":
        return None
    return await secret_store.put(
        str(value),
        label=f"whatsapp_{key}:{phone_number_id}",
        tenant_id=tenant_id,
    )


def _first_phone_number_id(payload: dict[str, Any]) -> str | None:
    for entry in payload.get("entry") or []:
        if not isinstance(entry, dict):
            continue
        for change in entry.get("changes") or []:
            if not isinstance(change, dict):
                continue
            md = (change.get("value") or {}).get("metadata") or {}
            pid = md.get("phone_number_id")
            if isinstance(pid, str) and pid:
                return pid
    return None


async def _lookup_installation(pool: Any, phone_number_id: str) -> dict[str, Any] | None:
    """Resolve a phone_number_id -> installation row before tenant context exists."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, tenant_id, phone_number_id, waba_id, display_phone_number,
                   app_secret, app_secret_ref,
                   verify_token, verify_token_ref,
                   access_token_ref,
                   enabled
              FROM whatsapp_installations
             WHERE phone_number_id = $1
            """,
            phone_number_id,
        )
    return dict(row) if row is not None else None


async def _ensure_tenant(pool: Any, tenant_id: UUID) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            tenant_id,
            "whatsapp",
        )


def _iter_change_values(payload: dict[str, Any]):
    for entry in payload.get("entry") or []:
        if not isinstance(entry, dict):
            continue
        for change in entry.get("changes") or []:
            if isinstance(change, dict) and isinstance(change.get("value"), dict):
                yield change["value"]


async def _ingest_item(
    deps: Any,
    tenant_id: UUID,
    channel: str,
    item_payload: dict[str, Any],
    headers: dict[str, str],
) -> dict[str, Any] | None:
    """Run one message/status through the real ingest() path. Returns a small
    result dict, or None if the item was rejected (logged, batch continues)."""
    try:
        result = await ingest(
            channel,
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
            "whatsapp.item_rejected",
            channel=channel,
            code=getattr(exc, "code", "error"),
            message=getattr(exc, "message", str(exc))[:200],
        )
        return None
    return {
        "channel": channel,
        "observation_id": str(result.observation.id),
        "deduped": result.deduped,
    }


def _dataplane_runtime(request: Request) -> tuple[Any, Any, Any]:
    """Resolve (kafka_producer, s3_raw_client, tenant_flags) for the Kafka
    cutover. Canonical source is ``app.state.integration_runtime`` (wired by
    the gateway); falls back to ``app.state`` attrs. Any may be None — when so,
    the router stays on the inline path. The self-contained mini-server wires
    none of these, so it is always inline."""
    state = request.app.state
    ir = getattr(state, "integration_runtime", None)

    def attr(name: str) -> Any:
        if ir is not None:
            v = getattr(ir, name, None)
            if v is not None:
                return v
        return getattr(state, name, None)

    return attr("kafka_producer"), attr("s3_raw_client"), attr("tenant_flags")


async def _publish_items_kafka(
    items: list[dict[str, Any]],
    *,
    tenant_id: UUID,
    phone_number_id: str,
    kafka_producer: Any,
    s3_client: Any,
) -> bool:
    """Kafka cutover: shadow-write one raw envelope per item to
    ``ingestion.raw.whatsapp`` (S3 PutIfAbsent + produce), then flush so the
    202 is only returned once events are DURABLY on the broker. Returns True on
    full success; False on any failure or incomplete flush (caller falls back to
    inline ingest — idempotent via external_id dedup)."""
    try:
        for item in items:
            event_type = "status" if "status" in item else "message"
            raw_body = json.dumps(item, separators=(",", ":")).encode("utf-8")
            await shadow_write_raw(
                tenant_id=tenant_id,
                source=_INGRESS.source_id,  # type: ignore[arg-type]
                ingress_kind=_INGRESS.ingress_kind,
                raw_body=raw_body,
                s3_client=s3_client,
                kafka_producer=kafka_producer,
                ingress_metadata={"event_type": event_type, "phone_number_id": phone_number_id},
            )
        remaining = await coalesced_flush(kafka_producer, timeout_seconds=CUTOVER_FLUSH_TIMEOUT_SEC)
        if remaining:
            log.warning("whatsapp.kafka_flush_incomplete", remaining=remaining)
            return False
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "whatsapp.kafka_publish_failed",
            error_type=type(exc).__name__,
            error_message=str(exc)[:200],
        )
        return False


# --------------------------------------------------------------------------- #
#  viewer page                                                                 #
# --------------------------------------------------------------------------- #
_VIEWER_PAGE = """
<!doctype html><html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>WhatsApp Live Ingestion</title>
<style nonce="__CSP_NONCE__">
  body{margin:0;font-family:Inter,system-ui,-apple-system,"Segoe UI",sans-serif;background:#0b141a;color:#e9edef}
  main{max-width:900px;margin:0 auto;padding:22px 16px}
  h1{font-size:20px;margin:0 0 4px}.sub{color:#8696a0;font-size:13px;margin-bottom:16px}
  .row{display:flex;gap:8px;align-items:center;margin-bottom:14px}
  input{background:#202c33;border:1px solid #2a3942;border-radius:6px;color:#e9edef;padding:8px 10px;font:inherit;flex:1}
  .live{display:inline-flex;align-items:center;gap:6px;color:#00a884;font-weight:600;font-size:13px}
  .dot{width:8px;height:8px;border-radius:50%;background:#00a884;animation:p 1.2s infinite}
  @keyframes p{0%,100%{opacity:1}50%{opacity:.25}}
  ul{list-style:none;margin:0;padding:0}
  li{background:#202c33;border-radius:8px;padding:10px 12px;margin-bottom:8px;border-left:3px solid #00a884}
  li.status{border-left-color:#53bdeb}
  .meta{color:#8696a0;font-size:12px;display:flex;gap:10px;margin-bottom:3px;flex-wrap:wrap}
  .txt{font-size:14px;white-space:pre-wrap;overflow-wrap:anywhere}
  .empty{color:#8696a0;text-align:center;padding:30px}
  .chip{background:#111b21;border-radius:10px;padding:1px 7px;font-size:11px}
</style></head><body><main>
  <h1>WhatsApp · live ingestion</h1>
  <div class="sub">Observations land here in real time as messages arrive. <span class="live"><span class="dot"></span>polling</span></div>
  <div class="row"><input id="tenant" placeholder="tenant_id (uuid)"/></div>
  <ul id="feed"><li class="empty">Waiting for messages…</li></ul>
</main><script nonce="__CSP_NONCE__">
  const feed=document.getElementById("feed"),tenant=document.getElementById("tenant");
  tenant.value=new URLSearchParams(location.search).get("tenant_id")||"";
  function esc(s){const d=document.createElement("div");d.textContent=s==null?"":String(s);return d.innerHTML;}
  function fmt(o){
    const c=o.content||{},isStatus=(c._whatsapp_kind==="status")||!!c.status;
    const who=isStatus?("→ "+esc(c.recipient_id||"")):esc(c.contact_name||c.from||o.source_actor_ref||"");
    const when=o.occurred_at?new Date(o.occurred_at).toLocaleString():"";
    return `<li class="${isStatus?'status':''}"><div class="meta"><span class="chip">${esc(o.source_channel)}</span><b>${who}</b><span>${when}</span>${o.deduped?'<span class="chip">dedup</span>':''}</div><div class="txt">${esc(o.content_text)}</div></li>`;
  }
  async function tick(){
    const t=tenant.value.trim(); if(!t){return;}
    try{
      const r=await fetch("/debug/whatsapp/recent?tenant_id="+encodeURIComponent(t));
      const d=await r.json();
      const items=(d.observations||[]);
      feed.innerHTML=items.length?items.map(fmt).join(""):'<li class="empty">No WhatsApp observations yet for this tenant.</li>';
    }catch(e){/* keep last view */}
  }
  setInterval(tick,2000); tick(); tenant.addEventListener("change",tick);
</script></body></html>
""".strip()


# --------------------------------------------------------------------------- #
#  router                                                                      #
# --------------------------------------------------------------------------- #
def build_whatsapp_router(*, debug_endpoints_enabled: bool = False) -> APIRouter:
    router = APIRouter(tags=["whatsapp"])

    # ---- Meta subscribe handshake (GET) ---------------------------------- #
    @router.get(_WEBHOOK_PATH, include_in_schema=False)
    async def verify_webhook(
        request: Request,
        hub_mode: str | None = Query(None, alias="hub.mode"),
        hub_verify_token: str | None = Query(None, alias="hub.verify_token"),
        hub_challenge: str | None = Query(None, alias="hub.challenge"),
    ) -> Any:
        env_token = _dev_env_secret("WHATSAPP_VERIFY_TOKEN")
        ok = bool(hub_mode == "subscribe" and hub_verify_token)
        matched = bool(
            ok
            and env_token is not None
            and hub_verify_token is not None
            and hmac.compare_digest(hub_verify_token, env_token)
        )
        # Fall back to matching any installation's verify_token/secret ref.
        if ok and not matched:
            deps = _deps_or_503(request)
            if deps is not None and deps.pool is not None:
                try:
                    secret_store = _secret_store_for_request(request, deps.pool)
                    matched = await _verify_token_matches_installation(
                        deps.pool,
                        secret_store,
                        hub_verify_token,
                    )
                except SecretStoreError as exc:
                    log.error(
                        "whatsapp.secret_store_unavailable",
                        path="verify",
                        error_type=type(exc).__name__,
                    )
                    return PlainTextResponse(
                        "secret store unavailable",
                        status_code=503,
                    )
        if matched and hub_challenge is not None:
            log.info("whatsapp.webhook_verified")
            return PlainTextResponse(hub_challenge, status_code=200)
        log.warning("whatsapp.webhook_verify_failed", mode=hub_mode)
        return PlainTextResponse("verification failed", status_code=403)

    # ---- inbound events (POST) ------------------------------------------- #
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

        phone_number_id = _first_phone_number_id(payload)
        if not phone_number_id:
            return JSONResponse({"status": "no_phone_number_id"}, status_code=400)

        install = await _lookup_installation(deps.pool, phone_number_id)
        if install is None or not install.get("enabled"):
            # Unknown/disabled number — ack 200 so Meta stops retrying, but record it.
            log.warning("whatsapp.unknown_installation", phone_number_id=phone_number_id)
            return JSONResponse(
                {"status": "ignored", "reason": "unknown_or_disabled_installation"},
                status_code=200,
            )

        allow_unsigned = _unsigned_webhooks_allowed()
        if not allow_unsigned:
            try:
                secret_store = _secret_store_for_request(request, deps.pool)
                app_secret = await _resolve_install_secret(
                    secret_store,
                    install,
                    ref_field="app_secret_ref",
                    legacy_field="app_secret",
                    label="app_secret",
                )
            except (SecretStoreError, _WhatsAppSecretResolutionError) as exc:
                original = getattr(exc, "original", exc)
                log.error(
                    "whatsapp.app_secret_unavailable",
                    phone_number_id=phone_number_id,
                    error_type=type(original).__name__,
                )
                return JSONResponse(
                    {"status": "app_secret_unavailable"},
                    status_code=503,
                )
            app_secret = app_secret or _dev_env_secret("WHATSAPP_APP_SECRET")
            if not app_secret:
                log.error("whatsapp.no_app_secret", phone_number_id=phone_number_id)
                return JSONResponse({"status": "no_app_secret_configured"}, status_code=503)
            sig = request.headers.get("X-Hub-Signature-256")
            if not verify_signature(app_secret, raw, sig):
                log.warning("whatsapp.signature_invalid", phone_number_id=phone_number_id)
                return JSONResponse({"status": "signature_invalid"}, status_code=401)

        tenant_id: UUID = install["tenant_id"]
        headers = {"x-whatsapp-phone-number-id": phone_number_id}

        # Fan the delivery out into one item per message/status. Each item is a
        # single-item dict the unified `whatsapp:message` handler branches on —
        # the SAME shape for both the inline and Kafka paths, so external_id
        # parity holds (a message processed inline and its Kafka twin dedup).
        items: list[dict[str, Any]] = []
        n_messages = n_statuses = 0
        for value in _iter_change_values(payload):
            metadata = value.get("metadata") if isinstance(value.get("metadata"), dict) else {}
            contacts = value.get("contacts") or []
            for msg in value.get("messages") or []:
                if isinstance(msg, dict):
                    n_messages += 1
                    items.append(
                        {
                            "message": msg,
                            "metadata": metadata,
                            "contacts": contacts,
                        }
                    )
            for st in value.get("statuses") or []:
                if isinstance(st, dict):
                    n_statuses += 1
                    items.append({"status": st, "metadata": metadata})

        # Data-plane decision: per-tenant Kafka cutover vs inline ingest. Mirrors
        # the generic webhook router — graceful degradation, not gate-relaxation:
        # a Kafka failure silently falls back to inline so Meta still gets a 2xx.
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
            phone_number_id=phone_number_id,
            kafka_producer=kafka_producer,
            s3_client=s3_client,
        ):
            log.info(
                "whatsapp.delivery_published_kafka",
                phone_number_id=phone_number_id,
                messages=n_messages,
                statuses=n_statuses,
            )
            return JSONResponse(
                {
                    "status": "accepted",
                    "path": "kafka",
                    "tenant_id": str(tenant_id),
                    "messages": n_messages,
                    "statuses": n_statuses,
                },
                status_code=202,
            )

        # Inline path (default, or Kafka fallback — idempotent via external_id dedup).
        results: list[dict[str, Any]] = []
        assert _INGRESS.channel is not None
        for item in items:
            r = await _ingest_item(
                deps,
                tenant_id,
                _INGRESS.channel,
                item,
                headers,
            )
            if r:
                results.append(r)
        log.info(
            "whatsapp.delivery_ingested",
            phone_number_id=phone_number_id,
            messages=n_messages,
            statuses=n_statuses,
            ingested=len(results),
            path="inline",
        )
        return JSONResponse(
            {
                "status": "accepted",
                "path": "inline",
                "tenant_id": str(tenant_id),
                "messages": n_messages,
                "statuses": n_statuses,
                "ingested": len(results),
                "results": results,
            },
            status_code=200,
        )

    if not debug_endpoints_enabled:
        return router

    # ---- dev: register an installation (creds) --------------------------- #
    @router.post("/debug/whatsapp/register")
    async def register_installation(request: Request) -> JSONResponse:
        deps = _deps_or_503(request)
        if deps is None or deps.pool is None:
            return JSONResponse({"status": "deps_unavailable"}, status_code=503)
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001
            return JSONResponse({"status": "bad_json"}, status_code=400)
        if not isinstance(body, dict):
            return JSONResponse({"status": "bad_payload"}, status_code=400)

        try:
            tenant_id = UUID(str(body["tenant_id"]))
            phone_number_id = str(body["phone_number_id"])
        except (KeyError, ValueError):
            return JSONResponse(
                {"status": "missing_or_bad", "need": ["tenant_id (uuid)", "phone_number_id"]},
                status_code=400,
            )
        if not phone_number_id:
            return JSONResponse({"status": "missing_phone_number_id"}, status_code=400)

        await _ensure_tenant(deps.pool, tenant_id)
        try:
            secret_store = _secret_store_for_request(request, deps.pool)
            app_secret_ref = await _store_optional_secret(
                secret_store,
                tenant_id=tenant_id,
                phone_number_id=phone_number_id,
                body=body,
                key="app_secret",
            )
            verify_token_ref = await _store_optional_secret(
                secret_store,
                tenant_id=tenant_id,
                phone_number_id=phone_number_id,
                body=body,
                key="verify_token",
            )
            access_token_ref = await _store_optional_secret(
                secret_store,
                tenant_id=tenant_id,
                phone_number_id=phone_number_id,
                body=body,
                key="access_token",
            )
        except (SecretStoreError, ValueError) as exc:
            log.error(
                "whatsapp.secret_store_write_failed",
                phone_number_id=phone_number_id,
                error_type=type(exc).__name__,
            )
            return JSONResponse({"status": "secret_store_unavailable"}, status_code=503)
        async with deps.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO whatsapp_installations
                    (tenant_id, phone_number_id, waba_id, display_phone_number,
                     app_secret_ref, verify_token_ref, access_token_ref,
                     app_secret, verify_token, access_token, enabled, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,NULL,NULL,NULL,true, now())
                ON CONFLICT (phone_number_id) DO UPDATE SET
                    tenant_id            = EXCLUDED.tenant_id,
                    waba_id              = COALESCE(
                                             EXCLUDED.waba_id,
                                             whatsapp_installations.waba_id
                                           ),
                    display_phone_number = COALESCE(
                                             EXCLUDED.display_phone_number,
                                             whatsapp_installations.display_phone_number
                                           ),
                    app_secret_ref       = COALESCE(
                                             EXCLUDED.app_secret_ref,
                                             whatsapp_installations.app_secret_ref
                                           ),
                    verify_token_ref     = COALESCE(
                                             EXCLUDED.verify_token_ref,
                                             whatsapp_installations.verify_token_ref
                                           ),
                    access_token_ref     = COALESCE(
                                             EXCLUDED.access_token_ref,
                                             whatsapp_installations.access_token_ref
                                           ),
                    app_secret           = CASE
                                             WHEN EXCLUDED.app_secret_ref IS NOT NULL THEN NULL
                                             ELSE whatsapp_installations.app_secret
                                           END,
                    verify_token         = CASE
                                             WHEN EXCLUDED.verify_token_ref IS NOT NULL THEN NULL
                                             ELSE whatsapp_installations.verify_token
                                           END,
                    access_token         = CASE
                                             WHEN EXCLUDED.access_token_ref IS NOT NULL THEN NULL
                                             ELSE whatsapp_installations.access_token
                                           END,
                    enabled              = true,
                    updated_at           = now()
                RETURNING id, tenant_id, phone_number_id, waba_id, display_phone_number, enabled,
                          (app_secret_ref IS NOT NULL) AS has_app_secret_ref,
                          (verify_token_ref IS NOT NULL) AS has_verify_token_ref,
                          (access_token_ref IS NOT NULL) AS has_access_token_ref
                """,
                tenant_id,
                phone_number_id,
                body.get("waba_id"),
                body.get("display_phone_number"),
                app_secret_ref,
                verify_token_ref,
                access_token_ref,
            )
        out = dict(row)
        out["id"] = str(out["id"])
        out["tenant_id"] = str(out["tenant_id"])
        log.info("whatsapp.installation_registered", phone_number_id=phone_number_id)
        return JSONResponse({"status": "ok", "installation": out}, status_code=200)

    # ---- dev: live viewer ------------------------------------------------- #
    @router.get("/debug/whatsapp", include_in_schema=False)
    async def viewer() -> HTMLResponse:
        return trusted_static_html_response(_VIEWER_PAGE)

    @router.get("/debug/whatsapp/recent")
    async def recent(
        request: Request,
        tenant_id: str = Query(..., min_length=1),
        limit: int = Query(50, ge=1, le=200),
    ) -> JSONResponse:
        deps = _deps_or_503(request)
        if deps is None or deps.pool is None:
            return JSONResponse({"status": "deps_unavailable"}, status_code=503)
        try:
            tenant_uuid = UUID(tenant_id)
        except ValueError:
            return JSONResponse({"status": "bad_tenant_id"}, status_code=400)

        async with deps.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT set_config('app.current_tenant', $1::text, true)",
                    str(tenant_uuid),
                )
                rows = await conn.fetch(
                    """
                    SELECT id, source_channel, source_actor_ref, content, content_text,
                           occurred_at, ingested_at
                      FROM observations
                     WHERE tenant_id = $1
                       AND source_channel LIKE 'whatsapp:%'
                     ORDER BY ingested_at DESC
                     LIMIT $2
                    """,
                    tenant_uuid,
                    limit,
                )

        observations = []
        for r in rows:
            content = r["content"]
            if isinstance(content, str):
                try:
                    content = json.loads(content)
                except json.JSONDecodeError:
                    content = {}
            observations.append(
                {
                    "id": str(r["id"]),
                    "source_channel": r["source_channel"],
                    "source_actor_ref": r["source_actor_ref"],
                    "content_text": r["content_text"],
                    "content": content,
                    "occurred_at": r["occurred_at"].isoformat() if r["occurred_at"] else None,
                    "ingested_at": r["ingested_at"].isoformat() if r["ingested_at"] else None,
                }
            )
        return JSONResponse({"tenant_id": str(tenant_uuid), "observations": observations})

    return router


__all__ = ["build_whatsapp_router"]
