"""services/integrations/notion/webhook.py — Notion webhook ingress (IN-14).

Two router-invoked entry points, mirroring the GitHub/Slack split between
a one-time handshake and the steady-state event path:

  * ``is_verification_handshake`` / ``handle_verification_handshake`` —
    Notion's one-time subscription verification POST. It is UNSIGNED
    (no ``verification_token`` exists yet to sign with) and its body is
    ``{"verification_token": "secret_…"}``. The router intercepts it
    BEFORE signature verification (see router.py), logs the token loudly
    for the operator to copy into ``NOTION_WEBHOOK_VERIFICATION_TOKEN``,
    and 200s. This is the documented one-and-only delivery of that token
    (see services/webhooks/secrets.py::_load_notion_app_secrets).

  * ``handle_notion_event`` — a verified, tenant-resolved event. Notion
    deliveries are THIN: ``entity.id`` + a dotted event ``type`` (e.g.
    ``"page.content_updated"``) + ``workspace_id``; the object body is NOT
    included. We fetch the full page via the per-workspace bot token
    (resolved from the resolved installation's ``secret_ref``), inject the
    ``_fyralis_workspace_id`` private key to mirror the backfill/poll
    fetcher exactly, and ``shadow_write_raw`` it onto the real data plane
    (``ingress_kind="webhook"``): S3 PutIfAbsent → Kafka ``ingestion.raw``
    → normalizer (``("notion","webhook") → notion:object``) →
    observation_writer. The handler keys on the object's native
    ``object`` field and derives ``external_id = notion:page:{id}`` — the
    SAME id the backfill/poll paths emit, so the dedup UNIQUE index
    collapses a webhook-delivered page and its backfill twin to one
    observation.

    Why the data plane and not inline ``ingest()``: Notion has no inline
    handler wired in the gateway, and we deliberately route Notion through
    the full pipeline. The producer + S3 client come from
    ``app.state.notion_data_plane`` (wired in
    services/gateway/main.py::_wire_ingestion_data_plane), scoped so the
    slack/github cutover stays inline. The observation lands once the
    tenant's ``ingestion.kafka_path_enabled`` flag is on (the
    observation_writer full-mode gate) — the same gate backfill lives
    behind.

Scope (v1): PAGE entities only. The NotionClient's single-object getter is
``retrieve_page``; pages are the high-value surface and the only entity a
thin event lets us resolve to a full object without a parent context.
Non-page entity types (database / block / comment) and un-fetchable
objects (deleted / un-shared since the event fired) are logged and
acknowledged with 200 WITHOUT a write — backfill/poll covers the rest of
the tree on its cadence.
"""
from __future__ import annotations

from typing import Any, Mapping

import orjson
import structlog
from fastapi import Request
from fastapi.responses import JSONResponse

from lib.shared.errors import NotionApiError
from services.ingestion.shadow_write import shadow_write_raw
from services.integrations.notion.client import short_workspace_hash


log = structlog.get_logger("integrations.notion.webhook")


# ---------------------------------------------------------------------
# Verification handshake (unsigned, intercepted pre-verify)
# ---------------------------------------------------------------------

def is_verification_handshake(payload: Mapping[str, Any] | None) -> bool:
    """True for Notion's one-time subscription verification POST.

    That POST carries a non-empty ``verification_token`` string and none
    of the event fields (``entity`` / dotted ``type``). It is unsigned, so
    the router MUST detect and short-circuit it before the signature
    verifier runs (there is no token yet to verify against).
    """
    if not isinstance(payload, dict):
        return False
    token = payload.get("verification_token")
    return isinstance(token, str) and bool(token)


def handle_verification_handshake(
    payload: Mapping[str, Any],
) -> JSONResponse:
    """Acknowledge the verification POST and surface the token.

    Notion delivers the ``verification_token`` exactly once, via this
    request body (it is also shown briefly in the integration dashboard).
    We log it at WARNING — this is the documented operator-retrieval
    mechanism: copy the value into ``NOTION_WEBHOOK_VERIFICATION_TOKEN``
    and redeploy the gateway so subsequent signed events verify.
    """
    token = payload["verification_token"]
    log.warning(
        "notion_webhook_verification_token_received",
        action=(
            "Copy this value into NOTION_WEBHOOK_VERIFICATION_TOKEN "
            "and redeploy the gateway so signed events verify."
        ),
        verification_token=token,
    )
    return JSONResponse({"handled": "verification"}, status_code=200)


# ---------------------------------------------------------------------
# Event path (verified, tenant-resolved)
# ---------------------------------------------------------------------

def _entity(payload: Mapping[str, Any]) -> tuple[str | None, str | None]:
    """Extract ``(entity_id, entity_type)`` from a Notion event payload."""
    ent = payload.get("entity")
    if not isinstance(ent, Mapping):
        return None, None
    eid = ent.get("id")
    etype = ent.get("type")
    return (
        eid if isinstance(eid, str) and eid else None,
        etype if isinstance(etype, str) and etype else None,
    )


async def _build_workspace_client(outcome: Any, workspace_id: str | None) -> Any:
    """Build a per-workspace NotionClient from the resolved installation.

    Reuses the fetcher's ``build_notion_client`` so the bot-token
    resolution, shared httpx pool, endpoint-resolver base URL, and sandbox
    spammer mode are all identical to the backfill/poll path. The
    ``install`` shape it consumes is just three fields, which we synthesize
    from the resolver outcome (a plain dict satisfies its ``[...]`` /
    ``in`` access).
    """
    from services.ingestion.fetchers._clients import build_notion_client

    install = {
        "installation_id": workspace_id,
        "tenant_id": outcome.tenant_id,
        "secret_ref": outcome.secret_ref,
    }
    return await build_notion_client(install)


async def handle_notion_event(
    *,
    request: Request,
    outcome: Any,
    payload: Mapping[str, Any],
) -> JSONResponse:
    """Fetch the changed page and shadow-write it onto the data plane.
    Always returns 200 — Notion retries non-2xx, and an unsupported
    entity / transient fetch miss is not worth a retry storm. The periodic
    backfill/poll reconcile is the correctness backstop.
    """
    workspace_id = payload.get("workspace_id")
    event_type = payload.get("type") if isinstance(payload.get("type"), str) else None
    entity_id, entity_type = _entity(payload)

    if entity_type != "page" or entity_id is None:
        log.info(
            "notion_webhook_ignored_entity",
            entity_type=entity_type,
            event_type=event_type,
            workspace=short_workspace_hash(str(workspace_id)) if workspace_id else None,
        )
        return JSONResponse(
            {"handled": "ignored", "reason": "unsupported_entity"},
            status_code=200,
        )

    client = await _build_workspace_client(outcome, str(workspace_id) if workspace_id else None)
    try:
        page = await client.retrieve_page(entity_id)
    except NotionApiError as exc:
        # 404 (page deleted / un-shared since the event fired), 401 (token
        # rotated), rate-limit-exhausted — ack and let backfill/poll
        # reconcile. Retrying via a non-2xx would not change the outcome.
        status = (exc.context or {}).get("http_status")
        log.info(
            "notion_webhook_fetch_failed",
            code=exc.code,
            http_status=status,
            event_type=event_type,
        )
        return JSONResponse(
            {"handled": "ignored", "reason": "fetch_failed"},
            status_code=200,
        )
    finally:
        # build_notion_client shares the process-wide httpx pool
        # (http_client injected ⇒ owns_client=False), so aclose() is a
        # no-op on the shared client; calling it is safe and future-proofs
        # against a non-shared client.
        try:
            await client.aclose()
        except Exception:  # noqa: BLE001
            pass

    # Mirror the fetcher's single private enrichment so the handler's
    # `content.workspace_id` is populated identically across ingress kinds.
    page["_fyralis_workspace_id"] = workspace_id

    written = await _shadow_write_page(
        request,
        tenant_id=outcome.tenant_id,
        page=page,
        event_type=event_type,
        entity_id=entity_id,
    )
    return JSONResponse(
        {
            "handled": "event",
            "event_type": event_type,
            "shadow_write": written,
        },
        status_code=200,
    )


async def _shadow_write_page(
    request: Request,
    *,
    tenant_id: Any,
    page: dict[str, Any],
    event_type: str | None,
    entity_id: str,
) -> bool:
    """Publish the fetched page onto the data plane via ``shadow_write_raw``.

    Uses the Notion-scoped producer + S3 client wired at
    ``app.state.notion_data_plane`` (see
    services/gateway/main.py::_wire_ingestion_data_plane). Returns True when
    the write was attempted, False when the data plane is unwired
    (KAFKA_BOOTSTRAP_SERVERS unset / startup failed). A failure mid-write
    propagates the exception to the caller's 200 path only via the log —
    the function swallows it (Notion must not retry a transient S3/Kafka
    hiccup; backfill/poll reconciles).
    """
    ndp = getattr(request.app.state, "notion_data_plane", None)
    if ndp is None:
        log.error("notion_webhook_data_plane_unwired", event_type=event_type)
        return False
    try:
        s3_key = await shadow_write_raw(
            tenant_id=tenant_id,
            source="notion",
            ingress_kind="webhook",
            raw_body=orjson.dumps(page),
            s3_client=ndp.s3_client,
            kafka_producer=ndp.producer,
            ingress_metadata={
                "event_type": event_type or "unknown",
                "entity_id": entity_id,
            },
        )
        # The gateway is a request/response service: unlike the always-on
        # ingestion workers, nothing else drives the producer's delivery
        # queue. `produce()` only enqueues into librdkafka's LOCAL buffer
        # (returns before broker-ack), so flush here to DURABLY deliver
        # before we 200 Notion — otherwise the event could be lost in the
        # local queue on a gateway restart (backfill/poll would reconcile,
        # but we avoid the gap). Bounded; remaining>0 ⇒ delivery in doubt.
        remaining = await ndp.producer.flush(timeout_seconds=10.0)
        if remaining:
            log.warning(
                "notion_webhook_kafka_flush_incomplete",
                event_type=event_type,
                remaining=remaining,
            )
            return False
        log.info(
            "notion_webhook_shadow_written",
            event_type=event_type,
            raw_s3_key=s3_key,
        )
        return True
    except Exception as exc:  # noqa: BLE001 — M2 prime directive
        log.warning(
            "notion_webhook_shadow_write_failed",
            error_type=type(exc).__name__,
            error_message=str(exc)[:200],
            event_type=event_type,
        )
        return False


__all__ = [
    "is_verification_handshake",
    "handle_verification_handshake",
    "handle_notion_event",
]
