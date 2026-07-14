"""services/app/gateway/slack_router.py — Slack DM testing control plane.

Powers the `/slack` debug panel: a developer surface to drive per-user-OAuth
human↔human DM ingestion (direct messages + group DMs) end-to-end through the
real pipeline, ALONGSIDE the existing channel-message path, without real Slack
credentials. Mirrors `finance_router.py`.

DMs and channel messages share ONE handler and ONE dedup namespace: every
record lands on `source_channel='slack:message'` with
`external_id="{channel_id}:{ts}"` (handlers/slack.py). DM-vs-channel is a
content attribute (`content->>'channel_type'` ∈ im|mpim|channel|group), NOT a
separate channel — so a backfilled DM and its live webhook twin dedup to one
observation.

Four controls, scoped by `X-Tenant-Id` (falls back to COMPANY_OS_TENANT_ID in
dev — same convention as finance_router / debug_router). `{user_id}` is the
consenting user whose DMs we ingest (per-user OAuth grain):

  POST /slack/{user_id}/install
      Register the per-workspace live-webhook row (provider_installations, with
      an HMAC signing secret in encrypted_secrets so the live path verifies +
      resolves tenant), store a mock per-user xoxp token, record the consenting
      user in slack_dm_installations, ensure observation partitions. Idempotent.

  POST /slack/{user_id}/backfill
      Synthesize a batch of HISTORICAL DM + group-DM + a couple channel
      messages and run them through the REAL handler via inline `ingest()`.
      Deterministic (seed-based ts → safe to re-run; twins dedup).

  POST /slack/{user_id}/live/emit
      Synthesize ONE fresh DM/MPIM/edit event and POST it, Slack-v0-HMAC-signed,
      to the gateway's own `/webhooks/slack/events` edge — the genuine live path
      (signature verify → tenant resolve → ingest, Kafka cutover when the
      tenant's kafka_path is on). Falls back to inline ingest if the self-call
      fails. Call repeatedly to drive live traffic CONCURRENTLY with backfill.

  GET /slack/{user_id}/status
      DM vs channel observation counts + the last N rows.

Mounted in `build_app()`; `/slack/` is a public path prefix (tenant via header,
no bearer) — a dev/testing tool, env-gated at mount time.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import httpx
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse


log = structlog.get_logger("gateway.slack")


_CHANNEL = "slack:message"
_SLACK_USER_SCOPES = (
    "channels:read,channels:history,groups:read,groups:history,"
    "im:history,mpim:history,im:read,mpim:read,users:read"
)


# ---------------------------------------------------------------------
# Helpers (mirror finance_router / debug_router)
# ---------------------------------------------------------------------

def _request_is_production(req: Request) -> bool:
    settings = getattr(req.app.state, "gateway_settings", None)
    return bool(getattr(settings, "is_production", False))


def _resolve_tenant(req: Request) -> UUID:
    hdr = req.headers.get("X-Tenant-Id")
    if hdr:
        try:
            return UUID(hdr)
        except Exception:  # noqa: BLE001
            raise HTTPException(status_code=400, detail="invalid X-Tenant-Id")
    if _request_is_production(req):
        raise HTTPException(status_code=400, detail="tenant_id missing")
    env_tid = os.environ.get("DEFAULT_TENANT_ID") or os.environ.get(
        "COMPANY_OS_TENANT_ID"
    )
    if env_tid:
        try:
            return UUID(env_tid)
        except Exception:  # noqa: BLE001
            pass
    raise HTTPException(status_code=400, detail="tenant_id missing")


def _deps(req: Request):  # type: ignore[no-untyped-def]
    deps = getattr(req.app.state, "deps", None)
    if deps is None:
        raise HTTPException(status_code=500, detail="service_unavailable")
    return deps


def _pool(req: Request) -> asyncpg.Pool:
    return _deps(req).pool


async def _ensure_tenant(pool: asyncpg.Pool, tenant_id: UUID) -> None:
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
        tenant_id, f"slack-dm-{tenant_id.hex[:8]}",
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _team_id_for(tenant_id: UUID) -> str:
    """Stable synthetic workspace id for this tenant's DM demo install."""
    return "T0DM" + tenant_id.hex[:10].upper()


# ---------------------------------------------------------------------
# Synthetic DM data
# ---------------------------------------------------------------------

# Counterparts the consenting user exchanges DMs with: two colleagues in the
# same workspace and one "friend" — the human↔human DM case.
_COUNTERPARTS = (
    ("U_BOB", "colleague"),
    ("U_CAROL", "colleague"),
    ("U_FRIEND", "friend"),
)
_DM_TEMPLATES = (
    "hey, did you get a chance to look at the deploy?",
    "lunch at 1?",
    "can you review my PR when you have a sec",
    "the client call moved to 3pm",
    "did you see the thread in #general lol",
    "I'll send over the deck tonight",
    "are we still on for friday?",
    "thanks for covering standup today",
)
_MPIM_USERS = ("U_BOB", "U_CAROL", "U_DAVE")
_MPIM_TEMPLATES = (
    "group ping: standup notes are in the doc",
    "who's on the incident? prod looks degraded",
    "+1 to shipping tomorrow morning",
    "moving this to a huddle, join when you can",
)
_CHANNEL_ID = "C0GENERAL01"
_CHANNEL_TEMPLATES = (
    "deploy is green :white_check_mark:",
    "reminder: retro at 4pm in the main room",
)

_BASE_TS = 1_700_000_000  # deterministic backfill epoch floor


def _dm_channel(user_id: str, counterpart: str) -> str:
    a, b = sorted((user_id, counterpart))
    h = hashlib.blake2b(f"{a}:{b}".encode("utf-8"), digest_size=6).hexdigest().upper()
    return f"D{h}"


def _mpim_channel(user_id: str) -> str:
    h = hashlib.blake2b(f"mpim:{user_id}".encode("utf-8"), digest_size=6).hexdigest().upper()
    # Modern multi-party DMs surface with channel_type='mpim'; id may be C/G-prefixed.
    return f"G{h}"


def _event_callback(team_id: str, event: dict[str, Any]) -> dict[str, Any]:
    return {"type": "event_callback", "team_id": team_id, "event": event}


def _slack_dm_backfill_records(user_id: str, team_id: str, n: int, seed: int) -> list[dict]:
    """Historical DM (im) + group-DM (mpim) + a couple channel messages, each in
    the Slack `event_callback` shape the real handler consumes.

    Timestamps are spread over the last few days (anchored on now) so
    occurred_at falls inside an existing observations partition. `idx` keeps
    each ts unique; re-running with the same seed re-derives nearby ts (twins
    dedup on identical ts).
    """
    recs: list[dict] = []
    now = int(time.time())

    def _ts(idx: int) -> str:
        # 600s apart → spreads ~n*counterparts messages across recent days,
        # comfortably inside the ±2-month partition window.
        return f"{now - 60 - idx * 600}.{idx % 1000000:06d}"

    # 1:1 DMs with each counterpart (human↔human).
    for ci, (cp, _rel) in enumerate(_COUNTERPARTS):
        ch = _dm_channel(user_id, cp)
        for i in range(n):
            idx = seed * 1000 + ci * 50 + i
            sender = cp if i % 2 == 0 else user_id  # alternate direction
            recs.append(_event_callback(team_id, {
                "type": "message",
                "channel": ch,
                "channel_type": "im",
                "user": sender,
                "text": _DM_TEMPLATES[(seed + i + ci) % len(_DM_TEMPLATES)],
                "ts": _ts(idx),
                "team": team_id,
            }))

    # One group DM (mpim).
    gch = _mpim_channel(user_id)
    for i in range(max(2, n // 2)):
        idx = seed * 1000 + 900 + i
        recs.append(_event_callback(team_id, {
            "type": "message",
            "channel": gch,
            "channel_type": "mpim",
            "user": _MPIM_USERS[i % len(_MPIM_USERS)],
            "text": _MPIM_TEMPLATES[(seed + i) % len(_MPIM_TEMPLATES)],
            "ts": _ts(idx),
            "team": team_id,
        }))

    # A couple of channel messages, to show channel signals land ALONGSIDE DMs.
    for i in range(len(_CHANNEL_TEMPLATES)):
        idx = seed * 1000 + 950 + i
        recs.append(_event_callback(team_id, {
            "type": "message",
            "channel": _CHANNEL_ID,
            "channel_type": "channel",
            "user": _MPIM_USERS[i % len(_MPIM_USERS)],
            "text": _CHANNEL_TEMPLATES[i],
            "ts": _ts(idx),
            "team": team_id,
        }))
    return recs


def _slack_dm_live_event(user_id: str, team_id: str, seq: int) -> dict:
    """One FRESH live event — rotates DM / group-DM / an edit so the demo
    exercises message.im, message.mpim and message_changed."""
    now = int(time.time())
    ts = f"{now}.{seq % 1000000:06d}"

    if seq % 5 == 4:
        # An edit (message_changed) of an earlier DM — captured as a distinct
        # edit signal (handler keys it on the edit ts).
        ch = _dm_channel(user_id, "U_FRIEND")
        orig_ts = f"{_BASE_TS}.{seq % 1000000:06d}"
        event = {
            "type": "message",
            "subtype": "message_changed",
            "channel": ch,
            "channel_type": "im",
            "message": {
                "type": "message",
                "user": "U_FRIEND",
                "text": f"(edited) actually let's make it 4pm [seq {seq}]",
                "ts": orig_ts,
                "edited_ts": ts,
            },
            "previous_message": {"type": "message", "user": "U_FRIEND",
                                 "text": "let's make it 3pm", "ts": orig_ts},
            "ts": ts,
            "event_ts": ts,
        }
    elif seq % 3 == 2:
        # Group DM (mpim).
        event = {
            "type": "message",
            "channel": _mpim_channel(user_id),
            "channel_type": "mpim",
            "user": _MPIM_USERS[seq % len(_MPIM_USERS)],
            "text": f"{_MPIM_TEMPLATES[seq % len(_MPIM_TEMPLATES)]} [live {seq}]",
            "ts": ts,
            "team": team_id,
        }
    else:
        # 1:1 DM.
        cp = _COUNTERPARTS[seq % len(_COUNTERPARTS)][0]
        event = {
            "type": "message",
            "channel": _dm_channel(user_id, cp),
            "channel_type": "im",
            "user": cp if seq % 2 == 0 else user_id,
            "text": f"{_DM_TEMPLATES[seq % len(_DM_TEMPLATES)]} [live {seq}]",
            "ts": ts,
            "team": team_id,
        }
    return _event_callback(team_id, event)


# ---------------------------------------------------------------------
# Inline ingest (deterministic backfill path)
# ---------------------------------------------------------------------

async def _ingest_record(req: Request, tenant_id: UUID, record: dict) -> dict:
    from services.ingest.ingestion.core import ingest

    deps = _deps(req)
    res = await ingest(
        _CHANNEL, record,
        pool=deps.pool,
        tenant_id=tenant_id,
        actor_repo=deps.actor_repo,
        alias_repo=deps.alias_repo,
        embedder=deps.embedder,
    )
    ev = (record.get("event") or {})
    return {
        "observation_id": str(res.observation.id),
        "external_id": res.observation.external_id,
        "deduped": res.deduped,
        "channel_type": ev.get("channel_type"),
    }


# ---------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------

def build_slack_router() -> APIRouter:
    router = APIRouter(prefix="/slack", tags=["slack"])

    router.add_api_route("/{user_id}/install", install, methods=["POST"])
    router.add_api_route("/{user_id}/backfill", backfill, methods=["POST"])
    router.add_api_route("/{user_id}/live/emit", live_emit, methods=["POST"])
    router.add_api_route("/{user_id}/status", status, methods=["GET"])

    return router


async def install(user_id: str, req: Request) -> JSONResponse:
    tenant_id = _resolve_tenant(req)
    pool = _pool(req)
    await _ensure_tenant(pool, tenant_id)
    team_id = _team_id_for(tenant_id)

    await _ensure_slack_observation_partitions(pool)
    signing_secret = f"slackdmsig-{team_id}-{uuid4().hex[:8]}"
    signing_ref, user_token_ref = await _store_slack_dm_secrets(
        req,
        tenant_id=tenant_id,
        team_id=team_id,
        user_id=user_id,
        signing_secret=signing_secret,
    )
    await _register_slack_provider_installation(
        pool,
        tenant_id=tenant_id,
        team_id=team_id,
        signing_ref=signing_ref,
    )
    await _record_slack_dm_installation(
        pool,
        tenant_id=tenant_id,
        team_id=team_id,
        user_id=user_id,
        user_token_ref=user_token_ref,
    )
    _cache_slack_signing_secret(req, tenant_id, signing_secret)

    return JSONResponse({
        "tenant_id": str(tenant_id),
        "team_id": team_id,
        "user_id": user_id,
        "user_token_stored": user_token_ref is not None,
        "webhook_secret_registered": signing_ref is not None,
        "user_scopes": _SLACK_USER_SCOPES,
        "message": f"slack DM install ready for user {user_id} "
                   f"(workspace {team_id}); backfill + live DM ingestion enabled.",
    }, status_code=201)


async def backfill(user_id: str, req: Request) -> JSONResponse:
    tenant_id = _resolve_tenant(req)
    body = await _json_body(req)
    per = max(1, min(50, int(body.get("count", 6)))) if isinstance(body, dict) else 6
    seed = int(body.get("seed", 0)) if isinstance(body, dict) else 0
    team_id = _team_id_for(tenant_id)

    results = [
        await _ingest_record(req, tenant_id, rec)
        for rec in _slack_dm_backfill_records(user_id, team_id, per, seed)
    ]
    new, deduped, by_type = _summarize_ingest_results(results)
    return JSONResponse({
        "user_id": user_id,
        "records": len(results),
        "ingested": new,
        "deduped": deduped,
        "by_channel_type": by_type,
        "results": results[:50],
        "message": f"backfill ingested {new} new DM/channel observations "
                   f"({deduped} deduped).",
    }, status_code=201)


async def live_emit(user_id: str, req: Request) -> JSONResponse:
    """Synthesize one fresh DM/MPIM/edit event and POST it, falling back inline."""
    tenant_id = _resolve_tenant(req)
    body = await _json_body(req)
    seq = int(body.get("seq", 0)) if isinstance(body, dict) else 0
    team_id = _team_id_for(tenant_id)

    payload = _slack_dm_live_event(user_id, team_id, seq)
    webhook_status, webhook_body, delivered_via = await _try_deliver_live_webhook(
        req,
        tenant_id=tenant_id,
        payload=payload,
    )

    inline_result = None
    if delivered_via is None:
        inline_result = await _ingest_record(req, tenant_id, payload)
        delivered_via = "inline_fallback"

    ev = payload["event"]
    return JSONResponse({
        "user_id": user_id,
        "delivered_via": delivered_via,
        "webhook_status": webhook_status,
        "webhook_response": webhook_body,
        "inline_result": inline_result,
        "event": {
            "channel": ev.get("channel"),
            "channel_type": ev.get("channel_type"),
            "subtype": ev.get("subtype"),
        },
    }, status_code=201)


async def status(user_id: str, req: Request) -> dict[str, Any]:
    tenant_id = _resolve_tenant(req)
    pool = _pool(req)
    team_id = _team_id_for(tenant_id)

    counts, dm_total, recent, install_row = await _load_slack_status_rows(
        pool,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    return {
        "user_id": user_id,
        "team_id": team_id,
        "channel": _CHANNEL,
        "installed": install_row is not None,
        "dm_observations": dm_total or 0,
        "counts_by_channel_type": {r["channel_type"]: r["n"] for r in counts},
        "recent": [_status_observation_payload(r) for r in recent],
    }


async def _ensure_slack_observation_partitions(pool: asyncpg.Pool) -> None:
    try:
        from services.domain.observations.partitions import ensure_partitions
        await ensure_partitions(pool, months_ahead=2)
        old = _now() - timedelta(days=60)
        await ensure_partitions(pool, as_of=old.date(), months_ahead=0)
    except Exception as exc:  # noqa: BLE001
        log.warning("slack_dm_partition_ensure_failed", error=str(exc))


async def _store_slack_dm_secrets(
    req: Request,
    *,
    tenant_id: UUID,
    team_id: str,
    user_id: str,
    signing_secret: str,
) -> tuple[str | None, str | None]:
    store = getattr(req.app.state, "secret_store", None)
    if store is None:
        return None, None
    try:
        signing_ref = await store.put(
            signing_secret,
            label=f"slack_signing_secret:dm:{team_id}",
            tenant_id=tenant_id,
        )
        user_token_ref = await store.put(
            f"xoxp-test-{user_id}",
            label=f"slack_user_token:{team_id}:{user_id}",
            tenant_id=tenant_id,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("slack_dm_secret_put_failed", error=str(exc))
        return None, None
    return signing_ref, user_token_ref


async def _register_slack_provider_installation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    team_id: str,
    signing_ref: str | None,
) -> None:
    await pool.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, 'slack', $3, $4, TRUE)
        ON CONFLICT (provider, installation_id) DO UPDATE
            SET secret_ref = EXCLUDED.secret_ref,
                enabled    = TRUE,
                tenant_id  = EXCLUDED.tenant_id
        """,
        uuid4(), tenant_id, team_id, signing_ref,
    )


async def _record_slack_dm_installation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    team_id: str,
    user_id: str,
    user_token_ref: str | None,
) -> None:
    await pool.execute(
        """
        INSERT INTO slack_dm_installations
            (id, tenant_id, team_id, user_id, base_url,
             user_token_secret_ref, granted_user_scopes)
        VALUES ($1, $2, $3, $4, NULL, $5, $6)
        ON CONFLICT (tenant_id, team_id, user_id) DO UPDATE
            SET user_token_secret_ref = EXCLUDED.user_token_secret_ref,
                granted_user_scopes    = EXCLUDED.granted_user_scopes,
                disabled_at            = NULL
        """,
        uuid4(), tenant_id, team_id, user_id, user_token_ref, _SLACK_USER_SCOPES,
    )


def _cache_slack_signing_secret(
    req: Request,
    tenant_id: UUID,
    signing_secret: str,
) -> None:
    cache = getattr(req.app.state, "_slack_dm_secrets", None)
    if cache is None:
        cache = {}
        req.app.state._slack_dm_secrets = cache
    cache[str(tenant_id)] = signing_secret


async def _json_body(req: Request) -> Any:
    try:
        return await req.json()
    except Exception:  # noqa: BLE001
        return {}


def _summarize_ingest_results(
    results: list[dict],
) -> tuple[int, int, dict[str, int]]:
    new = sum(1 for r in results if not r["deduped"])
    deduped = sum(1 for r in results if r["deduped"])
    by_type: dict[str, int] = {}
    for r in results:
        channel_type = r.get("channel_type") or "?"
        by_type[channel_type] = by_type.get(channel_type, 0) + 1
    return new, deduped, by_type


async def _try_deliver_live_webhook(
    req: Request,
    *,
    tenant_id: UUID,
    payload: dict,
) -> tuple[int | None, Any, str | None]:
    cache = getattr(req.app.state, "_slack_dm_secrets", {}) or {}
    secret = cache.get(str(tenant_id))
    if not secret:
        return None, None, None

    raw = json.dumps(payload).encode("utf-8")
    ts_header = str(int(time.time()))
    signature = _slack_signature(secret, ts_header, raw)
    port = os.environ.get("GATEWAY_SELF_PORT", "8000")
    url = f"http://127.0.0.1:{port}/webhooks/slack/events"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                url,
                content=raw,
                headers={
                    "content-type": "application/json",
                    "X-Slack-Signature": signature,
                    "X-Slack-Request-Timestamp": ts_header,
                    "X-Tenant-Id": str(tenant_id),
                },
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("slack_dm_live_webhook_failed", error=str(exc))
        return None, None, None

    webhook_body = _webhook_response_body(resp)
    delivered_via = "webhook" if resp.status_code in (200, 201, 202) else None
    return resp.status_code, webhook_body, delivered_via


def _slack_signature(secret: str, ts_header: str, raw: bytes) -> str:
    basestring = f"v0:{ts_header}:{raw.decode('utf-8')}".encode("utf-8")
    return "v0=" + hmac.new(
        secret.encode("utf-8"), basestring, hashlib.sha256,
    ).hexdigest()


def _webhook_response_body(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except Exception:  # noqa: BLE001
        return resp.text[:200]


async def _load_slack_status_rows(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    user_id: str,
) -> tuple[list[Any], int | None, list[Any], Any]:
    counts = await pool.fetch(
        """
        SELECT COALESCE(content->>'channel_type', 'unknown') AS channel_type,
               count(*) AS n
          FROM observations
         WHERE tenant_id = $1 AND source_channel = $2
         GROUP BY 1
         ORDER BY 1
        """,
        tenant_id, _CHANNEL,
    )
    dm_total = await pool.fetchval(
        """
        SELECT count(*) FROM observations
         WHERE tenant_id = $1 AND source_channel = $2
           AND (content->>'channel_type' IN ('im','mpim')
                OR left(external_id, 1) IN ('D','G'))
        """,
        tenant_id, _CHANNEL,
    )
    recent = await pool.fetch(
        """
        SELECT external_id, content->>'channel_type' AS channel_type,
               content->>'subtype' AS subtype, content_text,
               occurred_at, ingested_at
          FROM observations
         WHERE tenant_id = $1 AND source_channel = $2
         ORDER BY ingested_at DESC
         LIMIT 15
        """,
        tenant_id, _CHANNEL,
    )
    install_row = await pool.fetchrow(
        "SELECT user_id, team_id, granted_user_scopes, created_at "
        "FROM slack_dm_installations "
        "WHERE tenant_id = $1 AND user_id = $2 AND disabled_at IS NULL LIMIT 1",
        tenant_id, user_id,
    )
    return list(counts), dm_total, list(recent), install_row


def _status_observation_payload(row: Any) -> dict[str, Any]:
    return {
        "external_id": row["external_id"],
        "channel_type": row["channel_type"],
        "subtype": row["subtype"],
        "content_text": row["content_text"],
        "occurred_at": row["occurred_at"].isoformat() if row["occurred_at"] else None,
        "ingested_at": row["ingested_at"].isoformat() if row["ingested_at"] else None,
    }


__all__ = ["build_slack_router"]
