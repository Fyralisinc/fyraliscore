"""services/demo/simulator.py — signal injection from the demo UI panel.

Translates a payload from the simulator's tabs (Slack / Email / GitHub
/ Calendar / Stripe / Custom) into the same `ObservationCreate` that
real ingestion would produce, then routes through `services.ingestion.
core.ingest`.

Each tab maps onto an existing `source_channel`:
  Slack    → slack:message
  Email    → email:message
  GitHub   → github:event
  Calendar → calendar:event
  Stripe   → stripe:event
  Custom   → whatever the caller specifies

The payload shape per tab matches what the real handler in
`services/ingestion/handlers/<channel>.py` accepts. The simulator is a
thin pass-through — it doesn't impersonate signature verification
(since the request is authenticated as the demo CEO) but it does tag
the resulting observation with `demo_session_id` for cost attribution.

Suggested signals: a small library of pre-canned, per-company signals
the picker UI surfaces under each tab. `list_suggested_signals` is the
read API.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg

from services.demo.repo import get_demo_session, increment_signal_count
from services.demo.sse import publish_recommendation_event
from services.ingestion.core import ingest

# Eagerly import every ingestion handler we route to. The gateway's
# main module only imports `slack`, so without these the demo path
# would 400 with "no handler registered for channel" the first time
# the simulator targets email/github/calendar/internal channels.
import services.ingestion.handlers.email  # noqa: F401
import services.ingestion.handlers.github  # noqa: F401
import services.ingestion.handlers.calendar  # noqa: F401
import services.ingestion.handlers.system  # noqa: F401


# ---------------------------------------------------------------------
# UI → handler payload translation
# ---------------------------------------------------------------------
#
# The simulator UI tabs collect payloads in a friendly shape (e.g. Slack
# = {channel, author, message}). The real ingestion handlers expect the
# wire shape each provider actually emits (slack: {text, ts, channel,
# user}; github: real webhook body + X-GitHub-Event header; etc.).
# `_translate_payload` is the bridge — it accepts (ui_channel,
# ui_payload) and returns (real_channel, real_payload, real_headers)
# ready for `services.ingestion.core.ingest`.


def _translate_payload(
    channel: str, payload: dict[str, Any],
) -> tuple[str, dict[str, Any], dict[str, str]]:
    """Map the simulator's friendly payload onto the real handler shape.

    Returns `(real_channel, real_payload, real_headers)`. Unknown
    channels pass through untouched (Custom tab relies on this)."""
    if channel == "slack:message":
        return _translate_slack(payload)
    if channel == "email:message":
        return _translate_email(payload)
    if channel == "github:event":
        return _translate_github(payload)
    if channel == "calendar:event":
        return _translate_calendar(payload)
    if channel == "stripe:event":
        return _translate_stripe(payload)
    return (channel, dict(payload), {})


def _translate_slack(
    payload: dict[str, Any],
) -> tuple[str, dict[str, Any], dict[str, str]]:
    text = (payload.get("message") or payload.get("text") or "").strip()
    if not text:
        # Accept empty quietly — handler will reject with a clear error.
        text = "(no message)"
    slack_channel = payload.get("channel") or "#general"
    author = payload.get("author") or "demo-user"
    ts = payload.get("ts") or f"{time.time():.6f}"
    out: dict[str, Any] = {
        "text": text,
        "channel": slack_channel,
        "user": author,
        "ts": ts,
        "team": "T_DEMO",
    }
    if "demo_session_id" in payload:
        out["demo_session_id"] = payload["demo_session_id"]
    return ("slack:message", out, {})


def _translate_email(
    payload: dict[str, Any],
) -> tuple[str, dict[str, Any], dict[str, str]]:
    out = dict(payload)
    # UI's `from`/`to`/`subject`/`body` already match the email handler's
    # canonical shape; just add a stable message_id so dedup works.
    if "message_id" not in out:
        out["message_id"] = f"<demo-{time.time_ns()}@simulator>"
    if "date" not in out:
        out["date"] = datetime.now(timezone.utc).isoformat()
    return ("email:inbound", out, {})


def _translate_github(
    payload: dict[str, Any],
) -> tuple[str, dict[str, Any], dict[str, str]]:
    """Map the UI's `{repo, event_type, author, title}` into a synthetic
    GitHub webhook body + the X-GitHub-Event header the handler reads."""
    repo = payload.get("repo") or "demo/repo"
    event_type = payload.get("event_type") or "pr_opened"
    author = payload.get("author") or "demo-user"
    title = payload.get("title") or "(untitled)"
    now_iso = datetime.now(timezone.utc).isoformat()
    sender = {"login": author}
    repo_block = {"full_name": repo}

    if event_type in ("pr_opened", "pr_merged"):
        action = "opened" if event_type == "pr_opened" else "closed"
        merged = event_type == "pr_merged"
        body = {
            "action": action,
            "pull_request": {
                "number": int(time.time()) % 10000,
                "title": title,
                "node_id": f"PR_{time.time_ns()}",
                "merged": merged,
                "base": {"ref": "main"},
                "updated_at": now_iso,
                "created_at": now_iso,
            },
            "sender": sender,
            "repository": repo_block,
        }
        return ("github:webhook", body, {"X-GitHub-Event": "pull_request"})

    if event_type == "commit":
        body = {
            "ref": "refs/heads/main",
            "after": f"sha-{time.time_ns():x}",
            "commits": [{"message": title}],
            "sender": sender,
            "repository": repo_block,
        }
        return ("github:webhook", body, {"X-GitHub-Event": "push"})

    # Default: issue_comment-shaped event.
    body = {
        "action": "created",
        "comment": {
            "body": title,
            "node_id": f"IC_{time.time_ns()}",
            "created_at": now_iso,
        },
        "issue": {
            "number": int(time.time()) % 10000,
            "node_id": f"I_{time.time_ns()}",
        },
        "sender": sender,
        "repository": repo_block,
    }
    return ("github:webhook", body, {"X-GitHub-Event": "issue_comment"})


def _translate_calendar(
    payload: dict[str, Any],
) -> tuple[str, dict[str, Any], dict[str, str]]:
    """Map `{title, attendees, minutes_ago}` into the calendar handler's
    expected `{action, event: {summary, start, end, attendees, ...}}`."""
    title = payload.get("title") or "(meeting)"
    minutes_ago = int(payload.get("minutes_ago") or 0)
    start = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    end = start + timedelta(minutes=30)
    raw_attendees = payload.get("attendees") or []
    attendees: list[dict[str, str]] = []
    if isinstance(raw_attendees, list):
        for a in raw_attendees:
            if isinstance(a, str) and "@" in a:
                attendees.append({"email": a.strip().lower()})
    body = {
        "action": "created",
        "event": {
            "id": f"evt-{time.time_ns()}",
            "summary": title,
            "description": "",
            "start": {"dateTime": start.isoformat()},
            "end": {"dateTime": end.isoformat()},
            "attendees": attendees,
            "organizer": {"email": "demo-ceo@example.com"},
            "status": "confirmed",
        },
    }
    return ("calendar:sync", body, {})


_AUTHOR_LABEL_RE = None
_DISPLAY_NAME_FROM_LABEL_CACHE: dict[str, str] = {}


def _display_name_from_label(label: str | None) -> str | None:
    """Pull a person's display name out of the simulator's friendly
    author label.

      "Eng — Sarah Chen"           → "Sarah Chen"
      "Founder — Jules Park"       → "Jules Park"
      "Sasha"                      → "Sasha"
      "Replit — Eng Lead"          → "Replit — Eng Lead" (no comma to split)

    The em-dash separator is what the demo catalog uses; we tolerate
    plain hyphens too. Anything before the dash is a role hint we drop;
    anything after is the human name we resolve actors against.
    """
    if not label or not isinstance(label, str):
        return None
    cached = _DISPLAY_NAME_FROM_LABEL_CACHE.get(label)
    if cached is not None:
        return cached
    s = label.strip()
    # Both em-dash and ascii hyphen with surrounding spaces.
    for sep in (" \u2014 ", " \u2013 ", " - "):
        if sep in s:
            head, tail = s.split(sep, 1)
            # If the tail starts with a role label like "Eng Lead" rather
            # than a name, treat the head as the name (e.g. "Replit —
            # Eng Lead"). Heuristic: keep the side that has more capital
            # tokens past the first one (a name typically has 2+ caps).
            head_caps = sum(1 for w in head.split() if w[:1].isupper())
            tail_caps = sum(1 for w in tail.split() if w[:1].isupper())
            chosen = tail if tail_caps >= head_caps else head
            _DISPLAY_NAME_FROM_LABEL_CACHE[label] = chosen.strip()
            return _DISPLAY_NAME_FROM_LABEL_CACHE[label]
    _DISPLAY_NAME_FROM_LABEL_CACHE[label] = s
    return s


async def _resolve_actor_by_display_name(
    pool: asyncpg.Pool, tenant_id: UUID, display_name: str,
) -> UUID | None:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            """
            SELECT id
            FROM actors
            WHERE tenant_id = $1
              AND display_name = $2
              AND coalesce(status, 'active') <> 'archived'
            ORDER BY created_at ASC
            LIMIT 1
            """,
            tenant_id, display_name,
        )


async def _ensure_actor_identity_mapping(
    pool: asyncpg.Pool, *, actor_id: UUID, source_channel: str, source_actor_ref: str,
) -> None:
    """Idempotent upsert into actor_identity_mappings."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO actor_identity_mappings (
                actor_id, source_channel, source_actor_ref, confidence
            ) VALUES ($1, $2, $3, 1.0)
            ON CONFLICT (source_channel, source_actor_ref) DO NOTHING
            """,
            actor_id, source_channel, source_actor_ref,
        )


async def _seed_demo_actor_identity(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    real_channel: str,
    ui_payload: dict[str, Any],
    real_payload: dict[str, Any],
) -> dict[str, Any]:
    """Resolve the simulator's friendly author/from/organizer label to
    an actor in this tenant and stamp the wire payload with a stable
    channel-native id that's mapped in `actor_identity_mappings`. The
    ingestion core's actor resolver then finds the actor automatically,
    so downstream Think reasoning can connect the new signal to models
    scoped on that actor (e.g. Sarah's burnout state)."""
    # ActorRepo.resolve_by_source_actor_ref splits the ingestion-side
    # `<channel>:<ref>` string by `:` and looks up
    # actor_identity_mappings(source_channel, source_actor_ref). So we
    # store the *prefix* in source_channel ("slack" / "email") — NOT
    # the full source_channel name like "slack:message" — and the bare
    # external id in source_actor_ref. Otherwise the lookup misses
    # and observations land with actor_id=NULL.
    if real_channel == "slack:message":
        label = ui_payload.get("author")
        name = _display_name_from_label(label)
        if not name:
            return real_payload
        actor_uuid = await _resolve_actor_by_display_name(pool, tenant_id, name)
        if actor_uuid is None:
            return real_payload
        slack_user = f"DEMO-{str(actor_uuid)[:8].upper()}"
        await _ensure_actor_identity_mapping(
            pool, actor_id=actor_uuid,
            source_channel="slack",
            source_actor_ref=slack_user,
        )
        return {**real_payload, "user": slack_user}

    if real_channel == "email:inbound":
        label = ui_payload.get("from") or ui_payload.get("from_label")
        name = _display_name_from_label(label)
        if not name:
            return real_payload
        actor_uuid = await _resolve_actor_by_display_name(pool, tenant_id, name)
        if actor_uuid is None:
            return real_payload
        addr = f"demo-{str(actor_uuid)[:8]}@fyralis.demo"
        await _ensure_actor_identity_mapping(
            pool, actor_id=actor_uuid,
            source_channel="email",
            source_actor_ref=addr,
        )
        out = dict(real_payload)
        out["from"] = addr
        out.setdefault("from_name", name)
        return out

    return real_payload


def _translate_stripe(
    payload: dict[str, Any],
) -> tuple[str, dict[str, Any], dict[str, str]]:
    """No real stripe ingestion handler exists; route Stripe events as
    `internal:state_change` observations so the substrate still sees a
    record. Trust tier on internal:* is authoritative, which is fine
    for demo signals the operator manually injected."""
    event_type = payload.get("event_type") or "payment"
    customer = payload.get("customer") or "Unknown customer"
    amount = payload.get("amount_usd") or 0
    content_text = (
        f"Stripe {event_type}: {customer} — "
        f"${int(amount):,}"
    )
    body = {
        "content_text": content_text,
        "content": {
            "stripe_event": event_type,
            "customer": customer,
            "amount_usd": amount,
        },
        "external_id": f"stripe-{time.time_ns()}",
    }
    return ("internal:state_change", body, {})


# ---------------------------------------------------------------------
# Signal injection
# ---------------------------------------------------------------------


async def inject_signal(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    actor_id: UUID,
    channel: str,
    payload: dict[str, Any],
    demo_session_id: UUID | None,
    actor_repo: Any,
    alias_repo: Any,
    embedder: Any,
) -> dict[str, Any]:
    """Run the simulator payload through the real ingestion path.

    Returns `{observation_id, deduped, trigger_queue_id}` — the same
    shape the public POST /ingest endpoint returns.
    """
    # Translate the UI's friendly payload into the wire shape each
    # handler actually expects (channel names, headers, payload keys).
    real_channel, real_payload, real_headers = _translate_payload(
        channel, payload,
    )

    # Resolve the simulator's free-text "author" / "from" / "organizer"
    # to a real actor in this tenant and seed actor_identity_mappings
    # so the ingestion core's source_actor_ref → actor_id lookup
    # succeeds. Without this, observations come in with actor_id=NULL
    # and the Think reasoning treats the sender as "external" — which
    # is why a Slack message authored as Sarah didn't connect to the
    # burnout model already in the substrate.
    real_payload = await _seed_demo_actor_identity(
        pool=pool,
        tenant_id=tenant_id,
        real_channel=real_channel,
        ui_payload=payload,
        real_payload=real_payload,
    )

    # Tag with demo_session_id so cost attribution and replay can find it.
    if demo_session_id is not None:
        real_payload = {
            **real_payload,
            "demo_session_id": str(demo_session_id),
        }

    result = await ingest(
        real_channel,
        real_payload,
        pool=pool,
        tenant_id=tenant_id,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        request_headers=real_headers,
    )

    if demo_session_id is not None:
        async with pool.acquire() as conn:
            await increment_signal_count(conn, demo_session_id)

    return {
        "observation_id": str(result.observation.id),
        "deduped": result.deduped,
        "trigger_queue_id": (
            str(result.trigger_queue_id)
            if result.trigger_queue_id else None
        ),
    }


# ---------------------------------------------------------------------
# Suggested signals — UI affordance per tab per company
# ---------------------------------------------------------------------


SuggestedSignal = dict[str, Any]


def list_suggested_signals(company_id: str) -> dict[str, list[SuggestedSignal]]:
    """Return suggested signals grouped by channel tab for the given
    company. Used by the simulator UI's "Suggested signals" section.

    Each signal is one click → fills the form. Wording calibrated to
    the company's spec so the demo flow is rehearsable."""
    return _SUGGESTED.get(company_id, _DEFAULT_SUGGESTED)


_DEFAULT_SUGGESTED: dict[str, list[SuggestedSignal]] = {
    "slack": [
        {
            "label": "Customer asks about feature",
            "channel_name": "#sales",
            "author_label": "AE — Diego Rivera",
            "text": "Just got off a call with a prospect — they're asking about SAML SSO timeline. Third one this month.",
        },
        {
            "label": "Engineer reports slip",
            "channel_name": "#eng",
            "author_label": "Eng — Sarah Chen",
            "text": "I'm going to slip the rate-limiter work by 2 weeks — found a deeper issue in the request pipeline.",
        },
    ],
    "email": [
        {
            "label": "Customer escalation",
            "from_label": "CSM — Avery Nakamura",
            "to_label": "CEO",
            "subject": "Acme escalation thread",
            "body": "Escalating: Acme's CTO is unhappy about the missed Q2 commitment. We need a response by EOD.",
        },
    ],
    "github": [
        {
            "label": "PR merged for critical fix",
            "repo": "platform/gateway",
            "event_type": "pr_merged",
            "author_label": "Eng — Theo Schmidt",
            "title": "Fix request-timeout regression in /v1 endpoints",
        },
    ],
    "calendar": [
        {
            "label": "Founder/VP Eng sync",
            "title": "1:1 — Maya / Tom",
            "attendees_labels": ["CEO", "VP Eng — Tom Bishop"],
            "minutes_ago": 60,
        },
    ],
    "stripe": [
        {
            "label": "Payment failed",
            "event_type": "payment_failed",
            "customer_label": "Acme Corp",
            "amount_usd": 24000,
        },
    ],
    "custom": [
        {
            "label": "Custom JSON example",
            "channel": "system:custom",
            "json": {"event": "custom_demo", "note": "fill in your own payload"},
        },
    ],
}


_SUGGESTED: dict[str, dict[str, list[SuggestedSignal]]] = {
    "truss": {
        "slack": [
            {
                "label": "Linear asks about SSO",
                "channel_name": "#sales",
                "author_label": "Founder — Jules Park",
                "text": "Linear just asked us about SSO too — that's the 4th design partner this quarter. Should we accelerate?",
            },
            {
                "label": "Engineer slip warning",
                "channel_name": "#eng",
                "author_label": "Eng — Sarah Chen",
                "text": "API redesign is uglier than I scoped — going to need 3 more weeks. Three customers waiting on stable v1.",
            },
            {
                "label": "Founder context",
                "channel_name": "#founder-private",
                "author_label": "Founder — Jules Park",
                "text": "Haven't synced with Tom on hiring priorities in 3 weeks; need to fix that this week.",
            },
        ],
        "email": [
            {
                "label": "Replit follow-up",
                "from_label": "Replit — Eng Lead",
                "to_label": "CEO",
                "subject": "Following up on SSO conversation",
                "body": "Hey Maya — circling back on SSO. We're standardizing on enterprise auth across vendors and need a date.",
            },
        ],
        "github": [
            {
                "label": "Sarah opens PR for redesign",
                "repo": "truss/api",
                "event_type": "pr_opened",
                "author_label": "Eng — Sarah Chen",
                "title": "[WIP] API redesign — feedback wanted",
            },
        ],
        "calendar": [
            {
                "label": "Jules ↔ Tom 1:1",
                "title": "1:1 — Jules / Tom (overdue)",
                "attendees_labels": ["Founder — Jules Park", "VP Eng — Tom Bishop"],
                "minutes_ago": 0,
            },
        ],
        "stripe": [
            {
                "label": "Vercel invoice paid",
                "event_type": "payment",
                "customer_label": "Vercel",
                "amount_usd": 88000,
            },
        ],
        "custom": _DEFAULT_SUGGESTED["custom"],
    },
    "northwind": {
        "slack": [
            {
                "label": "Acme asks about SAML again",
                "channel_name": "#sales",
                "author_label": "AE — Diego Rivera",
                "text": "Acme is asking about the SAML feature again — 4th time in 2 weeks. They're starting to flag it as a contract risk.",
            },
            {
                "label": "Engineer raises capacity flag",
                "channel_name": "#eng-leads",
                "author_label": "EM — Marcus Lee",
                "text": "We're at 91% capacity heading into Q3 push. Anything else lands and we slip.",
            },
        ],
        "email": [
            {
                "label": "Acme contract renewal",
                "from_label": "CSM — Avery Nakamura",
                "to_label": "CEO",
                "subject": "Acme — contract renewal in 60 days",
                "body": "Acme is up for renewal. Their procurement is asking for SAML to be locked in writing.",
            },
        ],
        "github": _DEFAULT_SUGGESTED["github"],
        "calendar": [
            {
                "label": "Acme exec sync",
                "title": "Acme Q3 Roadmap Sync",
                "attendees_labels": ["CEO", "Acme CIO"],
                "minutes_ago": 15,
            },
        ],
        "stripe": [
            {
                "label": "Notion expansion payment",
                "event_type": "payment",
                "customer_label": "Notion",
                "amount_usd": 410000,
            },
        ],
        "custom": _DEFAULT_SUGGESTED["custom"],
    },
    "meridian": {
        "slack": [
            {
                "label": "Industrium gives 2-week extension",
                "channel_name": "#industrium-warroom",
                "author_label": "CSM — Avery Nakamura",
                "text": "Industrium CSM said they'll consider giving us 2 more weeks if we commit to a specific milestone by Friday.",
            },
            {
                "label": "VP Eng off-channel",
                "channel_name": "#eng-leads",
                "author_label": "VP Eng — Tom Bishop",
                "text": "I haven't been looped in on the Industrium thread — what's the actual scope risk?",
            },
        ],
        "email": [
            {
                "label": "Industrium escalation",
                "from_label": "Industrium — VP Operations",
                "to_label": "CEO",
                "subject": "Escalating — missed Q2 milestone",
                "body": "Sam, this is the second time we've missed on this commitment. We need a credible recovery plan by next week.",
            },
        ],
        "github": [
            {
                "label": "Industrium PR merged",
                "repo": "meridian/optimizer",
                "event_type": "pr_merged",
                "author_label": "Eng — Theo Schmidt",
                "title": "Industrium-specific batch sizing fix",
            },
        ],
        "calendar": [
            {
                "label": "Industrium war-room",
                "title": "Industrium Recovery War Room",
                "attendees_labels": ["CEO", "VP Eng — Tom Bishop", "CSM — Avery Nakamura"],
                "minutes_ago": 5,
            },
        ],
        "stripe": [
            {
                "label": "Acme Co. churn risk — invoice failed",
                "event_type": "payment_failed",
                "customer_label": "Acme Co.",
                "amount_usd": 31600,
            },
        ],
        "custom": _DEFAULT_SUGGESTED["custom"],
    },
}


__all__ = ["inject_signal", "list_suggested_signals"]
