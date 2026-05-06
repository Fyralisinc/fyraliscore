"""services/ingestion/handlers/email.py — inbound email handler.

BUILD-PLAN §3 Prompt 2.B:
    "Inbound via webhook (Postmark/SendGrid — either shape, document
     which). Minimum fields: from, to, subject, body, references.
     source_actor_ref derived from `from:` address (prefix `email:`
     to normalize).
     entities_hint: parse email addresses in body against `ActorRepo`
     identity mappings, URLs, and any phrases that match
     `EntityAliasRepo.fast_path_resolve`.
     Trust tier: `inferential`.
     `external_id` = `message-id` header."

Payload shape
-------------

Accepts EITHER shape:

Postmark inbound (docs.postmarkapp.com/developer/user-guide/inbound):
    {
        "From":        "alice@example.com",
        "FromFull":    {"Email": "alice@example.com", "Name": "Alice"},
        "To":          "bob@company.com",
        "ToFull":      [{"Email": "bob@company.com"}],
        "Subject":     "re: billing",
        "MessageID":   "<abc@mail>",
        "TextBody":    "Hi Bob, ...",
        "HtmlBody":    "<p>...</p>",
        "Headers":     [{"Name": "References", "Value": "<a@x>"}, ...],
        "Date":        "Mon, 21 Apr 2026 10:00:00 +0000",
    }

SendGrid inbound parse (docs.sendgrid.com/for-developers/parsing-email):
    form-fields: from, to, subject, text, html, headers, envelope
    (we accept a pre-normalized dict with the same keys as Postmark
     when plumbed through multipart→json).

Internal canonical (what we prefer — any other shape is coerced):
    {
        "from":       "alice@example.com",
        "from_name":  "Alice",
        "to":         ["bob@company.com"],
        "subject":    "re: billing",
        "body":       "Hi Bob, ...",
        "message_id": "<abc@mail>",
        "references": ["<prev@mail>"],
        "date":       "2026-04-21T10:00:00Z",
    }
"""
from __future__ import annotations

import hashlib
import hmac
import re
from datetime import datetime, timezone
from email.utils import parseaddr, parsedate_to_datetime
from typing import Any

from lib.shared.errors import ValidationError

from services.ingestion.handlers import (
    HandlerError,
    ObservationDraft,
    register,
)


_CHANNEL = "email:inbound"


class EmailSignatureError(HandlerError):
    default_code = "email_signature_invalid"


# Email-address regex (RFC 5322 subset, good enough for entity hints).
_EMAIL_RE = re.compile(
    r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"
)
# URL regex for entity hints.
_URL_RE = re.compile(r"https?://[^\s<>\"]+", flags=re.IGNORECASE)


def verify_email_signature(
    body: bytes,
    signature_header: str | None,
    secret: str | None,
) -> None:
    """Shared-secret verification for the inbound email webhook.

    Email providers (Postmark, SendGrid) each have their own scheme
    (Postmark verifies by basic-auth; SendGrid via shared webhook
    secret). We accept a generic `X-Webhook-Signature` HMAC-SHA256
    here, which the Gateway populates depending on the configured
    provider. Missing secret or mismatch raises EmailSignatureError.
    """
    if not secret:
        raise EmailSignatureError(
            "EMAIL_WEBHOOK_SECRET is not configured",
            reason="missing_secret",
        )
    if not signature_header:
        raise EmailSignatureError(
            "missing X-Webhook-Signature header", reason="missing_signature"
        )
    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256)
    expected = mac.hexdigest()
    if not hmac.compare_digest(expected.lower(), signature_header.strip().lower()):
        raise EmailSignatureError(
            "email signature mismatch", reason="mismatch"
        )


def _normalize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Coerce Postmark or SendGrid shape into the canonical dict.

    Returns a dict with keys: from, from_name, to, subject, body,
    message_id, references, date.
    """
    # Already canonical.
    if "from" in payload and "subject" in payload and "body" in payload:
        return {
            "from": payload.get("from"),
            "from_name": payload.get("from_name"),
            "to": payload.get("to") or [],
            "subject": payload.get("subject") or "",
            "body": payload.get("body") or "",
            "message_id": payload.get("message_id"),
            "references": payload.get("references") or [],
            "date": payload.get("date"),
        }

    # Postmark shape.
    if "From" in payload and "TextBody" in payload:
        from_full = payload.get("FromFull") or {}
        to_full = payload.get("ToFull") or []
        to_list = [
            addr.get("Email") for addr in to_full if isinstance(addr, dict)
        ] if isinstance(to_full, list) else []
        if not to_list:
            to_raw = payload.get("To") or ""
            to_list = [t.strip() for t in to_raw.split(",") if t.strip()]

        # Extract References: header if present.
        references: list[str] = []
        headers = payload.get("Headers") or []
        for h in headers if isinstance(headers, list) else []:
            if isinstance(h, dict) and h.get("Name", "").lower() == "references":
                references = [r for r in h.get("Value", "").split() if r]
                break

        return {
            "from": payload.get("From"),
            "from_name": from_full.get("Name") if isinstance(from_full, dict) else None,
            "to": to_list,
            "subject": payload.get("Subject") or "",
            "body": payload.get("TextBody") or payload.get("HtmlBody") or "",
            "message_id": payload.get("MessageID"),
            "references": references,
            "date": payload.get("Date"),
        }

    # SendGrid-like (flat keys but capitalised differently).
    lower = {k.lower(): v for k, v in payload.items()}
    if "from" in lower and ("text" in lower or "html" in lower):
        return {
            "from": lower.get("from"),
            "from_name": None,
            "to": [lower["to"]] if lower.get("to") else [],
            "subject": lower.get("subject") or "",
            "body": lower.get("text") or lower.get("html") or "",
            "message_id": lower.get("message-id") or lower.get("messageid"),
            "references": [],
            "date": None,
        }

    raise ValidationError(
        "unrecognised email payload shape — expected Postmark/SendGrid/canonical",
        channel=_CHANNEL,
    )


def _parse_date(date_str: Any) -> datetime:
    if not date_str:
        return datetime.now(timezone.utc)
    if isinstance(date_str, datetime):
        return date_str if date_str.tzinfo else date_str.replace(tzinfo=timezone.utc)
    s = str(date_str)
    # ISO first.
    try:
        s2 = s[:-1] + "+00:00" if s.endswith("Z") else s
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        pass
    # RFC 2822.
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)


async def _hint_from_body(
    body: str,
    tenant_id: Any,
    actor_resolver=None,
    alias_resolver=None,
) -> list[dict[str, Any]]:
    """Build entity hints from body text.

    - Every email address matched via `actor_resolver.resolve_by_source_actor_ref`
      against `email:<addr>`. Unknown addresses still land as
      {type: 'email_address', id: addr}.
    - Every URL → {type: 'url', id: <url>}.
    - Every candidate phrase matched via `alias_resolver.fast_path_resolve`
      lands as its canonical ref.

    Both resolvers are optional — in tests / handlers without a live
    DB, only string-matching hints are produced.
    """
    hints: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    def _add(entity: dict[str, Any]) -> None:
        key = (str(entity.get("type")), str(entity.get("id")))
        if key in seen:
            return
        seen.add(key)
        hints.append(entity)

    # Emails first — known wins over unknown.
    for match in _EMAIL_RE.finditer(body or ""):
        addr = match.group(0).lower()
        actor_id = None
        if actor_resolver is not None:
            try:
                actor_id = await actor_resolver.resolve_by_source_actor_ref(
                    f"email:{addr}"
                )
            except Exception:
                actor_id = None
        if actor_id:
            _add({"type": "actor", "id": str(actor_id)})
        else:
            _add({"type": "email_address", "id": addr})

    for match in _URL_RE.finditer(body or ""):
        _add({"type": "url", "id": match.group(0)})

    # Alias fast-path over candidate phrases. We take 2-4-word phrases
    # (the fast-path normalizer handles case/whitespace). Conservative:
    # only emit when the resolver confirms.
    if alias_resolver is not None and body:
        words = re.findall(r"[A-Za-z][A-Za-z0-9_\-]+", body)
        # 2- and 3-grams.
        candidates: set[str] = set()
        for n in (2, 3):
            for i in range(len(words) - n + 1):
                phrase = " ".join(words[i:i + n])
                if len(phrase) >= 5:
                    candidates.add(phrase)
        for phrase in candidates:
            try:
                ref = await alias_resolver.fast_path_resolve(phrase, tenant_id)
            except Exception:
                ref = None
            if ref:
                _add(ref)
    return hints


async def handle_email_webhook(
    payload: dict[str, Any],
    headers: dict[str, str],
    *,
    tenant_id: Any = None,
    actor_resolver: Any = None,
    alias_resolver: Any = None,
) -> ObservationDraft:
    """Inbound email → ObservationDraft.

    Optional `tenant_id`, `actor_resolver`, `alias_resolver` enrich
    `entities_hint` when the caller has a live DB. If absent the
    handler falls back to string-only hints.
    """
    if not isinstance(payload, dict):
        raise ValidationError(
            "email payload must be a JSON object", channel=_CHANNEL
        )

    norm = _normalize_payload(payload)
    from_raw = norm.get("from") or ""
    from_name, from_addr = parseaddr(str(from_raw))
    from_addr = (from_addr or "").strip().lower()
    if not from_addr:
        raise ValidationError(
            "email payload missing 'from' address", channel=_CHANNEL
        )
    from_display = from_name or norm.get("from_name") or from_addr

    subject = (norm.get("subject") or "").strip()
    body = norm.get("body") or ""
    message_id = norm.get("message_id")
    references = norm.get("references") or []
    to_list = norm.get("to") or []

    # Content text: readable one-liner that the resolver worker can
    # use as "context excerpt".
    content_text = (
        f"{from_display} emailed '{subject}': {body[:200]}".strip()
    )

    # Author resolution — always prefer the actor_id if available.
    source_actor_ref = f"email:{from_addr}"
    author_actor_id = None
    if actor_resolver is not None:
        try:
            author_actor_id = await actor_resolver.resolve_by_source_actor_ref(
                source_actor_ref
            )
        except Exception:
            author_actor_id = None

    entities_hint = await _hint_from_body(
        body, tenant_id, actor_resolver, alias_resolver
    )
    # Always include the sender as an entity hint.
    if author_actor_id:
        entities_hint.insert(0, {"type": "actor", "id": str(author_actor_id)})
    else:
        entities_hint.insert(
            0, {"type": "email_address", "id": from_addr}
        )

    occurred_at = _parse_date(norm.get("date"))

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "from": from_addr,
            "from_name": from_display,
            "to": to_list,
            "subject": subject,
            "body": body,
            "message_id": message_id,
            "references": references,
        },
        occurred_at=occurred_at,
        trust_tier="inferential",
        kind="signal",
        source_actor_ref=source_actor_ref,
        external_id=message_id,
        entities_hint=entities_hint,
        raw_payload=payload,
    )


# ---------------------------------------------------------------------
# Registry entry. The registry's HandlerFn signature is (payload,
# headers) -> ObservationDraft; the optional tenant_id / resolver
# kwargs let the ingestion core pass through enrichers.
# ---------------------------------------------------------------------

@register(_CHANNEL)
async def _registered_handler(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    return await handle_email_webhook(payload, headers)


__all__ = [
    "EmailSignatureError",
    "verify_email_signature",
    "handle_email_webhook",
]
