"""Self-contained WhatsApp webhook and semantic capabilities."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
from typing import Any

from services.ingest.source_contract.connector import BindingContext, OperationContext
from services.ingest.source_contract.capabilities.installation import (
    ConfigurationIssue,
    ConfigurationValidation,
    SecretRotationRequest,
    SecretRotationVerification,
)
from services.ingest.source_contract.errors import (
    AuthenticationRejectedError,
    PayloadRejectedError,
)
from services.ingest.source_contract.identity import SlotId
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    IdentityInput,
    NormalizationInput,
    ObservationDraft,
    SourceRecord,
    VerifiedWebhookEvent,
    VerifiedWebhookResult,
)


class WhatsAppConfiguration:
    async def validate_configuration(
        self,
        configuration: dict[str, Any],
        context: OperationContext,
    ) -> ConfigurationValidation:
        del context
        external_id = configuration.get("external_installation_id")
        if isinstance(external_id, str) and external_id.strip():
            return ConfigurationValidation(valid=True)
        return ConfigurationValidation(
            valid=False,
            issues=(
                ConfigurationIssue(
                    field="external_installation_id",
                    code="required",
                    message="the WhatsApp phone number id is required",
                ),
            ),
        )


class WhatsAppSecretRotation:
    async def verify_candidate(
        self,
        request: SecretRotationRequest,
        context: OperationContext,
    ) -> SecretRotationVerification:
        del context
        if str(request.slot) != "app_secret":
            return SecretRotationVerification(
                valid=False,
                reason_code="slot_not_declared",
                message="the candidate targets an undeclared WhatsApp slot",
            )
        return SecretRotationVerification(
            valid=True,
            reason_code="candidate_handle_accepted",
            message="the host may atomically promote the app secret handle",
        )


def _payload(record: SourceRecord) -> dict[str, Any]:
    if not isinstance(record.payload, dict):
        raise PayloadRejectedError("WhatsApp requires a JSON object payload")
    return record.payload


def whatsapp_external_id(input: IdentityInput) -> str:
    payload = _payload(input.record)
    metadata = payload.get("metadata")
    phone = metadata.get("phone_number_id") if isinstance(metadata, dict) else None
    message = payload.get("message")
    if isinstance(message, dict) and isinstance(message.get("id"), str):
        return f"whatsapp:{phone}:{message['id']}"
    status = payload.get("status")
    if (
        isinstance(status, dict)
        and isinstance(status.get("id"), str)
        and isinstance(status.get("status"), str)
    ):
        return f"whatsapp:{phone}:status:{status['id']}:{status['status']}"
    raise PayloadRejectedError("WhatsApp identity requires message or status id")


def _timestamp(value: Any) -> datetime:
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError) as exc:
        raise PayloadRejectedError("WhatsApp timestamp is invalid") from exc


def _contact_name(contacts: Any, sender: str | None) -> str | None:
    for contact in contacts if isinstance(contacts, list) else ():
        if not isinstance(contact, dict) or (
            sender is not None and contact.get("wa_id") != sender
        ):
            continue
        profile = contact.get("profile")
        name = profile.get("name") if isinstance(profile, dict) else None
        if isinstance(name, str) and name:
            return name
    return None


def _message_text(message: dict[str, Any]) -> str:
    message_type = message.get("type")
    if message_type == "text":
        text = (message.get("text") or {}).get("body")
        return text if isinstance(text, str) and text else "[empty text]"
    if message_type in {"image", "video", "audio", "document", "sticker", "voice"}:
        media = message.get(message_type) or {}
        caption = media.get("caption") if isinstance(media, dict) else None
        filename = media.get("filename") if isinstance(media, dict) else None
        label = f"[{message_type}]" + (f" {filename}" if filename else "")
        return f"{label}: {caption}" if caption else label
    if message_type == "location":
        location = message.get("location") or {}
        name = location.get("name") or location.get("address")
        coordinates = f"{location.get('latitude')},{location.get('longitude')}"
        return f"[location] {name or coordinates}"
    if message_type == "contacts":
        names = [
            str((contact.get("name") or {}).get("formatted_name"))
            for contact in message.get("contacts") or ()
            if isinstance(contact, dict)
            and (contact.get("name") or {}).get("formatted_name")
        ]
        return "[contact card] " + (", ".join(names) if names else "shared a contact")
    if message_type == "interactive":
        interactive = message.get("interactive") or {}
        interactive_type = interactive.get("type")
        reply = interactive.get(interactive_type) or {}
        return str(
            reply.get("title") or reply.get("id") or f"[interactive:{interactive_type}]"
        )
    if message_type == "button":
        return str((message.get("button") or {}).get("text") or "[button]")
    if message_type == "reaction":
        reaction = message.get("reaction") or {}
        return f"[reaction] {reaction.get('emoji') or ''} to {reaction.get('message_id')}".strip()
    if message_type == "order":
        order = message.get("order") or {}
        return f"[order] {len(order.get('product_items') or ())} item(s), catalog {order.get('catalog_id')}"
    if message_type == "system":
        return "[system] " + str(
            (message.get("system") or {}).get("body") or "system update"
        )
    if message_type == "unsupported":
        return "[unsupported message type]"
    return f"[{message_type or 'unknown'}]"


class WhatsAppNormalization:
    async def normalize(
        self, request: NormalizationInput, context: OperationContext
    ) -> tuple[ObservationDraft, ...]:
        payload = _payload(request.record)
        if isinstance(payload.get("status"), dict):
            return (self._status(payload),)
        if isinstance(payload.get("message"), dict):
            return (self._message(payload),)
        raise PayloadRejectedError("WhatsApp payload has no message or status")

    @staticmethod
    def _message(payload: dict[str, Any]) -> ObservationDraft:
        message = payload["message"]
        metadata = payload.get("metadata") or {}
        identifier = message.get("id")
        sender = message.get("from")
        if not isinstance(identifier, str) or not isinstance(sender, str):
            raise PayloadRejectedError("WhatsApp message requires id and sender")
        phone = metadata.get("phone_number_id")
        name = _contact_name(payload.get("contacts"), sender)
        text = _message_text(message)
        content: dict[str, Any] = {
            "wamid": identifier,
            "from": sender,
            "type": message.get("type"),
            "phone_number_id": phone,
            "display_phone_number": metadata.get("display_phone_number"),
            "contact_name": name,
            "timestamp": message.get("timestamp"),
        }
        message_type = message.get("type")
        if isinstance(message_type, str) and isinstance(
            message.get(message_type), (dict, list)
        ):
            content[message_type] = message[message_type]
        for optional in ("context", "referral"):
            if isinstance(message.get(optional), dict):
                content[optional] = message[optional]
        entities: list[dict[str, Any]] = [{"type": "whatsapp_user", "id": sender}]
        if name:
            entities.append({"type": "person_name", "id": name})
        return ObservationDraft(
            source_channel="whatsapp:message",
            content_text=text,
            content=content,
            occurred_at=_timestamp(message.get("timestamp")),
            trust_tier="attested_agent",
            kind="signal",
            source_actor_ref=f"whatsapp:{sender}",
            external_id=f"whatsapp:{phone}:{identifier}",
            entities_hint=tuple(entities),
            raw_payload=payload,
        )

    @staticmethod
    def _status(payload: dict[str, Any]) -> ObservationDraft:
        status = payload["status"]
        metadata = payload.get("metadata") or {}
        identifier = status.get("id")
        state = status.get("status")
        if not isinstance(identifier, str) or not isinstance(state, str):
            raise PayloadRejectedError("WhatsApp status requires id and state")
        recipient = status.get("recipient_id")
        phone = metadata.get("phone_number_id")
        content: dict[str, Any] = {
            "wamid": identifier,
            "status": state,
            "recipient_id": recipient,
            "phone_number_id": phone,
            "timestamp": status.get("timestamp"),
            "_whatsapp_kind": "status",
        }
        for optional in ("conversation", "pricing", "errors"):
            if status.get(optional) is not None:
                content[optional] = status[optional]
        return ObservationDraft(
            source_channel="whatsapp:message",
            content_text=f"WhatsApp message {identifier} to {recipient}: {state}",
            content=content,
            occurred_at=_timestamp(status.get("timestamp")),
            trust_tier="authoritative",
            kind="state_change",
            source_actor_ref=f"whatsapp:{recipient}" if recipient else None,
            external_id=f"whatsapp:{phone}:status:{identifier}:{state}",
            entities_hint=(),
            raw_payload=payload,
        )


class WhatsAppWebhook:
    def __init__(self, binding: BindingContext) -> None:
        self._binding = binding

    async def verify_and_decode(
        self, request: BoundedWebhookRequest, context: OperationContext
    ) -> VerifiedWebhookResult:
        secret = await self._binding.services.secrets.resolve(SlotId("app_secret"))
        signature = next(
            (
                value
                for key, value in request.headers.items()
                if key.lower() == "x-hub-signature-256"
            ),
            "",
        )
        expected = (
            "sha256="
            + hmac.new(secret.reveal_bytes(), request.body, hashlib.sha256).hexdigest()
        )
        if not hmac.compare_digest(expected, signature):
            raise AuthenticationRejectedError("Meta webhook signature is invalid")
        try:
            payload = json.loads(request.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PayloadRejectedError(
                "WhatsApp webhook body is not valid JSON"
            ) from exc
        if not isinstance(payload, dict):
            raise PayloadRejectedError("WhatsApp webhook body must be an object")
        events: list[VerifiedWebhookEvent] = []
        for entry in payload.get("entry") or ():
            if not isinstance(entry, dict):
                continue
            for change in entry.get("changes") or ():
                value = change.get("value") if isinstance(change, dict) else None
                if not isinstance(value, dict):
                    continue
                metadata = value.get("metadata") or {}
                phone = metadata.get("phone_number_id")
                if not isinstance(phone, str) or not phone:
                    continue
                contacts = value.get("contacts") or []
                for message in value.get("messages") or ():
                    if isinstance(message, dict):
                        events.append(
                            VerifiedWebhookEvent(
                                external_installation_id=phone,
                                native_event_type="message",
                                record=SourceRecord(
                                    native_type="message",
                                    payload={
                                        "message": message,
                                        "metadata": metadata,
                                        "contacts": contacts,
                                    },
                                ),
                                verification_evidence={"scheme": "meta-hmac-sha256"},
                            )
                        )
                for status in value.get("statuses") or ():
                    if isinstance(status, dict):
                        events.append(
                            VerifiedWebhookEvent(
                                external_installation_id=phone,
                                native_event_type="status",
                                record=SourceRecord(
                                    native_type="status",
                                    payload={"status": status, "metadata": metadata},
                                ),
                                verification_evidence={"scheme": "meta-hmac-sha256"},
                            )
                        )
        return VerifiedWebhookResult(events=tuple(events))


__all__ = [
    "WhatsAppConfiguration",
    "WhatsAppNormalization",
    "WhatsAppSecretRotation",
    "WhatsAppWebhook",
    "whatsapp_external_id",
]
