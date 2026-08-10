"""Session-oriented Discord, Telegram, and Signal connector capabilities."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from services.ingest.connectors.fleet import (
    FleetConfiguration,
    FleetIngestion,
    FleetNormalization,
    FleetSecretRotation,
    FleetWebhook,
    _identity,
)
from services.ingest.connectors.native import (
    CredentialHealthProbe,
    LocalCredentialCleanup,
    NativeIdentity,
    NativeSourceConnector,
    _manifest,
)
from services.ingest.connectors.provider_spec import SourceProfile
from services.ingest.source_contract.capabilities import (
    CLEANUP_V1,
    CONFIGURATION_V1,
    GATEWAY_STREAM_V1,
    HEALTH_PROBE_V1,
    HISTORICAL_PULL_V1,
    IDENTITY_V1,
    NORMALIZATION_V1,
    RECONCILIATION_V1,
    SECRET_ROTATION_V1,
    WEBHOOK_V1,
)
from services.ingest.source_contract.capabilities.ingestion import (
    GatewayBatch,
    GatewayOpenRequest,
    GatewayReceiveRequest,
    GatewaySession,
)
from services.ingest.source_contract.connector import BindingContext, OperationContext
from services.ingest.source_contract.errors import (
    AuthenticationRejectedError,
    PayloadRejectedError,
    RateLimitedError,
    TransientSourceError,
)
from services.ingest.source_contract.host_services import (
    GovernedGatewayRequest,
    GovernedHttpRequest,
)
from services.ingest.source_contract.identity import SlotId
from services.ingest.source_contract.models import CursorState, SourceRecord


DISCORD = SourceProfile(
    source="discord", ingress_kinds=("backfill", "gateway", "webhook"),
    api_origin="https://discord.com", collection_path="/api/v10/users/@me/guilds",
    channel="discord:message", native_type="message", record_keys=("messages", "items"),
    identity_fields=("id", "message_id"), occurred_fields=("timestamp", "edited_timestamp"),
    text_fields=("content", "name"), auth_slot="bot_token", auth_scheme="Bot",
    webhook_mode="ed25519", webhook_header="x-signature-ed25519",
    webhook_secret_slot="webhook_public_key",
)
TELEGRAM = SourceProfile(
    source="telegram", ingress_kinds=("backfill", "gateway"),
    api_origin="https://api.telegram.org", collection_path="/getUpdates",
    channel="telegram:message", native_type="message", record_keys=("result",),
    identity_fields=("update_id", "message.message_id", "message_id"),
    occurred_fields=("message.date", "date", "edit_date"),
    text_fields=("message.text", "text", "caption"), auth_slot="bot_token",
)
SIGNAL = SourceProfile(
    source="signal", ingress_kinds=("backfill", "gateway"),
    api_origin="https://chat.signal.org", collection_path="/v1/messages",
    channel="signal:message", native_type="message", record_keys=("messages", "items"),
    identity_fields=("timestamp", "id"), occurred_fields=("timestamp",),
    text_fields=("message", "body", "text"), auth_slot="linked_device_token",
)


class DiscordGateway:
    def __init__(self, binding: BindingContext) -> None:
        self._binding = binding

    async def open(self, request: GatewayOpenRequest, context: OperationContext) -> GatewaySession:
        resume = dict(request.resume_state.payload) if request.resume_state else {}
        url = str(resume.get("resume_gateway_url") or "wss://gateway.discord.gg/?v=10&encoding=json")
        connection_id = await context.services.gateway.connect(GovernedGatewayRequest(url=url))
        hello = await context.services.gateway.receive_json(connection_id)
        if hello.get("op") != 10:
            await context.services.gateway.close(connection_id, code=4000)
            raise TransientSourceError("Discord gateway did not send Hello")
        token = await self._binding.services.secrets.resolve(SlotId("bot_token"))
        if resume.get("session_id") and resume.get("sequence") is not None:
            command = {
                "op": 6,
                "d": {
                    "token": token.reveal_text(),
                    "session_id": resume["session_id"],
                    "seq": int(resume["sequence"]),
                },
            }
        else:
            data = await context.services.installation_store.read("discord_gateway")
            intents = int(data.values.get("intents", 513)) if data is not None else 513
            command = {
                "op": 2,
                "d": {
                    "token": token.reveal_text(),
                    "intents": intents,
                    "properties": {"os": "linux", "browser": "fyralis", "device": "fyralis"},
                },
            }
        await context.services.gateway.send_json(connection_id, command)
        resume["heartbeat_interval_ms"] = int((hello.get("d") or {}).get("heartbeat_interval", 45000))
        return GatewaySession(session_id=connection_id, resume_state=CursorState(schema_version=1, payload=resume))

    async def receive(self, request: GatewayReceiveRequest, context: OperationContext) -> GatewayBatch:
        state = dict(request.session.resume_state.payload) if request.session.resume_state else {}
        records: list[SourceRecord] = []
        closed = False
        while len(records) < request.max_records:
            heartbeat_seconds = max(
                1.0, float(state.get("heartbeat_interval_ms", 45_000)) / 1000
            )
            try:
                frame = await asyncio.wait_for(
                    context.services.gateway.receive_json(request.session.session_id),
                    timeout=heartbeat_seconds,
                )
            except TimeoutError:
                await context.services.gateway.send_json(
                    request.session.session_id,
                    {"op": 1, "d": state.get("sequence")},
                )
                state["heartbeat_sent"] = True
                break
            opcode = frame.get("op")
            if opcode == 1:
                await context.services.gateway.send_json(request.session.session_id, {"op": 1, "d": state.get("sequence")})
                continue
            if opcode == 11:
                state["heartbeat_sent"] = False
                continue
            if opcode == 7:
                closed = True
                break
            if opcode == 9:
                if frame.get("d") is not True:
                    state = {}
                closed = True
                break
            if opcode != 0:
                continue
            if frame.get("s") is not None:
                state["sequence"] = int(frame["s"])
            event_name = str(frame.get("t") or "DISPATCH")
            data = frame.get("d") if isinstance(frame.get("d"), dict) else {}
            if event_name == "READY":
                state["session_id"] = data.get("session_id")
                state["resume_gateway_url"] = data.get("resume_gateway_url")
                continue
            records.append(SourceRecord(native_type=event_name, payload=data, identity_hints={"sequence": str(frame.get("s") or "")}))
        return GatewayBatch(records=tuple(records), resume_state=CursorState(schema_version=1, payload=state), session_closed=closed)

    async def close(self, session: GatewaySession, context: OperationContext) -> None:
        await context.services.gateway.close(session.session_id, code=1000)


class TelegramGateway:
    def __init__(self, binding: BindingContext) -> None:
        self._binding = binding

    async def open(self, request: GatewayOpenRequest, context: OperationContext) -> GatewaySession:
        return GatewaySession(session_id=f"telegram:{self._binding.installation.id}", resume_state=request.resume_state or CursorState(schema_version=1, payload={"offset": 0}))

    async def receive(self, request: GatewayReceiveRequest, context: OperationContext) -> GatewayBatch:
        token = await self._binding.services.secrets.resolve(SlotId("bot_token"))
        offset = int((request.session.resume_state.payload if request.session.resume_state else {}).get("offset", 0))
        response = await context.services.http.send(
            GovernedHttpRequest(
                method="POST",
                url="https://api.telegram.org/bot{secret}/getUpdates",
                headers=(("content-type", "application/json"),),
                body=json.dumps({"offset": offset, "limit": request.max_records, "timeout": 25}).encode(),
                timeout_seconds=30,
                url_secret=token,
            )
        )
        if response.status_code == 429:
            raise RateLimitedError("Telegram getUpdates was rate limited")
        if response.status_code in {401, 403}:
            raise AuthenticationRejectedError("Telegram rejected the bot token")
        if response.status_code >= 500:
            raise TransientSourceError("Telegram is temporarily unavailable")
        try:
            payload = json.loads(response.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TransientSourceError("Telegram returned malformed JSON") from exc
        if response.status_code >= 400 or payload.get("ok") is not True:
            raise PayloadRejectedError("Telegram rejected getUpdates")
        updates = [item for item in payload.get("result", ()) if isinstance(item, dict)]
        if updates:
            offset = max(int(item.get("update_id", -1)) for item in updates) + 1
        return GatewayBatch(
            records=tuple(SourceRecord(native_type="update", payload=item) for item in updates),
            resume_state=CursorState(schema_version=1, payload={"offset": offset}),
        )

    async def close(self, session: GatewaySession, context: OperationContext) -> None:
        return None


class SignalGateway:
    def __init__(self, binding: BindingContext) -> None:
        self._binding = binding

    async def open(self, request: GatewayOpenRequest, context: OperationContext) -> GatewaySession:
        data = await context.services.installation_store.read("signal_gateway")
        url = str(data.values.get("wss_url") if data is not None else "wss://chat.signal.org/v1/messages")
        token = await self._binding.services.secrets.resolve(SlotId("linked_device_token"))
        connection_id = await context.services.gateway.connect(GovernedGatewayRequest(url=url, headers=(("authorization", f"Bearer {token.reveal_text()}"),)))
        return GatewaySession(session_id=connection_id, resume_state=request.resume_state)

    async def receive(self, request: GatewayReceiveRequest, context: OperationContext) -> GatewayBatch:
        state = dict(request.session.resume_state.payload) if request.session.resume_state else {}
        records: list[SourceRecord] = []
        for _ in range(request.max_records):
            frame = await context.services.gateway.receive_json(request.session.session_id)
            cursor = frame.get("timestamp") or frame.get("id")
            if cursor is not None:
                state["cursor"] = str(cursor)
            records.append(SourceRecord(native_type=str(frame.get("type") or "message"), payload=frame))
            if frame.get("more") is not True:
                break
        return GatewayBatch(records=tuple(records), resume_state=CursorState(schema_version=1, payload=state))

    async def close(self, session: GatewaySession, context: OperationContext) -> None:
        await context.services.gateway.close(session.session_id, code=1000)


def _build(profile: SourceProfile, gateway: Any) -> NativeSourceConnector:
    factories: dict[Any, Any] = {
        CONFIGURATION_V1.ref: lambda _context: FleetConfiguration(profile),
        SECRET_ROTATION_V1.ref: lambda _context: FleetSecretRotation(profile),
        HEALTH_PROBE_V1.ref: lambda context: CredentialHealthProbe(context, profile.secret_slots),
        CLEANUP_V1.ref: lambda _context: LocalCredentialCleanup(),
        HISTORICAL_PULL_V1.ref: lambda context: FleetIngestion(context, profile),
        RECONCILIATION_V1.ref: lambda context: FleetIngestion(context, profile),
        GATEWAY_STREAM_V1.ref: gateway,
        IDENTITY_V1.ref: lambda _context: NativeIdentity(lambda value: _identity(profile, value)),
        NORMALIZATION_V1.ref: lambda context: FleetNormalization(context, profile),
    }
    if profile.source == "discord":
        factories[WEBHOOK_V1.ref] = lambda context: FleetWebhook(context, profile)
    return NativeSourceConnector(_manifest(profile.source), factories)


def build_discord_connector() -> NativeSourceConnector:
    return _build(DISCORD, DiscordGateway)


def build_telegram_connector() -> NativeSourceConnector:
    return _build(TELEGRAM, TelegramGateway)


def build_signal_connector() -> NativeSourceConnector:
    return _build(SIGNAL, SignalGateway)


__all__ = ["build_discord_connector", "build_signal_connector", "build_telegram_connector"]
