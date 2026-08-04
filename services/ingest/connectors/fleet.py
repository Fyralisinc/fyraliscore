"""Reusable HTTP capability kit for first-party source connectors.

This module intentionally depends only on connector-local data and the public
source contract.  It does not call the legacy planner/fetcher/reconciler or
normalizer registries. Provider modules supply their own immutable wire
definitions; this module is not a source registry or dispatch table.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from services.ingest.connectors.native import (
    CredentialHealthProbe,
    LocalCredentialCleanup,
    NativeIdentity,
    NativeSourceConnector,
)
from services.ingest.connectors.provider_spec import SourceProfile
from services.ingest.source_contract.capabilities import (
    CLEANUP_V1,
    CONFIGURATION_V1,
    GATEWAY_STREAM_V1,
    HEALTH_PROBE_V1,
    HISTORICAL_PULL_V1,
    IDENTITY_V1,
    INCREMENTAL_POLL_V1,
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
from services.ingest.source_contract.capabilities.installation import (
    ConfigurationIssue,
    ConfigurationValidation,
    SecretRotationRequest,
    SecretRotationVerification,
)
from services.ingest.source_contract.connector import BindingContext, OperationContext
from services.ingest.source_contract.errors import (
    AuthenticationRejectedError,
    PayloadRejectedError,
    RateLimitedError,
    ResourceNotFoundError,
    TransientSourceError,
)
from services.ingest.source_contract.host_services import GovernedHttpRequest
from services.ingest.source_contract.identity import SlotId
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    CursorState,
    FetchedPage,
    FetchRequest,
    IdentityInput,
    NormalizationInput,
    ObservationDraft,
    PlanRequest,
    PlanResult,
    PollRequest,
    ReconciliationDecision,
    ReconciliationRequest,
    RepairShard,
    ShardPlan,
    SourceRecord,
    VerifiedWebhookEvent,
    VerifiedWebhookResult,
)


def _nested(value: Mapping[str, Any], path: str) -> Any:
    current: Any = value
    for part in path.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
    return current


def _first(value: Mapping[str, Any], fields: tuple[str, ...]) -> Any:
    for field in fields:
        candidate = _nested(value, field)
        if candidate not in (None, "", (), [], {}):
            return candidate
    return None


def _json_object(record: SourceRecord, profile: SourceProfile) -> dict[str, Any]:
    if not isinstance(record.payload, dict):
        raise PayloadRejectedError(f"{profile.source} requires a JSON object payload")
    return record.payload


def _time(value: Any, fallback: datetime) -> datetime:
    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 10_000_000_000:
            seconds /= 1000.0
        try:
            return datetime.fromtimestamp(seconds, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return fallback
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            return _time(int(raw), fallback)
        try:
            parsed = datetime.fromisoformat(
                raw[:-1] + "+00:00" if raw.endswith("Z") else raw
            )
        except ValueError:
            return fallback
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return fallback


def _identity(profile: SourceProfile, input: IdentityInput) -> str:
    payload = _json_object(input.record, profile)
    native_id = _first(payload, profile.identity_fields)
    if native_id is None:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        native_id = hashlib.sha256(canonical.encode()).hexdigest()[:32]
    return (
        f"{profile.source}:{input.external_installation_id}:"
        f"{input.record.native_type}:{native_id}"
    )


class FleetConfiguration:
    def __init__(self, profile: SourceProfile) -> None:
        self._profile = profile

    async def validate_configuration(
        self,
        configuration: dict[str, Any],
        context: OperationContext,
    ) -> ConfigurationValidation:
        issues: list[ConfigurationIssue] = []
        external_id = configuration.get("external_installation_id")
        if not isinstance(external_id, str) or not external_id.strip():
            issues.append(
                ConfigurationIssue(
                    field="external_installation_id",
                    code="required",
                    message="a provider installation identifier is required",
                )
            )
        resources = configuration.get("selected_resources", ())
        if not isinstance(resources, (list, tuple)) or any(
            not isinstance(item, str) or not item for item in resources
        ):
            issues.append(
                ConfigurationIssue(
                    field="selected_resources",
                    code="invalid",
                    message="selected resources must be non-empty strings",
                )
            )
        return ConfigurationValidation(valid=not issues, issues=tuple(issues))


class FleetSecretRotation:
    def __init__(self, profile: SourceProfile) -> None:
        self._profile = profile

    async def verify_candidate(
        self,
        request: SecretRotationRequest,
        context: OperationContext,
    ) -> SecretRotationVerification:
        if request.slot not in self._profile.secret_slots:
            return SecretRotationVerification(
                valid=False,
                reason_code="slot_not_declared",
                message="the candidate targets a slot outside the manifest",
            )
        return SecretRotationVerification(
            valid=True,
            reason_code="candidate_handle_accepted",
            message="the host may atomically promote the validated secret handle",
        )


class FleetIngestion:
    def __init__(self, binding: BindingContext, profile: SourceProfile) -> None:
        self._binding = binding
        self._profile = profile

    async def _headers(self) -> tuple[tuple[str, str], ...]:
        token = await self._binding.services.secrets.resolve(
            SlotId(self._profile.auth_slot)
        )
        return (
            ("authorization", f"{self._profile.auth_scheme} {token.reveal_text()}"),
        )

    async def _request(
        self,
        context: OperationContext,
        *,
        cursor: str | None,
        page_size: int,
    ) -> dict[str, Any] | list[Any]:
        context.services.cancellation.raise_if_cancelled()
        query = [(self._profile.limit_parameter, str(page_size))]
        if cursor:
            query.append((self._profile.cursor_parameter, cursor))
        response = await context.services.http.send(
            GovernedHttpRequest(
                method="GET",
                url=self._profile.api_origin + self._profile.collection_path,
                headers=await self._headers(),
                query=tuple(query),
            )
        )
        if response.status_code == 429:
            raise RateLimitedError(f"{self._profile.source} rate limit was reached")
        if response.status_code in {401, 403}:
            raise AuthenticationRejectedError(
                f"{self._profile.source} rejected the credential"
            )
        if response.status_code == 404:
            raise ResourceNotFoundError(
                f"{self._profile.source} collection was not found"
            )
        if response.status_code >= 500:
            raise TransientSourceError(
                f"{self._profile.source} is temporarily unavailable"
            )
        if response.status_code >= 400:
            raise PayloadRejectedError(
                f"{self._profile.source} rejected the collection request"
            )
        try:
            payload = json.loads(response.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TransientSourceError(
                f"{self._profile.source} returned malformed JSON"
            ) from exc
        if not isinstance(payload, (dict, list)):
            raise TransientSourceError(
                f"{self._profile.source} returned an invalid collection"
            )
        return payload

    def _records(self, payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        for key in self._profile.record_keys:
            value = _nested(payload, key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
            if isinstance(value, dict):
                for nested in value.values():
                    if isinstance(nested, list):
                        return [item for item in nested if isinstance(item, dict)]
        return [payload] if payload else []

    def _next(self, payload: dict[str, Any] | list[Any]) -> str | None:
        if not isinstance(payload, dict):
            return None
        for field in self._profile.next_cursor_fields:
            value = _nested(payload, field)
            if isinstance(value, (str, int)) and str(value):
                return str(value)
        return None

    async def plan(self, request: PlanRequest, context: OperationContext) -> PlanResult:
        selected = request.selected_resources or ("all",)
        return PlanResult(
            shards=tuple(
                ShardPlan(
                    kind=f"{self._profile.source}_collection",
                    identifier={"resource_id": resource},
                    window_start=request.window_start,
                    window_end=request.window_end,
                )
                for resource in selected
            )
        )

    async def fetch(
        self, request: FetchRequest, context: OperationContext
    ) -> FetchedPage:
        return await self._page(
            cursor=request.cursor,
            page_size=request.page_size_hint or 100,
            context=context,
        )

    async def poll(
        self, request: PollRequest, context: OperationContext
    ) -> FetchedPage:
        return await self._page(
            cursor=request.cursor,
            page_size=request.page_size_hint or 100,
            context=context,
        )

    async def _page(
        self,
        *,
        cursor: CursorState | None,
        page_size: int,
        context: OperationContext,
    ) -> FetchedPage:
        current = (
            str(cursor.payload.get("cursor"))
            if cursor and cursor.payload.get("cursor") is not None
            else None
        )
        payload = await self._request(context, cursor=current, page_size=page_size)
        records = tuple(
            SourceRecord(
                native_type=str(item.get("type") or self._profile.native_type),
                payload=item,
                occurred_at=_time(
                    _first(item, self._profile.occurred_fields),
                    context.services.clock.now(),
                ),
            )
            for item in self._records(payload)
        )
        next_value = self._next(payload)
        checkpoint_value = (
            str(_first(records[-1].payload, self._profile.identity_fields))
            if records and isinstance(records[-1].payload, dict)
            else next_value or current or "empty"
        )
        checkpoint = CursorState(
            schema_version=1,
            payload={"cursor": checkpoint_value, "source": self._profile.source},
        )
        return FetchedPage(
            records=records,
            next_cursor=(
                CursorState(schema_version=1, payload={"cursor": next_value})
                if next_value is not None
                else None
            ),
            checkpoint=checkpoint,
            end_of_data=next_value is None,
        )

    async def reconcile(
        self, request: ReconciliationRequest, context: OperationContext
    ) -> ReconciliationDecision:
        repairs = tuple(
            RepairShard(shard=item.shard, parent_shard_id=item.shard_id)
            for item in request.shards
            if item.state not in {"completed", "reconciled", "succeeded"}
        )
        return ReconciliationDecision(
            has_gaps=bool(repairs),
            reason_code="incomplete_shards" if repairs else "clean",
            message=(
                f"{len(repairs)} incomplete shard(s) require replay"
                if repairs
                else "all declared shards reached a terminal checkpoint"
            ),
            new_shards=repairs,
        )


class FleetWebhook:
    def __init__(self, binding: BindingContext, profile: SourceProfile) -> None:
        self._binding = binding
        self._profile = profile

    async def _valid(self, request: BoundedWebhookRequest) -> bool:
        slot = self._profile.webhook_secret_slot
        header = self._profile.webhook_header
        if slot is None or header is None or self._profile.webhook_mode is None:
            return False
        supplied = request.headers.get(header) or request.headers.get(header.title())
        if not supplied:
            return False
        secret = await self._binding.services.secrets.resolve(SlotId(slot))
        key = secret.reveal_bytes()
        if self._profile.webhook_mode == "token":
            return hmac.compare_digest(supplied.encode(), key)
        if self._profile.webhook_mode == "ed25519":
            timestamp = request.headers.get("x-signature-timestamp", "")
            try:
                Ed25519PublicKey.from_public_bytes(bytes.fromhex(key.decode())).verify(
                    bytes.fromhex(supplied), timestamp.encode() + request.body
                )
            except (InvalidSignature, ValueError, TypeError):
                return False
            return True
        expected = hmac.new(key, request.body, hashlib.sha256).hexdigest()
        candidate = supplied.removeprefix("sha256=")
        return hmac.compare_digest(candidate, expected)

    async def verify_and_decode(
        self, request: BoundedWebhookRequest, context: OperationContext
    ) -> VerifiedWebhookResult:
        if not await self._valid(request):
            raise PayloadRejectedError(
                f"{self._profile.source} webhook signature is invalid"
            )
        try:
            payload = json.loads(request.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PayloadRejectedError("webhook body is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise PayloadRejectedError("webhook body must be a JSON object")
        values: list[dict[str, Any]] = []
        for key in ("events", "items", "eventNotifications", "entry"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                values = [item for item in candidate if isinstance(item, dict)]
                break
        if not values:
            nested_event = payload.get("event")
            values = [nested_event if isinstance(nested_event, dict) else payload]
        external_id = _first(
            payload,
            (
                "installation.id",
                "team_id",
                "organization_id",
                "account_id",
                "realmId",
                "site.id",
                "company_id",
                "workspace_id",
            ),
        )
        if external_id is None:
            external_id = str(self._binding.installation.id)
        events = tuple(
            VerifiedWebhookEvent(
                external_installation_id=str(external_id),
                native_event_type=str(
                    _first(item, ("type", "event_type", "webhookEvent", "action"))
                    or self._profile.native_type
                ),
                record=SourceRecord(
                    native_type=str(
                        _first(item, ("type", "event_type", "webhookEvent"))
                        or self._profile.native_type
                    ),
                    payload=item,
                    occurred_at=_time(
                        _first(item, self._profile.occurred_fields),
                        request.received_at,
                    ),
                ),
                signed_at=request.received_at,
                verification_evidence={"algorithm": self._profile.webhook_mode},
            )
            for item in values
        )
        return VerifiedWebhookResult(events=events)


class FleetGateway:
    def __init__(self, binding: BindingContext, profile: SourceProfile) -> None:
        self._binding = binding
        self._profile = profile
        self._ingestion = FleetIngestion(binding, profile)

    async def open(
        self, request: GatewayOpenRequest, context: OperationContext
    ) -> GatewaySession:
        return GatewaySession(
            session_id=f"{self._profile.source}:{self._binding.installation.id}",
            resume_state=request.resume_state,
        )

    async def receive(
        self, request: GatewayReceiveRequest, context: OperationContext
    ) -> GatewayBatch:
        page = await self._ingestion.poll(
            PollRequest(
                cursor=request.session.resume_state,
                page_size_hint=request.max_records,
            ),
            context,
        )
        return GatewayBatch(
            records=page.records,
            resume_state=page.checkpoint,
            session_closed=False,
        )

    async def close(self, session: GatewaySession, context: OperationContext) -> None:
        context.services.cancellation.raise_if_cancelled()


class FleetNormalization:
    def __init__(self, binding: BindingContext, profile: SourceProfile) -> None:
        self._binding = binding
        self._profile = profile

    async def normalize(
        self, input: NormalizationInput, context: OperationContext
    ) -> tuple[ObservationDraft, ...]:
        payload = _json_object(input.record, self._profile)
        fragments = [
            str(value)
            for field in self._profile.text_fields
            if (value := _nested(payload, field)) not in (None, "")
        ]
        content_text = " · ".join(fragments) or (
            f"{self._profile.source} {input.record.native_type}"
        )
        external_id = _identity(
            self._profile,
            IdentityInput(
                record=input.record,
                external_installation_id=str(self._binding.installation.id),
                ingress_kind=input.ingress_kind,
            ),
        )
        occurred_at = input.record.occurred_at or _time(
            _first(payload, self._profile.occurred_fields),
            context.services.clock.now(),
        )
        actor = _first(
            payload,
            ("actor.id", "user.id", "sender.id", "author.id", "email"),
        )
        event_type = str(
            _first(payload, ("type", "event_type", "action", "status")) or ""
        ).lower()
        kind = (
            "state_change"
            if any(word in event_type for word in ("updated", "deleted", "status"))
            else "signal"
        )
        return (
            ObservationDraft(
                source_channel=self._profile.channel,
                content_text=content_text,
                content={
                    "source": self._profile.source,
                    "native_type": input.record.native_type,
                    "payload": payload,
                },
                occurred_at=occurred_at,
                trust_tier=self._profile.trust_tier,
                kind=kind,
                source_actor_ref=(
                    f"{self._profile.source}:{actor}" if actor is not None else None
                ),
                external_id=external_id,
                raw_payload=payload,
            ),
        )


def build_http_connector(
    profile: SourceProfile,
    *,
    oauth_spec: Any | None = None,
) -> NativeSourceConnector:
    from services.ingest.connectors.native import OAuthCleanup, _manifest
    from services.ingest.connectors.standard_oauth import StandardOAuthCapability
    from services.ingest.source_contract.capabilities import (
        OAUTH2_LIFECYCLE_V1,
        OAUTH2_V1,
    )

    factories: dict[Any, Callable[[BindingContext], object]] = {
        CONFIGURATION_V1.ref: lambda _context: FleetConfiguration(profile),
        SECRET_ROTATION_V1.ref: lambda _context: FleetSecretRotation(profile),
        HEALTH_PROBE_V1.ref: lambda context: CredentialHealthProbe(
            context, profile.secret_slots
        ),
        CLEANUP_V1.ref: lambda _context: LocalCredentialCleanup(),
        IDENTITY_V1.ref: lambda _context: NativeIdentity(
            lambda input: _identity(profile, input)
        ),
        NORMALIZATION_V1.ref: lambda context: FleetNormalization(context, profile),
    }
    if "backfill" in profile.ingress_kinds:
        factories[HISTORICAL_PULL_V1.ref] = lambda context: FleetIngestion(
            context, profile
        )
        factories[RECONCILIATION_V1.ref] = lambda context: FleetIngestion(
            context, profile
        )
    if "poll" in profile.ingress_kinds:
        factories[INCREMENTAL_POLL_V1.ref] = lambda context: FleetIngestion(
            context, profile
        )
    if "webhook" in profile.ingress_kinds:
        factories[WEBHOOK_V1.ref] = lambda context: FleetWebhook(context, profile)
    if "gateway" in profile.ingress_kinds:
        factories[GATEWAY_STREAM_V1.ref] = lambda context: FleetGateway(
            context, profile
        )
    if oauth_spec is not None:
        factories[OAUTH2_V1.ref] = lambda context: StandardOAuthCapability(
            context, oauth_spec
        )
        factories[OAUTH2_LIFECYCLE_V1.ref] = (
            lambda context: StandardOAuthCapability(context, oauth_spec)
        )
        factories[CLEANUP_V1.ref] = lambda context: OAuthCleanup(
            StandardOAuthCapability(context, oauth_spec)
        )
    return NativeSourceConnector(_manifest(profile.source), factories)


__all__ = [
    "FleetConfiguration",
    "FleetGateway",
    "FleetIngestion",
    "FleetNormalization",
    "FleetSecretRotation",
    "FleetWebhook",
    "build_http_connector",
]
