"""AWS CloudTrail connector with connector-owned Signature Version 4."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime
from typing import Any

from services.ingest.connectors.fleet import (
    FleetConfiguration,
    FleetNormalization,
    FleetSecretRotation,
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
    HEALTH_PROBE_V1,
    HISTORICAL_PULL_V1,
    IDENTITY_V1,
    INCREMENTAL_POLL_V1,
    NORMALIZATION_V1,
    RECONCILIATION_V1,
    SECRET_ROTATION_V1,
)
from services.ingest.source_contract.connector import BindingContext, OperationContext
from services.ingest.source_contract.errors import (
    AuthenticationRejectedError,
    PayloadRejectedError,
    RateLimitedError,
    TransientSourceError,
)
from services.ingest.source_contract.host_services import GovernedHttpRequest
from services.ingest.source_contract.identity import SlotId
from services.ingest.source_contract.models import (
    CursorState,
    FetchRequest,
    FetchedPage,
    PlanRequest,
    PlanResult,
    PollRequest,
    ReconciliationDecision,
    ReconciliationRequest,
    RepairShard,
    ShardPlan,
    SourceRecord,
)


AWS = SourceProfile(
    source="aws", ingress_kinds=("backfill", "poll"),
    api_origin="https://cloudtrail.amazonaws.com", collection_path="/",
    channel="aws:event", native_type="event", record_keys=("Events",),
    identity_fields=("EventId", "id"), occurred_fields=("EventTime", "eventTime"),
    text_fields=("EventName", "Username", "CloudTrailEvent"),
    auth_slot="aws_access_key_id", auth_scheme="AWS4-HMAC-SHA256",
    trust_tier="authoritative", cursor_parameter="NextToken", limit_parameter="MaxResults",
    next_cursor_fields=("NextToken",),
)


def _hmac(key: bytes, value: str) -> bytes:
    return hmac.new(key, value.encode(), hashlib.sha256).digest()


def _signature_key(secret: bytes, date: str, region: str) -> bytes:
    date_key = _hmac(b"AWS4" + secret, date)
    region_key = _hmac(date_key, region)
    service_key = _hmac(region_key, "cloudtrail")
    return _hmac(service_key, "aws4_request")


class AwsCloudTrailIngestion:
    def __init__(self, binding: BindingContext) -> None:
        self._binding = binding

    async def plan(self, request: PlanRequest, context: OperationContext) -> PlanResult:
        data = await context.services.installation_store.read("aws")
        configured = data.values.get("regions", ()) if data is not None else ()
        regions = request.selected_resources or tuple(str(item) for item in configured) or ("us-east-1",)
        return PlanResult(
            shards=tuple(
                ShardPlan(kind="aws_cloudtrail_region", identifier={"region": region}, window_start=request.window_start, window_end=request.window_end)
                for region in regions
            )
        )

    async def fetch(self, request: FetchRequest, context: OperationContext) -> FetchedPage:
        return await self._lookup(
            context,
            region=str(request.shard.identifier.get("region") or "us-east-1"),
            cursor=request.cursor,
            page_size=request.page_size_hint or 50,
            window_start=request.shard.window_start,
            window_end=request.shard.window_end,
        )

    async def poll(self, request: PollRequest, context: OperationContext) -> FetchedPage:
        data = await context.services.installation_store.read("aws")
        region = str(data.values.get("region", "us-east-1")) if data is not None else "us-east-1"
        return await self._lookup(context, region=region, cursor=request.cursor, page_size=request.page_size_hint or 50)

    async def _lookup(
        self,
        context: OperationContext,
        *,
        region: str,
        cursor: CursorState | None,
        page_size: int,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
    ) -> FetchedPage:
        context.services.cancellation.raise_if_cancelled()
        body: dict[str, Any] = {"MaxResults": min(page_size, 50)}
        if cursor is not None and cursor.payload.get("next_token"):
            body["NextToken"] = str(cursor.payload["next_token"])
        if window_start is not None:
            body["StartTime"] = window_start.astimezone(UTC).timestamp()
        if window_end is not None:
            body["EndTime"] = window_end.astimezone(UTC).timestamp()
        raw_body = json.dumps(body, separators=(",", ":"), sort_keys=True).encode()
        now = context.services.clock.now().astimezone(UTC)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        short_date = now.strftime("%Y%m%d")
        host = f"cloudtrail.{region}.amazonaws.com"
        target = "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents"
        content_type = "application/x-amz-json-1.1"
        access = await self._binding.services.secrets.resolve(SlotId("aws_access_key_id"))
        secret = await self._binding.services.secrets.resolve(SlotId("aws_secret_access_key"))
        try:
            session = await self._binding.services.secrets.resolve(SlotId("session_token"))
        except Exception:
            session = None
        canonical_headers = f"content-type:{content_type}\nhost:{host}\nx-amz-date:{amz_date}\nx-amz-target:{target}\n"
        signed_headers = "content-type;host;x-amz-date;x-amz-target"
        headers: list[tuple[str, str]] = []
        if session is not None:
            canonical_headers += f"x-amz-security-token:{session.reveal_text()}\n"
            signed_headers += ";x-amz-security-token"
            headers.append(("x-amz-security-token", session.reveal_text()))
        body_hash = hashlib.sha256(raw_body).hexdigest()
        canonical_request = f"POST\n/\n\n{canonical_headers}\n{signed_headers}\n{body_hash}"
        scope = f"{short_date}/{region}/cloudtrail/aws4_request"
        string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"
        signature = hmac.new(_signature_key(secret.reveal_bytes(), short_date, region), string_to_sign.encode(), hashlib.sha256).hexdigest()
        authorization = f"AWS4-HMAC-SHA256 Credential={access.reveal_text()}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
        headers.extend((("authorization", authorization), ("content-type", content_type), ("x-amz-date", amz_date), ("x-amz-target", target)))
        response = await context.services.http.send(
            GovernedHttpRequest(method="POST", url=f"https://{host}/", headers=tuple(headers), body=raw_body)
        )
        if response.status_code == 429:
            raise RateLimitedError("AWS CloudTrail throttled LookupEvents")
        if response.status_code in {401, 403}:
            raise AuthenticationRejectedError("AWS rejected the SigV4 credential")
        if response.status_code >= 500:
            raise TransientSourceError("AWS CloudTrail is temporarily unavailable")
        try:
            payload = json.loads(response.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TransientSourceError("AWS CloudTrail returned malformed JSON") from exc
        if response.status_code >= 400 or not isinstance(payload, dict):
            raise PayloadRejectedError("AWS CloudTrail rejected LookupEvents")
        values = payload.get("Events") if isinstance(payload.get("Events"), list) else []
        records = tuple(SourceRecord(native_type="event", payload=item) for item in values if isinstance(item, dict))
        next_token = payload.get("NextToken")
        previous_checkpoint = (
            str(cursor.payload.get("checkpoint") or "empty")
            if cursor is not None
            else "empty"
        )
        checkpoint = str(
            (values[-1] if values else {}).get("EventId") or previous_checkpoint
        )
        return FetchedPage(
            records=records,
            next_cursor=CursorState(schema_version=1, payload={"next_token": str(next_token), "checkpoint": checkpoint}) if next_token else None,
            checkpoint=CursorState(schema_version=1, payload={"checkpoint": checkpoint, "region": region}),
            end_of_data=not bool(next_token),
        )

    async def reconcile(self, request: ReconciliationRequest, context: OperationContext) -> ReconciliationDecision:
        repairs = tuple(RepairShard(shard=item.shard, parent_shard_id=item.shard_id) for item in request.shards if item.state not in {"completed", "reconciled", "succeeded"})
        return ReconciliationDecision(has_gaps=bool(repairs), reason_code="incomplete_shards" if repairs else "clean", new_shards=repairs)


def build_aws_connector() -> NativeSourceConnector:
    return NativeSourceConnector(
        _manifest("aws"),
        {
            CONFIGURATION_V1.ref: lambda _context: FleetConfiguration(AWS),
            SECRET_ROTATION_V1.ref: lambda _context: FleetSecretRotation(AWS),
            HEALTH_PROBE_V1.ref: lambda context: CredentialHealthProbe(context, ("aws_access_key_id", "aws_secret_access_key")),
            CLEANUP_V1.ref: lambda _context: LocalCredentialCleanup(),
            HISTORICAL_PULL_V1.ref: AwsCloudTrailIngestion,
            INCREMENTAL_POLL_V1.ref: AwsCloudTrailIngestion,
            RECONCILIATION_V1.ref: AwsCloudTrailIngestion,
            IDENTITY_V1.ref: lambda _context: NativeIdentity(lambda value: _identity(AWS, value)),
            NORMALIZATION_V1.ref: lambda context: FleetNormalization(context, AWS),
        },
    )


__all__ = ["build_aws_connector"]
