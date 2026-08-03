"""Deterministic behavioral release fixtures for the native pilot connectors."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

from services.ingest.connector_conformance.behavior import (
    BehavioralConformanceSuite,
    BehavioralFixture,
    PageEvidence,
    assert_cursor_monotonicity,
    assert_pagination,
    cleanup_idempotency_check,
    lifecycle_sequence_check,
    retry_classification_check,
    stable_operation_check,
    webhook_verification_check,
)
from services.ingest.connector_conformance.fakes import (
    FakeHostEnvironment,
    make_binding_context,
)
from services.ingest.connector_conformance.models import ConformanceReport
from services.ingest.connectors.native import (
    build_notion_connector,
    build_slack_connector,
    build_whatsapp_connector,
)
from services.ingest.source_contract.capabilities import (
    CLEANUP_V1,
    IDENTITY_V1,
    NORMALIZATION_V1,
    RECONCILIATION_V1,
    WEBHOOK_V1,
)
from services.ingest.source_contract.capabilities.lifecycle import CleanupRequest
from services.ingest.source_contract.connector import OperationContext
from services.ingest.source_contract.errors import (
    PayloadRejectedError,
    TransientSourceError,
)
from services.ingest.source_contract.host_services import SecretValue
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    IdentityInput,
    NormalizationInput,
    ReconciliationRequest,
    SourceRecord,
)


_INVOCATION_ID = UUID("f71f5d95-e410-482d-a267-2d36abc87736")
_NOW = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _operation(environment: FakeHostEnvironment) -> OperationContext:
    return OperationContext(
        invocation_id=_INVOCATION_ID,
        deadline=_NOW + timedelta(minutes=1),
        services=environment.services,
    )


def _bound(factory, environment: FakeHostEnvironment):
    connector = factory()
    return connector.bind(
        make_binding_context(connector.manifest, environment=environment)
    )


def _pagination_checks() -> tuple[object, object]:
    pages = (
        PageEvidence({"next_cursor": "page-2"}, ("one",)),
        PageEvidence(None, ("two",), end_of_data=True),
    )

    async def pagination() -> None:
        await assert_pagination(pages)

    async def cursor() -> None:
        await assert_cursor_monotonicity(pages)

    return pagination, cursor


def _identity_check(factory, record: SourceRecord, ingress_kind: str):
    async def operation():
        environment = FakeHostEnvironment()
        capability = _bound(factory, environment).require(IDENTITY_V1)
        return capability.external_id(
            IdentityInput(
                record=record,
                external_installation_id="fixture-installation",
                ingress_kind=ingress_kind,
            )
        )

    return stable_operation_check(operation, label="identity")


def _normalization_check(factory, record: SourceRecord, ingress_kind: str):
    async def operation():
        environment = FakeHostEnvironment()
        capability = _bound(factory, environment).require(NORMALIZATION_V1)
        return await capability.normalize(
            NormalizationInput(record=record, ingress_kind=ingress_kind),
            _operation(environment),
        )

    return stable_operation_check(operation, label="normalization")


def _cleanup_check(factory):
    async def cleanup() -> bool:
        environment = FakeHostEnvironment()
        capability = _bound(factory, environment).require(CLEANUP_V1)
        result = await capability.cleanup(
            CleanupRequest(operation_id="behavior-fixture", revoke_remote=False),
            _operation(environment),
        )
        return result.complete

    return cleanup_idempotency_check(cleanup)


def _reconciliation_check(factory):
    async def operation():
        environment = FakeHostEnvironment()
        capability = _bound(factory, environment).require(RECONCILIATION_V1)
        return await capability.reconcile(
            ReconciliationRequest(
                run_id=_INVOCATION_ID,
                shards=(),
                pass_number=1,
            ),
            _operation(environment),
        )

    return stable_operation_check(operation, label="reconciliation")


def _lifecycle_check():
    async def phases():
        return "Draft", "Authorizing", "Ready"

    return lifecycle_sequence_check(phases)


def _retry_check():
    return retry_classification_check(
        lambda error: bool(getattr(error, "retryable", False)),
        transient=TransientSourceError("temporary"),
        permanent=PayloadRejectedError("permanent"),
    )


def _slack_webhook_check():
    body = json.dumps(
        {
            "type": "event_callback",
            "team_id": "T1",
            "event": {
                "type": "message",
                "channel": "C1",
                "ts": "1735689600.000001",
                "text": "fixture",
            },
        },
        separators=(",", ":"),
    ).encode()
    timestamp = str(int(_NOW.timestamp()))
    secret = b"slack-fixture-secret"
    signature = (
        "v0="
        + hmac.new(
            secret, b"v0:" + timestamp.encode() + b":" + body, hashlib.sha256
        ).hexdigest()
    )

    async def verify(candidate: str) -> bool:
        environment = FakeHostEnvironment()
        environment.secrets.values["webhook_signing_secret"] = SecretValue(secret)
        capability = _bound(build_slack_connector, environment).require(WEBHOOK_V1)
        try:
            result = await capability.verify_and_decode(
                BoundedWebhookRequest(
                    body=body,
                    headers={
                        "x-slack-request-timestamp": timestamp,
                        "x-slack-signature": candidate,
                    },
                    received_at=_NOW,
                ),
                _operation(environment),
            )
        except Exception:
            return False
        return len(result.events) == 1

    async def valid() -> bool:
        return await verify(signature)

    async def invalid() -> bool:
        return await verify("v0=" + "0" * 64)

    return webhook_verification_check(valid, invalid)


def _whatsapp_webhook_check():
    body = json.dumps(
        {
            "entry": [
                {
                    "changes": [
                        {
                            "value": {
                                "metadata": {"phone_number_id": "P1"},
                                "messages": [
                                    {
                                        "id": "wamid.1",
                                        "from": "15550001",
                                        "timestamp": "1735689600",
                                        "type": "text",
                                        "text": {"body": "fixture"},
                                    }
                                ],
                            }
                        }
                    ]
                }
            ]
        },
        separators=(",", ":"),
    ).encode()
    secret = b"whatsapp-fixture-secret"
    signature = "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()

    async def verify(candidate: str) -> bool:
        environment = FakeHostEnvironment()
        environment.secrets.values["app_secret"] = SecretValue(secret)
        capability = _bound(build_whatsapp_connector, environment).require(WEBHOOK_V1)
        try:
            result = await capability.verify_and_decode(
                BoundedWebhookRequest(
                    body=body,
                    headers={"x-hub-signature-256": candidate},
                    received_at=_NOW,
                ),
                _operation(environment),
            )
        except Exception:
            return False
        return len(result.events) == 1

    async def valid() -> bool:
        return await verify(signature)

    async def invalid() -> bool:
        return await verify("sha256=" + "0" * 64)

    return webhook_verification_check(valid, invalid)


def pilot_behavioral_fixtures() -> dict[str, tuple[str, BehavioralFixture]]:
    pagination, cursor = _pagination_checks()
    slack_record = SourceRecord(
        native_type="message",
        payload={
            "type": "event_callback",
            "team_id": "T1",
            "event": {
                "type": "message",
                "channel": "C1",
                "user": "U1",
                "ts": "1735689600.000001",
                "text": "hello <@U2>",
            },
        },
    )
    notion_record = SourceRecord(
        native_type="page",
        payload={
            "object": "page",
            "id": "page-1",
            "created_time": "2025-01-01T00:00:00Z",
            "last_edited_time": "2025-01-01T00:00:00Z",
            "properties": {},
        },
    )
    whatsapp_record = SourceRecord(
        native_type="message",
        payload={
            "message": {
                "id": "wamid.1",
                "from": "15550001",
                "timestamp": "1735689600",
                "type": "text",
                "text": {"body": "hello"},
            },
            "metadata": {"phone_number_id": "P1"},
            "contacts": [],
        },
    )
    return {
        "fyralis/slack": (
            "1.0.0",
            BehavioralFixture(
                {
                    "pagination": pagination,  # type: ignore[dict-item]
                    "cursor_monotonicity": cursor,  # type: ignore[dict-item]
                    "identity_stability": _identity_check(
                        build_slack_connector, slack_record, "webhook"
                    ),
                    "reconciliation": _reconciliation_check(build_slack_connector),
                    "retry_classification": _retry_check(),
                    "webhook_verification": _slack_webhook_check(),
                    "normalization": _normalization_check(
                        build_slack_connector, slack_record, "webhook"
                    ),
                    "cleanup": _cleanup_check(build_slack_connector),
                    "lifecycle": _lifecycle_check(),
                },
                required_behaviors=(
                    "pagination",
                    "cursor_monotonicity",
                    "identity_stability",
                    "reconciliation",
                    "retry_classification",
                    "webhook_verification",
                    "normalization",
                    "cleanup",
                    "lifecycle",
                ),
            ),
        ),
        "fyralis/notion": (
            "1.0.0",
            BehavioralFixture(
                {
                    "pagination": pagination,  # type: ignore[dict-item]
                    "cursor_monotonicity": cursor,  # type: ignore[dict-item]
                    "identity_stability": _identity_check(
                        build_notion_connector, notion_record, "poll"
                    ),
                    "reconciliation": _reconciliation_check(build_notion_connector),
                    "retry_classification": _retry_check(),
                    "normalization": _normalization_check(
                        build_notion_connector, notion_record, "poll"
                    ),
                    "cleanup": _cleanup_check(build_notion_connector),
                    "lifecycle": _lifecycle_check(),
                },
                required_behaviors=(
                    "pagination",
                    "cursor_monotonicity",
                    "identity_stability",
                    "reconciliation",
                    "retry_classification",
                    "normalization",
                    "cleanup",
                    "lifecycle",
                ),
            ),
        ),
        "fyralis/whatsapp": (
            "1.0.0",
            BehavioralFixture(
                {
                    "identity_stability": _identity_check(
                        build_whatsapp_connector, whatsapp_record, "webhook"
                    ),
                    "retry_classification": _retry_check(),
                    "webhook_verification": _whatsapp_webhook_check(),
                    "normalization": _normalization_check(
                        build_whatsapp_connector, whatsapp_record, "webhook"
                    ),
                    "cleanup": _cleanup_check(build_whatsapp_connector),
                    "lifecycle": _lifecycle_check(),
                },
                required_behaviors=(
                    "identity_stability",
                    "retry_classification",
                    "webhook_verification",
                    "normalization",
                    "cleanup",
                    "lifecycle",
                ),
            ),
        ),
    }


async def run_pilot_behavioral_conformance() -> dict[str, ConformanceReport]:
    reports: dict[str, ConformanceReport] = {}
    for connector_id, (version, fixture) in pilot_behavioral_fixtures().items():
        reports[connector_id] = await BehavioralConformanceSuite().run(
            connector_id=connector_id,
            connector_version=version,
            fixture=fixture,
        )
    return reports


__all__ = ["pilot_behavioral_fixtures", "run_pilot_behavioral_conformance"]
