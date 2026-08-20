"""Deterministic behavioral release fixtures for the native connector fleet."""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

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
    state_migration_check,
    webhook_verification_check,
)
from services.ingest.connector_conformance.fakes import (
    FakeHostEnvironment,
    make_binding_context,
)
from services.ingest.connector_conformance.models import ConformanceReport
from services.ingest.connectors.aws_source import AWS, build_aws_connector
from services.ingest.connectors.gateway_sources import (
    DISCORD,
    SIGNAL,
    TELEGRAM,
    build_discord_connector,
    build_signal_connector,
    build_telegram_connector,
)
from services.ingest.connectors.google_sources import (
    CALENDAR,
    DRIVE,
    GMAIL,
    build_gmail_connector,
    build_google_calendar_connector,
    build_google_drive_connector,
)
from services.ingest.connectors.native import (
    build_notion_connector,
    build_slack_connector,
    build_whatsapp_connector,
)
from services.ingest.connectors.provider_spec import SourceProfile
from services.ingest.connectors import rest_sources
from services.ingest.source_contract.capabilities import (
    CLEANUP_V1,
    CONFIGURATION_V1,
    GATEWAY_STREAM_V1,
    HISTORICAL_PULL_V1,
    IDENTITY_V1,
    NORMALIZATION_V1,
    RECONCILIATION_V1,
    SECRET_ROTATION_V1,
    WEBHOOK_V1,
)
from services.ingest.source_contract.capabilities.ingestion import (
    GatewayOpenRequest,
    GatewayReceiveRequest,
)
from services.ingest.source_contract.capabilities.installation import (
    SecretRotationRequest,
)
from services.ingest.source_contract.capabilities.lifecycle import CleanupRequest
from services.ingest.source_contract.connector import OperationContext
from services.ingest.source_contract.errors import (
    ConnectorError,
    PayloadRejectedError,
    TransientSourceError,
)
from services.ingest.source_contract.host_services import (
    GovernedHttpResponse,
    SecretValue,
)
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    FetchRequest,
    IdentityInput,
    NormalizationInput,
    ReconciliationRequest,
    ShardPlan,
    SourceRecord,
    VersionedState,
)
from services.ingest.source_contract.state_migrations import (
    DowngradePolicy,
    StateMigration,
    StateMigrationRegistry,
    assert_mixed_worker_compatibility,
)

_INVOCATION_ID = UUID("f71f5d95-e410-482d-a267-2d36abc87736")
_NOW = datetime(2025, 1, 1, tzinfo=UTC)

_SOURCE_FIXTURES = {
    profile.source: (profile, factory)
    for profile, factory in (
        (rest_sources.GITHUB, rest_sources.build_github_connector),
        (rest_sources.JIRA, rest_sources.build_jira_connector),
        (rest_sources.MERCURY, rest_sources.build_mercury_connector),
        (rest_sources.QUICKBOOKS, rest_sources.build_quickbooks_connector),
        (rest_sources.GRAFANA, rest_sources.build_grafana_connector),
        (rest_sources.BREX, rest_sources.build_brex_connector),
        (rest_sources.RAMP, rest_sources.build_ramp_connector),
        (rest_sources.GUSTO, rest_sources.build_gusto_connector),
        (rest_sources.DEEL, rest_sources.build_deel_connector),
        (rest_sources.FIREFLIES, rest_sources.build_fireflies_connector),
        (rest_sources.MIRO, rest_sources.build_miro_connector),
        (rest_sources.FIGMA, rest_sources.build_figma_connector),
        (rest_sources.CARTA, rest_sources.build_carta_connector),
        (rest_sources.HIBOB, rest_sources.build_hibob_connector),
        (rest_sources.ASHBY, rest_sources.build_ashby_connector),
        (rest_sources.LINKEDIN, rest_sources.build_linkedin_connector),
        (GMAIL, build_gmail_connector),
        (CALENDAR, build_google_calendar_connector),
        (DRIVE, build_google_drive_connector),
        (DISCORD, build_discord_connector),
        (TELEGRAM, build_telegram_connector),
        (SIGNAL, build_signal_connector),
        (AWS, build_aws_connector),
    )
}


def _factory_for(source: str):
    return _SOURCE_FIXTURES[source][1]


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
        except ConnectorError:
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
        except ConnectorError:
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


async def _fleet_pages(profile: SourceProfile):
    environment = FakeHostEnvironment()
    environment.secrets.values[profile.auth_slot] = SecretValue.from_text("token")
    if profile.source == "aws":
        environment.secrets.values["aws_secret_access_key"] = (
            SecretValue.from_text("fixture-secret")
        )

    def page(record_id: str, *, continued: bool) -> dict[str, object]:
        record = {
            "id": record_id,
            "EventId": record_id,
            "type": profile.native_type,
            "updated_at": "2025-01-01T00:00:00Z",
            "title": record_id,
        }
        identity_path = profile.identity_fields[0]
        identity_target: dict[str, object] = record
        identity_parts = identity_path.split(".")
        for part in identity_parts[:-1]:
            nested = identity_target.get(part)
            if not isinstance(nested, dict):
                nested = {}
                identity_target[part] = nested
            identity_target = nested
        identity_target[identity_parts[-1]] = record_id
        if profile.source == "aws":
            value: dict[str, object] = {"Events": [record]}
            if continued:
                value["NextToken"] = "page-2"
            return value
        value = {profile.record_keys[0]: [record]}
        if continued:
            next_field = (
                "syncToken"
                if profile.source == "ashby"
                else profile.next_cursor_fields[0]
            )
            target: dict[str, object] = value
            parts = next_field.split(".")
            for part in parts[:-1]:
                nested: dict[str, object] = {}
                target[part] = nested
                target = nested
            target[parts[-1]] = "page-2"
            if profile.source == "ashby":
                value["moreDataAvailable"] = True
        return value

    environment.http.responses.extend(
        (
            GovernedHttpResponse(
                status_code=200,
                headers=(),
                body=json.dumps(page("one", continued=True)).encode(),
            ),
            GovernedHttpResponse(
                status_code=200,
                headers=(),
                body=json.dumps(page("two", continued=False)).encode(),
            ),
        )
    )
    factory = _factory_for(profile.source)
    capability = _bound(factory, environment).require(HISTORICAL_PULL_V1)
    operation = _operation(environment)
    shard = ShardPlan(
        kind=f"{profile.source}_collection", identifier={"resource_id": "all"}
    )
    first = await capability.fetch(FetchRequest(shard=shard), operation)
    if first.next_cursor is None or first.checkpoint is None or first.end_of_data:
        raise AssertionError("first native page did not expose continuation state")
    second = await capability.fetch(
        FetchRequest(shard=shard, cursor=first.next_cursor), operation
    )
    if not second.end_of_data or second.next_cursor is not None:
        raise AssertionError("terminal native page retained a continuation cursor")
    if second.checkpoint is None:
        raise AssertionError("terminal native page lost its durable checkpoint")
    return first, second


def _fleet_pagination_check(profile: SourceProfile):
    async def check() -> None:
        first, second = await _fleet_pages(profile)
        await assert_pagination(
            (
                PageEvidence(first.next_cursor.payload, ("one",)),
                PageEvidence(None, ("two",), end_of_data=second.end_of_data),
            )
        )

    return check


def _fleet_cursor_check(profile: SourceProfile):
    async def check() -> None:
        first, second = await _fleet_pages(profile)
        await assert_cursor_monotonicity(
            (
                PageEvidence(first.next_cursor.payload, ("one",)),  # type: ignore[union-attr]
                PageEvidence(None, ("two",), end_of_data=second.end_of_data),
            )
        )

    return check


def _fleet_checkpoint_check(profile: SourceProfile):
    async def check() -> None:
        first, second = await _fleet_pages(profile)
        if first.checkpoint == second.checkpoint:
            raise AssertionError("terminal checkpoint did not advance")
        if second.checkpoint is None or second.checkpoint.schema_version != 1:
            raise AssertionError("checkpoint is not replayable by a v1 worker")

    return check


def _fleet_configuration_check(profile: SourceProfile):
    async def check() -> None:
        environment = FakeHostEnvironment()
        capability = _bound(
            _factory_for(profile.source), environment
        ).require(CONFIGURATION_V1)
        valid = await capability.validate_configuration(
            {
                "external_installation_id": "fixture-installation",
                "selected_resources": ["all"],
            },
            _operation(environment),
        )
        invalid = await capability.validate_configuration(
            {"selected_resources": "all"}, _operation(environment)
        )
        if not valid.valid or invalid.valid:
            raise AssertionError("configuration validation accepted the wrong shape")

    return check


def _fleet_rotation_check(profile: SourceProfile):
    async def check() -> None:
        environment = FakeHostEnvironment()
        capability = _bound(
            _factory_for(profile.source), environment
        ).require(SECRET_ROTATION_V1)
        accepted = await capability.verify_candidate(
            SecretRotationRequest(
                slot=profile.auth_slot, candidate_handle="fixture-candidate"
            ),
            _operation(environment),
        )
        rejected = await capability.verify_candidate(
            SecretRotationRequest(
                slot="undeclared_slot", candidate_handle="fixture-candidate"
            ),
            _operation(environment),
        )
        if not accepted.valid or rejected.valid:
            raise AssertionError("secret rotation escaped manifest slot authority")

    return check


def _fleet_webhook_check(profile: SourceProfile):
    body = json.dumps(
        {
            "type": "fixture.updated",
            "id": "event-1",
            "installation": {"id": "fixture-installation"},
            "updated_at": "2025-01-01T00:00:00Z",
            "title": "fixture",
        },
        separators=(",", ":"),
    ).encode()
    timestamp = str(int(_NOW.timestamp()))
    private_key = Ed25519PrivateKey.from_private_bytes(b"\x01" * 32)
    public_hex = (
        private_key.public_key()
        .public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        .hex()
    )
    secret = b"fleet-webhook-secret"
    if profile.webhook_mode == "token":
        valid_signature = secret.decode()
    elif profile.webhook_mode == "ed25519":
        valid_signature = private_key.sign(timestamp.encode() + body).hex()
        secret = public_hex.encode()
    else:
        valid_signature = hmac.new(secret, body, hashlib.sha256).hexdigest()

    async def verify(candidate: str) -> bool:
        environment = FakeHostEnvironment()
        environment.secrets.values[profile.webhook_secret_slot] = SecretValue(secret)  # type: ignore[index]
        capability = _bound(
            _factory_for(profile.source), environment
        ).require(WEBHOOK_V1)
        headers = {profile.webhook_header: candidate}  # type: ignore[dict-item]
        if profile.webhook_mode == "ed25519":
            headers["x-signature-timestamp"] = timestamp
        try:
            result = await capability.verify_and_decode(
                BoundedWebhookRequest(
                    body=body,
                    headers=headers,
                    received_at=_NOW,
                ),
                _operation(environment),
            )
        except ConnectorError:
            return False
        return len(result.events) == 1

    async def valid() -> bool:
        return await verify(valid_signature)

    async def invalid() -> bool:
        return await verify("0" * len(valid_signature))

    return webhook_verification_check(valid, invalid)


def _fleet_gateway_check(profile: SourceProfile):
    async def check() -> None:
        environment = FakeHostEnvironment()
        environment.secrets.values[profile.auth_slot] = SecretValue.from_text("token")
        if profile.source == "telegram":
            environment.http.responses.append(
                GovernedHttpResponse(
                    status_code=200,
                    headers=(),
                    body=json.dumps(
                        {
                            "ok": True,
                            "result": [{"update_id": 1, "message": {"text": "hello"}}],
                        }
                    ).encode(),
                )
            )
        elif profile.source == "discord":
            environment.gateway.planned_connections.append(
                [
                    {"op": 10, "d": {"heartbeat_interval": 45_000}},
                    {
                        "op": 0,
                        "s": 1,
                        "t": "READY",
                        "d": {
                            "session_id": "fixture-session",
                            "resume_gateway_url": "wss://gateway.discord.gg",
                        },
                    },
                    {"op": 0, "s": 2, "t": "MESSAGE_CREATE", "d": {"id": "event-1", "content": "hello"}},
                    {"op": 7},
                ]
            )
        else:
            environment.gateway.planned_connections.append(
                [{"type": "message", "id": "event-1", "text": "hello"}]
            )
        capability = _bound(
            _factory_for(profile.source), environment
        ).require(GATEWAY_STREAM_V1)
        session = await capability.open(GatewayOpenRequest(), _operation(environment))
        batch = await capability.receive(
            GatewayReceiveRequest(session=session, max_records=10),
            _operation(environment),
        )
        if len(batch.records) != 1 or batch.resume_state is None:
            raise AssertionError("gateway session did not produce resumable state")
        await capability.close(session, _operation(environment))

    return check


def _fleet_state_migration_check():
    async def migrate():
        registry = StateMigrationRegistry(
            (
                StateMigration(
                    kind="connector.cursor",
                    from_schema=1,
                    to_schema=2,
                    upgrade=lambda payload: {**payload, "checkpoint": "durable"},
                    downgrade=lambda payload: {
                        key: value
                        for key, value in payload.items()
                        if key != "checkpoint"
                    },
                ),
            )
        )
        state = VersionedState(
            kind="connector.cursor",
            schema_version=1,
            producing_connector_version="1.0.0",
            revision=1,
            payload={"cursor": "one"},
        )
        first = registry.migrate(
            state, target_schema=2, producing_connector_version="1.1.0"
        )
        second = registry.migrate(
            state, target_schema=2, producing_connector_version="1.1.0"
        )
        return 1, 2, first.payload, second.payload

    return state_migration_check(migrate)


def _fleet_mixed_worker_check():
    async def check() -> None:
        state = VersionedState(
            kind="connector.cursor",
            schema_version=2,
            producing_connector_version="1.1.0",
            revision=2,
            payload={"cursor": "two"},
        )
        assert_mixed_worker_compatibility(
            state,
            worker_connector_version="1.0.3",
            accepted_state_schemas=frozenset({1, 2}),
        )
        try:
            assert_mixed_worker_compatibility(
                state,
                worker_connector_version="2.0.0",
                accepted_state_schemas=frozenset({2}),
            )
        except ValueError:
            return
        raise AssertionError("mixed workers crossed a connector major version")

    return check


def _fleet_downgrade_check():
    async def check() -> None:
        registry = StateMigrationRegistry(
            (
                StateMigration(
                    kind="connector.cursor",
                    from_schema=1,
                    to_schema=2,
                    upgrade=lambda payload: {**payload, "checkpoint": "durable"},
                    downgrade=lambda payload: {
                        key: value
                        for key, value in payload.items()
                        if key != "checkpoint"
                    },
                ),
            )
        )
        state = VersionedState(
            kind="connector.cursor",
            schema_version=2,
            producing_connector_version="1.1.0",
            revision=2,
            payload={"cursor": "one", "checkpoint": "durable"},
        )
        downgraded = registry.migrate(
            state,
            target_schema=1,
            producing_connector_version="1.0.0",
            downgrade_policy=DowngradePolicy.REQUIRE_REVERSIBLE,
        )
        if downgraded.payload != {"cursor": "one"}:
            raise AssertionError("reversible downgrade produced the wrong state")
        try:
            registry.migrate(
                state,
                target_schema=1,
                producing_connector_version="1.0.0",
            )
        except ValueError:
            return
        raise AssertionError("forbidden state downgrade was accepted")

    return check


def fleet_behavioral_fixtures() -> dict[str, tuple[str, BehavioralFixture]]:
    fixtures = dict(pilot_behavioral_fixtures())
    for source, (profile, factory) in _SOURCE_FIXTURES.items():
        ingress = profile.ingress_kinds[0]
        record = SourceRecord(
            native_type=profile.native_type,
            payload={
                "id": "fixture-record",
                "type": profile.native_type,
                "updated_at": "2025-01-01T00:00:00Z",
                "title": f"{source} fixture",
            },
        )
        checks = {
            "configuration": _fleet_configuration_check(profile),
            "secret_rotation": _fleet_rotation_check(profile),
            "identity_stability": _identity_check(factory, record, ingress),
            "retry_classification": _retry_check(),
            "normalization": _normalization_check(factory, record, ingress),
            "cleanup": _cleanup_check(factory),
            "lifecycle": _lifecycle_check(),
            "state_migration": _fleet_state_migration_check(),
            "mixed_worker": _fleet_mixed_worker_check(),
            "downgrade_policy": _fleet_downgrade_check(),
        }
        required = list(checks)
        if "backfill" in profile.ingress_kinds:
            checks.update(
                {
                    "pagination": _fleet_pagination_check(profile),
                    "cursor_monotonicity": _fleet_cursor_check(profile),
                    "checkpoint_replay": _fleet_checkpoint_check(profile),
                    "reconciliation": _reconciliation_check(factory),
                }
            )
            required.extend(
                (
                    "pagination",
                    "cursor_monotonicity",
                    "checkpoint_replay",
                    "reconciliation",
                )
            )
        if "webhook" in profile.ingress_kinds:
            checks["webhook_verification"] = _fleet_webhook_check(profile)
            required.append("webhook_verification")
        if "gateway" in profile.ingress_kinds:
            checks["gateway_resume"] = _fleet_gateway_check(profile)
            required.append("gateway_resume")
        fixtures[f"fyralis/{source}"] = (
            "1.0.0",
            BehavioralFixture(checks, required_behaviors=tuple(required)),
        )
    return fixtures


async def run_fleet_behavioral_conformance() -> dict[str, ConformanceReport]:
    reports: dict[str, ConformanceReport] = {}
    for connector_id, (version, fixture) in fleet_behavioral_fixtures().items():
        reports[connector_id] = await BehavioralConformanceSuite().run(
            connector_id=connector_id,
            connector_version=version,
            fixture=fixture,
        )
    return reports


run_pilot_behavioral_conformance = run_fleet_behavioral_conformance


__all__ = [
    "fleet_behavioral_fixtures",
    "pilot_behavioral_fixtures",
    "run_fleet_behavioral_conformance",
    "run_pilot_behavioral_conformance",
]
