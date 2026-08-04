from __future__ import annotations

import json
import hashlib
import hmac
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from services.ingest.connector_conformance.fakes import (
    FakeHostEnvironment,
    make_binding_context,
)
from services.ingest.connectors.native import (
    build_notion_connector,
    build_slack_connector,
    build_whatsapp_connector,
)
from services.ingest.source_contract.capabilities import (
    HISTORICAL_PULL_V1,
    IDENTITY_V1,
    INCREMENTAL_POLL_V1,
    NORMALIZATION_V1,
    RECONCILIATION_V1,
    WEBHOOK_V1,
)
from services.ingest.source_contract.connector import OperationContext
from services.ingest.source_contract.host_services import (
    GovernedHttpResponse,
    InstallationData,
    SecretValue,
)
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    FetchRequest,
    IdentityInput,
    NormalizationInput,
    PlanRequest,
    PollRequest,
    ReconciliationRequest,
    ShardPlan,
    ShardSummary,
    SourceRecord,
)


_NOW = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _operation(environment: FakeHostEnvironment) -> OperationContext:
    return OperationContext(
        invocation_id=uuid4(),
        deadline=_NOW + timedelta(minutes=1),
        services=environment.services,
    )


def _bind(factory, environment: FakeHostEnvironment):
    connector = factory()
    return connector.bind(
        make_binding_context(connector.manifest, environment=environment)
    )


def _response(payload: dict) -> GovernedHttpResponse:
    return GovernedHttpResponse(
        status_code=200,
        headers=(),
        body=json.dumps(payload).encode(),
    )


@pytest.mark.asyncio
async def test_slack_native_pull_normalization_and_reconciliation_need_no_ambient_binding() -> (
    None
):
    environment = FakeHostEnvironment()
    environment.secrets.values["oauth_access_token"] = SecretValue.from_text("xoxb")
    environment.installation_store.values["provider"] = InstallationData(
        namespace="provider",
        generation=1,
        values={"external_installation_id": "T1"},
    )
    environment.http.responses.extend(
        [
            _response(
                {
                    "ok": True,
                    "channels": [{"id": "C1", "name": "general"}],
                    "response_metadata": {"next_cursor": ""},
                }
            ),
            _response(
                {
                    "ok": True,
                    "messages": [
                        {
                            "type": "message",
                            "user": "U1",
                            "text": "hello",
                            "ts": "1735689600.000001",
                        }
                    ],
                    "response_metadata": {"next_cursor": ""},
                }
            ),
            _response({"ok": True, "messages": []}),
        ]
    )
    binding = _bind(build_slack_connector, environment)
    pull = binding.require(HISTORICAL_PULL_V1)
    plan = await pull.plan(PlanRequest(), _operation(environment))
    page = await pull.fetch(FetchRequest(shard=plan.shards[0]), _operation(environment))
    assert page.end_of_data and page.checkpoint is not None
    record = page.records[0]
    identity = binding.require(IDENTITY_V1).external_id(
        IdentityInput(
            record=record,
            external_installation_id="T1",
            ingress_kind="backfill",
        )
    )
    drafts = await binding.require(NORMALIZATION_V1).normalize(
        NormalizationInput(record=record, ingress_kind="backfill"),
        _operation(environment),
    )
    decision = await binding.require(RECONCILIATION_V1).reconcile(
        ReconciliationRequest(
            run_id=uuid4(),
            shards=(
                ShardSummary(
                    shard_id=uuid4(),
                    shard=plan.shards[0],
                    state="done",
                    cursor=page.checkpoint,
                    record_count=1,
                ),
            ),
            pass_number=1,
        ),
        _operation(environment),
    )
    assert identity == "C1:1735689600.000001"
    assert drafts[0].external_id == identity
    assert not decision.has_gaps


@pytest.mark.asyncio
async def test_slack_edits_and_deletions_preserve_one_object_history() -> None:
    environment = FakeHostEnvironment()
    binding = _bind(build_slack_connector, environment)
    normalizer = binding.require(NORMALIZATION_V1)
    edit = (
        await normalizer.normalize(
            NormalizationInput(
                record=SourceRecord(
                    native_type="event_callback",
                    payload={
                        "event": {
                            "type": "message",
                            "subtype": "message_changed",
                            "channel": "C1",
                            "event_ts": "1735689700.000001",
                            "message": {
                                "ts": "1735689600.000001",
                                "edited": {"ts": "1735689700.000001"},
                                "user": "U1",
                                "text": "audit is complete",
                            },
                        }
                    },
                ),
                ingress_kind="webhook",
            ),
            _operation(environment),
        )
    )[0]
    deletion = (
        await normalizer.normalize(
            NormalizationInput(
                record=SourceRecord(
                    native_type="event_callback",
                    payload={
                        "event": {
                            "type": "message",
                            "subtype": "message_deleted",
                            "channel": "C1",
                            "deleted_ts": "1735689600.000001",
                            "event_ts": "1735689800.000001",
                            "previous_message": {
                                "ts": "1735689600.000001",
                                "edited": {"ts": "1735689700.000001"},
                                "user": "U1",
                                "text": "audit is complete",
                            },
                        }
                    },
                ),
                ingress_kind="webhook",
            ),
            _operation(environment),
        )
    )[0]

    assert edit.external_id == deletion.external_id == "C1:1735689600.000001"
    assert edit.source_object is not None
    assert edit.source_object.operation == "update"
    assert deletion.source_object is not None
    assert deletion.source_object.operation == "delete"
    assert deletion.kind == "state_change"


@pytest.mark.asyncio
async def test_notion_native_plan_poll_and_normalization_need_no_ambient_binding() -> (
    None
):
    environment = FakeHostEnvironment()
    environment.secrets.values["oauth_access_token"] = SecretValue.from_text(
        "notion-token"
    )
    environment.installation_store.values["provider"] = InstallationData(
        namespace="provider",
        generation=1,
        values={"external_installation_id": "workspace-1"},
    )
    environment.http.responses.extend(
        [
            _response({"results": [], "has_more": False, "next_cursor": None}),
            _response(
                {
                    "results": [
                        {
                            "object": "page",
                            "id": "page-1",
                            "created_time": "2025-01-01T00:00:00Z",
                            "last_edited_time": "2025-01-01T00:00:00Z",
                            "parent": {"type": "workspace"},
                            "properties": {},
                        }
                    ],
                    "has_more": False,
                    "next_cursor": None,
                }
            ),
        ]
    )
    binding = _bind(build_notion_connector, environment)
    plan = await binding.require(HISTORICAL_PULL_V1).plan(
        PlanRequest(), _operation(environment)
    )
    page = await binding.require(INCREMENTAL_POLL_V1).poll(
        PollRequest(), _operation(environment)
    )
    assert plan.shards == (
        ShardPlan(
            kind="notion_page_tree",
            identifier={
                "shard_kind": "notion_page_tree",
                "workspace_id": "workspace-1",
            },
        ),
    )
    assert page.records[0].payload["id"] == "page-1"  # type: ignore[index]
    draft = (
        await binding.require(NORMALIZATION_V1).normalize(
            NormalizationInput(record=page.records[0], ingress_kind="poll"),
            _operation(environment),
        )
    )[0]
    assert draft.external_id == "notion:page:page-1"
    assert draft.source_object is not None
    assert draft.source_object.object_id == "page-1"
    assert draft.source_object.revision_id == "2025-01-01T00:00:00Z"
    assert draft.source_object.operation == "create"


@pytest.mark.asyncio
async def test_whatsapp_native_webhook_and_normalization_need_no_ambient_binding() -> (
    None
):
    environment = FakeHostEnvironment()
    secret = b"app-secret"
    environment.secrets.values["app_secret"] = SecretValue(secret)
    binding = _bind(build_whatsapp_connector, environment)
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
                                        "text": {"body": "hello"},
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
    signature = "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
    result = await binding.require(WEBHOOK_V1).verify_and_decode(
        BoundedWebhookRequest(
            body=body,
            headers={"x-hub-signature-256": signature},
            received_at=_NOW,
        ),
        _operation(environment),
    )
    draft = (
        await binding.require(NORMALIZATION_V1).normalize(
            NormalizationInput(
                record=result.events[0].record,
                ingress_kind="webhook",
            ),
            _operation(environment),
        )
    )[0]
    assert draft.external_id == "whatsapp:P1:wamid.1"
    assert result.events[0].external_installation_id == "P1"
