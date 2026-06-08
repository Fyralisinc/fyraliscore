"""Tests for services/ingest/ingestion/handlers/aws.py (IN-AWS)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.aws import aws_event, handle_aws_event


pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------

async def test_channel_registered_authoritative():
    assert get_handler("aws:event") is handle_aws_event
    assert CHANNEL_TRUST_MAP["aws:event"] == "authoritative"


# ---------------------------------------------------------------------
# external_id constructor (IMMUTABLE — no version suffix)
# ---------------------------------------------------------------------

async def test_external_id_is_immutable_and_namespaced():
    eid = aws_event("123456789012", "us-east-1", "evt-abc")
    assert eid == "aws:123456789012:us-east-1:event:evt-abc"
    # Same event id in another account / region is a DISTINCT key.
    assert aws_event("999999999999", "us-east-1", "evt-abc") != eid
    assert aws_event("123456789012", "eu-west-1", "evt-abc") != eid


# ---------------------------------------------------------------------
# Event channel — discrimination
# ---------------------------------------------------------------------

async def test_management_event_is_signal():
    rec = {
        "eventId": "evt-1",
        "eventName": "RunInstances",
        "eventSource": "ec2.amazonaws.com",
        "eventTime": 1_700_000_000_000,
        "_fyralis_record_type": "event",
        "_fyralis_account_id": "123456789012",
        "_fyralis_region": "us-east-1",
        "userIdentity": {
            "type": "AssumedRole",
            "arn": "arn:aws:iam::123456789012:role/deploy",
            "userName": "deploy-bot",
        },
    }
    draft = await handle_aws_event(rec, {})
    assert draft.source_channel == "aws:event"
    assert draft.kind == "signal"
    assert draft.external_id == "aws:123456789012:us-east-1:event:evt-1"
    assert draft.content["object_type"] == "management_event"
    assert draft.source_actor_ref == "aws:iam:arn:aws:iam::123456789012:role/deploy"


async def test_alarm_state_change_is_state_change():
    rec = {
        "eventId": "evt-2",
        "eventName": "DescribeAlarms",
        "eventSource": "monitoring.amazonaws.com",
        "eventTime": 1_700_000_500_000,
        "alarmName": "high-cpu-prod",
        "prevState": "OK", "newState": "ALARM",
        "_fyralis_account_id": "123456789012",
        "_fyralis_region": "us-east-1",
        "userIdentity": {"type": "AWSService"},
    }
    draft = await handle_aws_event(rec, {})
    assert draft.kind == "state_change"
    assert draft.content["object_type"] == "alarm_state_change"
    assert "OK → ALARM" in draft.content_text
    # Machine-generated alarm event -> actorless.
    assert draft.source_actor_ref is None


async def test_namespace_falls_back_to_recipient_account_and_region():
    rec = {
        "eventId": "evt-3",
        "eventName": "PutObject",
        "eventSource": "s3.amazonaws.com",
        "eventTime": 1_700_000_000_000,
        "recipientAccountId": "555555555555",
        "awsRegion": "eu-central-1",
    }
    draft = await handle_aws_event(rec, {})
    assert draft.external_id == "aws:555555555555:eu-central-1:event:evt-3"


async def test_missing_event_id_raises_validation():
    from lib.shared.errors import ValidationError

    with pytest.raises(ValidationError):
        await handle_aws_event({"eventName": "X", "_fyralis_account_id": "1"}, {})


async def test_event_time_parses_rfc3339():
    rec = {
        "eventId": "evt-4",
        "eventName": "CreateBucket",
        "eventSource": "s3.amazonaws.com",
        "eventTime": "2026-01-05T00:00:00Z",
        "_fyralis_account_id": "123456789012",
        "_fyralis_region": "us-east-1",
    }
    draft = await handle_aws_event(rec, {})
    assert draft.occurred_at.year == 2026
    assert draft.occurred_at.month == 1
