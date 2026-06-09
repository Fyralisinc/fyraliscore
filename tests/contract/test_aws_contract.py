"""Contract test: the AWS CloudTrail path parses a REAL botocore LookupEvents page.

Guards the Phase-3 drift fix (finding #2): a REAL `cloudtrail.lookup_events`
response is PascalCase — `Events[].{EventId,EventName,EventTime,Username,
CloudTrailEvent,Resources[]}` plus a top-level `NextToken` — whereas the
synthetic spammer / normalized records are camelCase (`eventId`, `eventTime` as
epoch ms, `cloudTrailEvent` as a dict). Two real-shape bugs are fixed additively:

  1. the fetcher's high-water extractor (`_event_time_ms`) and the handler's
     `_occurred_at` read camelCase epoch-ms ONLY — they must ALSO read PascalCase
     `EventTime`, which botocore returns as a `datetime` (and a captured fixture
     carries as an ISO-8601 string);
  2. `CloudTrailEvent` is a JSON STRING on the wire — the handler must
     `json.loads` it (the synthetic record already carries a dict).

The synthetic (camelCase / int-ms) path stays the fallback and is unchanged.
Verified against docs.aws.amazon.com (LookupEvents) + boto3 cloudtrail.
"""
from __future__ import annotations

import datetime as dt
import json

import pytest

from services.ingest.ingestion.fetchers import aws as af
from services.ingest.ingestion.handlers.aws import aws_event, handle_aws_event
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract

_ACCOUNT = "123456789012"
_REGION = "us-east-1"


def _fixture():
    return load_fixture("aws", "api_response", "cloudtrail_lookup_events")


def _page() -> dict:
    """The PascalCase LookupEvents response body as botocore returns it."""
    body = _fixture().response_body
    assert isinstance(body, dict)
    return body


def _tag(element: dict) -> dict:
    """Mirror what the fetcher does to each `Events[]` element: pass it through
    unchanged but tag it with the namespacing private fields the handler reads."""
    rec = dict(element)
    rec["_fyralis_record_type"] = "event"
    rec["_fyralis_account_id"] = _ACCOUNT
    rec["_fyralis_region"] = _REGION
    return rec


def test_fixture_is_real_pascalcase_shape():
    body = _page()
    # Top-level envelope is PascalCase, NOT the synthetic camelCase.
    assert "Events" in body and "events" not in body
    assert "NextToken" in body
    evt = body["Events"][0]
    assert "EventId" in evt and "eventId" not in evt
    assert "EventName" in evt
    # CloudTrailEvent is a JSON STRING on the wire (must be json.loads'd).
    assert isinstance(evt["CloudTrailEvent"], str)
    # Resources is the PascalCase list of {ResourceType, ResourceName}.
    assert evt["Resources"][0]["ResourceType"]


def test_fetcher_high_water_reads_pascalcase_event_time():
    """Bug #1 (fetcher): `_event_time_ms` must read PascalCase `EventTime` —
    both as the ISO-8601 string a capture holds AND as the `datetime` botocore
    actually returns — not just camelCase epoch-ms."""
    evt = _page()["Events"][0]
    expected_ms = int(
        dt.datetime(2026, 1, 15, 15, 5, 0, tzinfo=dt.timezone.utc).timestamp() * 1000
    )

    # ISO-8601 string form (as carried in the fixture).
    assert af._event_time_ms(evt) == expected_ms

    # botocore's real return type: a tz-aware datetime under PascalCase EventTime.
    evt_dt = dict(evt)
    evt_dt["EventTime"] = dt.datetime(2026, 1, 15, 15, 5, 0, tzinfo=dt.timezone.utc)
    assert af._event_time_ms(evt_dt) == expected_ms

    # Naive datetime (no tzinfo) is treated as UTC, not dropped.
    evt_naive = dict(evt)
    evt_naive["EventTime"] = dt.datetime(2026, 1, 15, 15, 5, 0)
    assert af._event_time_ms(evt_naive) == expected_ms


def test_fetcher_synthetic_camelcase_path_unchanged():
    """The camelCase epoch-ms fallback (synthetic spammer shape) is byte-for-byte
    preserved — read first, so the all-25 gate is unaffected."""
    assert af._event_time_ms({"eventTime": 1_700_000_000_000}) == 1_700_000_000_000
    assert af._event_time_ms({"eventTime": True}) is None
    assert af._event_time_ms({}) is None


async def test_handler_parses_real_pascalcase_element():
    """Bug #1 + #2 (handler): the handler parses the PascalCase element into the
    right observation fields, json.loads'ing the embedded CloudTrailEvent."""
    evt = _page()["Events"][0]
    draft = await handle_aws_event(_tag(evt), {})

    assert draft.source_channel == "aws:event"
    assert draft.kind == "signal"
    assert draft.external_id == aws_event(_ACCOUNT, _REGION, evt["EventId"])
    assert draft.content["object_type"] == "management_event"
    assert draft.content["event_id"] == evt["EventId"]
    assert draft.content["event_name"] == "RunInstances"
    assert draft.content["event_source"] == "ec2.amazonaws.com"

    # occurred_at parsed from PascalCase EventTime (ISO-8601 string).
    assert draft.occurred_at == dt.datetime(
        2026, 1, 15, 15, 5, 0, tzinfo=dt.timezone.utc
    )

    # CloudTrailEvent (a JSON STRING on the wire) is json.loads'd into a dict.
    cte = draft.content["cloud_trail_event"]
    assert isinstance(cte, dict)
    assert cte == json.loads(evt["CloudTrailEvent"])
    assert cte["eventName"] == "RunInstances"
    assert cte["recipientAccountId"] == _ACCOUNT

    # Actor resolved from the principal nested INSIDE the parsed CloudTrailEvent
    # (real elements carry no top-level userIdentity).
    assert draft.source_actor_ref == (
        "aws:iam:arn:aws:sts::123456789012:assumed-role/deploy/deploy-bot"
    )

    # Resources[] (PascalCase) become aws_resource entities.
    res_ids = {
        e["id"] for e in draft.entities_hint if e["type"] == "aws_resource"
    }
    assert "i-0abc1234def567890" in res_ids


async def test_handler_actor_falls_back_to_top_level_username():
    """A real element whose CloudTrailEvent lacks a resolvable arn still yields an
    actor from the top-level PascalCase `Username`."""
    evt = dict(_page()["Events"][0])
    # Drop the principal-bearing CloudTrailEvent; keep only top-level Username.
    evt["CloudTrailEvent"] = json.dumps({"eventName": "RunInstances"})
    draft = await handle_aws_event(_tag(evt), {})
    assert draft.source_actor_ref == "aws:iam:deploy-bot"
