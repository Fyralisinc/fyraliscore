"""AWS CloudTrail events fixture generator (IN-AWS, X2/X3 infra).

`make_aws(account_id=..., region=..., events=N, base_ms=..., per_page=P, ...)`
produces a deterministic set of CloudTrail event objects shaped to feed
`MockAwsClient`, which is in turn driven by the REAL backfill fetcher
(`services/ingest/ingestion/fetchers/aws.py`).

The event shape mirrors the REAL `CloudTrail:LookupEvents` array elements the
production fetcher consumes and the `aws:event` handler normalises: each element
carries `eventId` (stable, IMMUTABLE id), `eventTime` (epoch MILLISECONDS in the
synthetic shape), `eventName`, `eventSource`, `awsRegion`, `recipientAccountId`,
`userIdentity` — and, for a CloudWatch alarm-state-change event, `alarmName` /
`newState` / `prevState`. The fetcher fans each event out 1:1 into one
`_fyralis_record_type="event"` record, so the per-install observation count is
exactly `events`.

============================================================
NEWEST-FIRST ORDER (load-bearing)
============================================================
The real `LookupEvents` returns newest-first; the fetcher walks pages by the
opaque `NextToken` cursor within a frozen time window. This generator emits the
list **newest-first** (index 0 = newest), spacing each event one minute APART
going backward from `base_ms` so the resulting `occurred_at` instants are
distinct and well-ordered. `base_ms` defaults to 2026-01-05T00:00:00Z so every
event's `occurred_at` lands in the 2026-01 observations partition window.

Determinism: every id / name / principal is derived from a SHA-256 of (seed,
account, region, index), so a given call always yields byte-identical output.

============================================================
poll event template (for the live AwsPollGenerator)
============================================================
`make_aws` also returns a `poll_event` key: a CloudTrail-shaped event template
the live `aws:event` poll driver can reuse (it is NOT consumed by the backfill
mock client). See `_poll_event_template`.
"""
from __future__ import annotations

import hashlib
from typing import Any


_MS_PER_MINUTE = 60_000

# 2026-01-05T00:00:00Z in epoch milliseconds — keeps `occurred_at` in the
# 2026-01 observations partition window even after the backward 1-min spacing.
_DEFAULT_BASE_MS = 1767571200000

# Default per-install backfill event count (= the 1:1 fan-out count). The all-N
# overlap gate expects exactly this many backfill observations per tenant.
_DEFAULT_EVENTS = 3


def make_aws(
    *,
    account_id: str,
    region: str = "us-east-1",
    events: int = _DEFAULT_EVENTS,
    base_ms: int | None = None,
    per_page: int = 50,
    seed: object = None,
    include_alarm_events: bool = True,
) -> dict[str, Any]:
    """Build an AWS account/region fixture consumable by `MockAwsClient(fixture=...)`.

    Args:
      account_id: The 12-digit AWS account id; the fetcher derives the external_id
        namespace from it (`aws:{account_id}:{region}:event:{event_id}`).
      region: The AWS region (external_id namespace component).
      events: Number of event objects (== the 1:1 fan-out count). Defaults to 3.
      base_ms: epoch-ms timestamp of the NEWEST event; older events step 1 minute
        backward from here. Default lands in 2026-01.
      per_page: The mock client's per-page cap for `list_events` (mirrors the real
        `MaxResults` cap of 50). Drive token-pagination by setting this below
        `events`.
      seed: Optional determinism salt so distinct installs get distinct ids.
      include_alarm_events: When True, every 2nd event is a CloudWatch
        alarm-state-change event (carries `alarmName` + `newState`) so the
        handler's state_change path is exercised; the rest are plain management
        events with an IAM principal (actor path).

    Returns:
      A dict shaped exactly for `MockAwsClient`:
        {
          "account_id": "123456789012",
          "region": "us-east-1",
          "per_page": 50,
          # newest-first, like the real LookupEvents array.
          "events": [ <event dict>, ... ],
          # template only — for the live aws:event poll driver.
          "poll_event": { ... },
        }
    """
    n = max(0, int(events))
    base = int(base_ms) if base_ms is not None else _DEFAULT_BASE_MS
    items: list[dict[str, Any]] = []
    for idx in range(n):
        # idx 0 == newest; step backward in time so the list is newest-first
        # and every `eventTime` is distinct.
        time_ms = base - idx * _MS_PER_MINUTE
        is_alarm = include_alarm_events and (idx % 2 == 1)
        items.append(
            _event(seed, account_id, region, idx, time_ms, is_alarm=is_alarm)
        )

    return {
        "account_id": str(account_id),
        "region": str(region),
        "per_page": int(per_page),
        "events": items,
        "poll_event": _poll_event_template(seed, account_id, region, base),
    }


def _event(
    seed: object,
    account_id: str,
    region: str,
    idx: int,
    time_ms: int,
    *,
    is_alarm: bool,
) -> dict[str, Any]:
    """One `CloudTrail:LookupEvents` array element (bare event object)."""
    digest = _digest(seed, account_id, region, idx)
    # CloudTrail eventIds are UUIDs; keep them stable + distinct per install.
    event_id = f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"

    if is_alarm:
        # CloudWatch alarm-state-change event: machine-generated (no IAM user),
        # carries an alarm name + newState/prevState.
        new_state = "ALARM" if idx % 4 == 1 else "OK"
        prev_state = "OK" if idx % 4 == 1 else "ALARM"
        return {
            "eventId": event_id,
            "eventName": "DescribeAlarms",
            "eventSource": "monitoring.amazonaws.com",
            "eventTime": time_ms,
            "awsRegion": region,
            "recipientAccountId": str(account_id),
            "alarmName": f"high-cpu-{digest[:6]}",
            "newState": new_state,
            "prevState": prev_state,
            "userIdentity": {"type": "AWSService", "invokedBy": "cloudwatch.amazonaws.com"},
            "cloudTrailEvent": _cloud_trail_event_json(event_id, region, account_id),
        }

    # Plain management event: carries a real IAM principal so the handler's
    # actor-resolution path is exercised.
    role = f"arn:aws:iam::{account_id}:role/deploy-{digest[:6]}"
    return {
        "eventId": event_id,
        "eventName": "RunInstances" if idx % 3 == 0 else "PutObject",
        "eventSource": "ec2.amazonaws.com" if idx % 3 == 0 else "s3.amazonaws.com",
        "eventTime": time_ms,
        "awsRegion": region,
        "recipientAccountId": str(account_id),
        "userIdentity": {
            "type": "AssumedRole",
            "arn": role,
            "principalId": f"AROA{digest[:12].upper()}",
            "userName": f"deploy-{digest[:6]}",
        },
        "cloudTrailEvent": _cloud_trail_event_json(event_id, region, account_id),
    }


def _cloud_trail_event_json(event_id: str, region: str, account_id: str) -> dict[str, Any]:
    """A trimmed `cloudTrailEvent` JSON blob (preserved on content for audit)."""
    return {
        "eventVersion": "1.08",
        "eventID": event_id,
        "awsRegion": region,
        "recipientAccountId": str(account_id),
        "managementEvent": True,
        "readOnly": False,
    }


def _poll_event_template(
    seed: object, account_id: str, region: str, base_ms: int,
) -> dict[str, Any]:
    """A CloudTrail-shaped event template for the LIVE `aws:event` poll driver.
    NOT consumed by the backfill mock client — it documents the polled-event
    payload shape so `AwsPollGenerator` can mint fresh events from it. Mirrors
    what `handlers/aws.py::handle_aws_event` reads: top-level `eventId` /
    `eventTime` / `eventName` / `eventSource` / `awsRegion` /
    `recipientAccountId` / `userIdentity`, plus alarm fields for the alarm case.
    """
    digest = _digest(seed, account_id, region, "poll")
    event_id = f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"
    return {
        "eventId": event_id,
        "eventName": "StartInstances",
        "eventSource": "ec2.amazonaws.com",
        "eventTime": base_ms,
        "awsRegion": region,
        "recipientAccountId": str(account_id),
        "userIdentity": {
            "type": "AssumedRole",
            "arn": f"arn:aws:iam::{account_id}:role/ops-{digest[:6]}",
            "userName": f"ops-{digest[:6]}",
        },
        "cloudTrailEvent": _cloud_trail_event_json(event_id, region, account_id),
    }


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_aws"]
