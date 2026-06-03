"""Tests for services/ingest/ingestion/handlers/grafana.py (IN-GRAFANA)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.grafana import (
    handle_grafana_alert,
    handle_grafana_annotation,
)


pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------

async def test_channels_registered_authoritative():
    assert get_handler("grafana:annotation") is handle_grafana_annotation
    assert get_handler("grafana:alert") is handle_grafana_alert
    assert CHANNEL_TRUST_MAP["grafana:annotation"] == "authoritative"
    assert CHANNEL_TRUST_MAP["grafana:alert"] == "authoritative"


# ---------------------------------------------------------------------
# Annotation channel
# ---------------------------------------------------------------------

async def test_plain_annotation_is_signal():
    rec = {
        "id": 5, "time": 1_700_000_000_000, "text": "deployed v2.1",
        "tags": ["deploy", "prod"], "_fyralis_record_type": "annotation",
        "_fyralis_instance": "acme.grafana.net",
    }
    draft = await handle_grafana_annotation(rec, {})
    assert draft.source_channel == "grafana:annotation"
    assert draft.kind == "signal"
    assert draft.external_id == "grafana:acme.grafana.net:annotation:5:1700000000000"
    assert "deployed v2.1" in draft.content_text
    assert "deploy" in draft.content_text  # tags rendered
    assert draft.content["object_type"] == "annotation"


async def test_alert_state_annotation_is_state_change():
    rec = {
        "id": 6, "alertId": 42, "newState": "Alerting", "prevState": "Normal",
        "time": 1_700_000_500_000, "text": "CPU > 90%",
        "_fyralis_instance": "acme.grafana.net",
    }
    draft = await handle_grafana_annotation(rec, {})
    assert draft.kind == "state_change"
    assert draft.content["object_type"] == "alert_state_annotation"
    assert "Normal → Alerting" in draft.content_text


async def test_user_annotation_resolves_actor_alert_one_does_not():
    user_rec = {
        "id": 7, "userId": 3, "userName": "alice", "time": 1_700_000_000_000,
        "text": "manual note", "_fyralis_instance": "h",
    }
    user_draft = await handle_grafana_annotation(user_rec, {})
    assert user_draft.source_actor_ref == "grafana:user:3"

    # Alert-generated annotations have userId 0 -> machine -> actorless.
    machine_rec = {
        "id": 8, "userId": 0, "alertId": 9, "newState": "Normal",
        "time": 1_700_000_000_000, "_fyralis_instance": "h",
    }
    machine_draft = await handle_grafana_annotation(machine_rec, {})
    assert machine_draft.source_actor_ref is None


async def test_annotation_missing_id_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_grafana_annotation({"time": 1}, {})


# ---------------------------------------------------------------------
# Alert channel (live webhook group)
# ---------------------------------------------------------------------

def _alert(alertname, *, status="firing", starts="2026-06-02T10:00:00Z",
           ends="0001-01-01T00:00:00Z", fingerprint="fp1"):
    return {
        "status": status,
        "labels": {"alertname": alertname},
        "annotations": {"summary": f"{alertname} summary"},
        "startsAt": starts,
        "endsAt": ends,
        "fingerprint": fingerprint,
    }


async def test_alert_group_firing_is_state_change_and_actorless():
    payload = {
        "status": "firing",
        "externalURL": "https://acme.grafana.net",
        "orgId": 1,
        "groupKey": '{}/{alertname="CPUHigh"}:{}',
        "commonLabels": {"alertname": "CPUHigh", "service": "checkout"},
        "commonAnnotations": {"summary": "cpu"},
        "alerts": [_alert("CPUHigh")],
    }
    draft = await handle_grafana_alert(payload, {})
    assert draft.source_channel == "grafana:alert"
    assert draft.kind == "state_change"
    assert draft.source_actor_ref is None  # machine-generated
    assert draft.external_id.startswith("grafana:acme.grafana.net:alert:")
    assert ":firing:" in draft.external_id
    assert "FIRING" in draft.content_text
    assert "CPUHigh" in draft.content_text
    # occurred_at follows startsAt (firing).
    assert draft.occurred_at.isoformat().startswith("2026-06-02T10:00:00")
    # Entities: alert name + the salient `service` label.
    etypes = {(e["type"], e["id"]) for e in draft.entities_hint}
    assert ("grafana_alert", "CPUHigh") in etypes
    assert ("grafana_label_service", "checkout") in etypes
    assert draft.content["num_alerts"] == 1


async def test_alert_group_resolved_uses_endsat():
    payload = {
        "status": "resolved",
        "externalURL": "https://acme.grafana.net",
        "groupKey": "g1",
        "commonLabels": {"alertname": "DiskFull"},
        "alerts": [_alert("DiskFull", status="resolved",
                          starts="2026-06-02T09:00:00Z",
                          ends="2026-06-02T11:30:00Z")],
    }
    draft = await handle_grafana_alert(payload, {})
    assert ":resolved:" in draft.external_id
    assert draft.occurred_at.isoformat().startswith("2026-06-02T11:30:00")


async def test_firing_then_resolved_are_distinct_observations():
    base = {
        "externalURL": "https://acme.grafana.net",
        "groupKey": "g1",
        "commonLabels": {"alertname": "CPUHigh"},
    }
    firing = await handle_grafana_alert(
        {**base, "status": "firing", "alerts": [_alert("CPUHigh")]}, {},
    )
    resolved = await handle_grafana_alert(
        {**base, "status": "resolved",
         "alerts": [_alert("CPUHigh", status="resolved",
                           ends="2026-06-02T12:00:00Z")]}, {},
    )
    assert firing.external_id != resolved.external_id
