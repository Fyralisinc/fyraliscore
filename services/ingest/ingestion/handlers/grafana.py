"""services/ingest/ingestion/handlers/grafana.py — Grafana annotation + alert handlers (IN-GRAFANA).

TWO channels (Grafana is a two-channel source, unlike Jira's single channel):

  - `grafana:annotation` — BACKFILL/POLL annotations from `GET /api/annotations`.
    Records arrive tagged `_fyralis_record_type="annotation"` (set by the
    fetcher). A plain annotation -> kind="signal"; an alert-state-change
    annotation (carries `alertId` / `newState`) -> kind="state_change".
        external_id: grafana:{instance}:annotation:{id}:{time}   (versioned by time)

  - `grafana:alert` — LIVE Grafana Alerting webhook contact-point deliveries
    (Alertmanager-superset JSON). ONE webhook POST delivers a notification GROUP
    of alerts; v1 emits ONE state_change observation per delivery (the full
    per-alert detail is preserved in content["alerts"]). Per-alert fan-out is a
    documented v2 enhancement (it needs a normalizer-level group-explode step,
    which doesn't exist — the handler contract returns a single draft).
        external_id: grafana:{instance}:alert:{group_hash}:{status}:{rep_ts}

Both channels are `authoritative`: Grafana is the system of record for its own
monitoring/alert state and dashboard annotations.

Per the IN-15 mutable-source dedup lesson, external_id is versioned (annotation by
`time`; alert group by status + representative timestamp) so a re-delivery dedups
but a genuinely new state lands as a new observation.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingest.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    ObservationDraft,
    register,
)


_CHANNEL_ANNOTATION = "grafana:annotation"
_CHANNEL_ALERT = "grafana:alert"
_TRUST = "authoritative"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _from_ms(value: Any) -> datetime | None:
    """epoch milliseconds -> aware datetime."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and value > 0:
        try:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return None
    return None


def _parse_iso(value: Any) -> datetime | None:
    """Parse an RFC3339 timestamp (alert startsAt/endsAt, e.g.
    `2026-06-02T10:30:00.000Z`). Grafana's zero-value `0001-01-01T00:00:00Z`
    (used for an unset endsAt on a firing alert) parses but is far in the past;
    callers guard against using it."""
    if not isinstance(value, str) or not value:
        return None
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    elif len(s) >= 6 and s[-3] != ":" and s[-5] in "+-" and s[-6] != "T":
        s = s[:-2] + ":" + s[-2:]
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _short_hash(value: str) -> str:
    return hashlib.blake2b(value.encode("utf-8"), digest_size=8).hexdigest()


def _instance_of(payload: dict[str, Any]) -> str:
    """external_id namespace. Backfill/poll records carry `_fyralis_instance`; a
    live webhook carries the instance host in the top-level `externalURL`."""
    inst = payload.get("_fyralis_instance")
    if isinstance(inst, str) and inst:
        return inst
    ext = payload.get("externalURL")
    if isinstance(ext, str) and ext and "://" in ext:
        return ext.split("://", 1)[1].split("/", 1)[0].strip() or "unknown"
    return "unknown"


# ---------------------------------------------------------------------
# Annotation channel (backfill / poll)
# ---------------------------------------------------------------------

def _annotation_draft(ann: dict[str, Any], instance: str) -> ObservationDraft:
    ann_id = ann.get("id")
    if ann_id is None:
        raise ValidationError("grafana annotation missing id", channel=_CHANNEL_ANNOTATION)
    ann_id = str(ann_id)

    time_ms = ann.get("time")
    occurred = _from_ms(time_ms) or _utcnow()
    external_id = f"grafana:{instance}:annotation:{ann_id}:{time_ms if time_ms else 'none'}"

    text = ann.get("text") if isinstance(ann.get("text"), str) else ""
    tags = [t for t in (ann.get("tags") or []) if isinstance(t, str)]
    alert_id = ann.get("alertId")
    new_state = ann.get("newState") if isinstance(ann.get("newState"), str) else None
    prev_state = ann.get("prevState") if isinstance(ann.get("prevState"), str) else None
    # An alert-state-change annotation carries a non-zero alertId and/or a
    # newState; treat it as a state_change. A plain (manual / region / deploy)
    # annotation is a signal.
    is_alert_state = bool(alert_id) or bool(new_state)
    object_type = "alert_state_annotation" if is_alert_state else "annotation"

    if is_alert_state:
        transition = f"{prev_state or '∅'} → {new_state or '∅'}"
        content_text = f"[grafana alert] {transition}"
        if text:
            content_text += f": {_truncate(text)}"
    else:
        content_text = _truncate(text) if text else "(grafana annotation)"
    if tags:
        content_text += f" [{', '.join(tags[:8])}]"

    # Actor: a user-created annotation carries a non-zero userId + userName;
    # alert-generated annotations have userId 0 / no user (machine) -> actorless.
    user_id = ann.get("userId")
    user_name = ann.get("userName") if isinstance(ann.get("userName"), str) else None
    actor_ref: str | None = None
    if isinstance(user_id, int) and user_id > 0:
        actor_ref = f"grafana:user:{user_id}"

    entities: list[dict[str, Any]] = []
    dashboard_uid = ann.get("dashboardUID")
    if isinstance(dashboard_uid, str) and dashboard_uid:
        entities.append({"type": "grafana_dashboard", "id": dashboard_uid})
    if actor_ref and user_name:
        entities.append({"type": "grafana_user", "id": str(user_id),
                         "display_name": user_name, "role": "actor"})
    for tag in tags[:8]:
        entities.append({"type": "grafana_tag", "id": tag})

    content: dict[str, Any] = {
        "object_type": object_type,
        "annotation_id": ann_id,
        "alert_id": alert_id,
        "new_state": new_state,
        "prev_state": prev_state,
        "text": text,
        "tags": tags,
        "dashboard_uid": dashboard_uid,
        "panel_id": ann.get("panelId"),
        "time_ms": time_ms,
        "time_end_ms": ann.get("timeEnd"),
        "user_id": user_id,
        "user_name": user_name,
    }

    return ObservationDraft(
        source_channel=_CHANNEL_ANNOTATION,
        content_text=content_text,
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="state_change" if is_alert_state else "signal",
        source_actor_ref=actor_ref,
        external_id=external_id,
        entities_hint=entities,
        raw_payload=ann,
    )


@register(_CHANNEL_ANNOTATION)
async def handle_grafana_annotation(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError(
            "grafana annotation payload must be a JSON object",
            channel=_CHANNEL_ANNOTATION,
        )
    instance = _instance_of(payload)
    return _annotation_draft(payload, instance)


# ---------------------------------------------------------------------
# Alert channel (live webhook group)
# ---------------------------------------------------------------------

def _alert_name(alert: dict[str, Any]) -> str | None:
    labels = alert.get("labels")
    if isinstance(labels, dict):
        name = labels.get("alertname")
        if isinstance(name, str) and name:
            return name
    return None


def _representative_ts(alerts: list[dict[str, Any]], status: str) -> datetime:
    """The occurred_at for the group: newest startsAt (firing) or endsAt
    (resolved) across the alerts. Falls back to now."""
    field = "endsAt" if status == "resolved" else "startsAt"
    best: datetime | None = None
    for alert in alerts:
        ts = _parse_iso(alert.get(field))
        # Guard Grafana's zero-value endsAt sentinel (year 0001).
        if ts is not None and ts.year > 1 and (best is None or ts > best):
            best = ts
    return best or _utcnow()


@register(_CHANNEL_ALERT)
async def handle_grafana_alert(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError(
            "grafana alert payload must be a JSON object", channel=_CHANNEL_ALERT,
        )

    instance = _instance_of(payload)
    status = payload.get("status") if isinstance(payload.get("status"), str) else "firing"
    alerts = payload.get("alerts")
    alerts = [a for a in alerts if isinstance(a, dict)] if isinstance(alerts, list) else []
    group_key = payload.get("groupKey") if isinstance(payload.get("groupKey"), str) else ""
    common_labels = payload.get("commonLabels") if isinstance(payload.get("commonLabels"), dict) else {}
    common_annotations = (
        payload.get("commonAnnotations") if isinstance(payload.get("commonAnnotations"), dict) else {}
    )

    if not alerts and not group_key:
        raise ValidationError(
            "grafana alert payload carries neither alerts nor groupKey",
            channel=_CHANNEL_ALERT,
        )

    occurred = _representative_ts(alerts, status)
    rep_ts_iso = occurred.isoformat()
    group_hash = _short_hash(group_key or repr(sorted(common_labels.items())))
    external_id = f"grafana:{instance}:alert:{group_hash}:{status}:{rep_ts_iso}"

    # Human-legible synthesis.
    names = [n for n in (_alert_name(a) for a in alerts) if n]
    distinct_names = sorted(set(names))
    label_summary = ", ".join(f"{k}={v}" for k, v in list(common_labels.items())[:6])
    headline = ", ".join(distinct_names[:5]) or (common_labels.get("alertname") or "alert")
    content_text = f"[{status.upper()}×{len(alerts) or 1}] {headline}"
    if label_summary:
        content_text += f" ({label_summary})"

    # Entities: distinct alert names + a few salient resource labels. Machine-
    # generated, so no actor (source_actor_ref=None) — exercises the actorless
    # path in ingestion core's actor resolution.
    entities: list[dict[str, Any]] = [
        {"type": "grafana_alert", "id": name} for name in distinct_names
    ]
    for key in ("service", "namespace", "job", "instance", "cluster"):
        val = common_labels.get(key)
        if isinstance(val, str) and val:
            entities.append({"type": f"grafana_label_{key}", "id": val})

    content: dict[str, Any] = {
        "object_type": "alert_group",
        "status": status,
        "num_alerts": len(alerts),
        "group_key": group_key,
        "alert_names": distinct_names,
        "common_labels": common_labels,
        "common_annotations": common_annotations,
        "external_url": payload.get("externalURL"),
        "org_id": payload.get("orgId"),
        "title": payload.get("title"),
        "message": _truncate(payload.get("message"), 1000) if isinstance(payload.get("message"), str) else None,
        # Full per-alert detail preserved (per-alert fan-out is v2).
        "alerts": alerts,
    }

    return ObservationDraft(
        source_channel=_CHANNEL_ALERT,
        content_text=content_text,
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="state_change",
        source_actor_ref=None,
        external_id=external_id,
        entities_hint=entities,
        raw_payload=payload,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL_ANNOTATION, _TRUST)
CHANNEL_TRUST_MAP.setdefault(_CHANNEL_ALERT, _TRUST)


__all__ = ["handle_grafana_annotation", "handle_grafana_alert"]
