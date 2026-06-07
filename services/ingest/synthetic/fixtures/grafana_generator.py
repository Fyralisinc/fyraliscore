"""Grafana annotations fixture generator (IN-GRAFANA, X2/X3 infra).

`make_grafana(annotations=N, base_ms=..., per_page=P, ...)` produces a
deterministic set of Grafana annotation objects shaped to feed
`MockGrafanaClient`, which is in turn driven by the REAL backfill fetcher
(`services/ingest/ingestion/fetchers/grafana.py`).

The annotation shape mirrors the REAL `GET /api/annotations` array elements the
production fetcher consumes and the `grafana:annotation` handler normalises:
each element carries `id` (stable annotation id), `time` (epoch MILLISECONDS),
`text`, `tags`, `userId`/`userName`, and — for an alert-state-change annotation
— `alertId` / `newState` / `prevState` / `dashboardUID` / `panelId`. The fetcher
fans each annotation out 1:1 into one `_fyralis_record_type="annotation"` record,
so the per-install observation count is exactly `annotations`.

============================================================
NEWEST-FIRST ORDER (load-bearing)
============================================================
The real `/api/annotations` returns newest-first, and the fetcher's backward
walk relies on that (it lowers `to_ms` to `min(time seen) - 1` each page). This
generator emits the list **newest-first** (index 0 = newest), spacing each
annotation one minute APART going backward from `base_ms` so the resulting
`occurred_at` instants are distinct and well-ordered. `base_ms` defaults to
2026-01-05T00:00:00Z so every annotation's `occurred_at` lands in the 2026-01
observations partition window.

Determinism: every id / text / tag / user is derived from a SHA-256 of its
index, so a given call always yields byte-identical output.

============================================================
alert_webhook payload template (for the FUTURE live driver)
============================================================
`make_grafana` also returns an `alert_webhook` key: an Alertmanager-superset
contact-point delivery template the live `grafana:alert` driver can reuse later
(it is NOT consumed by the backfill mock client). See `_alert_webhook_template`.
"""
from __future__ import annotations

import hashlib
from typing import Any


_MS_PER_MINUTE = 60_000

# 2026-01-05T00:00:00Z in epoch milliseconds — keeps `occurred_at` in the
# 2026-01 observations partition window even after the backward 1-min spacing.
_DEFAULT_BASE_MS = 1767571200000


def make_grafana(
    *,
    annotations: int = 5,
    base_ms: int = _DEFAULT_BASE_MS,
    per_page: int = 100,
    include_alert_annotations: bool = True,
    base_url: str = "https://acme.grafana.net",
) -> dict[str, Any]:
    """Build a Grafana org fixture consumable by `MockGrafanaClient(fixture=...)`.

    Args:
      annotations: Number of annotation objects (== the 1:1 fan-out count).
      base_ms: epoch-ms timestamp of the NEWEST annotation; older annotations
        step 1 minute backward from here. Default lands in 2026-01.
      per_page: The mock client's per-page cap for `list_annotations` (mirrors
        the real `limit` default of 100). Drive backward-pagination by setting
        this below `annotations`.
      include_alert_annotations: When True, every 2nd annotation is an
        alert-state-change annotation (non-zero `alertId` + `newState`) so the
        handler's state_change path is exercised; the rest are plain (manual)
        annotations with a non-zero `userId` (actor path).
      base_url: The install's instance base URL; the fetcher derives the
        external_id `instance` host from it.

    Returns:
      A dict shaped exactly for `MockGrafanaClient`:
        {
          "base_url": "https://acme.grafana.net",
          "per_page": 100,
          # newest-first, like the real GET /api/annotations array.
          "annotations": [ <annotation dict>, ... ],
          # template only — for the future live grafana:alert driver.
          "alert_webhook": { ... },
        }
    """
    n = max(0, int(annotations))
    items: list[dict[str, Any]] = []
    for idx in range(n):
        # idx 0 == newest; step backward in time so the list is newest-first
        # and every `time` is distinct.
        time_ms = int(base_ms) - idx * _MS_PER_MINUTE
        is_alert = include_alert_annotations and (idx % 2 == 1)
        items.append(_annotation(idx, time_ms, is_alert=is_alert))

    return {
        "base_url": base_url,
        "per_page": int(per_page),
        "annotations": items,
        "alert_webhook": _alert_webhook_template(base_url),
    }


def _annotation(idx: int, time_ms: int, *, is_alert: bool) -> dict[str, Any]:
    """One `GET /api/annotations` array element (bare annotation object)."""
    digest = _digest(idx)
    # Grafana annotation ids are integers; keep them stable + distinct.
    ann_id = 1000 + idx
    tags = [f"env:{'prod' if idx % 2 == 0 else 'staging'}", f"team:{digest[:4]}"]

    if is_alert:
        # Alert-state-change annotation: Grafana auto-creates these with a
        # non-zero alertId + newState/prevState and NO user (machine actor).
        return {
            "id": ann_id,
            "alertId": 500 + idx,
            "dashboardUID": f"dash-{digest[:8]}",
            "panelId": idx + 1,
            "userId": 0,
            "userName": "",
            "newState": "Alerting" if idx % 4 == 1 else "OK",
            "prevState": "OK" if idx % 4 == 1 else "Alerting",
            "time": time_ms,
            "timeEnd": time_ms,
            "text": f"alert state change #{idx}",
            "tags": tags,
            "data": {},
        }

    # Plain (manual / deploy / region) annotation: carries a real user so the
    # handler's actor-resolution path is exercised.
    return {
        "id": ann_id,
        "alertId": 0,
        "dashboardUID": f"dash-{digest[:8]}",
        "panelId": idx + 1,
        "userId": 10 + idx,
        "userName": f"user-{digest[:6]}",
        "newState": "",
        "prevState": "",
        "time": time_ms,
        "timeEnd": 0,
        "text": f"deploy marker #{idx}",
        "tags": tags,
        "data": {},
    }


def _alert_webhook_template(base_url: str) -> dict[str, Any]:
    """Alertmanager-superset contact-point delivery template for the FUTURE
    live `grafana:alert` driver. NOT consumed by the backfill mock client —
    it documents the live-webhook payload shape so the live driver can reuse
    it. Mirrors what `handlers/grafana.py::handle_grafana_alert` reads:
    top-level `status` / `alerts` / `groupKey` / `commonLabels` /
    `commonAnnotations` / `externalURL` / `orgId` / `title` / `message`, with
    per-alert `labels` (incl. `alertname`) + `startsAt` / `endsAt`.
    """
    instance = base_url.replace("https://", "").replace("http://", "").rstrip("/")
    return {
        "status": "firing",
        "orgId": 1,
        "title": "[FIRING:1] HighErrorRate",
        "message": "1 alert is firing",
        "externalURL": base_url,
        "groupKey": '{}:{alertname="HighErrorRate"}',
        "commonLabels": {
            "alertname": "HighErrorRate",
            "service": "checkout",
            "namespace": "prod",
            "severity": "critical",
        },
        "commonAnnotations": {"summary": "Error rate above threshold"},
        "alerts": [
            {
                "status": "firing",
                "labels": {
                    "alertname": "HighErrorRate",
                    "service": "checkout",
                    "namespace": "prod",
                },
                "annotations": {"summary": "Error rate above threshold"},
                "startsAt": "2026-01-05T00:00:00.000Z",
                # Grafana's zero-value endsAt sentinel on a firing alert.
                "endsAt": "0001-01-01T00:00:00Z",
                "generatorURL": f"https://{instance}/alerting",
                "fingerprint": "deadbeefcafef00d",
            }
        ],
    }


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_grafana"]
