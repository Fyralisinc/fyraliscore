"""services/ingestion/alerts.py — shared best-effort ops-alert sink.

The ingestion services need to surface operational events (circuit
breaker trips, DLQ backlog growth, …) to an on-call channel. The
contract is deliberately minimal and uniform:

  - One env var, `INGESTION_ALERT_WEBHOOK_URL`, points at a generic
    incoming webhook (Slack / PagerDuty Events / a custom relay).
  - Each alert is a JSON POST `{"event": <name>, ...payload}`.
  - The POST is fire-and-forget: a 5s timeout, and any failure is
    logged but never raised. Alerting must NEVER perturb the loop it
    is reporting on — a flaky webhook can't be allowed to crash a
    consumer or stall a tick.
  - When the env var is unset/empty the call is a no-op (so local
    runs and tests need no webhook).

This generalises the pattern that lived inline in
`feature_flags/circuit_breaker._default_alert` so every emitter
(breaker, DLQ-depth monitor, …) routes through one place.
"""
from __future__ import annotations

import logging
import os
from typing import Any


log = logging.getLogger(__name__)


ALERT_WEBHOOK_ENV = "INGESTION_ALERT_WEBHOOK_URL"


def alert_webhook_configured() -> bool:
    """True iff an alert webhook URL is configured. Lets callers skip
    building a payload when nothing would be sent."""
    return bool(os.environ.get(ALERT_WEBHOOK_ENV, "").strip())


async def send_ops_alert(event: str, payload: dict[str, Any]) -> bool:
    """Best-effort POST of `{"event": event, **payload}` to the ops
    webhook. Returns True iff a POST was actually attempted and did
    not raise; False if no webhook is configured or the POST failed.
    Never raises — alerting is strictly side-channel."""
    webhook = os.environ.get(ALERT_WEBHOOK_ENV, "").strip()
    if not webhook:
        return False
    try:
        import httpx

        body = {"event": event, **payload}
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(webhook, json=body)
        return True
    except Exception as exc:  # noqa: BLE001 — alerting is best-effort
        log.warning(
            "ingestion.alert_webhook_failed",
            extra={"event": event, "error": str(exc)[:200]},
        )
        return False


__all__ = [
    "ALERT_WEBHOOK_ENV",
    "alert_webhook_configured",
    "send_ops_alert",
]
