"""SlackWebhookGenerator — synthetic Slack webhooks via FastAPI ASGI.

Per A25. Drives the Slack live-ingestion path end-to-end in-process:

  Generator → MockSlackClient fixture append (state coordination)
            → httpx.AsyncClient(transport=ASGITransport(app))
            → POST /webhooks/slack/events
            → signature verify (real HMAC-SHA256 v0 scheme)
            → tenant resolution (real, provider_installations by team_id)
            → inline ingest() → observations table write
              (or Kafka cutover path when the tenant flag is enabled)

What this exercises end-to-end:
  - FastAPI routing + body-size precheck.
  - Slack `v0` signature verification (real, against the configured
    signing secret).
  - Tenant resolution: `(provider='slack', team_id)` →
    `provider_installations` → tenant_id.
  - `slack:message` ingestion handler → observation write +
    `(source_channel, external_id, occurred_at)` UNIQUE dedup
    (external_id = `f"{channel}:{ts}"`).

What this bypasses (deliberately):
  - Real Slack Web API (the webhook ingest path never calls it; the
    mock-state coordination is for fidelity / downstream probes only).
  - Real secret store (uses the dev env-var signing secret, the same
    seam the IN-06/IN-08 webhook tests use).

Tenant binding (Z1.2): the driver targets a *seeded*
`provider_installations` row. It does NOT create installs — the caller
seeds `(provider='slack', installation_id=team_id, enabled=TRUE)` so
the production resolver maps the webhook to a real tenant.

Mock coordination (Z1.3): each dispatched message is first appended to
the mock Slack client's fixture (the `channels[].messages` list the
mock reads live), so a subsequent backfill/reconciler probe against the
mock sees the same message. The mock client is NOT modified — the
driver writes to the fixture data structure the mock already exposes.

Usage (high-level):

    async with SlackWebhookGenerator(
        app=fastapi_app, mock_client=mock_slack,
        signing_secret="test-secret",
    ) as gen:
        result = await gen.simulate_message(
            team_id="T0001", channel_id="C0001",
            content="shipped the fix",
        )
        assert result.http_status in (200, 201)
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import httpx
from fastapi import FastAPI

from services.synthetic.mock_clients import MockSlackClient
from services.synthetic.scenarios import LiveSlackScenario, SlackTenantTraffic


log = logging.getLogger(__name__)


# =====================================================================
# Result types.
# =====================================================================
@dataclass
class SimulatedWebhookResult:
    """One Slack webhook simulation's outcome."""

    team_id: str
    channel_id: str
    message_ts: str
    http_status: int
    response_body: dict[str, Any] = field(default_factory=dict)
    observation_id: str | None = None
    deduped: bool | None = None
    tenant_id: UUID | None = None
    was_replay: bool = False


@dataclass
class ScenarioResult:
    """Aggregate result for a `run_scenario` call."""

    results: list[SimulatedWebhookResult] = field(default_factory=list)
    duplicates_sent: int = 0
    wall_time_seconds: float = 0.0
    per_tenant_status_counts: dict[str, dict[int, int]] = field(
        default_factory=dict,
    )


# =====================================================================
# Generator.
# =====================================================================
class SlackWebhookGenerator:
    """Synthetic Slack webhook generator (Z1-slack).

    Construct with a FastAPI app (the gateway app with the webhook
    router mounted — build via `services.gateway.main.build_app`), the
    X2 mock Slack client, and the signing secret the app is configured
    with. Use as an async context manager.
    """

    def __init__(
        self,
        *,
        app: FastAPI,
        mock_client: MockSlackClient,
        signing_secret: str | None = None,
        replay_probability: float = 0.0,
        rng_seed: int = 0,
    ) -> None:
        self._app = app
        self._mock = mock_client
        self._secret = (
            signing_secret
            if signing_secret is not None
            else os.environ.get("WEBHOOK_SECRET_SLACK", "")
        )
        self._replay_probability = replay_probability
        self._rng = random.Random(rng_seed)
        self._exit_stack = AsyncExitStack()
        self._client: httpx.AsyncClient | None = None
        self._ts_counter = 0
        self._last_ts_by_channel: dict[str, str] = {}

    async def __aenter__(self) -> "SlackWebhookGenerator":
        self._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self._app),
            base_url="http://z1-slack",
        )
        await self._exit_stack.enter_async_context(self._client)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._exit_stack.aclose()

    # ---- Single-message API ----
    async def simulate_message(
        self,
        *,
        team_id: str,
        channel_id: str,
        content: str = "hello",
        tenant_id: UUID | None = None,
        user_id: str | None = None,
        replay: bool = False,
        tamper_signature: bool = False,
    ) -> SimulatedWebhookResult:
        """Append a message to the mock Slack state and dispatch a
        matching `event_callback` webhook. Returns the outcome.

        If `replay=True`, reuse the channel's previous `ts` (no new mock
        append) — simulates Slack's at-least-once redelivery; the
        observation layer must dedup it.

        If `tamper_signature=True`, send a deliberately wrong signature
        (negative test — expect 401, no observation).
        """
        assert self._client is not None
        user = user_id or f"U_{channel_id}"

        if replay and channel_id in self._last_ts_by_channel:
            ts = self._last_ts_by_channel[channel_id]
        else:
            ts = self._next_ts()
            self._append_to_mock(channel_id, ts=ts, user=user, text=content)
            self._last_ts_by_channel[channel_id] = ts

        payload = {
            "type": "event_callback",
            "team_id": team_id,
            "event": {
                "type": "message",
                "channel": channel_id,
                "user": user,
                "text": content,
                "ts": ts,
            },
        }
        body = json.dumps(payload).encode("utf-8")
        req_ts = str(int(time.time()))
        signature = (
            "v0=" + ("f" * 64)
            if tamper_signature
            else self._sign(req_ts, body)
        )

        response = await self._client.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Slack-Request-Timestamp": req_ts,
                "X-Slack-Signature": signature,
            },
        )
        resp_body = self._safe_json(response)
        return SimulatedWebhookResult(
            team_id=team_id,
            channel_id=channel_id,
            message_ts=ts,
            http_status=response.status_code,
            response_body=resp_body,
            observation_id=resp_body.get("observation_id"),
            deduped=resp_body.get("deduped"),
            tenant_id=tenant_id,
            was_replay=replay,
        )

    async def simulate_url_verification(
        self, *, challenge: str = "z1-challenge-token",
    ) -> SimulatedWebhookResult:
        """Dispatch Slack's one-time `url_verification` handshake. The
        router echoes the challenge with HTTP 200 after signature
        verification (no tenant named, no observation)."""
        assert self._client is not None
        payload = {"type": "url_verification", "challenge": challenge}
        body = json.dumps(payload).encode("utf-8")
        req_ts = str(int(time.time()))
        response = await self._client.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Slack-Request-Timestamp": req_ts,
                "X-Slack-Signature": self._sign(req_ts, body),
            },
        )
        resp_body = self._safe_json(response)
        return SimulatedWebhookResult(
            team_id="",
            channel_id="",
            message_ts="",
            http_status=response.status_code,
            response_body=resp_body,
        )

    # ---- Scenario API ----
    async def run_scenario(
        self, scenario: LiveSlackScenario,
    ) -> ScenarioResult:
        """Execute a multi-tenant scenario. Within a tenant, messages
        fire sequentially with configured delays; across tenants they
        run concurrently via asyncio.gather."""
        start = time.monotonic()
        result = ScenarioResult()
        prev_replay_p = self._replay_probability
        self._replay_probability = scenario.replay_probability
        try:
            per_tenant = await asyncio.gather(*(
                self._run_one_tenant(t, result) for t in scenario.tenants
            ))
            for chunk in per_tenant:
                result.results.extend(chunk)
        finally:
            self._replay_probability = prev_replay_p

        result.wall_time_seconds = time.monotonic() - start
        for r in result.results:
            counts = result.per_tenant_status_counts.setdefault(
                r.team_id, {},
            )
            counts[r.http_status] = counts.get(r.http_status, 0) + 1
        return result

    async def _run_one_tenant(
        self, traffic: SlackTenantTraffic, agg: ScenarioResult,
    ) -> list[SimulatedWebhookResult]:
        out: list[SimulatedWebhookResult] = []
        for delay_ms, count in traffic.message_pattern:
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)
            for _ in range(max(0, count)):
                r = await self.simulate_message(
                    team_id=traffic.team_id,
                    channel_id=traffic.channel_id,
                    content=f"z1-slack {traffic.tenant_slug}",
                )
                out.append(r)
                if (self._replay_probability > 0.0
                        and self._rng.random() < self._replay_probability):
                    replay = await self.simulate_message(
                        team_id=traffic.team_id,
                        channel_id=traffic.channel_id,
                        content=f"z1-slack {traffic.tenant_slug}",
                        replay=True,
                    )
                    out.append(replay)
                    agg.duplicates_sent += 1
        return out

    # ---- Helpers ----
    def _sign(self, req_ts: str, body: bytes) -> str:
        basestring = f"v0:{req_ts}:".encode("utf-8") + body
        digest = hmac.new(
            self._secret.encode("utf-8"), basestring, hashlib.sha256,
        ).hexdigest()
        return f"v0={digest}"

    def _next_ts(self) -> str:
        """Monotonic, unique Slack `ts` (`{epoch}.{seq}`). Uniqueness
        is what makes each message a distinct observation; replay reuses
        a prior ts to exercise dedup."""
        self._ts_counter += 1
        return f"{int(time.time())}.{self._ts_counter:06d}"

    def _append_to_mock(
        self, channel_id: str, *, ts: str, user: str, text: str,
    ) -> None:
        """Append the message to the mock Slack fixture so the mock's
        `conversations_history` would surface it. Writes to the data
        structure the mock reads (`_fixture['channels'][*]['messages']`)
        — the mock library itself is unchanged. Creates the channel
        entry if the fixture didn't declare it."""
        channels = self._mock._fixture.setdefault("channels", [])
        for c in channels:
            if c.get("id") == channel_id:
                c.setdefault("messages", []).append(
                    {"ts": ts, "user": user, "text": text},
                )
                return
        channels.append({
            "id": channel_id,
            "name": channel_id,
            "team_id": self._mock._fixture.get("team_id"),
            "messages": [{"ts": ts, "user": user, "text": text}],
        })

    @staticmethod
    def _safe_json(response: httpx.Response) -> dict[str, Any]:
        try:
            data = response.json()
            return data if isinstance(data, dict) else {"raw": data}
        except Exception:  # noqa: BLE001
            return {"raw": response.text[:500]}
