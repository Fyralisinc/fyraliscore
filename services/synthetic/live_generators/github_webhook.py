"""GithubWebhookGenerator — synthetic GitHub webhooks via FastAPI ASGI.

Per A25. Drives the GitHub live-ingestion path end-to-end in-process:

  Generator → MockGithubClient fixture append (state coordination)
            → httpx.AsyncClient(transport=ASGITransport(app))
            → POST /webhooks/github/events
            → signature verify (real HMAC-SHA256, X-Hub-Signature-256)
            → tenant resolution (real, provider_installations by
              installation.id)
            → inline ingest() → observations table write
              (or Kafka cutover path when the tenant flag is enabled)

What this exercises end-to-end:
  - FastAPI routing + body-size precheck.
  - GitHub `sha256=` signature verification (real, App-level secret).
  - Replay-cache short-circuit (router drops a re-delivered
    `(installation_id, delivery_id)` with HTTP 200 `handled:replay`).
  - Tenant resolution: `installation.id` → `provider_installations` →
    tenant_id.
  - `selected_repositories` repo filter (NULL = all repos).
  - `github:webhook` ingestion handler (issues / pull_request shapers)
    → observation write + `(source_channel, external_id, occurred_at)`
    UNIQUE dedup (external_id = the event's `node_id`).

Tenant binding (Z1.2): the driver targets a *seeded*
`provider_installations` row (`provider='github'`,
`installation_id=<id>`). It does NOT create installs.

Mock coordination (Z1.3): each dispatched event is first appended to
the mock GitHub client's fixture (`repos[].events_by_type[*]`), so a
subsequent backfill/reconciler probe against the mock sees it. The mock
library is NOT modified — the driver writes the fixture data structure
the mock already exposes.

Usage (high-level):

    async with GithubWebhookGenerator(
        app=fastapi_app, mock_client=mock_github,
        signing_secret="test-secret",
    ) as gen:
        result = await gen.simulate_issue_event(
            installation_id="999001", repo_full_name="octo/repo",
            action="opened", issue_title="bug: rate limiter",
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
from uuid import UUID, uuid4

import httpx
from fastapi import FastAPI

from services.synthetic.mock_clients import MockGithubClient
from services.synthetic.scenarios import (
    GithubTenantTraffic,
    LiveGithubScenario,
)


log = logging.getLogger(__name__)


# =====================================================================
# Result types.
# =====================================================================
@dataclass
class GithubWebhookResult:
    """One GitHub webhook simulation's outcome."""

    event_type: str
    installation_id: str
    repo_full_name: str
    node_id: str
    delivery_id: str
    http_status: int
    response_body: dict[str, Any] = field(default_factory=dict)
    observation_id: str | None = None
    deduped: bool | None = None
    tenant_id: UUID | None = None
    was_replay: bool = False


@dataclass
class GithubScenarioResult:
    """Aggregate result for a `run_scenario` call."""

    results: list[GithubWebhookResult] = field(default_factory=list)
    duplicates_sent: int = 0
    wall_time_seconds: float = 0.0
    per_installation_status_counts: dict[str, dict[int, int]] = field(
        default_factory=dict,
    )


# =====================================================================
# Generator.
# =====================================================================
class GithubWebhookGenerator:
    """Synthetic GitHub webhook generator (Z1-github).

    Construct with a FastAPI app (the gateway app — build via
    `services.gateway.main.build_app`), the X2 mock GitHub client, and
    the App-level webhook secret the app is configured with
    (`WEBHOOK_SECRET_GITHUB`). Use as an async context manager.
    """

    def __init__(
        self,
        *,
        app: FastAPI,
        mock_client: MockGithubClient,
        signing_secret: str | None = None,
        replay_probability: float = 0.0,
        rng_seed: int = 0,
    ) -> None:
        self._app = app
        self._mock = mock_client
        self._secret = (
            signing_secret
            if signing_secret is not None
            else os.environ.get("WEBHOOK_SECRET_GITHUB", "")
        )
        self._replay_probability = replay_probability
        self._rng = random.Random(rng_seed)
        self._exit_stack = AsyncExitStack()
        self._client: httpx.AsyncClient | None = None
        self._seq = 0
        # Last-dispatched request per (repo, event_type) for replay.
        self._last: dict[tuple[str, str], tuple[dict[str, Any], str]] = {}

    async def __aenter__(self) -> "GithubWebhookGenerator":
        self._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self._app),
            base_url="http://z1-github",
        )
        await self._exit_stack.enter_async_context(self._client)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._exit_stack.aclose()

    # ---- Single-event API ----
    async def simulate_issue_event(
        self,
        *,
        installation_id: str,
        repo_full_name: str,
        action: str = "opened",
        issue_title: str = "synthetic issue",
        tenant_id: UUID | None = None,
        sender_login: str = "octocat",
        replay: bool = False,
        tamper_signature: bool = False,
    ) -> GithubWebhookResult:
        """Append an issue to the mock and dispatch an `issues` webhook.

        `replay=True` re-dispatches the previous issue event for this
        repo (same delivery id + node_id) — exercises the router replay
        cache + observation dedup."""
        if replay:
            return await self._replay("issues", repo_full_name,
                                      installation_id, tenant_id,
                                      tamper_signature)

        self._seq += 1
        number = self._seq
        node_id = f"I_{installation_id}_{self._seq}"
        ts = "2026-05-19T00:00:00Z"
        self._append_to_mock(
            repo_full_name, "issues",
            {"id": node_id, "number": number, "title": issue_title,
             "state": "open", "updated_at": ts},
        )
        payload = {
            "action": action,
            "issue": {
                "number": number,
                "title": issue_title,
                "node_id": node_id,
                "created_at": ts,
                "updated_at": ts,
            },
            "repository": {"full_name": repo_full_name},
            "installation": {"id": int(installation_id)
                             if installation_id.isdigit()
                             else installation_id},
            "sender": {"login": sender_login},
        }
        return await self._dispatch(
            "issues", payload, repo_full_name, installation_id,
            node_id, tenant_id, tamper_signature, was_replay=False,
        )

    async def simulate_pull_request_event(
        self,
        *,
        installation_id: str,
        repo_full_name: str,
        action: str = "opened",
        pr_title: str = "synthetic PR",
        base_ref: str = "main",
        merged: bool = False,
        tenant_id: UUID | None = None,
        sender_login: str = "octocat",
        replay: bool = False,
        tamper_signature: bool = False,
    ) -> GithubWebhookResult:
        """Append a PR to the mock and dispatch a `pull_request`
        webhook."""
        if replay:
            return await self._replay("pull_request", repo_full_name,
                                      installation_id, tenant_id,
                                      tamper_signature)

        self._seq += 1
        number = self._seq
        node_id = f"PR_{installation_id}_{self._seq}"
        ts = "2026-05-19T00:00:00Z"
        self._append_to_mock(
            repo_full_name, "pull_requests",
            {"id": node_id, "number": number, "title": pr_title,
             "state": "open", "updated_at": ts},
        )
        payload = {
            "action": action,
            "pull_request": {
                "number": number,
                "title": pr_title,
                "node_id": node_id,
                "merged": merged,
                "base": {"ref": base_ref},
                "created_at": ts,
                "updated_at": ts,
            },
            "repository": {"full_name": repo_full_name},
            "installation": {"id": int(installation_id)
                             if installation_id.isdigit()
                             else installation_id},
            "sender": {"login": sender_login},
        }
        return await self._dispatch(
            "pull_request", payload, repo_full_name, installation_id,
            node_id, tenant_id, tamper_signature, was_replay=False,
        )

    # ---- Scenario API ----
    async def run_scenario(
        self, scenario: LiveGithubScenario,
    ) -> GithubScenarioResult:
        """Execute a multi-tenant scenario. Within a tenant, events fire
        sequentially with configured delays; across tenants they run
        concurrently via asyncio.gather."""
        start = time.monotonic()
        result = GithubScenarioResult()
        prev_replay_p = self._replay_probability
        self._replay_probability = scenario.replay_probability
        try:
            per_tenant = await asyncio.gather(*(
                self._run_one_tenant(t, scenario.event_type, result)
                for t in scenario.tenants
            ))
            for chunk in per_tenant:
                result.results.extend(chunk)
        finally:
            self._replay_probability = prev_replay_p

        result.wall_time_seconds = time.monotonic() - start
        for r in result.results:
            counts = result.per_installation_status_counts.setdefault(
                r.installation_id, {},
            )
            counts[r.http_status] = counts.get(r.http_status, 0) + 1
        return result

    async def _run_one_tenant(
        self,
        traffic: GithubTenantTraffic,
        event_type: str,
        agg: GithubScenarioResult,
    ) -> list[GithubWebhookResult]:
        out: list[GithubWebhookResult] = []
        emit = (
            self.simulate_pull_request_event
            if event_type == "pull_request"
            else self.simulate_issue_event
        )
        for delay_ms, count in traffic.event_pattern:
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)
            for _ in range(max(0, count)):
                r = await emit(
                    installation_id=traffic.installation_id,
                    repo_full_name=traffic.repo_full_name,
                )
                out.append(r)
                if (self._replay_probability > 0.0
                        and self._rng.random() < self._replay_probability):
                    replay = await emit(
                        installation_id=traffic.installation_id,
                        repo_full_name=traffic.repo_full_name,
                        replay=True,
                    )
                    out.append(replay)
                    agg.duplicates_sent += 1
        return out

    # ---- Dispatch core ----
    async def _dispatch(
        self,
        event_type: str,
        payload: dict[str, Any],
        repo_full_name: str,
        installation_id: str,
        node_id: str,
        tenant_id: UUID | None,
        tamper_signature: bool,
        *,
        was_replay: bool,
        delivery_id: str | None = None,
    ) -> GithubWebhookResult:
        assert self._client is not None
        body = json.dumps(payload).encode("utf-8")
        delivery = delivery_id or uuid4().hex
        signature = (
            "sha256=" + ("f" * 64)
            if tamper_signature
            else self._sign(body)
        )
        if not was_replay:
            self._last[(repo_full_name, event_type)] = (payload, delivery)

        response = await self._client.post(
            "/webhooks/github/events",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-GitHub-Event": event_type,
                "X-GitHub-Delivery": delivery,
                "X-Hub-Signature-256": signature,
            },
        )
        resp_body = self._safe_json(response)
        return GithubWebhookResult(
            event_type=event_type,
            installation_id=installation_id,
            repo_full_name=repo_full_name,
            node_id=node_id,
            delivery_id=delivery,
            http_status=response.status_code,
            response_body=resp_body,
            observation_id=resp_body.get("observation_id"),
            deduped=resp_body.get("deduped"),
            tenant_id=tenant_id,
            was_replay=was_replay,
        )

    async def _replay(
        self,
        event_type: str,
        repo_full_name: str,
        installation_id: str,
        tenant_id: UUID | None,
        tamper_signature: bool,
    ) -> GithubWebhookResult:
        key = (repo_full_name, event_type)
        if key not in self._last:
            raise ValueError(
                f"no prior {event_type} event to replay for "
                f"{repo_full_name!r}",
            )
        payload, delivery = self._last[key]
        node_id = self._node_id_of(event_type, payload)
        return await self._dispatch(
            event_type, payload, repo_full_name, installation_id,
            node_id, tenant_id, tamper_signature,
            was_replay=True, delivery_id=delivery,
        )

    # ---- Helpers ----
    def _sign(self, body: bytes) -> str:
        digest = hmac.new(
            self._secret.encode("utf-8"), body, hashlib.sha256,
        ).hexdigest()
        return f"sha256={digest}"

    @staticmethod
    def _node_id_of(event_type: str, payload: dict[str, Any]) -> str:
        obj_key = "issue" if event_type == "issues" else "pull_request"
        obj = payload.get(obj_key) or {}
        return str(obj.get("node_id", ""))

    def _append_to_mock(
        self, repo_full_name: str, event_key: str, event: dict[str, Any],
    ) -> None:
        """Append the event to the mock GitHub fixture so the mock's
        `list_repo_events` would surface it. Writes the data structure
        the mock reads (`_fixture['repos'][*]['events_by_type']`) — the
        mock library itself is unchanged. Creates the repo / event-type
        bucket if the fixture didn't declare them."""
        repos = self._mock._fixture.setdefault("repos", [])
        for r in repos:
            if r.get("full_name") == repo_full_name:
                r.setdefault("events_by_type", {}).setdefault(
                    event_key, [],
                ).append(event)
                return
        repos.append({
            "full_name": repo_full_name,
            "events_by_type": {event_key: [event]},
        })

    @staticmethod
    def _safe_json(response: httpx.Response) -> dict[str, Any]:
        try:
            data = response.json()
            return data if isinstance(data, dict) else {"raw": data}
        except Exception:  # noqa: BLE001
            return {"raw": response.text[:500]}
