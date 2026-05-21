"""GmailPubSubGenerator — synthetic Pub/Sub notifications via FastAPI ASGI.

Per A23. Drives the Gmail live-ingestion path end-to-end in-process:

  Generator → httpx.AsyncClient(transport=ASGITransport(app))
            → POST /webhooks/gmail/pubsub
            → verify_pubsub_oidc_token (no-op'd in tests)
            → handle_push
            → _drain_history (monkeypatched to use mock client)
            → drain_mailbox_history (REAL — exercises the meaty path)
            → mock client's history_list + get_message
            → dispatch_gmail_message_resource → observations table write

What this exercises end-to-end:
  - FastAPI routing.
  - OIDC envelope validation surface (test-mode no-op'd; see audit).
  - Pub/Sub envelope decoding.
  - `gmail_pubsub_topics` tenant resolution.
  - `handle_push` rate-limit + Google-error branches.
  - `drain_mailbox_history` page-by-page fetch logic.
  - Per-message ingest → observations row write + thread canonicalization.

What this bypasses (deliberately):
  - DWD token minting (not part of the M6 chain logic).
  - Real Google httpx client (replaced by MockGmailClient).
  - Real OIDC certificate fetch (replaced by no-op verifier).

Usage (high-level):

    async with GmailPubSubGenerator(
        app=fastapi_app, pool=pool,
        mailboxes={"alice@x.com": mock_gmail_alice, ...},
    ) as gen:
        result = await gen.simulate_push(
            mailbox_email="alice@x.com", new_messages=3,
        )
        assert result.http_status == 200
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import random
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import httpx
from fastapi import FastAPI

from lib.shared.ids import uuid7
from services.synthetic.fixtures.gmail_generator import _digest
from services.synthetic.mock_clients import MockGmailClient
from services.synthetic.scenarios import (
    LivePubSubScenario,
    PerTenantBurst,
)


log = logging.getLogger(__name__)


# =====================================================================
# Result types.
# =====================================================================
@dataclass
class SimulatedPushResult:
    """One Pub/Sub simulation's outcome."""

    mailbox_email: str
    new_history_id: str
    http_status: int
    response_body: dict[str, Any] = field(default_factory=dict)
    was_replay: bool = False


@dataclass
class ScenarioResult:
    """Aggregate result for a `run_scenario` call."""

    pushes: list[SimulatedPushResult] = field(default_factory=list)
    duplicates_sent: int = 0
    wall_time_seconds: float = 0.0
    per_tenant_status_counts: dict[str, dict[int, int]] = field(
        default_factory=dict,
    )


# =====================================================================
# Per-mailbox state held by the generator.
# =====================================================================
@dataclass
class _MailboxBinding:
    """Wires a mailbox email to its mock client + DB install context.

    The generator's lookup table is keyed by `mailbox_email.lower()`;
    `handle_push` resolves subscription→tenant via the
    `gmail_pubsub_topics` table the generator seeded at setup.
    """

    tenant_id: UUID
    gmail_installation_id: UUID
    subscription_name: str
    mock_client: MockGmailClient


# =====================================================================
# Generator.
# =====================================================================
class GmailPubSubGenerator:
    """Synthetic Gmail Pub/Sub generator (Y1).

    Construct with a FastAPI app instance (Gmail Pub/Sub router
    mounted) and a `mailboxes` map of `{email: MockGmailClient}`. The
    generator handles the rest:
      - Seeds `gmail_pubsub_topics` + `gmail_installations` +
        `gmail_mailbox_watches` rows so `handle_push` resolves the
        tenant correctly.
      - Monkeypatches `verify_pubsub_oidc_token` + `_drain_history`
        so the request bypasses real OIDC + real Google client.
      - Builds standard Pub/Sub envelopes and POSTs them.

    Use as an async context manager: `async with gen as g: ...`.
    """

    def __init__(
        self,
        *,
        app: FastAPI,
        pool: asyncpg.Pool,
        mailboxes: dict[str, MockGmailClient],
        tenant_slugs: dict[str, str] | None = None,
        replay_probability: float = 0.0,
        rng_seed: int = 0,
    ) -> None:
        self._app = app
        self._pool = pool
        self._mock_clients = {
            email.lower(): client for email, client in mailboxes.items()
        }
        self._tenant_slugs = tenant_slugs or {}
        self._replay_probability = replay_probability
        self._rng = random.Random(rng_seed)
        self._bindings: dict[str, _MailboxBinding] = {}
        self._exit_stack = AsyncExitStack()
        self._original_verify: Any = None
        self._original_drain_history: Any = None
        self._original_global_pool: Any = None
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "GmailPubSubGenerator":
        await self._seed_db()
        self._install_patches()
        self._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self._app),
            base_url="http://x3-pubsub",
        )
        await self._exit_stack.enter_async_context(self._client)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self._uninstall_patches()
        await self._exit_stack.aclose()

    # ---- Setup ----
    async def _seed_db(self) -> None:
        """For each registered mailbox, ensure a tenants row +
        gmail_installations row + gmail_mailbox_watches row +
        gmail_pubsub_topics row exist. The handler reads from these
        to resolve subscription → tenant_id + email.

        REUSE (A30.1): if a `gmail_mailbox_watches` row already exists
        for the email — e.g. created by the X3 backfill harness — bind
        to its tenant_id + gmail_installation_id instead of minting a
        fresh install. This is what lets a live push share the SAME
        install as backfill, so the cross-path dedup twin's external_id
        (`gmail:{install}:{message_id}`) actually collides. If the
        existing watch has no pubsub-topic row yet (backfill doesn't
        create one), we add it so `handle_push` can resolve the
        subscription. With no existing watch the original
        create-fresh behaviour is preserved."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for email, client in self._mock_clients.items():
                    existing = await conn.fetchrow(
                        """
                        SELECT w.tenant_id,
                               w.gmail_installation_id,
                               t.subscription_name
                          FROM gmail_mailbox_watches w
                     LEFT JOIN gmail_pubsub_topics t
                            ON t.gmail_installation_id
                               = w.gmail_installation_id
                         WHERE lower(w.email_address) = $1
                         LIMIT 1
                        """,
                        email,
                    )
                    if existing is not None:
                        tenant_id = existing["tenant_id"]
                        install_id = existing["gmail_installation_id"]
                        sub_name = existing["subscription_name"]
                        if sub_name is None:
                            sub_name = (
                                f"projects/y1-test/subscriptions/"
                                f"gmail-{tenant_id.hex[:8]}-sub"
                            )
                            await conn.execute(
                                """
                                INSERT INTO gmail_pubsub_topics (
                                    id, tenant_id, gmail_installation_id,
                                    topic_name, subscription_name
                                ) VALUES ($1, $2, $3, $4, $5)
                                """,
                                uuid7(), tenant_id, install_id,
                                f"projects/y1-test/topics/"
                                f"gmail-{tenant_id.hex[:8]}",
                                sub_name,
                            )
                        self._bindings[email] = _MailboxBinding(
                            tenant_id=tenant_id,
                            gmail_installation_id=install_id,
                            subscription_name=sub_name,
                            mock_client=client,
                        )
                        continue

                    slug = self._tenant_slugs.get(email, f"y1-{email}")
                    tenant_id = uuid4()
                    await conn.execute(
                        "INSERT INTO tenants (id, name) VALUES ($1, $2) "
                        "ON CONFLICT (id) DO NOTHING",
                        tenant_id, slug,
                    )
                    install_id = uuid7()
                    await conn.execute(
                        """
                        INSERT INTO gmail_installations (
                            id, tenant_id, workspace_domain,
                            service_account_email, scope
                        ) VALUES ($1, $2, $3, $4, 'gmail.metadata')
                        """,
                        install_id, tenant_id,
                        email.split("@", 1)[1] if "@" in email else "x.com",
                        "sa@y1-test.iam.gserviceaccount.com",
                    )
                    await conn.execute(
                        """
                        INSERT INTO gmail_mailbox_watches (
                            id, tenant_id, gmail_installation_id,
                            email_address, history_id, state
                        ) VALUES ($1, $2, $3, $4, $5, 'active')
                        """,
                        uuid7(), tenant_id, install_id, email,
                        client._fixture["current_history_id"],
                    )
                    sub_name = (
                        f"projects/y1-test/subscriptions/"
                        f"gmail-{tenant_id.hex[:8]}-sub"
                    )
                    await conn.execute(
                        """
                        INSERT INTO gmail_pubsub_topics (
                            id, tenant_id, gmail_installation_id,
                            topic_name, subscription_name
                        ) VALUES ($1, $2, $3, $4, $5)
                        """,
                        uuid7(), tenant_id, install_id,
                        f"projects/y1-test/topics/gmail-{tenant_id.hex[:8]}",
                        sub_name,
                    )
                    self._bindings[email] = _MailboxBinding(
                        tenant_id=tenant_id,
                        gmail_installation_id=install_id,
                        subscription_name=sub_name,
                        mock_client=client,
                    )

    def _install_patches(self) -> None:
        """Install module-level patches for OIDC validation, the
        `_drain_history` helper, and the global db pool that
        `tenant_transaction` consults. Restored on context exit."""
        from services.webhooks import gmail_pubsub as webhook_mod
        from services.integrations.gmail import push_handler as ph_mod
        from lib.shared import db as db_mod

        self._original_verify = webhook_mod.verify_pubsub_oidc_token
        self._original_drain_history = ph_mod._drain_history
        self._original_global_pool = db_mod._pool

        async def _noop_verify(*args, **kwargs) -> None:
            return None

        async def _mock_drain_history(
            *, pool, tenant_id, gmail_installation_id, email_address,
        ):
            from services.integrations.gmail.fetcher import (
                drain_mailbox_history,
            )
            binding = self._bindings.get(email_address.lower())
            if binding is None:
                return {"status": "no_mock_for_email"}
            return await drain_mailbox_history(
                pool=pool,
                gmail=binding.mock_client,
                tenant_id=tenant_id,
                gmail_installation_id=gmail_installation_id,
                email_address=email_address,
                read_path="push",
            )

        webhook_mod.verify_pubsub_oidc_token = _noop_verify
        ph_mod._drain_history = _mock_drain_history
        # dispatch_gmail_message_resource uses tenant_transaction()
        # which calls get_pool() (module-level). Point it at our
        # test pool for the duration of the run.
        db_mod._pool = self._pool

    def _uninstall_patches(self) -> None:
        from services.webhooks import gmail_pubsub as webhook_mod
        from services.integrations.gmail import push_handler as ph_mod
        from lib.shared import db as db_mod

        if self._original_verify is not None:
            webhook_mod.verify_pubsub_oidc_token = self._original_verify
        if self._original_drain_history is not None:
            ph_mod._drain_history = self._original_drain_history
        # Restore global pool to whatever was there pre-run (often None).
        db_mod._pool = self._original_global_pool

    # ---- Single-push API ----
    async def simulate_push(
        self,
        *,
        mailbox_email: str,
        new_messages: int = 1,
        replay: bool = False,
        message_id: str | None = None,
        internal_date: str | None = None,
    ) -> SimulatedPushResult:
        """Append `new_messages` to the mock mailbox and dispatch
        a matching Pub/Sub notification. Returns the result.

        If `replay=True`, the message append is skipped and the
        previous-call's historyId is reused (simulates Pub/Sub's
        at-least-once-delivery duplicate).

        If `message_id` / `internal_date` are provided, the FIRST new
        message uses them instead of the auto-minted id/timestamp.
        Gmail's `external_id` is `gmail:{install}:{message_id}` and its
        `occurred_at` derives from `internalDate`, so injecting both —
        against the SAME install backfill used (see `_seed_db` reuse) —
        lets a caller dispatch a live message matching a backfilled one
        for the cross-path dedup twin (A30.2). `None` preserves
        auto-mint.
        """
        binding = self._bindings.get(mailbox_email.lower())
        if binding is None:
            raise ValueError(
                f"No mock client registered for {mailbox_email!r}; "
                f"register at construction time via mailboxes=...",
            )

        if not replay:
            new_msg_dicts = self._build_new_messages(
                mailbox_email, new_messages,
                message_id=message_id, internal_date=internal_date,
            )
            new_history_id = binding.mock_client.append_messages(
                new_msg_dicts,
            )
        else:
            new_history_id = binding.mock_client._fixture[
                "current_history_id"
            ]

        envelope = self._build_envelope(
            email=mailbox_email,
            history_id=new_history_id,
            subscription=binding.subscription_name,
        )
        assert self._client is not None
        response = await self._client.post(
            "/webhooks/gmail/pubsub",
            content=json.dumps(envelope).encode("utf-8"),
            headers={
                "Authorization": "Bearer y1-test-fake-jwt",
                "Content-Type": "application/json",
            },
        )
        body: dict[str, Any] = {}
        try:
            body = response.json()
        except Exception:  # noqa: BLE001
            body = {"raw": response.text[:500]}

        return SimulatedPushResult(
            mailbox_email=mailbox_email,
            new_history_id=new_history_id,
            http_status=response.status_code,
            response_body=body,
            was_replay=replay,
        )

    # ---- Scenario API ----
    async def run_scenario(
        self, scenario: LivePubSubScenario,
    ) -> ScenarioResult:
        """Execute a multi-tenant scenario. Per-tenant bursts run
        sequentially within a tenant; across tenants run concurrently
        via asyncio.gather.
        """
        start = time.monotonic()
        result = ScenarioResult()
        # Override replay_probability for this run.
        prev_replay_p = self._replay_probability
        self._replay_probability = scenario.replay_probability
        try:
            per_tenant_results = await asyncio.gather(*(
                self._run_one_tenant(tb, result)
                for tb in scenario.tenants
            ))
            for tenant_burst, pushes in zip(scenario.tenants,
                                            per_tenant_results):
                result.pushes.extend(pushes)
        finally:
            self._replay_probability = prev_replay_p

        result.wall_time_seconds = time.monotonic() - start
        for push in result.pushes:
            counts = result.per_tenant_status_counts.setdefault(
                push.mailbox_email, {},
            )
            counts[push.http_status] = counts.get(push.http_status, 0) + 1
        return result

    async def _run_one_tenant(
        self,
        tenant_burst: PerTenantBurst,
        agg: ScenarioResult,
    ) -> list[SimulatedPushResult]:
        pushes: list[SimulatedPushResult] = []
        for delay_ms, msg_count in tenant_burst.burst_pattern:
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)
            if msg_count <= 0:
                continue
            push = await self.simulate_push(
                mailbox_email=tenant_burst.mailbox_email,
                new_messages=msg_count,
            )
            pushes.append(push)
            # Maybe replay (at-least-once delivery simulation).
            if (self._replay_probability > 0.0
                    and self._rng.random() < self._replay_probability):
                replay_push = await self.simulate_push(
                    mailbox_email=tenant_burst.mailbox_email,
                    new_messages=0,
                    replay=True,
                )
                pushes.append(replay_push)
                agg.duplicates_sent += 1
        return pushes

    # ---- Helpers ----
    def _build_envelope(
        self, *, email: str, history_id: str, subscription: str,
    ) -> dict[str, Any]:
        """Standard Google Pub/Sub push envelope shape (matches
        production)."""
        inner = {"emailAddress": email, "historyId": history_id}
        return {
            "message": {
                "data": base64.b64encode(
                    json.dumps(inner).encode("utf-8"),
                ).decode("ascii"),
                "messageId": f"y1-msg-{uuid4().hex[:16]}",
                "publishTime": "2026-05-19T00:00:00Z",
            },
            "subscription": subscription,
        }

    def _build_new_messages(
        self, mailbox_email: str, count: int,
        *, message_id: str | None = None, internal_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """Construct `count` new message dicts shaped like Gmail API
        message resources, deterministic per mailbox + per-call
        sequence (uses time-based suffix so successive calls produce
        distinct ids without an explicit counter).

        When `message_id` / `internal_date` are given, the i==0 message
        uses them verbatim (the cross-path twin seam, A30.2). Note:
        Gmail's `external_id` derives from the RFC822 **Message-ID
        header** (not the resource id), so `message_id` overrides that
        header — the handler strips the angle brackets, yielding
        `gmail:{install}:{message_id}`."""
        out: list[dict[str, Any]] = []
        nonce = uuid4().hex[:6]
        for i in range(count):
            mid = f"msg-y1-{_digest(mailbox_email, nonce, i)[:14]}"
            tid = f"thr-y1-{_digest(mailbox_email, nonce, i // 3)[:14]}"
            idate = str(int(time.time() * 1000) + i)
            if i == 0 and internal_date is not None:
                idate = internal_date
            msgid_header = f"<y1-{nonce}-{i}@example.com>"
            if i == 0 and message_id is not None:
                msgid_header = f"<{message_id}>"
            out.append({
                "id": mid,
                "threadId": tid,
                "snippet": f"Y1 synthetic msg #{i}",
                "internalDate": idate,
                "historyId": "",  # filled by mock's append_messages
                "payload": {
                    "headers": [
                        {"name": "Message-ID",
                         "value": msgid_header},
                        {"name": "From",
                         "value": f"sender-{i}@example.com"},
                        {"name": "To", "value": mailbox_email},
                        {"name": "Subject",
                         "value": f"Y1 subject {nonce}-{i}"},
                        {"name": "Date",
                         "value": "Mon, 19 May 2026 00:00:00 +0000"},
                    ],
                    "body": {"size": 1024},
                },
            })
        return out
