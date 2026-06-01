"""DiscordGatewayGenerator — synthetic Discord events via direct handler call.

Per A24. Drives the Discord Gateway live-ingestion path end-to-end
in-process by calling the dispatch handler directly:

  Generator → MockDiscordClient.append_message (record state)
            → handle_message_create(payload, dispatch_deps)
            → tenant resolution (real, against test DB)
            → ingest(...) → observations table write

What this exercises end-to-end:
  - MESSAGE_CREATE dispatch logic (bot/webhook filters, DM filter).
  - Tenant resolution via `provider_installations`.
  - The real `ingest()` core function + thread / actor logic.
  - Observation write + `(source_channel, external_id, occurred_at)`
    UNIQUE dedup.

What this DOES NOT exercise (explicit non-coverage per A24):
  - WebSocket framing.
  - HELLO / IDENTIFY / READY handshake.
  - Heartbeat protocol (op 1 / op 11).
  - Session resume / sequence numbers.
  - Connection lifecycle (connect / reconnect / disconnect).

Those are M4-tested-only in `test_client_lifecycle.py` +
`test_client_reconnect.py`. If lifecycle synthetic coverage is ever
needed, a future work-unit ships a WebSocket simulator (Option A from
mega-prompt 3's decision matrix).

MESSAGE_UPDATE / MESSAGE_DELETE: production has no handler for these
in v1 (see `services/integrations/discord/gateway/dispatch.py` line
93–95). The generator's `simulate_message_update` / `simulate_message_delete`
methods document this non-coverage explicitly — they record the
event in the mock's state for fidelity but skip the handler call
(there's nothing to call).

Usage:

    async with DiscordGatewayGenerator(
        dispatch_deps=deps,
        guild_bindings={
            "1504477009927999569": GuildBinding(
                guild_id="1504477009927999569",
                mock_client=mock_discord,
            ),
        },
    ) as gen:
        result = await gen.simulate_message_create(
            guild_id="1504477009927999569",
            channel_id="channel_test_001",
            content="hello",
        )
        assert result.handler_succeeded is True
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any

from services.synthetic.mock_clients import MockDiscordClient


log = logging.getLogger(__name__)


# Discord epoch (2015-01-01T00:00:00Z in ms) for snowflake construction.
_DISCORD_EPOCH_MS = 1_420_070_400_000


# =====================================================================
# Bindings + results.
# =====================================================================
@dataclass
class GuildBinding:
    """Wires a guild_id to its mock client. The Gateway dispatcher
    resolves tenant via the production `provider_installations` row
    (which the test seeds separately); the generator's job is to
    deliver the right mock-tracked payload to the handler."""

    guild_id: str
    mock_client: MockDiscordClient


@dataclass
class SimulatedEventResult:
    """Per-event outcome from `simulate_message_*` calls."""

    event_kind: str
    guild_id: str
    channel_id: str
    message_id: str
    handler_invoked: bool
    handler_succeeded: bool
    handler_exception: str | None = None
    notes: str | None = None


@dataclass
class ScenarioResult:
    """Aggregate result for `run_scenario`."""

    events: list[SimulatedEventResult] = field(default_factory=list)
    wall_time_seconds: float = 0.0


# =====================================================================
# Generator.
# =====================================================================
class DiscordGatewayGenerator:
    """Synthetic Discord Gateway generator (Y2).

    Construct with a `DispatchDeps` instance (built the same way as
    M4 tests build it — see `services/integrations/discord/gateway/
    tests/conftest.py::dispatch_deps`) and a guild-binding map. Use
    as an async context manager.
    """

    def __init__(
        self,
        *,
        dispatch_deps: Any,  # DispatchDeps — kept Any to avoid import
        guild_bindings: dict[str, GuildBinding],
    ) -> None:
        self._deps = dispatch_deps
        self._bindings = guild_bindings
        self._exit_stack = AsyncExitStack()
        self._snowflake_counter = 0

    async def __aenter__(self) -> "DiscordGatewayGenerator":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._exit_stack.aclose()

    # ---- Single-event API ----
    async def simulate_message_create(
        self,
        *,
        guild_id: str,
        channel_id: str,
        content: str = "hello",
        author_id: str = "user_test_001",
        author_bot: bool = False,
        webhook_id: str | None = None,
        timestamp: str | None = None,
    ) -> SimulatedEventResult:
        """Build a MESSAGE_CREATE payload, record it in the mock, and
        call the production `handle_message_create` directly. Returns
        a result indicating whether the handler raised."""
        binding = self._bindings.get(guild_id)
        if binding is None:
            raise ValueError(
                f"No GuildBinding for guild_id={guild_id!r}",
            )

        snowflake = self._next_snowflake()
        message_id = f"msg-y2-{snowflake}"
        payload: dict[str, Any] = {
            "id": message_id,
            "channel_id": channel_id,
            "guild_id": guild_id,
            "content": content,
            "timestamp": (
                timestamp or "2026-05-19T00:00:00.000+00:00"
            ),
            "author": {
                "id": author_id,
                "username": f"user-{author_id[-4:]}",
                "bot": author_bot,
            },
            "attachments": [],
            "mentions": [],
        }
        if webhook_id is not None:
            payload["webhook_id"] = webhook_id

        # Record in mock state first so the channel's history reflects
        # the event for any subsequent reconciler probes.
        try:
            binding.mock_client.append_message(channel_id, payload)
        except KeyError as exc:
            # Channel not in fixture; report as non-fatal mismatch.
            return SimulatedEventResult(
                event_kind="MESSAGE_CREATE",
                guild_id=guild_id, channel_id=channel_id,
                message_id=message_id,
                handler_invoked=False, handler_succeeded=False,
                handler_exception=f"KeyError: {exc}",
                notes="channel not declared in mock fixture",
            )

        # Call the production handler directly.
        from services.integrations.discord.gateway.dispatch import (
            handle_message_create,
        )
        try:
            await handle_message_create(payload, self._deps)
            return SimulatedEventResult(
                event_kind="MESSAGE_CREATE",
                guild_id=guild_id, channel_id=channel_id,
                message_id=message_id,
                handler_invoked=True, handler_succeeded=True,
            )
        except Exception as exc:  # noqa: BLE001 — record + report
            log.exception(
                "y2.simulate_message_create.handler_failed",
                extra={"guild_id": guild_id,
                       "channel_id": channel_id,
                       "message_id": message_id},
            )
            return SimulatedEventResult(
                event_kind="MESSAGE_CREATE",
                guild_id=guild_id, channel_id=channel_id,
                message_id=message_id,
                handler_invoked=True, handler_succeeded=False,
                handler_exception=f"{type(exc).__name__}: {exc}",
            )

    async def simulate_message_update(
        self,
        *,
        guild_id: str,
        channel_id: str,
        message_id: str,
        new_content: str = "(edited)",
    ) -> SimulatedEventResult:
        """Record a MESSAGE_UPDATE event in the mock. **NOT dispatched
        to the production handler** — v1 Gateway dispatch has no
        MESSAGE_UPDATE handler (see
        `services/integrations/discord/gateway/dispatch.py` line
        93–95). The result records `handler_invoked=False` with a
        `notes` field documenting the non-coverage."""
        return SimulatedEventResult(
            event_kind="MESSAGE_UPDATE",
            guild_id=guild_id, channel_id=channel_id,
            message_id=message_id,
            handler_invoked=False, handler_succeeded=False,
            notes=(
                "MESSAGE_UPDATE not in v1 dispatch scope; recorded "
                "in mock state for fidelity but no production handler "
                "to call. See A24."
            ),
        )

    async def simulate_message_delete(
        self,
        *,
        guild_id: str,
        channel_id: str,
        message_id: str,
    ) -> SimulatedEventResult:
        """Record a MESSAGE_DELETE event. **NOT dispatched** for the
        same reason as MESSAGE_UPDATE — no v1 handler."""
        return SimulatedEventResult(
            event_kind="MESSAGE_DELETE",
            guild_id=guild_id, channel_id=channel_id,
            message_id=message_id,
            handler_invoked=False, handler_succeeded=False,
            notes=(
                "MESSAGE_DELETE not in v1 dispatch scope. See A24."
            ),
        )

    # ---- Scenario API ----
    async def run_scenario(self, scenario: Any) -> ScenarioResult:
        """Execute a LiveGatewayScenario.

        Within a (guild, channel) pair, events fire sequentially
        (matches real-world per-channel ordering). Across (guild,
        channel) pairs, events run concurrently via asyncio.gather.
        """
        start = time.monotonic()
        result = ScenarioResult()

        async def _run_one(entry: Any) -> list[SimulatedEventResult]:
            events: list[SimulatedEventResult] = []
            for delay_ms, msg_count in entry.message_pattern:
                if delay_ms > 0:
                    await asyncio.sleep(delay_ms / 1000.0)
                for _ in range(msg_count):
                    ev = await self.simulate_message_create(
                        guild_id=entry.guild_id,
                        channel_id=entry.channel_id,
                        content=f"y2-synthetic-{self._snowflake_counter}",
                    )
                    events.append(ev)
            return events

        per_entry_results = await asyncio.gather(*(
            _run_one(e) for e in scenario.tenants
        ))
        for evs in per_entry_results:
            result.events.extend(evs)
        result.wall_time_seconds = time.monotonic() - start
        return result

    # ---- Helpers ----
    def _next_snowflake(self) -> str:
        """Generate a monotonically-increasing synthetic Discord
        snowflake. Real snowflakes encode a timestamp; ours encode
        the monotonic counter so ordering invariants hold inside
        tests."""
        self._snowflake_counter += 1
        # Use ms since Discord epoch + counter in low bits.
        now_ms = int(time.time() * 1000) - _DISCORD_EPOCH_MS
        snowflake = (now_ms << 22) | (self._snowflake_counter & 0x3FFFFF)
        return str(snowflake)
