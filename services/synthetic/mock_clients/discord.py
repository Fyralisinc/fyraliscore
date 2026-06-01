"""MockDiscordClient — Discord REST surface used by M6.6 backfill.

Implements the methods M6.6 planner/fetcher/reconciler call:
  - list_guilds() -> list[dict]
  - list_guild_channels(guild_id) -> list[dict]
  - get_messages(channel_id, before=None, after=None, limit=None)
    -> list[dict]

Stateful per channel: paginates messages via snowflake IDs. `before`
returns messages with id < before (older direction; backfill uses
this). `after` returns messages with id > after (gap-detection probe;
reconciler uses this).
"""
from __future__ import annotations

from typing import Any, NoReturn

from lib.shared.errors import DiscordApiError
from services.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.synthetic.mock_clients._base import _MockBase


class MockDiscordClient(_MockBase):
    """Stateful in-process replacement for `DiscordClient`.

    `fixture` shape (per `make_discord_guild`):
        {
          "guild_id": "G_TEST",
          "channels": [
            {
              "id": "C_001", "name": "general", "type": 0,
              "messages": [{"id": "<snowflake-str>", "content": "...",
                            "author": {...}, ...}, ...],
            },
            ...
          ],
          "page_size": 100,
        }
    """

    def __init__(
        self,
        *,
        fixture: dict[str, Any],
        profile: FaultProfile = HAPPY_PATH,
    ) -> None:
        super().__init__(profile=profile)
        self._fixture = fixture
        self._page_size = int(fixture.get("page_size", 100))

    # ---- Live-ingestion extension (Y2) ----
    def append_message(
        self, channel_id: str, message: dict[str, Any],
    ) -> str:
        """Append one message to the channel's state.

        Used by Y2's `DiscordGatewayGenerator` to record events that
        the Gateway dispatcher will see. Returns the message id for
        convenience.

        Mirrors MockGmailClient.append_messages from Y1 in shape: the
        client owns the message store, the generator owns when to
        write to it.
        """
        for c in self._fixture["channels"]:
            if c["id"] == channel_id:
                c.setdefault("messages", []).append(message)
                return str(message["id"])
        raise KeyError(
            f"MockDiscordClient.append_message: unknown channel "
            f"{channel_id!r}; declare it in the fixture first.",
        )

    # ---- M6.6 surface ----
    async def list_guilds(self) -> list[dict[str, Any]]:
        self._check_fault()
        return [{"id": self._fixture["guild_id"]}]

    async def list_guild_channels(
        self, guild_id: str,
    ) -> list[dict[str, Any]]:
        self._check_fault()
        return [
            {
                "id": c["id"],
                "name": c.get("name"),
                "type": c.get("type", 0),
            }
            for c in self._fixture["channels"]
        ]

    async def get_messages(
        self,
        *,
        channel_id: str,
        before: str | None = None,
        after: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        self._check_fault()
        messages = self._messages_for(channel_id)
        # Discord returns newest-first; sort fixture by snowflake desc.
        ordered = sorted(messages, key=lambda m: int(m["id"]), reverse=True)
        if before is not None:
            ordered = [m for m in ordered if int(m["id"]) < int(before)]
        if after is not None:
            # `after` direction returns messages newer than `after`,
            # also newest-first per Discord's behavior.
            ordered = [m for m in ordered if int(m["id"]) > int(after)]
        page_size = limit if limit is not None else self._page_size
        return ordered[:page_size]

    # ---- Helpers ----
    def _messages_for(self, channel_id: str) -> list[dict[str, Any]]:
        for c in self._fixture["channels"]:
            if c["id"] == channel_id:
                return list(c.get("messages", []))
        return []

    # ---- Fault raisers ----
    def _raise_rate_limit(self) -> NoReturn:
        raise DiscordApiError("MockDiscordClient: 429 (X2 fault)")

    def _raise_5xx(self) -> NoReturn:
        raise DiscordApiError("MockDiscordClient: 503 (X2 fault)")

    def _raise_auth_error(self) -> NoReturn:
        raise DiscordApiError("MockDiscordClient: 401 (X2 fault)")

    def _raise_transient(self) -> NoReturn:
        raise DiscordApiError(
            "MockDiscordClient: transient transport error (X2 fault)",
        )
