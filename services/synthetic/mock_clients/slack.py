"""MockSlackClient — Slack Web API surface used by M6.5 backfill.

Implements the two methods M6.5 planner/fetcher/reconciler call:
  - conversations_list() -> list[dict]
  - conversations_history(channel, cursor=None, oldest=None, limit=None)
    -> tuple[list[dict], next_cursor: str | None]

Stateful per channel: paginates messages via an opaque `next_cursor`
string (here implemented as a stringified offset). When `oldest` is
provided (reconciler's gap-detection probe), returns only messages
whose `ts` is strictly greater than `oldest`.
"""
from __future__ import annotations

from typing import Any, NoReturn

from services.integrations.slack.client import SlackApiError
from services.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.synthetic.mock_clients._base import _MockBase


class MockSlackClient(_MockBase):
    """Stateful in-process replacement for `SlackClient`.

    `fixture` shape (per `make_slack_workspace`):
        {
          "team_id": "T_TEST",
          "channels": [
            {
              "id": "C_001", "name": "general", "team_id": "T_TEST",
              "messages": [{"ts": "1700000000.000001", "user": "...",
                            "text": "...", ...}, ...],
            },
            ...
          ],
          "page_size": 10,
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
        self._page_size = int(fixture.get("page_size", 10))

    # ---- M6.5 surface ----
    async def conversations_list(self) -> list[dict[str, Any]]:
        self._check_fault()
        return [
            {
                "id": c["id"],
                "name": c.get("name"),
                "team_id": c.get("team_id", self._fixture.get("team_id")),
            }
            for c in self._fixture["channels"]
        ]

    async def conversations_history(
        self,
        *,
        channel: str,
        cursor: str | None = None,
        oldest: str | None = None,
        limit: int | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        self._check_fault()
        messages = self._messages_for(channel)
        # Slack returns newest-first; sort fixture by ts desc to match.
        ordered = sorted(messages, key=lambda m: m["ts"], reverse=True)
        if oldest is not None:
            ordered = [m for m in ordered if float(m["ts"]) > float(oldest)]
        page_size = limit if limit is not None else self._page_size
        start = int(cursor) if cursor else 0
        end = start + page_size
        page = ordered[start:end]
        next_cursor = str(end) if end < len(ordered) else None
        return page, next_cursor

    # ---- Helpers ----
    def _messages_for(self, channel_id: str) -> list[dict[str, Any]]:
        for c in self._fixture["channels"]:
            if c["id"] == channel_id:
                return list(c.get("messages", []))
        return []

    # ---- Fault raisers ----
    def _raise_rate_limit(self) -> NoReturn:
        raise SlackApiError("MockSlackClient: ratelimited (X2 fault)")

    def _raise_5xx(self) -> NoReturn:
        raise SlackApiError("MockSlackClient: 503 (X2 fault)")

    def _raise_auth_error(self) -> NoReturn:
        # Slack's API uses ok=false + error="invalid_auth"; the client
        # raises SlackApiError on that response shape.
        raise SlackApiError("MockSlackClient: invalid_auth (X2 fault)")

    def _raise_transient(self) -> NoReturn:
        raise SlackApiError(
            "MockSlackClient: transient transport error (X2 fault)",
        )
