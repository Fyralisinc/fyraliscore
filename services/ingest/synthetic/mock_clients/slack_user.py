"""MockSlackUserClient — per-user Slack DM read surface (xoxp grain).

Mirrors `SlackUserClient` for the worker-fetch DM backfill: serves ONE
consenting user's `im`/`mpim` conversations + their history from a
`make_slack_dm_workspace` fixture. Implements the two methods the DM
planner / fetcher / reconciler call:

  - conversations_list(types="im,mpim") -> list[dict]  (DM enumeration)
  - conversations_history(channel, cursor=None, oldest=None, limit=None)
    -> tuple[list[dict], next_cursor: str | None]

Bound to a single `user_id`; the planner builds one per consenting user.
"""
from __future__ import annotations

from typing import Any, NoReturn

from services.ingest.integrations.slack.client import SlackApiError
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.mock_clients._base import _MockBase


class MockSlackUserClient(_MockBase):
    """Stateful in-process replacement for `SlackUserClient`.

    `fixture` is a `make_slack_dm_workspace` dict; `user_id` selects which
    consenting user's conversations this instance serves (defaults to the
    fixture's first `dm_users` entry).
    """

    def __init__(
        self,
        *,
        fixture: dict[str, Any],
        user_id: str | None = None,
        profile: FaultProfile = HAPPY_PATH,
    ) -> None:
        super().__init__(profile=profile)
        self._fixture = fixture
        self._page_size = int(fixture.get("page_size", 50))
        dm_users = fixture.get("dm_users", [])
        entry: dict[str, Any] | None = None
        if user_id is not None:
            entry = next(
                (d for d in dm_users if d.get("user_id") == user_id), None,
            )
        elif dm_users:
            entry = dm_users[0]
        self._user_id = user_id or (entry or {}).get("user_id")
        self._conversations: list[dict[str, Any]] = (
            (entry or {}).get("conversations", [])
        )
        self._by_channel = {c["id"]: c for c in self._conversations}

    # ---- DM surface ----
    async def conversations_list(
        self, *, types: str = "im,mpim",
    ) -> list[dict[str, Any]]:
        self._check_fault()
        type_set = {t.strip() for t in types.split(",") if t.strip()}
        out: list[dict[str, Any]] = []
        for c in self._conversations:
            ctype = c.get("channel_type")
            if ctype not in type_set:
                continue
            out.append({
                "id": c["id"],
                "channel_type": ctype,
                "user": c.get("user"),
                "name": c.get("name"),
                "team_id": self._fixture.get("team_id"),
            })
        return out

    async def conversations_history(
        self,
        *,
        channel: str,
        cursor: str | None = None,
        oldest: str | None = None,
        limit: int | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        self._check_fault()
        conv = self._by_channel.get(channel, {})
        messages = list(conv.get("messages", []))
        ordered = sorted(messages, key=lambda m: m["ts"], reverse=True)
        if oldest is not None:
            ordered = [m for m in ordered if float(m["ts"]) > float(oldest)]
        page_size = limit if limit is not None else self._page_size
        start = int(cursor) if cursor else 0
        end = start + page_size
        page = ordered[start:end]
        next_cursor = str(end) if end < len(ordered) else None
        return page, next_cursor

    # ---- Fault raisers ----
    def _raise_rate_limit(self) -> NoReturn:
        raise SlackApiError("MockSlackUserClient: ratelimited (X2 fault)")

    def _raise_5xx(self) -> NoReturn:
        raise SlackApiError("MockSlackUserClient: 503 (X2 fault)")

    def _raise_auth_error(self) -> NoReturn:
        raise SlackApiError("MockSlackUserClient: invalid_auth (X2 fault)")

    def _raise_transient(self) -> NoReturn:
        raise SlackApiError(
            "MockSlackUserClient: transient transport error (X2 fault)",
        )
