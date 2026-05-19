"""MockGmailClient — Gmail API surface used by M6.3 backfill.

Implements the four methods M6.3 fetcher/reconciler call:
  - messages_list(user_email, scope, page_token, max_results, query)
  - history_list(user_email, scope, start_history_id, page_token, history_types)
  - get_message(user_email, scope, message_id)
  - get_profile(user_email, scope)

Stateful: paginates a fixture's `messages` list, advances `history_id`
according to the fixture's `history_events`, returns the highest
historyId from `get_profile` so the reconciler's gap-detection logic
sees newer data when the fixture is configured to.

Returns dicts with Gmail's literal API field names (`nextPageToken`,
`historyId`, etc.) so the M6.3 fetcher code path is exercised exactly
as it would be against the real Gmail API.
"""
from __future__ import annotations

from typing import Any, NoReturn

from services.integrations.gmail.client import (
    GoogleApiError, GoogleRateLimited,
)
from services.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.synthetic.mock_clients._base import _MockBase


class MockGmailClient(_MockBase):
    """Stateful in-process replacement for `GmailClient`.

    `fixture` is a dict produced by `services.synthetic.fixtures.
    gmail_generator.make_gmail_mailbox(...)`. Shape:
        {
          "email": "alice@x.com",
          "messages": [{"id": ..., "threadId": ..., ...}, ...],
          "history_events": [{"id": "1234", "messages": [...]}, ...],
          "starting_history_id": "1000",
          "current_history_id": "1015",
          "page_size": 5,
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
        # `messages_list` cursor: index into the messages list.
        self._page_size = int(fixture.get("page_size", 5))

    # ---- M6.3 surface ----
    async def messages_list(
        self,
        *,
        user_email: str,
        scope: str,
        page_token: str | None = None,
        max_results: int = 100,
        query: str | None = None,
    ) -> dict[str, Any]:
        self._check_fault()
        messages = self._fixture["messages"]
        page_size = min(self._page_size, max_results)
        start = int(page_token) if page_token else 0
        end = start + page_size
        page = messages[start:end]
        next_token = str(end) if end < len(messages) else None
        result: dict[str, Any] = {
            "messages": [
                {"id": m["id"], "threadId": m["threadId"]}
                for m in page
            ],
            "resultSizeEstimate": len(messages),
        }
        if next_token is not None:
            result["nextPageToken"] = next_token
        return result

    async def history_list(
        self,
        *,
        user_email: str,
        scope: str,
        start_history_id: str,
        page_token: str | None = None,
        history_types: tuple[str, ...] = ("messageAdded",),
    ) -> dict[str, Any]:
        self._check_fault()
        events = [
            e for e in self._fixture.get("history_events", [])
            if int(e["id"]) >= int(start_history_id)
        ]
        page_size = self._page_size
        start = int(page_token) if page_token else 0
        end = start + page_size
        page = events[start:end]
        next_token = str(end) if end < len(events) else None
        result: dict[str, Any] = {
            "history": page,
            "historyId": self._fixture["current_history_id"],
        }
        if next_token is not None:
            result["nextPageToken"] = next_token
        return result

    async def get_message(
        self,
        *,
        user_email: str,
        scope: str,
        message_id: str,
    ) -> dict[str, Any]:
        self._check_fault()
        for m in self._fixture["messages"]:
            if m["id"] == message_id:
                return m
        # Match production: individual 404 — raise GoogleApiError.
        raise GoogleApiError(
            f"Gmail message {message_id} not found in fixture",
        )

    async def get_profile(
        self, *, user_email: str, scope: str,
    ) -> dict[str, Any]:
        self._check_fault()
        return {
            "emailAddress": self._fixture["email"],
            "historyId": self._fixture["current_history_id"],
        }

    # ---- Fault raisers (Gmail-specific error types) ----
    def _raise_rate_limit(self) -> NoReturn:
        raise GoogleRateLimited("MockGmailClient: rate limit (X2 fault)")

    def _raise_5xx(self) -> NoReturn:
        raise GoogleApiError("MockGmailClient: 503 (X2 fault)")

    def _raise_auth_error(self) -> NoReturn:
        raise GoogleApiError("MockGmailClient: 401 unauthorized (X2 fault)")

    def _raise_transient(self) -> NoReturn:
        # Gmail's production client surfaces transient errors as
        # GoogleApiError too (the underlying httpx error is wrapped).
        raise GoogleApiError(
            "MockGmailClient: transient transport error (X2 fault)",
        )
