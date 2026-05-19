"""M6.3 clean-path test-dispatch helper.

Patches `_open_gmail_client` in fetchers/gmail.py + reconcilers/gmail.py
with a fake that returns canned API responses. Stamps final_history_id
== "100" on the backfill's last page; reconciler's getProfile also
returns "100" → clean decision.
"""
from __future__ import annotations

from typing import Any

from services.ingestion.fetchers import gmail as gmail_fetcher_mod
from services.ingestion.reconcilers import gmail as gmail_reconciler_mod


_FAKE_PROFILE_HISTORY_ID = "100"


class _FakeGmailClient:
    """Canned Gmail API surface. Same for both shard_fetch and
    reconciler subprocesses (clean path: matching historyIds)."""

    def __init__(self) -> None:
        self.list_pages = [
            {
                "messages": [
                    {"id": "m1", "threadId": "t1"},
                    {"id": "m2", "threadId": "t2"},
                    {"id": "m3", "threadId": "t3"},
                ],
                "nextPageToken": None,
            },
        ]

    async def messages_list(self, **kwargs: Any) -> dict:
        if not self.list_pages:
            return {"messages": [], "nextPageToken": None}
        return self.list_pages.pop(0)

    async def get_message(self, *, user_email: str, scope: str,
                          message_id: str) -> dict:
        return {
            "id": message_id,
            "threadId": f"thread-{message_id}",
            "snippet": f"fake message {message_id}",
            "payload": {"headers": [
                {"name": "Subject", "value": f"Subject {message_id}"},
                {"name": "From", "value": user_email},
            ]},
        }

    async def get_profile(self, **kwargs: Any) -> dict:
        return {"historyId": _FAKE_PROFILE_HISTORY_ID,
                "emailAddress": kwargs.get("user_email", "")}

    async def history_list(self, **kwargs: Any) -> dict:
        # Clean path: no gap, history_list never called. Defensive.
        return {"history": [], "historyId": _FAKE_PROFILE_HISTORY_ID,
                "nextPageToken": None}


async def _fake_open_gmail_client(install: Any):
    fake = _FakeGmailClient()

    async def close() -> None:
        return None

    return fake, close


# Rebind the seam in BOTH modules (fetcher + reconciler each have
# their own seam pointing at the same logical concept). Production
# uses the real GoogleHttpClient + GmailClient; tests rebind here.
gmail_fetcher_mod._open_gmail_client = _fake_open_gmail_client
gmail_reconciler_mod._open_gmail_client = _fake_open_gmail_client
