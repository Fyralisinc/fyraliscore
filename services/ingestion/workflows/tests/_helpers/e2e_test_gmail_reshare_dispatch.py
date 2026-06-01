"""M6.3 reshare-path test-dispatch helper.

Patches `_open_gmail_client` in both fetchers/gmail.py and
reconcilers/gmail.py with subprocess-specific fakes. The shard_fetch
subprocess's fake stamps final_history_id="100" on backfill, then
returns historyId="500" on the gap-fill history.list call. The
reconciler subprocess's fake returns historyId="500" on every
get_profile call — meaning pass-0 (vs 100) detects gap; pass-1
(vs 500) is clean.

Stateless per subprocess: no in-process counter. The reshare cycle's
state surface is `source_onboarding_runs.reconciliation_pass_count`
+ shard states; the fake just returns canned values that produce the
right gap/clean decisions given those state surfaces.
"""
from __future__ import annotations

from typing import Any

from services.ingestion.fetchers import gmail as gmail_fetcher_mod
from services.ingestion.reconcilers import gmail as gmail_reconciler_mod


class _ShardFetchFakeGmailClient:
    """Used by the shard_fetch subprocess.

    - messages.list: 1 page, 3 messages, end_of_data
    - get_profile (during backfill last page): "100"
    - history.list (during gap-fill): historyId="500", 1 message
    """

    async def messages_list(self, **kwargs: Any) -> dict:
        return {
            "messages": [
                {"id": "m1"}, {"id": "m2"}, {"id": "m3"},
            ],
            "nextPageToken": None,
        }

    async def get_message(self, *, user_email, scope, message_id) -> dict:
        return {
            "id": message_id, "threadId": f"thread-{message_id}",
            "snippet": f"fake {message_id}",
        }

    async def get_profile(self, **kwargs: Any) -> dict:
        # During backfill: the fetcher's last page stamps
        # final_history_id from this. Pin to "100".
        return {"historyId": "100"}

    async def history_list(self, **kwargs: Any) -> dict:
        # Gap fill: 1 event with 1 message; response's historyId is
        # the gap shard's stamped final_history_id ("500").
        return {
            "history": [
                {
                    "id": "h-gap-1",
                    "messagesAdded": [{"message": {"id": "gm1"}}],
                },
            ],
            "historyId": "500",
            "nextPageToken": None,
        }


class _ReconcilerFakeGmailClient:
    """Used by the reconciler subprocess.

    - get_profile: returns "500" on every call.

    Pass 0 (after backfill): mailbox_window shard's final_history_id
    is "100". 500 > 100 → gap.

    Pass 1 (after gap fill): gap shard's final_history_id is "500".
    500 <= 500 → clean. (Original mailbox_window shard is now in
    'reconciliation_resharded' state and excluded from the check.)
    """

    async def get_profile(self, **kwargs: Any) -> dict:
        return {"historyId": "500"}

    # Defensive: not actually called by reconciler, but provide for
    # parity with the client surface.
    async def history_list(self, **kwargs: Any) -> dict:
        return {"history": [], "historyId": "500", "nextPageToken": None}

    async def messages_list(self, **kwargs: Any) -> dict:
        return {"messages": [], "nextPageToken": None}

    async def get_message(self, **kwargs: Any) -> dict:
        return {"id": kwargs.get("message_id"), "threadId": "t"}


# Different fake per subprocess based on which module the seam lives
# in. The shard_fetch subprocess's fetcher.gmail._open_gmail_client
# returns the shard-fetch fake. The reconciler subprocess's
# reconciler.gmail._open_gmail_client returns the reconciler fake.
async def _shard_fetch_open(install: Any):
    fake = _ShardFetchFakeGmailClient()
    async def close() -> None: return None
    return fake, close


async def _reconciler_open(install: Any):
    fake = _ReconcilerFakeGmailClient()
    async def close() -> None: return None
    return fake, close


# Each subprocess imports this helper. The seams in BOTH modules get
# rebound. The shard_fetch subprocess only invokes the fetcher.gmail
# seam (it doesn't import reconciler.gmail unless something triggers
# it). The reconciler subprocess only invokes reconciler.gmail's seam.
# Either way, both rebinds are safe — they don't interfere.
gmail_fetcher_mod._open_gmail_client = _shard_fetch_open
gmail_reconciler_mod._open_gmail_client = _reconciler_open
