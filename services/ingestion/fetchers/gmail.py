"""services/ingestion/fetchers/gmail.py — Gmail backfill fetcher (M6.3).

Per ingestion LLD §4 (per-source fetchers + Gmail specifics) + §3.1
(N1 invariant) + [05-lld-amendments.md A16] (transactional patterns)
+ A17 (Reconciler state machine — gap-fill shards processed here) +
A18 (M6.3 ships per-source backfill as net-new code via
users.messages.list, NOT a refactor of the existing inline path).

============================================================
TWO SHARD-KIND PATHS (per-source dispatch in the fetcher)
============================================================
The M6.2a FETCHER_DISPATCH keys on `source`, so all Gmail shards
land here. Inside this module we dispatch on `shard_kind`:

  - `"gmail_mailbox_window"` (initial backfill from the planner):
    Pages through `users.messages.list` from the start of the
    mailbox, hydrating each id via `users.messages.get`. The final
    page calls `users.getProfile` and stamps `final_history_id` in
    the cursor — the reconciler's reference point for gap detection.

  - `"gmail_history_gap"` (re-share gap fill from the reconciler):
    Pages through `users.history.list` between `start_history_id`
    and `end_history_id`, extracting messageAdded events and
    hydrating each via `users.messages.get`. End-of-data when the
    history range is exhausted.

The two paths produce records in the SAME envelope shape (see
`_build_record` below) so the downstream Kafka consumer doesn't
care which path produced the row.

============================================================
CURSOR SCHEMA (`GmailCursor`)
============================================================
The N1 primitive treats the cursor as opaque (dict | None) and
stores it in `workflow_states.state_data["cursor"]`. The Pydantic
model below is what M6.3 round-trips through it; per-source
fetchers own the cursor shape per the M6.2a contract.

Fields:
  - `page_token` — Gmail's nextPageToken (messages.list or history.list).
  - `messages_seen` — running count; diagnostic only.
  - `final_history_id` — populated on the LAST page of a
    `gmail_mailbox_window` shard via `users.getProfile`. The
    reconciler reads this for gap detection. NULL until the last
    page; reading before end_of_data is incorrect.
  - `start_history_id` / `end_history_id` — for `gmail_history_gap`
    shards, the range bounds (mirrored from shard_identifier for
    convenience).

============================================================
N1 INVARIANT
============================================================
ShardFetch owns the N1 advance (one publish-then-persist round per
fetch_page_gmail() call). The fetcher does NOT call
`advance_cursor_atomic_with_kafka_publish` directly; it returns
`FetchResult(records, next_cursor, end_of_data)` and ShardFetch
runs the N1 invariant on top. The fetcher's job is to:
  1. Read the previous cursor.
  2. Call the right Gmail API for the next page.
  3. Build records in the framework envelope shape.
  4. Compute the next cursor (or stamp end_of_data).

If ShardFetch's flush fails, the fetcher is called again on the
next tick with the SAME cursor; the response from Gmail must be
idempotent at the same page (it is — `pageToken` is stable across
calls; messageId hydration is read-only).

============================================================
RECORD ENVELOPE (Kafka payload shape)
============================================================
The fetcher's `records` field is a list of dicts. Each dict goes
into `ingestion.raw` Kafka under M6.2a's ShardFetch envelope:

    {
      "tenant_id": ...,
      "source": "gmail",
      "shard_id": ...,
      "record": <the dict returned here>
    }

The inner record matches the existing inline-handler's raw_payload
shape (per A18's two-path coexistence note) so a future normalizer
can read either source:

    {
      "message_resource": <Gmail API message resource>,
      "mailbox_email": "alice@acme.com",
      "scope_used": "gmail.metadata" | "gmail.readonly",
      "gmail_installation_id": "<uuid>",
      "read_path": "backfill" | "reconciliation_gap",
    }

============================================================
RATE LIMITING
============================================================
Gmail API returns 429 and 403-quotaExceeded as `GoogleRateLimited`.
The fetcher wraps each network call in
`retry_with_backoff_on_429(... retry_on=GoogleRateLimited)` per
[04-implementation-plan.md §M6 pattern-alignment rule #3]. 5xx
errors (`GoogleApiError` with `status in (500..504)`) are wrapped
in `retry_with_jitter_on_5xx`. Non-transient errors propagate; the
shard transitions to 'failed'.

Both retry helpers live in
`services/ingestion/workflows/retry.py` (substrate, M6.0).

============================================================
WIRE-IN
============================================================
This module assigns into `FETCHER_DISPATCH['gmail']` at import time;
the package `services/ingestion/fetchers/__init__.py` imports this
module to trigger the assignment. Tests rebind via
`monkeypatch.setitem(FETCHER_DISPATCH, "gmail", test_fn)`.
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.integrations.gmail.client import (
    GMAIL_METADATA_SCOPE,
    GMAIL_READONLY_SCOPE,
    GmailClient,
    GoogleApiError,
    GoogleHttpClient,
    GoogleRateLimited,
)
from services.integrations.gmail.dwd import get_minter
from services.ingestion.workflows.retry import (
    retry_with_backoff_on_429,
    retry_with_jitter_on_5xx,
)


log = logging.getLogger(__name__)


SHARD_KIND_MAILBOX_WINDOW = "gmail_mailbox_window"
SHARD_KIND_HISTORY_GAP = "gmail_history_gap"


# Scope alias used by gmail_installations.scope; map to the long URL
# scope strings the Gmail client expects.
_SCOPE_ALIAS = {
    "gmail.metadata": GMAIL_METADATA_SCOPE,
    "gmail.readonly": GMAIL_READONLY_SCOPE,
}


# Default page size for messages.list. Gmail caps at 500.
_DEFAULT_MESSAGES_PAGE_SIZE = 100


# ---------------------------------------------------------------------
# Cursor shape (per-source Pydantic model; round-trips through opaque
# dict in workflow_states.state_data per the M6.2a contract).
# ---------------------------------------------------------------------
class GmailCursor(BaseModel):
    """Cursor for Gmail backfill + gap-fill fetchers.

    `page_token` advances within either Gmail API. `final_history_id`
    is stamped on the LAST page of an initial-backfill shard via
    `users.getProfile`; the reconciler reads it for gap detection.
    `messages_seen` is diagnostic.

    For `gmail_history_gap` shards the cursor also carries
    `start_history_id` / `end_history_id` for the range; these stay
    constant across pages of a single gap shard.
    """

    model_config = ConfigDict(extra="forbid")

    page_token: str | None = None
    messages_seen: int = 0
    final_history_id: str | None = None
    start_history_id: str | None = None
    end_history_id: str | None = None


# ---------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------
def _resolve_scope(install: asyncpg.Record) -> str:
    """Map install's scope alias to the long Gmail scope URL.

    The install row's `scope` column carries either 'gmail.metadata'
    or 'gmail.readonly' (CHECK constraint per migration 0031). The
    Gmail client expects the long URL form.
    """
    alias = install["scope"]
    long_scope = _SCOPE_ALIAS.get(alias)
    if long_scope is None:
        raise ValueError(
            f"gmail install carries unknown scope alias: {alias!r}",
        )
    return long_scope


# A27.3: the `gmail:` handler validates `read_path in ("push","poll")`
# and rejects anything else. Backfill + reconciliation-gap are both
# pull-based historical reads, so they conform to the handler as
# "poll". The backfill-vs-gap distinction is diagnostic only and does
# NOT affect external_id (`gmail:{install}:{message_id}`), so parity
# with the live push/poll webhook path holds. The genuine source of
# the read is preserved in the cursor + the shard, not the record.
_HANDLER_READ_PATH = "poll"


def _build_record(
    *,
    message_resource: dict[str, Any],
    mailbox_email: str,
    scope_alias: str,
    gmail_installation_id: str,
    read_path: str,
) -> dict[str, Any]:
    """Build one handler-conformant Gmail record (A27.3).

    Shape matches the `gmail:` handler's `raw_payload` contract so the
    normalizer dispatches it through the same handler the live
    push/poll path uses — yielding the same external_id. The
    `read_path` argument names the producing path (backfill |
    reconciliation_gap) for callers' readability; it is normalised to
    the handler-accepted `"poll"` here.
    """
    return {
        "message_resource": message_resource,
        "mailbox_email": mailbox_email,
        "scope_used": scope_alias,
        "gmail_installation_id": gmail_installation_id,
        "read_path": _HANDLER_READ_PATH,
    }


def _decode_cursor(cursor: dict[str, Any] | None) -> GmailCursor:
    """Round-trip the opaque cursor dict through GmailCursor."""
    if cursor is None:
        return GmailCursor()
    return GmailCursor.model_validate(cursor)


def _encode_cursor(cursor: GmailCursor) -> dict[str, Any]:
    """Round-trip GmailCursor back to an opaque dict."""
    return cursor.model_dump(mode="json")


# ---------------------------------------------------------------------
# Gmail-client factory hook (test seam).
# ---------------------------------------------------------------------
# Tests patch this symbol via `monkeypatch.setattr` to inject a fake
# Gmail client. Production callers always go through `get_minter()` +
# `GoogleHttpClient`.
async def _open_gmail_client(install: asyncpg.Record):  # noqa: ANN202
    """Yield (gmail_client, http_close_callable). The http_close
    callable releases the underlying httpx client when done."""
    minter = get_minter()
    http = GoogleHttpClient(minter)
    await http.__aenter__()

    async def close() -> None:
        await http.__aexit__(None, None, None)

    return GmailClient(http), close


# ---------------------------------------------------------------------
# Main entrypoint — dispatch on shard_kind.
# ---------------------------------------------------------------------
async def fetch_page_gmail(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of records + next cursor for a Gmail shard.

    Dispatches on the shard kind embedded in `shard_identifier`:
      - `"gmail_mailbox_window"` (M6.3 planner): users.messages.list
        backfill.
      - `"gmail_history_gap"` (M6.3 reconciler): users.history.list
        gap fill.

    The kind is read from `shard_identifier["shard_kind"]` — populated
    by the framework or by the planner/reconciler. If absent (test
    fixture or pre-A17 shard), default to mailbox_window for
    backward compatibility.
    """
    kind = shard_identifier.get("shard_kind") or SHARD_KIND_MAILBOX_WINDOW
    if kind == SHARD_KIND_HISTORY_GAP:
        return await _fetch_page_history_gap(
            install, shard_identifier, cursor,
        )
    return await _fetch_page_mailbox_window(
        install, shard_identifier, cursor,
    )


# ---------------------------------------------------------------------
# Backfill path — users.messages.list.
# ---------------------------------------------------------------------
async def _fetch_page_mailbox_window(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """Page through users.messages.list for one mailbox."""
    mailbox_email = shard_identifier["mailbox_email"]
    scope_alias = install["scope"]
    scope_long = _resolve_scope(install)
    install_id = str(install["id"])

    gmail, close = await _open_gmail_client(install)
    try:
        cur = _decode_cursor(cursor)

        list_resp = await retry_with_backoff_on_429(
            lambda: gmail.messages_list(
                user_email=mailbox_email,
                scope=scope_long,
                page_token=cur.page_token,
                max_results=_DEFAULT_MESSAGES_PAGE_SIZE,
            ),
            retry_on=GoogleRateLimited,
        )

        message_stubs = list_resp.get("messages") or []
        next_page_token = list_resp.get("nextPageToken")
        is_last_page = not next_page_token

        records: list[dict[str, Any]] = []
        for stub in message_stubs:
            msg_id = stub.get("id")
            if not msg_id:
                continue
            try:
                message_resource = await retry_with_backoff_on_429(
                    lambda mid=msg_id: gmail.get_message(
                        user_email=mailbox_email,
                        scope=scope_long,
                        message_id=mid,
                    ),
                    retry_on=GoogleRateLimited,
                )
            except GoogleApiError as exc:
                # Individual-message failure: a deleted-then-listed
                # message can 404. Log and skip; the page still
                # advances. Same shape as the inline fetcher's
                # exception swallow at fetcher.py:125-130.
                log.warning(
                    "fetchers.gmail.get_message_failed",
                    extra={
                        "mailbox_email": mailbox_email,
                        "message_id": msg_id,
                        "error": str(exc)[:200],
                    },
                )
                continue
            records.append(
                _build_record(
                    message_resource=message_resource,
                    mailbox_email=mailbox_email,
                    scope_alias=scope_alias,
                    gmail_installation_id=install_id,
                    read_path="backfill",
                )
            )

        next_cursor = GmailCursor(
            page_token=next_page_token,
            messages_seen=cur.messages_seen + len(records),
            final_history_id=cur.final_history_id,
        )

        # On the last page, stamp the watermark via users.getProfile.
        # This is the reconciler's reference point for gap detection;
        # without it the reconciler can't tell whether new mail
        # arrived between the last list call and reconciliation.
        if is_last_page:
            profile = await retry_with_backoff_on_429(
                lambda: gmail.get_profile(
                    user_email=mailbox_email, scope=scope_long,
                ),
                retry_on=GoogleRateLimited,
            )
            history_id = profile.get("historyId")
            if history_id is not None:
                next_cursor = next_cursor.model_copy(update={
                    "final_history_id": str(history_id),
                })

        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(next_cursor),
            end_of_data=is_last_page,
        )
    finally:
        await close()


# ---------------------------------------------------------------------
# Gap-fill path — users.history.list.
# ---------------------------------------------------------------------
async def _fetch_page_history_gap(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """Page through users.history.list between start_history_id and
    end_history_id for one gap-fill shard.

    Gmail's history.list returns messageAdded events from
    `startHistoryId` forward. There is no native `endHistoryId`
    parameter; the fetcher terminates when:
      - No `nextPageToken` is returned, OR
      - The response's `historyId` (the canonical "you are caught
        up through this point" bookmark) is >= the shard's
        `end_history_id`.
    """
    mailbox_email = shard_identifier["mailbox_email"]
    start_history_id = shard_identifier["start_history_id"]
    end_history_id = shard_identifier["end_history_id"]
    scope_alias = install["scope"]
    scope_long = _resolve_scope(install)
    install_id = str(install["id"])

    gmail, close = await _open_gmail_client(install)
    try:
        cur = _decode_cursor(cursor)
        # On first call, prime the cursor with the shard's history-range
        # bounds so the cursor carries them across pages.
        if cur.start_history_id is None:
            cur = cur.model_copy(update={
                "start_history_id": start_history_id,
                "end_history_id": end_history_id,
            })

        list_resp = await retry_with_backoff_on_429(
            lambda: gmail.history_list(
                user_email=mailbox_email,
                scope=scope_long,
                start_history_id=cur.start_history_id or start_history_id,
                page_token=cur.page_token,
            ),
            retry_on=GoogleRateLimited,
        )

        events = list_resp.get("history") or []
        next_page_token = list_resp.get("nextPageToken")
        latest_history_id = list_resp.get("historyId")

        new_message_ids: list[str] = []
        for entry in events:
            for added in entry.get("messagesAdded") or []:
                msg = (added or {}).get("message") or {}
                mid = msg.get("id")
                if mid:
                    new_message_ids.append(mid)

        records: list[dict[str, Any]] = []
        for mid in new_message_ids:
            try:
                message_resource = await retry_with_backoff_on_429(
                    lambda mid=mid: gmail.get_message(
                        user_email=mailbox_email,
                        scope=scope_long,
                        message_id=mid,
                    ),
                    retry_on=GoogleRateLimited,
                )
            except GoogleApiError as exc:
                log.warning(
                    "fetchers.gmail.gap_get_message_failed",
                    extra={
                        "mailbox_email": mailbox_email,
                        "message_id": mid,
                        "error": str(exc)[:200],
                    },
                )
                continue
            records.append(
                _build_record(
                    message_resource=message_resource,
                    mailbox_email=mailbox_email,
                    scope_alias=scope_alias,
                    gmail_installation_id=install_id,
                    read_path="reconciliation_gap",
                )
            )

        # End-of-data when there's no nextPageToken OR we've
        # advanced past end_history_id.
        is_end = not next_page_token
        if not is_end and latest_history_id is not None and end_history_id:
            try:
                is_end = int(latest_history_id) >= int(end_history_id)
            except (TypeError, ValueError):
                is_end = False

        next_cursor = GmailCursor(
            page_token=next_page_token,
            messages_seen=cur.messages_seen + len(records),
            final_history_id=(
                str(latest_history_id) if latest_history_id is not None
                else cur.final_history_id
            ),
            start_history_id=cur.start_history_id,
            end_history_id=cur.end_history_id,
        )
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(next_cursor),
            end_of_data=is_end,
        )
    finally:
        await close()


# Wire into the dispatch table at module-import time.
FETCHER_DISPATCH["gmail"] = fetch_page_gmail


__all__ = [
    "GmailCursor",
    "SHARD_KIND_HISTORY_GAP",
    "SHARD_KIND_MAILBOX_WINDOW",
    "fetch_page_gmail",
]
