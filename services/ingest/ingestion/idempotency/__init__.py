"""external_id constructors (M5 / LLD §6) — one per source dedup key.

Single source of truth for how each ingestion source **composes** its
`external_id`, the dedup key the observations repo enforces via
`UNIQUE (source_channel, external_id, occurred_at)` (the partition-key
`occurred_at` rides the index but is ignored by the dedup pre-check —
`services/domain/observations/repo.py`). Every webhook / backfill / poll
path for a source routes through one handler, and that handler calls the
constructor here, so the key can't drift between paths
(`normalizer/tests/test_backfill_external_id_parity.py` is the
load-bearing guard).

Two families of key:

  * **Immutable** — the upstream id is globally unique and stable, so the
    key is just a namespaced id (`gmail:{install}:{message_id}`,
    `discord:{snowflake}`). Re-fetches collapse to one observation.
  * **Versioned (the mutable-source dedup lesson, from IN-15)** — the
    resource mutates in place, so the key encodes the mutation dimension
    (status / version / sync-token / updated-time). A real change lands a
    NEW observation; identical re-fetches dedup. Each versioned
    constructor names its discriminator in its docstring.

**Adopted-verbatim keys are deliberately NOT here.** A Stripe `evt_…`, a
GitHub `node_id`, an RFC-5322 `Message-ID`, a Linear object id, a system
payload's caller-supplied id — these are unique upstream and assigned
directly by their handler. There is no composition to centralize and
nothing that can drift, so wrapping them would add indirection without
the drift-protection that is this module's whole point. This module owns
every **composed** key; pass-throughs stay inline.

Pure functions: no I/O, no ingestion imports (leaf module).
"""
from __future__ import annotations


# --- Slack -----------------------------------------------------------
def slack_message(channel_id: str, ts: str) -> str:
    """`{channel}:{ts}` — immutable; an edit arrives with a fresh `ts`."""
    return f"{channel_id}:{ts}"


# --- Gmail -----------------------------------------------------------
def gmail_message(installation_id: object, message_id: str) -> str:
    """`gmail:{install}:{message_id}` — namespaced by install so the same
    RFC-5322 Message-ID seen by two tenants stays distinct. Immutable."""
    return f"gmail:{installation_id}:{message_id}"


# --- Discord ---------------------------------------------------------
def discord_event(snowflake: str) -> str:
    """`discord:{snowflake}` — shared by interaction + message; snowflakes
    are immutable (an edit keeps the id)."""
    return f"discord:{snowflake}"


# --- Notion ----------------------------------------------------------
def notion_object(object_type: str, object_id: str) -> str:
    """`notion:{object_type}:{id}` for object_type in page/block/comment —
    immutable per object (Notion ids are stable across edits)."""
    return f"notion:{object_type}:{object_id}"


# --- GitHub (push only; PR/issue/review/etc. adopt the node_id verbatim) -
def github_push(repo_full: str | None, after: str | None) -> str | None:
    """`{repo_full}@{after}` (destination-branch tip), or None when either
    part is missing. The same commit pushed to two branches is two keys."""
    return f"{repo_full}@{after}" if repo_full and after else None


# --- Grafana ---------------------------------------------------------
def grafana_annotation(instance: str, ann_id: str, time_ms: object) -> str:
    """`grafana:{instance}:annotation:{id}:{time}` — VERSIONED by time so a
    re-published annotation at a new time re-observes; `none` when the
    annotation carries no time."""
    return f"grafana:{instance}:annotation:{ann_id}:{time_ms if time_ms else 'none'}"


def grafana_alert(
    instance: str, group_hash: str, status: str, rep_ts_iso: str,
) -> str:
    """`grafana:{instance}:alert:{group_hash}:{status}:{rep_ts}` —
    VERSIONED by (status, representative-ts) so firing→resolved and each
    re-fire land as distinct observations."""
    return f"grafana:{instance}:alert:{group_hash}:{status}:{rep_ts_iso}"


# --- Google Calendar -------------------------------------------------
def google_calendar_event(
    calendar_id: str, event_id: str, status: str, start_key: str,
) -> str:
    """`gcal:{calendar_id}:{event_id}:{status}:{start}` — VERSIONED by
    (status, start instant): a cancellation or reschedule is a new
    observation while RSVP-only churn + identical re-fetches dedup."""
    return f"gcal:{calendar_id}:{event_id}:{status}:{start_key}"


# --- Google Drive ----------------------------------------------------
def google_drive_file(
    file_id: str, *, version: object, removed: bool, change_time: object,
) -> str:
    """`gdrive:{file_id}:{version}` — VERSIONED by Drive's monotonic
    version counter (an edit bumps it → new observation; `v0` when
    absent). A removal carrying no version keys on the change time
    instead: `gdrive:{file_id}:removed:{change_time|now}`."""
    if removed and version is None:
        ct = change_time if isinstance(change_time, str) and change_time else "now"
        return f"gdrive:{file_id}:removed:{ct}"
    return f"gdrive:{file_id}:{version if version is not None else 'v0'}"


def google_drive_comment(file_id: str, comment_id: str, mod_key: str) -> str:
    """`gdrive-comment:{file_id}:{comment_id}:{modified}` — VERSIONED by
    modifiedTime (`none` when absent) so a comment edit re-observes."""
    return f"gdrive-comment:{file_id}:{comment_id}:{mod_key}"


def google_drive_revision(file_id: str, revision_id: str) -> str:
    """`gdrive-revision:{file_id}:{revision_id}` — immutable (revisions are
    historical)."""
    return f"gdrive-revision:{file_id}:{revision_id}"


# --- Jira ------------------------------------------------------------
def jira_issue(site: str, issue_id: str, updated: object) -> str:
    """`jira:{site}:issue:{id}:{updated}` — VERSIONED by the issue's
    updated timestamp (`none` when absent) so each edit re-observes."""
    return f"jira:{site}:issue:{issue_id}:{updated or 'none'}"


def jira_transition(site: str, issue_id: str, history_id: str) -> str:
    """`jira:{site}:transition:{id}:{history_id}` — immutable (a changelog
    entry id uniquely identifies the transition)."""
    return f"jira:{site}:transition:{issue_id}:{history_id}"


def jira_comment(site: str, comment_id: str, updated: object) -> str:
    """`jira:{site}:comment:{id}:{updated}` — VERSIONED by the comment's
    updated timestamp (`none` when absent)."""
    return f"jira:{site}:comment:{comment_id}:{updated or 'none'}"


# --- Mercury ---------------------------------------------------------
def mercury_transaction(account_id: str, txn_id: str, status: str) -> str:
    """`mercury:{account}:txn:{id}:{status}` — VERSIONED by status so a
    pending→posted transition lands a new observation."""
    return f"mercury:{account_id}:txn:{txn_id}:{status}"


def mercury_balance(account_id: str, as_of_date: str) -> str:
    """`mercury:{account}:balance:{YYYY-MM-DD}` — one balance snapshot per
    account per day."""
    return f"mercury:{account_id}:balance:{as_of_date}"


# --- QuickBooks ------------------------------------------------------
def quickbooks_entity(
    realm_id: str, entity_kind: str, entity_id: str, sync_token: str,
) -> str:
    """`qbo:{realm}:{kind}:{id}:{sync_token}` — VERSIONED by SyncToken
    (QBO bumps it on every field change) so each edit re-observes."""
    return f"qbo:{realm_id}:{entity_kind}:{entity_id}:{sync_token}"


def quickbooks_change(
    realm_id: str, entity_kind: str, entity_id: str, ver: str,
) -> str:
    """`qbo:{realm}:{kind}:{id}:chg:{ver}` — the thin webhook change event
    (no SyncToken); VERSIONED by LastUpdatedTime so each notification is
    distinct until the next poll re-fetches the authoritative body."""
    return f"qbo:{realm_id}:{entity_kind}:{entity_id}:chg:{ver}"


# --- Telegram --------------------------------------------------------
def telegram_message(
    installation_id: object, dialog_id: object, message_id: object,
    edit_date: object,
) -> str:
    """`telegram:{install}:{dialog}:{message_id}:{edit}` — namespaced by
    install (so the same dialog/message id seen by two tenants stays distinct,
    per the global UNIQUE-without-tenant_id rule) and VERSIONED by edit_date.

    A brand-new message has edit_date None → `…:{id}:none`; backfill and the
    live `updateNewMessage` gateway twin both derive the same key (an unedited
    message is `none` on both paths), so they collapse to one observation. An
    edit (updateEditMessage) carries a fresh edit_date → a NEW observation,
    matching the mutable-source dedup lesson (the edit is a distinct signal)."""
    return (
        f"telegram:{installation_id}:{dialog_id}:{message_id}:"
        f"{edit_date if edit_date else 'none'}"
    )


__all__ = [
    "discord_event",
    "github_push",
    "gmail_message",
    "google_calendar_event",
    "google_drive_comment",
    "google_drive_file",
    "google_drive_revision",
    "grafana_alert",
    "grafana_annotation",
    "jira_comment",
    "jira_issue",
    "jira_transition",
    "mercury_balance",
    "mercury_transaction",
    "notion_object",
    "quickbooks_change",
    "quickbooks_entity",
    "slack_message",
    "telegram_message",
]
