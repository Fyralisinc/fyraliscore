"""Maps a raw envelope's (source, ingress_kind) → handler-registry channel.

Per M2 work-order §M2.3 and CHANNEL_TRUST_MAP in
`services/ingestion/handlers/__init__.py`.

The handler registry is keyed by channel name (e.g. "slack:message",
"discord:message", "github:webhook"); the raw envelope carries a
(source, ingress_kind) pair. The normalizer needs to translate so
it can dispatch each envelope through the right pure-transform
handler.

Combinations intentionally absent (returning None) are not bugs —
they're M2-scoped omissions documented in the table below. The
normalizer treats None as "skip this envelope" with a structured
log + `parse_failure` metric (per M2 work-order).
"""
from __future__ import annotations

from services.ingestion.raw_tier.envelope import (
    IngressKindLiteral,
    SourceLiteral,
)


# Mapping table. Keep alphabetic by source for grep-ability.
_CHANNEL_MAP: dict[tuple[str, str], str] = {
    # Discord — two live ingress surfaces + backfill.
    ("discord", "gateway"): "discord:message",      # IN-12 MESSAGE_CREATE
    ("discord", "webhook"): "discord:interaction",  # IN-09 slash commands
    ("discord", "backfill"): "discord:message",     # M6.7 (A27.2) — same
                                                    # handler as the gateway
                                                    # MESSAGE_CREATE path.
    # GitHub — webhook + backfill.
    ("github", "webhook"): "github:webhook",
    ("github", "backfill"): "github:webhook",       # M6.7 (A27.2)
    # Slack — webhook + backfill.
    ("slack", "webhook"): "slack:message",
    ("slack", "backfill"): "slack:message",         # M6.7 (A27.2)
    # Gmail — backfill resolves to the canonical "gmail:" message
    # handler (A27.2). The Pub/Sub notification ingress stays
    # INTENTIONALLY OMITTED: that payload is a notification
    # (emailAddress + historyId), NOT a Gmail message resource, so it
    # has no direct handler. M6's backfill path fetches the actual
    # message resources and publishes them under ingress_kind=backfill,
    # which the "gmail:" handler consumes.
    ("gmail", "backfill"): "gmail:",                # M6.7 (A27.2)
    # Gmail live-via-Kafka cutover: the push handler / poller fetches the
    # message resource and publishes it under ingress_kind="poll" instead
    # of ingesting inline. Same handler as backfill, so external_id
    # (`gmail:{install}:{message_id}`) is identical — cross-path dedup
    # collapses a backfilled message and its live "poll" twin to one row.
    ("gmail", "poll"): "gmail:",
    # Notion — backfill + poll (IN-14, D3). Notion has no reliable
    # content push, so there is no `webhook`/`gateway` ingress: backfill
    # walks the workspace once, and the incremental driver re-runs the
    # same fetcher under ingress_kind="poll" on a cadence. BOTH route to
    # the single `notion:object` channel; the handler branches on the
    # Notion object's native `object` field (page/block/comment) and sets
    # kind + content.object_type per record (mirrors the github:webhook
    # one-channel/many-event-types shape). external_id parity across the
    # two paths (`notion:{object}:{id}`) collapses a backfilled object and
    # its live "poll" twin to one observation.
    #
    # IN-14 webhooks ADD a third ingress: Notion subscriptions deliver a
    # thin change event (entity.id + type); the webhook handler fetches the
    # full object via the per-workspace bot token and shadow-writes it under
    # ingress_kind="webhook". Same `notion:object` channel and same
    # `notion:{object}:{id}` external_id, so a webhook-delivered object and
    # its backfill/poll twin collapse to one observation.
    ("notion", "webhook"): "notion:object",
    ("notion", "backfill"): "notion:object",
    ("notion", "poll"): "notion:object",
    # Google Calendar — backfill + poll (IN-15, D3). A Google Workspace
    # API on the shared Gmail DWD auth substrate; no push-webhook in v1, so
    # there is no `webhook`/`gateway` ingress. Backfill walks each calendar
    # once (events.list windowed by timeMin); the incremental driver re-runs
    # the same fetcher under ingress_kind="poll" using Google's native
    # syncToken. BOTH route to the single `google_calendar:event` channel;
    # the handler branches on the event `status` (cancelled -> state_change).
    # external_id parity across the two paths (`gcal:{calendar_id}:{event_id}`)
    # collapses a backfilled event and its live "poll" twin to one observation.
    ("google_calendar", "backfill"): "google_calendar:event",
    ("google_calendar", "poll"): "google_calendar:event",
    # Google Drive — backfill + poll (IN-16, D3). A Google Workspace API on the
    # shared Gmail DWD auth substrate; no push-webhook in v1. Backfill walks each
    # drive once (files.list windowed by modifiedTime); the incremental driver
    # re-runs the same fetcher under ingress_kind="poll" using the Changes API
    # start-page-token. BOTH route to the single `google_drive:file` channel; the
    # handler branches on removed/trashed -> state_change. external_id parity
    # (`gdrive:{file_id}:{version}`) collapses a backfilled file and its live
    # "poll" twin to one observation.
    ("google_drive", "backfill"): "google_drive:file",
    ("google_drive", "poll"): "google_drive:file",
    # Jira — backfill + poll + webhook (IN-17). Jira Cloud has BOTH a live
    # push surface (dynamic webhooks: jira:issue_created/updated,
    # comment_created/updated) AND a historical query surface (JQL search).
    # Backfill walks each project once (JQL `project = KEY ORDER BY updated
    # ASC`, expand=changelog); the incremental driver re-runs the same fetcher
    # under ingress_kind="poll" using the per-project `updated` high-water
    # cursor; the webhook ingress delivers live change events. ALL route to
    # the single `jira:issue` channel; the handler branches on the reshaped
    # event/record type (issue / changelog-transition / comment) — mirrors the
    # github:webhook one-channel/many-event-types shape. external_id parity
    # across the three paths (`jira:{site}:issue:{id}:{updated}` and friends)
    # collapses a backfilled issue and its live twins to one observation.
    ("jira", "backfill"): "jira:issue",
    ("jira", "poll"): "jira:issue",
    ("jira", "webhook"): "jira:issue",
}


def resolve_channel(
    source: SourceLiteral, ingress_kind: IngressKindLiteral,
) -> str | None:
    """Return the handler-registry channel for (source, ingress_kind),
    or None if the combination has no handler in M2 scope.

    Callers MUST handle None — the normalizer skips with a structured
    log + a `parse_failure` metric increment (M2 work-order metric).
    """
    return _CHANNEL_MAP.get((source, ingress_kind))


__all__ = ["resolve_channel"]
