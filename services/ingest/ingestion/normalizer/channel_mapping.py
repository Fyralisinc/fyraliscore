"""Maps a raw envelope's (source, ingress_kind) → handler-registry channel.

Per M2 work-order §M2.3 and CHANNEL_TRUST_MAP in
`services/ingest/ingestion/handlers/__init__.py`.

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

from services.ingest.ingestion.raw_tier.envelope import (
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
    # Facebook Pages — live Meta webhooks + all Graph-paginated history.
    ("facebook_pages", "webhook"): "facebook_pages:message",
    ("facebook_pages", "backfill"): "facebook_pages:message",
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
    # Mercury — backfill + poll + webhook (finance). Mercury (banking/cash) has
    # BOTH a live push surface (HMAC-signed webhooks) AND a historical query
    # surface (GET /accounts, /account/{id}/transactions). Backfill walks each
    # account once; the incremental driver re-runs the same fetcher under
    # ingress_kind="poll" using the transaction `createdAt` high-water cursor;
    # the webhook ingress delivers live transaction.created events. ALL route to
    # the single `mercury:transaction` channel; the handler branches on the
    # reshaped `_fyralis_record_type` (transaction / account_snapshot). external_id
    # parity across the paths (`mercury:{account}:txn:{id}:{status}` and the
    # balance snapshot) collapses a backfilled transaction and its live twin to
    # one observation.
    ("mercury", "backfill"): "mercury:transaction",
    ("mercury", "poll"): "mercury:transaction",
    ("mercury", "webhook"): "mercury:transaction",
    # QuickBooks — backfill + poll + webhook (finance). QuickBooks Online
    # (accounting/AR-AP) has BOTH a live push surface (HMAC-SHA256 intuit-signature
    # webhooks, Intuit eventNotifications) AND a historical query surface (the
    # query endpoint, SELECT ... WHERE Metadata.LastUpdatedTime). Backfill walks
    # each entity (Invoice/Bill/BillPayment/Payment) once; the incremental driver
    # re-runs under ingress_kind="poll" using the LastUpdatedTime high-water; the
    # webhook ingress delivers thin change events (the poll re-fetch fills the
    # body). ALL route to the single `quickbooks:object` channel; the handler
    # branches on the reshaped entity type. external_id parity
    # (`qbo:{realm}:{entity}:{id}:{SyncToken}`) collapses a backfilled object and
    # its live twin to one observation.
    ("quickbooks", "backfill"): "quickbooks:object",
    ("quickbooks", "poll"): "quickbooks:object",
    ("quickbooks", "webhook"): "quickbooks:object",
    # Grafana — annotations backfill/poll + alert webhook (IN-GRAFANA). Grafana
    # is a TWO-channel source: the historical pull surface (GET /api/annotations,
    # which includes Grafana's auto alert-state-change annotations) routes to
    # `grafana:annotation`; the live push surface (Alerting webhook contact point
    # delivering Alertmanager-superset alert groups) routes to `grafana:alert`.
    # No external_id collision between the two — annotations key on the annotation
    # id, alert groups key on groupKey+status — so they are independent streams
    # (alert backfill via the Loki state-history timeline is a documented v2).
    ("grafana", "backfill"): "grafana:annotation",
    ("grafana", "poll"): "grafana:annotation",
    ("grafana", "webhook"): "grafana:alert",
    # Telegram — gateway (live) + backfill (IN-TELEGRAM). Telegram uses the
    # MTProto user-account API: backfill pages each dialog's history
    # (messages.getHistory, cursored on offset_id) under ingress_kind="backfill";
    # the live path is a PERSISTENT updates connection (no HTTP webhook for
    # MTProto), so live updateNewMessage events shadow-write under
    # ingress_kind="gateway" — exactly like Discord. BOTH route to the single
    # `telegram:message` channel; the handler derives the SAME external_id
    # (`telegram:{installation_id}:{dialog_id}:{message_id}`, edit-versioned) for
    # both paths, so a backfilled message and its live gateway twin collapse to
    # one observation. There is no `webhook`/`poll` ingress (MTProto has no
    # webhook; gap-recovery is updates.getDifference inside the live worker, not a
    # poll re-fetch). See ADR-0003.
    ("telegram", "gateway"): "telegram:message",
    ("telegram", "backfill"): "telegram:message",
    # Brex — backfill + poll + webhook (finance, IN-FIN2, Bearer/Mercury
    # archetype). Brex (cash/card) has BOTH a live push surface (HMAC-signed
    # webhooks) AND a historical query surface (account transactions). Backfill
    # walks each account once; the incremental driver re-runs the same fetcher
    # under ingress_kind="poll" using the transaction high-water cursor; the
    # webhook ingress delivers live transaction events. ALL route to the single
    # `brex:transaction` channel; external_id parity
    # (`brex:{account}:txn:{id}:{status}`) collapses a backfilled transaction and
    # its live twin to one observation.
    ("brex", "backfill"): "brex:transaction",
    ("brex", "poll"): "brex:transaction",
    ("brex", "webhook"): "brex:transaction",
    # Ramp — backfill + poll + webhook (finance, IN-FIN2, OAuth
    # client-credentials, keyset-paginated REST — verified docs.ramp.com).
    # Ramp (card/spend) has BOTH a live push surface (HMAC-signed flat
    # webhooks) AND a historical query surface (the /transactions,
    # /reimbursements, /cards, /users collections). Backfill walks each
    # entity stream once via `page.next` keyset pagination; the incremental
    # driver re-runs under ingress_kind="poll" using the per-stream high-water
    # (`from_date` / `updated_after`); the webhook ingress delivers live
    # transaction events. ALL route to the single `ramp:transaction` channel;
    # external_id parity (`ramp:{business}:txn:{id}:{state}`) collapses a
    # backfilled transaction and its live twin to one observation.
    ("ramp", "backfill"): "ramp:transaction",
    ("ramp", "poll"): "ramp:transaction",
    ("ramp", "webhook"): "ramp:transaction",
    # Gusto — backfill + poll + webhook (finance, IN-FIN2, OAuth/QuickBooks
    # archetype). Gusto (payroll) has BOTH a live push surface (HMAC-signed
    # webhooks) AND a historical query surface (payrolls/employees/contractor
    # payments). Backfill walks each entity type once; the incremental driver
    # re-runs under ingress_kind="poll" using the `updated_at` high-water; the
    # webhook ingress delivers live change events. ALL route to the single
    # `gusto:object` channel; the handler branches on the reshaped entity type.
    # external_id parity (`gusto:{company}:{entity}:{id}:{version}`) collapses a
    # backfilled object and its live twin to one observation.
    ("gusto", "backfill"): "gusto:object",
    ("gusto", "poll"): "gusto:object",
    ("gusto", "webhook"): "gusto:object",
    # Deel — backfill + poll + webhook (finance, IN-FIN2, Bearer/Mercury
    # archetype). Deel (contractor payments) has BOTH a live push surface
    # (HMAC-signed webhooks) AND a historical query surface (contract payments).
    # Backfill walks each contract once; the incremental driver re-runs under
    # ingress_kind="poll" using the payment high-water cursor; the webhook
    # ingress delivers live payment events. ALL route to the single
    # `deel:payment` channel; external_id parity
    # (`deel:{contract}:payment:{id}:{status}`) collapses a backfilled payment
    # and its live twin to one observation.
    ("deel", "backfill"): "deel:payment",
    ("deel", "poll"): "deel:payment",
    ("deel", "webhook"): "deel:payment",
    # Fireflies — backfill + poll + webhook (IN-VERTICALS, Brex/HMAC archetype).
    # The AI-notetaker meeting-transcript source has a live push surface
    # (HMAC-signed webhooks, transcript.completed) AND a historical query surface
    # (transcripts list). Backfill walks the workspace once; the incremental
    # driver re-runs the same fetcher under ingress_kind="poll" using the
    # transcript high-water cursor; the webhook ingress delivers live
    # transcript-completed events. ALL route to the single `fireflies:transcript`
    # channel; external_id parity
    # (`fireflies:{workspace}:transcript:{id}:{version}`) collapses a backfilled
    # transcript and its live twin to one observation.
    ("fireflies", "backfill"): "fireflies:transcript",
    ("fireflies", "poll"): "fireflies:transcript",
    ("fireflies", "webhook"): "fireflies:transcript",
    # Miro — backfill + poll + webhook (IN-VERTICALS, Brex/HMAC archetype). The
    # whiteboard source has a live push surface (HMAC-signed webhooks,
    # board_item.created/updated/deleted) AND a historical query surface (board
    # items, opaque-cursor paginated). Backfill walks each board once; the
    # incremental driver re-runs under ingress_kind="poll" using the item
    # high-water; the webhook ingress delivers live item events. ALL route to the
    # single `miro:item` channel; the handler maps a .deleted/.removed suffix to
    # a state_change. external_id parity (`miro:{org}:item:{id}:{version}`)
    # collapses a backfilled item and its live twin to one observation.
    ("miro", "backfill"): "miro:item",
    ("miro", "poll"): "miro:item",
    ("miro", "webhook"): "miro:item",
    # Figma — backfill + poll + webhook (IN-VERTICALS, Brex/HMAC archetype).
    # Event records and the durable ``file_snapshot`` record share one handler
    # function; that handler returns either `figma:event` or
    # `figma:file_snapshot` based on `_fyralis_record_type`.  The mapping stays
    # on the event entry point because the normalizer dispatches a function,
    # while the emitted NormalizedEnvelope carries the handler-selected source
    # channel.  Backfill walks versions/comments plus one design snapshot per
    # selected file; webhooks/poll deliver events and later schedule refreshes.
    ("figma", "backfill"): "figma:event",
    ("figma", "poll"): "figma:event",
    ("figma", "webhook"): "figma:event",
    # Signal — gateway (live) + backfill (IN-VERTICALS, Telegram/gateway
    # archetype). Signal uses a linked-device account session: backfill pages
    # each thread's history (cursored on offset_id) under ingress_kind="backfill";
    # the live path is a persistent linked-device receive loop (no HTTP webhook),
    # so live messages shadow-write under ingress_kind="gateway" — exactly like
    # Telegram/Discord. BOTH route to the single `signal:message` channel; the
    # handler derives the SAME external_id
    # (`signal:{install}:{thread}:{message_id}:none`) for both paths, so a
    # backfilled message and its live gateway twin collapse to one observation.
    # There is no `webhook`/`poll` ingress (gap-recovery is in the live worker).
    ("signal", "gateway"): "signal:message",
    ("signal", "backfill"): "signal:message",
    # AWS — backfill + poll (IN-VERTICALS, Grafana-backfill / poll-live
    # archetype). AWS CloudTrail has a historical query surface (LookupEvents)
    # AND a live edge that is a POLL (no inbound webhook): the live driver
    # re-runs the fetcher-shaped record build under ingress_kind="poll". Backfill
    # walks each (account, region) once; the incremental driver re-runs using the
    # events high-water. BOTH route to the single `aws:event` channel; the handler
    # branches on the event for signal-vs-state_change. external_id parity
    # (`aws:{account}:{region}:event:{id}`, immutable) collapses a backfilled
    # event and its live poll twin to one observation.
    ("aws", "backfill"): "aws:event",
    ("aws", "poll"): "aws:event",
    # Carta — backfill + poll (IN-VERTICALS, Gusto-backfill / poll-live
    # archetype). The cap-table source has a historical query surface (firm
    # entities) AND a live edge that is a POLL (no inbound webhook): the live
    # driver re-runs the cap-table change build under ingress_kind="poll".
    # Backfill walks each entity type once; the incremental driver re-runs using
    # the per-entity updated high-water. BOTH route to the single `carta:object`
    # channel; the handler maps lifecycle states to state_change. external_id
    # parity (`carta:{firm}:{kind}:{id}:{sync_token}`) collapses a backfilled
    # entity and its live poll twin to one observation.
    ("carta", "backfill"): "carta:object",
    ("carta", "poll"): "carta:object",
    # HiBob — backfill + poll + webhook (IN-PEOPLE, Gusto-structure / Brex-auth
    # archetype). The People/HR source has BOTH a live push surface (HMAC-SHA512,
    # base64 digest, Bob-Signature header) AND a historical query surface (per
    # entity_type: employee/lifecycle/timeoff/payroll). Backfill walks each
    # entity type once; the incremental driver re-runs the same fetcher under
    # ingress_kind="poll" using the per-entity `modified` high-water; the webhook
    # ingress delivers live change events. ALL route to the single `hibob:object`
    # channel; the handler branches on the reshaped entity type. external_id
    # parity (`hibob:{company}:{entity}:{id}:{ver}`) collapses a backfilled object
    # and its live twin to one observation.
    ("hibob", "backfill"): "hibob:object",
    ("hibob", "poll"): "hibob:object",
    ("hibob", "webhook"): "hibob:object",
    # Ashby — backfill + poll + webhook (IN-PEOPLE, Gusto-structure archetype).
    # The recruiting ATS source has BOTH a live push surface (HMAC-SHA256, hex
    # digest, Ashby-Signature `sha256=<hex>`, verified over the RAW body) AND a
    # historical query surface (RPC POST /CATEGORY.list, cursor-paginated, per
    # entity_type: candidate/application/job/interview/offer). Backfill walks each
    # entity type once; the incremental driver re-runs under ingress_kind="poll"
    # using the persisted syncToken; the webhook ingress delivers live events. ALL
    # route to the single `ashby:object` channel; the handler branches on the
    # reshaped entity type. external_id parity (`ashby:{org}:{entity}:{id}`)
    # collapses a backfilled object and its live twin to one observation.
    ("ashby", "backfill"): "ashby:object",
    ("ashby", "poll"): "ashby:object",
    ("ashby", "webhook"): "ashby:object",
    # LinkedIn — backfill + poll (IN-PEOPLE, Carta-structure archetype). The
    # recruiting source has a historical query surface (per entity_type:
    # share/social_action/follower_stat) AND a live edge that is a POLL (no
    # inbound webhook): the live driver re-runs the change build under
    # ingress_kind="poll". Backfill walks each entity type once; the incremental
    # driver re-runs using the per-entity updated high-water. BOTH route to the
    # single `linkedin:object` channel; the handler branches on the reshaped kind.
    # external_id parity (`linkedin:{org}:{kind}:{id}`) collapses a backfilled
    # entity and its live poll twin to one observation. The recruitment APIs are
    # partner-gated in production (no webhook entitlement → poll-only live edge).
    ("linkedin", "backfill"): "linkedin:object",
    ("linkedin", "poll"): "linkedin:object",
    # Instagram Messaging — Meta webhooks plus Conversations API backfill/poll.
    # All ingress paths feed canonical records produced by
    # integrations.instagram.records, so the single handler can branch by
    # `_fyralis_record_type` while keeping external_id parity across live,
    # backfill, and poll recovery copies of the same DM.
    ("instagram", "webhook"): "instagram:message",
    ("instagram", "backfill"): "instagram:message",
    ("instagram", "poll"): "instagram:message",
    # WhatsApp — webhook (live) only in Phase 1. Like github:webhook /
    # notion:object, this is a ONE-channel/many-event-types source: BOTH
    # inbound customer messages AND outbound delivery-status callbacks route
    # to the single `whatsapp:message` channel, whose handler branches on the
    # item (message -> signal/attested_agent; status -> state_change/
    # authoritative) and sets external_id accordingly. The dedicated
    # whatsapp_router fans a delivery out into one raw envelope per item
    # (`{message,...}` or `{status,...}`), so the normalizer runs the unified
    # handler per item. external_id parity (`whatsapp:{phone_number_id}:{wamid}`)
    # collapses an inline-fallback message and its Kafka twin to one observation.
    # Backfill (Coexistence/BSP) is a deferred phase — no backfill ingress here.
    ("whatsapp", "webhook"): "whatsapp:message",
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
