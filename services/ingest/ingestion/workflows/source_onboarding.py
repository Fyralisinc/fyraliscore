"""services/ingest/ingestion/workflows/source_onboarding.py
   — M6.2a SourceOnboarding service. Per-source planner-driven shard
     fan-out.

Per ingestion LLD §2 (SourceOnboardingWorkflow shape, ported to
asyncio per [05-lld-amendments.md A11]) + §1.2 (onboarding_shards
schema, M1-shipped per A15) + §3 (per-source planners).

============================================================
RESPONSIBILITY (the two per-tick phases — same shape as M6.1)
============================================================
(a) **New-request phase.** Consume `source_onboarding_requested`
    signals from the inbox `(source_onboarding, source_onboarding)`.
    Per signal: load the `source_onboarding_runs` row, load the
    install row from `provider_installations` or
    `gmail_installations`, call `PLANNER_DISPATCH[source](tenant_id,
    install)` → `list[Shard]`. INSERT one `onboarding_shards` row per
    shard. Emit one `shard_fetch_requested` per shard to ShardFetch's
    inbox `(shard_fetch, shard_fetch)`. Mark the parent
    `source_onboarding_runs.status='in_progress'`.

    Empty planner result → mark run 'completed', emit
    `source_onboarding_completed` with success. `NotImplementedError`
    from a stubbed planner → mark run 'failed', emit
    `source_onboarding_completed` with failure (the pre-M6.3 expected
    pre-real-planner steady state).

(b) **Shard-completion phase.** Consume `shard_fetch_completed`
    signals from the same inbox `(source_onboarding,
    source_onboarding)`. Per signal: mark the `onboarding_shards.state`
    'done' (or 'failed' if `failure_reason` is present in
    signal_data). If all shards for the parent
    `source_onboarding_runs` are terminal, mark the parent
    'completed' (or 'failed' if any shard failed) and emit
    `source_onboarding_completed` to M6.1's TenantOnboarding inbox
    `(tenant_onboarding, tenant_onboarding)`.

Each signal consumption runs in its own transaction — same shape as
M6.1's TenantOnboarding orchestrator. Per-signal-per-transaction is
the established M6 default; failure rolls back the claim AND any
adjacent writes; the next tick re-claims and retries.

============================================================
SIGNAL ADDRESSING (per A13)
============================================================
The service's inbox is `(kind="source_onboarding",
id="source_onboarding")` — what M6.1 emits to. Both
`source_onboarding_requested` (from M6.1) and `shard_fetch_completed`
(from M6.2a's own ShardFetch) land here; the service dispatches on
`signal_kind` in Python after claim. Same shared-inbox pattern as
M6.1's TenantOnboarding which consumes both `onboarding_run_created`
and `source_onboarding_completed`.

Emits:
  - `shard_fetch_requested` → `(shard_fetch, shard_fetch)` —
    M6.2a's ShardFetch inbox.
  - **`source_shards_completed` → `(reconciler, reconciler)`** —
    M6.2b's Reconciler inbox (success path; M6.2b chain change
    below). Idempotency key includes the
    `reconciliation_pass_count` to survive re-share cycles.
  - `source_onboarding_completed` → `(tenant_onboarding,
    tenant_onboarding)` — M6.1's orchestrator inbox. **Failure
    path only** post-M6.2b; the success path goes through
    Reconciler.

============================================================
M6.2b CHAIN CHANGE (success path goes through Reconciler)
============================================================
Per M6.2b: the all-shards-success roll-up emits
`source_shards_completed` to the Reconciler's inbox instead of
emitting `source_onboarding_completed` directly to TenantOnboarding.
The Reconciler runs per-source gap-detection and emits
`source_onboarding_completed` on the CLEAN path; on the RE-SHARE
path it creates new shards (with `parent_shard_id` linkage) and
emits `shard_fetch_requested` per new shard. The re-share cycle
can repeat until reconciliation is clean.

Implementation impacts in THIS file:
  1. `_COUNT_UNFINISHED_SHARDS_SQL` now treats
     `'reconciliation_resharded'` as terminal — so the roll-up
     re-fires after re-share + new-shard completion.
  2. The success-path roll-up reads
     `source_onboarding_runs.reconciliation_pass_count` (migration
     0056) and emits with idempotency_key
     `f"{run_id}:{source}:pass_{N}"` — without this, the second
     emit after a re-share cycle would collide on UNIQUE and be
     silently deduped.
  3. The failure path is UNCHANGED — failed runs still emit
     `source_onboarding_completed` directly via
     `_emit_source_completed`; they bypass the Reconciler because
     there's nothing to reconcile.

============================================================
SCHEMA — A15 COLUMN-NAMING MAP (LOAD-BEARING for M6.2a)
============================================================
M6.2a uses the M1-shipped `onboarding_shards` schema (LLD §1.2;
migration 0045). The M6.2a prompt described a different schema; per
[05-lld-amendments.md A15](../../../docs/ingestion/05-lld-amendments.md#a15--m62a-uses-m1-shipped-onboarding_shards-schema-no-new-migration),
the existing schema is authoritative and M6.2a uses it without
modification. The column-naming map:

  | M6.2a prompt term | Existing column (0045) |
  |---|---|
  | `shard_id` (PK) | `id UUID PRIMARY KEY` |
  | `shard_descriptor` | `shard_identifier JSONB` + `shard_kind TEXT` |
  | `cursor` | `cursor_token TEXT` (M6.2a leaves NULL) |
  | `status` | `state` |
  | `failure_reason` | `last_error` |

Status-value mapping: `pending → in_progress → done | failed`. M6.2a
does NOT write the `'reconciliation_resharded'` state (reserved for
M6.2b's Reconciler). The shard `cursor_token` column stays NULL —
the N1 primitive's cursor lives in `workflow_states.state_data`,
keyed by `(workflow_kind="shard_fetch", workflow_id=str(shard_id))`,
per the M6.0 substrate contract.

============================================================
PATTERN-ALIGNMENT MAPPING
============================================================
  Rule 1 (orchestration separated from side effects):
    `tick()` is the orchestrator; module-level `_load_*` / `_insert_*`
    / `_mark_*` functions own DB I/O. The class method passes the
    connection through; no `await self._pool.X(...)` calls in the
    class body.

  Rule 2 (state in Postgres, not memory):
    `state.persist_state` after every tick. The per-signal claim +
    state mutations are themselves Postgres-state changes.

  Rule 3 (retry in named functions):
    None needed at this granularity. Failure → txn rollback → next
    tick re-claims. No inline `try/except` retry loops.

  Rule 4 (signals via Postgres polling):
    The service consumes two signal kinds AND produces two more. All
    via the substrate.

  Rule 5 (no cross-workflow shared state):
    No module-level mutable state in this file. `PLANNER_DISPATCH`
    in `services/ingest/ingestion/planners/__init__.py` is ALL_CAPS
    (constant-style) and outside the analyzer's `services/ingest/ingestion/
    workflows/*.py` scope; it's the established dispatch-table
    pattern (same shape as the not-yet-shipped `FETCHER_DISPATCH`).
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from lib.shared.http_headers import redact_log_mapping
from lib.shared.product_workflow_metrics import record_product_workflow_event
from lib.shared.tenant_context import TenantContext, bind_tenant
from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.planners.context import PlannerContext
from services.ingest.ingestion.progress.events import (
    ProgressEvent,
    SourceOnboardingStarted,
)
from services.ingest.ingestion.progress.publisher import publish_progress_events
from services.ingest.ingestion.workflows.runtime import LongRunningService
from services.ingest.ingestion.workflows.signals import (
    WorkflowSignal,
    claim_signals,
    emit_signal,
    process_signal_with_serialization_retry,
)
from services.ingest.ingestion.workflows.state import (
    WorkflowState,
    load_state,
    persist_state,
)


log = logging.getLogger(__name__)


WORKFLOW_KIND = "source_onboarding"
WORKFLOW_ID_INBOX = "source_onboarding"  # per A13: workflow_id = inbox
WORKFLOW_ID_DEFAULT = "default"  # for workflow_states diagnostics

# Signal kinds.
SIGNAL_KIND_REQUESTED = "source_onboarding_requested"   # consumed from M6.1
SIGNAL_KIND_SHARD_REQUESTED = "shard_fetch_requested"   # emitted to ShardFetch
SIGNAL_KIND_SHARD_COMPLETED = "shard_fetch_completed"   # consumed from ShardFetch
SIGNAL_KIND_SHARDS_COMPLETED = "source_shards_completed"  # M6.2b: success path → Reconciler
SIGNAL_KIND_COMPLETED = "source_onboarding_completed"   # failure path → M6.1 directly

# Downstream inbox addresses.
SHARD_FETCH_INBOX_KIND = "shard_fetch"
SHARD_FETCH_INBOX_ID = "shard_fetch"
RECONCILER_INBOX_KIND = "reconciler"  # M6.2b: success path target
RECONCILER_INBOX_ID = "reconciler"
TENANT_ONBOARDING_INBOX_KIND = "tenant_onboarding"
TENANT_ONBOARDING_INBOX_ID = "tenant_onboarding"

DEFAULT_TICK_INTERVAL_SECONDS = 5.0
DEFAULT_MAX_SIGNALS_PER_TICK = 50
_FIGMA_CONNECTION_ERROR_MAX_CHARS = 512

VALID_SOURCES = ("slack", "github", "discord", "gmail", "notion", "google_calendar", "google_drive", "jira", "mercury", "quickbooks", "grafana", "telegram", "brex", "ramp", "gusto", "deel", "fireflies", "signal", "aws", "miro", "figma", "carta", "hibob", "ashby", "linkedin", "whatsapp", "facebook_pages")


# ---------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------
_LOAD_SOURCE_RUN_SQL = """
SELECT onboarding_run_id, source, tenant_id, status
  FROM source_onboarding_runs
 WHERE onboarding_run_id = $1 AND source = $2
"""

_LOAD_PROVIDER_INSTALL_SQL = """
SELECT id, tenant_id, provider, installation_id, secret_ref, enabled
  FROM provider_installations
 WHERE tenant_id = $1 AND provider = $2 AND enabled = TRUE
 LIMIT 1
"""

_LOAD_PROVIDER_INSTALL_BY_ID_SQL = """
SELECT id, tenant_id, provider, installation_id, secret_ref, enabled
  FROM provider_installations
 WHERE id = $1 AND tenant_id = $2 AND provider = $3 AND enabled = TRUE
 LIMIT 1
"""

# Per M6.3 S1 amendment (per [05-lld-amendments.md A18]
# — per-source enrichment via JSON-aggregating LEFT JOIN):
# Gmail's install record is workspace-scoped, but the planner needs the
# 1-to-N active-mailbox list to emit one shard per mailbox. The
# enrichment lives in `gmail_mailbox_watches` (per-mailbox table
# populated at install-time by `_provision_install`).
#
# The LEFT JOIN aggregates active mailboxes into a JSON array column;
# the planner decodes `install["mailboxes"]` (string) via orjson and
# stays stateless (no DB I/O in the planner). Filter on
# `state = 'active'` so paused / opted_out / errored mailboxes don't
# get planned — matches the existing steady-state code's
# `_lease_due_mailboxes` filter (services/ingest/integrations/gmail/
# history_poller.py:55).
#
# ShardFetch's own `_LOAD_GMAIL_INSTALL_SQL` does NOT need this
# enrichment (the fetcher works on one mailbox at a time via
# `shard_identifier`); only the planner's loader carries the aggregate.
_LOAD_GMAIL_INSTALL_SQL = """
SELECT gi.id, gi.tenant_id, gi.workspace_domain, gi.service_account_email,
       gi.scope, gi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'email_address', mw.email_address,
             'google_user_id', mw.google_user_id,
             'history_id', mw.history_id
           ) ORDER BY mw.email_address
         ) FILTER (WHERE mw.id IS NOT NULL),
         '[]'::json
       ) AS mailboxes
  FROM gmail_installations gi
  LEFT JOIN gmail_mailbox_watches mw
    ON mw.gmail_installation_id = gi.id AND mw.state = 'active'
 WHERE gi.tenant_id = $1 AND gi.disabled_at IS NULL
 GROUP BY gi.id
 LIMIT 1
"""

# IN-15: Google Calendar mirrors the Gmail loader (A18.2) — the planner
# needs the 1-to-N active-calendar list aggregated onto the workspace
# install so it can emit one shard per calendar (no DB I/O in the planner).
_LOAD_GCAL_INSTALL_SQL = """
SELECT gi.id, gi.tenant_id, gi.workspace_domain, gi.service_account_email,
       gi.scope, gi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'calendar_id', cc.calendar_id,
             'owner_email', cc.owner_email,
             'sync_token', cc.sync_token
           ) ORDER BY cc.calendar_id
         ) FILTER (WHERE cc.id IS NOT NULL),
         '[]'::json
       ) AS calendars
  FROM google_calendar_installations gi
  LEFT JOIN google_calendar_calendars cc
    ON cc.google_calendar_installation_id = gi.id AND cc.state = 'active'
 WHERE gi.tenant_id = $1 AND gi.disabled_at IS NULL
 GROUP BY gi.id
 LIMIT 1
"""

# IN-16: Google Drive mirrors the Gmail/Calendar loader (A18.2) — the planner
# needs the 1-to-N active-target list aggregated onto the workspace install so
# it can emit one shard per drive (My Drive + Shared Drives; no DB I/O in the
# planner).
_LOAD_GDRIVE_INSTALL_SQL = """
SELECT gi.id, gi.tenant_id, gi.workspace_domain, gi.service_account_email,
       gi.scope, gi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'drive_kind', dt.drive_kind,
             'drive_id', dt.drive_id,
             'owner_email', dt.owner_email,
             'start_page_token', dt.start_page_token
           ) ORDER BY dt.drive_kind, dt.drive_id, dt.owner_email
         ) FILTER (WHERE dt.id IS NOT NULL),
         '[]'::json
       ) AS targets
  FROM google_drive_installations gi
  LEFT JOIN google_drive_targets dt
    ON dt.google_drive_installation_id = gi.id AND dt.state = 'active'
 WHERE gi.tenant_id = $1 AND gi.disabled_at IS NULL
 GROUP BY gi.id
 LIMIT 1
"""

# IN-17: Jira mirrors the Gmail/Calendar loader (A18.2) — the planner needs
# the 1-to-N active-project list aggregated onto the site install so it can
# emit one shard per project (no DB I/O in the planner). The api_token lives
# in encrypted_secrets behind secret_ref; base_url + account_email are needed
# by the client.
_LOAD_JIRA_INSTALL_SQL = """
SELECT ji.id, ji.tenant_id, ji.base_url, ji.account_email, ji.secret_ref,
       ji.cloud_id, ji.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'project_key', jp.project_key,
             'project_id', jp.project_id,
             'project_name', jp.project_name,
             'updated_cursor', jp.updated_cursor
           ) ORDER BY jp.project_key
         ) FILTER (WHERE jp.id IS NOT NULL),
         '[]'::json
       ) AS projects
  FROM jira_installations ji
  LEFT JOIN jira_projects jp
    ON jp.jira_installation_id = ji.id AND jp.state = 'active'
 WHERE ji.tenant_id = $1 AND ji.disabled_at IS NULL
 GROUP BY ji.id
 LIMIT 1
"""

# Finance: Mercury mirrors the Jira/Calendar loader (A18.2) — the planner needs
# the 1-to-N active-account list aggregated onto the install so it can emit one
# shard per account (no DB I/O in the planner). The api_token lives in
# encrypted_secrets behind secret_ref; base_url is needed by the client.
_LOAD_MERCURY_INSTALL_SQL = """
SELECT mi.id, mi.tenant_id, mi.base_url, mi.secret_ref, mi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'account_id', ma.account_id,
             'account_name', ma.account_name,
             'account_kind', ma.account_kind,
             'txn_cursor', ma.txn_cursor
           ) ORDER BY ma.account_id
         ) FILTER (WHERE ma.id IS NOT NULL),
         '[]'::json
       ) AS accounts
  FROM mercury_installations mi
  LEFT JOIN mercury_accounts ma
    ON ma.mercury_installation_id = mi.id AND ma.state = 'active'
 WHERE mi.tenant_id = $1 AND mi.disabled_at IS NULL
 GROUP BY mi.id
 LIMIT 1
"""

# Finance: QuickBooks mirrors the Jira loader — one shard per (realm, entity
# type). The access token lives in encrypted_secrets behind secret_ref; realm_id
# + base_url are needed by the client.
_LOAD_QUICKBOOKS_INSTALL_SQL = """
SELECT qi.id, qi.tenant_id, qi.realm_id, qi.base_url, qi.secret_ref,
       qi.refresh_secret_ref, qi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', qe.entity_type,
             'updated_cursor', qe.updated_cursor
           ) ORDER BY qe.entity_type
         ) FILTER (WHERE qe.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM quickbooks_installations qi
  LEFT JOIN quickbooks_entities qe
    ON qe.quickbooks_installation_id = qi.id AND qe.state = 'active'
 WHERE qi.tenant_id = $1 AND qi.disabled_at IS NULL
 GROUP BY qi.id
 LIMIT 1
"""

# IN-GRAFANA: Grafana annotations/alerts are ORG-WIDE — no per-resource child
# table (unlike Jira's per-project / Mercury's per-account aggregation). The
# planner emits exactly ONE shard from the install row, so the loader just
# selects the install (no JSON aggregation). The service-account token lives in
# encrypted_secrets behind secret_ref; base_url + org_id are needed by the client
# and planner. `annotations_cursor_ms` is the warm-start high-water (None on
# first sync -> full walk).
_LOAD_GRAFANA_INSTALL_SQL = """
SELECT gi.id, gi.tenant_id, gi.base_url, gi.org_id, gi.secret_ref,
       gi.annotations_cursor_ms, gi.disabled_at
  FROM grafana_installations gi
 WHERE gi.tenant_id = $1 AND gi.disabled_at IS NULL
 LIMIT 1
"""

# IN-TELEGRAM: Telegram mirrors the Jira/Mercury loader (A18.2) — the planner
# needs the 1-to-N active-dialog list aggregated onto the account install so it
# can emit one shard per dialog (no DB I/O in the planner). The MTProto session
# refs + api credentials ride on the install row for the client builder.
_LOAD_TELEGRAM_INSTALL_SQL = """
SELECT ti.id, ti.tenant_id, ti.account_label, ti.api_id, ti.api_hash_secret_ref,
       ti.session_secret_ref, ti.backfill_session_secret_ref, ti.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'dialog_id', td.dialog_id,
             'dialog_kind', td.dialog_kind,
             'access_hash', td.access_hash,
             'title', td.title,
             'offset_id_cursor', td.offset_id_cursor
           ) ORDER BY td.dialog_id
         ) FILTER (WHERE td.id IS NOT NULL),
         '[]'::json
       ) AS dialogs
  FROM telegram_installations ti
  LEFT JOIN telegram_dialogs td
    ON td.telegram_installation_id = ti.id AND td.state = 'active'
 WHERE ti.tenant_id = $1 AND ti.disabled_at IS NULL
 GROUP BY ti.id
 LIMIT 1
"""

# IN-FIN2: Brex mirrors the Mercury loader (A18.2) — one shard per account. The
# Bearer api_token lives in encrypted_secrets behind secret_ref; base_url is
# needed by the client. The 1-to-N active-account list is aggregated onto the
# install so the planner emits one shard per account (no DB I/O in the planner).
_LOAD_BREX_INSTALL_SQL = """
SELECT bi.id, bi.tenant_id, bi.base_url, bi.secret_ref, bi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'account_id', ba.account_id,
             'account_name', ba.account_name,
             'account_kind', ba.account_kind,
             'txn_cursor', ba.txn_cursor
           ) ORDER BY ba.account_id
         ) FILTER (WHERE ba.id IS NOT NULL),
         '[]'::json
       ) AS accounts
  FROM brex_installations bi
  LEFT JOIN brex_accounts ba
    ON ba.brex_installation_id = bi.id AND ba.state = 'active'
 WHERE bi.tenant_id = $1 AND bi.disabled_at IS NULL
 GROUP BY bi.id
 LIMIT 1
"""

# IN-FIN2: Ramp mirrors the QuickBooks loader — one shard per (business, entity
# type). The OAuth access token lives behind secret_ref; business_id (scope id)
# + base_url + refresh_secret_ref are needed by the client.
_LOAD_RAMP_INSTALL_SQL = """
SELECT ri.id, ri.tenant_id, ri.business_id, ri.base_url, ri.secret_ref,
       ri.refresh_secret_ref, ri.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', re.entity_type,
             'updated_cursor', re.updated_cursor
           ) ORDER BY re.entity_type
         ) FILTER (WHERE re.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM ramp_installations ri
  LEFT JOIN ramp_entities re
    ON re.ramp_installation_id = ri.id AND re.state = 'active'
 WHERE ri.tenant_id = $1 AND ri.disabled_at IS NULL
 GROUP BY ri.id
 LIMIT 1
"""

# IN-FIN2: Gusto mirrors the QuickBooks loader — one shard per (company, entity
# type). company_uuid is the scope id; access token behind secret_ref +
# refresh_secret_ref + base_url are needed by the client.
_LOAD_GUSTO_INSTALL_SQL = """
SELECT gi.id, gi.tenant_id, gi.company_uuid, gi.base_url, gi.secret_ref,
       gi.refresh_secret_ref, gi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', ge.entity_type,
             'updated_cursor', ge.updated_cursor
           ) ORDER BY ge.entity_type
         ) FILTER (WHERE ge.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM gusto_installations gi
  LEFT JOIN gusto_entities ge
    ON ge.gusto_installation_id = gi.id AND ge.state = 'active'
 WHERE gi.tenant_id = $1 AND gi.disabled_at IS NULL
 GROUP BY gi.id
 LIMIT 1
"""

# IN-FIN2: Deel mirrors the Mercury loader — one shard per contract. The Bearer
# api_token lives behind secret_ref; base_url is needed by the client. The
# 1-to-N active-contract list is aggregated onto the install.
_LOAD_DEEL_INSTALL_SQL = """
SELECT di.id, di.tenant_id, di.base_url, di.secret_ref, di.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'contract_id', dc.contract_id,
             'payment_cursor', dc.payment_cursor
           ) ORDER BY dc.contract_id
         ) FILTER (WHERE dc.id IS NOT NULL),
         '[]'::json
       ) AS contracts
  FROM deel_installations di
  LEFT JOIN deel_contracts dc
    ON dc.deel_installation_id = di.id AND dc.state = 'active'
 WHERE di.tenant_id = $1 AND di.disabled_at IS NULL
 GROUP BY di.id
 LIMIT 1
"""

# IN-VERTICALS: Fireflies is workspace-scoped with NO child table — the planner
# emits exactly ONE shard per install. workspace_id + transcript_cursor ride on
# the install row; the Bearer api_token lives behind secret_ref; base_url is
# needed by the client.
_LOAD_FIREFLIES_INSTALL_SQL = """
SELECT fi.id, fi.tenant_id, fi.base_url, fi.workspace_id,
       fi.transcript_cursor, fi.secret_ref, fi.disabled_at
  FROM fireflies_installations fi
 WHERE fi.tenant_id = $1 AND fi.disabled_at IS NULL
 LIMIT 1
"""

# IN-VERTICALS: Signal mirrors the Telegram loader — one shard per thread. The
# planner needs the 1-to-N active-thread list aggregated onto the account
# install. The linked-device session refs ride on the install row for the client
# builder; there are NO MTProto app credentials (no api_id / api_hash) and NO
# access_hash on threads.
_LOAD_SIGNAL_INSTALL_SQL = """
SELECT si.id, si.tenant_id, si.account_label, si.session_secret_ref,
       si.backfill_session_secret_ref, si.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'thread_id', st.thread_id,
             'thread_kind', st.thread_kind,
             'title', st.title,
             'offset_id_cursor', st.offset_id_cursor
           ) ORDER BY st.thread_id
         ) FILTER (WHERE st.id IS NOT NULL),
         '[]'::json
       ) AS threads
  FROM signal_installations si
  LEFT JOIN signal_threads st
    ON st.signal_installation_id = si.id AND st.state = 'active'
 WHERE si.tenant_id = $1 AND si.disabled_at IS NULL
 GROUP BY si.id
 LIMIT 1
"""

# IN-VERTICALS: AWS is (account, region)-scoped with NO child table — the planner
# emits one shard per install. account_id + region + events_cursor_ms (warm-start
# high-water) ride on the install row; credentials resolve from credential_kind +
# secret_ref.
_LOAD_AWS_INSTALL_SQL = """
SELECT ai.id, ai.tenant_id, ai.account_id, ai.region, ai.credential_kind,
       ai.secret_ref, ai.events_cursor_ms, ai.disabled_at
  FROM aws_installations ai
 WHERE ai.tenant_id = $1 AND ai.disabled_at IS NULL
 LIMIT 1
"""

# IN-VERTICALS: Miro mirrors the Brex loader — one shard per board. org_id is the
# namespacing scope on the install row; the 1-to-N active-board list is
# aggregated onto the install so the planner emits one shard per board.
_LOAD_MIRO_INSTALL_SQL = """
SELECT mi.id, mi.tenant_id, mi.base_url, mi.org_id, mi.secret_ref,
       mi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'board_id', mb.board_id,
             'board_name', mb.board_name,
             'board_kind', mb.board_kind,
             'item_cursor', mb.item_cursor
           ) ORDER BY mb.board_id
         ) FILTER (WHERE mb.id IS NOT NULL),
         '[]'::json
       ) AS boards
  FROM miro_installations mi
  LEFT JOIN miro_boards mb
    ON mb.miro_installation_id = mi.id AND mb.state = 'active'
 WHERE mi.tenant_id = $1 AND mi.disabled_at IS NULL
 GROUP BY mi.id
 LIMIT 1
"""

# IN-VERTICALS: Figma mirrors the Brex loader — one shard per file. team_id is the
# namespacing scope on the install row; the 1-to-N active-file list is aggregated
# onto the install so the planner emits one shard per file.
_LOAD_FIGMA_INSTALL_SQL = """
SELECT fi.id, fi.tenant_id, fi.base_url, fi.team_id, fi.secret_ref,
       fi.auth_kind, fi.refresh_secret_ref, fi.token_expires_at, fi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'file_key', ff.file_key,
             'file_name', ff.file_name,
             'project_name', ff.project_name,
             'event_cursor', ff.event_cursor,
             'snapshot_version', ff.snapshot_version
           ) ORDER BY ff.file_key
         ) FILTER (WHERE ff.id IS NOT NULL),
         '[]'::json
       ) AS files
  FROM figma_installations fi
  LEFT JOIN figma_files ff
    ON ff.figma_installation_id = fi.id AND ff.state = 'active'
 WHERE fi.tenant_id = $1
   AND fi.disabled_at IS NULL
   AND ($2::uuid IS NULL OR fi.id = $2)
 GROUP BY fi.id
 LIMIT 1
"""

# IN-VERTICALS: Carta mirrors the Gusto loader — one shard per (firm, entity
# type). firm_id is the scope id; the OAuth access token lives behind secret_ref +
# refresh_secret_ref; the 1-to-N active entity-type list is aggregated onto the
# install.
_LOAD_CARTA_INSTALL_SQL = """
SELECT ci.id, ci.tenant_id, ci.firm_id, ci.base_url, ci.secret_ref,
       ci.refresh_secret_ref, ci.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', ce.entity_type,
             'updated_cursor', ce.updated_cursor
           ) ORDER BY ce.entity_type
         ) FILTER (WHERE ce.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM carta_installations ci
  LEFT JOIN carta_entities ce
    ON ce.carta_installation_id = ci.id AND ce.state = 'active'
 WHERE ci.tenant_id = $1 AND ci.disabled_at IS NULL
 GROUP BY ci.id
 LIMIT 1
"""

# IN-PEOPLE: HiBob mirrors the Gusto loader — one shard per (company, entity
# type). company_id is the scope id; the service-user Basic credential lives
# behind secret_ref (service_user_id is the public half); base_url is needed by
# the client. The 1-to-N active entity-type list is aggregated onto the install.
_LOAD_HIBOB_INSTALL_SQL = """
SELECT hi.id, hi.tenant_id, hi.company_id, hi.service_user_id, hi.base_url,
       hi.secret_ref, hi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', he.entity_type,
             'updated_cursor', he.updated_cursor
           ) ORDER BY he.entity_type
         ) FILTER (WHERE he.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM hibob_installations hi
  LEFT JOIN hibob_entities he
    ON he.hibob_installation_id = hi.id AND he.state = 'active'
 WHERE hi.tenant_id = $1 AND hi.disabled_at IS NULL
 GROUP BY hi.id
 LIMIT 1
"""

# IN-PEOPLE: Ashby mirrors the Gusto loader — one shard per (org, entity type).
# org_id is the scope id; the API key (Basic username) lives behind secret_ref;
# base_url is needed by the client. The incremental primitive is the persisted
# Ashby syncToken (`sync_cursor`), NOT a timestamp.
_LOAD_ASHBY_INSTALL_SQL = """
SELECT ai.id, ai.tenant_id, ai.org_id, ai.base_url, ai.secret_ref,
       ai.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', ae.entity_type,
             'sync_cursor', ae.sync_cursor
           ) ORDER BY ae.entity_type
         ) FILTER (WHERE ae.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM ashby_installations ai
  LEFT JOIN ashby_entities ae
    ON ae.ashby_installation_id = ai.id AND ae.state = 'active'
 WHERE ai.tenant_id = $1 AND ai.disabled_at IS NULL
 GROUP BY ai.id
 LIMIT 1
"""

# IN-PEOPLE: LinkedIn mirrors the Carta loader — one shard per (org, entity
# type). organization_urn is the scope id; the OAuth access token lives behind
# secret_ref + refresh_secret_ref; base_url is needed by the client. Partner-
# gated (poll-only); the 1-to-N active entity-type list is aggregated onto the
# install.
_LOAD_LINKEDIN_INSTALL_SQL = """
SELECT li.id, li.tenant_id, li.organization_urn, li.base_url, li.secret_ref,
       li.refresh_secret_ref, li.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', le.entity_type,
             'updated_cursor', le.updated_cursor
           ) ORDER BY le.entity_type
         ) FILTER (WHERE le.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM linkedin_installations li
  LEFT JOIN linkedin_entities le
    ON le.linkedin_installation_id = li.id AND le.state = 'active'
 WHERE li.tenant_id = $1 AND li.disabled_at IS NULL
 GROUP BY li.id
 LIMIT 1
"""

_LOAD_FACEBOOK_PAGES_INSTALL_SQL = """
SELECT id, tenant_id, page_id, page_name, page_access_token_ref,
       app_secret_ref, verify_token_ref, granted_scopes, subscribed_fields,
       webhook_subscribed_at, enabled, oldest_message_at,
       backfill_exhausted_at, backfill_exhausted_reason,
       conversation_count, message_count
  FROM facebook_page_installations
 WHERE tenant_id = $1 AND enabled = true
 ORDER BY updated_at DESC
 LIMIT 1
"""

_LOAD_FACEBOOK_PAGES_INSTALL_BY_ID_SQL = """
SELECT id, tenant_id, page_id, page_name, page_access_token_ref,
       app_secret_ref, verify_token_ref, granted_scopes, subscribed_fields,
       webhook_subscribed_at, enabled, oldest_message_at,
       backfill_exhausted_at, backfill_exhausted_reason,
       conversation_count, message_count
  FROM facebook_page_installations
 WHERE id = $1 AND tenant_id = $2 AND enabled = true
 LIMIT 1
"""

_MARK_SOURCE_RUN_IN_PROGRESS_SQL = """
UPDATE source_onboarding_runs
   SET status = 'in_progress', started_at = COALESCE(started_at, now())
 WHERE onboarding_run_id = $1 AND source = $2 AND status = 'pending'
"""

_MARK_SOURCE_RUN_COMPLETED_SQL = """
UPDATE source_onboarding_runs
   SET status = 'completed', completed_at = now()
 WHERE onboarding_run_id = $1 AND source = $2
   AND status IN ('pending', 'in_progress')
"""

_MARK_SOURCE_RUN_FAILED_SQL = """
UPDATE source_onboarding_runs
   SET status = 'failed', completed_at = now(), failure_reason = $3
 WHERE onboarding_run_id = $1 AND source = $2
   AND status IN ('pending', 'in_progress')
   AND tenant_id = $4
"""

# The source-run failure and connection-state update are deliberately executed
# by the same caller-managed transaction.  A Figma installation that has
# already been explicitly disconnected or needs a new authorization must win
# over a generic sync failure: neither state is downgraded to ``degraded``.
_MARK_FIGMA_INSTALLATION_DEGRADED_SQL = """
UPDATE figma_installations
   SET connection_state = 'degraded', last_error = $3
 WHERE id = $1
   AND tenant_id = $2
   AND disabled_at IS NULL
   AND connection_state NOT IN ('reauthorization_required', 'disconnected')
"""

# Use the existing M1-shipped 0045 columns. `cursor_token` is omitted
# (stays NULL); the N1 primitive's cursor lives in workflow_states.
_INSERT_SHARD_SQL = """
INSERT INTO onboarding_shards
    (id, onboarding_run_id, tenant_id, source, shard_kind,
     shard_identifier, window_start, window_end, recency_score,
     state, created_at)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, 'pending', now())
"""

_LOAD_SHARD_SQL = """
SELECT id, onboarding_run_id, tenant_id, source, shard_kind, shard_identifier,
       state
  FROM onboarding_shards
 WHERE id = $1
"""

_LOCK_SOURCE_RUN_FOR_ROLLUP_SQL = """
SELECT onboarding_run_id
  FROM source_onboarding_runs
 WHERE onboarding_run_id = $1 AND source = $2
 FOR UPDATE
"""

_MARK_SHARD_DONE_SQL = """
UPDATE onboarding_shards
   SET state = 'done', completed_at = now()
 WHERE id = $1 AND state IN ('pending', 'in_progress')
"""

_MARK_SHARD_FAILED_SQL = """
UPDATE onboarding_shards
   SET state = 'failed', completed_at = now(), last_error = $2
 WHERE id = $1 AND state IN ('pending', 'in_progress')
"""

# Count non-terminal shards for the parent (run, source) pair.
# `reconciliation_resharded` is treated as TERMINAL per the M6.2b
# chain change (the original shard's data has been collected; a
# child shard with parent_shard_id is filling the gap). When the
# Reconciler re-shares, original shards transition done →
# reconciliation_resharded and new shards take over; the rollup
# fires again once all NEW shards reach 'done' (because the
# originals are already in this terminal set).
_COUNT_UNFINISHED_SHARDS_SQL = """
SELECT count(*) FROM onboarding_shards
 WHERE onboarding_run_id = $1 AND source = $2
   AND state NOT IN ('done', 'failed', 'reconciliation_resharded')
"""

_ANY_SHARD_FAILED_SQL = """
SELECT count(*) FROM onboarding_shards
 WHERE onboarding_run_id = $1 AND source = $2 AND state = 'failed'
"""

# Collect failure reasons across failed shards, for rollup into the
# parent source_onboarding_runs.failure_reason.
_COLLECT_SHARD_FAILURES_SQL = """
SELECT id, last_error FROM onboarding_shards
 WHERE onboarding_run_id = $1 AND source = $2 AND state = 'failed'
 ORDER BY completed_at ASC
"""


# ---------------------------------------------------------------------
# Config.
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class SourceOnboardingConfig:
    """Configuration knobs. Test injection + env-driven production."""

    tick_interval_seconds: float = DEFAULT_TICK_INTERVAL_SECONDS
    max_signals_per_tick: int = DEFAULT_MAX_SIGNALS_PER_TICK
    instance_name: str = WORKFLOW_ID_DEFAULT


# ---------------------------------------------------------------------
# Named side-effect functions (Rule 1).
# ---------------------------------------------------------------------
async def _load_source_run(
    conn: asyncpg.Connection, *, run_id: UUID, source: str,
) -> asyncpg.Record | None:
    return await conn.fetchrow(_LOAD_SOURCE_RUN_SQL, run_id, source)


async def _load_install(
    conn: asyncpg.Connection, *, tenant_id: UUID, source: str,
    installation_row_id: UUID | None = None,
) -> asyncpg.Record | None:
    """Load the active install row for this (tenant, source).

    Returns None if no active install exists (the source got disabled
    between trigger-fire and source-onboarding-pickup — an A14 race).
    """
    if source == "gmail":
        return await conn.fetchrow(_LOAD_GMAIL_INSTALL_SQL, tenant_id)
    if source == "google_calendar":
        return await conn.fetchrow(_LOAD_GCAL_INSTALL_SQL, tenant_id)
    if source == "google_drive":
        return await conn.fetchrow(_LOAD_GDRIVE_INSTALL_SQL, tenant_id)
    if source == "jira":
        return await conn.fetchrow(_LOAD_JIRA_INSTALL_SQL, tenant_id)
    if source == "mercury":
        return await conn.fetchrow(_LOAD_MERCURY_INSTALL_SQL, tenant_id)
    if source == "quickbooks":
        return await conn.fetchrow(_LOAD_QUICKBOOKS_INSTALL_SQL, tenant_id)
    if source == "grafana":
        return await conn.fetchrow(_LOAD_GRAFANA_INSTALL_SQL, tenant_id)
    if source == "telegram":
        return await conn.fetchrow(_LOAD_TELEGRAM_INSTALL_SQL, tenant_id)
    if source == "brex":
        return await conn.fetchrow(_LOAD_BREX_INSTALL_SQL, tenant_id)
    if source == "ramp":
        return await conn.fetchrow(_LOAD_RAMP_INSTALL_SQL, tenant_id)
    if source == "gusto":
        return await conn.fetchrow(_LOAD_GUSTO_INSTALL_SQL, tenant_id)
    if source == "deel":
        return await conn.fetchrow(_LOAD_DEEL_INSTALL_SQL, tenant_id)
    if source == "fireflies":
        return await conn.fetchrow(_LOAD_FIREFLIES_INSTALL_SQL, tenant_id)
    if source == "signal":
        return await conn.fetchrow(_LOAD_SIGNAL_INSTALL_SQL, tenant_id)
    if source == "aws":
        return await conn.fetchrow(_LOAD_AWS_INSTALL_SQL, tenant_id)
    if source == "miro":
        return await conn.fetchrow(_LOAD_MIRO_INSTALL_SQL, tenant_id)
    if source == "figma":
        # OAuth/manual-replay triggers carry the Figma installation id.  Keep
        # planning pinned to that exact row so a tenant with multiple Figma
        # API origins cannot have one installation's failure reflected on
        # another installation's onboarding card.
        return await conn.fetchrow(
            _LOAD_FIGMA_INSTALL_SQL, tenant_id, installation_row_id,
        )
    if source == "carta":
        return await conn.fetchrow(_LOAD_CARTA_INSTALL_SQL, tenant_id)
    if source == "hibob":
        return await conn.fetchrow(_LOAD_HIBOB_INSTALL_SQL, tenant_id)
    if source == "ashby":
        return await conn.fetchrow(_LOAD_ASHBY_INSTALL_SQL, tenant_id)
    if source == "linkedin":
        return await conn.fetchrow(_LOAD_LINKEDIN_INSTALL_SQL, tenant_id)
    if source == "facebook_pages":
        if installation_row_id is not None:
            return await conn.fetchrow(
                _LOAD_FACEBOOK_PAGES_INSTALL_BY_ID_SQL,
                installation_row_id,
                tenant_id,
            )
        return await conn.fetchrow(_LOAD_FACEBOOK_PAGES_INSTALL_SQL, tenant_id)
    if installation_row_id is not None:
        return await conn.fetchrow(
            _LOAD_PROVIDER_INSTALL_BY_ID_SQL,
            installation_row_id,
            tenant_id,
            source,
        )
    return await conn.fetchrow(_LOAD_PROVIDER_INSTALL_SQL, tenant_id, source)


async def _build_source_client(
    source: str, pool: asyncpg.Pool, install: asyncpg.Record,
) -> Any:
    """Construct a per-source API client for the planner's PlannerContext.

    Per M6.4 / A18.6: per-source planners that enumerate resources at
    plan time (e.g., GitHub repos) receive a source-side client via
    `ctx.source_client`. Sources whose planner only reads DB state
    (Gmail) receive None.

    Production: lazy-imports the per-source client so unrelated
    services don't pay the import cost. Tests rebind this function
    via `monkeypatch.setattr` to inject fakes.
    """
    # Planners that enumerate at plan time (github repos / slack channels /
    # discord guilds) get a real client; gmail reads DB state → None. The
    # builders resolve the base URL via the endpoint resolver and, in
    # spammer mode, carry a spammer-recognized identity token (no real
    # JWT / secret material needed). See services/ingest/ingestion/fetchers/_clients.py.
    from services.ingest.ingestion.fetchers import _clients
    if source == "github":
        return await _clients.build_github_client(install, pool=pool)
    if source == "slack":
        return await _clients.build_slack_client(install, pool=pool)
    if source == "discord":
        return await _clients.build_discord_client(install, pool=pool)
    if source == "notion":
        return await _clients.build_notion_client(install, pool=pool)
    # IN-FIN2: finance sources (brex/ramp/gusto/deel — like mercury/quickbooks)
    # shard per resource from child rows the loader aggregates onto the install;
    # their planners read that DB state, not the API, so no plan-time source
    # client is needed. Branches kept explicit for parity with _load_install /
    # the §2.6 client builders (which serve the FETCHER, not the planner).
    if source in ("brex", "ramp", "gusto", "deel"):
        return None
    # IN-VERTICALS: fireflies/signal/aws/miro/figma/carta planners all read DB
    # state only (workspace_id / threads / install scope / boards / files /
    # entities pre-aggregated by the loader), so no plan-time source client is
    # needed. Branch kept explicit for parity with _load_install / the §2.6
    # FETCHER client builders.
    if source in ("fireflies", "signal", "aws", "miro", "figma", "carta"):
        return None
    # IN-PEOPLE: hibob/ashby/linkedin planners read DB state only (entity-type
    # list pre-aggregated by the loader, like gusto/carta), so no plan-time
    # source client is needed.
    if source in ("hibob", "ashby", "linkedin"):
        return None
    if source == "facebook_pages":
        return None
    return None


async def _insert_shard(
    conn: asyncpg.Connection, *,
    shard_id: UUID, run_id: UUID, tenant_id: UUID, source: str,
    shard: Shard,
) -> None:
    """INSERT one onboarding_shards row using the existing 0045 schema.

    Per A15: writes `shard_kind`, `shard_identifier`, leaves
    `cursor_token` NULL (cursor lives in workflow_states under the
    N1 primitive).
    """
    import orjson
    await conn.execute(
        _INSERT_SHARD_SQL,
        shard_id, run_id, tenant_id, source,
        shard.shard_kind,
        orjson.dumps(shard.shard_identifier).decode("utf-8"),
        shard.window_start, shard.window_end,
        shard.recency_score,
    )


async def _load_shard(
    conn: asyncpg.Connection, shard_id: UUID,
) -> asyncpg.Record | None:
    return await conn.fetchrow(_LOAD_SHARD_SQL, shard_id)


def _sanitize_figma_connection_error(failure_reason: object) -> str:
    """Make a bounded UI-visible Figma sync error safe to persist.

    Shard failures can originate at an HTTP/provider boundary, so their text
    is not trusted to be free of bearer tokens, query credentials, or PII.
    Reuse the central log-string redactor before storing it on the install
    record, flatten line breaks for the onboarding card, and bound its size.
    The full diagnostic remains on the source run / shard for operators.
    """
    raw = " ".join(str(failure_reason or "").split())
    if not raw:
        return "Figma sync failed; retry shortly"
    redacted = redact_log_mapping({"message": raw}).get("message")
    safe = " ".join(str(redacted or "").split())
    return safe[:_FIGMA_CONNECTION_ERROR_MAX_CHARS] or "Figma sync failed; retry shortly"


def _figma_installation_id_from_shard(
    shard: asyncpg.Record,
) -> UUID | None:
    """Read the planner-persisted installation id without trusting it alone.

    The eventual UPDATE also matches the source-run tenant and runs under its
    tenant context.  This parser merely narrows a Figma run to the installation
    that actually produced the shard; malformed/legacy shard JSON safely skips
    the card-state mutation instead of guessing across installations.
    """
    raw_identifier = shard["shard_identifier"]
    if isinstance(raw_identifier, (str, bytes, bytearray)):
        try:
            import orjson
            identifier = orjson.loads(raw_identifier)
        except (orjson.JSONDecodeError, TypeError, ValueError):
            return None
    elif isinstance(raw_identifier, dict):
        identifier = raw_identifier
    else:
        return None
    if not isinstance(identifier, dict):
        return None
    value = identifier.get("installation_id")
    try:
        return UUID(str(value)) if value is not None else None
    except (TypeError, ValueError, AttributeError):
        return None


async def _mark_figma_installation_degraded(
    tctx: TenantContext,
    *,
    tenant_id: UUID,
    installation_row_id: UUID | None,
    failure_reason: str,
) -> None:
    """Reflect a terminal Figma onboarding failure on its active install.

    ``tctx`` is already tenant-bound by ``_mark_source_run_failed``.  The
    explicit tenant predicate is retained as defense in depth so a stale or
    malformed shard's installation id cannot affect another tenant.
    """
    if installation_row_id is None:
        return
    await tctx.execute(
        _MARK_FIGMA_INSTALLATION_DEGRADED_SQL,
        installation_row_id,
        tenant_id,
        _sanitize_figma_connection_error(failure_reason),
    )


async def _mark_source_run_failed(
    conn: asyncpg.Connection,
    *,
    run_id: UUID,
    source: str,
    tenant_id: UUID,
    failure_reason: str,
    figma_installation_row_id: UUID | None = None,
) -> None:
    """Atomically mark the source run failed and update its Figma UI state.

    This is the one terminal-failure choke point for SourceOnboarding.  It
    binds ``app.current_tenant`` on the already-open signal transaction, so the
    source run and matching Figma installation are both protected by RLS and
    commit (or roll back) together.
    """
    async with bind_tenant(conn, tenant_id) as tctx:
        result = await tctx.execute(
            _MARK_SOURCE_RUN_FAILED_SQL,
            run_id,
            source,
            failure_reason,
            tenant_id,
        )
        if source == "figma" and result.endswith(" 1"):
            await _mark_figma_installation_degraded(
                tctx,
                tenant_id=tenant_id,
                installation_row_id=figma_installation_row_id,
                failure_reason=failure_reason,
            )


async def _lock_source_run_for_rollup(
    conn: asyncpg.Connection, *, run_id: UUID, source: str,
) -> None:
    await conn.fetchval(_LOCK_SOURCE_RUN_FOR_ROLLUP_SQL, run_id, source)


async def _count_unfinished_shards(
    conn: asyncpg.Connection, *, run_id: UUID, source: str,
) -> int:
    return int(await conn.fetchval(
        _COUNT_UNFINISHED_SHARDS_SQL, run_id, source,
    ))


async def _any_shard_failed(
    conn: asyncpg.Connection, *, run_id: UUID, source: str,
) -> bool:
    return int(await conn.fetchval(
        _ANY_SHARD_FAILED_SQL, run_id, source,
    )) > 0


async def _collect_shard_failure_summary(
    conn: asyncpg.Connection, *, run_id: UUID, source: str,
) -> str:
    """Roll up failed-shard `last_error` strings into a single summary
    for the parent run's failure_reason column."""
    rows = await conn.fetch(_COLLECT_SHARD_FAILURES_SQL, run_id, source)
    parts = [
        f"shard {row['id']}: {row['last_error'] or '<no reason>'}"
        for row in rows
    ]
    return "; ".join(parts) if parts else "<no failed shards found>"


# ---------------------------------------------------------------------
# Service.
# ---------------------------------------------------------------------
class SourceOnboarding(LongRunningService):
    """LongRunningService draining the source_onboarding inbox.

    Two signal kinds expected: `source_onboarding_requested` (from M6.1)
    and `shard_fetch_completed` (from M6.2a's own ShardFetch, Phase 2).
    Python-dispatch on `signal_kind` after claiming each signal.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        kafka_producer: Any | None = None,
        config: SourceOnboardingConfig | None = None,
    ) -> None:
        self._pool = pool
        # OPTIONAL progress-event producer (see TenantOnboarding); None in
        # unit tests, wired by `_run_service` in production.
        self._kafka_producer = kafka_producer
        self._config = config or SourceOnboardingConfig()

    @property
    def tick_interval_seconds(self) -> float:
        return self._config.tick_interval_seconds

    async def tick(self) -> None:
        """One tick: drain up to `max_signals_per_tick` inbox signals.

        Each signal runs in its own transaction. The two signal kinds
        share the inbox; dispatch on `signal_kind` in Python.
        """
        signals_processed = 0
        for _ in range(self._config.max_signals_per_tick):
            processed = await self._process_one_signal()
            if not processed:
                break
            signals_processed += 1

        await self._persist_scan_state(signals_processed=signals_processed)

    async def _process_one_signal(self) -> bool:
        """Claim + dispatch ONE signal, retrying transient serialization
        conflicts on the shared `workflow_signals` table (see
        `process_signal_with_serialization_retry`). Previously an unhandled
        `DeadlockDetectedError` from the signal INSERT crashed the worker."""
        return await process_signal_with_serialization_retry(
            self._process_one_signal_once, label="source_onboarding",
        )

    async def _process_one_signal_once(self) -> bool:
        """Claim ONE signal under SKIP LOCKED + dispatch by kind.

        Returns True iff a signal was processed. False signals an
        empty inbox.

        Failure mode: signal claim succeeds but downstream write
        raises → transaction rolls back → signal claimable again on
        next tick (the A12 + A13 property + the M6.1 precedent).

        `source.onboarding.started` is returned by `_handle_source_requested`
        and published AFTER commit (claim-via-UPDATE ordering). The
        terminal `source.onboarding.complete` is emitted by the Reconciler
        on its clean pass, not here (M6.2b chain change).
        """
        events: list[ProgressEvent] = []
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                signals = await claim_signals(
                    conn,
                    workflow_kind=WORKFLOW_KIND,
                    workflow_id=WORKFLOW_ID_INBOX,
                    consumed_by=self._config.instance_name,
                    batch_size=1,
                )
                if not signals:
                    return False
                sig = signals[0]
                if sig.signal_kind == SIGNAL_KIND_REQUESTED:
                    events = await self._handle_source_requested(conn, sig)
                elif sig.signal_kind == SIGNAL_KIND_SHARD_COMPLETED:
                    await self._handle_shard_completed(conn, sig)
                else:
                    log.warning(
                        "source_onboarding.unknown_signal_kind",
                        extra={
                            "signal_id": str(sig.id),
                            "signal_kind": sig.signal_kind,
                            "workflow_kind": sig.workflow_kind,
                        },
                    )
        await publish_progress_events(self._kafka_producer, events)
        return True

    async def _handle_source_requested(
        self, conn: asyncpg.Connection, sig: WorkflowSignal,
    ) -> list[ProgressEvent]:
        """Handle one `source_onboarding_requested` signal.

        Atomic transaction body — all of:
          - Load source_onboarding_runs row.
          - Idempotency check on status.
          - Mark in_progress.
          - Load install row.
          - Call planner via dispatch.
          - INSERT shard rows + emit shard_fetch_requested per shard.
        commit together or roll back together.

        Returns `[SourceOnboardingStarted]` once a plan is produced
        (`planned_shard_count = len(shards)`, including the empty-plan
        case which starts then immediately completes). Returns `[]` when
        the source never starts: invalid source, missing run, idempotent
        re-claim, missing install, or a planner failure (there is no
        `source.onboarding` failed event in the contract)."""
        run_id = UUID(sig.signal_data["onboarding_run_id"])
        tenant_id = UUID(sig.signal_data["tenant_id"])
        source = sig.signal_data["source"]
        installation_row_id = (
            UUID(sig.signal_data["installation_row_id"])
            if sig.signal_data.get("installation_row_id")
            else None
        )

        if source not in VALID_SOURCES:
            log.warning(
                "source_onboarding.invalid_source",
                extra={"source": source, "signal_id": str(sig.id)},
            )
            return []

        run = await _load_source_run(conn, run_id=run_id, source=source)
        if run is None:
            log.warning(
                "source_onboarding.run_missing",
                extra={
                    "run_id": str(run_id), "source": source,
                    "signal_id": str(sig.id),
                },
            )
            return []
        if run["tenant_id"] != tenant_id:
            # The signal payload is transport data; source-run ownership is
            # authoritative.  Continue under the authoritative tenant instead
            # of letting a malformed replay cross the boundary or strand an
            # otherwise valid source run.
            log.warning(
                "source_onboarding.tenant_mismatch",
                extra={
                    "run_id": str(run_id),
                    "source": source,
                    "signal_tenant_id": str(tenant_id),
                    "run_tenant_id": str(run["tenant_id"]),
                },
            )
            tenant_id = run["tenant_id"]
        if run["status"] != "pending":
            # Idempotency: a re-claimed signal whose run already
            # advanced is a no-op success.
            return []

        install = await _load_install(
            conn, tenant_id=tenant_id, source=source,
            installation_row_id=installation_row_id,
        )
        if install is None:
            failure_reason = (
                f"No active install for tenant {tenant_id} source "
                f"{source!r} at source-onboarding tick-time. The "
                f"install was likely disabled between trigger fire "
                f"and source-onboarding pickup (A14 race)."
            )
            await _mark_source_run_failed(
                conn,
                run_id=run_id,
                source=source,
                tenant_id=run["tenant_id"],
                failure_reason=failure_reason,
                figma_installation_row_id=(
                    installation_row_id if source == "figma" else None
                ),
            )
            await self._emit_source_completed(
                conn, run_id=run_id, source=source,
                failure_reason=failure_reason,
            )
            return []

        # Mark in-progress BEFORE planner call so planner failures
        # can transition to 'failed' cleanly (the WHERE clause on
        # _MARK_SOURCE_RUN_FAILED_SQL accepts both 'pending' and
        # 'in_progress').
        await conn.execute(_MARK_SOURCE_RUN_IN_PROGRESS_SQL, run_id, source)

        # M6.4 / A18.6: planners receive a PlannerContext bundle.
        # Build it from the in-transaction conn + per-source client (if
        # any). Per-source clients for sources that need them
        # (GitHub) are constructed via `_build_source_client`; sources
        # whose planner only reads `install` (Gmail) receive None.
        source_client = await _build_source_client(
            source, self._pool, install,
        )
        ctx = PlannerContext(
            tenant_id=tenant_id, install=install, conn=conn,
            source_client=source_client,
        )
        try:
            shards = await PLANNER_DISPATCH[source](ctx)
        except NotImplementedError as exc:
            failure_reason = str(exc)
            await _mark_source_run_failed(
                conn,
                run_id=run_id,
                source=source,
                tenant_id=run["tenant_id"],
                failure_reason=failure_reason,
                figma_installation_row_id=(
                    install["id"] if source == "figma" else None
                ),
            )
            await self._emit_source_completed(
                conn, run_id=run_id, source=source,
                failure_reason=failure_reason,
            )
            return []
        except Exception as exc:  # noqa: BLE001
            # Any other planner exception (config error, transient API
            # failure, real bug): mark the run failed and continue
            # serving. A single bad signal must NOT crash the
            # orchestrator. The exception type + message are preserved
            # in failure_reason for diagnosis.
            failure_reason = f"{type(exc).__name__}: {exc}"
            log.exception(
                "source_onboarding.planner_exception",
                extra={"source": source, "run_id": str(run_id)},
            )
            await _mark_source_run_failed(
                conn,
                run_id=run_id,
                source=source,
                tenant_id=run["tenant_id"],
                failure_reason=failure_reason,
                figma_installation_row_id=(
                    install["id"] if source == "figma" else None
                ),
            )
            await self._emit_source_completed(
                conn, run_id=run_id, source=source,
                failure_reason=failure_reason,
            )
            return []

        # The source has started: a plan was produced. `started_event`
        # carries the planned shard count (0 for the empty-plan case).
        started_event = SourceOnboardingStarted(
            tenant_id=tenant_id,
            source=source,  # type: ignore[arg-type]  # validated ∈ VALID_SOURCES
            started_at=dt.datetime.now(tz=dt.timezone.utc),
            planned_shard_count=len(shards),
        )

        if not shards:
            # Empty planner result: source has nothing to fetch.
            # Mark complete immediately + emit success via Reconciler
            # (M6.2b chain change for consistency — even the
            # zero-shard case goes through Reconciler so all
            # success paths converge on one shape; the Reconciler's
            # dispatch will return clean on an empty shard list).
            await conn.execute(
                _MARK_SOURCE_RUN_COMPLETED_SQL, run_id, source,
            )
            # pass_count is 0 here (the default; no Reconciler
            # re-shares have happened).
            await self._emit_shards_completed(
                conn, run_id=run_id, source=source,
                tenant_id=tenant_id, pass_count=0,
            )
            return [started_event]

        # Fan out: INSERT one shard row per planner output, emit one
        # shard_fetch_requested per shard. All in this transaction.
        for shard in shards:
            shard_id = uuid7()
            await _insert_shard(
                conn,
                shard_id=shard_id, run_id=run_id,
                tenant_id=tenant_id, source=source, shard=shard,
            )
            await emit_signal(
                conn,
                workflow_kind=SHARD_FETCH_INBOX_KIND,
                workflow_id=SHARD_FETCH_INBOX_ID,
                signal_kind=SIGNAL_KIND_SHARD_REQUESTED,
                idempotency_key=str(shard_id),
                signal_data={
                    "shard_id": str(shard_id),
                    "onboarding_run_id": str(run_id),
                    "tenant_id": str(tenant_id),
                    "source": source,
                },
            )

        return [started_event]

    async def _handle_shard_completed(
        self, conn: asyncpg.Connection, sig: WorkflowSignal,
    ) -> None:
        """Handle one `shard_fetch_completed` signal.

        Atomic transaction body. If this completion is the last
        non-terminal shard for the parent (run, source) pair, also
        emit `source_onboarding_completed` to M6.1's inbox.

        Wire vocabulary: `signal_data["status"]` is `'done'` or
        `'failed'` (matches the onboarding_shards.state values per
        A15). `signal_data.get("failure_reason")` is set on failure.
        """
        shard_id = UUID(sig.signal_data["shard_id"])
        status = sig.signal_data.get("status", "done")
        failure_reason = sig.signal_data.get("failure_reason")

        shard = await _load_shard(conn, shard_id)
        if shard is None:
            log.warning(
                "source_onboarding.shard_missing",
                extra={"shard_id": str(shard_id), "signal_id": str(sig.id)},
            )
            return

        run_id = shard["onboarding_run_id"]
        source = shard["source"]

        # Multiple SourceOnboarding replicas can drain sibling shard
        # completion signals concurrently. Serialize the parent roll-up
        # so the unfinished-shard count always observes earlier sibling
        # completions before deciding whether to emit the handoff.
        await _lock_source_run_for_rollup(
            conn, run_id=run_id, source=source,
        )

        if status == "failed":
            await conn.execute(
                _MARK_SHARD_FAILED_SQL,
                shard_id, failure_reason or "<unspecified failure>",
            )
        else:
            await conn.execute(_MARK_SHARD_DONE_SQL, shard_id)

        unfinished = await _count_unfinished_shards(
            conn, run_id=run_id, source=source,
        )
        if unfinished > 0:
            return

        # All shards terminal — roll up to parent.
        if await _any_shard_failed(conn, run_id=run_id, source=source):
            rollup = await _collect_shard_failure_summary(
                conn, run_id=run_id, source=source,
            )
            await _mark_source_run_failed(
                conn,
                run_id=run_id,
                source=source,
                tenant_id=shard["tenant_id"],
                failure_reason=rollup,
                figma_installation_row_id=(
                    _figma_installation_id_from_shard(shard)
                    if source == "figma" else None
                ),
            )
            await self._emit_source_completed(
                conn, run_id=run_id, source=source, failure_reason=rollup,
            )
            return

        await conn.execute(_MARK_SOURCE_RUN_COMPLETED_SQL, run_id, source)
        # M6.2b chain change: success path → Reconciler (not direct to
        # TenantOnboarding). Reconciler runs gap-detection then emits
        # source_onboarding_completed to TenantOnboarding on the
        # clean path. The failure path below STILL emits direct to
        # TenantOnboarding (failed runs have nothing to reconcile).
        # Need the run's reconciliation_pass_count for the
        # idempotency key — re-read on the same connection.
        pass_count = int(await conn.fetchval(
            "SELECT reconciliation_pass_count FROM source_onboarding_runs "
            "WHERE onboarding_run_id = $1 AND source = $2",
            run_id, source,
        ) or 0)
        await self._emit_shards_completed(
            conn, run_id=run_id, source=source,
            tenant_id=shard["tenant_id"],
            pass_count=pass_count,
        )

    async def _emit_shards_completed(
        self, conn: asyncpg.Connection, *,
        run_id: UUID, source: str, tenant_id: UUID | None,
        pass_count: int,
    ) -> None:
        """Emit `source_shards_completed` to Reconciler's inbox
        (M6.2b chain change).

        Idempotency key: `f"{run_id}:{source}:pass_{N}"` where N is
        `reconciliation_pass_count` at the moment of emit. This
        gives a fresh key per re-share cycle — without it, the second
        emit (after Reconciler reshare + new-shard completion) would
        collide with the first emit's key and emit_signal would
        silently dedup, breaking the cycle. See migration 0056's
        header for the load-bearing rationale.
        """
        data: dict[str, Any] = {
            "onboarding_run_id": str(run_id),
            "source": source,
            "reconciliation_pass_count": pass_count,
        }
        if tenant_id is not None:
            data["tenant_id"] = str(tenant_id)
        await emit_signal(
            conn,
            workflow_kind=RECONCILER_INBOX_KIND,
            workflow_id=RECONCILER_INBOX_ID,
            signal_kind=SIGNAL_KIND_SHARDS_COMPLETED,
            idempotency_key=f"{run_id}:{source}:pass_{pass_count}",
            signal_data=data,
        )

    async def _emit_source_completed(
        self, conn: asyncpg.Connection, *,
        run_id: UUID, source: str, failure_reason: str | None,
    ) -> None:
        """Emit `source_onboarding_completed` to M6.1's inbox.

        Per M6.2b chain change: this method is now used ONLY for the
        failure path. The success path emits `source_shards_completed`
        to Reconciler instead (via `_emit_shards_completed`); the
        Reconciler is the one that emits `source_onboarding_completed`
        on the CLEAN reconciliation pass. Failed runs have nothing to
        reconcile and bypass the Reconciler entirely.

        Idempotency key matches M6.1's TenantOnboarding orchestrator
        expectation: `f"{run_id}:{source}"`. Preserved across the
        M6.2b chain change so M6.1 needs no modification.
        """
        data: dict[str, Any] = {
            "onboarding_run_id": str(run_id),
            "source": source,
        }
        if failure_reason is not None:
            data["failure_reason"] = failure_reason
        result = await emit_signal(
            conn,
            workflow_kind=TENANT_ONBOARDING_INBOX_KIND,
            workflow_id=TENANT_ONBOARDING_INBOX_ID,
            signal_kind=SIGNAL_KIND_COMPLETED,
            idempotency_key=f"{run_id}:{source}",
            signal_data=data,
        )
        if failure_reason is not None and result.was_new:
            record_product_workflow_event(
                workflow="source_onboarding",
                event="source_onboarding_failed",
                outcome="error",
            )

    async def _persist_scan_state(
        self, *, signals_processed: int,
    ) -> None:
        """Diagnostic state row. Not load-bearing; operator queries
        against workflow_states grep this for progress signals."""
        existing = await load_state(
            self._pool, WORKFLOW_KIND, self._config.instance_name,
        )
        state = WorkflowState(
            workflow_kind=WORKFLOW_KIND,
            workflow_id=self._config.instance_name,
            tenant_id=None,
            state_data={
                "last_tick_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
                "last_signals_processed": signals_processed,
                "lifetime_signals_processed": (
                    (existing.state_data.get("lifetime_signals_processed", 0)
                     if existing else 0)
                    + signals_processed
                ),
            },
            last_advanced_at=dt.datetime.now(tz=dt.timezone.utc),
        )
        await persist_state(self._pool, state)


# ---------------------------------------------------------------------
# CLI entrypoint — `python -m services.ingest.ingestion.workflows.source_onboarding`.
# ---------------------------------------------------------------------
# ENV:
#   DATABASE_URL                — Postgres DSN (required).
#   SOURCE_ONBOARDING_TICK_SEC  — tick interval (default 5.0).
#   SOURCE_ONBOARDING_BATCH     — max signals per tick (default 50).
#   SOURCE_ONBOARDING_INSTANCE  — instance name for diagnostics.
#   WORKFLOWS_LOG_LEVEL         — log level (default INFO).
async def _run_service() -> None:
    import asyncio
    import os
    import signal as sig_module

    from services.ingest.ingestion.kafka.producer import (
        IdempotentProducer,
        ProducerConfig,
    )
    from services.ingest.ingestion.workflows.runtime import (
        make_workflow_pool,
        start_workflow_health,
    )

    pool = await make_workflow_pool(os.environ["DATABASE_URL"])
    # Progress-event producer for `source.onboarding.started` (LLD §6).
    producer = IdempotentProducer(ProducerConfig(
        bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
        ),
        client_id="workflow-source_onboarding",
    ))
    await producer.start()
    config = SourceOnboardingConfig(
        tick_interval_seconds=float(
            os.environ.get("SOURCE_ONBOARDING_TICK_SEC", "5.0"),
        ),
        max_signals_per_tick=int(
            os.environ.get("SOURCE_ONBOARDING_BATCH", "50"),
        ),
        instance_name=os.environ.get(
            "SOURCE_ONBOARDING_INSTANCE", WORKFLOW_ID_DEFAULT,
        ),
    )
    service = SourceOnboarding(pool, kafka_producer=producer, config=config)

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for s in (sig_module.SIGTERM, sig_module.SIGINT):
        loop.add_signal_handler(s, stop_event.set)

    log.info("workflow.source_onboarding.started", extra={
        "instance": config.instance_name,
    })
    # Liveness + metrics surface (opt-in via INGESTION_HEALTH_PORT).
    health_shutdown = start_workflow_health(stop_event)
    try:
        await service.run(stop_event=stop_event)
    finally:
        log.info("workflow.source_onboarding.shutting_down")
        await health_shutdown()
        await producer.stop()
        await pool.close()
    log.info("workflow.source_onboarding.exited")


def main() -> None:
    import asyncio
    import os
    logging.basicConfig(
        level=os.environ.get("WORKFLOWS_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_run_service())


if __name__ == "__main__":
    main()


__all__ = [
    "DEFAULT_MAX_SIGNALS_PER_TICK",
    "DEFAULT_TICK_INTERVAL_SECONDS",
    "RECONCILER_INBOX_ID",
    "RECONCILER_INBOX_KIND",
    "SHARD_FETCH_INBOX_ID",
    "SHARD_FETCH_INBOX_KIND",
    "SIGNAL_KIND_COMPLETED",
    "SIGNAL_KIND_REQUESTED",
    "SIGNAL_KIND_SHARD_COMPLETED",
    "SIGNAL_KIND_SHARD_REQUESTED",
    "SIGNAL_KIND_SHARDS_COMPLETED",
    "SourceOnboarding",
    "SourceOnboardingConfig",
    "TENANT_ONBOARDING_INBOX_ID",
    "TENANT_ONBOARDING_INBOX_KIND",
    "WORKFLOW_ID_DEFAULT",
    "WORKFLOW_ID_INBOX",
    "WORKFLOW_KIND",
    "main",
]
