"""HmacWebhookGenerator — synthetic HMAC-signed webhooks for the finance/ops
providers that share the gateway webhook router + the M5.3 Kafka cutover:
**jira, mercury, quickbooks, grafana** plus the IN-FIN2 finance sources
**brex, ramp, gusto, deel**, the IN-FF/IN-MIRO/IN-FIGMA sources
**fireflies, miro, figma** (Brex archetype: HMAC-SHA256, `sha256=`+hex), and the
IN-PEOPLE/IN-RECRUITING sources **hibob, ashby** (hibob = HMAC-SHA512/base64/
no-prefix; ashby = HMAC-SHA256/`sha256=`+hex like brex). (LinkedIn is poll-only —
NOT an HMAC webhook source — so it is driven by LinkedinPollGenerator, not here.)

This is the generalisation of `slack_webhook.py` / `github_webhook.py` to the
providers added after the original 4-source live harness. All four:

  - POST to `/webhooks/{provider}` on the SAME shared FastAPI app the slack /
    github generators use (built by `services.app.gateway.main.build_app`).
  - Are verified by the real per-provider `signatures/{provider}.py` verifier
    (so the generator reproduces each scheme byte-for-byte).
  - Resolve the tenant via the real `tenant_resolver` against a seeded
    `provider_installations` row (NOT the dedicated backfill install table).
  - Take the M5.3 cutover (publish a RawEnvelope to `ingestion.raw.{provider}`,
    return HTTP 202) when the tenant's `ingestion.kafka_path_enabled` flag is
    TRUE and `app.state.{kafka_producer,s3_raw_client,tenant_flags}` are wired
    — exactly the deps the runner sets for slack/github.

Per-provider signature scheme (verified against signatures/*.py):
  - jira:       header `X-Hub-Signature`            = `sha256=` + hex(HMAC-SHA256(body))
  - mercury:    header `Mercury-Signature`          = `sha256=` + hex(HMAC-SHA256(body))
  - quickbooks: header `intuit-signature`           = base64(HMAC-SHA256(body))   (no prefix)
  - grafana:    header `X-Grafana-Alerting-Signature`= hex(HMAC-SHA256(body))     (no prefix, lowercase)
  - brex:       header `Brex-Signature`             = `sha256=` + hex(HMAC-SHA256(body))
  - ramp:       header `x-ramp-signature`           = base64(HMAC-SHA256(body))   (no prefix; UNVERIFIED default)
  - gusto:      header `intuit-signature`           = base64(HMAC-SHA256(body))   (no prefix; UNVERIFIED QBO default)
  - deel:       header `Deel-Signature`             = `sha256=` + hex(HMAC-SHA256(body))
  - fireflies:  header `X-Fireflies-Signature`      = `sha256=` + hex(HMAC-SHA256(body))
  - miro:       header `X-Miro-Signature`           = `sha256=` + hex(HMAC-SHA256(body))
  - figma:      header `Figma-Signature`            = `sha256=` + hex(HMAC-SHA256(body))
  - hibob:      header `Bob-Signature`              = base64(HMAC-SHA512(body))   (NO prefix; CONFIRMED)
  - ashby:      header `Ashby-Signature`            = `sha256=` + hex(HMAC-SHA256(body))  (CONFIRMED, brex-shaped)
  (brex/ramp/gusto/deel/fireflies/miro/figma schemes mirror their signatures/*.py
   module constants, which are UNVERIFIED archetype defaults — see those modules'
   TODO(human). figma's real scheme is passcode-in-body, not an HMAC header.
   hibob's SHA512+base64 and ashby's SHA256+hex schemes are CONFIRMED from each
   vendor's first-party webhook docs.)

Per-provider tenant-resolution key (verified against tenant_resolver.py):
  - jira:       host of `issue.self`     (== provider_installations.installation_id)
  - mercury:    top-level `organizationId`
  - quickbooks: `eventNotifications[0].realmId`
  - grafana:    host of top-level `externalURL`
  - brex:       top-level `organizationId`
  - ramp:       `eventNotifications[0].business_id` (snake) / top-level `business_id`
  - gusto:      `eventNotifications[0].company_uuid` (snake) / top-level `company_uuid`
  - deel:       top-level `organizationId`
  - fireflies:  top-level `workspaceId`
  - miro:       top-level `organizationId`
  - figma:      top-level `team_id`
  - hibob:      top-level `companyId`
  - ashby:      top-level `organizationId`

Each `simulate_event` mints a DISTINCT entity (unique id + a current-window
timestamp) so the live observation is a fresh row, never an accidental
cross-path dedup against backfill. The payload fields are exactly what each
handler reads to derive `external_id` + `occurred_at`.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import httpx
from fastapi import FastAPI


log = logging.getLogger(__name__)

HMAC_PROVIDERS = (
    "jira", "mercury", "quickbooks", "grafana",
    "brex", "ramp", "gusto", "deel",
    "fireflies", "miro", "figma",
    "hibob", "ashby",
)

# Live timestamps land inside the observations partition window (2025-06..
# 2026-09) and at/after the 90-day backfill floor, distinct from the 2026-01
# fixture window so live ids never collide with backfill ids.
_LIVE_BASE_MS = 1781000000000  # ~2026-06-09T... within the partition window


@dataclass
class HmacWebhookResult:
    provider: str
    http_status: int
    external_hint: str
    response_body: dict[str, Any] = field(default_factory=dict)
    tenant_id: UUID | None = None
    was_tamper: bool = False


def _host_of(url: str) -> str:
    return url.replace("https://", "").replace("http://", "").rstrip("/").split("/")[0]


class HmacWebhookGenerator:
    """Drives one HMAC provider's live webhook ingress in-process.

    Construct one per provider against the shared gateway app + the provider's
    signing secret (the same value the runner exported as
    `WEBHOOK_SECRET_<PROVIDER>` with `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`).
    Use as an async context manager.
    """

    def __init__(
        self,
        *,
        app: FastAPI,
        provider: str,
        signing_secret: str,
    ) -> None:
        if provider not in HMAC_PROVIDERS:
            raise ValueError(f"unsupported HMAC provider {provider!r}")
        self._app = app
        self._provider = provider
        self._secret = signing_secret
        self._exit_stack = AsyncExitStack()
        self._client: httpx.AsyncClient | None = None
        self._seq = 0

    async def __aenter__(self) -> "HmacWebhookGenerator":
        self._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self._app),
            base_url=f"http://live-{self._provider}",
        )
        await self._exit_stack.enter_async_context(self._client)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self._exit_stack.aclose()

    # ---- Signing (byte-exact per provider) ----
    def _sign(self, body: bytes) -> str:
        # hibob is the ONE non-SHA256 scheme: HMAC-SHA512, base64, NO prefix
        # (CONFIRMED; matches signatures/hibob.py _HASH=sha512 + _DIGEST_ENCODING
        # ="base64" + _PREFIX=""). Branch first so the sha256 MAC below is never
        # built for it.
        if self._provider == "hibob":
            mac512 = hmac.new(self._secret.encode("utf-8"), body, hashlib.sha512)
            return base64.b64encode(mac512.digest()).decode("ascii")
        mac = hmac.new(self._secret.encode("utf-8"), body, hashlib.sha256)
        # `sha256=`+hex schemes: jira, mercury, brex, deel, fireflies, miro, figma,
        # ashby (ashby is brex-shaped: sha256= + hex, CONFIRMED).
        if self._provider in (
            "jira", "mercury", "brex", "deel", "fireflies", "miro", "figma",
            "ashby",
        ):
            return "sha256=" + mac.hexdigest()
        # base64-no-prefix schemes: quickbooks, ramp.
        if self._provider in ("quickbooks", "ramp"):
            return base64.b64encode(mac.digest()).decode("ascii")
        # bare lowercase hex (no prefix): grafana, gusto (gusto CONFIRMED hex —
        # docs.gusto.com / Gusto/gusto.github.io Ruby OpenSSL::HMAC.hexdigest).
        return mac.hexdigest()

    @property
    def _header_name(self) -> str:
        return {
            "jira": "X-Hub-Signature",
            "mercury": "Mercury-Signature",
            "quickbooks": "intuit-signature",
            "grafana": "X-Grafana-Alerting-Signature",
            "brex": "Brex-Signature",
            "ramp": "x-ramp-signature",
            "gusto": "X-Gusto-Signature",  # CONFIRMED (signatures/gusto.py)
            "deel": "Deel-Signature",
            # Must match each verifier's _HEADER_NAME constant (signatures/<src>.py).
            "fireflies": "x-hub-signature",  # CONFIRMED (docs.fireflies.ai)
            "miro": "X-Miro-Signature",
            "figma": "Figma-Signature",
            "hibob": "Bob-Signature",      # CONFIRMED (signatures/hibob.py)
            "ashby": "Ashby-Signature",    # CONFIRMED (signatures/ashby.py)
        }[self._provider]

    def _next_iso(self) -> tuple[str, str]:
        """A unique (id-suffix, ISO-8601 ms-Z) pair inside the partition
        window. The id-suffix keys the entity; the ISO is occurred_at."""
        self._seq += 1
        ms = _LIVE_BASE_MS + self._seq * 1000
        iso = (
            time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms / 1000.0))
            + f".{ms % 1000:03d}Z"
        )
        return f"{self._seq:06d}", iso

    # ---- Per-provider payload builders ----
    def _build_payload(
        self, target: "Any", content: str,
    ) -> tuple[dict[str, Any], str]:
        suffix, iso = self._next_iso()
        if self._provider == "jira":
            site = target.jira_site
            issue_id = f"live-{target.slug}-{suffix}"
            payload = {
                "webhookEvent": "jira:issue_updated",
                "issue": {
                    "id": issue_id,
                    "key": f"LIVE-{suffix}",
                    "self": f"https://{site}/rest/api/2/issue/{issue_id}",
                    "fields": {"summary": content, "updated": iso},
                },
            }
            return payload, f"jira:{site}:issue:{issue_id}:{iso}"
        if self._provider == "mercury":
            org, acct = target.mercury_org, target.mercury_account
            txn_id = f"live-{target.slug}-{suffix}"
            payload = {
                "type": "transaction.created",
                "organizationId": org,
                "transaction": {
                    "id": txn_id,
                    "accountId": acct,
                    "status": "sent",
                    "amount": -1000.0,
                    "counterpartyName": content,
                    "createdAt": iso,
                },
            }
            return payload, f"mercury:{acct}:txn:{txn_id}:sent"
        if self._provider == "quickbooks":
            realm, kind = target.qbo_realm, target.qbo_entity
            ent_id = f"live-{target.slug}-{suffix}"
            payload = {
                "eventNotifications": [{
                    "realmId": realm,
                    "dataChangeEvent": {"entities": [{
                        "name": kind, "id": ent_id,
                        "operation": "Update", "lastUpdated": iso,
                    }]},
                }],
            }
            return payload, f"qbo:{realm}:{kind.lower()}:{ent_id}:chg:{iso}"
        if self._provider == "brex":
            # Brex transaction.created webhook (Bearer/Mercury archetype). Body
            # matches handlers/brex.py: top-level `type` + `accountId` +
            # `transaction`. `organizationId` keys tenant_resolver._extract_brex.
            org, acct = target.brex_org, target.brex_account
            txn_id = f"live-{target.slug}-{suffix}"
            payload = {
                "type": "transaction.created",
                "organizationId": org,
                "accountId": acct,
                "transaction": {
                    "id": txn_id,
                    "accountId": acct,
                    "status": "posted",
                    "amount": -1000.0,
                    "counterpartyName": content,
                    "createdAt": iso,
                },
            }
            return payload, f"brex:{acct}:txn:{txn_id}:posted"
        if self._provider == "ramp":
            # REAL Ramp flat event (VERIFIED against docs.ramp.com): root-level
            # business_id is the tenant; `type` is dot.notation; `object.id` is
            # the affected resource; `id` is the stable event id (dedup key). No
            # eventNotifications wrapper. Signature header x-ramp-signature,
            # HMAC-SHA256 base64 (encoding still UNCONFIRMED upstream — see
            # signatures/ramp.py; generator+verifier stay in lockstep).
            biz = target.ramp_business
            ent_id = f"live-{target.slug}-{suffix}"
            event_id = f"evt-{target.slug}-{suffix}"
            payload = {
                "id": event_id,
                "type": "transactions.cleared",
                "created_at": iso,
                "business_id": biz,
                "object": {"id": ent_id},
            }
            return payload, f"ramp:{biz}:txn:{ent_id}:chg:{event_id}"
        if self._provider == "gusto":
            # REAL Gusto thin notification (flat snake_case, VERIFIED against
            # docs.gusto.com): resource_uuid is ALWAYS the company; entity_*
            # names the changed resource; the body has no entity payload (poll
            # re-fetch fills it). Signed as bare lowercase hex HMAC-SHA256 over
            # the raw body in X-Gusto-Signature; versioned by the delivery uuid.
            import datetime as _dt

            company = target.gusto_company
            ent_id = f"live-{target.slug}-{suffix}"
            event_uuid = f"evt-{target.slug}-{suffix}"
            ts_epoch = int(
                _dt.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
            )
            payload = {
                "uuid": event_uuid,
                "event_type": "employee.updated",
                "resource_type": "Company",
                "resource_uuid": company,
                "entity_type": "Employee",
                "entity_uuid": ent_id,
                "timestamp": ts_epoch,
            }
            return payload, f"gusto:{company}:employee:{ent_id}:chg:{event_uuid}"
        if self._provider == "deel":
            # Deel payment.created webhook (Bearer/Mercury archetype). Body
            # matches handlers/deel.py: top-level `type` + `contractId` +
            # `payment`. `organizationId` keys tenant_resolver._extract_deel.
            org = target.deel_org
            ctr = f"ctr-{target.slug}"
            pay_id = f"live-{target.slug}-{suffix}"
            payload = {
                "type": "payment.created",
                "organizationId": org,
                "contractId": ctr,
                "payment": {
                    "id": pay_id,
                    "contractId": ctr,
                    "status": "paid",
                    "amount": -1000.0,
                    "counterpartyName": content,
                    "createdAt": iso,
                },
            }
            return payload, f"deel:{ctr}:payment:{pay_id}:paid"
        if self._provider == "fireflies":
            # Fireflies transcript.completed webhook (Brex/HMAC archetype). Body
            # matches handlers/fireflies.py: top-level `type` + `workspaceId` +
            # `transcript`. `workspaceId` keys tenant_resolver._extract_fireflies.
            # The handler versions external_id by the transcript `version`; use
            # the live ISO so it is fresh (never dedups against backfill).
            ws = target.fireflies_workspace
            tid = f"live-{target.slug}-{suffix}"
            payload = {
                "type": "transcript.completed",
                "workspaceId": ws,
                "transcript": {
                    "id": tid,
                    "title": content,
                    "dateTime": iso,
                    "version": iso,
                },
            }
            return payload, f"fireflies:{ws}:transcript:{tid}:{iso}"
        if self._provider == "miro":
            # Miro board_item.created webhook (Brex/HMAC archetype). Body matches
            # handlers/miro.py: top-level `event` + `organizationId` + `item`.
            # `organizationId` keys tenant_resolver._extract_miro. The handler
            # versions external_id by the item `version`; use the live ISO.
            org = target.miro_org
            board = target.miro_board
            item_id = f"live-{target.slug}-{suffix}"
            payload = {
                "event": "board_item.created",
                "organizationId": org,
                "item": {
                    "id": item_id,
                    "boardId": board,
                    "version": iso,
                    "modifiedAt": iso,
                    "type": "sticky_note",
                    "data": {"content": content},
                },
            }
            return payload, f"miro:{org}:item:{item_id}:{iso}"
        if self._provider == "figma":
            # REAL Figma Webhooks V2 (VERIFIED against figma.com developers docs,
            # R2): PASSCODE-IN-BODY (no HMAC, no signature header) — the body
            # echoes the shared `passcode` (== signing secret) which the verifier
            # compares. The delivery carries a Figma-assigned `webhook_id` (the
            # install scope — keys tenant_resolver._extract_figma + the seeded
            # provider_installations row) and NO `team_id`, and has NO stable
            # event id, so the handler discriminates by (file_key, timestamp).
            webhook_id = target.figma_webhook_id
            file_key = target.figma_file
            payload = {
                "event_type": "FILE_VERSION_UPDATE",
                "passcode": self._secret,
                "webhook_id": webhook_id,
                "file_key": file_key,
                "file_name": content,
                "timestamp": iso,
                "triggered_by": {"id": f"user-{target.slug}", "handle": content},
            }
            return payload, f"figma:{webhook_id}:event:{file_key}:{iso}"
        if self._provider == "hibob":
            # HiBob employee.updated webhook (gusto-structure / Basic-service-user
            # archetype). Body matches handlers/hibob.py LIVE WEBHOOK path:
            # top-level `companyId` + `type` (`<kind>.<event>`) + a full `entity`
            # body. `companyId` keys tenant_resolver._extract_hibob. external_id is
            # versioned by the entity's `modified` field — use the live ISO so it
            # is fresh (never dedups against backfill).
            company = target.hibob_company
            emp_id = f"live-{target.slug}-{suffix}"
            payload = {
                "companyId": company,
                "type": "employee.updated",
                "entity": {
                    "id": emp_id,
                    "displayName": content,
                    "status": "active",
                    "modified": iso,
                },
            }
            # entity_kind="employee"; external_id = hibob:{co}:employee:{id}:{ver}.
            return payload, f"hibob:{company}:employee:{emp_id}:{iso}"
        if self._provider == "ashby":
            # Ashby applicationUpdate webhook (gusto-structure / API-key-as-Basic
            # archetype). Body matches handlers/ashby.py LIVE WEBHOOK path:
            # top-level `action` + `organizationId` + a `data` entity body.
            # `organizationId` keys tenant_resolver._extract_ashby. The entity's
            # `resourceType` resolves entity_kind; external_id is NOT versioned
            # (ashby:{org}:{kind}:{id}) — the live ISO `updatedAt` only sets
            # occurred_at, keeping the live row a fresh observation via its
            # globally-unique id.
            org = target.ashby_org
            app_id = f"live-{target.slug}-{suffix}"
            payload = {
                "action": "applicationUpdate",
                "organizationId": org,
                "data": {
                    "id": app_id,
                    "resourceType": "application",
                    "name": content,
                    "status": "active",
                    "updatedAt": iso,
                },
            }
            # entity_kind="application"; external_id = ashby:{org}:application:{id}.
            return payload, f"ashby:{org}:application:{app_id}"
        # grafana alert (the `grafana:alert` channel — distinct from the
        # backfill `grafana:annotation` channel).
        inst = target.grafana_instance
        fp = f"live-{target.slug}-{suffix}"
        payload = {
            "status": "firing",
            "externalURL": f"https://{inst}",
            "orgId": 1,
            "groupKey": "{}:{alertname=\"" + fp + "\"}",
            "commonLabels": {"alertname": fp, "service": content},
            "commonAnnotations": {"summary": content},
            "alerts": [{
                "status": "firing",
                "labels": {"alertname": fp},
                "annotations": {"summary": content},
                "startsAt": iso,
                "endsAt": "0001-01-01T00:00:00Z",
                "fingerprint": fp,
            }],
        }
        return payload, f"grafana:{inst}:alert:{fp}:firing"

    async def simulate_event(
        self,
        *,
        target: "Any",
        content: str = "live",
        tamper_signature: bool = False,
    ) -> HmacWebhookResult:
        """POST one signed webhook for `target`. Returns the HTTP outcome.

        `tamper_signature=True` sends a deliberately wrong signature — the
        gate must reject it (401/403) with no observation written."""
        assert self._client is not None
        payload, external_hint = self._build_payload(target, content)
        if self._provider == "figma" and tamper_signature:
            # Figma verifies a body passcode (no HMAC header), so its tamper probe
            # corrupts the passcode rather than a signature header.
            payload = {**payload, "passcode": "wrong-passcode-tamper"}
        body = json.dumps(payload).encode("utf-8")
        if not tamper_signature:
            signature = self._sign(body)
        elif self._provider == "hibob":
            # hibob's header value is a RAW base64 digest (no "sha256="/"sha512="
            # prefix), so the tamper probe is a wrong base64 value — NOT an
            # "sha256=fff…" string. The verifier (_PREFIX="") skips the prefix
            # check and rejects on the HMAC compare.
            signature = base64.b64encode(b"wrong-hibob-signature-tamper").decode("ascii")
        elif self._provider in (
            "jira", "mercury", "brex", "deel", "fireflies", "miro", "figma",
            "ashby",
        ):
            # `sha256=`+hex schemes (incl. ashby, brex-shaped): keep the prefix so
            # the verifier reaches the HMAC compare (and rejects on the wrong
            # digest, not the prefix).
            signature = "sha256=" + ("f" * 64)
        else:
            # base64 (quickbooks/ramp/gusto) + bare-hex (grafana): a wrong value.
            signature = "f" * 64
        # R3: Ashby resolves the tenant from a PER-INSTALL ENDPOINT URL
        # (`/webhooks/ashby/{installId}`), not a body field. Post to the
        # per-install path (installId == the seeded provider_installations id)
        # so the gate exercises the real path-based resolution. Other providers
        # post to the bare `/webhooks/{provider}` endpoint.
        post_path = f"/webhooks/{self._provider}"
        if self._provider == "ashby":
            post_path = f"/webhooks/ashby/{target.ashby_org}"
        response = await self._client.post(
            post_path,
            content=body,
            headers={
                "Content-Type": "application/json",
                self._header_name: signature,
            },
        )
        try:
            data = response.json()
            resp_body = data if isinstance(data, dict) else {"raw": data}
        except Exception:  # noqa: BLE001
            resp_body = {"raw": response.text[:500]}
        return HmacWebhookResult(
            provider=self._provider,
            http_status=response.status_code,
            external_hint=external_hint,
            response_body=resp_body,
            tenant_id=getattr(target, "tenant_id", None),
            was_tamper=tamper_signature,
        )


__all__ = ["HmacWebhookGenerator", "HmacWebhookResult", "HMAC_PROVIDERS"]
