"""HmacWebhookGenerator — synthetic HMAC-signed webhooks for the finance/ops
providers that share the gateway webhook router + the M5.3 Kafka cutover:
**jira, mercury, quickbooks, grafana** plus the IN-FIN2 finance sources
**brex, ramp, gusto, deel**.

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
  (brex/ramp/gusto/deel schemes mirror their signatures/*.py module constants,
   which are UNVERIFIED archetype defaults — see those modules' TODO(human).)

Per-provider tenant-resolution key (verified against tenant_resolver.py):
  - jira:       host of `issue.self`     (== provider_installations.installation_id)
  - mercury:    top-level `organizationId`
  - quickbooks: `eventNotifications[0].realmId`
  - grafana:    host of top-level `externalURL`
  - brex:       top-level `organizationId`
  - ramp:       `eventNotifications[0].business_id` (snake) / top-level `business_id`
  - gusto:      `eventNotifications[0].company_uuid` (snake) / top-level `company_uuid`
  - deel:       top-level `organizationId`

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
        mac = hmac.new(self._secret.encode("utf-8"), body, hashlib.sha256)
        # `sha256=`+hex schemes: jira, mercury, brex, deel.
        if self._provider in ("jira", "mercury", "brex", "deel"):
            return "sha256=" + mac.hexdigest()
        # base64-no-prefix schemes: quickbooks, ramp, gusto.
        if self._provider in ("quickbooks", "ramp", "gusto"):
            return base64.b64encode(mac.digest()).decode("ascii")
        return mac.hexdigest()  # grafana: bare lowercase hex

    @property
    def _header_name(self) -> str:
        return {
            "jira": "X-Hub-Signature",
            "mercury": "Mercury-Signature",
            "quickbooks": "intuit-signature",
            "grafana": "X-Grafana-Alerting-Signature",
            "brex": "Brex-Signature",
            "ramp": "x-ramp-signature",
            "gusto": "Gusto-Signature",
            "deel": "Deel-Signature",
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
            # Ramp eventNotifications webhook (OAuth/QBO archetype). The handler
            # reads `businessId` (camel) inside the notification; the resolver
            # reads `business_id` (snake) — send BOTH. The handler emits a thin
            # change keyed by lastUpdated.
            biz = target.ramp_business
            ent_id = f"live-{target.slug}-{suffix}"
            payload = {
                "business_id": biz,
                "eventNotifications": [{
                    "business_id": biz,
                    "businessId": biz,
                    "dataChangeEvent": {"entities": [{
                        "name": "Invoice", "id": ent_id,
                        "operation": "Update", "lastUpdated": iso,
                    }]},
                }],
            }
            return payload, f"ramp:{biz}:txn:{ent_id}:chg:{iso}"
        if self._provider == "gusto":
            # Gusto eventNotifications webhook (OAuth/QBO archetype). The handler
            # reads `companyId` (camel); the resolver reads `company_uuid`
            # (snake) — send BOTH. Thin change keyed by lastUpdated.
            company = target.gusto_company
            ent_id = f"live-{target.slug}-{suffix}"
            payload = {
                "company_uuid": company,
                "eventNotifications": [{
                    "company_uuid": company,
                    "companyId": company,
                    "dataChangeEvent": {"entities": [{
                        "name": "Invoice", "id": ent_id,
                        "operation": "Update", "lastUpdated": iso,
                    }]},
                }],
            }
            return payload, f"gusto:{company}:invoice:{ent_id}:chg:{iso}"
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
        body = json.dumps(payload).encode("utf-8")
        if not tamper_signature:
            signature = self._sign(body)
        elif self._provider in ("jira", "mercury", "brex", "deel"):
            # `sha256=`+hex schemes: keep the prefix so the verifier reaches the
            # HMAC compare (and rejects on the wrong digest, not the prefix).
            signature = "sha256=" + ("f" * 64)
        else:
            # base64 (quickbooks/ramp/gusto) + bare-hex (grafana): a wrong value.
            signature = "f" * 64
        response = await self._client.post(
            f"/webhooks/{self._provider}",
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
