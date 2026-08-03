import hashlib
import hmac
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.ingest.connector_platform.pilots import build_pilot_composition
from services.ingest.connector_platform.webhook_ingress import (
    execute_migrated_webhook,
)


@pytest.mark.asyncio
async def test_migrated_slack_webhook_resolves_native_capability() -> None:
    installation_id = uuid4()
    tenant_id = uuid4()
    secret = "signing-secret"
    received = datetime.now(timezone.utc).replace(microsecond=0)
    timestamp = str(int(received.timestamp()))
    payload = {
        "type": "event_callback",
        "team_id": "T1",
        "event": {"type": "message", "channel": "C1", "ts": "1.1"},
    }
    body = json.dumps(payload, separators=(",", ":")).encode()
    signature = "v0=" + hmac.new(
        secret.encode(), f"v0:{timestamp}:".encode() + body, hashlib.sha256
    ).hexdigest()

    class Pool:
        async def fetchrow(self, query, *args):
            if "FROM provider_installations" in query:
                return {
                    "id": installation_id,
                    "tenant_id": tenant_id,
                    "provider": "slack",
                    "installation_id": "T1",
                    "secret_ref": "signing-ref",
                    "enabled": True,
                }
            if "FROM source_connector_authority_grants" in query:
                return {
                    "installation_id": installation_id,
                    "tenant_id": tenant_id,
                    "connector_id": "fyralis/slack",
                    "authority_generation": 1,
                    "credential_owner": "oauth_callback",
                    "granted_secret_slots": [
                        "oauth_access_token",
                        "webhook_signing_secret",
                    ],
                    "granted_scopes": [
                        "channels:read",
                        "channels:history",
                        "groups:read",
                        "groups:history",
                        "users:read",
                        "team:read",
                    ],
                    "granted_outbound_hosts": ["slack.com"],
                    "maximum_trust_tier": "attested_agent",
                    "provenance": {},
                    "granted_at": received,
                    "revoked_at": None,
                }
            if "FROM source_connector_installations" in query:
                return {
                    "id": installation_id,
                    "tenant_id": tenant_id,
                    "connector_id": "fyralis/slack",
                    "external_installation_id": "T1",
                }
            raise AssertionError(query)

        async def fetchval(self, query, *args):
            if "FROM source_connector_credentials" in query:
                return "signing-ref"
            raise AssertionError(query)

    class Secrets:
        async def get(self, ref, *, tenant_id):
            return secret.encode()

        async def put(self, value, *, label, tenant_id):
            return "new-ref"

        async def rotate(self, ref, value, *, tenant_id):
            return None

        async def delete(self, ref, *, tenant_id):
            return None

    state = SimpleNamespace(
        source_connector_runtime=build_pilot_composition(),
        integration_runtime=SimpleNamespace(
            pool=Pool(),
            secret_store=Secrets(),
        ),
    )

    async def legacy_verify():
        raise AssertionError("native connector must be authoritative")

    verified = await execute_migrated_webhook(
        app_state=state,
        provider="slack",
        installation_row_id=installation_id,
        tenant_id=tenant_id,
        body=body,
        headers={
            "X-Slack-Request-Timestamp": timestamp,
            "X-Slack-Signature": signature,
        },
        legacy_verify=legacy_verify,
    )

    assert verified.provider == "slack"
    assert verified.tenant_hint == {"installation_id": "T1"}
