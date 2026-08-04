import hashlib
import hmac
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.ingest.connector_platform.catalog import build_connector_runtime
from services.ingest.connector_platform.webhook_ingress import (
    execute_connector_webhook,
)


@pytest.mark.asyncio
async def test_slack_webhook_uses_common_installation_and_emits_raw() -> None:
    installation_id = uuid4()
    tenant_id = uuid4()
    secret = "signing-secret"
    received = datetime.now(timezone.utc).replace(microsecond=0)
    timestamp = str(int(received.timestamp()))
    body = json.dumps(
        {
            "type": "event_callback",
            "team_id": "T1",
            "event": {"type": "message", "channel": "C1", "ts": "1.1"},
        },
        separators=(",", ":"),
    ).encode()
    signature = "v0=" + hmac.new(
        secret.encode(), f"v0:{timestamp}:".encode() + body, hashlib.sha256
    ).hexdigest()

    class Pool:
        async def fetchrow(self, query, *args):
            if "FROM source_connector_authority_grants" in query:
                return {
                    "installation_id": installation_id,
                    "tenant_id": tenant_id,
                    "connector_id": "fyralis/slack",
                    "authority_generation": 1,
                    "credential_owner": "connector_oauth",
                    "granted_slot_names": [
                        "oauth_access_token",
                        "oauth_user_access_token",
                        "webhook_signing_secret",
                    ],
                    "granted_scopes": [
                        "channels:read",
                        "channels:history",
                        "groups:read",
                        "groups:history",
                        "users:read",
                        "team:read",
                        "im:read",
                        "im:history",
                        "mpim:read",
                        "mpim:history",
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
                    "enabled": True,
                }
            raise AssertionError(query)

        async def fetchval(self, query, *args):
            if "FROM source_connector_credentials" in query:
                return "signing-ref"
            raise AssertionError(query)

    class Secrets:
        async def get(self, ref, *, tenant_id):
            assert ref == "signing-ref"
            return secret.encode()

    class S3:
        def __init__(self):
            self.items = {}

        async def put_if_absent(self, key, value):
            self.items.setdefault(key, value)

    class Kafka:
        def __init__(self):
            self.messages = []

        async def produce(self, **message):
            self.messages.append(message)

        async def flush(self, _timeout_seconds):
            return 0

    s3 = S3()
    kafka = Kafka()
    state = SimpleNamespace(
        source_connector_runtime=build_connector_runtime(),
        integration_runtime=SimpleNamespace(pool=Pool(), secret_store=Secrets()),
        s3_raw_client=s3,
        kafka_producer=kafka,
    )
    verified = await execute_connector_webhook(
        app_state=state,
        provider="slack",
        installation_id=installation_id,
        tenant_id=tenant_id,
        body=body,
        headers={
            "X-Slack-Request-Timestamp": timestamp,
            "X-Slack-Signature": signature,
        },
    )

    assert verified.provider == "slack"
    assert verified.tenant_hint == {"installation_id": "T1"}
    assert len(s3.items) == 1
    assert len(kafka.messages) == 1
