from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.ingest.connector_platform.pilots import build_pilot_composition
from services.ingest.connector_platform.whatsapp_ingress import (
    verify_migrated_whatsapp_webhook,
)
from services.ingest.connector_runtime.policy import ExecutionMode, RoutingPolicy
from services.ingest.integrations.whatsapp.signature import sign_payload


@pytest.mark.asyncio
async def test_whatsapp_signature_resolves_native_registry_capability() -> None:
    installation_id = uuid4()
    tenant_id = uuid4()
    secret = "meta-secret"
    payload = {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "metadata": {"phone_number_id": "P1"},
                            "messages": [{"id": "wamid.1", "from": "1555"}],
                        }
                    }
                ]
            }
        ]
    }
    body = json.dumps(payload, separators=(",", ":")).encode()

    class Pool:
        async def fetchrow(self, query, *_args):
            if "FROM source_connector_authority_grants" in query:
                return {
                    "installation_id": installation_id,
                    "tenant_id": tenant_id,
                    "connector_id": "fyralis/whatsapp",
                    "authority_generation": 1,
                    "credential_owner": "whatsapp_installations",
                    "granted_secret_slots": ["app_secret"],
                    "granted_scopes": [],
                    "granted_outbound_hosts": [],
                    "maximum_trust_tier": "attested_agent",
                    "provenance": {},
                    "granted_at": datetime.now(timezone.utc),
                    "revoked_at": None,
                }
            if "FROM source_connector_installations" in query:
                return {
                    "id": installation_id,
                    "tenant_id": tenant_id,
                    "connector_id": "fyralis/whatsapp",
                    "external_installation_id": "P1",
                }
            raise AssertionError(query)

        async def fetchval(self, query, *_args):
            if "FROM source_connector_credentials" in query:
                return "app-secret-ref"
            raise AssertionError(query)

    class Secrets:
        async def get(self, ref, *, tenant_id):
            assert ref == "app-secret-ref"
            return secret.encode()

    state = SimpleNamespace(
        source_connector_runtime=build_pilot_composition(
            RoutingPolicy(global_mode=ExecutionMode.CONNECTOR)
        ),
        integration_runtime=SimpleNamespace(pool=Pool(), secret_store=Secrets()),
    )

    async def legacy_verify() -> bool:
        raise AssertionError("legacy verifier must not be authoritative")

    verified = await verify_migrated_whatsapp_webhook(
        app_state=state,
        install={
            "id": installation_id,
            "tenant_id": tenant_id,
            "phone_number_id": "P1",
            "app_secret_ref": "app-secret-ref",
            "enabled": True,
        },
        body=body,
        headers={"X-Hub-Signature-256": sign_payload(secret, body)},
        legacy_verify=legacy_verify,
    )

    assert verified is True
