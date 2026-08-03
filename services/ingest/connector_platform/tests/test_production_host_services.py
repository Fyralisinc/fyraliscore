from uuid import uuid4

import httpx
import pytest

from services.ingest.connector_platform.production_host_services import (
    ProductionHostBackends,
    build_production_host_services_factory,
)
from services.ingest.source_contract.connector import GrantedAuthority
from services.ingest.source_contract.host_services import (
    InstallationDataPatch,
    SecretCandidate,
    SecretValue,
)
from services.ingest.source_contract.identity import SlotId


class _Pool:
    def __init__(self, installation_id, tenant_id) -> None:
        self.installation_id = installation_id
        self.tenant_id = tenant_id
        self.executed: list[tuple[str, tuple]] = []

    async def fetchrow(self, query, *args):
        if "FROM source_connector_installations" in query:
            return {
                "id": self.installation_id,
                "tenant_id": self.tenant_id,
                "connector_id": "fyralis/notion",
                "external_installation_id": "workspace-1",
            }
        if "INSERT INTO source_connector_installation_data" in query:
            return {"generation": 2}
        if "FROM source_connector_installation_data" in query:
            return {"generation": 1, "values": {"selected": ["page-1"]}}
        raise AssertionError(query)

    async def fetchval(self, query, *args):
        if "FROM source_connector_credentials" in query and "MAX" not in query:
            return "secret-current"
        if "MAX(generation)" in query:
            return 2
        raise AssertionError(query)

    async def execute(self, query, *args):
        self.executed.append((query, args))
        return "INSERT 0 1"


class _Secrets:
    def __init__(self) -> None:
        self.values = {"secret-current": b"current-token"}

    async def get(self, ref, *, tenant_id):
        return self.values[ref]

    async def put(self, value, *, label, tenant_id):
        self.values["secret-pending"] = (
            value.encode() if isinstance(value, str) else value
        )
        return "secret-pending"

    async def rotate(self, ref, new_plaintext, *, tenant_id):
        self.values[ref] = new_plaintext

    async def delete(self, ref, *, tenant_id):
        self.values.pop(ref, None)


@pytest.mark.asyncio
async def test_production_host_services_scope_secrets_cas_and_callbacks() -> None:
    installation_id = uuid4()
    tenant_id = uuid4()
    pool = _Pool(installation_id, tenant_id)
    secrets = _Secrets()
    async with httpx.AsyncClient() as client:
        factory = build_production_host_services_factory(
            ProductionHostBackends(
                pool=pool,
                secret_store=secrets,
                http_client=client,
                callback_base_url="https://gateway.fyralis.test",
            )
        )
        services = factory.build(
            installation_id,
            GrantedAuthority(
                secret_slots=frozenset({"oauth_access_token"}),
                outbound_hosts=frozenset({"api.notion.com"}),
            ),
            connector_id="fyralis/notion",
        )

        current = await services.secrets.resolve(SlotId("oauth_access_token"))
        assert current.reveal_text() == "current-token"

        candidate_ref = await services.secrets.store_candidate(
            SecretCandidate(
                slot=SlotId("oauth_access_token"),
                value=SecretValue.from_text("next-token"),
            )
        )
        assert candidate_ref == "secret-pending"

        data = await services.installation_store.read("selection")
        assert data is not None and data.values == {"selected": ["page-1"]}
        generation = await services.installation_store.compare_and_set(
            InstallationDataPatch(
                namespace="selection",
                expected_generation=1,
                values={"selected": ["page-2"]},
            )
        )
        assert generation == 2

        callback = await services.subscription_callbacks.allocate("events")
        assert callback.callback_url.startswith(
            "https://gateway.fyralis.test/connectors/fyralis/notion/webhook/"
        )
        assert callback.verification_nonce.reveal_text()

    assert any("source_connector_credentials" in query for query, _ in pool.executed)
    assert any("source_connector_callbacks" in query for query, _ in pool.executed)
