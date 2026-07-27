from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import UUID

from starlette.requests import Request

from services.app.webhooks import metrics
from services.app.webhooks.tenant_resolver import Resolved, UnknownInstallation
from services.app.webhooks.verifier import VerifiedContext
from services.ingest.integrations.quickbooks.webhook_ingress import (
    handle_verified_tenant,
)


_TENANT_A = UUID("00000000-0000-0000-0000-0000000000a1")
_TENANT_B = UUID("00000000-0000-0000-0000-0000000000b2")
_INSTALL_A = UUID("10000000-0000-0000-0000-0000000000a1")
_INSTALL_B = UUID("10000000-0000-0000-0000-0000000000b2")


def setup_function() -> None:
    metrics.reset()


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/webhooks/quickbooks",
            "headers": [(b"intuit-signature", b"signed")],
        }
    )


def _payload() -> dict[str, object]:
    return {
        "eventNotifications": [
            {
                "realmId": "realm-a",
                "dataChangeEvent": {
                    "entities": [
                        {
                            "name": "Invoice",
                            "id": "1",
                            "operation": "Update",
                            "lastUpdated": "2026-01-01T00:00:00Z",
                        },
                        {
                            "name": "Payment",
                            "id": "2",
                            "operation": "Create",
                            "lastUpdated": "2026-01-02T00:00:00Z",
                        },
                    ]
                },
            },
            {
                "realmId": "realm-b",
                "dataChangeEvent": {
                    "entities": [
                        {
                            "name": "Bill",
                            "id": "3",
                            "operation": "Delete",
                            "lastUpdated": "2026-01-03T00:00:00Z",
                        }
                    ]
                },
            },
        ]
    }


class _Resolver:
    def __init__(self, outcomes: dict[str, object]) -> None:
        self.outcomes = outcomes
        self.calls: list[tuple[str, dict[str, str], dict[str, str]]] = []

    async def resolve(
        self,
        provider: str,
        payload: dict[str, str],
        headers: dict[str, str],
    ) -> object:
        self.calls.append((provider, payload, headers))
        return self.outcomes[payload["realmId"]]


def _resolved(tenant_id: UUID, installation_id: UUID) -> Resolved:
    return Resolved(
        tenant_id=tenant_id,
        installation_row_id=installation_id,
        secret_ref="secret-ref",
    )


async def test_multi_realm_batch_resolves_each_realm_and_processes_every_unit() -> None:
    resolver = _Resolver(
        {
            "realm-a": _resolved(_TENANT_A, _INSTALL_A),
            "realm-b": _resolved(_TENANT_B, _INSTALL_B),
        }
    )
    processed: list[tuple[UUID, dict[str, object]]] = []

    async def process_unit(
        *,
        tenant_id: UUID,
        payload: dict[str, object],
    ) -> int:
        processed.append((tenant_id, payload))
        return 202 if payload["id"] == "3" else 200

    response = await handle_verified_tenant(
        request=_request(),
        runtime=SimpleNamespace(
            tenant_resolver=resolver,
            record_failure=metrics.record_failure,
        ),
        outcome=_resolved(_TENANT_A, _INSTALL_A),
        tenant_id=_TENANT_A,
        payload=_payload(),
        verified=VerifiedContext(
            provider="quickbooks",
            body=b"{}",
            secret_label="rotation-b",
        ),
        process_unit=process_unit,
    )

    assert response is not None
    assert response.status_code == 202
    assert response.headers["X-Secret-Label"] == "rotation-b"
    assert json.loads(response.body) == {
        "status": "accepted",
        "units": 3,
        "unknown_realms": 0,
        "secret_label": "rotation-b",
    }
    assert [(tenant, payload["id"]) for tenant, payload in processed] == [
        (_TENANT_A, "1"),
        (_TENANT_A, "2"),
        (_TENANT_B, "3"),
    ]
    assert [(provider, payload) for provider, payload, _headers in resolver.calls] == [
        ("quickbooks", {"realmId": "realm-a"}),
        ("quickbooks", {"realmId": "realm-b"}),
    ]


async def test_batch_skips_unknown_realm_without_cross_tenant_ingest() -> None:
    resolver = _Resolver(
        {
            "realm-a": _resolved(_TENANT_A, _INSTALL_A),
            "realm-b": UnknownInstallation(provider="quickbooks"),
        }
    )
    processed: list[tuple[UUID, str]] = []

    async def process_unit(
        *,
        tenant_id: UUID,
        payload: dict[str, object],
    ) -> int:
        processed.append((tenant_id, str(payload["id"])))
        return 200

    response = await handle_verified_tenant(
        request=_request(),
        runtime=SimpleNamespace(tenant_resolver=resolver),
        outcome=_resolved(_TENANT_A, _INSTALL_A),
        tenant_id=_TENANT_A,
        payload=_payload(),
        verified=VerifiedContext(provider="quickbooks", body=b"{}"),
        process_unit=process_unit,
    )

    assert response is not None
    assert response.status_code == 200
    assert json.loads(response.body)["unknown_realms"] == 1
    assert processed == [(_TENANT_A, "1"), (_TENANT_A, "2")]


async def test_batch_with_no_resolvable_units_is_rejected() -> None:
    resolver = _Resolver(
        {
            "realm-a": UnknownInstallation(provider="quickbooks"),
            "realm-b": UnknownInstallation(provider="quickbooks"),
        }
    )

    async def process_unit(**_: object) -> int:
        raise AssertionError("unknown realms must never be ingested")

    response = await handle_verified_tenant(
        request=_request(),
        runtime=SimpleNamespace(
            tenant_resolver=resolver,
            record_failure=metrics.record_failure,
        ),
        outcome=UnknownInstallation(provider="quickbooks"),
        tenant_id=None,
        payload=_payload(),
        verified=VerifiedContext(provider="quickbooks", body=b"{}"),
        process_unit=process_unit,
    )

    assert response is not None
    assert response.status_code == 401
    assert json.loads(response.body)["context"] == {
        "provider": "quickbooks",
        "reason": "unknown_installation",
    }
    assert metrics.get_count("quickbooks", "unknown_installation") == 1


async def test_empty_event_batch_is_a_validation_error() -> None:
    response = await handle_verified_tenant(
        request=_request(),
        runtime=SimpleNamespace(tenant_resolver=None),
        outcome=None,
        tenant_id=None,
        payload={"eventNotifications": []},
        verified=VerifiedContext(provider="quickbooks", body=b"{}"),
        process_unit=lambda **_: None,
    )

    assert response is not None
    assert response.status_code == 400
    assert json.loads(response.body)["code"] == "validation_error"
