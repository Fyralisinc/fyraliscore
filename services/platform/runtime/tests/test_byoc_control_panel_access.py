from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from services.platform.runtime.byoc_control_panel_access import (
    ByocControlPanelAccessGrant,
    ByocControlPanelAccessQuery,
    InMemoryByocControlPanelAccessGrantStore,
    PostgresByocControlPanelAccessGrantStore,
    evaluate_byoc_control_panel_access,
    model_json_schema_bundle,
    render_control_panel_access_schema_bundle_json,
)


TENANT_ID = UUID("11111111-1111-4111-8111-111111111111")
OTHER_TENANT_ID = UUID("22222222-2222-4222-8222-222222222222")
DEPLOYMENT_ID = "dep_access01"
OTHER_DEPLOYMENT_ID = "dep_access02"
CUSTOMER_ID = "cus_access01"
OTHER_CUSTOMER_ID = "cus_access02"
OBSERVED_AT = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)


class _FakeAccessGrantPool:
    def __init__(self) -> None:
        self.rows: dict[tuple[UUID, str, str], dict] = {}
        self.calls: list[tuple[str, tuple]] = []

    async def fetchrow(self, query: str, *args):
        self.calls.append((query, args))
        normalized = query.lstrip().upper()
        if normalized.startswith("INSERT"):
            row = {
                "tenant_id": args[0],
                "customer_id": args[1],
                "deployment_id": args[2],
                "role": args[3],
                "enabled": args[4],
                "granted_at": args[5],
                "expires_at": args[6],
                "stored_scope": args[7],
            }
            self.rows[(args[0], args[1], args[2])] = row
            return row
        if normalized.startswith("UPDATE"):
            key = (args[0], args[1], args[2])
            row = self.rows.get(key)
            if row is None:
                return None
            row = {**row, "enabled": False}
            self.rows[key] = row
            return row
        return None

    async def fetch(self, query: str, *args):
        self.calls.append((query, args))
        rows = list(self.rows.values())
        arg_index = 0
        tenant_id = args[arg_index]
        arg_index += 1
        rows = [row for row in rows if row["tenant_id"] == tenant_id]
        if "customer_id = $" in query:
            customer_id = args[arg_index]
            arg_index += 1
            rows = [row for row in rows if row["customer_id"] == customer_id]
        if "deployment_id = $" in query:
            deployment_id = args[arg_index]
            rows = [row for row in rows if row["deployment_id"] == deployment_id]
        return sorted(
            rows,
            key=lambda row: (row["customer_id"], row["deployment_id"]),
        )


def _grant(
    *,
    tenant_id: UUID = TENANT_ID,
    customer_id: str = CUSTOMER_ID,
    deployment_ids: tuple[str, ...] = (DEPLOYMENT_ID,),
    role: str = "viewer",
    enabled: bool = True,
    expires_at: datetime | None = None,
) -> ByocControlPanelAccessGrant:
    return ByocControlPanelAccessGrant(
        schema_version="fyralis.byoc.control_panel_access_grant.v1",
        tenant_id=tenant_id,
        customer_id=customer_id,
        deployment_ids=deployment_ids,
        role=role,
        enabled=enabled,
        granted_at=OBSERVED_AT - timedelta(days=1),
        expires_at=expires_at,
        stored_scope="sanitized_control_panel_access_metadata_only",
    )


def _query(
    *,
    tenant_id: UUID = TENANT_ID,
    deployment_id: str = DEPLOYMENT_ID,
    customer_id: str | None = CUSTOMER_ID,
) -> ByocControlPanelAccessQuery:
    return ByocControlPanelAccessQuery(
        tenant_id=tenant_id,
        deployment_id=deployment_id,
        customer_id=customer_id,
    )


def test_control_panel_access_allows_matching_tenant_customer_deployment() -> None:
    decision = evaluate_byoc_control_panel_access(
        query=_query(),
        grants=(_grant(role="operator"),),
        evaluated_at=OBSERVED_AT,
    )
    rendered = decision.model_dump_json()

    assert decision.schema_version == "fyralis.byoc.control_panel_access_decision.v1"
    assert decision.allowed is True
    assert decision.reason_code == "allowed"
    assert decision.role == "operator"
    assert decision.customer_id == CUSTOMER_ID
    assert decision.stored_scope == "sanitized_control_panel_access_metadata_only"
    assert "secret" not in rendered.lower()
    assert "token" not in rendered.lower()
    assert "signature" not in rendered.lower()
    assert "payload" not in rendered.lower()


def test_control_panel_access_derives_customer_when_query_omits_it() -> None:
    decision = evaluate_byoc_control_panel_access(
        query=_query(customer_id=None),
        grants=(_grant(),),
        evaluated_at=OBSERVED_AT,
    )

    assert decision.allowed is True
    assert decision.customer_id == CUSTOMER_ID


def test_control_panel_access_rejects_missing_grant() -> None:
    decision = evaluate_byoc_control_panel_access(
        query=_query(),
        grants=(),
        evaluated_at=OBSERVED_AT,
    )

    assert decision.allowed is False
    assert decision.reason_code == "grant_missing"
    assert decision.role is None


def test_control_panel_access_rejects_customer_mismatch() -> None:
    decision = evaluate_byoc_control_panel_access(
        query=_query(customer_id=OTHER_CUSTOMER_ID),
        grants=(_grant(customer_id=CUSTOMER_ID),),
        evaluated_at=OBSERVED_AT,
    )

    assert decision.allowed is False
    assert decision.reason_code == "customer_mismatch"


def test_control_panel_access_rejects_deployment_not_allowed() -> None:
    decision = evaluate_byoc_control_panel_access(
        query=_query(deployment_id=OTHER_DEPLOYMENT_ID),
        grants=(_grant(),),
        evaluated_at=OBSERVED_AT,
    )

    assert decision.allowed is False
    assert decision.reason_code == "deployment_not_allowed"


def test_control_panel_access_rejects_disabled_or_expired_grant() -> None:
    disabled = evaluate_byoc_control_panel_access(
        query=_query(),
        grants=(_grant(enabled=False),),
        evaluated_at=OBSERVED_AT,
    )
    expired = evaluate_byoc_control_panel_access(
        query=_query(),
        grants=(_grant(expires_at=OBSERVED_AT - timedelta(seconds=1)),),
        evaluated_at=OBSERVED_AT,
    )

    assert disabled.allowed is False
    assert disabled.reason_code == "grant_disabled"
    assert expired.allowed is False
    assert expired.reason_code == "grant_expired"


def test_control_panel_access_chooses_strongest_matching_role() -> None:
    decision = evaluate_byoc_control_panel_access(
        query=_query(),
        grants=(_grant(role="viewer"), _grant(role="admin")),
        evaluated_at=OBSERVED_AT,
    )

    assert decision.allowed is True
    assert decision.role == "admin"


def test_control_panel_access_rejects_duplicate_or_malformed_deployments() -> None:
    try:
        _grant(deployment_ids=(DEPLOYMENT_ID, DEPLOYMENT_ID))
    except ValidationError as exc:
        assert "deployment_ids" in str(exc)
    else:  # pragma: no cover - defensive assertion shape.
        raise AssertionError("duplicate deployment ids should fail validation")

    try:
        _grant(deployment_ids=("unsafe",))
    except ValidationError as exc:
        assert "deployment_ids" in str(exc)
    else:  # pragma: no cover - defensive assertion shape.
        raise AssertionError("malformed deployment ids should fail validation")


def test_control_panel_access_schema_bundle_is_exportable() -> None:
    bundle = model_json_schema_bundle()
    rendered = render_control_panel_access_schema_bundle_json()

    assert bundle["schema_version"] == "fyralis.byoc.control_panel_access_bundle.v1"
    assert bundle["grant"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.control_panel_access_grant.v1"
    )
    assert bundle["decision"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.control_panel_access_decision.v1"
    )
    assert bundle["stored_scope"] == "sanitized_control_panel_access_metadata_only"
    assert json.loads(rendered) == bundle


def test_control_panel_access_ignores_other_tenants() -> None:
    decision = evaluate_byoc_control_panel_access(
        query=_query(),
        grants=(_grant(tenant_id=OTHER_TENANT_ID),),
        evaluated_at=OBSERVED_AT,
    )

    assert decision.allowed is False
    assert decision.reason_code == "grant_missing"


@pytest.mark.asyncio
async def test_in_memory_control_panel_access_store_lists_and_revokes_metadata() -> None:
    store = InMemoryByocControlPanelAccessGrantStore()
    grant = _grant(deployment_ids=(DEPLOYMENT_ID, OTHER_DEPLOYMENT_ID))

    await store.put(grant)
    all_grants = await store.list_grants(tenant_id=TENANT_ID)
    filtered = await store.list_grants(
        tenant_id=TENANT_ID,
        customer_id=CUSTOMER_ID,
        deployment_id=OTHER_DEPLOYMENT_ID,
    )
    revoked = await store.revoke(
        tenant_id=TENANT_ID,
        customer_id=CUSTOMER_ID,
        deployment_id=OTHER_DEPLOYMENT_ID,
    )

    assert {item.deployment_ids[0] for item in all_grants} == {
        DEPLOYMENT_ID,
        OTHER_DEPLOYMENT_ID,
    }
    assert len(filtered) == 1
    assert filtered[0].deployment_ids == (OTHER_DEPLOYMENT_ID,)
    assert revoked is not None
    assert revoked.enabled is False


@pytest.mark.asyncio
async def test_postgres_control_panel_access_store_round_trips_scalar_metadata() -> None:
    pool = _FakeAccessGrantPool()
    store = PostgresByocControlPanelAccessGrantStore(pool)
    grant = _grant(role="admin")

    await store.put(grant)
    listed = await store.list_grants(
        tenant_id=TENANT_ID,
        customer_id=CUSTOMER_ID,
        deployment_id=DEPLOYMENT_ID,
    )
    revoked = await store.revoke(
        tenant_id=TENANT_ID,
        customer_id=CUSTOMER_ID,
        deployment_id=DEPLOYMENT_ID,
    )

    assert len(listed) == 1
    assert listed[0] == grant
    assert revoked is not None
    assert revoked.enabled is False
    flattened_args = json.dumps([str(arg) for _, args in pool.calls for arg in args])
    query_text = " ".join(query for query, _ in pool.calls)
    assert "read_key" not in query_text.lower()
    assert "endpoint_url" not in query_text.lower()
    assert "signature" not in query_text.lower()
    assert "payload" not in flattened_args.lower()
    assert "secret" not in flattened_args.lower()
