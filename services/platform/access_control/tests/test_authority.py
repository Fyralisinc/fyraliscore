from __future__ import annotations

import json
from uuid import UUID

import pytest

from lib.shared.ids import uuid7
from services.platform.access_control import authority
from services.platform.access_control.authority import (
    AuthorityDecision,
    AuthorityGrantError,
    ObjectRef,
    Principal,
    authorize_read,
    authorized_reader,
    authority_fingerprint,
    grant_read_authority,
    labels_for_observation_channel,
    labels_for_resource_kind,
    record_access_label,
    record_derived_access_labels,
    record_provenance_edge,
    revoke_read_authority,
)
from services.platform.access_control.checks import AccessDecision


pytestmark = pytest.mark.asyncio


class _FakeConn:
    def __init__(
        self,
        *,
        tenant_id: UUID,
        labels: list[dict] | None = None,
        provenance: list[dict] | None = None,
        grants: list[dict] | None = None,
        rows: dict[tuple[str, UUID], dict] | None = None,
        grant_rows: dict[UUID, dict] | None = None,
    ) -> None:
        self.tenant_id = tenant_id
        self.labels = labels or []
        self.provenance = provenance or []
        self.grants = grants or []
        self.rows = rows or {}
        self.grant_rows = grant_rows or {}
        self.executed: list[tuple[str, tuple]] = []

    async def fetch(self, query: str, *args):
        if "FROM object_access_labels" in query:
            tenant_id, object_kind, object_id = args
            return [
                row
                for row in self.labels
                if row["tenant_id"] == tenant_id
                and row["object_kind"] == object_kind
                and row["object_id"] == object_id
            ]
        if "FROM object_provenance_edges" in query:
            tenant_id, derived_kind, derived_id = args
            return [
                row
                for row in self.provenance
                if row["tenant_id"] == tenant_id
                and row["derived_kind"] == derived_kind
                and row["derived_id"] == derived_id
            ]
        return []

    async def fetchval(self, query: str, *args):
        if "FROM access_grant_epochs" in query:
            return 0
        if "FROM read_authority_grants" not in query:
            return None
        tenant_id, actor_id, purpose = args[:3]
        active = [
            grant
            for grant in self.grants
            if grant["tenant_id"] == tenant_id
            and grant["grantee_actor_id"] == actor_id
            and grant.get("revoked_at") is None
            and grant["purpose"] in (purpose, "*")
        ]
        if "grant_kind = 'object'" in query:
            object_kind, object_id = args[3], args[4]
            return next(
                (
                    1
                    for grant in active
                    if grant["grant_kind"] == "object"
                    and grant["object_kind"] == object_kind
                    and grant["object_id"] == object_id
                ),
                None,
            )
        if "grant_kind = 'label'" in query:
            labels = set(args[3])
            return next(
                (
                    1
                    for grant in active
                    if grant["grant_kind"] == "label"
                    and grant["label"].lower() in labels
                ),
                None,
            )
        return None

    async def fetchrow(self, query: str, *args):
        if "FROM read_authority_grants" in query:
            return self.grant_rows.get(args[1])
        tenant_id, object_id = args
        for (object_kind, oid), row in self.rows.items():
            if oid == object_id and row["tenant_id"] == tenant_id:
                return row | {"id": object_id, "kind": object_kind}
        return None

    async def execute(self, query: str, *args):
        self.executed.append((query, args))
        if query.lstrip().upper().startswith("UPDATE"):
            return "UPDATE 1"
        return "INSERT 0 1"


def _label(
    tenant_id: UUID,
    object_kind: str,
    object_id: UUID,
    label: str,
) -> dict:
    return {
        "tenant_id": tenant_id,
        "object_kind": object_kind,
        "object_id": object_id,
        "label": label,
        "source": "test",
        "metadata": {},
    }


def _edge(
    tenant_id: UUID,
    derived_kind: str,
    derived_id: UUID,
    source_kind: str,
    source_id: UUID,
) -> dict:
    return {
        "tenant_id": tenant_id,
        "derived_kind": derived_kind,
        "derived_id": derived_id,
        "source_kind": source_kind,
        "source_id": source_id,
        "derivation_kind": "test",
        "metadata": {},
    }


def _grant(
    tenant_id: UUID,
    actor_id: UUID,
    *,
    grant_kind: str,
    purpose: str = "ask",
    object_kind: str | None = None,
    object_id: UUID | None = None,
    label: str | None = None,
) -> dict:
    return {
        "tenant_id": tenant_id,
        "grantee_actor_id": actor_id,
        "purpose": purpose,
        "grant_kind": grant_kind,
        "object_kind": object_kind,
        "object_id": object_id,
        "label": label,
        "revoked_at": None,
    }


async def test_authority_fingerprint_changes_with_authority_inputs():
    tenant = uuid7()
    actor = uuid7()
    base = Principal(tenant_id=tenant, actor_id=actor)
    base_fp = authority_fingerprint(base, "ask").cache_key

    assert authority_fingerprint(
        Principal(tenant_id=tenant, actor_id=actor, roles=("finance",)),
        "ask",
    ).cache_key != base_fp
    assert authority_fingerprint(
        Principal(tenant_id=tenant, actor_id=actor, active_grant_epoch=1),
        "ask",
    ).cache_key != base_fp
    assert authority_fingerprint(base, "today").cache_key != base_fp
    assert authority_fingerprint(base, "ask", scope={"card": "acme"}).cache_key != base_fp


@pytest.fixture
def patch_base_can_read(monkeypatch):
    decisions: dict[tuple[str, UUID], AccessDecision] = {}

    async def fake_can_read_by_id(actor_id, kind, entity_id, *, conn, tenant_id):
        return decisions.get((kind, entity_id), AccessDecision(True, "base_allowed"))

    monkeypatch.setattr(authority, "can_read_by_id", fake_can_read_by_id)
    return decisions


async def test_tenant_mismatch_denies_without_db(patch_base_can_read):
    tenant = uuid7()
    actor = uuid7()
    other_tenant_object = ObjectRef(
        tenant_id=uuid7(),
        object_kind="model",
        object_id=uuid7(),
    )
    conn = _FakeConn(tenant_id=tenant)
    decision = await authorize_read(
        Principal(tenant_id=tenant, actor_id=actor),
        "ask",
        other_tenant_object,
        conn=conn,  # type: ignore[arg-type]
    )
    assert not decision.allowed
    assert decision.reason == "tenant_mismatch"


async def test_restricted_label_denies_without_role_or_grant(patch_base_can_read):
    tenant = uuid7()
    actor = uuid7()
    model = uuid7()
    conn = _FakeConn(
        tenant_id=tenant,
        labels=[_label(tenant, "model", model, "classification:restricted")],
    )
    decision = await authorize_read(
        Principal(tenant_id=tenant, actor_id=actor),
        "ask",
        ObjectRef(tenant_id=tenant, object_kind="model", object_id=model),
        conn=conn,  # type: ignore[arg-type]
    )
    assert not decision.allowed
    assert decision.reason == "label_denied:classification:restricted"


async def test_label_grant_allows_restricted_label(patch_base_can_read):
    tenant = uuid7()
    actor = uuid7()
    model = uuid7()
    label = "classification:restricted"
    conn = _FakeConn(
        tenant_id=tenant,
        labels=[_label(tenant, "model", model, label)],
        grants=[_grant(tenant, actor, grant_kind="label", label=label)],
    )
    decision = await authorize_read(
        Principal(tenant_id=tenant, actor_id=actor),
        "ask",
        ObjectRef(tenant_id=tenant, object_kind="model", object_id=model),
        conn=conn,  # type: ignore[arg-type]
    )
    assert decision.allowed
    assert decision.delegation_applied
    assert decision.audit_required


async def test_finance_role_allows_financial_label(patch_base_can_read):
    tenant = uuid7()
    actor = uuid7()
    model = uuid7()
    conn = _FakeConn(
        tenant_id=tenant,
        labels=[_label(tenant, "model", model, "domain:financial")],
    )
    decision = await authorize_read(
        Principal(tenant_id=tenant, actor_id=actor, roles=("finance",)),
        "ask",
        ObjectRef(tenant_id=tenant, object_kind="model", object_id=model),
        conn=conn,  # type: ignore[arg-type]
    )
    assert decision.allowed
    assert not decision.delegation_applied


async def test_object_grant_can_authorize_base_denied_object(patch_base_can_read):
    tenant = uuid7()
    actor = uuid7()
    model = uuid7()
    patch_base_can_read[("model", model)] = AccessDecision(False, "model_out_of_scope")
    conn = _FakeConn(
        tenant_id=tenant,
        grants=[
            _grant(
                tenant,
                actor,
                grant_kind="object",
                object_kind="model",
                object_id=model,
            )
        ],
    )
    decision = await authorize_read(
        Principal(tenant_id=tenant, actor_id=actor),
        "ask",
        ObjectRef(tenant_id=tenant, object_kind="model", object_id=model),
        conn=conn,  # type: ignore[arg-type]
    )
    assert decision.allowed
    assert decision.reason == "delegated_read"
    assert decision.delegation_applied


async def test_provenance_source_denial_blocks_derived_model(patch_base_can_read):
    tenant = uuid7()
    actor = uuid7()
    model = uuid7()
    source_obs = uuid7()
    patch_base_can_read[("observation", source_obs)] = AccessDecision(
        False,
        "observation_out_of_scope",
    )
    conn = _FakeConn(
        tenant_id=tenant,
        provenance=[
            _edge(tenant, "model", model, "observation", source_obs),
        ],
    )
    decision = await authorize_read(
        Principal(tenant_id=tenant, actor_id=actor),
        "ask",
        ObjectRef(tenant_id=tenant, object_kind="model", object_id=model),
        conn=conn,  # type: ignore[arg-type]
    )
    assert not decision.allowed
    assert decision.reason == "provenance_denied:observation"
    assert decision.provenance_considered == (
        ObjectRef(tenant_id=tenant, object_kind="observation", object_id=source_obs),
    )


async def test_derived_evidence_authorizes_through_allowed_provenance(
    patch_base_can_read,
):
    tenant = uuid7()
    actor = uuid7()
    evidence = uuid7()
    source_obs = uuid7()
    conn = _FakeConn(
        tenant_id=tenant,
        provenance=[
            _edge(tenant, "evidence", evidence, "observation", source_obs),
        ],
    )

    decision = await authorize_read(
        Principal(tenant_id=tenant, actor_id=actor),
        "ask",
        ObjectRef(tenant_id=tenant, object_kind="evidence", object_id=evidence),
        conn=conn,  # type: ignore[arg-type]
    )

    assert decision.allowed
    assert decision.reason == "authorized"
    assert decision.base_reason == "derived_authority:evidence"
    assert decision.provenance_considered == (
        ObjectRef(tenant_id=tenant, object_kind="observation", object_id=source_obs),
    )


async def test_derived_evidence_without_provenance_fails_closed(patch_base_can_read):
    tenant = uuid7()
    actor = uuid7()
    evidence = uuid7()
    conn = _FakeConn(tenant_id=tenant)

    decision = await authorize_read(
        Principal(tenant_id=tenant, actor_id=actor),
        "ask",
        ObjectRef(tenant_id=tenant, object_kind="evidence", object_id=evidence),
        conn=conn,  # type: ignore[arg-type]
    )

    assert not decision.allowed
    assert decision.reason == "unsupported_object_kind:evidence"


async def test_authorized_reader_returns_none_when_denied(patch_base_can_read):
    tenant = uuid7()
    actor = uuid7()
    model = uuid7()
    patch_base_can_read[("model", model)] = AccessDecision(False, "model_out_of_scope")
    conn = _FakeConn(tenant_id=tenant)
    reader = authorized_reader(
        Principal(tenant_id=tenant, actor_id=actor),
        "ask",
        conn=conn,  # type: ignore[arg-type]
    )
    assert await reader.get_model(model) is None


async def test_authorized_reader_fetches_after_authorization(patch_base_can_read):
    tenant = uuid7()
    actor = uuid7()
    model = uuid7()
    conn = _FakeConn(
        tenant_id=tenant,
        rows={("model", model): {"tenant_id": tenant, "natural": "safe fact"}},
    )
    reader = authorized_reader(
        Principal(tenant_id=tenant, actor_id=actor),
        "ask",
        conn=conn,  # type: ignore[arg-type]
    )
    row = await reader.get_model(model)
    assert row is not None
    assert row["natural"] == "safe fact"


async def test_record_label_and_provenance_write_jsonb():
    tenant = uuid7()
    model = uuid7()
    obs = uuid7()
    conn = _FakeConn(tenant_id=tenant)

    await record_access_label(
        conn=conn,  # type: ignore[arg-type]
        tenant_id=tenant,
        object_kind="model",
        object_id=model,
        label="domain:financial",
        source="test",
        metadata={"confidence": 0.9},
    )
    await record_provenance_edge(
        conn=conn,  # type: ignore[arg-type]
        tenant_id=tenant,
        derived_kind="model",
        derived_id=model,
        source_kind="observation",
        source_id=obs,
        derivation_kind="supporting_event",
        metadata={"role": "supporting"},
    )

    assert len(conn.executed) == 2
    assert conn.executed[0][1][-1] == '{"confidence": 0.9}'
    assert conn.executed[1][1][-1] == '{"role": "supporting"}'


async def test_label_derivation_and_derived_label_copy():
    tenant = uuid7()
    model = uuid7()
    obs = uuid7()
    conn = _FakeConn(tenant_id=tenant)

    assert labels_for_resource_kind("financial") == (
        "classification:internal",
        "resource_kind:financial",
        "domain:financial",
    )
    assert labels_for_observation_channel("ramp:transaction") == (
        "classification:internal",
        "domain:financial",
        "channel:finance",
    )

    await record_derived_access_labels(
        conn=conn,  # type: ignore[arg-type]
        tenant_id=tenant,
        derived_kind="model",
        derived_id=model,
        source_refs=[
            ObjectRef(tenant_id=tenant, object_kind="observation", object_id=obs),
        ],
    )

    query, args = conn.executed[0]
    assert "JOIN object_access_labels" in query
    assert args[0:4] == (tenant, "model", model, "provenance")
    assert args[4] == ["observation"]
    assert args[5] == [obs]


async def test_derived_label_copy_dedupes_shared_source_labels(tx_conn, tenant):
    derived_model = uuid7()
    source_a = uuid7()
    source_b = uuid7()

    await record_access_label(
        conn=tx_conn,
        tenant_id=tenant,
        object_kind="observation",
        object_id=source_a,
        label="classification:internal",
        source="source_channel",
        metadata={"source_channel": "slack:customer"},
    )
    await record_access_label(
        conn=tx_conn,
        tenant_id=tenant,
        object_kind="observation",
        object_id=source_b,
        label="classification:internal",
        source="source_channel",
        metadata={"source_channel": "email:customer"},
    )

    await record_derived_access_labels(
        conn=tx_conn,
        tenant_id=tenant,
        derived_kind="model",
        derived_id=derived_model,
        source_refs=[
            ObjectRef(
                tenant_id=tenant,
                object_kind="observation",
                object_id=source_a,
            ),
            ObjectRef(
                tenant_id=tenant,
                object_kind="observation",
                object_id=source_b,
            ),
        ],
        source="model_provenance",
    )

    rows = await tx_conn.fetch(
        """
        SELECT label, source, metadata
        FROM object_access_labels
        WHERE tenant_id = $1
          AND object_kind = 'model'
          AND object_id = $2
        ORDER BY label, source
        """,
        tenant,
        derived_model,
    )

    assert len(rows) == 1
    assert rows[0]["label"] == "classification:internal"
    assert rows[0]["source"] == "model_provenance"
    metadata = rows[0]["metadata"]
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    assert metadata["source_count"] == 2
    assert {
        source["source_id"]
        for source in metadata["sources"]
    } == {str(source_a), str(source_b)}


async def test_grant_read_authority_rejects_non_authoritative_object_grant(
    monkeypatch,
):
    tenant = uuid7()
    grantor = uuid7()
    grantee = uuid7()
    model = uuid7()
    conn = _FakeConn(tenant_id=tenant)

    async def fake_principal_for_actor(actor_id, *, conn, tenant_id):
        return Principal(tenant_id=tenant_id, actor_id=actor_id)

    async def fake_authorize_read(principal, purpose, object_ref, *, conn):
        return AuthorityDecision(False, "model_out_of_scope")

    monkeypatch.setattr(authority, "principal_for_actor", fake_principal_for_actor)
    monkeypatch.setattr(authority, "authorize_read", fake_authorize_read)

    with pytest.raises(AuthorityGrantError):
        await grant_read_authority(
            conn=conn,  # type: ignore[arg-type]
            tenant_id=tenant,
            grantee_actor_id=grantee,
            granted_by_actor_id=grantor,
            purpose="ask",
            grant_kind="object",
            object_ref=ObjectRef(tenant_id=tenant, object_kind="model", object_id=model),
            reason="needs review",
        )
    assert conn.executed == []


async def test_grant_read_authority_inserts_label_grant_for_role_authority(
    monkeypatch,
):
    tenant = uuid7()
    grantor = uuid7()
    grantee = uuid7()
    conn = _FakeConn(tenant_id=tenant)

    async def fake_principal_for_actor(actor_id, *, conn, tenant_id):
        return Principal(tenant_id=tenant_id, actor_id=actor_id, roles=("finance",))

    monkeypatch.setattr(authority, "principal_for_actor", fake_principal_for_actor)

    grant_id = await grant_read_authority(
        conn=conn,  # type: ignore[arg-type]
        tenant_id=tenant,
        grantee_actor_id=grantee,
        granted_by_actor_id=grantor,
        purpose="ask",
        grant_kind="label",
        label="domain:financial",
        reason="finance review",
    )

    assert isinstance(grant_id, UUID)
    assert len(conn.executed) == 1
    assert conn.executed[0][1][5] == "label"
    assert conn.executed[0][1][8] == "domain:financial"


async def test_revoke_read_authority_updates_when_revoker_is_grantor(monkeypatch):
    tenant = uuid7()
    grantor = uuid7()
    grant_id = uuid7()
    conn = _FakeConn(
        tenant_id=tenant,
        grant_rows={
            grant_id: {
                "id": grant_id,
                "tenant_id": tenant,
                "granted_by_actor_id": grantor,
                "revoked_at": None,
            }
        },
    )

    async def fake_principal_for_actor(actor_id, *, conn, tenant_id):
        return Principal(tenant_id=tenant_id, actor_id=actor_id)

    monkeypatch.setattr(authority, "principal_for_actor", fake_principal_for_actor)

    revoked = await revoke_read_authority(
        conn=conn,  # type: ignore[arg-type]
        tenant_id=tenant,
        grant_id=grant_id,
        revoked_by_actor_id=grantor,
        reason="done",
    )

    assert revoked is True
    assert len(conn.executed) == 1
    assert "UPDATE read_authority_grants" in conn.executed[0][0]
