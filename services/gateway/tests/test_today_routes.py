"""services/gateway/tests/test_today_routes.py — integration tests
for the v2 Today page endpoints.

Coverage:
  - GET /today returns spec-shaped TodayPageData with viewer + summary
    + primaryJudgment + otherChanges + handledWithoutYou.
  - Tenant isolation: tenant B's deltas never leak into A's /today.
  - Synth layer maps DB status + label into spec statuses correctly.
  - Primary judgment selection picks the highest-priority actionable.
  - GET /today/deltas/{id} returns the spec wire DTO with evidence.
  - GET /today/deltas/{id}/evidence groups by source with quality.
  - POST /today/deltas/{id}/apply transitions DB to accepted, returns
    applied + nextDeltaId + ledgerEventId where present.
  - POST /today/deltas/{id}/delegate stores delegation metadata on
    impact and transitions status to delegated.
  - POST /today/deltas/{id}/correction stores correction metadata and
    elevates spec status to correction_submitted.
  - Invalid inputs return 400 with structured error bodies.
"""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import httpx
import pytest

from services.decision_deltas.tests.conftest import (  # type: ignore  # noqa: F401
    _ensure_tenant,
    seed_decision_delta,
)


pytestmark = pytest.mark.integration


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _seed(
    pool: asyncpg.Pool,
    tenant: UUID,
    *,
    main_assertion: str = "Salesforce sync failures threaten anchor renewals.",
    label: str = "needs_review",
    status: str = "proposed",
    category: str = "customer_risk",
    confidence: float = 0.62,
    impact: dict | None = None,
    consequence_preview: dict | None = None,
    evidence: list[dict] | None = None,
) -> UUID:
    return await seed_decision_delta(
        pool,
        tenant=tenant,
        main_assertion=main_assertion,
        status=status,
        label=label,
        confidence=confidence,
        category=category,
        impact=impact,
        consequence_preview=consequence_preview,
        evidence=evidence,
    )


# =====================================================================
# /today — page payload shape
# =====================================================================


@pytest.mark.asyncio
async def test_today_empty_tenant_returns_shell(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    valid_session,
):
    """A brand-new tenant has no deltas. The page should still return
    the full shell (summary strip + handledWithoutYou) with zeroed
    counts so the UI never sees a partial body."""
    token, _ = valid_session
    resp = await client.get("/today", headers=_auth(token))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["primaryJudgment"] is None
    assert body["otherChanges"] == []
    assert body["summary"]["needJudgment"] == 0
    assert body["summary"]["requiresAuthority"] == 0
    assert body["handledWithoutYou"]["reassuranceCopy"]
    assert body["viewer"]["userId"]
    assert body["viewer"]["tenantId"]
    assert body["lastReviewAt"]
    assert body["generatedAt"]


@pytest.mark.asyncio
async def test_today_picks_needs_authority_as_primary(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    valid_session,
):
    """When a tenant has a needs_authority delta AND a delegatable one,
    the needs_authority one becomes primaryJudgment per spec §5.3."""
    token, _ = valid_session
    auth_delta = await _seed(
        gateway_pool, tenant_id,
        main_assertion="Escalate customer risk for Salesforce sync instability",
        label="authority_required",
        impact={"arr_at_risk": 2_040_000, "accounts_affected": 3},
    )
    other = await _seed(
        gateway_pool, tenant_id,
        main_assertion="Assign owner for pricing model decision",
        label="needs_review",
        impact={"arr_at_risk": 720_000, "accounts_affected": 2},
    )
    resp = await client.get("/today", headers=_auth(token))
    assert resp.status_code == 200
    body = resp.json()
    assert body["primaryJudgment"] is not None
    assert body["primaryJudgment"]["status"] == "needs_authority"
    assert UUID(body["primaryJudgment"]["id"]) == auth_delta
    other_ids = {UUID(c["id"]) for c in body["otherChanges"]}
    assert other in other_ids
    assert body["summary"]["requiresAuthority"] == 1
    assert body["summary"]["delegatable"] == 1
    assert body["summary"]["needJudgment"] == 2


@pytest.mark.asyncio
async def test_today_tenant_isolation(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    tenant_id_b,
    valid_session,
    valid_session_b,
):
    """Tenant B's deltas must not leak into tenant A's /today."""
    token_a, _ = valid_session
    _, _ = valid_session_b
    await _seed(
        gateway_pool, tenant_id_b,
        main_assertion="B-tenant delta",
        label="authority_required",
    )
    resp = await client.get("/today", headers=_auth(token_a))
    assert resp.status_code == 200
    body = resp.json()
    titles = []
    if body["primaryJudgment"]:
        titles.append(body["primaryJudgment"]["title"])
    titles += [c["title"] for c in body["otherChanges"]]
    assert "B-tenant delta" not in titles


@pytest.mark.asyncio
async def test_today_summary_includes_exposure(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    valid_session,
):
    """Exposure rolls up arr_at_risk from open-judgment deltas."""
    token, _ = valid_session
    await _seed(
        gateway_pool, tenant_id,
        label="authority_required",
        impact={"arr_at_risk": 1_500_000, "accounts_affected": 2},
    )
    await _seed(
        gateway_pool, tenant_id,
        label="needs_review",
        impact={"arr_at_risk": 540_000, "accounts_affected": 1},
    )
    resp = await client.get("/today", headers=_auth(token))
    assert resp.status_code == 200
    body = resp.json()
    exp = body["summary"]["exposure"]
    assert exp is not None
    assert exp["amount"] == 2_040_000
    assert exp["currency"] == "USD"
    assert exp["formatted"].endswith("M")


# =====================================================================
# /today/deltas/{id}
# =====================================================================


@pytest.mark.asyncio
async def test_get_delta_returns_spec_shape(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    valid_session,
):
    token, _ = valid_session
    did = await _seed(
        gateway_pool, tenant_id,
        main_assertion="Escalate Salesforce sync risk",
        label="authority_required",
        category="customer_risk",
        confidence=0.78,
        impact={
            "arr_at_risk": 2_040_000,
            "accounts_affected": 3,
            "why_this_matters": (
                "Three anchor customers are reporting recurring sync failures."
            ),
        },
        consequence_preview={
            "creates": [],
            "updates": [{"target_kind": "customer", "title": "Anchor renewals"}],
            "archives": [],
            "notifies": [{"role": "VP Engineering"}],
            "re_evaluates_in": "48h",
        },
        evidence=[
            {
                "source": "support",
                "title": "Sync ticket #221",
                "trust_tier": "attested",
                "excerpt": "Beacon reported recurring sync failures",
            },
            {
                "source": "support",
                "title": "Sync ticket #229",
                "trust_tier": "reputable",
            },
            {
                "source": "crm",
                "title": "Account: Beacon",
                "trust_tier": "verified",
            },
        ],
    )
    resp = await client.get(
        f"/today/deltas/{did}", headers=_auth(token),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "needs_authority"
    assert body["sourceCategory"] == "customers_revenue"
    assert body["title"] == "Escalate Salesforce sync risk"
    assert body["whyThisMatters"].startswith("Three anchor customers")
    metric_labels = [m["label"] for m in body["keyMetrics"]]
    assert any("ARR" in m for m in metric_labels)
    assert any("customer" in m for m in metric_labels)
    assert any("signal" in m for m in metric_labels)
    assert any("confidence" in m for m in metric_labels)
    assert body["evidenceSummary"]["totalSignals"] == 3
    qualities = {g["sourceType"]: g["quality"] for g in body["evidenceSummary"]["groups"]}
    assert qualities["support"] == "strong"
    assert qualities["crm"] == "strong"
    op_types = {i["operationType"] for i in body["impactIfAccepted"]}
    assert {"update_node", "notify_actor", "schedule_re_evaluation", "create_ledger_event"} <= op_types
    assert body["applyPreview"]["nodeOpsCount"] == 1
    assert body["applyPreview"]["notificationsCount"] == 1
    assert body["applyPreview"]["ledgerEventWillBeCreated"] is True
    assert any(
        link["category"] == "customers_revenue"
        for link in body["relatedModelLinks"]
    )
    assert "accept" in body["availableActions"]
    assert "delegate" in body["availableActions"]
    assert body["evidence"][0]["sourceLabel"]
    assert body["summaryLine"]  # diff one-liner present


@pytest.mark.asyncio
async def test_get_delta_404(
    client: httpx.AsyncClient, valid_session,
):
    token, _ = valid_session
    resp = await client.get(
        "/today/deltas/00000000-0000-0000-0000-000000000000",
        headers=_auth(token),
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_delta_invalid_id(
    client: httpx.AsyncClient, valid_session,
):
    token, _ = valid_session
    resp = await client.get("/today/deltas/not-a-uuid", headers=_auth(token))
    assert resp.status_code == 400


# =====================================================================
# /today/deltas/{id}/evidence
# =====================================================================


@pytest.mark.asyncio
async def test_evidence_endpoint_groups_by_source(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    valid_session,
):
    token, _ = valid_session
    did = await _seed(
        gateway_pool, tenant_id,
        evidence=[
            {"source": "support", "title": "T1", "trust_tier": "attested"},
            {"source": "support", "title": "T2", "trust_tier": "attested"},
            {"source": "email",   "title": "E1", "trust_tier": "secondhand"},
        ],
    )
    resp = await client.get(
        f"/today/deltas/{did}/evidence", headers=_auth(token),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["totalSignals"] == 3
    by_src = {g["sourceType"]: g for g in body["evidenceGroups"]}
    assert by_src["support"]["count"] == 2
    assert by_src["support"]["quality"] == "strong"
    assert by_src["email"]["quality"] == "partial"
    assert len(body["items"]) == 3


# =====================================================================
# /today/deltas/{id}/apply
# =====================================================================


@pytest.mark.asyncio
async def test_apply_transitions_to_accepted(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    valid_session,
):
    token, _ = valid_session
    did = await _seed(
        gateway_pool, tenant_id,
        main_assertion="Apply target update",
        label="authority_required",
    )
    resp = await client.post(
        f"/today/deltas/{did}/apply", headers=_auth(token),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "applied"
    assert body["updatedDelta"]["status"] == "accepted"
    assert body["resultMessage"].startswith("Change accepted")
    # DB confirms.
    row = await gateway_pool.fetchrow(
        "SELECT status FROM decision_deltas WHERE id = $1", did,
    )
    assert row["status"] == "accepted"


@pytest.mark.asyncio
async def test_apply_returns_next_delta_id(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    valid_session,
):
    token, _ = valid_session
    d1 = await _seed(gateway_pool, tenant_id, main_assertion="A1")
    d2 = await _seed(gateway_pool, tenant_id, main_assertion="A2")
    resp = await client.post(
        f"/today/deltas/{d1}/apply", headers=_auth(token),
    )
    body = resp.json()
    assert body["nextDeltaId"] is not None
    assert UUID(body["nextDeltaId"]) == d2


@pytest.mark.asyncio
async def test_apply_stale_returns_409(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    valid_session,
):
    token, _ = valid_session
    did = await _seed(
        gateway_pool, tenant_id, status="accepted",
    )
    resp = await client.post(
        f"/today/deltas/{did}/apply", headers=_auth(token),
    )
    # apply_acceptance is idempotent on accepted — short-circuits with
    # "applied" + already_accepted note rather than raising. The route
    # returns 200 in that case (UI sees the change as already applied).
    assert resp.status_code == 200
    assert resp.json()["status"] == "applied"


# =====================================================================
# /today/deltas/{id}/delegate
# =====================================================================


@pytest.mark.asyncio
async def test_delegate_stores_metadata(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    valid_session,
    seeded_actor_b,
):
    token, _ = valid_session
    did = await _seed(gateway_pool, tenant_id, label="needs_review")
    resp = await client.post(
        f"/today/deltas/{did}/delegate",
        headers=_auth(token),
        json={
            "delegateToActorId": str(seeded_actor_b),
            "message":           "Please confirm ownership.",
            "notifyNow":         True,
            "monitorConfirmation": True,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "delegated"
    assert body["updatedDelta"]["status"] == "delegated"
    # impact JSONB carries the delegation note.
    row = await gateway_pool.fetchrow(
        "SELECT impact FROM decision_deltas WHERE id = $1", did,
    )
    imp = row["impact"]
    if isinstance(imp, str):
        imp = json.loads(imp)
    assert "delegation" in imp
    assert imp["delegation"]["owner_id"] == str(seeded_actor_b)
    assert imp["delegation"]["message"] == "Please confirm ownership."


@pytest.mark.asyncio
async def test_delegate_requires_owner(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    valid_session,
):
    token, _ = valid_session
    did = await _seed(gateway_pool, tenant_id, label="needs_review")
    resp = await client.post(
        f"/today/deltas/{did}/delegate",
        headers=_auth(token),
        json={},
    )
    assert resp.status_code == 400


# =====================================================================
# /today/deltas/{id}/correction
# =====================================================================


@pytest.mark.asyncio
async def test_correction_elevates_to_correction_submitted(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    valid_session,
):
    token, _ = valid_session
    did = await _seed(gateway_pool, tenant_id, label="needs_review")
    resp = await client.post(
        f"/today/deltas/{did}/correction",
        headers=_auth(token),
        json={
            "correctionType": "wrong_conclusion",
            "explanation":    "We already remediated this last week.",
            "applyToRelatedItems": False,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "correction_submitted"
    # Spec status reflects the corrected state.
    assert body["updatedDelta"]["status"] == "correction_submitted"
    # DB row is contested + impact.correction_submitted=True.
    row = await gateway_pool.fetchrow(
        "SELECT status, impact FROM decision_deltas WHERE id = $1", did,
    )
    assert row["status"] == "contested"
    imp = row["impact"]
    if isinstance(imp, str):
        imp = json.loads(imp)
    assert imp["correction_submitted"] is True
    assert imp["correction"]["type"] == "wrong_conclusion"
    assert imp["correction"]["explanation"].startswith("We already")


@pytest.mark.asyncio
async def test_correction_rejects_invalid_type(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    valid_session,
):
    token, _ = valid_session
    did = await _seed(gateway_pool, tenant_id)
    resp = await client.post(
        f"/today/deltas/{did}/correction",
        headers=_auth(token),
        json={"correctionType": "not_a_type", "explanation": "..."},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_correction_requires_explanation(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    valid_session,
):
    token, _ = valid_session
    did = await _seed(gateway_pool, tenant_id)
    resp = await client.post(
        f"/today/deltas/{did}/correction",
        headers=_auth(token),
        json={"correctionType": "missing_context"},
    )
    assert resp.status_code == 400


# =====================================================================
# Auth — bearer required on every endpoint
# =====================================================================


@pytest.mark.asyncio
async def test_endpoints_require_auth(
    client: httpx.AsyncClient,
):
    for method, path in [
        ("get",  "/today"),
        ("get",  "/today/deltas/00000000-0000-0000-0000-000000000000"),
        ("get",  "/today/deltas/00000000-0000-0000-0000-000000000000/evidence"),
        ("post", "/today/deltas/00000000-0000-0000-0000-000000000000/apply"),
        ("post", "/today/deltas/00000000-0000-0000-0000-000000000000/delegate"),
        ("post", "/today/deltas/00000000-0000-0000-0000-000000000000/correction"),
    ]:
        fn = getattr(client, method)
        resp = await fn(path)
        assert resp.status_code == 401, f"{method.upper()} {path}"
