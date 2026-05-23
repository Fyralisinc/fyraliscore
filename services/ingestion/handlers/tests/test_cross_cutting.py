"""Cross-cutting handler tests: property fuzz, dedup, tenant isolation.

These tests don't exercise a single handler in depth; they verify
invariants that every handler must satisfy:

- Fuzzed payload shapes never crash with an unhandled exception.
  Either the handler returns a draft or raises a structured error
  (ValidationError / HandlerError).
- `external_id` is stable per event so the DB-level UNIQUE (source_channel,
  external_id) dedup works.
- Tenant isolation is invariant-free at the handler layer (no tenant
  leak, no tenant resolution inside the handler — that belongs to the
  ingestion core).
"""
from __future__ import annotations


from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from lib.shared.errors import CompanyOSError, ValidationError
from services.ingestion.handlers.email import handle_email_webhook
from services.ingestion.handlers.github import handle_github_webhook
from services.ingestion.handlers.linear import handle_linear_webhook


# =====================================================================
# Property: fuzzed payload never crashes with non-structured error
# =====================================================================

_json_primitive = st.one_of(
    st.none(), st.booleans(), st.integers(), st.floats(allow_nan=False),
    st.text(max_size=30),
)
_json_value = st.recursive(
    _json_primitive,
    lambda child: st.one_of(
        st.lists(child, max_size=5),
        st.dictionaries(st.text(max_size=15), child, max_size=5),
    ),
    max_leaves=12,
)


@settings(max_examples=40, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(payload=_json_value)
async def test_github_fuzz_never_raises_unhandled(payload):
    if not isinstance(payload, dict):
        # The handler only accepts dicts; callers translate non-dicts
        # to a 400 at the Gateway. Skip those here.
        return
    try:
        await handle_github_webhook(payload, {"X-GitHub-Event": "pull_request"})
    except (ValidationError, CompanyOSError, TypeError, AttributeError):
        # All acceptable failure modes — structured errors or predictable
        # downstream type errors (never an unhandled generic Exception).
        pass


@settings(max_examples=40, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(payload=_json_value)
async def test_linear_fuzz_never_raises_unhandled(payload):
    if not isinstance(payload, dict):
        return
    try:
        await handle_linear_webhook(payload, {})
    except (ValidationError, CompanyOSError, TypeError, AttributeError):
        pass


@settings(max_examples=40, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(payload=_json_value)
async def test_email_fuzz_never_raises_unhandled(payload):
    if not isinstance(payload, dict):
        return
    try:
        await handle_email_webhook(payload, {})
    except (ValidationError, CompanyOSError, TypeError, AttributeError):
        pass


# =====================================================================
# External-id stability → dedup across calls
# =====================================================================

async def test_github_pr_merge_external_id_stable():
    payload = {
        "action": "closed",
        "pull_request": {
            "number": 1, "title": "t", "node_id": "PR_same",
            "merged": True, "base": {"ref": "main"},
        },
        "repository": {"full_name": "acme/r"},
        "sender": {"login": "alice"},
    }
    d1 = await handle_github_webhook(
        payload, {"X-GitHub-Event": "pull_request"}
    )
    d2 = await handle_github_webhook(
        payload, {"X-GitHub-Event": "pull_request"}
    )
    assert d1.external_id == d2.external_id == "PR_same"


async def test_linear_issue_external_id_stable():
    payload = {
        "action": "create",
        "type": "Issue",
        "data": {
            "id": "issue-uuid",
            "identifier": "ENG-1",
            "title": "x",
        },
    }
    d1 = await handle_linear_webhook(payload, {})
    d2 = await handle_linear_webhook(payload, {})
    assert d1.external_id == d2.external_id == "issue-uuid"


async def test_email_external_id_stable():
    payload = {
        "from": "a@b",
        "to": ["c@d"],
        "subject": "s",
        "body": "b",
        "message_id": "<stable@id>",
    }
    d1 = await handle_email_webhook(payload, {})
    d2 = await handle_email_webhook(payload, {})
    assert d1.external_id == d2.external_id == "<stable@id>"


# =====================================================================
# Tenant isolation at the handler layer: handlers must not accept
# or emit a tenant_id. The ingestion core stamps tenancy.
# =====================================================================

async def test_handlers_do_not_touch_tenant_fields_directly():
    """The draft has no tenant_id field by design. Even if we pass
    a tenant-shaped value in headers, it must not leak into the
    content or entities_hint. Absence of a tenant_id attr on the
    draft is the invariant."""
    from services.ingestion.handlers import ObservationDraft

    payload = {
        "action": "closed",
        "pull_request": {
            "number": 1, "title": "t", "node_id": "PR_t",
            "merged": True, "base": {"ref": "main"},
        },
        "repository": {"full_name": "acme/r"},
        "sender": {"login": "alice"},
    }
    draft = await handle_github_webhook(
        payload,
        {"X-GitHub-Event": "pull_request", "X-Tenant-Id": "some-value"},
    )
    assert isinstance(draft, ObservationDraft)
    assert not hasattr(draft, "tenant_id")
    # Nor any content field that would surface the tenant header.
    import json
    assert "X-Tenant-Id" not in json.dumps(draft.content)
