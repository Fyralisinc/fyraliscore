from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.ingest.connector_runtime.authority import (
    InstallationAuthority,
    scope_authority,
)
from services.ingest.connector_runtime.tests.helpers import make_manifest
from services.ingest.source_contract.connector import GrantedAuthority
from services.ingest.source_contract.errors import BindingError
from services.ingest.source_contract.models import InstallationRef


def _installation() -> InstallationRef:
    return InstallationRef(
        id=uuid4(),
        tenant_id=uuid4(),
        connector_id="fyralis/slack",
        generation=3,
    )


def test_durable_authority_validates_identity_generation_and_grants() -> None:
    installation = _installation()
    authority = InstallationAuthority(
        installation_id=installation.id,
        tenant_id=installation.tenant_id,
        connector_id=installation.connector_id,
        generation=installation.generation,
        credential_owner="oauth_callback",
        secret_slots=frozenset({"oauth_access_token"}),
        scopes=frozenset({"channels:read"}),
        outbound_hosts=frozenset({"slack.com"}),
        maximum_trust_tier="attested_agent",
        provenance={"state_nonce": "redacted-digest"},
    )

    grant = authority.validate_for(installation)

    assert grant.secret_slots == frozenset({"oauth_access_token"})
    assert grant.scopes == frozenset({"channels:read"})
    assert grant.outbound_hosts == frozenset({"slack.com"})


@pytest.mark.parametrize("failure", ["revoked", "stale", "wrong_tenant"])
def test_durable_authority_fails_closed(failure: str) -> None:
    installation = _installation()
    authority = InstallationAuthority(
        installation_id=installation.id,
        tenant_id=(uuid4() if failure == "wrong_tenant" else installation.tenant_id),
        connector_id=installation.connector_id,
        generation=(1 if failure == "stale" else installation.generation),
        credential_owner="oauth_callback",
        revoked_at=(datetime.now(timezone.utc) if failure == "revoked" else None),
    )

    with pytest.raises(BindingError):
        authority.validate_for(installation)


def test_authority_is_reduced_to_manifest_permissions_and_trust_ceiling() -> None:
    manifest = make_manifest()
    scoped = scope_authority(
        manifest,
        GrantedAuthority(
            secret_slots=frozenset({"api_token", "unrelated_secret"}),
            outbound_hosts=frozenset({"api.example.com", "unrelated.example.com"}),
            scopes=frozenset({"events:read", "admin:everything"}),
            maximum_trust_tier="authoritative",
        ),
    )

    assert scoped.secret_slots == frozenset({"api_token"})
    assert scoped.outbound_hosts == frozenset({"api.example.com"})
    assert scoped.scopes == frozenset({"events:read"})
    assert scoped.maximum_trust_tier == manifest.spec.trust.maximum_tier


def test_authority_scope_allows_partial_installation_grants() -> None:
    scoped = scope_authority(make_manifest(), GrantedAuthority())
    assert scoped.secret_slots == frozenset()
    assert scoped.outbound_hosts == frozenset()
    assert scoped.scopes == frozenset()
