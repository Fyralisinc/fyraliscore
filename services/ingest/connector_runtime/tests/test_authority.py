from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.ingest.connector_runtime.authority import InstallationAuthority
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
