from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from services.ingest.connector_conformance.fakes import (
    FakeHostEnvironment,
    make_binding_context,
)
from services.ingest.connector_platform.fleet_validation import validate_native_fleet
from services.ingest.connector_platform.pilots import build_fleet_candidates
from services.ingest.connector_runtime.discovery import resolve_connector_factory
from services.ingest.connectors.profiles import FLEET_PROFILES
from services.ingest.source_contract.capabilities import (
    HEALTH_PROBE_V1,
    HISTORICAL_PULL_V1,
    NORMALIZATION_V1,
)
from services.ingest.source_contract.capabilities.lifecycle import HealthProbeRequest
from services.ingest.source_contract.connector import GrantedAuthority, OperationContext
from services.ingest.source_contract.errors import (
    PayloadRejectedError,
    RateLimitedError,
    TransientSourceError,
)
from services.ingest.source_contract.host_services import (
    GovernedHttpResponse,
    SecretValue,
)
from services.ingest.source_contract.models import (
    FetchRequest,
    NormalizationInput,
    ShardPlan,
    SourceRecord,
)
from services.ingest.source_contract.source_catalog import source_ids


def _operation(environment: FakeHostEnvironment) -> OperationContext:
    from uuid import UUID

    return OperationContext(
        invocation_id=UUID("d7dbeecf-95a6-45ba-a390-5e9e5a52a6fb"),
        deadline=datetime(2030, 1, 1, tzinfo=UTC),
        services=environment.services,
    )


def test_complete_fleet_is_native_stable_v1_and_wiring_is_generated() -> None:
    candidates = build_fleet_candidates()
    validate_native_fleet(candidates)
    assert len(candidates) == 26
    assert {item.manifest.source for item in candidates} == set(source_ids())
    assert all(item.origin.startswith("first-party-native:") for item in candidates)
    assert all(
        item.manifest.api_version == "sources.fyralis.io/v1" for item in candidates
    )


def test_native_implementation_modules_do_not_import_legacy_layers() -> None:
    forbidden = (
        "services.ingest.ingestion",
        "services.ingest.integrations",
        "services.ingest.connector_platform",
        "services.app",
    )
    roots = (
        Path("services/ingest/connectors/fleet.py"),
        Path("services/ingest/connectors/profiles.py"),
        Path("services/ingest/connectors/native.py"),
        Path("services/ingest/connectors/slack.py"),
        Path("services/ingest/connectors/notion.py"),
        Path("services/ingest/connectors/whatsapp.py"),
    )
    for path in roots:
        source = path.read_text(encoding="utf-8")
        assert not any(value in source for value in forbidden), path


@pytest.mark.parametrize("source", sorted(FLEET_PROFILES))
def test_configured_capabilities_are_withheld_without_credential_grants(
    source: str,
) -> None:
    candidate = next(
        item for item in build_fleet_candidates() if item.manifest.source == source
    )
    connector = resolve_connector_factory(candidate.manifest)()
    environment = FakeHostEnvironment()
    context = make_binding_context(
        candidate.manifest,
        environment=environment,
        authority=GrantedAuthority(
            outbound_hosts=frozenset(
                candidate.manifest.spec.permissions.outbound_hosts
            ),
            maximum_trust_tier=candidate.manifest.spec.trust.maximum_tier,
        ),
    )
    binding = connector.bind(context)
    if "backfill" in candidate.manifest.spec.ingress_kinds:
        assert binding.capability(HISTORICAL_PULL_V1) is None
    assert binding.capability(NORMALIZATION_V1) is not None


@pytest.mark.parametrize("source", sorted(FLEET_PROFILES))
@pytest.mark.asyncio
async def test_provider_throttling_and_outage_are_retryable(source: str) -> None:
    profile = FLEET_PROFILES[source]
    candidate = next(
        item for item in build_fleet_candidates() if item.manifest.source == source
    )
    connector = resolve_connector_factory(candidate.manifest)()
    environment = FakeHostEnvironment()
    environment.secrets.values[profile.auth_slot] = SecretValue.from_text("token")
    binding = connector.bind(
        make_binding_context(candidate.manifest, environment=environment)
    )
    capability = binding.require(HISTORICAL_PULL_V1)
    request = FetchRequest(
        shard=ShardPlan(kind=f"{source}_collection", identifier={"resource_id": "all"})
    )
    environment.http.responses.append(GovernedHttpResponse(429, (), b"{}"))
    with pytest.raises(RateLimitedError):
        await capability.fetch(request, _operation(environment))
    environment.http.responses.append(GovernedHttpResponse(503, (), b"{}"))
    with pytest.raises(TransientSourceError):
        await capability.fetch(request, _operation(environment))


@pytest.mark.parametrize("source", sorted(FLEET_PROFILES))
@pytest.mark.asyncio
async def test_revoked_credentials_and_poison_payload_fail_closed(source: str) -> None:
    profile = FLEET_PROFILES[source]
    candidate = next(
        item for item in build_fleet_candidates() if item.manifest.source == source
    )
    connector = resolve_connector_factory(candidate.manifest)()
    environment = FakeHostEnvironment()
    binding = connector.bind(
        make_binding_context(candidate.manifest, environment=environment)
    )
    health = await binding.require(HEALTH_PROBE_V1).probe(
        HealthProbeRequest(), _operation(environment)
    )
    assert not health.healthy
    with pytest.raises(PayloadRejectedError):
        await binding.require(NORMALIZATION_V1).normalize(
            NormalizationInput(
                record=SourceRecord(native_type=profile.native_type, payload=b"poison"),
                ingress_kind=profile.ingress_kinds[0],
            ),
            _operation(environment),
        )
