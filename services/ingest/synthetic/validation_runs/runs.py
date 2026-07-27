"""Deterministic validation-run scenario definitions.

``certification_history_scenarios`` covers every canonical source whose source
contract declares history. Its source membership comes from
``SOURCE_DEFINITIONS`` and fixture ownership comes from the certification
runtime; it intentionally owns neither a source registry nor source-specific
fixture builders.

Each history source owns both its deterministic fixture factory and its exact
Observation-count oracle. This module only composes those source-owned
callables; it never guesses a count or maintains a second source registry.
"""

from __future__ import annotations

from services.ingest.source_certification.runtime import (
    resolve_fixture_count_oracle,
    resolve_fixture_factory,
)
from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.synthetic.backfill_harness.scenarios import BackfillScenario


_EXPLICIT_HISTORY_EXCLUSION = "whatsapp"
_HISTORY_UNSUPPORTED_SOURCE_IDS = tuple(
    source.source_id for source in SOURCE_DEFINITIONS if source.history is None
)
if _HISTORY_UNSUPPORTED_SOURCE_IDS != (_EXPLICIT_HISTORY_EXCLUSION,):
    raise RuntimeError(
        "validation run history exclusions drifted from the source contract: "
        f"expected {(_EXPLICIT_HISTORY_EXCLUSION,)!r}, "
        f"got {_HISTORY_UNSUPPORTED_SOURCE_IDS!r}",
    )

_HISTORY_SOURCE_IDS = tuple(
    source.source_id for source in SOURCE_DEFINITIONS if source.history is not None
)

def _validate_tenant_count(tenants_per_source: int) -> None:
    if not isinstance(tenants_per_source, int) or isinstance(
        tenants_per_source,
        bool,
    ):
        raise TypeError("tenants_per_source must be an int")
    if tenants_per_source < 0:
        raise ValueError("tenants_per_source must be non-negative")


def _validate_installation_count(installations_per_tenant: int) -> None:
    if not isinstance(installations_per_tenant, int) or isinstance(
        installations_per_tenant,
        bool,
    ):
        raise TypeError("installations_per_tenant must be an int")
    if installations_per_tenant < 1:
        raise ValueError("installations_per_tenant must be positive")


def _certification_scenario(
    source_id: str,
    tenant_index: int,
    installation_index: int,
    *,
    installations_per_tenant: int,
) -> BackfillScenario:
    tenant_slug = f"val-{source_id}-{tenant_index}"
    installation_key = (
        None
        if installations_per_tenant == 1
        else f"{tenant_slug}-installation-{installation_index}"
    )
    installation_id = (
        f"x3-{installation_key or tenant_slug}-{source_id}"
    )
    factory = resolve_fixture_factory(source_id)
    count_observations = resolve_fixture_count_oracle(source_id)
    fixture_params: dict[str, object] = {}
    fixture = factory(
        fixture_params=fixture_params,
        installation_id=installation_id,
    )
    expected_observation_count = count_observations(fixture)
    if (
        isinstance(expected_observation_count, bool)
        or not isinstance(expected_observation_count, int)
        or expected_observation_count <= 0
    ):
        raise ValueError(
            f"{source_id} certification expected_observation_count must be "
            f"a positive exact integer, got {expected_observation_count!r}"
        )
    return BackfillScenario(
        tenant_slug=tenant_slug,
        source=source_id,
        installation_key=installation_key,
        # The source-owned certification factory supplies its deterministic
        # defaults and receives the harness's slug-derived installation_id.
        fixture_params=fixture_params,
        expected_observation_count=expected_observation_count,
    )


def certification_history_scenarios(
    tenants_per_source: int = 4,
    *,
    installations_per_tenant: int = 1,
) -> list[BackfillScenario]:
    """Build historical certification scenarios in canonical source order.

    The default produces 104 scenarios: four deterministic tenants with one
    installation each for every one of the 26 history-capable canonical
    sources. ``installations_per_tenant`` expands each tenant into exact
    sibling-installation scenarios without creating another source registry.
    WhatsApp is excluded explicitly because its source contract declares
    ``history=None``.
    """
    _validate_tenant_count(tenants_per_source)
    _validate_installation_count(installations_per_tenant)

    scenarios: list[BackfillScenario] = []
    for source_id in _HISTORY_SOURCE_IDS:
        # Resolve both bindings before adding any scenarios for this source.
        # Missing, mis-owned, malformed, zero, or non-exact count declarations
        # therefore fail before the validation run begins.
        resolve_fixture_factory(source_id)
        resolve_fixture_count_oracle(source_id)
        scenarios.extend(
            _certification_scenario(
                source_id,
                tenant_index,
                installation_index,
                installations_per_tenant=installations_per_tenant,
            )
            for tenant_index in range(tenants_per_source)
            for installation_index in range(installations_per_tenant)
        )
    return scenarios


__all__ = ["certification_history_scenarios"]
