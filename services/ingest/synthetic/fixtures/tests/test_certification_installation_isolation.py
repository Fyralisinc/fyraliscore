"""Catalog-wide sibling-installation guarantees for certification fixtures."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from services.ingest.source_certification.runtime import (
    resolve_fixture_count_oracle,
    resolve_fixture_factory,
)
from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS


_HISTORY_SOURCE_IDS = tuple(
    source.source_id for source in SOURCE_DEFINITIONS if source.history is not None
)


def _is_identity_field(field: str) -> bool:
    compact = "".join(character for character in field.lower() if character.isalnum())
    return compact == "id" or compact.endswith(
        ("id", "uuid", "key", "urn", "email", "url"),
    )


def _identity_projection(
    value: Any,
    *,
    path: tuple[str, ...] = (),
) -> tuple[tuple[tuple[str, ...], str], ...]:
    identities: list[tuple[tuple[str, ...], str]] = []
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key)
            child_path = (*path, key)
            if (
                _is_identity_field(key)
                and not isinstance(child, (Mapping, list))
                and child is not None
            ):
                identities.append((child_path, str(child)))
            identities.extend(_identity_projection(child, path=child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            identities.extend(
                _identity_projection(child, path=(*path, str(index))),
            )
    return tuple(identities)


@pytest.mark.parametrize("source_id", _HISTORY_SOURCE_IDS)
def test_certification_fixture_isolates_same_tenant_sibling_installations(
    source_id: str,
) -> None:
    """Installation identity must reach provider scope or qualified record IDs."""

    factory = resolve_fixture_factory(source_id)
    count_observations = resolve_fixture_count_oracle(source_id)
    first_installation_id = f"x3-same-tenant-{source_id}-installation-0"
    second_installation_id = f"x3-same-tenant-{source_id}-installation-1"

    first = factory(
        fixture_params={},
        installation_id=first_installation_id,
    )
    second = factory(
        fixture_params={},
        installation_id=second_installation_id,
    )

    first_identities = _identity_projection(first)
    second_identities = _identity_projection(second)

    assert first_identities, f"{source_id} fixture exposes no provider identity"
    assert second_identities, f"{source_id} fixture exposes no provider identity"
    assert first_identities != second_identities, (
        f"{source_id} emitted the same provider scope/record identity for "
        "two installations in one tenant"
    )
    assert first != second
    assert count_observations(first) == count_observations(second)
    assert (
        factory(
            fixture_params={},
            installation_id=first_installation_id,
        )
        == first
    )


def test_fixture_isolation_covers_contract_history_catalog() -> None:
    assert len(_HISTORY_SOURCE_IDS) == 26
    assert "whatsapp" not in _HISTORY_SOURCE_IDS


def test_fireflies_fixture_forces_exact_installation_workspace() -> None:
    """A caller-supplied fixture default cannot merge sibling installations."""

    factory = resolve_fixture_factory("fireflies")
    first = factory(
        fixture_params={"workspace_id": "legacy-shared-workspace"},
        installation_id="x3-fireflies-installation-0",
    )
    second = factory(
        fixture_params={"workspace_id": "legacy-shared-workspace"},
        installation_id="x3-fireflies-installation-1",
    )

    assert first["workspace_id"] == "x3-fireflies-installation-0"
    assert second["workspace_id"] == "x3-fireflies-installation-1"
    assert {
        transcript["workspaceId"] for transcript in first["transcripts"]
    } == {"x3-fireflies-installation-0"}
    assert {
        transcript["workspaceId"] for transcript in second["transcripts"]
    } == {"x3-fireflies-installation-1"}
    assert {
        transcript["id"] for transcript in first["transcripts"]
    }.isdisjoint(
        transcript["id"] for transcript in second["transcripts"]
    )


@pytest.mark.parametrize(
    ("source_id", "collection", "timestamp_field"),
    (
        ("aws", "events", "eventTime"),
        ("grafana", "annotations", "time"),
    ),
)
def test_time_windowed_certification_fixtures_remain_recent(
    source_id: str,
    collection: str,
    timestamp_field: str,
) -> None:
    fixture = resolve_fixture_factory(source_id)(
        fixture_params={},
        installation_id=f"x3-recent-{source_id}",
    )
    now = datetime.now(timezone.utc)
    floor_ms = int((now - timedelta(days=3)).timestamp() * 1000)
    ceiling_ms = int((now + timedelta(minutes=1)).timestamp() * 1000)

    timestamps = [row[timestamp_field] for row in fixture[collection]]

    assert timestamps
    assert all(floor_ms <= timestamp <= ceiling_ms for timestamp in timestamps)


def test_hibob_timeoff_fixture_remains_inside_six_month_api_window() -> None:
    fixture = resolve_fixture_factory("hibob")(
        fixture_params={},
        installation_id="x3-recent-hibob",
    )
    now = datetime.now(timezone.utc)
    floor = now - timedelta(days=3)
    ceiling = now + timedelta(minutes=1)

    modified = [
        datetime.fromisoformat(row["modified"].replace("Z", "+00:00"))
        for rows in fixture["entities"].values()
        for row in rows
    ]

    assert modified
    assert all(floor <= timestamp <= ceiling for timestamp in modified)
