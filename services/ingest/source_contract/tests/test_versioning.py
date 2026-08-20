from __future__ import annotations

import pytest

from services.ingest.source_contract.versioning import SemanticVersion, VersionRange


def test_semantic_version_round_trips() -> None:
    version = SemanticVersion.parse("1.4.2-preview.1+build.9")
    assert str(version) == "1.4.2-preview.1+build.9"


def test_semver_prerelease_precedence() -> None:
    ordered = [
        "1.0.0-alpha",
        "1.0.0-alpha.1",
        "1.0.0-alpha.beta",
        "1.0.0-beta",
        "1.0.0-beta.2",
        "1.0.0-beta.11",
        "1.0.0-rc.1",
        "1.0.0",
    ]
    versions = [SemanticVersion.parse(value) for value in ordered]
    assert sorted(reversed(versions)) == versions


def test_build_metadata_does_not_affect_precedence_or_hash() -> None:
    left = SemanticVersion.parse("1.0.0+one")
    right = SemanticVersion.parse("1.0.0+two")
    assert left == right
    assert hash(left) == hash(right)


def test_version_range_selects_highest_compatible_host() -> None:
    version_range = VersionRange.parse(">=1.0,<2.0")
    selected = version_range.select_highest(
        SemanticVersion.parse(value)
        for value in ("0.9.9", "1.0.0", "1.7.2", "2.0.0")
    )
    assert selected == SemanticVersion.parse("1.7.2")


@pytest.mark.parametrize("value", ("", "1.0", "^1.0", ">=one", ">=1.x"))
def test_invalid_version_ranges_are_rejected(value: str) -> None:
    with pytest.raises(ValueError):
        VersionRange.parse(value)


def test_numeric_prerelease_leading_zero_is_rejected() -> None:
    with pytest.raises(ValueError, match="leading zeroes"):
        SemanticVersion.parse("1.0.0-alpha.01")
