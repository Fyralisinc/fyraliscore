"""Small, dependency-free Semantic Versioning and compatibility ranges."""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import total_ordering
from typing import Iterable, Literal


_SEMVER_RE = re.compile(
    r"^(?P<major>0|[1-9]\d*)\."
    r"(?P<minor>0|[1-9]\d*)\."
    r"(?P<patch>0|[1-9]\d*)"
    r"(?:-(?P<prerelease>[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+(?P<build>[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)
_PARTIAL_VERSION_RE = re.compile(
    r"^(?P<major>0|[1-9]\d*)"
    r"(?:\.(?P<minor>0|[1-9]\d*))?"
    r"(?:\.(?P<patch>0|[1-9]\d*))?$"
)
_COMPARATOR_RE = re.compile(r"^(?P<operator>>=|<=|==|>|<)\s*(?P<version>.+)$")


@total_ordering
@dataclass(frozen=True)
class SemanticVersion:
    """A SemVer 2.0 value.

    Build metadata is retained for diagnostics but intentionally ignored for
    precedence, as required by Semantic Versioning.
    """

    major: int
    minor: int
    patch: int
    prerelease: tuple[str, ...] = ()
    build: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if min(self.major, self.minor, self.patch) < 0:
            raise ValueError("semantic version components cannot be negative")
        for identifier in self.prerelease:
            if identifier.isdigit() and len(identifier) > 1 and identifier[0] == "0":
                raise ValueError(
                    "numeric prerelease identifiers cannot contain leading zeroes"
                )

    @classmethod
    def parse(cls, value: str, *, allow_partial: bool = False) -> "SemanticVersion":
        match = _SEMVER_RE.fullmatch(value.strip())
        if match is not None:
            prerelease = tuple((match.group("prerelease") or "").split("."))
            build = tuple((match.group("build") or "").split("."))
            return cls(
                major=int(match.group("major")),
                minor=int(match.group("minor")),
                patch=int(match.group("patch")),
                prerelease=tuple(part for part in prerelease if part),
                build=tuple(part for part in build if part),
            )
        if allow_partial:
            partial = _PARTIAL_VERSION_RE.fullmatch(value.strip())
            if partial is not None:
                return cls(
                    major=int(partial.group("major")),
                    minor=int(partial.group("minor") or 0),
                    patch=int(partial.group("patch") or 0),
                )
        raise ValueError(f"invalid semantic version: {value!r}")

    def __str__(self) -> str:
        value = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            value += "-" + ".".join(self.prerelease)
        if self.build:
            value += "+" + ".".join(self.build)
        return value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SemanticVersion):
            return NotImplemented
        return self._precedence_key() == other._precedence_key()

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, SemanticVersion):
            return NotImplemented
        stable = (self.major, self.minor, self.patch)
        other_stable = (other.major, other.minor, other.patch)
        if stable != other_stable:
            return stable < other_stable
        return _compare_prerelease(self.prerelease, other.prerelease) < 0

    def __hash__(self) -> int:
        return hash(self._precedence_key())

    def _precedence_key(self) -> tuple[int, int, int, tuple[str, ...]]:
        return (self.major, self.minor, self.patch, self.prerelease)


ComparatorOperator = Literal[">=", "<=", "==", ">", "<"]


@dataclass(frozen=True)
class VersionComparator:
    operator: ComparatorOperator
    version: SemanticVersion

    def matches(self, candidate: SemanticVersion) -> bool:
        if self.operator == ">=":
            return candidate >= self.version
        if self.operator == "<=":
            return candidate <= self.version
        if self.operator == ">":
            return candidate > self.version
        if self.operator == "<":
            return candidate < self.version
        return candidate == self.version


@dataclass(frozen=True)
class VersionRange:
    """A conjunction of comma-separated SemVer comparators."""

    comparators: tuple[VersionComparator, ...]

    @classmethod
    def parse(cls, value: str) -> "VersionRange":
        raw_parts = tuple(part.strip() for part in value.split(",") if part.strip())
        if not raw_parts:
            raise ValueError("version range must contain at least one comparator")
        comparators: list[VersionComparator] = []
        for part in raw_parts:
            match = _COMPARATOR_RE.fullmatch(part)
            if match is None:
                raise ValueError(
                    f"invalid version comparator {part!r}; expected >=1.0,<2.0"
                )
            comparators.append(
                VersionComparator(
                    operator=match.group("operator"),  # type: ignore[arg-type]
                    version=SemanticVersion.parse(
                        match.group("version"), allow_partial=True
                    ),
                )
            )
        return cls(tuple(comparators))

    def contains(self, version: SemanticVersion) -> bool:
        return all(comparator.matches(version) for comparator in self.comparators)

    def select_highest(
        self, versions: Iterable[SemanticVersion]
    ) -> SemanticVersion | None:
        matches = [version for version in versions if self.contains(version)]
        return max(matches, default=None)

    def __contains__(self, version: object) -> bool:
        return isinstance(version, SemanticVersion) and self.contains(version)

    def __str__(self) -> str:
        return ",".join(
            f"{comparator.operator}{comparator.version}"
            for comparator in self.comparators
        )


def _compare_prerelease(left: tuple[str, ...], right: tuple[str, ...]) -> int:
    if not left and not right:
        return 0
    if not left:
        return 1
    if not right:
        return -1
    for left_part, right_part in zip(left, right, strict=False):
        if left_part == right_part:
            continue
        left_numeric = left_part.isdigit()
        right_numeric = right_part.isdigit()
        if left_numeric and right_numeric:
            return -1 if int(left_part) < int(right_part) else 1
        if left_numeric != right_numeric:
            return -1 if left_numeric else 1
        return -1 if left_part < right_part else 1
    if len(left) == len(right):
        return 0
    return -1 if len(left) < len(right) else 1


__all__ = [
    "SemanticVersion",
    "VersionComparator",
    "VersionRange",
]
