"""Progress invariant shared by cursor-driven provider fetchers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lib.shared.provider_transport.contracts import ProviderPermanentError


class ZeroProgressError(ProviderPermanentError):
    """A nonterminal fetch returned neither data nor a cursor transition."""

    default_code = "provider_zero_progress"


@dataclass(frozen=True, slots=True)
class FetchProgress:
    records_emitted: int
    cursor_advanced: bool
    end_of_data: bool

    @property
    def made_progress(self) -> bool:
        return self.records_emitted > 0 or self.cursor_advanced or self.end_of_data


def validate_fetch_progress(
    *,
    cursor_before: Any,
    cursor_after: Any,
    records_emitted: int,
    end_of_data: bool,
) -> FetchProgress:
    """Reject the empty/same-cursor/nonterminal shape that causes hot loops.

    A provider cooldown is represented by raising ``RetryLater`` before a
    ``FetchResult`` is built, never by weakening this invariant.
    """
    if records_emitted < 0:
        raise ValueError("records_emitted must be >= 0")
    progress = FetchProgress(
        records_emitted=records_emitted,
        cursor_advanced=cursor_before != cursor_after,
        end_of_data=end_of_data,
    )
    if not progress.made_progress:
        raise ZeroProgressError(
            "nonterminal fetch made zero progress; raise RetryLater instead",
            records_emitted=records_emitted,
            cursor_advanced=False,
            end_of_data=False,
        )
    return progress


__all__ = ["FetchProgress", "ZeroProgressError", "validate_fetch_progress"]
