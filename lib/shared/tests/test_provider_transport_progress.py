from __future__ import annotations

import pytest

from lib.shared.provider_transport import (
    ZeroProgressError,
    validate_fetch_progress,
)


def test_nonterminal_empty_same_cursor_is_rejected() -> None:
    cursor = {"page": 7}

    with pytest.raises(ZeroProgressError, match="raise RetryLater"):
        validate_fetch_progress(
            cursor_before=cursor,
            cursor_after={"page": 7},
            records_emitted=0,
            end_of_data=False,
        )


def test_terminal_empty_page_is_valid_progress() -> None:
    progress = validate_fetch_progress(
        cursor_before={"page": 7},
        cursor_after={"page": 7},
        records_emitted=0,
        end_of_data=True,
    )

    assert progress.end_of_data is True
    assert progress.made_progress is True


def test_empty_page_with_cursor_transition_is_valid_progress() -> None:
    progress = validate_fetch_progress(
        cursor_before={"page": 7},
        cursor_after={"page": 8},
        records_emitted=0,
        end_of_data=False,
    )

    assert progress.cursor_advanced is True
    assert progress.made_progress is True
