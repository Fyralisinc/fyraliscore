"""Contract test: the Gmail watch-renewal path keeps the history cursor
monotonic against a REAL users.watch() response.

Guards the Phase-3 drift fix (finding #9): the real Gmail users.watch()
response is exactly ``{"historyId": <str>, "expiration": <str>}`` where both
values are numbers serialized as strings. The watch_scheduler USED to overwrite
the stored ``history_id`` with ``watch().historyId`` on every renewal — but a
renewal can return a LOWER historyId than the push/poll fetchers have already
advanced the stored cursor to. Overwriting rewinds the bookmark, which makes
users.history.list re-fetch or skip. The fix keeps ``GREATEST(stored, returned)``
compared NUMERICALLY (lexical compare is wrong: "9" > "10").

Verified against developers.google.com/gmail/api/reference/rest/v1/users/watch.
"""
from __future__ import annotations

import pytest

from services.ingest.integrations.gmail.watch import _expiration_to_dt
from services.ingest.integrations.gmail.watch_scheduler import (
    _as_history_int,
    _monotonic_history_id,
)
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract


def _fixture():
    return load_fixture("gmail", "api_response", "watch_and_history")


def test_watch_response_is_doc_shaped():
    """Real users.watch() returns historyId + expiration, both numeric strings."""
    body = _fixture().response_body
    assert set(body) == {"historyId", "expiration"}
    # Numbers serialized AS STRINGS — the production code stringifies/parses them.
    assert isinstance(body["historyId"], str) and body["historyId"].isdigit()
    assert isinstance(body["expiration"], str) and body["expiration"].isdigit()


def test_expiration_string_parses_to_aware_datetime():
    """_expiration_to_dt accepts the doc-shaped ms-epoch STRING."""
    body = _fixture().response_body
    dt = _expiration_to_dt(body["expiration"])
    assert dt is not None
    assert dt.tzinfo is not None
    # ms-epoch / 1000 == the POSIX seconds the watch expires at.
    assert dt.timestamp() == int(body["expiration"]) / 1000.0


def test_renewal_does_not_rewind_when_stored_is_higher():
    """The drift guard: a renewal returning a LOWER historyId than already
    stored must PRESERVE the stored cursor (never go backwards)."""
    returned = _fixture().response_body["historyId"]  # "987650"
    stored = "990000"  # fetcher already advanced past the watch's id
    assert int(stored) > int(returned)
    assert _monotonic_history_id(stored, returned) == stored


def test_renewal_advances_when_returned_is_higher():
    """When the watch returns a HIGHER historyId (quiet mailbox the fetcher
    hasn't caught up to), the cursor advances to the returned id."""
    returned = _fixture().response_body["historyId"]  # "987650"
    stored = "12345"  # well below the returned id
    assert int(returned) > int(stored)
    assert _monotonic_history_id(stored, returned) == returned


def test_monotonic_uses_numeric_not_lexical_compare():
    """Lexical compare would (wrongly) treat "9" > "10"; the guard must not."""
    # stored "9" must NOT win over returned "10".
    assert _monotonic_history_id("9", "10") == "10"
    # returned "9" must NOT clobber stored "10".
    assert _monotonic_history_id("10", "9") == "10"


def test_monotonic_fallbacks_for_missing_or_nonnumeric():
    """Missing / non-numeric ids fall back without crashing, preferring not to
    lose ground (keep the usable side)."""
    returned = _fixture().response_body["historyId"]
    assert _monotonic_history_id(None, returned) == returned       # no prior cursor
    assert _monotonic_history_id("", returned) == returned         # empty prior
    assert _monotonic_history_id(returned, None) == returned       # watch gave none
    assert _monotonic_history_id(returned, "") == returned         # watch gave empty
    # _as_history_int is the parse primitive the guard + SQL rely on.
    assert _as_history_int(returned) == int(returned)
    assert _as_history_int("not-a-number") is None
    assert _as_history_int(None) is None
