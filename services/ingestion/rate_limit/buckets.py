"""Per-(source, method) default bucket specs.

Per ingestion LLD §13. Capacity/refill values track Slack's published
Web API tiers (https://docs.slack.dev/apis/web-api/rate-limits/).

Each call site (FetchPage activity in M3) picks the bucket spec via
`BUCKET_DEFAULTS[(source, method)]`. The composite key keeps the
table close to grep-readable for ops.

SLACK TIER CONFIG (SLACK_API_TIER)
==================================
On 2025-05-29 Slack moved `conversations.history` and
`conversations.replies` from Tier 3 → Tier 1 (1 req/min, ≤15 objects/req)
for NEW commercially-distributed apps that are NOT Marketplace-approved
("unlisted"), and for new installs of such apps. Marketplace apps and
internal customer-built apps stay on Tier 3. An app cannot introspect its
own listing status at runtime, so the tier for those two methods is
operator config via the `SLACK_API_TIER` env var (default 3). The other
Slack methods are unaffected by that change and carry their canonical
tiers (conversations.list = Tier 2, users.info = Tier 4).
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BucketSpec:
    """Token-bucket parameters for one (source, method).

    `capacity`         — burst size in tokens.
    `refill_per_sec`   — steady-state refill rate.
    """

    capacity: int
    refill_per_sec: float


# Slack Web API tiers. Each tier advertises a per-minute floor ("N+ per
# minute"); we bucket at a conservative steady-state rate (≤ the floor) with a
# modest burst and lean on the client's 429/Retry-After handler for the rest.
_SLACK_TIER_SPECS: dict[int, BucketSpec] = {
    1: BucketSpec(capacity=1,  refill_per_sec=1.0 / 60.0),   # Tier 1: 1+/min
    2: BucketSpec(capacity=20, refill_per_sec=20.0 / 60.0),  # Tier 2: 20+/min
    3: BucketSpec(capacity=40, refill_per_sec=0.67),         # Tier 3: 50+/min (conservative)
    4: BucketSpec(capacity=80, refill_per_sec=100.0 / 60.0), # Tier 4: 100+/min
}

_DEFAULT_SLACK_HISTORY_TIER = 3


def slack_history_tier() -> int:
    """Tier for `conversations.history` / `conversations.replies`, from env.

    `SLACK_API_TIER` accepts 1..4 (default 3). Set it to 1 if the app is
    commercially distributed and NOT Marketplace-approved (post-2025-05-29
    rate-limit change); leave it at 3 for Marketplace or internal apps.
    Unparseable / out-of-range values fall back to the default.
    """
    raw = os.environ.get("SLACK_API_TIER", str(_DEFAULT_SLACK_HISTORY_TIER)).strip()
    try:
        tier = int(raw)
    except ValueError:
        return _DEFAULT_SLACK_HISTORY_TIER
    return tier if tier in _SLACK_TIER_SPECS else _DEFAULT_SLACK_HISTORY_TIER


def _slack_history_spec() -> BucketSpec:
    return _SLACK_TIER_SPECS[slack_history_tier()]


# Keys: (source, method). Method strings match the per-source FetchPage
# call sites in M3 — names follow each source's API conventions:
#   slack:    Web API method strings, e.g. "conversations.history"
#   github:   logical group, e.g. "rest_authenticated" (one bucket per app)
#   gmail:    "per-user" — Gmail's per-user quota
#   discord:  logical group, e.g. "channels_messages"
BUCKET_DEFAULTS: dict[tuple[str, str], BucketSpec] = {
    # conversations.history (+ replies) tier is operator-configurable via
    # SLACK_API_TIER — see slack_history_tier() above. Resolved at import.
    ("slack",   "conversations.history"): _slack_history_spec(),
    ("slack",   "conversations.replies"): _slack_history_spec(),
    # conversations.list is Tier 2 and was NOT affected by the 2025 change.
    ("slack",   "conversations.list"):    _SLACK_TIER_SPECS[2],
    # users.info is Tier 4.
    ("slack",   "users.info"):            _SLACK_TIER_SPECS[4],
    ("github",  "rest_authenticated"):    BucketSpec(capacity=4000, refill_per_sec=1.11),
    ("gmail",   "per-user"):              BucketSpec(capacity=200,  refill_per_sec=200.0),
    ("discord", "channels_messages"):     BucketSpec(capacity=30,   refill_per_sec=5.0),
}


__all__ = ["BUCKET_DEFAULTS", "BucketSpec", "slack_history_tier"]
