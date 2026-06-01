"""Per-(source, method) default bucket specs.

Per ingestion LLD §13. Capacity/refill values match the table at the
end of that section; tier-multiplier support is a future feature (v1
has one tier).

Each call site (FetchPage activity in M3) picks the bucket spec via
`BUCKET_DEFAULTS[(source, method)]`. The composite key keeps the
table close to grep-readable for ops.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BucketSpec:
    """Token-bucket parameters for one (source, method).

    `capacity`         — burst size in tokens.
    `refill_per_sec`   — steady-state refill rate.
    """

    capacity: int
    refill_per_sec: float


# Keys: (source, method). Method strings match the per-source FetchPage
# call sites in M3 — names follow each source's API conventions:
#   slack:    Web API method strings, e.g. "conversations.history"
#   github:   logical group, e.g. "rest_authenticated" (one bucket per app)
#   gmail:    "per-user" — Gmail's per-user quota
#   discord:  logical group, e.g. "channels_messages"
BUCKET_DEFAULTS: dict[tuple[str, str], BucketSpec] = {
    ("slack",   "conversations.history"): BucketSpec(capacity=40,   refill_per_sec=0.67),
    ("slack",   "conversations.list"):    BucketSpec(capacity=40,   refill_per_sec=0.67),
    ("slack",   "users.info"):            BucketSpec(capacity=40,   refill_per_sec=0.67),
    ("github",  "rest_authenticated"):    BucketSpec(capacity=4000, refill_per_sec=1.11),
    ("gmail",   "per-user"):              BucketSpec(capacity=200,  refill_per_sec=200.0),
    ("discord", "channels_messages"):     BucketSpec(capacity=30,   refill_per_sec=5.0),
}


__all__ = ["BUCKET_DEFAULTS", "BucketSpec"]
