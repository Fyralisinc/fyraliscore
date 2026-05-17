"""services/ingestion/normalizer/invariants.py — post-validation
cross-field checks for raw envelopes.

Per M2 work-order §M2.4.

Pydantic schema validation (`RawEnvelope.model_validate`) covers
type / shape / required-field correctness. This module covers what
Pydantic can't easily express:

  - producer-discipline (e.g. raw_s3_key follows the LLD §5.1
    key scheme; content_hash is exactly 40 hex chars per
    blake2b-160).
  - cross-field consistency (e.g. ingested_at not absurdly skewed).
  - security invariants (SC-006: raw identifiers like Discord
    guild_id MUST NOT appear in ingress_metadata; the producer
    must use short_guild_hash).

PRIME DIRECTIVE
===============
`EnvelopeInvariantError` is parse-failure-class. The normalizer
worker catches it, LOGS, BUMPS a metric, COMMITS the Kafka offset,
and CONTINUES with the next message. It is NEVER propagated up the
consumer loop.

Why: a malformed envelope is a deterministic poison pill. If the
worker re-raised, the consumer would deadline-loop forever on the
same offset, blocking the entire partition's downstream pipeline.
The shadow path is best-effort during M2; one bad envelope must
not become a denial-of-service against the rest of the partition.

The "don't get stuck on garbage" property is tested at
`services/ingestion/normalizer/tests/test_worker_garbage_envelope.py`.
"""
from __future__ import annotations

import datetime as dt
import re

from services.ingestion.raw_tier.envelope import RawEnvelope


class EnvelopeInvariantError(ValueError):
    """Raised when a RawEnvelope passes Pydantic validation but
    fails a cross-field / producer-discipline / security invariant.

    Inherits ValueError so callers that catch ValueError (legacy /
    third-party code paths) still see this as a recoverable
    parse-failure-class condition, not a programmer error.
    """


# blake2b-160 → 20 bytes → 40 hex chars. Hard-coded length: any
# producer-side drift (e.g. someone swaps to blake2b-256 = 64 chars)
# fails this check immediately.
_CONTENT_HASH_RE = re.compile(r"^[0-9a-f]{40}$")

# Canonical key shape per LLD §5.1:
#   {env}/{source}/{tenant_id}/{yyyy-mm}/{hash[:2]}/{hash}.json[.zst]
# `env` is lowercase identifier (dev/stage/prod/...). `source` is
# pinned to the four canonical sources. `tenant_id` is a v4/v7 UUID.
_S3_KEY_RE = re.compile(
    r"^[a-z0-9_-]+"                              # env
    r"/(slack|github|discord|gmail)"             # source
    r"/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"  # tenant
    r"/[0-9]{4}-[0-9]{2}"                        # yyyy-mm
    r"/[0-9a-f]{2}"                              # hash prefix
    r"/[0-9a-f]{40}\.json(?:\.zst)?$"            # hash + ext
)

# Clock-skew tolerance (matches the Slack webhook handler's replay
# window — LLD §5.1 also expects producers to stamp ingested_at
# close to now()).
_MAX_FUTURE_SECONDS = 3600          # 1 hour
_MAX_PAST_SECONDS = 30 * 24 * 3600  # 30 days


# SC-006 compliance: raw identifiers MUST NOT land in ingress
# metadata. The shadow path's Discord helper hashes guild_id and
# emits `short_guild_hash`; a regression there would surface here.
_FORBIDDEN_METADATA_KEYS: frozenset[str] = frozenset(
    {"guild_id", "raw_guild_id"}
)


def assert_envelope_invariants(
    envelope: RawEnvelope,
    *,
    now: dt.datetime | None = None,
) -> None:
    """Raise `EnvelopeInvariantError` if `envelope` violates a
    post-validation invariant.

    Callers MUST catch the exception, log + bump a metric, and
    CONTINUE. Do NOT propagate (PRIME DIRECTIVE — see module
    docstring).

    Args:
      envelope: a Pydantic-validated RawEnvelope.
      now:      injectable clock for tests.
    """
    # 1. content_hash format.
    if not _CONTENT_HASH_RE.match(envelope.content_hash):
        raise EnvelopeInvariantError(
            f"content_hash must be 40 lower-hex chars (blake2b-160); "
            f"got {envelope.content_hash!r}"
        )

    # 2. raw_s3_key follows LLD §5.1 shape.
    if not _S3_KEY_RE.match(envelope.raw_s3_key):
        raise EnvelopeInvariantError(
            f"raw_s3_key does not match LLD §5.1 shape "
            f"env/source/tenant/yyyy-mm/aa/HASH.json[.zst]: "
            f"{envelope.raw_s3_key!r}"
        )

    # 3. content_hash prefix consistency: the third-to-last segment
    # of raw_s3_key MUST equal the first 2 chars of content_hash.
    # Mismatch = producer bug confusing two different bodies.
    try:
        prefix_segment = envelope.raw_s3_key.split("/")[-2]
    except IndexError:
        prefix_segment = ""
    if prefix_segment != envelope.content_hash[:2]:
        raise EnvelopeInvariantError(
            f"raw_s3_key prefix segment {prefix_segment!r} does not "
            f"match content_hash[:2] {envelope.content_hash[:2]!r}"
        )

    # 4. ingested_at within a sane clock window.
    now = now or dt.datetime.now(tz=dt.timezone.utc)
    delta = (envelope.ingested_at - now).total_seconds()
    if delta > _MAX_FUTURE_SECONDS:
        raise EnvelopeInvariantError(
            f"envelope.ingested_at is {delta:.0f}s in the future "
            f"(max allowed clock skew: {_MAX_FUTURE_SECONDS}s)"
        )
    if -delta > _MAX_PAST_SECONDS:
        raise EnvelopeInvariantError(
            f"envelope.ingested_at is {-delta:.0f}s in the past "
            f"(max allowed: {_MAX_PAST_SECONDS}s — backfill envelopes "
            f"should use ingress_kind='backfill' and bypass this check "
            f"in M5+)"
        )

    # 5. SC-006: forbidden raw identifiers in ingress_metadata.
    forbidden = _FORBIDDEN_METADATA_KEYS & set(envelope.ingress_metadata.keys())
    if forbidden:
        raise EnvelopeInvariantError(
            f"envelope.ingress_metadata contains forbidden raw "
            f"identifiers {sorted(forbidden)} (SC-006: producers must "
            f"hash raw identifiers — e.g. Discord short_guild_hash — "
            f"before emitting metadata)"
        )


__all__ = ["EnvelopeInvariantError", "assert_envelope_invariants"]
