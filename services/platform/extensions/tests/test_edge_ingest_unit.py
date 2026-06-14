"""Pure unit tests for edge-ingest trust-ceiling + channel namespacing (M4 / INV-6).
No DB.
"""
from __future__ import annotations

import pytest

from lib.shared.trust import TrustTier
from services.platform.extensions.edge_ingest import (
    EdgeIngestError, namespaced_channel, resolve_trust_tier,
)


def test_default_tier_capped_to_ceiling():
    # default (inferential_external) when ceiling is at/above it
    assert resolve_trust_tier(None, "attested_agent") == TrustTier.inferential_external
    assert resolve_trust_tier(None, "inferential_external") == TrustTier.inferential_external
    # a ceiling LESS trustworthy than the default caps the default down
    assert resolve_trust_tier(None, "unvetted") == TrustTier.unvetted


def test_request_at_or_below_ceiling_allowed():
    assert resolve_trust_tier("inferential_external", "attested_agent") == TrustTier.inferential_external
    assert resolve_trust_tier("attested_agent", "attested_agent") == TrustTier.attested_agent
    assert resolve_trust_tier("reputable", "attested_agent") == TrustTier.reputable


def test_request_above_ceiling_rejected_not_downgraded():
    with pytest.raises(EdgeIngestError) as e:
        resolve_trust_tier("attested_agent", "inferential_external")
    assert e.value.code == "trust_tier_over_ceiling" and e.value.status == 403


def test_authoritative_tiers_unreachable_regardless_of_ceiling():
    # both authoritative AND authoritative_external are unreachable even at the
    # top ceiling (authoritative_external actually ranks BELOW attested_agent, so
    # only the explicit blocklist catches it — that's the load-bearing check).
    for tier in ("authoritative", "authoritative_external"):
        with pytest.raises(EdgeIngestError) as e:
            resolve_trust_tier(tier, "attested_agent")
        assert e.value.code == "trust_tier_unreachable" and e.value.status == 403


def test_invalid_tier_rejected():
    with pytest.raises(EdgeIngestError) as e:
        resolve_trust_tier("totally_made_up", "attested_agent")
    assert e.value.code == "invalid_trust_tier"


def test_namespaced_channel():
    assert namespaced_channel("github_intel", "derived") == "ext:github_intel:derived"
    assert namespaced_channel("acme", "risk.score") == "ext:acme:risk.score"
    # an extension cannot smuggle a core channel or path separators
    for bad in ("", "github:webhook", "../x", "Bad Caps", "a/b"):
        with pytest.raises(EdgeIngestError):
            namespaced_channel("acme", bad)
