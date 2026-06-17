"""lib.extensions.host_api.proposed.diff — reasoning-write contract (PROPOSED).

E5 / "model-edge contributor": submit a *typed* diff for the host's Think loop to
validate and apply — never raw SQL, never a direct substrate write. This is the
single highest-risk extension point, so it is:

  * **proposed** (not SemVer-stable), and
  * **first-party only, indefinitely** (ADR-0004 INV-1) — the host owns what
    becomes belief; third parties contribute signals at the *edge*, not here.

The surface is defined so first-party code can target a stable name, but the
apply path is intentionally NOT exposed to extensions: ``submit_diff`` refuses
any non-first-party caller and otherwise routes to the host's internal think
apply path (which lives in ``services.reasoning`` and is not importable from
``lib``). It therefore raises ``NotImplementedError`` here — wiring it is a host
decision, gated on the INV-1 precondition review.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ProposedDiff:
    """A typed proposal for the synthesis loop (NOT applied directly)."""

    kind: str  # e.g. "model.create", "edge.add"
    payload: dict[str, Any] = field(default_factory=dict)
    rationale: str = ""


def submit_diff(diff: ProposedDiff, *, trust_tier: str) -> None:
    """Submit a proposed reasoning diff. First-party only (INV-1).

    Enforces the trust gate here so the boundary is explicit; the apply path
    itself is host-internal and not wired through the extension API.
    """
    if trust_tier != "first_party":
        raise PermissionError(
            "submit_diff (reasoning writes / E5) is first-party only (ADR-0004 INV-1); "
            f"trust_tier={trust_tier!r} may not contribute into the synthesis loop"
        )
    raise NotImplementedError(
        "submit_diff is a PROPOSED, host-internal path; reasoning writes are not "
        "exposed through the extension API. Use the internal think apply path."
    )


__all__ = ["ProposedDiff", "submit_diff"]
