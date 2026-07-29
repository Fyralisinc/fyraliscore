"""Source-owned bounded OAuth renewal entry points.

The eight-source lifecycle contract names these functions directly.  The
wrappers deliberately contain no mutable registry or source switch: the source
identity is fixed by the function selected from ``SourceDefinition`` and the
shared executor only implements the common exact-installation lease protocol.
"""
from __future__ import annotations

from services.ingest.integrations.bounded_renewal import (
    RenewalInvocation,
    RenewalOutcome,
    run_credential_renewal,
)


async def renew_quickbooks_installation(
    invocation: RenewalInvocation,
) -> RenewalOutcome:
    return await run_credential_renewal(invocation, source_id="quickbooks")


async def renew_ramp_installation(
    invocation: RenewalInvocation,
) -> RenewalOutcome:
    return await run_credential_renewal(invocation, source_id="ramp")


async def renew_gusto_installation(
    invocation: RenewalInvocation,
) -> RenewalOutcome:
    return await run_credential_renewal(invocation, source_id="gusto")


async def renew_carta_installation(
    invocation: RenewalInvocation,
) -> RenewalOutcome:
    return await run_credential_renewal(invocation, source_id="carta")


async def renew_linkedin_installation(
    invocation: RenewalInvocation,
) -> RenewalOutcome:
    return await run_credential_renewal(invocation, source_id="linkedin")


__all__ = [
    "renew_carta_installation",
    "renew_gusto_installation",
    "renew_linkedin_installation",
    "renew_quickbooks_installation",
    "renew_ramp_installation",
]
