"""Ramp integration (finance source).

Ramp is a spend/card-management platform; its Developer API (verified
docs.ramp.com) is plain REST under ``https://api.ramp.com/developer/v1``:
keyset-paginated collections (``GET /transactions``, ``/reimbursements``,
``/cards``, ``/users`` — envelope ``{"data": [...], "page": {"next": …}}``)
authenticated with OAuth 2.0 **client credentials** (Bearer token minted at
``POST /token``; no refresh token — expiry is handled by re-minting). Every
install is scoped to a company ``business_id`` (discovered via the
``GET /business`` probe; the same id every webhook carries at root). The
ingestion source key is ``ramp`` and the single channel is
``ramp:transaction``.
"""
