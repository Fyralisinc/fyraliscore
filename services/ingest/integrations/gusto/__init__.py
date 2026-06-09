"""Gusto integration (finance source).

Gusto is a payroll/HR API authenticated with OAuth 2.0 (Bearer access token,
short-lived; rotating refresh token). Every call is scoped to a company
``company_uuid``. It exposes payroll/HR entities (payrolls, employees,
contractor payments, …) plus HMAC-signed webhooks (signature scheme UNVERIFIED —
see `services/app/webhooks/signatures/gusto.py`). The ingestion source key is
``gusto`` and the single channel is ``gusto:object``.

This package clones the QuickBooks OAuth2 archetype; the read surface, pagination,
signature scheme, and OAuth refresh are flagged `TODO(human): confirm ...` where
the real Gusto behavior is unverified (per the implementation blueprint §5).
"""
