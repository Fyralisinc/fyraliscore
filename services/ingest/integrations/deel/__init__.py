"""Deel integration (finance source).

Deel is a contractor-payments / payroll API authenticated with a long-lived API
token (HTTP Bearer). It exposes contracts and their per-contract payments
(offset-paginated), plus HMAC-signed webhooks on resource changes. The ingestion
source key is ``deel`` and the single channel is ``deel:payment``.
"""
