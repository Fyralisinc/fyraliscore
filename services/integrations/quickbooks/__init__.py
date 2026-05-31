"""QuickBooks Online integration (finance source).

QuickBooks Online is an accounting API authenticated with OAuth 2.0 (Bearer
access token, ~60 min lifetime; refresh token ~100 days that ROTATES on every
refresh). Every call is scoped to a company ``realmId``. It exposes accounting
entities (Invoice, Bill, BillPayment, Payment, …) via a SQL-like query endpoint,
plus HMAC-SHA256-signed webhooks (`intuit-signature`, CloudEvents). The
ingestion source key is ``quickbooks`` and the single channel is
``quickbooks:object``.
"""
