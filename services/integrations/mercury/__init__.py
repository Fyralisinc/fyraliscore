"""Mercury banking integration (finance source).

Mercury is a business-banking API authenticated with a long-lived API token
(HTTP Bearer or Basic with the token as the username). It exposes accounts +
their balances and per-account transactions (cursor-paginated), plus HMAC-signed
webhooks on resource changes. The ingestion source key is ``mercury`` and the
single channel is ``mercury:transaction``.
"""
