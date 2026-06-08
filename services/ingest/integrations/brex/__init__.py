"""Brex integration (finance source).

Brex is a corporate-card / cash-management API authenticated with a long-lived
API token (HTTP Bearer). It exposes accounts + their balances and per-account
transactions, plus webhooks on resource changes. The ingestion source key is
``brex`` and the single channel is ``brex:transaction``.

This package clones the Mercury Bearer-token archetype. Several external-API
details are UNVERIFIED (pagination scheme, webhook signature scheme, exact host
+ read endpoints/scopes); see the ``TODO(human): confirm …`` markers in
``client.py``, ``../../ingestion/fetchers/brex.py``, and
``../../../app/webhooks/signatures/brex.py``.
"""
