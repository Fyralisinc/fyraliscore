"""Miro integration (whiteboard / design source).

Miro is a collaborative-whiteboard API authenticated with a long-lived Bearer
token issued to an org-level app. It exposes boards and their items (sticky
notes, shapes, frames, cards, text, connectors, …), plus webhooks on item
changes. The ingestion source key is ``miro`` and the single channel is
``miro:item``.

This package clones the Brex Bearer-token archetype. Several external-API
details are UNVERIFIED (pagination cursor shape, webhook signature scheme, exact
host + read endpoints/scopes); see the ``TODO(human): confirm …`` markers in
``client.py``, ``../../ingestion/fetchers/miro.py``, and
``../../../app/webhooks/signatures/miro.py``.
"""
