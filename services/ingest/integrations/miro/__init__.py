"""Miro integration (whiteboard / design source).

Miro is a collaborative-whiteboard API authenticated with a long-lived Bearer
token issued to an org-level app. Fyralis polls boards and their items (sticky
notes, shapes, frames, cards, text, connectors, …). The ingestion source key is
``miro`` and the single channel is ``miro:item``.

Miro's discontinued experimental webhook does not have a production handler,
signature verifier, tenant resolver, or installation binding.
"""
