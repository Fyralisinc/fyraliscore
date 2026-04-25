"""
services/query — handles CEO Ask input.

Accepts a natural-language query, classifies it, gathers context via
`services/retrieval/`, calls `services/rendering/` to render a
conversation turn, and returns the result.

Public surfaces:
  - classifier.py : QueryCategory + QueryClassifier
  - core.py       : QueryHandler.answer_query
  - api.py        : FastAPI routes `/view/ceo/ask`, `/view/ceo/turn-action`
  - prefetch.py   : pre-compute responses for query-grid chips
  - strategies/   : one module per category

Agent-QRY owns this package. Read-only into services/retrieval/ and
services/rendering/. Cache writes via adapters that stub until
Agent-GRT's view_ceo_cache migration lands.
"""
from __future__ import annotations
