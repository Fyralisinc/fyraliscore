"""
services/product/query — handles CEO Ask input.

Accepts a natural-language query, classifies it, gathers context via
`services/reasoning/retrieval/`, calls `services/product/rendering/` to render a
conversation turn, and returns the result.

Public surfaces:
  - classifier.py : QueryCategory + QueryClassifier
  - core.py       : QueryHandler.answer_query
  - api.py        : FastAPI routes `/view/ceo/ask`, `/view/ceo/turn-action`
  - prefetch.py   : pre-compute responses for query-grid chips
  - strategies/   : one module per category

Agent-QRY owns this package. Read-only into services/reasoning/retrieval/ and
services/product/rendering/. Cache writes via adapters that stub until
Agent-GRT's view_ceo_cache migration lands.
"""
from __future__ import annotations
