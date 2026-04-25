"""services/entity_aliases — Entity alias resolution store (Wave 1-B).

Schema refs: SCHEMA-LOCK.md S6.1, S6.2.
Public surface: `EntityAliasRepo`, `normalize_phrase` in `.repo`.
"""
from services.entity_aliases.repo import EntityAliasRepo, normalize_phrase

__all__ = ["EntityAliasRepo", "normalize_phrase"]
