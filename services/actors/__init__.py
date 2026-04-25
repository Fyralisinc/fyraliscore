"""services/actors — Actor store (Wave 1-B).

Schema refs: SCHEMA-LOCK.md S5.1, S5.2, S5.3.
Public surface: `ActorRepo` in `.repo`.
"""
from services.actors.repo import ActorRepo

__all__ = ["ActorRepo"]
